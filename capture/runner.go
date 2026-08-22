package capture

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
)

const MaxExpectedSubscriptions = 256

var (
	ErrInvalidRunnerConfig  = errors.New("capture: invalid runner configuration")
	ErrRunnerNotStarted     = errors.New("capture: runner not started")
	ErrRunnerClosed         = errors.New("capture: runner closed")
	ErrObserverContract     = errors.New("capture: observer returned an invalid observation")
	ErrRESTRequestIDChanged = errors.New("capture: REST retry changed request identity")
)

type RunnerState uint8

const (
	RunnerCreated RunnerState = iota + 1
	RunnerRunning
	RunnerBackpressured
	RunnerClosed
)

type FaultKind uint8

const (
	FaultACKPartial FaultKind = iota + 1
	FaultACKWrong
	FaultACKDuplicate
	FaultACKRejected
	FaultACKTimeout
	FaultHeartbeatMissed
	FaultDisconnectAbrupt
	FaultDNS
	FaultTLS
	FaultConnect
	FaultRateExhausted
	FaultRateRetryable
	FaultRateTerminal
	FaultRateCircuit
	FaultSchemaMalformed
	FaultSchemaAdditive
	FaultSchemaTypeChanged
	FaultSchemaOversized
	FaultSchemaUnknownRole
	FaultQueuePressure
	FaultSinkFailure
	FaultCanceled
	FaultACKOverflow
	FaultHeartbeatEarly
	FaultUsefulDataMissed
)

type Fault struct {
	Kind             FaultKind
	HTTPStatus       int
	RetryAtMonotonic uint64
}

type StepResult struct {
	State         RunnerState
	Progressed    bool
	Blocked       bool
	Envelopes     []EnvelopeV1
	Controls      []ControlRecord
	Opportunities []OpportunityRecord
	Faults        []Fault
	Unresolved    []EnvelopeV1
}

type RunnerConfig struct {
	Epoch                 StreamEpoch
	ChannelOrEndpoint     string
	DataFamily            string
	RecorderVersion       string
	NativeSymbol          OptionalString
	SubscriptionRequestID string
	ExpectedSubscriptions []string
	ScheduledAtNS         OptionalInt64
}

type runnerActionAfter uint8

const (
	afterNone runnerActionAfter = iota
	afterConnected
	afterSubscribed
	afterRequestStarted
)

type runnerAction struct {
	kind             ControlKind
	outcome          TerminalOutcome
	state            string
	requestID        string
	scheduledAt      OptionalInt64
	requestStartedAt OptionalInt64
	extensions       []byte
	fault            *Fault
	opportunity      *OpportunityRecord
	close            *closePlan
	after            runnerActionAfter
}

type closePlan struct {
	reason CloseReason
	blind  *BlindInterval
}

type pendingWrite struct {
	envelope EnvelopeV1
	control  *ControlRecord
	event    *TransportEvent
	action   *runnerAction
	close    *closePlan
}

// Runner processes at most one transport event or one internal control action
// per Step. It retains at most one exact envelope under pressure.
type Runner struct {
	mu sync.Mutex

	contract  SourceContract
	config    RunnerConfig
	transport Transport
	clock     Clock
	rate      RateBudget
	sink      RawSink
	observer  RawObserver
	assigner  *OrdinalAssigner

	state                        RunnerState
	committed                    uint64
	pending                      *pendingWrite
	action                       *runnerAction
	needConnect                  bool
	needSubscribe                bool
	connected                    bool
	ackDone                      bool
	ackTimer                     Timer
	ackDeadlineWallNS            int64
	ackedSubscriptions           map[string]struct{}
	ackBatchCount                uint16
	heartbeatTimer               Timer
	heartbeatDeadlineWallNS      int64
	heartbeatEarliestMonotonicNS uint64
	heartbeatCount               uint64
	usefulDataTimer              Timer
	usefulDataDeadlineWallNS     int64
	requestActive                bool
	requestStartedNS             int64
	requestID                    string
	attempts                     uint16
}

func NewRunner(contract SourceContract, config RunnerConfig, transport Transport, clock Clock, rate RateBudget, sink RawSink, observer RawObserver) (*Runner, error) {
	if err := contract.Validate(); err != nil {
		return nil, err
	}
	if transport == nil || clock == nil || rate == nil || sink == nil || observer == nil {
		return nil, fmt.Errorf("%w: transport, clock, rate budget, sink, and observer are required", ErrInvalidRunnerConfig)
	}
	if err := validateRunnerConfig(contract, config); err != nil {
		return nil, err
	}
	assigner, err := NewOrdinalAssigner(contract.SourceID, config.Epoch)
	if err != nil {
		return nil, err
	}
	config.ExpectedSubscriptions = slices.Clone(config.ExpectedSubscriptions)
	return &Runner{
		contract:           contract,
		config:             config,
		transport:          transport,
		clock:              clock,
		rate:               rate,
		sink:               sink,
		observer:           observer,
		assigner:           assigner,
		state:              RunnerCreated,
		ackedSubscriptions: make(map[string]struct{}, len(config.ExpectedSubscriptions)),
	}, nil
}

func validateRunnerConfig(contract SourceContract, config RunnerConfig) error {
	if err := config.Epoch.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRunnerConfig, err)
	}
	wantEpoch := EpochConnection
	if contract.Topology.Transport == TransportREST {
		wantEpoch = EpochPollCycle
	}
	if config.Epoch.Kind != wantEpoch {
		return fmt.Errorf("%w: epoch kind does not match transport topology", ErrInvalidRunnerConfig)
	}
	if err := validateContractText("channel_or_endpoint", config.ChannelOrEndpoint, MaxContractIDBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRunnerConfig, err)
	}
	if err := validateContractText("data_family", config.DataFamily, MaxIdentityBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRunnerConfig, err)
	}
	capability, ok := contract.Capability(config.ChannelOrEndpoint, config.DataFamily)
	if !ok || capability.Support != SupportAvailable {
		return fmt.Errorf("%w: selected capability is not explicitly available", ErrInvalidRunnerConfig)
	}
	if err := validateContractText("recorder_version", config.RecorderVersion, MaxRecorderVersionBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRunnerConfig, err)
	}
	if err := validateOptionalString("native_symbol", config.NativeSymbol, MaxSymbolBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRunnerConfig, err)
	}
	if len(config.ExpectedSubscriptions) > MaxExpectedSubscriptions || len(config.ExpectedSubscriptions) > int(contract.Topology.MaxSubscriptions) {
		return fmt.Errorf("%w: expected subscription inventory exceeds its bound", ErrInvalidRunnerConfig)
	}
	seen := make(map[string]struct{}, len(config.ExpectedSubscriptions))
	for i, subscription := range config.ExpectedSubscriptions {
		if err := validateContractText(fmt.Sprintf("expected_subscriptions[%d]", i), subscription, MaxIdentityBytes); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidRunnerConfig, err)
		}
		if _, ok := seen[subscription]; ok {
			return fmt.Errorf("%w: duplicate expected subscription %q", ErrInvalidRunnerConfig, subscription)
		}
		seen[subscription] = struct{}{}
	}
	if contract.Topology.Transport == TransportWebSocket {
		if contract.Subscription.ACKMode == ACKExact {
			if len(config.ExpectedSubscriptions) == 0 || config.SubscriptionRequestID == "" {
				return fmt.Errorf("%w: exact ACK requires request ID and expected inventory", ErrInvalidRunnerConfig)
			}
			perBatch := uint64(contract.Topology.MaxSubscriptionsPerACK)
			if perBatch == 0 {
				return fmt.Errorf("%w: exact ACK requires a positive per-batch bound", ErrInvalidRunnerConfig)
			}
			subscriptions := uint64(len(config.ExpectedSubscriptions))
			requiredBatches := subscriptions / perBatch
			if subscriptions%perBatch != 0 {
				requiredBatches++
			}
			if requiredBatches > uint64(contract.Subscription.MaxPendingACK) {
				return fmt.Errorf("%w: expected subscription inventory requires %d ACK batches, maximum is %d", ErrInvalidRunnerConfig, requiredBatches, contract.Subscription.MaxPendingACK)
			}
		} else if len(config.ExpectedSubscriptions) != 0 || config.SubscriptionRequestID != "" {
			return fmt.Errorf("%w: ACK-none cannot carry expected ACK state", ErrInvalidRunnerConfig)
		}
		if config.ScheduledAtNS.Valid {
			return fmt.Errorf("%w: WebSocket runner cannot carry scheduled poll time", ErrInvalidRunnerConfig)
		}
	} else {
		if !config.ScheduledAtNS.Valid {
			return fmt.Errorf("%w: REST runner requires scheduled poll time", ErrInvalidRunnerConfig)
		}
		if len(config.ExpectedSubscriptions) != 0 || config.SubscriptionRequestID != "" {
			return fmt.Errorf("%w: REST runner cannot carry subscription state", ErrInvalidRunnerConfig)
		}
	}
	return nil
}

func (r *Runner) Start(ctx context.Context) (StepResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := StepResult{State: r.state}
	if r.state != RunnerCreated {
		return result, fmt.Errorf("capture: runner Start in state %d", r.state)
	}
	r.state = RunnerRunning
	if r.contract.Topology.Transport == TransportWebSocket {
		reading := r.clock.Read()
		decision, err := r.rate.Acquire(reading.MonotonicNS, r.contract.Rate.ConnectionCost)
		if err != nil {
			return r.abortRateError(ctx, result, err)
		}
		if !decision.Allowed {
			r.needConnect = true
			fault := Fault{Kind: FaultRateExhausted, RetryAtMonotonic: decision.RetryAtMonotonic}
			action := runnerAction{kind: ControlRateLimited, outcome: TerminalRateLimited, fault: &fault}
			return r.runAction(ctx, result, &action)
		}
		action := runnerAction{kind: ControlConnectAttempt, outcome: TerminalObserved}
		return r.runAction(ctx, result, &action)
	}
	action := runnerAction{kind: ControlPollScheduled, outcome: TerminalObserved}
	return r.runAction(ctx, result, &action)
}

func (r *Runner) Step(ctx context.Context) (StepResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := StepResult{State: r.state}
	if r.state == RunnerCreated {
		return result, ErrRunnerNotStarted
	}
	if r.state == RunnerClosed {
		return result, ErrRunnerClosed
	}
	if err := ctx.Err(); err != nil {
		return r.cancelRunner(ctx, result, err)
	}
	if r.pending != nil {
		return r.flushPending(ctx, result)
	}
	if r.action != nil {
		action := r.action
		r.action = nil
		return r.runAction(ctx, result, action)
	}
	if r.needConnect {
		return r.retryConnect(ctx, result)
	}
	if r.needSubscribe {
		r.needSubscribe = false
		action := runnerAction{kind: ControlSubscribeRequest, outcome: TerminalObserved, state: r.config.SubscriptionRequestID, after: afterSubscribed}
		return r.runAction(ctx, result, &action)
	}
	if r.ackTimer != nil && r.ackTimer.Fired() && !r.ackDone {
		r.ackTimer.Stop()
		r.ackTimer = nil
		fault := Fault{Kind: FaultACKTimeout}
		opportunity := r.ackOpportunity(OpportunityOutcomeSourceStale)
		action := runnerAction{
			kind:        ControlTimeout,
			outcome:     TerminalTimeout,
			fault:       &fault,
			opportunity: &opportunity,
			close:       &closePlan{reason: CloseACKTimeout, blind: r.blindInterval(CloseACKTimeout)},
		}
		return r.runAction(ctx, result, &action)
	}
	if r.heartbeatTimer != nil && r.heartbeatTimer.Fired() {
		r.heartbeatTimer.Stop()
		r.heartbeatTimer = nil
		fault := Fault{Kind: FaultHeartbeatMissed}
		opportunity := r.heartbeatOpportunity(OpportunityOutcomeSourceStale)
		action := runnerAction{
			kind:        ControlTimeout,
			outcome:     TerminalTimeout,
			fault:       &fault,
			opportunity: &opportunity,
			close:       &closePlan{reason: CloseHeartbeatMissed, blind: r.blindInterval(CloseHeartbeatMissed)},
		}
		return r.runAction(ctx, result, &action)
	}
	if r.usefulDataTimer != nil && r.usefulDataTimer.Fired() {
		r.usefulDataTimer.Stop()
		r.usefulDataTimer = nil
		fault := Fault{Kind: FaultUsefulDataMissed}
		action := runnerAction{
			kind:    ControlTimeout,
			outcome: TerminalTimeout,
			fault:   &fault,
			close:   &closePlan{reason: CloseUsefulDataMissed, blind: r.blindInterval(CloseUsefulDataMissed)},
		}
		return r.runAction(ctx, result, &action)
	}
	event, err := r.transport.Next(ctx)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return r.cancelRunner(ctx, result, err)
		}
		return result, err
	}
	return r.handleTransportEvent(ctx, result, event)
}

func (r *Runner) cancelRunner(ctx context.Context, result StepResult, cause error) (StepResult, error) {
	result.Faults = append(result.Faults, Fault{Kind: FaultCanceled})
	if r.config.Epoch.Kind == EpochPollCycle {
		result.Opportunities = append(result.Opportunities, r.restOpportunity(OpportunityOutcomeCollectorFailed))
	}
	if r.pending != nil {
		if flushErr := r.flushPendingForCancellation(ctx, &result); flushErr != nil {
			return result, errors.Join(cause, flushErr)
		}
	}
	closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseCanceled, blind: r.blindInterval(CloseCanceled)})
	return result, errors.Join(cause, closeErr)
}

func (r *Runner) retryConnect(ctx context.Context, result StepResult) (StepResult, error) {
	reading := r.clock.Read()
	decision, err := r.rate.Acquire(reading.MonotonicNS, r.contract.Rate.ConnectionCost)
	if err != nil {
		return r.abortRateError(ctx, result, err)
	}
	if !decision.Allowed {
		result.Blocked = true
		result.State = RunnerBackpressured
		r.state = RunnerBackpressured
		return result, nil
	}
	r.needConnect = false
	r.state = RunnerRunning
	action := runnerAction{kind: ControlConnectAttempt, outcome: TerminalObserved}
	return r.runAction(ctx, result, &action)
}

func (r *Runner) handleTransportEvent(ctx context.Context, result StepResult, event TransportEvent) (StepResult, error) {
	if err := validateTransportEvent(event); err != nil {
		return result, err
	}
	switch event.Kind {
	case TransportEventConnected:
		if r.contract.Topology.Transport != TransportWebSocket || r.connected {
			return result, fmt.Errorf("capture: invalid connected transition")
		}
		r.connected = true
		action := runnerAction{kind: ControlConnected, outcome: TerminalObserved, state: event.WSState, after: afterConnected}
		return r.runAction(ctx, result, &action)
	case TransportEventRequest:
		return r.handleRequest(ctx, result, event)
	case TransportEventApplication, TransportEventAcknowledgement, TransportEventHeartbeat, TransportEventHTTPResponse:
		return r.persistRawEvent(ctx, result, event)
	case TransportEventDisconnected:
		reason := CloseAbrupt
		if event.Planned {
			reason = ClosePlanned
		} else {
			result.Faults = append(result.Faults, Fault{Kind: FaultDisconnectAbrupt})
		}
		plan := closePlan{reason: reason}
		if !event.Planned {
			plan.blind = r.blindInterval(reason)
		}
		err := r.closeEpoch(ctx, &result, plan)
		return result, err
	case TransportEventFailure:
		fault := Fault{Kind: FaultConnect}
		if event.Failure == TransportFailureDNS {
			fault.Kind = FaultDNS
		} else if event.Failure == TransportFailureTLS {
			fault.Kind = FaultTLS
		}
		result.Faults = append(result.Faults, fault)
		err := r.closeEpoch(ctx, &result, closePlan{reason: CloseTransportFailure, blind: r.blindInterval(CloseTransportFailure)})
		return result, err
	default:
		return result, fmt.Errorf("capture: unsupported transport event %d", event.Kind)
	}
}

func (r *Runner) handleRequest(ctx context.Context, result StepResult, event TransportEvent) (StepResult, error) {
	if r.contract.Topology.Transport != TransportREST || r.requestActive {
		return result, fmt.Errorf("capture: invalid request transition")
	}
	if r.requestID != "" && event.RequestID != r.requestID {
		result.Faults = append(result.Faults, Fault{Kind: FaultRateTerminal})
		closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseRateTerminal, blind: r.blindInterval(CloseRateTerminal)})
		return result, errors.Join(ErrRESTRequestIDChanged, closeErr)
	}
	if r.attempts >= r.contract.Rate.MaxAttempts {
		fault := Fault{Kind: FaultRateExhausted}
		opportunity := r.restOpportunity(OpportunityOutcomeRateLimited)
		action := runnerAction{kind: ControlRateLimited, outcome: TerminalRateLimited, fault: &fault, opportunity: &opportunity, close: &closePlan{reason: CloseRateTerminal}}
		return r.runAction(ctx, result, &action)
	}
	reading := r.clock.Read()
	cost := r.contract.Rate.RequestCost
	decision, err := r.rate.Acquire(reading.MonotonicNS, cost)
	if err != nil {
		return r.abortRateError(ctx, result, err)
	}
	if !decision.Allowed {
		fault := Fault{Kind: FaultRateExhausted, RetryAtMonotonic: decision.RetryAtMonotonic}
		action := runnerAction{kind: ControlRateLimited, outcome: TerminalRateLimited, fault: &fault}
		return r.runAction(ctx, result, &action)
	}
	if r.requestID == "" {
		r.requestID = event.RequestID
	}
	r.attempts++
	r.requestActive = true
	r.requestStartedNS = reading.WallTimeNS
	evidence, err := MarshalRESTRequestEvidence(RESTRequestEvidenceV1{
		Version:       RESTEvidenceVersion,
		Kind:          "request",
		RequestID:     r.requestID,
		Method:        event.Method,
		Parameters:    event.SanitizedParameters,
		ScheduledAtNS: r.config.ScheduledAtNS.Value,
		StartedAtNS:   reading.WallTimeNS,
	})
	if err != nil {
		return result, err
	}
	action := runnerAction{
		kind:             ControlRequestStarted,
		outcome:          TerminalObserved,
		requestID:        r.requestID,
		scheduledAt:      r.config.ScheduledAtNS,
		requestStartedAt: OptionalInt64{Value: reading.WallTimeNS, Valid: true},
		extensions:       evidence,
		after:            afterRequestStarted,
	}
	return r.runAction(ctx, result, &action)
}

func (r *Runner) persistRawEvent(ctx context.Context, result StepResult, event TransportEvent) (StepResult, error) {
	envelope, control, err := r.rawEnvelope(event)
	if err != nil {
		return result, err
	}
	if err := r.assigner.Assign(&envelope); err != nil {
		return result, err
	}
	if control != nil {
		control.Envelope = envelope
		if err := control.Validate(); err != nil {
			return result, err
		}
	} else if err := envelope.Validate(); err != nil {
		return result, err
	}
	pending := &pendingWrite{envelope: envelope, control: control, event: pointerToEvent(event)}
	err = r.sink.WriteRaw(ctx, cloneEnvelope(envelope))
	if errors.Is(err, ErrSinkFull) {
		r.pending = pending
		r.state = RunnerBackpressured
		result.Blocked = true
		result.State = r.state
		result.Faults = append(result.Faults, Fault{Kind: FaultQueuePressure})
		if !r.contract.Topology.Throttleable {
			plan := closePlan{reason: CloseQueuePressure, blind: r.blindInterval(CloseQueuePressure)}
			r.pending.close = &plan
			_ = r.transport.Close(ctx, CloseQueuePressure)
		}
		return result, nil
	}
	if err != nil {
		r.pending = pending
		return r.abortSinkFailure(ctx, result, err)
	}
	r.committed = envelope.ArrivalOrdinal
	return r.completeRaw(ctx, result, pending)
}

func (r *Runner) rawEnvelope(event TransportEvent) (EnvelopeV1, *ControlRecord, error) {
	reading := r.clock.Read()
	envelope := r.baseEnvelope(reading, TerminalObserved)
	envelope.PayloadEncoding = event.Encoding
	envelope.SetRawPayload(event.Raw)
	envelope.SubscriptionOrRequestID = OptionalString{}
	envelope.HTTPStatusOrWSState = OptionalString{}
	if event.RequestID != "" {
		envelope.SubscriptionOrRequestID = OptionalString{Value: event.RequestID, Valid: true}
	}
	if event.WSState != "" {
		envelope.HTTPStatusOrWSState = OptionalString{Value: event.WSState, Valid: true}
	}
	switch event.Kind {
	case TransportEventApplication:
		if r.contract.Topology.Transport != TransportWebSocket {
			return EnvelopeV1{}, nil, errors.New("capture: application event requires WebSocket topology")
		}
		envelope.RecordKind = RecordKindWebSocket
	case TransportEventAcknowledgement:
		if r.contract.Topology.Transport != TransportWebSocket {
			return EnvelopeV1{}, nil, errors.New("capture: acknowledgement requires WebSocket topology")
		}
		envelope.RecordKind = RecordKindControl
		envelope.ControlKind = OptionalControlKind{Value: ControlAcknowledgement, Valid: true}
		return envelope, &ControlRecord{Envelope: envelope}, nil
	case TransportEventHeartbeat:
		if r.contract.Topology.Transport != TransportWebSocket {
			return EnvelopeV1{}, nil, errors.New("capture: heartbeat requires WebSocket topology")
		}
		envelope.RecordKind = RecordKindControl
		envelope.ControlKind = OptionalControlKind{Value: ControlHeartbeat, Valid: true}
		return envelope, &ControlRecord{Envelope: envelope}, nil
	case TransportEventHTTPResponse:
		if r.contract.Topology.Transport != TransportREST || !r.requestActive {
			return EnvelopeV1{}, nil, errors.New("capture: HTTP response has no active request")
		}
		if event.RequestID != r.requestID {
			return EnvelopeV1{}, nil, errors.New("capture: HTTP response request ID mismatch")
		}
		envelope.RecordKind = RecordKindREST
		envelope.ScheduledAtNS = r.config.ScheduledAtNS
		envelope.RequestStartedAtNS = OptionalInt64{Value: r.requestStartedNS, Valid: true}
		envelope.RequestCompletedAtNS = OptionalInt64{Value: reading.WallTimeNS, Valid: true}
		envelope.HTTPStatusOrWSState = OptionalString{Value: fmt.Sprintf("%d", event.HTTPStatus), Valid: true}
		evidence, err := MarshalRESTResponseEvidence(RESTResponseEvidenceV1{
			Version:       RESTEvidenceVersion,
			Kind:          "response",
			RequestID:     event.RequestID,
			CompletedAtNS: reading.WallTimeNS,
			Status:        event.HTTPStatus,
			RetryAfterNS:  event.RetryAfterNS,
			Headers:       event.ResponseHeaders,
		})
		if err != nil {
			return EnvelopeV1{}, nil, err
		}
		envelope.Extensions = evidence
	default:
		return EnvelopeV1{}, nil, errors.New("capture: event has no raw envelope")
	}
	return envelope, nil, nil
}

func (r *Runner) completeRaw(ctx context.Context, result StepResult, pending *pendingWrite) (StepResult, error) {
	envelope := cloneEnvelope(pending.envelope)
	result.Envelopes = append(result.Envelopes, envelope)
	if pending.control != nil {
		control := ControlRecord{Envelope: cloneEnvelope(envelope)}
		result.Controls = append(result.Controls, control)
	}
	result.Progressed = true
	result.State = r.state
	event := *pending.event
	if uint32(len(envelope.RawPayload)) > r.contract.Payload.MaxRawBytes {
		action := r.schemaQuarantineAction(event.Kind, FaultSchemaOversized, TerminalRejected, OpportunityOutcomeSchemaRejected)
		r.action = &action
		return r.closeAfterPending(ctx, result, pending)
	}
	observation, err := r.observer.Observe(ctx, cloneEnvelope(envelope))
	if err != nil {
		action := r.schemaQuarantineAction(event.Kind, FaultSchemaMalformed, TerminalMalformed, OpportunityOutcomeMalformed)
		r.action = &action
		return r.closeAfterPending(ctx, result, pending)
	}
	if err := validateObservation(event.Kind, observation); err != nil {
		return r.abortObserverFailure(ctx, result, err)
	}
	if err := r.validateObservationBounds(observation); err != nil {
		return r.abortObserverFailure(ctx, result, err)
	}
	if observation.Schema == SchemaAdditive {
		result.Faults = append(result.Faults, Fault{Kind: FaultSchemaAdditive})
	} else if observation.Schema != SchemaAccepted {
		faultKind := FaultSchemaTypeChanged
		terminal := TerminalRejected
		opportunityOutcome := OpportunityOutcomeSchemaRejected
		switch observation.Schema {
		case SchemaMalformed:
			faultKind = FaultSchemaMalformed
			terminal = TerminalMalformed
			opportunityOutcome = OpportunityOutcomeMalformed
		case SchemaUnknownRole:
			faultKind = FaultSchemaUnknownRole
		}
		action := r.schemaQuarantineAction(event.Kind, faultKind, terminal, opportunityOutcome)
		r.action = &action
		return r.closeAfterPending(ctx, result, pending)
	}
	switch event.Kind {
	case TransportEventAcknowledgement:
		result, err = r.handleACKObservation(ctx, result, observation.ACK)
	case TransportEventHeartbeat:
		result, err = r.handleHeartbeatObservation(ctx, result, observation)
	case TransportEventApplication:
		if observation.Role == MessageData {
			if timerErr := r.observeUsefulData(); timerErr != nil {
				err = timerErr
			}
		}
	case TransportEventHTTPResponse:
		result, err = r.handleHTTPObservation(ctx, result, event, observation)
	}
	if err != nil {
		return result, err
	}
	return r.closeAfterPending(ctx, result, pending)
}

func (r *Runner) closeAfterPending(ctx context.Context, result StepResult, pending *pendingWrite) (StepResult, error) {
	if pending.close == nil {
		return result, nil
	}
	err := r.closeEpoch(ctx, &result, *pending.close)
	return result, err
}

func (r *Runner) schemaQuarantineAction(eventKind TransportEventKind, faultKind FaultKind, terminal TerminalOutcome, opportunityOutcome OpportunityOutcome) runnerAction {
	fault := Fault{Kind: faultKind}
	action := runnerAction{kind: ControlParseQuarantine, outcome: terminal, fault: &fault}
	switch eventKind {
	case TransportEventAcknowledgement:
		opportunity := r.ackOpportunity(opportunityOutcome)
		action.opportunity = &opportunity
		action.close = &closePlan{reason: CloseSchemaRejected, blind: r.blindInterval(CloseSchemaRejected)}
	case TransportEventHeartbeat:
		opportunity := r.heartbeatOpportunity(opportunityOutcome)
		action.opportunity = &opportunity
		action.close = &closePlan{reason: CloseSchemaRejected, blind: r.blindInterval(CloseSchemaRejected)}
	case TransportEventHTTPResponse:
		r.requestActive = false
		opportunity := r.restOpportunity(opportunityOutcome)
		action.opportunity = &opportunity
		action.close = &closePlan{reason: CloseSchemaRejected}
	}
	return action
}

func (r *Runner) handleACKObservation(ctx context.Context, result StepResult, ack ACKObservation) (StepResult, error) {
	if r.contract.Subscription.ACKMode != ACKExact {
		return r.abortObserverFailure(ctx, result, errors.New("ACK observed for ACK-none contract"))
	}
	if r.ackDone {
		result.Faults = append(result.Faults, Fault{Kind: FaultACKDuplicate})
		closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseACKRejected, blind: r.blindInterval(CloseACKRejected)})
		return result, closeErr
	}
	if ack.RequestID != r.config.SubscriptionRequestID {
		return r.rejectACK(ctx, result, FaultACKWrong, OpportunityOutcomeSourceStale)
	}
	if !ack.Accepted {
		return r.rejectACK(ctx, result, FaultACKRejected, OpportunityOutcomeVenueUnavailable)
	}
	if len(ack.Subscriptions) == 0 && !ack.FinalBatch {
		return r.rejectACK(ctx, result, FaultACKWrong, OpportunityOutcomeMalformed)
	}
	if r.ackBatchCount >= r.contract.Subscription.MaxPendingACK ||
		len(ack.Subscriptions) > int(r.contract.Topology.MaxSubscriptionsPerACK) {
		return r.rejectACK(ctx, result, FaultACKOverflow, OpportunityOutcomeSourceStale)
	}
	r.ackBatchCount++
	for _, subscription := range ack.Subscriptions {
		if !slices.Contains(r.config.ExpectedSubscriptions, subscription) {
			return r.rejectACK(ctx, result, FaultACKWrong, OpportunityOutcomeSourceStale)
		}
		if _, duplicate := r.ackedSubscriptions[subscription]; duplicate {
			return r.rejectACK(ctx, result, FaultACKDuplicate, OpportunityOutcomeMalformed)
		}
		r.ackedSubscriptions[subscription] = struct{}{}
	}
	if !ack.FinalBatch {
		return result, nil
	}
	if len(r.ackedSubscriptions) != len(r.config.ExpectedSubscriptions) {
		return r.rejectACK(ctx, result, FaultACKPartial, OpportunityOutcomeSourceStale)
	}
	r.ackDone = true
	if r.ackTimer != nil {
		r.ackTimer.Stop()
		r.ackTimer = nil
	}
	result.Opportunities = append(result.Opportunities, r.ackOpportunity(OpportunityOutcomeObserved))
	return result, nil
}

func (r *Runner) rejectACK(ctx context.Context, result StepResult, kind FaultKind, outcome OpportunityOutcome) (StepResult, error) {
	r.ackDone = true
	if r.ackTimer != nil {
		r.ackTimer.Stop()
		r.ackTimer = nil
	}
	result.Faults = append(result.Faults, Fault{Kind: kind})
	result.Opportunities = append(result.Opportunities, r.ackOpportunity(outcome))
	closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseACKRejected, blind: r.blindInterval(CloseACKRejected)})
	return result, closeErr
}

func (r *Runner) handleHeartbeatObservation(ctx context.Context, result StepResult, observation Observation) (StepResult, error) {
	if r.contract.Heartbeat.Mode == HeartbeatNone {
		return r.abortObserverFailure(ctx, result, errors.New("heartbeat observed for heartbeat-none contract"))
	}
	reading := r.clock.Read()
	if reading.MonotonicNS < r.heartbeatEarliestMonotonicNS {
		result.Faults = append(result.Faults, Fault{Kind: FaultHeartbeatEarly})
		result.Opportunities = append(result.Opportunities, r.heartbeatOpportunity(OpportunityOutcomeMalformed))
		closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseHeartbeatCadence, blind: r.blindInterval(CloseHeartbeatCadence)})
		return result, closeErr
	}
	if r.heartbeatTimer != nil {
		r.heartbeatTimer.Stop()
	}
	r.heartbeatCount++
	outcome := OpportunityOutcomeObserved
	if observation.Unchanged {
		outcome = OpportunityOutcomeObservedUnchanged
	}
	result.Opportunities = append(result.Opportunities, r.heartbeatOpportunity(outcome))
	if err := r.armHeartbeatTimer(); err != nil {
		return result, err
	}
	return result, nil
}

func (r *Runner) handleSchemaObservation(ctx context.Context, result StepResult, observation Observation) (StepResult, error) {
	switch observation.Schema {
	case SchemaAccepted:
		return result, nil
	case SchemaAdditive:
		result.Faults = append(result.Faults, Fault{Kind: FaultSchemaAdditive})
		return result, nil
	case SchemaNonSemanticTypeChanged, SchemaSemanticChanged:
		fault := Fault{Kind: FaultSchemaTypeChanged}
		action := runnerAction{kind: ControlParseQuarantine, outcome: TerminalRejected, fault: &fault}
		r.action = &action
		return result, nil
	case SchemaMalformed:
		fault := Fault{Kind: FaultSchemaMalformed}
		action := runnerAction{kind: ControlParseQuarantine, outcome: TerminalMalformed, fault: &fault}
		r.action = &action
		return result, nil
	case SchemaUnknownRole:
		fault := Fault{Kind: FaultSchemaUnknownRole}
		action := runnerAction{kind: ControlParseQuarantine, outcome: TerminalRejected, fault: &fault}
		r.action = &action
		return result, nil
	default:
		return r.abortObserverFailure(ctx, result, errors.New("invalid schema disposition"))
	}
}

func (r *Runner) handleHTTPObservation(ctx context.Context, result StepResult, event TransportEvent, observation Observation) (StepResult, error) {
	r.requestActive = false
	reading := r.clock.Read()
	decision, err := r.rate.ObserveResponse(reading.MonotonicNS, event.HTTPStatus, event.RetryAfterNS)
	if err != nil {
		return r.abortRateError(ctx, result, err)
	}
	switch decision.Disposition {
	case ResponseAccepted:
		if event.HTTPStatus >= 200 && event.HTTPStatus < 300 {
			result.Opportunities = append(result.Opportunities, r.restOpportunity(OpportunityOutcomeObserved))
			closeErr := r.closeEpoch(ctx, &result, closePlan{reason: ClosePlanned})
			return result, closeErr
		}
		result.Faults = append(result.Faults, Fault{Kind: FaultRateTerminal, HTTPStatus: event.HTTPStatus})
		result.Opportunities = append(result.Opportunities, r.restOpportunity(OpportunityOutcomeVenueUnavailable))
		closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseRateTerminal, blind: r.blindInterval(CloseRateTerminal)})
		return result, closeErr
	case ResponseRetryable:
		fault := Fault{Kind: FaultRateRetryable, HTTPStatus: event.HTTPStatus, RetryAtMonotonic: decision.RetryAtMonotonic}
		if r.attempts >= r.contract.Rate.MaxAttempts {
			opportunity := r.restOpportunity(OpportunityOutcomeRateLimited)
			action := runnerAction{kind: ControlRateLimited, outcome: TerminalRateLimited, fault: &fault, opportunity: &opportunity, close: &closePlan{reason: CloseRateTerminal}}
			r.action = &action
		} else {
			action := runnerAction{kind: ControlRateLimited, outcome: TerminalRateLimited, fault: &fault}
			r.action = &action
		}
	case ResponseTerminal:
		fault := Fault{Kind: FaultRateTerminal, HTTPStatus: event.HTTPStatus}
		opportunity := r.restOpportunity(OpportunityOutcomeVenueUnavailable)
		action := runnerAction{kind: ControlRateLimited, outcome: TerminalRejected, fault: &fault, opportunity: &opportunity, close: &closePlan{reason: CloseRateTerminal, blind: r.blindInterval(CloseRateTerminal)}}
		r.action = &action
	case ResponseCircuitOpened:
		fault := Fault{Kind: FaultRateCircuit, HTTPStatus: event.HTTPStatus, RetryAtMonotonic: decision.RetryAtMonotonic}
		opportunity := r.restOpportunity(OpportunityOutcomeVenueUnavailable)
		action := runnerAction{kind: ControlRateLimited, outcome: TerminalRejected, fault: &fault, opportunity: &opportunity, close: &closePlan{reason: CloseRateCircuit, blind: r.blindInterval(CloseRateCircuit)}}
		r.action = &action
	default:
		return r.abortRateError(ctx, result, errors.New("unknown response disposition"))
	}
	return result, nil
}

func (r *Runner) runAction(ctx context.Context, result StepResult, action *runnerAction) (StepResult, error) {
	envelope, control, err := r.controlEnvelope(action)
	if err != nil {
		return result, err
	}
	if err := r.assigner.Assign(&envelope); err != nil {
		return result, err
	}
	control.Envelope = envelope
	if err := control.Validate(); err != nil {
		return result, err
	}
	err = r.sink.WriteRaw(ctx, cloneEnvelope(envelope))
	if errors.Is(err, ErrSinkFull) {
		r.pending = &pendingWrite{envelope: envelope, control: &control, action: action}
		r.state = RunnerBackpressured
		result.Blocked = true
		result.State = r.state
		result.Faults = append(result.Faults, Fault{Kind: FaultQueuePressure})
		if !r.contract.Topology.Throttleable && r.connected && action.close == nil {
			action.close = &closePlan{reason: CloseQueuePressure, blind: r.blindInterval(CloseQueuePressure)}
			_ = r.transport.Close(ctx, CloseQueuePressure)
		}
		return result, nil
	}
	if err != nil {
		r.pending = &pendingWrite{envelope: envelope, control: &control, action: action}
		return r.abortSinkFailure(ctx, result, err)
	}
	r.committed = envelope.ArrivalOrdinal
	result.Envelopes = append(result.Envelopes, cloneEnvelope(envelope))
	result.Controls = append(result.Controls, control)
	result.Progressed = true
	return r.completeAction(ctx, result, action)
}

func (r *Runner) completeAction(ctx context.Context, result StepResult, action *runnerAction) (StepResult, error) {
	r.state = RunnerRunning
	result.State = r.state
	if action.fault != nil {
		result.Faults = append(result.Faults, *action.fault)
	}
	if action.opportunity != nil {
		result.Opportunities = append(result.Opportunities, *action.opportunity)
	}
	switch action.after {
	case afterConnected:
		if err := r.armHeartbeatTimer(); err != nil {
			return result, err
		}
		if err := r.armUsefulDataTimer(); err != nil {
			return result, err
		}
		if r.contract.Subscription.ACKMode == ACKExact {
			r.needSubscribe = true
		}
	case afterSubscribed:
		if err := r.armACKTimer(); err != nil {
			return result, err
		}
	}
	if action.close != nil {
		err := r.closeEpoch(ctx, &result, *action.close)
		return result, err
	}
	return result, nil
}

func (r *Runner) flushPending(ctx context.Context, result StepResult) (StepResult, error) {
	pending := r.pending
	err := r.sink.WriteRaw(ctx, cloneEnvelope(pending.envelope))
	if errors.Is(err, ErrSinkFull) {
		result.Blocked = true
		result.State = RunnerBackpressured
		return result, nil
	}
	if err != nil {
		return r.abortSinkFailure(ctx, result, err)
	}
	r.pending = nil
	r.committed = pending.envelope.ArrivalOrdinal
	r.state = RunnerRunning
	if pending.action != nil {
		result.Envelopes = append(result.Envelopes, cloneEnvelope(pending.envelope))
		result.Controls = append(result.Controls, ControlRecord{Envelope: cloneEnvelope(pending.envelope)})
		result.Progressed = true
		return r.completeAction(ctx, result, pending.action)
	}
	return r.completeRaw(ctx, result, pending)
}

func (r *Runner) controlEnvelope(action *runnerAction) (EnvelopeV1, ControlRecord, error) {
	reading := r.clock.Read()
	envelope := r.baseEnvelope(reading, action.outcome)
	envelope.RecordKind = RecordKindControl
	envelope.PayloadEncoding = PayloadEncodingNone
	envelope.SetRawPayload(nil)
	envelope.ControlKind = OptionalControlKind{Value: action.kind, Valid: true}
	envelope.Extensions = append([]byte(nil), action.extensions...)
	if action.state != "" {
		envelope.HTTPStatusOrWSState = OptionalString{Value: action.state, Valid: true}
	}
	if action.kind == ControlSubscribeRequest {
		envelope.SubscriptionOrRequestID = OptionalString{Value: r.config.SubscriptionRequestID, Valid: true}
	}
	if action.kind == ControlPollScheduled {
		envelope.ScheduledAtNS = r.config.ScheduledAtNS
	}
	if action.kind == ControlRequestStarted {
		envelope.SubscriptionOrRequestID = OptionalString{Value: action.requestID, Valid: true}
		envelope.ScheduledAtNS = action.scheduledAt
		envelope.RequestStartedAtNS = action.requestStartedAt
	}
	return envelope, ControlRecord{Envelope: envelope}, nil
}

func (r *Runner) baseEnvelope(reading ClockReading, outcome TerminalOutcome) EnvelopeV1 {
	envelope := EnvelopeV1{
		EnvelopeVersion:            EnvelopeVersion,
		SourceID:                   r.contract.SourceID,
		ChannelOrEndpoint:          r.config.ChannelOrEndpoint,
		NativeSymbol:               r.config.NativeSymbol,
		ReceivedWallTimeNS:         reading.WallTimeNS,
		ClockEpochID:               reading.ClockEpochID,
		MonotonicNSSinceClockEpoch: reading.MonotonicNS,
		PayloadEncoding:            PayloadEncodingNone,
		TerminalOutcome:            outcome,
		RecorderVersion:            r.config.RecorderVersion,
	}
	if r.config.Epoch.Kind == EpochConnection {
		envelope.ConnectionEpoch = OptionalEpoch{Value: r.config.Epoch.ID, Valid: true}
	} else {
		envelope.PollCycleID = OptionalEpoch{Value: r.config.Epoch.ID, Valid: true}
	}
	return envelope
}

func (r *Runner) armACKTimer() error {
	reading := r.clock.Read()
	timer, err := r.clock.NewTimer(r.contract.Subscription.ACKTimeoutNS)
	if err != nil {
		return err
	}
	r.ackTimer = timer
	r.ackDeadlineWallNS = addWallSaturating(reading.WallTimeNS, r.contract.Subscription.ACKTimeoutNS)
	return nil
}

func (r *Runner) armHeartbeatTimer() error {
	if r.contract.Heartbeat.Mode == HeartbeatNone {
		return nil
	}
	reading := r.clock.Read()
	window := addSaturating(r.contract.Heartbeat.IntervalNS, r.contract.Heartbeat.TimeoutNS)
	timer, err := r.clock.NewTimer(window)
	if err != nil {
		return err
	}
	r.heartbeatTimer = timer
	r.heartbeatEarliestMonotonicNS = addSaturating(reading.MonotonicNS, r.contract.Heartbeat.MinimumIntervalNS)
	r.heartbeatDeadlineWallNS = addWallSaturating(reading.WallTimeNS, window)
	return nil
}

func (r *Runner) armUsefulDataTimer() error {
	if r.contract.UsefulData.MaxSilenceNS == 0 {
		return nil
	}
	reading := r.clock.Read()
	timer, err := r.clock.NewTimer(r.contract.UsefulData.MaxSilenceNS)
	if err != nil {
		return err
	}
	r.usefulDataTimer = timer
	r.usefulDataDeadlineWallNS = addWallSaturating(reading.WallTimeNS, r.contract.UsefulData.MaxSilenceNS)
	return nil
}

func (r *Runner) observeUsefulData() error {
	if r.usefulDataTimer == nil {
		return nil
	}
	r.usefulDataTimer.Stop()
	return r.armUsefulDataTimer()
}

func (r *Runner) ackOpportunity(outcome OpportunityOutcome) OpportunityRecord {
	reading := r.clock.Read()
	return OpportunityRecord{
		OpportunityID:           fmt.Sprintf("ack.%x", r.config.Epoch.ID),
		Expectation:             OpportunityAcknowledgementDeadline,
		SourceID:                r.contract.SourceID,
		ChannelOrEndpoint:       r.config.ChannelOrEndpoint,
		NativeSymbol:            r.config.NativeSymbol,
		ConnectionEpoch:         OptionalEpoch{Value: r.config.Epoch.ID, Valid: true},
		SubscriptionOrRequestID: OptionalString{Value: r.config.SubscriptionRequestID, Valid: true},
		DeadlineAtNS:            OptionalInt64{Value: r.ackDeadlineWallNS, Valid: true},
		TerminalOutcome:         outcome,
		TerminalAtNS:            OptionalInt64{Value: reading.WallTimeNS, Valid: true},
		RecorderVersion:         r.config.RecorderVersion,
	}
}

func (r *Runner) heartbeatOpportunity(outcome OpportunityOutcome) OpportunityRecord {
	reading := r.clock.Read()
	return OpportunityRecord{
		OpportunityID:     fmt.Sprintf("heartbeat.%x.%d", r.config.Epoch.ID, r.heartbeatCount),
		Expectation:       OpportunityHeartbeatDeadline,
		SourceID:          r.contract.SourceID,
		ChannelOrEndpoint: r.config.ChannelOrEndpoint,
		NativeSymbol:      r.config.NativeSymbol,
		ConnectionEpoch:   OptionalEpoch{Value: r.config.Epoch.ID, Valid: true},
		DeadlineAtNS:      OptionalInt64{Value: r.heartbeatDeadlineWallNS, Valid: true},
		TerminalOutcome:   outcome,
		TerminalAtNS:      OptionalInt64{Value: reading.WallTimeNS, Valid: true},
		RecorderVersion:   r.config.RecorderVersion,
	}
}

func (r *Runner) restOpportunity(outcome OpportunityOutcome) OpportunityRecord {
	reading := r.clock.Read()
	requestID := r.requestID
	if requestID == "" {
		requestID = "unsent"
	}
	return OpportunityRecord{
		OpportunityID:           fmt.Sprintf("poll.%x", r.config.Epoch.ID),
		Expectation:             OpportunityScheduledRESTPoll,
		SourceID:                r.contract.SourceID,
		ChannelOrEndpoint:       r.config.ChannelOrEndpoint,
		NativeSymbol:            r.config.NativeSymbol,
		PollCycleID:             OptionalEpoch{Value: r.config.Epoch.ID, Valid: true},
		SubscriptionOrRequestID: OptionalString{Value: requestID, Valid: true},
		ScheduledAtNS:           r.config.ScheduledAtNS,
		TerminalOutcome:         outcome,
		TerminalAtNS:            OptionalInt64{Value: reading.WallTimeNS, Valid: true},
		RecorderVersion:         r.config.RecorderVersion,
	}
}

func (r *Runner) closeEpoch(ctx context.Context, result *StepResult, plan closePlan) error {
	if r.state == RunnerClosed {
		return nil
	}
	if r.ackTimer != nil {
		r.ackTimer.Stop()
		r.ackTimer = nil
	}
	if r.heartbeatTimer != nil {
		r.heartbeatTimer.Stop()
		r.heartbeatTimer = nil
	}
	if r.usefulDataTimer != nil {
		r.usefulDataTimer.Stop()
		r.usefulDataTimer = nil
	}
	closeContext := ctx
	if ctx.Err() != nil {
		closeContext = context.WithoutCancel(ctx)
	}
	preCommit := EpochCommit{SourceID: r.contract.SourceID, Epoch: r.config.Epoch, LastArrivalOrdinal: r.committed}
	if err := r.sink.Commit(closeContext, preCommit); err != nil {
		result.Faults = append(result.Faults, Fault{Kind: FaultSinkFailure})
		r.state = RunnerClosed
		result.State = r.state
		_ = r.transport.Close(closeContext, plan.reason)
		return fmt.Errorf("capture: committing epoch before close: %w", err)
	}
	kind := ControlDisconnect
	if r.config.Epoch.Kind == EpochPollCycle {
		kind = ControlShutdown
	}
	outcome := TerminalDisconnected
	switch plan.reason {
	case ClosePlanned:
		outcome = TerminalObserved
	case CloseSinkFailure:
		outcome = TerminalFailed
	case CloseSchemaRejected:
		outcome = TerminalRejected
	}
	closeAction := runnerAction{kind: kind, outcome: outcome, state: closeReasonString(plan.reason)}
	envelope, control, err := r.controlEnvelope(&closeAction)
	if err != nil {
		return err
	}
	if err := r.assigner.Assign(&envelope); err != nil {
		return err
	}
	control.Envelope = envelope
	if err := control.Validate(); err != nil {
		return err
	}
	commit := EpochCommit{SourceID: r.contract.SourceID, Epoch: r.config.Epoch, LastArrivalOrdinal: envelope.ArrivalOrdinal}
	closeRecord := EpochClose{Commit: commit, Terminal: control, Reason: plan.reason, BlindInterval: cloneBlindInterval(plan.blind)}
	if r.pending != nil {
		unresolved := cloneEnvelope(r.pending.envelope)
		closeRecord.UnresolvedPending = &unresolved
	}
	if err := r.sink.CloseEpoch(closeContext, closeRecord); err != nil {
		result.Faults = append(result.Faults, Fault{Kind: FaultSinkFailure})
		r.state = RunnerClosed
		result.State = r.state
		_ = r.transport.Close(closeContext, plan.reason)
		return fmt.Errorf("capture: closing epoch: %w", err)
	}
	if err := r.assigner.Finalize(commit); err != nil {
		return err
	}
	if closeRecord.UnresolvedPending != nil {
		result.Unresolved = append(result.Unresolved, cloneEnvelope(*closeRecord.UnresolvedPending))
	}
	r.pending = nil
	clear(r.ackedSubscriptions)
	r.ackBatchCount = 0
	r.committed = envelope.ArrivalOrdinal
	r.state = RunnerClosed
	result.State = r.state
	result.Progressed = true
	result.Envelopes = append(result.Envelopes, cloneEnvelope(envelope))
	result.Controls = append(result.Controls, control)
	return r.transport.Close(closeContext, plan.reason)
}

func (r *Runner) flushPendingForCancellation(ctx context.Context, result *StepResult) error {
	pending := r.pending
	closeContext := context.WithoutCancel(ctx)
	err := r.sink.WriteRaw(closeContext, cloneEnvelope(pending.envelope))
	if errors.Is(err, ErrSinkFull) {
		result.Faults = append(result.Faults, Fault{Kind: FaultQueuePressure})
		blind := r.blindInterval(CloseCanceled)
		blind.QueuePressure = true
		closeErr := r.closeEpoch(closeContext, result, closePlan{reason: CloseCanceled, blind: blind})
		return closeErr
	}
	if err != nil {
		aborted, abortErr := r.abortSinkFailure(closeContext, *result, err)
		*result = aborted
		return abortErr
	}
	r.pending = nil
	r.committed = pending.envelope.ArrivalOrdinal
	r.state = RunnerRunning
	result.State = r.state
	result.Progressed = true
	result.Envelopes = append(result.Envelopes, cloneEnvelope(pending.envelope))
	if pending.control != nil {
		result.Controls = append(result.Controls, ControlRecord{Envelope: cloneEnvelope(pending.envelope)})
	}
	return nil
}

func (r *Runner) abortSinkFailure(ctx context.Context, result StepResult, err error) (StepResult, error) {
	result.Faults = append(result.Faults, Fault{Kind: FaultSinkFailure})
	if r.config.Epoch.Kind == EpochPollCycle {
		result.Opportunities = append(result.Opportunities, r.restOpportunity(OpportunityOutcomeCollectorFailed))
	}
	closeErr := r.closeEpoch(context.WithoutCancel(ctx), &result, closePlan{reason: CloseSinkFailure, blind: r.blindInterval(CloseSinkFailure)})
	return result, errors.Join(fmt.Errorf("capture: writing exact raw envelope: %w", err), closeErr)
}

func (r *Runner) abortObserverFailure(ctx context.Context, result StepResult, err error) (StepResult, error) {
	if r.config.Epoch.Kind == EpochPollCycle {
		result.Opportunities = append(result.Opportunities, r.restOpportunity(OpportunityOutcomeCollectorFailed))
	}
	closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseSinkFailure, blind: r.blindInterval(CloseSinkFailure)})
	return result, errors.Join(fmt.Errorf("%w: %v", ErrObserverContract, err), closeErr)
}

func (r *Runner) abortRateError(ctx context.Context, result StepResult, err error) (StepResult, error) {
	if r.config.Epoch.Kind == EpochPollCycle {
		result.Opportunities = append(result.Opportunities, r.restOpportunity(OpportunityOutcomeCollectorFailed))
	}
	closeErr := r.closeEpoch(ctx, &result, closePlan{reason: CloseRateTerminal, blind: r.blindInterval(CloseRateTerminal)})
	return result, errors.Join(err, closeErr)
}

func (r *Runner) blindInterval(reason CloseReason) *BlindInterval {
	reading := r.clock.Read()
	return &BlindInterval{StartedWallTimeNS: reading.WallTimeNS, DetectedWallTimeNS: reading.WallTimeNS, Reason: reason}
}

func validateObservation(eventKind TransportEventKind, observation Observation) error {
	if observation.Role < MessageData || observation.Role > MessageUnknown {
		return ErrObserverContract
	}
	if observation.Schema < SchemaAccepted || observation.Schema > SchemaUnknownRole {
		return ErrObserverContract
	}
	switch eventKind {
	case TransportEventAcknowledgement:
		if observation.Role != MessageAcknowledgement {
			return ErrObserverContract
		}
	case TransportEventHeartbeat:
		if observation.Role != MessageHeartbeat {
			return ErrObserverContract
		}
	case TransportEventApplication:
		if observation.Role != MessageData && observation.Role != MessageUnknown {
			return ErrObserverContract
		}
	case TransportEventHTTPResponse:
		if observation.Role != MessageData && observation.Role != MessageUnknown {
			return ErrObserverContract
		}
	}
	return nil
}

func (r *Runner) validateObservationBounds(observation Observation) error {
	if observation.Role != MessageAcknowledgement {
		if observation.ACK.RequestID != "" || len(observation.ACK.Subscriptions) != 0 ||
			observation.ACK.Accepted || observation.ACK.FinalBatch {
			return ErrObserverContract
		}
		return nil
	}
	if err := validateContractText("ack.request_id", observation.ACK.RequestID, MaxIdentityBytes); err != nil {
		return err
	}
	if len(observation.ACK.Subscriptions) > MaxExpectedSubscriptions {
		return fmt.Errorf("%w: ACK batch exceeds the observer bound", ErrObserverContract)
	}
	for i, subscription := range observation.ACK.Subscriptions {
		if err := validateContractText(fmt.Sprintf("ack.subscriptions[%d]", i), subscription, MaxIdentityBytes); err != nil {
			return err
		}
	}
	return nil
}

func exactInventory(got, want []string) (match bool, duplicate bool) {
	gotCopy := slices.Clone(got)
	wantCopy := slices.Clone(want)
	slices.Sort(gotCopy)
	slices.Sort(wantCopy)
	for i := 1; i < len(gotCopy); i++ {
		if gotCopy[i] == gotCopy[i-1] {
			duplicate = true
		}
	}
	return slices.Equal(gotCopy, wantCopy), duplicate
}

func cloneEnvelope(envelope EnvelopeV1) EnvelopeV1 {
	envelope.RawPayload = append([]byte(nil), envelope.RawPayload...)
	envelope.Extensions = append([]byte(nil), envelope.Extensions...)
	return envelope
}

func pointerToEvent(event TransportEvent) *TransportEvent {
	cloned := event.clone()
	return &cloned
}

func cloneBlindInterval(interval *BlindInterval) *BlindInterval {
	if interval == nil {
		return nil
	}
	cloned := *interval
	return &cloned
}

func addWallSaturating(wall int64, delta uint64) int64 {
	if delta > math.MaxInt64 || wall > math.MaxInt64-int64(delta) {
		return math.MaxInt64
	}
	return wall + int64(delta)
}

func closeReasonString(reason CloseReason) string {
	values := []string{
		"",
		"planned",
		"abrupt",
		"ack_rejected",
		"heartbeat_missed",
		"rate_terminal",
		"rate_circuit",
		"queue_pressure",
		"sink_failure",
		"canceled",
		"transport_failure",
		"schema_rejected",
		"ack_timeout",
		"heartbeat_cadence",
		"useful_data_missed",
	}
	if int(reason) < len(values) {
		return values[reason]
	}
	return fmt.Sprintf("close_reason_%d", reason)
}

func safeDetail(value string) string {
	value = strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || r == 0 {
			return -1
		}
		return r
	}, value)
	if len(value) > MaxIdentityBytes {
		return value[:MaxIdentityBytes]
	}
	return value
}
