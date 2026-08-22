package capture

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

const MaxScriptedTransportEvents = 1024

var (
	ErrTransportScriptExhausted = errors.New("capture: scripted transport exhausted")
	ErrTransportClosed          = errors.New("capture: transport closed")
	ErrSinkFull                 = errors.New("capture: durable sink queue full")
	ErrSinkClosed               = errors.New("capture: durable sink epoch closed")
)

type TransportEventKind uint8

const (
	TransportEventConnected TransportEventKind = iota + 1
	TransportEventApplication
	TransportEventAcknowledgement
	TransportEventHeartbeat
	TransportEventRequest
	TransportEventHTTPResponse
	TransportEventDisconnected
	TransportEventFailure
)

type TransportFailureKind uint8

const (
	TransportFailureDNS TransportFailureKind = iota + 1
	TransportFailureTLS
	TransportFailureConnect
	TransportFailureRequest
	TransportFailureClockRegression
	TransportFailureResponseEvidence
)

// TransportEvent carries exact application or response-body bytes where Raw is
// present. HTTP metadata is transport evidence, never reconstructed from Raw.
type TransportEvent struct {
	Kind                TransportEventKind
	Raw                 []byte
	Encoding            PayloadEncoding
	HTTPStatus          int
	RetryAfterNS        uint64
	RequestID           string
	Method              RESTMethod
	SanitizedParameters []SanitizedParameter
	RequestHeaders      []RESTHeader
	ResponseHeaders     []RESTHeader
	WSState             string
	Planned             bool
	Failure             TransportFailureKind
	AfterRawFailure     TransportFailureKind
}

func (e TransportEvent) clone() TransportEvent {
	e.Raw = append([]byte(nil), e.Raw...)
	e.SanitizedParameters = slices.Clone(e.SanitizedParameters)
	e.RequestHeaders = slices.Clone(e.RequestHeaders)
	e.ResponseHeaders = slices.Clone(e.ResponseHeaders)
	return e
}

type Transport interface {
	Next(context.Context) (TransportEvent, error)
	Close(context.Context, CloseReason) error
}

// PlannedDrainTransport exposes only complete transport messages already
// buffered at a planned close boundary. It must never wait for new source I/O.
type PlannedDrainTransport interface {
	NextBuffered(context.Context) (TransportEvent, bool, error)
}

// ScriptedTransport is a bounded, manually stepped fake. Next consumes exactly
// one event; it has no goroutines, network, sleeps, ambient time, or randomness.
type ScriptedTransport struct {
	mu          sync.Mutex
	events      []TransportEvent
	next        int
	closed      bool
	closeReason CloseReason
}

func NewScriptedTransport(events []TransportEvent) (*ScriptedTransport, error) {
	if len(events) > MaxScriptedTransportEvents {
		return nil, fmt.Errorf("capture: transport script has %d events, maximum is %d", len(events), MaxScriptedTransportEvents)
	}
	cloned := make([]TransportEvent, len(events))
	for i, event := range events {
		if err := validateTransportEvent(event); err != nil {
			return nil, fmt.Errorf("capture: transport event %d: %w", i, err)
		}
		cloned[i] = event.clone()
	}
	return &ScriptedTransport{events: cloned}, nil
}

func (t *ScriptedTransport) Next(ctx context.Context) (TransportEvent, error) {
	if err := ctx.Err(); err != nil {
		return TransportEvent{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return TransportEvent{}, ErrTransportClosed
	}
	if t.next == len(t.events) {
		return TransportEvent{}, ErrTransportScriptExhausted
	}
	event := t.events[t.next].clone()
	t.next++
	return event, nil
}

// Append adds deterministic future input without exceeding the script bound.
func (t *ScriptedTransport) Append(event TransportEvent) error {
	if err := validateTransportEvent(event); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrTransportClosed
	}
	if len(t.events) == MaxScriptedTransportEvents {
		return fmt.Errorf("capture: transport script reached %d events", MaxScriptedTransportEvents)
	}
	t.events = append(t.events, event.clone())
	return nil
}

func (t *ScriptedTransport) Close(ctx context.Context, reason CloseReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		t.closeReason = reason
	}
	return nil
}

func (t *ScriptedTransport) Remaining() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.events) - t.next
}

func (t *ScriptedTransport) Closed() (bool, CloseReason) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed, t.closeReason
}

func validateTransportEvent(event TransportEvent) error {
	if event.Kind < TransportEventConnected || event.Kind > TransportEventFailure {
		return errors.New("invalid event kind")
	}
	if len(event.Raw) > MaxPayloadBytes {
		return fmt.Errorf("raw payload has %d bytes, maximum is %d", len(event.Raw), MaxPayloadBytes)
	}
	hasRaw := event.Kind == TransportEventApplication ||
		event.Kind == TransportEventAcknowledgement ||
		event.Kind == TransportEventHeartbeat ||
		event.Kind == TransportEventHTTPResponse
	if !hasRaw && len(event.Raw) != 0 {
		return errors.New("only application, acknowledgement, heartbeat, and HTTP response events may carry raw bytes")
	}
	if hasRaw && (event.Encoding < PayloadEncodingJSON || event.Encoding > PayloadEncodingText) {
		return errors.New("raw event requires a payload encoding")
	}
	if !hasRaw && event.Encoding != 0 {
		return errors.New("events without raw bytes must use the zero payload encoding")
	}
	if err := validateOptionalString("request_id", OptionalString{Value: event.RequestID, Valid: event.RequestID != ""}, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateOptionalString("ws_state", OptionalString{Value: event.WSState, Valid: event.WSState != ""}, MaxIdentityBytes); err != nil {
		return err
	}
	if (event.Kind == TransportEventRequest || event.Kind == TransportEventHTTPResponse) && event.RequestID == "" {
		return errors.New("request and HTTP response events require a request ID")
	}
	if event.Kind == TransportEventRequest {
		evidence := RESTRequestEvidenceV1{
			Version:       RESTEvidenceVersion,
			Kind:          "request",
			RequestID:     event.RequestID,
			Method:        event.Method,
			Parameters:    event.SanitizedParameters,
			Headers:       event.RequestHeaders,
			ScheduledAtNS: 1,
			StartedAtNS:   1,
		}
		if err := evidence.Validate(); err != nil {
			return err
		}
	} else if event.Method != "" || len(event.SanitizedParameters) != 0 || len(event.RequestHeaders) != 0 {
		return errors.New("REST method, parameters, and request headers are valid only for a request event")
	}
	if event.Kind == TransportEventHTTPResponse {
		if event.AfterRawFailure == TransportFailureResponseEvidence && event.HTTPStatus == 0 {
			if event.RetryAfterNS != 0 || len(event.ResponseHeaders) != 0 {
				return errors.New("invalid response evidence without a valid status must omit interpreted HTTP metadata")
			}
		} else {
			evidence := RESTResponseEvidenceV1{
				Version:       RESTEvidenceVersion,
				Kind:          "response",
				RequestID:     event.RequestID,
				CompletedAtNS: 1,
				Status:        event.HTTPStatus,
				RetryAfterNS:  event.RetryAfterNS,
				Headers:       event.ResponseHeaders,
			}
			if err := evidence.Validate(); err != nil {
				return err
			}
		}
	} else if event.HTTPStatus != 0 || event.RetryAfterNS != 0 || len(event.ResponseHeaders) != 0 {
		return errors.New("HTTP metadata is valid only for an HTTP response")
	}
	if event.Kind != TransportEventFailure && event.Failure != 0 {
		return errors.New("failure kind is valid only for a failure event")
	}
	if event.Kind == TransportEventFailure && (event.Failure < TransportFailureDNS || event.Failure > TransportFailureRequest) {
		return errors.New("failure event requires a synchronous failure kind")
	}
	if event.AfterRawFailure != 0 {
		if !hasRaw {
			return errors.New("post-persistence failure requires a raw event")
		}
		switch event.AfterRawFailure {
		case TransportFailureClockRegression:
			if event.Kind == TransportEventHTTPResponse {
				return errors.New("clock regression post-failure is valid only for WebSocket raw events")
			}
		case TransportFailureResponseEvidence:
			if event.Kind != TransportEventHTTPResponse {
				return errors.New("response-evidence post-failure requires an HTTP response")
			}
		default:
			return errors.New("raw event has an invalid post-persistence failure kind")
		}
	}
	if event.Kind != TransportEventDisconnected && event.Planned {
		return errors.New("planned flag is valid only for a disconnect event")
	}
	return nil
}

type MessageRole uint8

const (
	MessageData MessageRole = iota + 1
	MessageAcknowledgement
	MessageHeartbeat
	MessageUnknown
)

type SchemaDisposition uint8

const (
	SchemaAccepted SchemaDisposition = iota + 1
	SchemaAdditive
	SchemaNonSemanticTypeChanged
	SchemaSemanticChanged
	SchemaMalformed
	SchemaUnknownRole
)

type ACKObservation struct {
	RequestID     string
	Subscriptions []string
	Accepted      bool
	FinalBatch    bool
}

type Observation struct {
	Role      MessageRole
	Schema    SchemaDisposition
	ACK       ACKObservation
	Unchanged bool
}

type RawObserver interface {
	Observe(context.Context, EnvelopeV1) (Observation, error)
}

type CloseReason uint8

const (
	ClosePlanned CloseReason = iota + 1
	CloseAbrupt
	CloseACKRejected
	CloseHeartbeatMissed
	CloseRateTerminal
	CloseRateCircuit
	CloseQueuePressure
	CloseSinkFailure
	CloseCanceled
	CloseTransportFailure
	CloseSchemaRejected
	CloseACKTimeout
	CloseHeartbeatCadence
	CloseUsefulDataMissed
)

type BlindInterval struct {
	StartedWallTimeNS  int64
	DetectedWallTimeNS int64
	Reason             CloseReason
	QueuePressure      bool
}

type EpochClose struct {
	Commit            EpochCommit
	Terminal          ControlRecord
	Reason            CloseReason
	BlindInterval     *BlindInterval
	UnresolvedPending *EnvelopeV1
}

// RawSink is defined at capture's consuming boundary. WriteRaw must durably
// accept an exact envelope before returning nil. Commit and CloseEpoch are
// explicit reserved control paths and must not depend on ordinary queue space.
type RawSink interface {
	WriteRaw(context.Context, EnvelopeV1) error
	Commit(context.Context, EpochCommit) error
	CloseEpoch(context.Context, EpochClose) error
}
