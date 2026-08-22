package quality

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	MaxFaultScriptEvents = 256
	MaxHarnessEvidence   = 2048
)

var (
	ErrHarnessBound     = errors.New("quality: fault harness bound exceeded")
	ErrInjectedSink     = errors.New("quality: injected sink failure")
	ErrInjectedObserver = errors.New("quality: injected observer failure")
)

type Trace struct {
	mu      sync.Mutex
	maximum int
	events  []string
}

func NewTrace(maximum int) (*Trace, error) {
	if maximum <= 0 || maximum > MaxHarnessEvidence {
		return nil, fmt.Errorf("%w: trace maximum must be within 1..%d", ErrHarnessBound, MaxHarnessEvidence)
	}
	return &Trace{maximum: maximum}, nil
}

func (t *Trace) add(event string) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.events) == t.maximum {
		return ErrHarnessBound
	}
	t.events = append(t.events, event)
	return nil
}

func (t *Trace) Events() []string {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.events)
}

type SinkOperationKind uint8

const (
	SinkWrite SinkOperationKind = iota + 1
	SinkCommit
	SinkClose
)

type SinkOperation struct {
	Kind     SinkOperationKind
	Envelope capture.EnvelopeV1
	Commit   capture.EpochCommit
	Close    capture.EpochClose
}

// MemorySink is a bounded durable-queue fake. Commit and CloseEpoch use a
// reserved path and remain available when the ordinary write queue is full.
type MemorySink struct {
	mu               sync.Mutex
	queueCapacity    int
	evidenceCapacity int
	queue            []capture.EnvelopeV1
	operations       []SinkOperation
	trace            *Trace
	closed           bool
	failWrite        error
	failCommit       error
	failClose        error
}

func NewMemorySink(queueCapacity, evidenceCapacity int, trace *Trace) (*MemorySink, error) {
	if queueCapacity <= 0 || evidenceCapacity < queueCapacity+2 || evidenceCapacity > MaxHarnessEvidence {
		return nil, fmt.Errorf("%w: invalid sink queue/evidence capacity", ErrHarnessBound)
	}
	return &MemorySink{queueCapacity: queueCapacity, evidenceCapacity: evidenceCapacity, trace: trace}, nil
}

func (s *MemorySink) WriteRaw(ctx context.Context, envelope capture.EnvelopeV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := envelope.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWrite != nil {
		err := s.failWrite
		s.failWrite = nil
		return err
	}
	if s.closed {
		return capture.ErrSinkClosed
	}
	if len(s.queue) == s.queueCapacity {
		return capture.ErrSinkFull
	}
	if len(s.operations) == s.evidenceCapacity {
		return ErrHarnessBound
	}
	cloned := cloneEnvelope(envelope)
	s.queue = append(s.queue, cloned)
	s.operations = append(s.operations, SinkOperation{Kind: SinkWrite, Envelope: cloned})
	return s.trace.add(fmt.Sprintf("write:%d", envelope.ArrivalOrdinal))
}

func (s *MemorySink) Commit(ctx context.Context, commit capture.EpochCommit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCommit != nil {
		err := s.failCommit
		s.failCommit = nil
		return err
	}
	if s.closed {
		return capture.ErrSinkClosed
	}
	if len(s.operations) == s.evidenceCapacity {
		return ErrHarnessBound
	}
	s.operations = append(s.operations, SinkOperation{Kind: SinkCommit, Commit: commit})
	return s.trace.add(fmt.Sprintf("commit:%d", commit.LastArrivalOrdinal))
}

func (s *MemorySink) CloseEpoch(ctx context.Context, closeRecord capture.EpochClose) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := closeRecord.Terminal.Validate(); err != nil {
		return err
	}
	if closeRecord.Commit.SourceID != closeRecord.Terminal.Envelope.SourceID ||
		closeRecord.Commit.LastArrivalOrdinal != closeRecord.Terminal.Envelope.ArrivalOrdinal {
		return errors.New("quality: close commit does not cover terminal control")
	}
	if closeRecord.UnresolvedPending != nil {
		if err := closeRecord.UnresolvedPending.Validate(); err != nil {
			return err
		}
		if closeRecord.UnresolvedPending.SourceID != closeRecord.Terminal.Envelope.SourceID ||
			closeRecord.UnresolvedPending.ArrivalOrdinal+1 != closeRecord.Terminal.Envelope.ArrivalOrdinal {
			return errors.New("quality: unresolved pending evidence is not immediately before terminal control")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failClose != nil {
		err := s.failClose
		s.failClose = nil
		return err
	}
	if s.closed {
		return capture.ErrSinkClosed
	}
	if len(s.operations) == s.evidenceCapacity {
		return ErrHarnessBound
	}
	cloned := cloneClose(closeRecord)
	s.operations = append(s.operations, SinkOperation{Kind: SinkClose, Close: cloned})
	s.closed = true
	if closeRecord.UnresolvedPending != nil {
		if err := s.trace.add(fmt.Sprintf("unresolved:%d", closeRecord.UnresolvedPending.ArrivalOrdinal)); err != nil {
			return err
		}
	}
	return s.trace.add(fmt.Sprintf("close:%d", closeRecord.Commit.LastArrivalOrdinal))
}

func (s *MemorySink) Drain(maximum int) []capture.EnvelopeV1 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maximum < 0 || maximum > len(s.queue) {
		maximum = len(s.queue)
	}
	drained := make([]capture.EnvelopeV1, maximum)
	for i := range maximum {
		drained[i] = cloneEnvelope(s.queue[i])
	}
	copy(s.queue, s.queue[maximum:])
	clear(s.queue[len(s.queue)-maximum:])
	s.queue = s.queue[:len(s.queue)-maximum]
	return drained
}

func (s *MemorySink) QueueDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *MemorySink) Operations() []SinkOperation {
	s.mu.Lock()
	defer s.mu.Unlock()
	operations := make([]SinkOperation, len(s.operations))
	for i, operation := range s.operations {
		operations[i] = operation
		operations[i].Envelope = cloneEnvelope(operation.Envelope)
		operations[i].Close = cloneClose(operation.Close)
	}
	return operations
}

func (s *MemorySink) FailNextWrite(err error) {
	if err == nil {
		err = ErrInjectedSink
	}
	s.mu.Lock()
	s.failWrite = err
	s.mu.Unlock()
}

func (s *MemorySink) FailNextCommit(err error) {
	if err == nil {
		err = ErrInjectedSink
	}
	s.mu.Lock()
	s.failCommit = err
	s.mu.Unlock()
}

func (s *MemorySink) FailNextClose(err error) {
	if err == nil {
		err = ErrInjectedSink
	}
	s.mu.Lock()
	s.failClose = err
	s.mu.Unlock()
}

type ObserverStep struct {
	Observation capture.Observation
	Err         error
}

type ObserverCall struct {
	ArrivalOrdinal uint64
	RawSHA256      [32]byte
}

type ScriptedObserver struct {
	mu      sync.Mutex
	steps   []ObserverStep
	next    int
	calls   []ObserverCall
	maximum int
	trace   *Trace
}

func NewScriptedObserver(steps []ObserverStep, maximum int, trace *Trace) (*ScriptedObserver, error) {
	if len(steps) > MaxFaultScriptEvents || maximum < len(steps) || maximum > MaxHarnessEvidence {
		return nil, fmt.Errorf("%w: invalid observer script/call capacity", ErrHarnessBound)
	}
	return &ScriptedObserver{steps: slices.Clone(steps), maximum: maximum, trace: trace}, nil
}

func (o *ScriptedObserver) Observe(ctx context.Context, envelope capture.EnvelopeV1) (capture.Observation, error) {
	if err := ctx.Err(); err != nil {
		return capture.Observation{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.next == len(o.steps) || len(o.calls) == o.maximum {
		return capture.Observation{}, ErrHarnessBound
	}
	if err := o.trace.add(fmt.Sprintf("observe:%d", envelope.ArrivalOrdinal)); err != nil {
		return capture.Observation{}, err
	}
	o.calls = append(o.calls, ObserverCall{ArrivalOrdinal: envelope.ArrivalOrdinal, RawSHA256: envelope.RawPayloadSHA256})
	step := o.steps[o.next]
	o.next++
	return step.Observation, step.Err
}

func (o *ScriptedObserver) Calls() []ObserverCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.calls)
}

type FaultScript struct {
	Name         string
	Events       []capture.TransportEvent
	Observations []ObserverStep
}

func ACKFaultScripts() []FaultScript {
	accepted := capture.Observation{Role: capture.MessageAcknowledgement, Schema: capture.SchemaAccepted, ACK: capture.ACKObservation{RequestID: "sub-1", Subscriptions: []string{"trades", "ticker"}, Accepted: true, FinalBatch: true}}
	partial := accepted
	partial.ACK.Subscriptions = []string{"trades"}
	wrong := accepted
	wrong.ACK.RequestID = "sub-wrong"
	rejected := accepted
	rejected.ACK.Accepted = false
	return []FaultScript{
		{Name: "success", Events: []capture.TransportEvent{rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-1","ok":true}`))}, Observations: []ObserverStep{{Observation: accepted}}},
		{Name: "partial", Events: []capture.TransportEvent{rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-1","streams":["trades"]}`))}, Observations: []ObserverStep{{Observation: partial}}},
		{Name: "wrong", Events: []capture.TransportEvent{rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-wrong","ok":true}`))}, Observations: []ObserverStep{{Observation: wrong}}},
		{Name: "rejected", Events: []capture.TransportEvent{rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-1","ok":false}`))}, Observations: []ObserverStep{{Observation: rejected}}},
		{Name: "duplicate", Events: []capture.TransportEvent{rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-1","ok":true}`)), rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-1","ok":true}`))}, Observations: []ObserverStep{{Observation: accepted}, {Observation: accepted}}},
	}
}

func HeartbeatFaultScripts() []FaultScript {
	heartbeat := capture.Observation{Role: capture.MessageHeartbeat, Schema: capture.SchemaAccepted, Unchanged: true}
	return []FaultScript{
		{Name: "heartbeat_only", Events: []capture.TransportEvent{rawEvent(capture.TransportEventHeartbeat, []byte(`{"pong":1}`)), rawEvent(capture.TransportEventHeartbeat, []byte(`{"pong":2}`))}, Observations: []ObserverStep{{Observation: heartbeat}, {Observation: heartbeat}}},
		{Name: "missed", Events: nil, Observations: nil},
	}
}

func DisconnectFaultScripts() []FaultScript {
	return []FaultScript{
		{Name: "planned", Events: []capture.TransportEvent{{Kind: capture.TransportEventDisconnected, Planned: true}}},
		{Name: "abrupt", Events: []capture.TransportEvent{{Kind: capture.TransportEventDisconnected}}},
	}
}

func SchemaFaultScripts() []FaultScript {
	cases := []struct {
		name        string
		bytes       []byte
		disposition capture.SchemaDisposition
	}{
		{"malformed", []byte(`{"price":`), capture.SchemaMalformed},
		{"additive", []byte(`{"price":"1","new_field":true}`), capture.SchemaAdditive},
		{"type_changed", []byte(`{"price":1}`), capture.SchemaSemanticChanged},
		{"unknown_role", []byte(`{"mystery":true}`), capture.SchemaUnknownRole},
	}
	scripts := make([]FaultScript, len(cases))
	for i, test := range cases {
		scripts[i] = FaultScript{
			Name:         test.name,
			Events:       []capture.TransportEvent{rawEvent(capture.TransportEventApplication, test.bytes)},
			Observations: []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: test.disposition}}},
		}
		if test.disposition == capture.SchemaUnknownRole {
			scripts[i].Observations[0].Observation.Role = capture.MessageUnknown
		}
	}
	return scripts
}

func RateFaultStatuses() []int { return []int{403, 418, 429, 500, 502, 503} }

func AssertRawBeforeObserve(trace []string) error {
	written := make(map[string]struct{})
	for _, event := range trace {
		if ordinal, ok := cutTrace(event, "write:"); ok {
			written[ordinal] = struct{}{}
			continue
		}
		if ordinal, ok := cutTrace(event, "observe:"); ok {
			if _, found := written[ordinal]; !found {
				return fmt.Errorf("quality: observer ran before durable write for ordinal %s", ordinal)
			}
		}
	}
	return nil
}

func AssertOperationOrder(operations []SinkOperation) error {
	var last uint64
	closed := false
	for i, operation := range operations {
		switch operation.Kind {
		case SinkWrite:
			if closed || operation.Envelope.ArrivalOrdinal != last+1 {
				return fmt.Errorf("quality: operation %d has unordered write ordinal %d after %d", i, operation.Envelope.ArrivalOrdinal, last)
			}
			last = operation.Envelope.ArrivalOrdinal
		case SinkCommit:
			if closed || operation.Commit.LastArrivalOrdinal != last {
				return fmt.Errorf("quality: operation %d commits %d after %d", i, operation.Commit.LastArrivalOrdinal, last)
			}
		case SinkClose:
			increment := uint64(1)
			if operation.Close.UnresolvedPending != nil {
				if operation.Close.UnresolvedPending.ArrivalOrdinal != last+1 {
					return fmt.Errorf("quality: operation %d unresolved ordinal %d after %d", i, operation.Close.UnresolvedPending.ArrivalOrdinal, last)
				}
				increment++
			}
			if closed || operation.Close.Commit.LastArrivalOrdinal != last+increment {
				return fmt.Errorf("quality: operation %d closes at %d after %d", i, operation.Close.Commit.LastArrivalOrdinal, last)
			}
			last += increment
			closed = true
		default:
			return fmt.Errorf("quality: operation %d has invalid kind", i)
		}
	}
	return nil
}

func AssertExactRaw(events []capture.TransportEvent, operations []SinkOperation) error {
	var want [][]byte
	for _, event := range events {
		if len(event.Raw) != 0 {
			want = append(want, event.Raw)
		}
	}
	var got [][]byte
	for _, operation := range operations {
		if operation.Kind == SinkWrite && len(operation.Envelope.RawPayload) != 0 {
			got = append(got, operation.Envelope.RawPayload)
		}
		if operation.Kind == SinkClose && operation.Close.UnresolvedPending != nil && len(operation.Close.UnresolvedPending.RawPayload) != 0 {
			got = append(got, operation.Close.UnresolvedPending.RawPayload)
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("quality: durable raw count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			return fmt.Errorf("quality: durable raw %d changed", i)
		}
	}
	return nil
}

func rawEvent(kind capture.TransportEventKind, raw []byte) capture.TransportEvent {
	return capture.TransportEvent{Kind: kind, Raw: append([]byte(nil), raw...), Encoding: capture.PayloadEncodingJSON}
}

func cutTrace(value, prefix string) (string, bool) {
	if len(value) <= len(prefix) || value[:len(prefix)] != prefix {
		return "", false
	}
	return value[len(prefix):], true
}

func cloneEnvelope(envelope capture.EnvelopeV1) capture.EnvelopeV1 {
	envelope.RawPayload = append([]byte(nil), envelope.RawPayload...)
	envelope.Extensions = append([]byte(nil), envelope.Extensions...)
	return envelope
}

func cloneClose(closeRecord capture.EpochClose) capture.EpochClose {
	closeRecord.Terminal.Envelope = cloneEnvelope(closeRecord.Terminal.Envelope)
	if closeRecord.BlindInterval != nil {
		interval := *closeRecord.BlindInterval
		closeRecord.BlindInterval = &interval
	}
	if closeRecord.UnresolvedPending != nil {
		unresolved := cloneEnvelope(*closeRecord.UnresolvedPending)
		closeRecord.UnresolvedPending = &unresolved
	}
	return closeRecord
}
