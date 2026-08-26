package capture

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	ErrWriterPressure       = errors.New("capture: writer pressure")
	ErrPressureQueueFull    = errors.New("capture: pressure queue is full")
	ErrPressureIncomplete   = errors.New("capture: connection is incomplete")
	ErrPressureNotRecovered = errors.New("capture: durable capacity has not recovered")
)

type PressureTransport string

const (
	PressureTransportREST      PressureTransport = "rest"
	PressureTransportWebSocket PressureTransport = "websocket"
)

type PressureState string

const (
	PressureRunning       PressureState = "running"
	PressureRESTThrottled PressureState = "rest_throttled"
	PressureDraining      PressureState = "draining"
	PressureBlindGap      PressureState = "blind_gap"
	PressureFaulted       PressureState = "faulted"
)

type PressureAction string

const (
	PressureStopREST       PressureAction = "stop_rest_not_started"
	PressureRecordOutcomes PressureAction = "record_rest_backpressure_outcomes"
	PressureCloseWebSocket PressureAction = "close_websocket_before_capacity"
	PressureFlushCommit    PressureAction = "flush_commit_last_complete"
	PressureOpenBlindGap   PressureAction = "open_blind_gap"
	PressureAwaitCapacity  PressureAction = "await_low_water"
	PressureResumeREST     PressureAction = "resume_rest"
	PressureReconnect      PressureAction = "reconnect_websocket"
)

type PressureOutcome string

const (
	PressureOutcomeBackpressureDelayed PressureOutcome = "backpressure_delayed"
	PressureOutcomeRateLimited         PressureOutcome = "rate_limited"
)

type WriterPressureConfig struct {
	Transport            PressureTransport
	DecodeQueueCapacity  int
	DurableQueueCapacity int
	DecodeHighWater      int
	DurableHighWater     int
	DecodeLowWater       int
	DurableLowWater      int
	MaxRawMessageBytes   int
	PendingRESTCapacity  int
}

type RawMessage struct {
	Stream        string
	Coordinate    string
	Payload       []byte
	FrameComplete bool
}

type DurableCommit struct {
	SegmentID      string
	LastCoordinate string
}

type DurableBoundary interface {
	WriteRaw(context.Context, RawMessage) error
	FlushCommit(context.Context) (DurableCommit, error)
}

type BlindGap struct {
	DetectedWallTimeNS int64
	LastCommit         DurableCommit
	Reason             string
}

type PressureHooks struct {
	RecordRESTOutcome func(context.Context, string, PressureOutcome) error
	CloseWebSocket    func(context.Context) error
	OpenBlindGap      func(context.Context, BlindGap) error
	Reconnect         func(context.Context) error
	ResumeREST        func(context.Context) error
}

type WriterPressureSnapshot struct {
	State        PressureState
	Complete     bool
	DecodeDepth  int
	DurableDepth int
	PendingREST  int
	LastCommit   DurableCommit
	LastActions  []PressureAction
}

// WriterPressure owns two bounded FIFO queues and the §10.6 state machine for
// one source connection. It never accepts a partial frame as complete, never
// coalesces canonical raw messages, and removes a message only after WriteRaw
// succeeds at the injected durable boundary.
type WriterPressure struct {
	mu           sync.Mutex
	config       WriterPressureConfig
	clock        Clock
	durable      DurableBoundary
	hooks        PressureHooks
	state        PressureState
	complete     bool
	decode       []RawMessage
	durableQueue []RawMessage
	pendingREST  []string
	lastCommit   DurableCommit
	lastActions  []PressureAction
}

func NewWriterPressure(config WriterPressureConfig, clock Clock, durable DurableBoundary, hooks PressureHooks) (*WriterPressure, error) {
	if err := validateWriterPressureConfig(config); err != nil {
		return nil, err
	}
	if clock == nil || durable == nil || hooks.RecordRESTOutcome == nil {
		return nil, errors.New("capture: writer pressure clock, durable boundary, and REST outcome recorder are required")
	}
	if config.Transport == PressureTransportWebSocket && (hooks.CloseWebSocket == nil || hooks.OpenBlindGap == nil || hooks.Reconnect == nil) {
		return nil, errors.New("capture: WebSocket pressure hooks are incomplete")
	}
	if config.Transport == PressureTransportREST && hooks.ResumeREST == nil {
		return nil, errors.New("capture: REST resume hook is required")
	}
	return &WriterPressure{
		config: config, clock: clock, durable: durable, hooks: hooks, state: PressureRunning, complete: true,
		decode:       make([]RawMessage, 0, config.DecodeQueueCapacity),
		durableQueue: make([]RawMessage, 0, config.DurableQueueCapacity),
		pendingREST:  make([]string, 0, config.PendingRESTCapacity), lastActions: make([]PressureAction, 0, 8),
	}, nil
}

func (w *WriterPressure) ScheduleREST(opportunityID string) error {
	if w == nil || !validPressureText(opportunityID) {
		return errors.New("capture: invalid REST opportunity identity")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != PressureRunning {
		return ErrWriterPressure
	}
	if len(w.pendingREST) >= w.config.PendingRESTCapacity {
		return ErrPressureQueueFull
	}
	if slices.Contains(w.pendingREST, opportunityID) {
		return errors.New("capture: duplicate pending REST opportunity")
	}
	w.pendingREST = append(w.pendingREST, opportunityID)
	return nil
}

func (w *WriterPressure) StartREST(opportunityID string) error {
	if w == nil {
		return ErrWriterPressure
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != PressureRunning {
		return ErrWriterPressure
	}
	index := slices.Index(w.pendingREST, opportunityID)
	if index < 0 {
		return errors.New("capture: REST opportunity is not pending")
	}
	w.pendingREST = slices.Delete(w.pendingREST, index, index+1)
	return nil
}

// EnqueueDecoded copies one complete raw message into bounded memory. Crossing
// either high-water mark synchronously closes/drains an unthrottleable socket;
// the message that caused pressure is durable before the blind gap opens.
func (w *WriterPressure) EnqueueDecoded(ctx context.Context, message RawMessage) error {
	if w == nil || !validPressureText(message.Stream) || !validPressureText(message.Coordinate) || len(message.Payload) == 0 {
		return errors.New("capture: invalid raw message")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != PressureRunning && (w.config.Transport != PressureTransportREST || w.state != PressureRESTThrottled) {
		if !w.complete {
			return ErrPressureIncomplete
		}
		return ErrWriterPressure
	}
	if !w.complete {
		return ErrPressureIncomplete
	}
	if !message.FrameComplete {
		w.complete = false
		w.state = PressureFaulted
		return ErrPressureIncomplete
	}
	if len(message.Payload) > w.config.MaxRawMessageBytes {
		w.complete = false
		w.state = PressureFaulted
		return errors.New("capture: raw message exceeds configured bound")
	}
	if len(w.decode) >= w.config.DecodeQueueCapacity {
		w.complete = false
		w.state = PressureFaulted
		return ErrPressureQueueFull
	}
	message.Payload = slices.Clone(message.Payload)
	w.decode = append(w.decode, message)
	if w.state != PressureRunning || !w.atHighWater() {
		return nil
	}
	return w.handleHighWater(ctx)
}

// AdvanceDecode preserves FIFO order while moving one message to the durable
// queue. It does not decode, coalesce, or copy the canonical payload.
func (w *WriterPressure) AdvanceDecode(ctx context.Context) error {
	if w == nil {
		return ErrWriterPressure
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.decode) == 0 {
		return nil
	}
	if len(w.durableQueue) >= w.config.DurableQueueCapacity {
		return ErrPressureQueueFull
	}
	message := w.decode[0]
	w.durableQueue = append(w.durableQueue, message)
	w.decode[0] = RawMessage{}
	w.decode = w.decode[1:]
	if w.state == PressureRunning && w.atHighWater() {
		return w.handleHighWater(ctx)
	}
	return nil
}

// CommitOne advances one canonical message across the durable boundary. A
// failed write leaves the message at the queue head for explicit recovery.
func (w *WriterPressure) CommitOne(ctx context.Context) error {
	if w == nil {
		return ErrWriterPressure
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.durableQueue) == 0 {
		return nil
	}
	if err := w.durable.WriteRaw(ctx, w.durableQueue[0]); err != nil {
		w.complete = false
		w.state = PressureFaulted
		return err
	}
	w.durableQueue[0] = RawMessage{}
	w.durableQueue = w.durableQueue[1:]
	return nil
}

// Recover resumes scheduling or reconnects only when both queues are at their
// low-water marks. WebSocket completeness becomes true only after reconnect
// succeeds; the prior blind gap remains recorded.
func (w *WriterPressure) Recover(ctx context.Context) error {
	if w == nil {
		return ErrWriterPressure
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != PressureRESTThrottled && w.state != PressureBlindGap {
		return ErrWriterPressure
	}
	if err := w.recordPendingRESTOutcomes(ctx); err != nil {
		return err
	}
	if len(w.decode) > w.config.DecodeLowWater || len(w.durableQueue) > w.config.DurableLowWater {
		return ErrPressureNotRecovered
	}
	if w.config.Transport == PressureTransportREST {
		if err := w.hooks.ResumeREST(ctx); err != nil {
			return err
		}
		w.recordAction(PressureResumeREST)
		w.state = PressureRunning
		w.complete = true
		return nil
	}
	if err := w.hooks.Reconnect(ctx); err != nil {
		return err
	}
	w.recordAction(PressureReconnect)
	w.state = PressureRunning
	w.complete = true
	return nil
}

func (w *WriterPressure) Snapshot() WriterPressureSnapshot {
	if w == nil {
		return WriterPressureSnapshot{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return WriterPressureSnapshot{
		State: w.state, Complete: w.complete, DecodeDepth: len(w.decode), DurableDepth: len(w.durableQueue),
		PendingREST: len(w.pendingREST), LastCommit: w.lastCommit, LastActions: slices.Clone(w.lastActions),
	}
}

func (w *WriterPressure) atHighWater() bool {
	return len(w.decode) >= w.config.DecodeHighWater || len(w.durableQueue) >= w.config.DurableHighWater
}

func (w *WriterPressure) handleHighWater(ctx context.Context) error {
	w.lastActions = w.lastActions[:0]
	w.recordAction(PressureStopREST)
	outcomeErr := w.recordPendingRESTOutcomes(ctx)
	w.recordAction(PressureRecordOutcomes)
	if w.config.Transport == PressureTransportREST {
		w.state = PressureRESTThrottled
		return outcomeErr
	}

	w.state = PressureDraining
	w.complete = false
	w.recordAction(PressureCloseWebSocket)
	if err := w.hooks.CloseWebSocket(ctx); err != nil {
		w.state = PressureFaulted
		return errors.Join(outcomeErr, err)
	}
	if err := w.drainRaw(ctx); err != nil {
		w.state = PressureFaulted
		return errors.Join(outcomeErr, err)
	}
	commit, err := w.durable.FlushCommit(ctx)
	if err != nil {
		w.state = PressureFaulted
		return errors.Join(outcomeErr, err)
	}
	if !validPressureText(commit.SegmentID) || !validPressureText(commit.LastCoordinate) {
		w.state = PressureFaulted
		return errors.Join(outcomeErr, errors.New("capture: durable boundary returned an invalid commit"))
	}
	w.lastCommit = commit
	w.recordAction(PressureFlushCommit)
	gap := BlindGap{DetectedWallTimeNS: w.clock.Read().WallTimeNS, LastCommit: commit, Reason: "writer_backpressure"}
	if err := w.hooks.OpenBlindGap(ctx, gap); err != nil {
		w.recordAction(PressureOpenBlindGap)
		w.state = PressureFaulted
		return errors.Join(outcomeErr, err)
	}
	w.recordAction(PressureOpenBlindGap)
	w.state = PressureBlindGap
	w.recordAction(PressureAwaitCapacity)
	return outcomeErr
}

func (w *WriterPressure) drainRaw(ctx context.Context) error {
	for len(w.durableQueue) > 0 {
		if err := w.durable.WriteRaw(ctx, w.durableQueue[0]); err != nil {
			return err
		}
		w.durableQueue[0] = RawMessage{}
		w.durableQueue = w.durableQueue[1:]
	}
	for len(w.decode) > 0 {
		if err := w.durable.WriteRaw(ctx, w.decode[0]); err != nil {
			return err
		}
		w.decode[0] = RawMessage{}
		w.decode = w.decode[1:]
	}
	return nil
}

func (w *WriterPressure) recordPendingRESTOutcomes(ctx context.Context) error {
	kept := 0
	var outcomeErrors []error
	for _, opportunityID := range w.pendingREST {
		if err := w.hooks.RecordRESTOutcome(ctx, opportunityID, PressureOutcomeBackpressureDelayed); err != nil {
			w.pendingREST[kept] = opportunityID
			kept++
			outcomeErrors = append(outcomeErrors, err)
		}
	}
	for index := kept; index < len(w.pendingREST); index++ {
		w.pendingREST[index] = ""
	}
	w.pendingREST = w.pendingREST[:kept]
	return errors.Join(outcomeErrors...)
}

func (w *WriterPressure) recordAction(action PressureAction) {
	if len(w.lastActions) < 8 {
		w.lastActions = append(w.lastActions, action)
	}
}

func validateWriterPressureConfig(config WriterPressureConfig) error {
	if config.Transport != PressureTransportREST && config.Transport != PressureTransportWebSocket {
		return errors.New("capture: writer pressure transport is invalid")
	}
	if config.DecodeQueueCapacity < 2 || config.DurableQueueCapacity < 2 || config.MaxRawMessageBytes < 1 || config.PendingRESTCapacity < 1 {
		return errors.New("capture: writer pressure capacities must be positive")
	}
	if config.DecodeHighWater < 1 || config.DecodeHighWater >= config.DecodeQueueCapacity ||
		config.DurableHighWater < 1 || config.DurableHighWater >= config.DurableQueueCapacity ||
		config.DecodeLowWater < 0 || config.DecodeLowWater >= config.DecodeHighWater ||
		config.DurableLowWater < 0 || config.DurableLowWater >= config.DurableHighWater {
		return errors.New("capture: writer pressure water marks are invalid")
	}
	return nil
}

func validPressureText(value string) bool {
	if value == "" || len(value) > 512 {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e || character == '?' || character == '#' {
			return false
		}
	}
	return true
}

func (s WriterPressureSnapshot) String() string {
	return fmt.Sprintf("state=%s complete=%t decode=%d durable=%d", s.State, s.Complete, s.DecodeDepth, s.DurableDepth)
}
