package binance

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/enable-xyz/marketdata/capture"
)

const SpotMaxConnectionEpochs = 64

var (
	ErrSpotConnection      = errors.New("binance: Spot WebSocket connection failure")
	ErrSpotControlRate     = errors.New("binance: Spot WebSocket control-message rate exceeded")
	ErrSpotPingDeadline    = errors.New("binance: Spot WebSocket pong deadline missed")
	ErrSpotEpochExhausted  = errors.New("binance: Spot connection epoch inventory exhausted")
	ErrSpotClockRegression = errors.New("binance: Spot transport monotonic clock regressed")
)

type SpotWSFrameKind uint8

const (
	SpotWSFrameText SpotWSFrameKind = iota + 1
	SpotWSFrameBinary
	SpotWSFramePing
	SpotWSFramePong
	SpotWSFrameClose
)

type SpotWSFrame struct {
	Kind    SpotWSFrameKind
	Payload []byte
}

type SpotWSWriteKind uint8

const (
	SpotWSWriteText SpotWSWriteKind = iota + 1
	SpotWSWritePong
)

type SpotWSConnectRequest struct {
	Endpoint            string
	TimeUnit            string
	MaxApplicationBytes uint32
}

// SpotWSConnection returns complete application messages after negotiated
// WebSocket decompression and fragmentation. Read and ReadBuffered must honor
// maxBytes before allocating or returning a larger message. ReadBuffered must
// never wait for new network input and reports only already-complete messages.
type SpotWSConnection interface {
	Read(context.Context, uint32) (SpotWSFrame, error)
	ReadBuffered(context.Context, uint32) (SpotWSFrame, bool, error)
	Write(context.Context, SpotWSWriteKind, []byte) error
	Close(context.Context, capture.CloseReason) error
}

type SpotWSConnector interface {
	Connect(context.Context, SpotWSConnectRequest) (SpotWSConnection, error)
}

type SpotConnectFailure struct {
	Kind  capture.TransportFailureKind
	Cause error
}

func (e *SpotConnectFailure) Error() string {
	return fmt.Sprintf("%v: kind %d", ErrSpotConnection, e.Kind)
}

func (e *SpotConnectFailure) Unwrap() error { return e.Cause }

type SpotWSConfig struct {
	Symbols         []string
	MicrosecondTime bool
	RecorderVersion string
	Endpoint        string
	Epochs          []capture.StreamEpoch
}

type SpotCapture struct {
	config           SpotWSConfig
	plan             SpotSubscriptionPlan
	connector        SpotWSConnector
	clock            capture.Clock
	budget           capture.RateBudget
	sink             capture.RawSink
	runner           *capture.Runner
	transport        *spotWSTransport
	nextEpoch        int
	started          bool
	reconnect        bool
	rotationDraining bool
	terminated       bool
}

func NewSpotCapture(config SpotWSConfig, connector SpotWSConnector, clock capture.Clock, budget capture.RateBudget, sink capture.RawSink) (*SpotCapture, error) {
	if connector == nil || clock == nil || budget == nil || sink == nil {
		return nil, fmt.Errorf("%w: connector, clock, rate budget, and raw sink are required", ErrSpotConfiguration)
	}
	if config.RecorderVersion == "" || len(config.RecorderVersion) > capture.MaxRecorderVersionBytes {
		return nil, fmt.Errorf("%w: recorder version is required and bounded", ErrSpotConfiguration)
	}
	if config.Endpoint == "" {
		config.Endpoint = SpotWSEndpoint
	}
	if config.Endpoint != SpotWSEndpoint {
		return nil, fmt.Errorf("%w: WebSocket endpoint is not the public Spot contract", ErrSpotConfiguration)
	}
	if len(config.Epochs) == 0 || len(config.Epochs) > SpotMaxConnectionEpochs {
		return nil, fmt.Errorf("%w: connection epochs must be within 1..%d", ErrSpotBounds, SpotMaxConnectionEpochs)
	}
	seenEpochs := make(map[[16]byte]struct{}, len(config.Epochs))
	for i, epoch := range config.Epochs {
		if err := epoch.Validate(); err != nil || epoch.Kind != capture.EpochConnection {
			return nil, fmt.Errorf("%w: epoch %d is not a connection epoch", ErrSpotConfiguration, i)
		}
		if _, duplicate := seenEpochs[epoch.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate connection epoch %d", ErrSpotConfiguration, i)
		}
		seenEpochs[epoch.ID] = struct{}{}
	}
	plan, err := NewSpotSubscriptionPlan(config.Symbols)
	if err != nil {
		return nil, err
	}
	config.Symbols = slices.Clone(config.Symbols)
	config.Epochs = slices.Clone(config.Epochs)
	return &SpotCapture{
		config:    config,
		plan:      plan,
		connector: connector,
		clock:     clock,
		budget:    budget,
		sink:      sink,
	}, nil
}

func (c *SpotCapture) SubscriptionPlan() SpotSubscriptionPlan {
	return cloneSpotSubscriptionPlan(c.plan)
}

func (c *SpotCapture) Start(ctx context.Context) (capture.StepResult, error) {
	if c.started {
		return capture.StepResult{}, fmt.Errorf("%w: capture already started", ErrSpotConfiguration)
	}
	c.started = true
	if err := c.startEpoch(false); err != nil {
		return capture.StepResult{}, err
	}
	result, err := c.runner.Start(ctx)
	c.afterStep(ctx, result)
	return result, err
}

func (c *SpotCapture) Step(ctx context.Context) (capture.StepResult, error) {
	if !c.started {
		return capture.StepResult{}, capture.ErrRunnerNotStarted
	}
	if c.terminated {
		return capture.StepResult{}, capture.ErrRunnerClosed
	}
	if c.runner == nil {
		if err := c.startEpoch(c.reconnect); err != nil {
			c.terminated = true
			return capture.StepResult{}, err
		}
		result, err := c.runner.Start(ctx)
		c.afterStep(ctx, result)
		return result, err
	}
	var result capture.StepResult
	var err error
	if c.rotationDraining || c.transport.rotationDue() {
		c.rotationDraining = true
		result, err = c.runner.ClosePlanned(ctx)
	} else {
		result, err = c.runner.Step(ctx)
	}
	c.afterStep(ctx, result)
	if result.State == capture.RunnerClosed {
		c.runner = nil
		c.transport = nil
		c.reconnect = true
		c.rotationDraining = false
		if c.nextEpoch == len(c.config.Epochs) {
			c.terminated = true
		}
	}
	return result, err
}

// Close performs a bounded planned drain and commits the terminal disconnect
// control record. It is used by caller-authorized live verification limits.
func (c *SpotCapture) Close(ctx context.Context) (capture.StepResult, error) {
	if !c.started {
		return capture.StepResult{}, capture.ErrRunnerNotStarted
	}
	if c.terminated || c.runner == nil {
		return capture.StepResult{State: capture.RunnerClosed}, nil
	}
	c.rotationDraining = true
	result, err := c.runner.ClosePlanned(ctx)
	c.afterStep(ctx, result)
	if result.State == capture.RunnerClosed {
		c.runner = nil
		c.transport = nil
		c.terminated = true
		c.rotationDraining = false
	}
	return result, err
}

func (c *SpotCapture) startEpoch(reconnect bool) error {
	if c.nextEpoch >= len(c.config.Epochs) {
		return ErrSpotEpochExhausted
	}
	request := SpotWSConnectRequest{
		Endpoint:            c.config.Endpoint,
		MaxApplicationBytes: SpotMaxRawPayloadBytes,
	}
	if c.config.MicrosecondTime {
		request.TimeUnit = "MICROSECOND"
	}
	transport := &spotWSTransport{
		connector: c.connector,
		clock:     c.clock,
		request:   request,
	}
	observer := NewSpotRawObserver(c.plan)
	runner, err := capture.NewRunner(SpotWSSourceContract(), capture.RunnerConfig{
		Epoch:                 c.config.Epochs[c.nextEpoch],
		ChannelOrEndpoint:     SpotRawChannel,
		DataFamily:            SpotRawDataFamily,
		RecorderVersion:       c.config.RecorderVersion,
		SubscriptionRequestID: SpotSubscriptionRequestID,
		ExpectedSubscriptions: c.plan.Inventory,
		SubscriptionEvidence:  c.plan.Evidence,
		Reconnect:             reconnect,
	}, transport, c.clock, c.budget, c.sink, observer)
	if err != nil {
		return err
	}
	c.nextEpoch++
	c.transport = transport
	c.runner = runner
	return nil
}

func (c *SpotCapture) afterStep(ctx context.Context, result capture.StepResult) {
	if c.transport == nil || c.transport.closed || result.State == capture.RunnerClosed {
		return
	}
	for _, control := range result.Controls {
		switch control.Envelope.ControlKind.Value {
		case capture.ControlConnectAttempt:
			c.transport.prepareConnection(ctx)
		case capture.ControlSubscribeRequest:
			if c.rotationDraining {
				continue
			}
			if err := c.transport.sendSubscriptions(ctx, c.plan.Requests); err != nil {
				c.transport.reject(ctx)
			}
		case capture.ControlHeartbeat:
			if err := c.transport.echoPing(ctx, control.Envelope.RawPayload); err != nil {
				c.transport.reject(ctx)
			}
		}
	}
	for _, fault := range result.Faults {
		if fault.Kind == capture.FaultSchemaUnknownRole {
			c.transport.reject(ctx)
		}
	}
}

type spotWSTransport struct {
	connector          SpotWSConnector
	clock              capture.Clock
	request            SpotWSConnectRequest
	connection         SpotWSConnection
	pendingFailure     capture.TransportFailureKind
	connectedPending   bool
	connectedAtNS      uint64
	lastClockNS        uint64
	controlTimes       [SpotControlMessagesPerSecond]uint64
	controlCount       int
	pendingPing        []byte
	pendingPingPresent bool
	pingReceivedAt     uint64
	pendingRejected    bool
	closed             bool
}

func (t *spotWSTransport) Next(ctx context.Context) (capture.TransportEvent, error) {
	if err := ctx.Err(); err != nil {
		return capture.TransportEvent{}, err
	}
	if t.closed {
		return capture.TransportEvent{}, capture.ErrTransportClosed
	}
	if t.pendingRejected {
		t.pendingRejected = false
		return capture.TransportEvent{Kind: capture.TransportEventDisconnected}, nil
	}
	if t.pendingFailure != 0 {
		failure := t.pendingFailure
		t.pendingFailure = 0
		return capture.TransportEvent{Kind: capture.TransportEventFailure, Failure: failure}, nil
	}
	reading := t.clock.Read()
	if reading.MonotonicNS < t.lastClockNS {
		return capture.TransportEvent{}, ErrSpotClockRegression
	}
	t.lastClockNS = reading.MonotonicNS
	if t.connection == nil {
		t.prepareConnection(ctx)
		if t.pendingFailure != 0 {
			failure := t.pendingFailure
			t.pendingFailure = 0
			return capture.TransportEvent{Kind: capture.TransportEventFailure, Failure: failure}, nil
		}
		if t.connection == nil {
			return capture.TransportEvent{Kind: capture.TransportEventFailure, Failure: capture.TransportFailureConnect}, nil
		}
	}
	state := "connected;timeUnit=MILLISECOND"
	if t.request.TimeUnit == "MICROSECOND" {
		state = "connected;timeUnit=MICROSECOND"
	}
	if t.connectedPending {
		t.connectedPending = false
		return capture.TransportEvent{Kind: capture.TransportEventConnected, WSState: state}, nil
	}
	if reading.MonotonicNS-t.connectedAtNS >= SpotSocketLifetimeNS {
		_ = t.connection.Close(ctx, capture.ClosePlanned)
		return capture.TransportEvent{Kind: capture.TransportEventDisconnected, Planned: true}, nil
	}
	frame, err := t.connection.Read(ctx, SpotMaxRawPayloadBytes)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return capture.TransportEvent{}, err
		}
		return capture.TransportEvent{Kind: capture.TransportEventDisconnected}, nil
	}
	return t.frameEvent(frame)
}

func (t *spotWSTransport) NextBuffered(ctx context.Context) (capture.TransportEvent, bool, error) {
	if err := ctx.Err(); err != nil {
		return capture.TransportEvent{}, false, err
	}
	if t.connection == nil || t.closed {
		return capture.TransportEvent{}, false, nil
	}
	frame, available, err := t.connection.ReadBuffered(ctx, SpotMaxRawPayloadBytes)
	if err != nil || !available {
		return capture.TransportEvent{}, false, err
	}
	event, err := t.frameEvent(frame)
	return event, err == nil, err
}

func (t *spotWSTransport) frameEvent(frame SpotWSFrame) (capture.TransportEvent, error) {
	received := t.clock.Read()
	afterRawFailure := capture.TransportFailureKind(0)
	if received.MonotonicNS < t.lastClockNS {
		afterRawFailure = capture.TransportFailureClockRegression
	} else {
		t.lastClockNS = received.MonotonicNS
	}
	if len(frame.Payload) > SpotMaxRawPayloadBytes {
		return capture.TransportEvent{}, fmt.Errorf("%w: WebSocket message has %d bytes, maximum is %d", ErrSpotBounds, len(frame.Payload), SpotMaxRawPayloadBytes)
	}
	raw := append([]byte(nil), frame.Payload...)
	switch frame.Kind {
	case SpotWSFrameText:
		return capture.TransportEvent{
			Kind: capture.TransportEventApplication, Raw: raw, Encoding: capture.PayloadEncodingJSON,
			AfterRawFailure: afterRawFailure,
		}, nil
	case SpotWSFrameBinary, SpotWSFramePong:
		return capture.TransportEvent{
			Kind: capture.TransportEventApplication, Raw: raw, Encoding: capture.PayloadEncodingBinary,
			AfterRawFailure: afterRawFailure,
		}, nil
	case SpotWSFramePing:
		if len(raw) > SpotMaxPingPayloadBytes {
			return capture.TransportEvent{}, fmt.Errorf("%w: ping payload has %d bytes, maximum is %d", ErrSpotBounds, len(raw), SpotMaxPingPayloadBytes)
		}
		t.pendingPing = raw
		t.pendingPingPresent = true
		t.pingReceivedAt = received.MonotonicNS
		return capture.TransportEvent{
			Kind: capture.TransportEventHeartbeat, Raw: raw, Encoding: capture.PayloadEncodingBinary,
			AfterRawFailure: afterRawFailure,
		}, nil
	case SpotWSFrameClose:
		if afterRawFailure != 0 {
			return capture.TransportEvent{}, ErrSpotClockRegression
		}
		return capture.TransportEvent{Kind: capture.TransportEventDisconnected}, nil
	default:
		return capture.TransportEvent{}, fmt.Errorf("%w: unsupported WebSocket frame kind %d", ErrSpotConfiguration, frame.Kind)
	}
}

func (t *spotWSTransport) prepareConnection(ctx context.Context) {
	if t.connection != nil || t.pendingFailure != 0 || t.closed {
		return
	}
	reading := t.clock.Read()
	if reading.MonotonicNS < t.lastClockNS {
		t.pendingFailure = capture.TransportFailureConnect
		return
	}
	t.lastClockNS = reading.MonotonicNS
	connection, err := t.connector.Connect(ctx, t.request)
	if err != nil {
		failure := capture.TransportFailureConnect
		var classified *SpotConnectFailure
		if errors.As(err, &classified) && classified.Kind >= capture.TransportFailureDNS && classified.Kind <= capture.TransportFailureConnect {
			failure = classified.Kind
		}
		t.pendingFailure = failure
		return
	}
	if connection == nil {
		t.pendingFailure = capture.TransportFailureConnect
		return
	}
	t.connection = connection
	t.connectedAtNS = reading.MonotonicNS
	t.connectedPending = true
}

func (t *spotWSTransport) Close(ctx context.Context, reason capture.CloseReason) error {
	if t.closed {
		return nil
	}
	t.closed = true
	if t.connection == nil {
		return nil
	}
	return t.connection.Close(ctx, reason)
}

func (t *spotWSTransport) rotationDue() bool {
	if t.connection == nil || t.connectedPending || t.closed {
		return false
	}
	reading := t.clock.Read()
	return reading.MonotonicNS >= t.connectedAtNS && reading.MonotonicNS-t.connectedAtNS >= SpotSocketLifetimeNS
}

func (t *spotWSTransport) sendSubscriptions(ctx context.Context, requests []SpotSubscriptionRequest) error {
	if t.closed || t.connection == nil || len(requests) == 0 || len(requests) > SpotSubscriptionBatchCount {
		return ErrSpotConnection
	}
	for _, request := range requests {
		if len(request.Raw) == 0 || len(request.Raw) > SpotMaxControlMessageBytes {
			return ErrSpotBounds
		}
		if err := t.reserveControlMessage(); err != nil {
			return err
		}
		if err := t.connection.Write(ctx, SpotWSWriteText, append([]byte(nil), request.Raw...)); err != nil {
			return ErrSpotConnection
		}
	}
	return nil
}

func (t *spotWSTransport) echoPing(ctx context.Context, payload []byte) error {
	if t.closed || t.connection == nil || !t.pendingPingPresent || !slices.Equal(payload, t.pendingPing) {
		return ErrSpotConnection
	}
	reading := t.clock.Read()
	if reading.MonotonicNS < t.pingReceivedAt {
		return ErrSpotClockRegression
	}
	if reading.MonotonicNS-t.pingReceivedAt > SpotPongDeadlineNS {
		return ErrSpotPingDeadline
	}
	if err := t.reserveControlMessage(); err != nil {
		return err
	}
	pong := append([]byte(nil), t.pendingPing...)
	t.pendingPing = nil
	t.pendingPingPresent = false
	return t.connection.Write(ctx, SpotWSWritePong, pong)
}

func (t *spotWSTransport) reserveControlMessage() error {
	now := t.clock.Read().MonotonicNS
	if now < t.lastClockNS {
		return ErrSpotClockRegression
	}
	t.lastClockNS = now
	retained := 0
	for i := range t.controlCount {
		if now-t.controlTimes[i] < uint64(1_000_000_000) {
			t.controlTimes[retained] = t.controlTimes[i]
			retained++
		}
	}
	t.controlCount = retained
	if t.controlCount == SpotControlMessagesPerSecond {
		return ErrSpotControlRate
	}
	t.controlTimes[t.controlCount] = now
	t.controlCount++
	return nil
}

func (t *spotWSTransport) reject(ctx context.Context) {
	t.pendingRejected = true
	if t.connection != nil {
		_ = t.connection.Close(ctx, capture.CloseSchemaRejected)
	}
}
