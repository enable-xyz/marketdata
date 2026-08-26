package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	DerivativeMaxConnectionEpochs = 64
	DerivativeSocketLifetimeNS    = uint64(24 * time.Hour)
	DerivativeACKDeadlineNS       = uint64(10 * time.Second)
	DerivativePongDeadlineNS      = uint64(10 * time.Minute)
)

var (
	ErrDerivativeConnection      = errors.New("binance: derivative WebSocket connection failure")
	ErrDerivativeControlRate     = errors.New("binance: derivative WebSocket control-message rate exceeded")
	ErrDerivativePingDeadline    = errors.New("binance: derivative WebSocket pong deadline missed")
	ErrDerivativeEpochExhausted  = errors.New("binance: derivative connection epoch inventory exhausted")
	ErrDerivativeClockRegression = errors.New("binance: derivative transport monotonic clock regressed")
)

type DerivativeWSFrameKind uint8

const (
	DerivativeWSFrameText DerivativeWSFrameKind = iota + 1
	DerivativeWSFrameBinary
	DerivativeWSFramePing
	DerivativeWSFramePong
	DerivativeWSFrameClose
)

type DerivativeWSFrame struct {
	Kind    DerivativeWSFrameKind
	Payload []byte
}

type DerivativeWSWriteKind uint8

const (
	DerivativeWSWriteText DerivativeWSWriteKind = iota + 1
	DerivativeWSWritePong
)

type DerivativeWSConnectRequest struct {
	Product             DerivativeProduct
	Endpoint            string
	MaxApplicationBytes uint32
}

// DerivativeWSConnection returns complete application messages after WebSocket
// decompression and fragmentation. Both read methods must enforce maxBytes
// before allocating or returning a larger message; ReadBuffered never waits for
// new network input.
type DerivativeWSConnection interface {
	Read(context.Context, uint32) (DerivativeWSFrame, error)
	ReadBuffered(context.Context, uint32) (DerivativeWSFrame, bool, error)
	Write(context.Context, DerivativeWSWriteKind, []byte) error
	Close(context.Context, capture.CloseReason) error
}

type DerivativeWSConnector interface {
	Connect(context.Context, DerivativeWSConnectRequest) (DerivativeWSConnection, error)
}

type DerivativeConnectFailure struct {
	Kind  capture.TransportFailureKind
	Cause error
}

func (e *DerivativeConnectFailure) Error() string {
	return fmt.Sprintf("%v: kind %d", ErrDerivativeConnection, e.Kind)
}

func (e *DerivativeConnectFailure) Unwrap() error { return e.Cause }

type DerivativeWSConfig struct {
	Symbols         []string
	RecorderVersion string
	Endpoint        string
	Epochs          []capture.StreamEpoch
}

type DerivativeCapture struct {
	product          DerivativeProduct
	config           DerivativeWSConfig
	plan             DerivativeSubscriptionPlan
	contract         capture.SourceContract
	connector        DerivativeWSConnector
	clock            capture.Clock
	budget           capture.RateBudget
	sink             capture.RawSink
	runner           *capture.Runner
	transport        *derivativeWSTransport
	nextEpoch        int
	started          bool
	reconnect        bool
	rotationDraining bool
	terminated       bool
}

// NewUSDMDerivativeCapture constructs a credential-free USD-M public capture
// using the concrete coder/websocket client. Endpoint must be one of the two
// routes declared by the USD-M source contract.
func NewUSDMDerivativeCapture(config DerivativeWSConfig, client *http.Client, clock capture.Clock, budget capture.RateBudget, sink capture.RawSink) (*DerivativeCapture, error) {
	connector, err := NewCoderDerivativeWSConnector(client)
	if err != nil {
		return nil, err
	}
	return newDerivativeCapture(DerivativeProductUSDM, config, connector, clock, budget, sink)
}

// NewCoinMDerivativeCapture constructs a credential-free COIN-M public capture
// using the concrete coder/websocket client and the declared DAPI endpoint.
func NewCoinMDerivativeCapture(config DerivativeWSConfig, client *http.Client, clock capture.Clock, budget capture.RateBudget, sink capture.RawSink) (*DerivativeCapture, error) {
	connector, err := NewCoderDerivativeWSConnector(client)
	if err != nil {
		return nil, err
	}
	return newDerivativeCapture(DerivativeProductCoinM, config, connector, clock, budget, sink)
}

func newDerivativeCapture(product DerivativeProduct, config DerivativeWSConfig, connector DerivativeWSConnector, clock capture.Clock, budget capture.RateBudget, sink capture.RawSink) (*DerivativeCapture, error) {
	if connector == nil || clock == nil || budget == nil || sink == nil {
		return nil, fmt.Errorf("%w: connector, clock, rate budget, and raw sink are required", ErrDerivativeConfiguration)
	}
	if config.RecorderVersion == "" || len(config.RecorderVersion) > capture.MaxRecorderVersionBytes {
		return nil, fmt.Errorf("%w: recorder version is required and bounded", ErrDerivativeConfiguration)
	}
	if len(config.Epochs) == 0 || len(config.Epochs) > DerivativeMaxConnectionEpochs {
		return nil, fmt.Errorf("%w: connection epochs must be within 1..%d", ErrDerivativeBounds, DerivativeMaxConnectionEpochs)
	}
	seenEpochs := make(map[[16]byte]struct{}, len(config.Epochs))
	for i, epoch := range config.Epochs {
		if err := epoch.Validate(); err != nil || epoch.Kind != capture.EpochConnection {
			return nil, fmt.Errorf("%w: epoch %d is not a connection epoch", ErrDerivativeConfiguration, i)
		}
		if _, duplicate := seenEpochs[epoch.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate connection epoch %d", ErrDerivativeConfiguration, i)
		}
		seenEpochs[epoch.ID] = struct{}{}
	}
	var (
		plan     DerivativeSubscriptionPlan
		contract capture.SourceContract
		err      error
	)
	switch product {
	case DerivativeProductUSDM:
		plan, err = NewUSDMDerivativeSubscriptionPlan(config.Endpoint, config.Symbols)
		if err == nil {
			contract = usdmWebSocketSourceContract(usdmRouteForEndpoint(config.Endpoint))
		}
	case DerivativeProductCoinM:
		plan, err = NewCoinMDerivativeSubscriptionPlan(config.Endpoint, config.Symbols)
		if err == nil {
			contract = CoinMSourceContract()
		}
	default:
		err = fmt.Errorf("%w: unknown derivative product %q", ErrDerivativeConfiguration, product)
	}
	if err != nil {
		return nil, err
	}
	contract = derivativeCaptureContract(contract, product, config.Endpoint)
	if err := contract.Validate(); err != nil {
		return nil, fmt.Errorf("%w: live source contract: %v", ErrDerivativeConfiguration, err)
	}
	config.Symbols = slices.Clone(config.Symbols)
	config.Epochs = slices.Clone(config.Epochs)
	return &DerivativeCapture{
		product: product, config: config, plan: plan, contract: contract, connector: connector,
		clock: clock, budget: budget, sink: sink,
	}, nil
}

func derivativeCaptureContract(contract capture.SourceContract, product DerivativeProduct, endpoint string) capture.SourceContract {
	channel := derivativeChannel(product, endpoint)
	family := derivativeDataFamily(product)
	contract.Capabilities = slices.Clone(contract.Capabilities)
	contract.Capabilities = append(contract.Capabilities, capture.Capability{
		ChannelOrEndpoint: channel, DataFamily: family, Entitlement: "public", Support: capture.SupportAvailable,
	})
	route := "merged"
	if product == DerivativeProductUSDM {
		route = string(usdmRouteForEndpoint(endpoint))
	}
	contract.ContractID = "binance." + string(product) + ".ws." + route + ".live-capture.v1"
	contract.Topology.MaxSubscriptions = capture.MaxExpectedSubscriptions
	contract.Topology.MaxSubscriptionsPerACK = DerivativeSubscriptionBatchLimit
	contract.Subscription = capture.SubscriptionPolicy{
		ACKMode: capture.ACKExact, ACKTimeoutNS: DerivativeACKDeadlineNS, MaxPendingACK: DerivativeSubscriptionBatchCount,
	}
	return contract
}

func (c *DerivativeCapture) Product() DerivativeProduct { return c.product }

func (c *DerivativeCapture) SubscriptionPlan() DerivativeSubscriptionPlan {
	return cloneDerivativeSubscriptionPlan(c.plan)
}

func (c *DerivativeCapture) Start(ctx context.Context) (capture.StepResult, error) {
	if c.started {
		return capture.StepResult{}, fmt.Errorf("%w: capture already started", ErrDerivativeConfiguration)
	}
	c.started = true
	if err := c.startEpoch(false); err != nil {
		return capture.StepResult{}, err
	}
	result, err := c.runner.Start(ctx)
	c.afterStep(ctx, result)
	return result, err
}

func (c *DerivativeCapture) Step(ctx context.Context) (capture.StepResult, error) {
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
	var (
		result capture.StepResult
		err    error
	)
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
	}
	return result, err
}

// Close performs a bounded planned drain and terminates without opening another
// epoch. It is the caller-owned live-canary stop boundary.
func (c *DerivativeCapture) Close(ctx context.Context) (capture.StepResult, error) {
	if !c.started {
		return capture.StepResult{}, capture.ErrRunnerNotStarted
	}
	if c.terminated || c.runner == nil {
		c.terminated = true
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

func (c *DerivativeCapture) startEpoch(reconnect bool) error {
	if c.nextEpoch >= len(c.config.Epochs) {
		return ErrDerivativeEpochExhausted
	}
	transport := &derivativeWSTransport{
		connector: c.connector,
		clock:     c.clock,
		request: DerivativeWSConnectRequest{
			Product: c.product, Endpoint: c.config.Endpoint, MaxApplicationBytes: derivativeMaxRawPayload(c.product),
		},
	}
	observer := newDerivativeRawObserver(c.plan, c.contract.Payload)
	config := capture.RunnerConfig{
		Epoch: c.config.Epochs[c.nextEpoch], ChannelOrEndpoint: derivativeChannel(c.product, c.config.Endpoint),
		DataFamily: derivativeDataFamily(c.product), RecorderVersion: c.config.RecorderVersion,
		SubscriptionRequestID: derivativeSubscriptionRequestID(c.product, c.config.Endpoint),
		ExpectedSubscriptions: c.plan.Inventory, SubscriptionEvidence: c.plan.Evidence, Reconnect: reconnect,
	}
	if c.product == DerivativeProductUSDM && len(c.plan.Symbols) == 1 {
		config.NativeSymbol = capture.OptionalString{Value: strings.ToUpper(c.plan.Symbols[0]), Valid: true}
	}
	runner, err := capture.NewRunner(c.contract, config, transport, c.clock, c.budget, c.sink, observer)
	if err != nil {
		return err
	}
	c.nextEpoch++
	c.transport = transport
	c.runner = runner
	return nil
}

func (c *DerivativeCapture) afterStep(ctx context.Context, result capture.StepResult) {
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
		switch fault.Kind {
		case capture.FaultSchemaMalformed, capture.FaultSchemaTypeChanged, capture.FaultSchemaUnknownRole, capture.FaultSchemaOversized:
			c.transport.reject(ctx)
		}
	}
}

func derivativeMaxRawPayload(product DerivativeProduct) uint32 {
	if product == DerivativeProductUSDM {
		return USDMMaxRawPayloadBytes
	}
	if product == DerivativeProductCoinM {
		return CoinMMaxRawPayloadBytes
	}
	return 0
}

type derivativeWSTransport struct {
	connector          DerivativeWSConnector
	clock              capture.Clock
	request            DerivativeWSConnectRequest
	connection         DerivativeWSConnection
	pendingFailure     capture.TransportFailureKind
	connectedPending   bool
	connectedAtNS      uint64
	lastClockNS        uint64
	controlTimes       [DerivativeControlMessagesPerSecond]uint64
	controlCount       int
	pendingPing        []byte
	pendingPingPresent bool
	pingReceivedAt     uint64
	pendingRejected    bool
	pendingACKs        map[string]struct{}
	closed             bool
}

func (t *derivativeWSTransport) Next(ctx context.Context) (capture.TransportEvent, error) {
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
		return capture.TransportEvent{}, ErrDerivativeClockRegression
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
	if t.connectedPending {
		t.connectedPending = false
		return capture.TransportEvent{Kind: capture.TransportEventConnected, WSState: derivativeConnectedState(t.request)}, nil
	}
	if reading.MonotonicNS-t.connectedAtNS >= DerivativeSocketLifetimeNS {
		_ = t.connection.Close(ctx, capture.ClosePlanned)
		return capture.TransportEvent{Kind: capture.TransportEventDisconnected, Planned: true}, nil
	}
	frame, err := t.connection.Read(ctx, derivativeMaxRawPayload(t.request.Product))
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return capture.TransportEvent{}, err
		}
		if errors.Is(err, ErrDerivativeBounds) {
			t.reject(ctx)
			return capture.TransportEvent{}, err
		}
		return capture.TransportEvent{Kind: capture.TransportEventDisconnected}, nil
	}
	event, err := t.frameEvent(frame)
	if err != nil {
		t.reject(ctx)
	}
	return event, err
}

func (t *derivativeWSTransport) NextBuffered(ctx context.Context) (capture.TransportEvent, bool, error) {
	if err := ctx.Err(); err != nil {
		return capture.TransportEvent{}, false, err
	}
	if t.connection == nil || t.closed {
		return capture.TransportEvent{}, false, nil
	}
	frame, available, err := t.connection.ReadBuffered(ctx, derivativeMaxRawPayload(t.request.Product))
	if err != nil || !available {
		if errors.Is(err, ErrDerivativeBounds) {
			t.reject(ctx)
		}
		return capture.TransportEvent{}, false, err
	}
	event, err := t.frameEvent(frame)
	if err != nil {
		t.reject(ctx)
		return capture.TransportEvent{}, false, err
	}
	return event, true, nil
}

func (t *derivativeWSTransport) frameEvent(frame DerivativeWSFrame) (capture.TransportEvent, error) {
	received := t.clock.Read()
	afterRawFailure := capture.TransportFailureKind(0)
	if received.MonotonicNS < t.lastClockNS {
		afterRawFailure = capture.TransportFailureClockRegression
	} else {
		t.lastClockNS = received.MonotonicNS
	}
	maximum := derivativeMaxRawPayload(t.request.Product)
	if len(frame.Payload) > int(maximum) {
		return capture.TransportEvent{}, fmt.Errorf("%w: WebSocket message has %d bytes, maximum is %d", ErrDerivativeBounds, len(frame.Payload), maximum)
	}
	raw := slices.Clone(frame.Payload)
	switch frame.Kind {
	case DerivativeWSFrameText:
		kind := capture.TransportEventApplication
		if t.isAcknowledgement(raw) {
			kind = capture.TransportEventAcknowledgement
		}
		return capture.TransportEvent{Kind: kind, Raw: raw, Encoding: capture.PayloadEncodingJSON, AfterRawFailure: afterRawFailure}, nil
	case DerivativeWSFrameBinary, DerivativeWSFramePong:
		return capture.TransportEvent{Kind: capture.TransportEventApplication, Raw: raw, Encoding: capture.PayloadEncodingBinary, AfterRawFailure: afterRawFailure}, nil
	case DerivativeWSFramePing:
		if len(raw) > DerivativeMaxPingPayloadBytes {
			return capture.TransportEvent{}, fmt.Errorf("%w: ping payload has %d bytes", ErrDerivativeBounds, len(raw))
		}
		t.pendingPing = raw
		t.pendingPingPresent = true
		t.pingReceivedAt = received.MonotonicNS
		return capture.TransportEvent{Kind: capture.TransportEventHeartbeat, Raw: raw, Encoding: capture.PayloadEncodingBinary, AfterRawFailure: afterRawFailure}, nil
	case DerivativeWSFrameClose:
		if afterRawFailure != 0 {
			return capture.TransportEvent{}, ErrDerivativeClockRegression
		}
		return capture.TransportEvent{Kind: capture.TransportEventDisconnected}, nil
	default:
		return capture.TransportEvent{}, fmt.Errorf("%w: unsupported WebSocket frame kind %d", ErrDerivativeConfiguration, frame.Kind)
	}
}

func (t *derivativeWSTransport) isAcknowledgement(raw []byte) bool {
	if len(t.pendingACKs) == 0 {
		return false
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return false
	}
	if _, hasResult := object["result"]; !hasResult {
		if _, hasCode := object["code"]; !hasCode {
			return false
		}
	}
	var id int64
	if json.Unmarshal(object["id"], &id) != nil {
		return false
	}
	key := strconv.FormatInt(id, 10)
	if _, known := t.pendingACKs[key]; known {
		delete(t.pendingACKs, key)
		if len(t.pendingACKs) == 0 {
			t.pendingACKs = nil
		}
	}
	return true
}

func (t *derivativeWSTransport) prepareConnection(ctx context.Context) {
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
		var classified *DerivativeConnectFailure
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

func (t *derivativeWSTransport) Close(ctx context.Context, reason capture.CloseReason) error {
	if t.closed {
		return nil
	}
	t.closed = true
	if t.connection == nil {
		return nil
	}
	return t.connection.Close(ctx, reason)
}

func (t *derivativeWSTransport) rotationDue() bool {
	if t.connection == nil || t.connectedPending || t.closed {
		return false
	}
	reading := t.clock.Read()
	return reading.MonotonicNS >= t.connectedAtNS && reading.MonotonicNS-t.connectedAtNS >= DerivativeSocketLifetimeNS
}

func (t *derivativeWSTransport) sendSubscriptions(ctx context.Context, requests []DerivativeSubscriptionRequest) error {
	if t.closed || t.connection == nil || len(requests) == 0 || len(requests) > DerivativeSubscriptionBatchCount {
		return ErrDerivativeConnection
	}
	t.pendingACKs = make(map[string]struct{}, len(requests))
	for _, request := range requests {
		t.pendingACKs[strconv.FormatInt(request.ID, 10)] = struct{}{}
	}
	for _, request := range requests {
		if len(request.Raw) == 0 || len(request.Raw) > DerivativeMaxControlMessageBytes {
			return ErrDerivativeBounds
		}
		if err := t.reserveControlMessage(); err != nil {
			return err
		}
		if err := t.connection.Write(ctx, DerivativeWSWriteText, slices.Clone(request.Raw)); err != nil {
			return ErrDerivativeConnection
		}
	}
	return nil
}

func (t *derivativeWSTransport) echoPing(ctx context.Context, payload []byte) error {
	if t.closed || t.connection == nil || !t.pendingPingPresent || !slices.Equal(payload, t.pendingPing) {
		return ErrDerivativeConnection
	}
	reading := t.clock.Read()
	if reading.MonotonicNS < t.pingReceivedAt {
		return ErrDerivativeClockRegression
	}
	if reading.MonotonicNS-t.pingReceivedAt > DerivativePongDeadlineNS {
		return ErrDerivativePingDeadline
	}
	if err := t.reserveControlMessage(); err != nil {
		return err
	}
	pong := slices.Clone(t.pendingPing)
	t.pendingPing = nil
	t.pendingPingPresent = false
	return t.connection.Write(ctx, DerivativeWSWritePong, pong)
}

func (t *derivativeWSTransport) reserveControlMessage() error {
	now := t.clock.Read().MonotonicNS
	if now < t.lastClockNS {
		return ErrDerivativeClockRegression
	}
	t.lastClockNS = now
	retained := 0
	for i := range t.controlCount {
		if now-t.controlTimes[i] < uint64(time.Second) {
			t.controlTimes[retained] = t.controlTimes[i]
			retained++
		}
	}
	t.controlCount = retained
	if t.controlCount == DerivativeControlMessagesPerSecond {
		return ErrDerivativeControlRate
	}
	t.controlTimes[t.controlCount] = now
	t.controlCount++
	return nil
}

func (t *derivativeWSTransport) reject(ctx context.Context) {
	t.pendingRejected = true
	if t.connection != nil {
		_ = t.connection.Close(ctx, capture.CloseSchemaRejected)
	}
}

func derivativeConnectedState(request DerivativeWSConnectRequest) string {
	route := "merged"
	if request.Product == DerivativeProductUSDM {
		route = string(usdmRouteForEndpoint(request.Endpoint))
	}
	return "connected;product=" + string(request.Product) + ";route=" + route + ";combined=true"
}

type derivativeRawObserver struct {
	plan      DerivativeSubscriptionPlan
	policy    capture.PayloadPolicy
	batches   map[string]int
	inventory map[string]struct{}
	requestID string
}

func newDerivativeRawObserver(plan DerivativeSubscriptionPlan, policy capture.PayloadPolicy) *derivativeRawObserver {
	batches := make(map[string]int, len(plan.Requests))
	for i, request := range plan.Requests {
		batches[strconv.FormatInt(request.ID, 10)] = i
	}
	inventory := make(map[string]struct{}, len(plan.Inventory))
	for _, stream := range plan.Inventory {
		inventory[stream] = struct{}{}
	}
	return &derivativeRawObserver{
		plan: cloneDerivativeSubscriptionPlan(plan), policy: policy, batches: batches, inventory: inventory,
		requestID: derivativeSubscriptionRequestID(plan.Product, plan.Endpoint),
	}
}

func (o *derivativeRawObserver) Observe(ctx context.Context, envelope capture.EnvelopeV1) (capture.Observation, error) {
	if err := ctx.Err(); err != nil {
		return capture.Observation{}, err
	}
	if envelope.RecordKind == capture.RecordKindControl && envelope.ControlKind.Valid && envelope.ControlKind.Value == capture.ControlHeartbeat {
		if len(envelope.RawPayload) > DerivativeMaxPingPayloadBytes {
			return capture.Observation{Role: capture.MessageHeartbeat, Schema: capture.SchemaMalformed}, nil
		}
		return capture.Observation{Role: capture.MessageHeartbeat, Schema: capture.SchemaAccepted, Unchanged: true}, nil
	}
	if envelope.PayloadEncoding != capture.PayloadEncodingJSON || len(envelope.RawPayload) == 0 || len(envelope.RawPayload) > int(o.policy.MaxRawBytes) {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
	}
	value, err := decodeBoundedJSON(envelope.RawPayload, o.policy)
	if err != nil {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaMalformed}, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
	}
	if _, hasResult := object["result"]; hasResult {
		return o.observeACK(object), nil
	}
	if _, hasCode := object["code"]; hasCode {
		return o.observeACK(object), nil
	}
	var wrapper struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(envelope.RawPayload, &wrapper); err != nil || wrapper.Stream == "" || len(wrapper.Data) == 0 {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
	}
	if _, subscribed := o.inventory[wrapper.Stream]; !subscribed {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
	}
	disposition := o.validateStream(wrapper.Stream, wrapper.Data, envelope.ReceivedWallTimeNS, envelope.RawPayload)
	return capture.Observation{Role: capture.MessageData, Schema: disposition}, nil
}

func (o *derivativeRawObserver) observeACK(object map[string]any) capture.Observation {
	actualID := jsonID(object["id"])
	requestID := actualID
	batch, known := o.batches[actualID]
	if known {
		requestID = o.requestID
	}
	ack := capture.ACKObservation{RequestID: requestID}
	if code, rejected := object["code"]; rejected {
		_, codeOK := code.(json.Number)
		_, messageOK := object["msg"].(string)
		if !codeOK || !messageOK || actualID == "" {
			ack.RequestID = actualID
		}
		return capture.Observation{Role: capture.MessageAcknowledgement, Schema: capture.SchemaAccepted, ACK: ack}
	}
	result, hasResult := object["result"]
	if !hasResult || result != nil || len(object) != 2 || actualID == "" || !known {
		return capture.Observation{Role: capture.MessageAcknowledgement, Schema: capture.SchemaAccepted, ACK: ack}
	}
	ack.Accepted = true
	ack.Subscriptions = slices.Clone(o.plan.Requests[batch].Streams)
	ack.FinalBatch = batch == len(o.plan.Requests)-1
	return capture.Observation{Role: capture.MessageAcknowledgement, Schema: capture.SchemaAccepted, ACK: ack}
}

func (o *derivativeRawObserver) validateStream(stream string, data []byte, receivedTimeNS int64, wrapper []byte) capture.SchemaDisposition {
	if o.plan.Product == DerivativeProductCoinM && strings.HasPrefix(stream, "!") {
		decisions, err := RouteCoinMMergedRecords(wrapper)
		if err != nil {
			return capture.SchemaMalformed
		}
		for _, decision := range decisions {
			if decision.Route == CoinMMergedRouteRejected {
				return capture.SchemaSemanticChanged
			}
		}
		return capture.SchemaAccepted
	}
	separator := strings.IndexByte(stream, '@')
	if separator <= 0 {
		return capture.SchemaUnknownRole
	}
	nativeSymbol := strings.ToUpper(stream[:separator])
	instrument := normalize.InstrumentIdentity{NativeID: nativeSymbol, BaseAssetID: "base", QuoteAssetID: "quote"}
	var err error
	if o.plan.Product == DerivativeProductUSDM {
		err = validateUSDMStreamPayload(stream[separator:], data, receivedTimeNS, instrument)
	} else {
		err = validateCoinMStreamPayload(stream[separator:], data, receivedTimeNS, instrument)
	}
	if err != nil {
		return capture.SchemaMalformed
	}
	return capture.SchemaAccepted
}

// derivativeSchemaValidationReceivedTime keeps clock offset out of schema
// classification. Raw capture retains the independently observed receive time;
// normalization still applies its stricter source-before-receive contract.
func derivativeSchemaValidationReceivedTime(raw []byte, receivedTimeNS int64) int64 {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return receivedTimeNS
	}
	var eventTimeMS int64
	if json.Unmarshal(object["E"], &eventTimeMS) != nil || eventTimeMS < 0 || eventTimeMS > (1<<63-1)/1_000_000 {
		return receivedTimeNS
	}
	sourceTimeNS := eventTimeMS * 1_000_000
	if sourceTimeNS > receivedTimeNS {
		return sourceTimeNS
	}
	return receivedTimeNS
}

func validateUSDMStreamPayload(suffix string, raw []byte, receivedTimeNS int64, instrument normalize.InstrumentIdentity) error {
	receivedTimeNS = derivativeSchemaValidationReceivedTime(raw, receivedTimeNS)
	switch suffix {
	case "@depth@100ms":
		event, err := ParseUSDMDepthUpdate(raw)
		if err == nil && event.Symbol != instrument.NativeID {
			err = fmt.Errorf("%w: depth symbol does not match subscribed stream", ErrUSDMInvalidMarketPayload)
		}
		return err
	case "@bookTicker":
		event, err := ParseUSDMBookTicker(raw)
		if err == nil && event.Symbol != instrument.NativeID {
			err = fmt.Errorf("%w: BBO symbol does not match subscribed stream", ErrUSDMInvalidMarketPayload)
		}
		return err
	case "@aggTrade":
		event, err := ParseUSDMAggregateTrade(raw)
		if err == nil && event.Symbol != instrument.NativeID {
			err = fmt.Errorf("%w: trade symbol does not match subscribed stream", ErrUSDMInvalidMarketPayload)
		}
		return err
	case "@ticker":
		event, err := ParseUSDMTicker24h(raw)
		if err == nil && event.Symbol != instrument.NativeID {
			err = fmt.Errorf("%w: ticker symbol does not match subscribed stream", ErrUSDMInvalidMarketPayload)
		}
		return err
	case "@markPrice@1s":
		_, err := ParseUSDMDerivativeTicker(raw, receivedTimeNS, instrument)
		return err
	case "@indexPrice":
		_, err := ParseUSDMIndexPriceUpdate(raw, receivedTimeNS, instrument)
		return err
	case "@forceOrder":
		event, err := ParseUSDMLiquidation(raw)
		if err == nil && event.Symbol != instrument.NativeID {
			err = fmt.Errorf("%w: liquidation symbol does not match subscribed stream", ErrUSDMInvalidMarketPayload)
		}
		return err
	default:
		return ErrUSDMUnknownStream
	}
}

func validateCoinMStreamPayload(suffix string, raw []byte, receivedTimeNS int64, instrument normalize.InstrumentIdentity) error {
	receivedTimeNS = derivativeSchemaValidationReceivedTime(raw, receivedTimeNS)
	switch suffix {
	case "@depth@100ms":
		_, err := ParseCoinMDepthUpdate(raw, instrument)
		return err
	case "@bookTicker":
		_, err := ParseCoinMBookTicker(raw, instrument)
		return err
	case "@aggTrade":
		_, err := ParseCoinMAggregateTrade(raw, instrument)
		return err
	case "@ticker":
		_, err := ParseCoinMTicker24h(raw, instrument)
		return err
	case "@markPrice@1s":
		_, err := ParseCoinMDerivativeTicker(raw, receivedTimeNS, instrument)
		return err
	default:
		return ErrCoinMUnknownStream
	}
}
