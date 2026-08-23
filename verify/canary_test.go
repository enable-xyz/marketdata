package verify

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/capture"
)

type fakeCanaryClock struct {
	mu     sync.Mutex
	manual *capture.ManualClock
}

func newFakeCanaryClock(t *testing.T) *fakeCanaryClock {
	t.Helper()
	manual, err := capture.NewManualClock(1_780_000_000_000_000_000, "canary-test")
	if err != nil {
		t.Fatal(err)
	}
	return &fakeCanaryClock{manual: manual}
}

func (c *fakeCanaryClock) Read() capture.ClockReading { return c.manual.Read() }
func (c *fakeCanaryClock) NewTimer(afterNS uint64) (capture.Timer, error) {
	return c.manual.NewTimer(afterNS)
}
func (c *fakeCanaryClock) WaitUntil(ctx context.Context, target uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.advanceTo(target)
}
func (c *fakeCanaryClock) advanceTo(target uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.Read().MonotonicNS
	if target <= now {
		return nil
	}
	return c.manual.Advance(target - now)
}

type fakeCanaryScheduledEvent struct {
	at    uint64
	event CanaryEvent
	err   error
}

type fakeCanarySpec struct {
	noACK          bool
	badACK         bool
	noHeartbeatACK bool
	rotationNS     uint64
	events         []fakeCanaryScheduledEvent
}

type fakeCanaryBarrier struct {
	mu        sync.Mutex
	remaining int
	done      chan struct{}
}

func newFakeCanaryBarrier(participants int) *fakeCanaryBarrier {
	return &fakeCanaryBarrier{remaining: participants, done: make(chan struct{})}
}

func (b *fakeCanaryBarrier) arriveAndWait(ctx context.Context) error {
	b.mu.Lock()
	b.remaining--
	if b.remaining == 0 {
		close(b.done)
	}
	done := b.done
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

type fakeCanaryDialer struct {
	mu                sync.Mutex
	clock             *fakeCanaryClock
	specs             []fakeCanarySpec
	dials             int
	subscribe         int
	inventories       [][]string
	beforeACKBarrier  *fakeCanaryBarrier
	afterACKBarrier   *fakeCanaryBarrier
	synchronizeRoutes bool
}

func (d *fakeCanaryDialer) Dial(_ context.Context, config CanaryConfig) (CanaryConnection, error) {
	expected, err := canarySubscriptions(config)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	spec := fakeCanarySpec{}
	if d.dials < len(d.specs) {
		spec = d.specs[d.dials]
	}
	if spec.rotationNS == 0 && config.Selector == CanarySelectorBinanceSpot {
		spec.rotationNS = 24 * uint64(time.Hour)
	}
	d.dials++
	d.inventories = append(d.inventories, slices.Clone(expected))
	if d.synchronizeRoutes && d.beforeACKBarrier == nil {
		d.beforeACKBarrier = newFakeCanaryBarrier(2)
		d.afterACKBarrier = newFakeCanaryBarrier(2)
	}
	beforeACK, afterACK := d.beforeACKBarrier, d.afterACKBarrier
	d.mu.Unlock()
	return &fakeCanaryConnection{
		clock: d.clock, expected: expected, spec: spec,
		beforeACKBarrier: beforeACK,
		afterACKBarrier:  afterACK,
		onSubscribe: func() {
			d.mu.Lock()
			d.subscribe++
			d.mu.Unlock()
		},
	}, nil
}

type fakeCanaryConnection struct {
	clock            *fakeCanaryClock
	expected         []string
	spec             fakeCanarySpec
	beforeACKBarrier *fakeCanaryBarrier
	afterACKBarrier  *fakeCanaryBarrier
	onSubscribe      func()
	subscribed       bool
	ackDelivered     bool
	afterACKReleased bool
	heartbeatPending bool
	closed           bool
}

func (c *fakeCanaryConnection) Subscriptions() []string { return slices.Clone(c.expected) }
func (c *fakeCanaryConnection) Subscribe(context.Context) error {
	if c.subscribed || c.closed {
		return errors.New("fake: invalid subscribe transition")
	}
	c.subscribed = true
	c.onSubscribe()
	return nil
}
func (c *fakeCanaryConnection) Read(ctx context.Context, deadline uint64) (CanaryEvent, error) {
	if err := ctx.Err(); err != nil {
		return CanaryEvent{}, err
	}
	if c.closed || !c.subscribed {
		return CanaryEvent{}, errors.New("fake: invalid read transition")
	}
	if !c.ackDelivered && !c.spec.noACK {
		if c.beforeACKBarrier != nil {
			if err := c.beforeACKBarrier.arriveAndWait(ctx); err != nil {
				return CanaryEvent{}, err
			}
		}
		c.ackDelivered = true
		identities := slices.Clone(c.expected)
		if c.spec.badACK {
			identities = []string{"wrong-stream"}
		}
		return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: []byte("ack"), ACKIdentities: identities}, nil
	}
	if c.afterACKBarrier != nil && !c.afterACKReleased {
		if err := c.afterACKBarrier.arriveAndWait(ctx); err != nil {
			return CanaryEvent{}, err
		}
		c.afterACKReleased = true
	}
	if c.heartbeatPending && !c.spec.noHeartbeatACK {
		c.heartbeatPending = false
		return CanaryEvent{Kind: CanaryEventHeartbeatACK, Payload: []byte("pong")}, nil
	}
	if len(c.spec.events) > 0 && c.spec.events[0].at <= deadline {
		next := c.spec.events[0]
		c.spec.events = c.spec.events[1:]
		if err := c.clock.advanceTo(next.at); err != nil {
			return CanaryEvent{}, err
		}
		return next.event, next.err
	}
	if err := c.clock.advanceTo(deadline); err != nil {
		return CanaryEvent{}, err
	}
	return CanaryEvent{}, ErrCanaryDeadline
}
func (c *fakeCanaryConnection) Heartbeat(context.Context, uint64) error {
	if c.heartbeatPending || c.closed {
		return errors.New("fake: invalid heartbeat transition")
	}
	c.heartbeatPending = true
	return nil
}
func (*fakeCanaryConnection) HeartbeatMode() CanaryHeartbeatMode { return CanaryHeartbeatActive }
func (c *fakeCanaryConnection) RotationIntervalNS() uint64       { return c.spec.rotationNS }
func (c *fakeCanaryConnection) Close() error {
	c.closed = true
	return nil
}

func canaryTestConfig(t *testing.T, selector, instrument, dex string, dialer *fakeCanaryDialer) CanaryConfig {
	t.Helper()
	return CanaryConfig{
		Selector:        selector,
		Instrument:      instrument,
		DEX:             dex,
		DurationNS:      26 * uint64(time.Hour),
		Reconnect:       CanaryReconnectPolicy{MaxAttempts: 2, BackoffNS: uint64(time.Second)},
		MaxMessageBytes: 1024,
		Heartbeat: CanaryHeartbeatSchedule{
			IntervalNS:   7 * uint64(time.Hour),
			TimeoutNS:    uint64(time.Minute),
			ACKTimeoutNS: uint64(time.Minute),
		},
		Clock: dialer.clock,
		Dial:  dialer.Dial,
	}
}

func TestCanaryDispatcherRunsEveryPublicSelectorForSimulated26Hours(t *testing.T) {
	tests := []struct {
		selector   string
		instrument string
		dex        string
	}{
		{CanarySelectorBinanceSpot, "BTCUSDT", ""},
		{CanarySelectorBinanceUSDMPublic, "BTCUSDT", ""},
		{CanarySelectorBinanceUSDMMarket, "BTCUSDT", ""},
		{CanarySelectorBinanceCoinM, "BTCUSD_PERP", ""},
		{CanarySelectorBybitSpot, "BTCUSDT", ""},
		{CanarySelectorBybitLinear, "BTCUSDT", ""},
		{CanarySelectorBybitInverse, "BTCUSD", ""},
		{CanarySelectorBybitOption, "BTC-30DEC30-50000-C", ""},
		{CanarySelectorOKXSpot, "BTC-USDT", ""},
		{CanarySelectorOKXSwap, "BTC-USDT-SWAP", ""},
		{CanarySelectorOKXFutures, "BTC-USDT-260925", ""},
		{CanarySelectorOKXOption, "BTC-USD-260925-50000-C", ""},
		{CanarySelectorDeribitPublic100MS, "BTC-PERPETUAL", ""},
		{CanarySelectorHyperliquidMain, "BTC", ""},
		{CanarySelectorHyperliquidSpot, "@1", ""},
		{CanarySelectorHyperliquidHIP3, "BTC", "xyz"},
	}
	for _, test := range tests {
		t.Run(test.selector, func(t *testing.T) {
			clock := newFakeCanaryClock(t)
			dialer := &fakeCanaryDialer{clock: clock}
			config := canaryTestConfig(t, test.selector, test.instrument, test.dex, dialer)
			receipt, err := RunCanary(t.Context(), config)
			if err != nil {
				t.Fatalf("RunCanary() error = %v", err)
			}
			if receipt.TerminalReason != CanaryTerminalPlannedDuration || receipt.DurationNS != 26*uint64(time.Hour) {
				t.Fatalf("terminal = %q duration = %d", receipt.TerminalReason, receipt.DurationNS)
			}
			if receipt.SubscriptionsRequested == 0 || receipt.SubscriptionsACKed != receipt.SubscriptionsRequested {
				t.Fatalf("subscriptions requested/acked = %d/%d", receipt.SubscriptionsRequested, receipt.SubscriptionsACKed)
			}
			if receipt.HeartbeatsSent == 0 || receipt.HeartbeatsACKed != receipt.HeartbeatsSent {
				t.Fatalf("heartbeats sent/acked = %d/%d", receipt.HeartbeatsSent, receipt.HeartbeatsACKed)
			}
			if len(receipt.UnexplainedIntervals) != 0 {
				t.Fatalf("unexplained intervals = %#v", receipt.UnexplainedIntervals)
			}
			if err := ValidateCanaryReceipt(receipt); err != nil {
				t.Fatalf("ValidateCanaryReceipt() error = %v", err)
			}
			if test.selector == CanarySelectorBinanceSpot {
				if receipt.PlannedRotations != 1 || dialer.dials != 2 || len(receipt.ExplainedIntervals) != 1 {
					t.Fatalf("Binance rotation receipt = %#v, dials = %d", receipt, dialer.dials)
				}
			}
			if test.selector == CanarySelectorDeribitPublic100MS && !slices.Contains(receipt.Limitations, DeribitRaw1MSLimitation) {
				t.Fatalf("Deribit limitations = %#v", receipt.Limitations)
			}
		})
	}
}

func TestBinanceUSDMAggregateCanaryBindsBothRoutesToOneInterval(t *testing.T) {
	clock := newFakeCanaryClock(t)
	dialer := &fakeCanaryDialer{clock: clock, synchronizeRoutes: true}
	config := canaryTestConfig(t, CanarySelectorBinanceUSDM, "BTCUSDT", "", dialer)
	config.DurationNS = 1
	receipt, err := RunCanary(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Selector != CanarySelectorBinanceUSDM || receipt.DurationNS != 1 || len(receipt.Routes) != 2 {
		t.Fatalf("aggregate receipt = %#v", receipt)
	}
	if receipt.Routes[0].Selector != CanarySelectorBinanceUSDMMarket || receipt.Routes[1].Selector != CanarySelectorBinanceUSDMPublic {
		t.Fatalf("aggregate routes = %#v", receipt.Routes)
	}
	if err := ValidateCanaryReceipt(receipt); err != nil {
		t.Fatal(err)
	}
}

func TestCanaryACKAndHeartbeatTimeoutsFailClosed(t *testing.T) {
	tests := []struct {
		name   string
		spec   fakeCanarySpec
		reason CanaryTerminalReason
	}{
		{"ack timeout", fakeCanarySpec{noACK: true}, CanaryTerminalACKTimeout},
		{"heartbeat timeout", fakeCanarySpec{noHeartbeatACK: true}, CanaryTerminalHeartbeatTimeout},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newFakeCanaryClock(t)
			dialer := &fakeCanaryDialer{clock: clock, specs: []fakeCanarySpec{test.spec}}
			config := canaryTestConfig(t, CanarySelectorBybitSpot, "BTCUSDT", "", dialer)
			config.Heartbeat.IntervalNS = uint64(time.Minute)
			config.Heartbeat.TimeoutNS = uint64(time.Second)
			receipt, err := RunCanary(t.Context(), config)
			var terminal *CanaryError
			if !errors.As(err, &terminal) || terminal.Reason != test.reason || receipt.TerminalReason != test.reason {
				t.Fatalf("receipt terminal = %q error = %v", receipt.TerminalReason, err)
			}
			if len(receipt.UnexplainedIntervals) != 1 || ValidateCanaryReceipt(receipt) != nil {
				t.Fatalf("invalid failure receipt = %#v", receipt)
			}
		})
	}
}

func TestCanaryACKMismatchFailsClosed(t *testing.T) {
	clock := newFakeCanaryClock(t)
	dialer := &fakeCanaryDialer{clock: clock, specs: []fakeCanarySpec{{badACK: true}}}
	receipt, err := RunCanary(t.Context(), canaryTestConfig(t, CanarySelectorOKXSpot, "BTC-USDT", "", dialer))
	var terminal *CanaryError
	if !errors.As(err, &terminal) || terminal.Reason != CanaryTerminalACKMismatch || receipt.TerminalReason != CanaryTerminalACKMismatch {
		t.Fatalf("receipt terminal = %q error = %v", receipt.TerminalReason, err)
	}
	if len(receipt.UnexplainedIntervals) != 1 || ValidateCanaryReceipt(receipt) != nil {
		t.Fatalf("invalid mismatch receipt = %#v", receipt)
	}
}

func TestCanaryReconnectUsesExactResubscriptionAndExplainsGap(t *testing.T) {
	clock := newFakeCanaryClock(t)
	disconnectAt := uint64(time.Hour)
	dialer := &fakeCanaryDialer{clock: clock, specs: []fakeCanarySpec{
		{events: []fakeCanaryScheduledEvent{{at: disconnectAt, event: CanaryEvent{Kind: CanaryEventDisconnect, Payload: []byte("disconnect")}}}},
		{},
	}}
	config := canaryTestConfig(t, CanarySelectorBybitLinear, "BTCUSDT", "", dialer)
	receipt, err := RunCanary(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if dialer.dials != 2 || dialer.subscribe != 2 || len(dialer.inventories) != 2 || !slices.Equal(dialer.inventories[0], dialer.inventories[1]) {
		t.Fatalf("dials=%d subscribe=%d inventories=%#v", dialer.dials, dialer.subscribe, dialer.inventories)
	}
	if receipt.Reconnects != 1 || receipt.SubscriptionsRequested != 2 || receipt.SubscriptionsACKed != 2 || len(receipt.ExplainedIntervals) != 1 || len(receipt.UnexplainedIntervals) != 0 {
		t.Fatalf("reconnect receipt = %#v", receipt)
	}
	if err := ValidateCanaryReceipt(receipt); err != nil {
		t.Fatal(err)
	}
}

func TestCanaryExhaustedReconnectBudgetFailsClosed(t *testing.T) {
	clock := newFakeCanaryClock(t)
	dialer := &fakeCanaryDialer{clock: clock, specs: []fakeCanarySpec{
		{events: []fakeCanaryScheduledEvent{{at: uint64(time.Hour), event: CanaryEvent{Kind: CanaryEventDisconnect}}}},
		{events: []fakeCanaryScheduledEvent{{at: 2 * uint64(time.Hour), event: CanaryEvent{Kind: CanaryEventDisconnect}}}},
	}}
	config := canaryTestConfig(t, CanarySelectorBybitSpot, "BTCUSDT", "", dialer)
	config.Reconnect.MaxAttempts = 1
	receipt, err := RunCanary(t.Context(), config)
	var terminal *CanaryError
	if !errors.As(err, &terminal) || terminal.Reason != CanaryTerminalReconnectExhausted || receipt.Reconnects != 2 || dialer.dials != 2 {
		t.Fatalf("receipt = %#v error = %v dials = %d", receipt, err, dialer.dials)
	}
	if len(receipt.UnexplainedIntervals) != 1 || ValidateCanaryReceipt(receipt) != nil {
		t.Fatal("reconnect exhaustion receipt did not validate")
	}
}

func TestCanaryUnknownStreamRecordsUnexplainedInterval(t *testing.T) {
	clock := newFakeCanaryClock(t)
	dialer := &fakeCanaryDialer{clock: clock, specs: []fakeCanarySpec{{events: []fakeCanaryScheduledEvent{{
		at:    uint64(time.Hour),
		event: CanaryEvent{Kind: CanaryEventMessage, Payload: []byte("unknown"), StreamIdentity: "other-stream"},
	}}}}}
	receipt, err := RunCanary(t.Context(), canaryTestConfig(t, CanarySelectorDeribitPublic100MS, "BTC-PERPETUAL", "", dialer))
	var terminal *CanaryError
	if !errors.As(err, &terminal) || terminal.Reason != CanaryTerminalUnknownStream || len(receipt.UnexplainedIntervals) != 1 {
		t.Fatalf("receipt = %#v error = %v", receipt, err)
	}
	if ValidateCanaryReceipt(receipt) != nil {
		t.Fatal("unknown-stream receipt did not validate")
	}
}

func TestCanaryMessageByteCapFailsBeforeInterpretation(t *testing.T) {
	clock := newFakeCanaryClock(t)
	dialer := &fakeCanaryDialer{clock: clock, specs: []fakeCanarySpec{{events: []fakeCanaryScheduledEvent{{
		at:    uint64(time.Hour),
		event: CanaryEvent{Kind: CanaryEventMessage, Payload: []byte("12345"), StreamIdentity: "publicTrade.BTCUSDT"},
	}}}}}
	config := canaryTestConfig(t, CanarySelectorBybitSpot, "BTCUSDT", "", dialer)
	config.MaxMessageBytes = 4
	receipt, err := RunCanary(t.Context(), config)
	var terminal *CanaryError
	if !errors.As(err, &terminal) || terminal.Reason != CanaryTerminalOversize || receipt.Bytes != 8 {
		t.Fatalf("receipt = %#v error = %v", receipt, err)
	}
	if len(receipt.UnexplainedIntervals) != 1 || ValidateCanaryReceipt(receipt) != nil {
		t.Fatal("oversize failure receipt did not validate")
	}
}

func TestCanaryCallerCancellationClassification(t *testing.T) {
	clock := newFakeCanaryClock(t)
	dialer := &fakeCanaryDialer{clock: clock}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	receipt, err := RunCanary(ctx, canaryTestConfig(t, CanarySelectorHyperliquidMain, "BTC", "", dialer))
	var terminal *CanaryError
	if !errors.As(err, &terminal) || terminal.Reason != CanaryTerminalCallerCanceled || receipt.TerminalReason != CanaryTerminalCallerCanceled || dialer.dials != 0 {
		t.Fatalf("receipt = %#v error = %v dials = %d", receipt, err, dialer.dials)
	}
	if ValidateCanaryReceipt(receipt) != nil {
		t.Fatal("cancellation receipt did not validate")
	}
}

func TestCanaryReceiptHashIsDeterministicAndValidated(t *testing.T) {
	receipt := CanaryReceipt{
		Version:                CanaryReceiptVersion,
		Selector:               CanarySelectorBybitSpot,
		StartedAtUTCNS:         100,
		EndedAtUTCNS:           200,
		DurationNS:             100,
		Messages:               2,
		Bytes:                  8,
		RollingSHA256:          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SubscriptionsRequested: 1,
		SubscriptionsACKed:     1,
		HeartbeatsSent:         1,
		HeartbeatsACKed:        1,
		ExplainedIntervals:     []CanaryInterval{{StartedAtUTCNS: 120, EndedAtUTCNS: 130, Reason: "bounded reconnect"}},
		TerminalReason:         CanaryTerminalPlannedDuration,
	}
	first := CanaryReceiptSHA256(receipt)
	second := CanaryReceiptSHA256(receipt)
	if first == "" || first != second {
		t.Fatalf("hashes = %q %q", first, second)
	}
	receipt.ReceiptSHA256 = first
	if err := ValidateCanaryReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Bytes++
	if err := ValidateCanaryReceipt(receipt); !errors.Is(err, ErrCanaryConfiguration) {
		t.Fatalf("mutated receipt error = %v", err)
	}
}

func TestCanaryRejectsMissingHIP3DEXAndCredentialGatedDeribitRaw(t *testing.T) {
	clock := newFakeCanaryClock(t)
	dialer := &fakeCanaryDialer{clock: clock}
	config := canaryTestConfig(t, CanarySelectorHyperliquidHIP3, "BTC", "", dialer)
	if _, err := RunCanary(t.Context(), config); !errors.Is(err, ErrCanaryConfiguration) {
		t.Fatalf("missing HIP-3 DEX error = %v", err)
	}
	if _, err := RunDeribitCanary(t.Context(), CanaryConfig{Selector: "deribit-v2-public-raw"}); !errors.Is(err, ErrCanaryUnsupported) || !stringsContains(err.Error(), DeribitRaw1MSLimitation) {
		t.Fatalf("raw Deribit error = %v", err)
	}
}

func TestDeribitCanaryClassifiesPublicTestResponseAsControl(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`),
		[]byte(`{"jsonrpc":"2.0","id":1000,"result":"ok"}`),
		[]byte(`{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"heartbeat"}}`),
	} {
		event, err := classifyDeribitCanaryEnvelope(payload, 1)
		if err != nil || event.Kind != CanaryEventControl {
			t.Fatalf("classifyDeribitCanaryEnvelope(%s) = %#v, %v", payload, event, err)
		}
	}
	payload := []byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.100ms","data":[]}}`)
	event, err := classifyDeribitCanaryEnvelope(payload, 1)
	if err != nil || event.Kind != CanaryEventMessage || event.StreamIdentity != "trades.BTC-PERPETUAL.100ms" {
		t.Fatalf("subscription event = %#v, %v", event, err)
	}
	if _, err := classifyDeribitCanaryEnvelope([]byte(`{"jsonrpc":"2.0","id":99,"result":"unexpected"}`), 1); !errors.Is(err, errCanaryUnknownStream) {
		t.Fatalf("unknown response error = %v", err)
	}
}

func TestCanaryAsyncReaderDeadlineDoesNotCancelSocketRead(t *testing.T) {
	clock := newFakeCanaryClock(t)
	input := make(chan int, 1)
	canceled := make(chan struct{})
	reader := newCanaryAsyncReader(func(ctx context.Context) (int, error) {
		select {
		case value := <-input:
			return value, nil
		case <-ctx.Done():
			close(canceled)
			return 0, ctx.Err()
		}
	})

	now := clock.Read().MonotonicNS
	if _, err := reader.Read(t.Context(), clock, now); !errors.Is(err, ErrCanaryDeadline) {
		t.Fatalf("expired deadline error = %v", err)
	}
	select {
	case <-canceled:
		t.Fatal("expired deadline canceled the persistent socket read")
	default:
	}

	input <- 42
	value, err := reader.Read(t.Context(), clock, boundedAdd(now, uint64(time.Hour)))
	if err != nil || value != 42 {
		t.Fatalf("read after deadline = %d, %v", value, err)
	}

	reader.Close()
	select {
	case <-canceled:
	case <-t.Context().Done():
		t.Fatal("reader close did not cancel the persistent socket read")
	}
}

func TestBinanceSpotCanaryClassifiesCurrentBookTickerShape(t *testing.T) {
	tests := []struct {
		payload  string
		identity string
	}{
		{`{"e":"trade","s":"BTCUSDT"}`, "btcusdt@trade"},
		{`{"e":"trade","E":1787492500000,"s":"BTCUSDT","t":1,"p":"77490","q":"0.001","b":42,"a":43,"T":1787492500000,"m":false,"M":true}`, "btcusdt@trade"},
		{`{"e":"depthUpdate","s":"BTCUSDT"}`, "btcusdt@depth@100ms"},
		{`{"e":"24hrTicker","s":"BTCUSDT"}`, "btcusdt@ticker"},
		{`{"u":98982176124,"s":"BTCUSDT","b":"77458.59000000","B":"0.38412000","a":"77458.60000000","A":"2.04782000"}`, "btcusdt@bookTicker"},
	}
	for _, test := range tests {
		identity, err := classifyBinanceSpotCanaryStream([]byte(test.payload))
		if err != nil || identity != test.identity {
			t.Fatalf("classify %s = %q, %v", test.payload, identity, err)
		}
	}
	for _, payload := range []string{
		`{"s":"BTCUSDT","b":"1","a":"2"}`,
		`{"e":"newEvent","s":"BTCUSDT"}`,
		`{"e":"trade"}`,
	} {
		if _, err := classifyBinanceSpotCanaryStream([]byte(payload)); !errors.Is(err, errCanaryUnknownStream) {
			t.Fatalf("invalid payload %s error = %v", payload, err)
		}
	}
}

func TestBybitCanaryClassifiesCurrentOptionACKAndHeartbeat(t *testing.T) {
	clock := newFakeCanaryClock(t)
	readEvent := func(payload string, connection *bybitCanaryConnection) CanaryEvent {
		t.Helper()
		connection.clock = clock
		connection.reader = newCanaryAsyncReader(func(context.Context) ([]byte, error) {
			return []byte(payload), nil
		})
		defer connection.reader.Close()
		event, err := connection.Read(t.Context(), boundedAdd(clock.Read().MonotonicNS, uint64(time.Second)))
		if err != nil {
			t.Fatalf("Read(%s) error = %v", payload, err)
		}
		return event
	}

	topic := "orderbook.25.BTC-25JUN27-160000-P-USDT"
	optionACK := `{"success":true,"conn_id":"option-connection","data":{"failTopics":[],"successTopics":["` + topic + `"]},"type":"COMMAND_RESP"}`
	event := readEvent(optionACK, &bybitCanaryConnection{option: true})
	if event.Kind != CanaryEventSubscriptionACK || !slices.Equal(event.ACKIdentities, []string{topic}) {
		t.Fatalf("option ACK event = %#v", event)
	}

	pong := `{"success":true,"ret_msg":"pong","conn_id":"public-connection","req_id":"canary-ping-7","op":"ping"}`
	event = readEvent(pong, &bybitCanaryConnection{pendingPingID: "canary-ping-7"})
	if event.Kind != CanaryEventHeartbeatACK {
		t.Fatalf("heartbeat event = %#v", event)
	}

	optionPong := `{"args":["1787493211670"],"op":"pong"}`
	event = readEvent(optionPong, &bybitCanaryConnection{option: true, pendingPingID: "canary-ping-8"})
	if event.Kind != CanaryEventHeartbeatACK {
		t.Fatalf("option heartbeat event = %#v", event)
	}
}

func TestBinanceDerivativeStepPayloadPrefersDataOverTerminalControl(t *testing.T) {
	data := []byte(`{"stream":"btcusdt@forceOrder","data":{"e":"forceOrder"}}`)
	terminal := []byte(`{"reason":"schema_rejected"}`)
	result := capture.StepResult{Envelopes: []capture.EnvelopeV1{
		{RecordKind: capture.RecordKindWebSocket, RawPayload: data},
		{RecordKind: capture.RecordKindControl, RawPayload: terminal},
	}}
	got := binanceDerivativeStepPayload(result)
	if !slices.Equal(got, data) {
		t.Fatalf("step payload = %s, want data envelope %s", got, data)
	}
	got[0] = 'x'
	if data[0] != '{' {
		t.Fatal("step payload aliases the capture envelope")
	}
}

func stringsContains(value, substring string) bool {
	for index := 0; index+len(substring) <= len(value); index++ {
		if value[index:index+len(substring)] == substring {
			return true
		}
	}
	return false
}
