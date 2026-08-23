package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

type derivativeTestWrite struct {
	kind    DerivativeWSWriteKind
	payload []byte
}

type derivativeTestConnection struct {
	frames         []DerivativeWSFrame
	next           int
	writes         []derivativeTestWrite
	closeReasons   []capture.CloseReason
	allowOversized bool
}

func (c *derivativeTestConnection) Read(ctx context.Context, maximum uint32) (DerivativeWSFrame, error) {
	if err := ctx.Err(); err != nil {
		return DerivativeWSFrame{}, err
	}
	if c.next == len(c.frames) {
		return DerivativeWSFrame{}, io.EOF
	}
	frame := c.frames[c.next]
	c.next++
	if !c.allowOversized && len(frame.Payload) > int(maximum) {
		return DerivativeWSFrame{}, ErrDerivativeBounds
	}
	frame.Payload = slices.Clone(frame.Payload)
	return frame, nil
}

func (c *derivativeTestConnection) ReadBuffered(ctx context.Context, maximum uint32) (DerivativeWSFrame, bool, error) {
	if err := ctx.Err(); err != nil {
		return DerivativeWSFrame{}, false, err
	}
	if c.next == len(c.frames) {
		return DerivativeWSFrame{}, false, nil
	}
	frame, err := c.Read(ctx, maximum)
	return frame, err == nil, err
}

func (c *derivativeTestConnection) Write(ctx context.Context, kind DerivativeWSWriteKind, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writes = append(c.writes, derivativeTestWrite{kind: kind, payload: slices.Clone(payload)})
	return nil
}

func (c *derivativeTestConnection) Close(ctx context.Context, reason capture.CloseReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.closeReasons = append(c.closeReasons, reason)
	return nil
}

type derivativeTestConnector struct {
	connections []*derivativeTestConnection
	requests    []DerivativeWSConnectRequest
	next        int
}

func (c *derivativeTestConnector) Connect(ctx context.Context, request DerivativeWSConnectRequest) (DerivativeWSConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.requests = append(c.requests, request)
	if c.next == len(c.connections) {
		return nil, errors.New("scripted derivative connector exhausted")
	}
	connection := c.connections[c.next]
	c.next++
	return connection, nil
}

type derivativeTestSink struct {
	records []capture.EnvelopeV1
	commits []capture.EpochCommit
	closes  []capture.EpochClose
}

func (s *derivativeTestSink) WriteRaw(ctx context.Context, envelope capture.EnvelopeV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	envelope.RawPayload = slices.Clone(envelope.RawPayload)
	envelope.Extensions = slices.Clone(envelope.Extensions)
	s.records = append(s.records, envelope)
	return nil
}

func (s *derivativeTestSink) Commit(_ context.Context, commit capture.EpochCommit) error {
	s.commits = append(s.commits, commit)
	return nil
}

func (s *derivativeTestSink) CloseEpoch(_ context.Context, closeRecord capture.EpochClose) error {
	s.closes = append(s.closes, closeRecord)
	return nil
}

func (s *derivativeTestSink) hasPayload(payload []byte) bool {
	return slices.ContainsFunc(s.records, func(record capture.EnvelopeV1) bool {
		return bytes.Equal(record.RawPayload, payload)
	})
}

func TestDerivativeCaptureHappyPathPreservesEveryRawPayload(t *testing.T) {
	tests := []struct {
		name     string
		product  DerivativeProduct
		endpoint string
		symbol   string
		payloads map[string][]byte
	}{
		{
			name: "USD-M public route", product: DerivativeProductUSDM, endpoint: USDMPublicEndpoint, symbol: "BTCUSDT",
			payloads: map[string][]byte{
				"btcusdt@depth@100ms": derivativeFixture(t, "usdm", "official/depth_update.json"),
				"btcusdt@bookTicker":  derivativeFixture(t, "usdm", "official/book_ticker.json"),
			},
		},
		{
			name: "USD-M market route", product: DerivativeProductUSDM, endpoint: USDMMarketEndpoint, symbol: "BTCUSDT",
			payloads: map[string][]byte{
				"btcusdt@aggTrade":     derivativeFixture(t, "usdm", "official/agg_trade.json"),
				"btcusdt@ticker":       derivativeFixture(t, "usdm", "official/ticker.json"),
				"btcusdt@markPrice@1s": derivativeFixture(t, "usdm", "official/mark_price.json"),
				"btcusdt@indexPrice":   derivativeFixture(t, "usdm", "official/index_price.json"),
				"btcusdt@forceOrder":   derivativeFixture(t, "usdm", "official/liquidation.json"),
			},
		},
		{
			name: "COIN-M symbol and merged routes", product: DerivativeProductCoinM, endpoint: CoinMWebSocketEndpoint, symbol: "BTCUSD_PERP",
			payloads: map[string][]byte{
				"btcusd_perp@aggTrade":     derivativeFixture(t, "coinm", "official/agg_trade.json"),
				"btcusd_perp@depth@100ms":  derivativeFixture(t, "coinm", "official/depth_update.json"),
				"btcusd_perp@bookTicker":   derivativeFixture(t, "coinm", "official/book_ticker.json"),
				"btcusd_perp@ticker":       derivativeFixture(t, "coinm", "official/ticker.json"),
				"btcusd_perp@markPrice@1s": bytes.ReplaceAll(derivativeFixture(t, "coinm", "official/delivery_funding_empty.json"), []byte("BTCUSD_201225"), []byte("BTCUSD_PERP")),
				"!forceOrder@arr":          []byte(`[{"e":"forceOrder","s":"BTCUSD_PERP","ps":"BTCUSD","st":2},{"e":"forceOrder","s":"BTCUSDT","ps":"BTCUSDT","st":1}]`),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &derivativeTestConnection{}
			adapter, _, sink, connector := newDerivativeTestCapture(t, test.product, test.endpoint, []string{test.symbol}, []*derivativeTestConnection{connection}, 1)
			plan := adapter.SubscriptionPlan()
			connection.frames = append(connection.frames, derivativeACKFrames(plan)...)
			wrapped := make([][]byte, 0, len(plan.Inventory))
			for _, stream := range plan.Inventory {
				payload, ok := test.payloads[stream]
				if !ok {
					t.Fatalf("missing payload for planned stream %q", stream)
				}
				wrappedPayload := derivativeCombinedPayload(t, stream, payload)
				wrapped = append(wrapped, wrappedPayload)
				connection.frames = append(connection.frames, DerivativeWSFrame{Kind: DerivativeWSFrameText, Payload: wrappedPayload})
			}
			startDerivativeSubscribed(t, adapter, connection)
			for range wrapped {
				result := mustDerivativeStep(t, adapter)
				if len(result.Faults) != 0 {
					t.Fatalf("valid stream faulted: %+v", result)
				}
			}
			for _, payload := range wrapped {
				if !sink.hasPayload(payload) {
					t.Fatalf("raw combined payload was not delivered byte-for-byte: %q", payload)
				}
			}
			if len(connector.requests) != 1 || connector.requests[0].Product != test.product || connector.requests[0].Endpoint != test.endpoint {
				t.Fatalf("connect requests = %+v", connector.requests)
			}
		})
	}
}

func TestDerivativeACKReconciliationAndTimeoutFailClosed(t *testing.T) {
	tests := []struct {
		name       string
		frames     func(DerivativeSubscriptionPlan) []DerivativeWSFrame
		advance    uint64
		wantFault  capture.FaultKind
		wantReason capture.CloseReason
		wantKinds  []capture.ControlKind
	}{
		{
			name: "partial final batch",
			frames: func(plan DerivativeSubscriptionPlan) []DerivativeWSFrame {
				last := plan.Requests[len(plan.Requests)-1].ID
				return []DerivativeWSFrame{{Kind: DerivativeWSFrameText, Payload: []byte(`{"result":null,"id":` + strconv.FormatInt(last, 10) + `}`)}}
			},
			wantFault: capture.FaultACKPartial, wantReason: capture.CloseACKRejected, wantKinds: []capture.ControlKind{capture.ControlAcknowledgement, capture.ControlDisconnect},
		},
		{
			name: "mismatched request",
			frames: func(DerivativeSubscriptionPlan) []DerivativeWSFrame {
				return []DerivativeWSFrame{{Kind: DerivativeWSFrameText, Payload: []byte(`{"result":null,"id":999}`)}}
			},
			wantFault: capture.FaultACKWrong, wantReason: capture.CloseACKRejected, wantKinds: []capture.ControlKind{capture.ControlAcknowledgement, capture.ControlDisconnect},
		},
		{
			name: "timeout", frames: func(DerivativeSubscriptionPlan) []DerivativeWSFrame { return nil }, advance: DerivativeACKDeadlineNS + 1,
			wantFault: capture.FaultACKTimeout, wantReason: capture.CloseACKTimeout, wantKinds: []capture.ControlKind{capture.ControlTimeout, capture.ControlDisconnect},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &derivativeTestConnection{}
			adapter, clock, sink, _ := newDerivativeTestCapture(t, DerivativeProductUSDM, USDMMarketEndpoint, []string{"BTCUSDT"}, []*derivativeTestConnection{connection}, 1)
			connection.frames = test.frames(adapter.SubscriptionPlan())
			startDerivativeConnectedAndSubscribed(t, adapter, connection)
			if test.advance != 0 {
				if err := clock.Advance(test.advance); err != nil {
					t.Fatal(err)
				}
			}
			result := mustDerivativeStep(t, adapter)
			if !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == test.wantFault }) {
				t.Fatalf("faults = %+v, want %d", result.Faults, test.wantFault)
			}
			assertDerivativeControlKinds(t, result, test.wantKinds...)
			if len(sink.closes) != 1 || sink.closes[0].Reason != test.wantReason {
				t.Fatalf("close evidence = %+v, want reason %d", sink.closes, test.wantReason)
			}
		})
	}
}

func TestDerivativeHeartbeatAbruptDisconnectAndUnknownRole(t *testing.T) {
	t.Run("server ping is captured before exact pong", func(t *testing.T) {
		ping := []byte("server-heartbeat")
		connection := &derivativeTestConnection{}
		adapter, _, sink, _ := newDerivativeTestCapture(t, DerivativeProductCoinM, CoinMWebSocketEndpoint, []string{"BTCUSD_201225"}, []*derivativeTestConnection{connection}, 1)
		connection.frames = append(derivativeACKFrames(adapter.SubscriptionPlan()), DerivativeWSFrame{Kind: DerivativeWSFramePing, Payload: ping})
		startDerivativeSubscribed(t, adapter, connection)
		result := mustDerivativeStep(t, adapter)
		assertDerivativeControlKinds(t, result, capture.ControlHeartbeat)
		if !sink.hasPayload(ping) || len(connection.writes) == 0 {
			t.Fatalf("heartbeat evidence/writes = %v / %+v", sink.hasPayload(ping), connection.writes)
		}
		write := connection.writes[len(connection.writes)-1]
		if write.kind != DerivativeWSWritePong || !bytes.Equal(write.payload, ping) {
			t.Fatalf("pong write = %+v", write)
		}
	})

	t.Run("abrupt disconnect", func(t *testing.T) {
		connection := &derivativeTestConnection{}
		adapter, _, sink, _ := newDerivativeTestCapture(t, DerivativeProductUSDM, USDMMarketEndpoint, []string{"BTCUSDT"}, []*derivativeTestConnection{connection}, 1)
		connection.frames = derivativeACKFrames(adapter.SubscriptionPlan())
		startDerivativeSubscribed(t, adapter, connection)
		result := mustDerivativeStep(t, adapter)
		assertDerivativeControlKinds(t, result, capture.ControlDisconnect)
		if !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultDisconnectAbrupt }) || len(sink.closes) != 1 || sink.closes[0].Reason != capture.CloseAbrupt {
			t.Fatalf("abrupt result = %+v, closes = %+v", result, sink.closes)
		}
	})

	t.Run("unknown stream role is raw-first then rejected", func(t *testing.T) {
		unknown := derivativeCombinedPayload(t, "btcusdt@rpiDepth@500ms", []byte(`{"e":"depthUpdate"}`))
		connection := &derivativeTestConnection{}
		adapter, _, sink, _ := newDerivativeTestCapture(t, DerivativeProductUSDM, USDMMarketEndpoint, []string{"BTCUSDT"}, []*derivativeTestConnection{connection}, 1)
		connection.frames = append(derivativeACKFrames(adapter.SubscriptionPlan()), DerivativeWSFrame{Kind: DerivativeWSFrameText, Payload: unknown})
		startDerivativeSubscribed(t, adapter, connection)
		mustDerivativeStep(t, adapter)
		quarantine := mustDerivativeStep(t, adapter)
		assertDerivativeControlKinds(t, quarantine, capture.ControlParseQuarantine)
		if !sink.hasPayload(unknown) || !slices.ContainsFunc(quarantine.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultSchemaUnknownRole }) {
			t.Fatalf("unknown-role raw/fault = %v / %+v", sink.hasPayload(unknown), quarantine)
		}
		disconnected := mustDerivativeStep(t, adapter)
		assertDerivativeControlKinds(t, disconnected, capture.ControlDisconnect)
		if len(connection.closeReasons) == 0 || connection.closeReasons[0] != capture.CloseSchemaRejected {
			t.Fatalf("connection close reasons = %+v", connection.closeReasons)
		}
	})
}

func TestDerivativePlannedRotationReconnectsAndExhaustsExplicitEpochs(t *testing.T) {
	first := &derivativeTestConnection{}
	second := &derivativeTestConnection{}
	adapter, clock, sink, connector := newDerivativeTestCapture(t, DerivativeProductUSDM, USDMMarketEndpoint, []string{"BTCUSDT"}, []*derivativeTestConnection{first, second}, 2)
	firstEpochPayload := derivativeCombinedPayload(t, "btcusdt@ticker", derivativeFixture(t, "usdm", "official/ticker.json"))
	first.frames = append(derivativeACKFrames(adapter.SubscriptionPlan()), DerivativeWSFrame{Kind: DerivativeWSFrameText, Payload: firstEpochPayload})
	secondEpochPayload := derivativeCombinedPayload(t, "btcusdt@ticker", derivativeFixture(t, "usdm", "official/ticker.json"))
	second.frames = append(derivativeACKFrames(adapter.SubscriptionPlan()), DerivativeWSFrame{Kind: DerivativeWSFrameText, Payload: secondEpochPayload})
	startDerivativeSubscribed(t, adapter, first)
	if err := clock.Advance(DerivativeSocketLifetimeNS); err != nil {
		t.Fatal(err)
	}
	drained := mustDerivativeStep(t, adapter)
	if len(drained.Faults) != 0 || !sink.hasPayload(firstEpochPayload) {
		t.Fatalf("rotation drain raw delivery = %+v / %v", drained, sink.hasPayload(firstEpochPayload))
	}
	assertDerivativeControlKinds(t, drained)
	rotated := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, rotated, capture.ControlDisconnect)
	if len(sink.closes) != 1 || sink.closes[0].Reason != capture.ClosePlanned || len(first.closeReasons) != 1 || first.closeReasons[0] != capture.ClosePlanned {
		t.Fatalf("first rotation = %+v / %+v", sink.closes, first.closeReasons)
	}
	reconnect := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, reconnect, capture.ControlReconnect)
	attempt := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, attempt, capture.ControlConnectAttempt)
	connected := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, connected, capture.ControlConnected)
	subscribed := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, subscribed, capture.ControlSubscribeRequest)
	for range adapter.SubscriptionPlan().Requests {
		acknowledged := mustDerivativeStep(t, adapter)
		assertDerivativeControlKinds(t, acknowledged, capture.ControlAcknowledgement)
	}
	dataResult := mustDerivativeStep(t, adapter)
	if len(dataResult.Faults) != 0 || !sink.hasPayload(secondEpochPayload) {
		t.Fatalf("second-epoch raw delivery = %+v / %v", dataResult, sink.hasPayload(secondEpochPayload))
	}
	if len(connector.requests) != 2 || connector.requests[1].Product != DerivativeProductUSDM {
		t.Fatalf("reconnect requests = %+v", connector.requests)
	}
	if err := clock.Advance(DerivativeSocketLifetimeNS); err != nil {
		t.Fatal(err)
	}
	secondClose := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, secondClose, capture.ControlDisconnect)
	if len(sink.closes) != 2 || sink.closes[1].Reason != capture.ClosePlanned {
		t.Fatalf("second rotation closes = %+v", sink.closes)
	}
	if _, err := adapter.Step(t.Context()); !errors.Is(err, ErrDerivativeEpochExhausted) {
		t.Fatalf("exhausted epoch error = %v", err)
	}
}

func TestDerivativeOversizeFailsClosedWithoutTruncation(t *testing.T) {
	oversized := bytes.Repeat([]byte{'x'}, USDMMaxRawPayloadBytes+1)
	connection := &derivativeTestConnection{allowOversized: true}
	adapter, _, sink, _ := newDerivativeTestCapture(t, DerivativeProductUSDM, USDMMarketEndpoint, []string{"BTCUSDT"}, []*derivativeTestConnection{connection}, 1)
	connection.frames = append(derivativeACKFrames(adapter.SubscriptionPlan()), DerivativeWSFrame{Kind: DerivativeWSFrameText, Payload: oversized})
	startDerivativeSubscribed(t, adapter, connection)
	if _, err := adapter.Step(t.Context()); !errors.Is(err, ErrDerivativeBounds) {
		t.Fatalf("oversize error = %v", err)
	}
	if sink.hasPayload(oversized) || slices.ContainsFunc(sink.records, func(record capture.EnvelopeV1) bool {
		return len(record.RawPayload) != 0 && len(record.RawPayload) < len(oversized) && bytes.Equal(record.RawPayload, oversized[:len(record.RawPayload)])
	}) {
		t.Fatal("oversize payload was silently truncated or delivered past the declared bound")
	}
	if len(connection.closeReasons) == 0 || connection.closeReasons[0] != capture.CloseSchemaRejected {
		t.Fatalf("oversize close reasons = %+v", connection.closeReasons)
	}
	disconnected := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, disconnected, capture.ControlDisconnect)
}

func newDerivativeTestCapture(t *testing.T, product DerivativeProduct, endpoint string, symbols []string, connections []*derivativeTestConnection, epochCount int) (*DerivativeCapture, *capture.ManualClock, *derivativeTestSink, *derivativeTestConnector) {
	t.Helper()
	clock, err := capture.NewManualClock(2_000_000_000_000_000_000, "derivative-test-clock")
	if err != nil {
		t.Fatal(err)
	}
	contract := CoinMSourceContract()
	if product == DerivativeProductUSDM {
		contract = usdmWebSocketSourceContract(usdmRouteForEndpoint(endpoint))
	}
	budget, err := capture.NewTokenRateBudget(contract.Rate, 0)
	if err != nil {
		t.Fatal(err)
	}
	epochs := make([]capture.StreamEpoch, epochCount)
	for i := range epochCount {
		epochs[i] = capture.StreamEpoch{Kind: capture.EpochConnection, ID: [16]byte{byte(i + 1)}}
	}
	connector := &derivativeTestConnector{connections: connections}
	sink := &derivativeTestSink{}
	adapter, err := newDerivativeCapture(product, DerivativeWSConfig{
		Symbols: symbols, RecorderVersion: "derivative-test", Endpoint: endpoint, Epochs: epochs,
	}, connector, clock, budget, sink)
	if err != nil {
		t.Fatal(err)
	}
	return adapter, clock, sink, connector
}

func startDerivativeSubscribed(t *testing.T, adapter *DerivativeCapture, connection *derivativeTestConnection) {
	t.Helper()
	startDerivativeConnectedAndSubscribed(t, adapter, connection)
	for range adapter.SubscriptionPlan().Requests {
		result := mustDerivativeStep(t, adapter)
		if len(result.Faults) != 0 {
			t.Fatalf("ACK result = %+v", result)
		}
		assertDerivativeControlKinds(t, result, capture.ControlAcknowledgement)
	}
}

func startDerivativeConnectedAndSubscribed(t *testing.T, adapter *DerivativeCapture, connection *derivativeTestConnection) {
	t.Helper()
	result, err := adapter.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertDerivativeControlKinds(t, result, capture.ControlConnectAttempt)
	connected := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, connected, capture.ControlConnected)
	subscribed := mustDerivativeStep(t, adapter)
	assertDerivativeControlKinds(t, subscribed, capture.ControlSubscribeRequest)
	plan := adapter.SubscriptionPlan()
	if len(connection.writes) != len(plan.Requests) {
		t.Fatalf("subscription writes = %d, want %d", len(connection.writes), len(plan.Requests))
	}
	for i, request := range plan.Requests {
		if connection.writes[i].kind != DerivativeWSWriteText || !bytes.Equal(connection.writes[i].payload, request.Raw) {
			t.Fatalf("subscription write %d = %+v, want %q", i, connection.writes[i], request.Raw)
		}
	}
}

func derivativeACKFrames(plan DerivativeSubscriptionPlan) []DerivativeWSFrame {
	frames := make([]DerivativeWSFrame, len(plan.Requests))
	for i, request := range plan.Requests {
		frames[i] = DerivativeWSFrame{Kind: DerivativeWSFrameText, Payload: []byte(`{"result":null,"id":` + strconv.FormatInt(request.ID, 10) + `}`)}
	}
	return frames
}

func derivativeCombinedPayload(t *testing.T, stream string, data []byte) []byte {
	t.Helper()
	encoded, err := json.Marshal(struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}{Stream: stream, Data: json.RawMessage(data)})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func derivativeFixture(t *testing.T, product, relative string) []byte {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("..", "testdata", "binance", product, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func mustDerivativeStep(t *testing.T, adapter *DerivativeCapture) capture.StepResult {
	t.Helper()
	result, err := adapter.Step(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertDerivativeControlKinds(t *testing.T, result capture.StepResult, want ...capture.ControlKind) {
	t.Helper()
	got := make([]capture.ControlKind, len(result.Controls))
	for i, control := range result.Controls {
		got[i] = control.Envelope.ControlKind.Value
	}
	if !slices.Equal(got, want) {
		t.Fatalf("control kinds = %v, want %v; result = %+v", got, want, result)
	}
}

type derivativeRoundTripFunc func(*http.Request) (*http.Response, error)

func (f derivativeRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestPublicDerivativeConstructorsUseBoundedCoderConnector(t *testing.T) {
	client := &http.Client{Transport: derivativeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network must not be reached")
	}), Timeout: time.Second}
	for _, test := range []struct {
		product  DerivativeProduct
		endpoint string
		build    func(DerivativeWSConfig, *http.Client, capture.Clock, capture.RateBudget, capture.RawSink) (*DerivativeCapture, error)
	}{
		{DerivativeProductUSDM, USDMMarketEndpoint, NewUSDMDerivativeCapture},
		{DerivativeProductCoinM, CoinMWebSocketEndpoint, NewCoinMDerivativeCapture},
	} {
		clock, err := capture.NewManualClock(2_000_000_000_000_000_000, "public-constructor")
		if err != nil {
			t.Fatal(err)
		}
		contract := CoinMSourceContract()
		if test.product == DerivativeProductUSDM {
			contract = USDMMarketSourceContract()
		}
		budget, err := capture.NewTokenRateBudget(contract.Rate, 0)
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := test.build(DerivativeWSConfig{
			Symbols: []string{"BTCUSDT"}, RecorderVersion: "public-constructor", Endpoint: test.endpoint,
			Epochs: []capture.StreamEpoch{{Kind: capture.EpochConnection, ID: [16]byte{1}}},
		}, client, clock, budget, &derivativeTestSink{})
		if err != nil {
			t.Fatal(err)
		}
		if adapter.Product() != test.product {
			t.Fatalf("product = %q, want %q", adapter.Product(), test.product)
		}
		if _, ok := adapter.connector.(*CoderDerivativeWSConnector); !ok {
			t.Fatalf("connector type = %T", adapter.connector)
		}
	}
	if _, err := NewCoderDerivativeWSConnector(&http.Client{Timeout: time.Second}); err == nil {
		t.Fatal("connector accepted ambient HTTP transport")
	}
	connector, err := NewCoderDerivativeWSConnector(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connector.Connect(t.Context(), DerivativeWSConnectRequest{Product: DerivativeProductUSDM, Endpoint: "wss://fstream.binance.com/private", MaxApplicationBytes: USDMMaxRawPayloadBytes}); err == nil {
		t.Fatal("connector accepted a private/non-contract USD-M route")
	}
}

func TestDerivativeValidatorsAcceptCurrentPublicWireShapes(t *testing.T) {
	const receivedTimeNS = int64(1787492300000000000)
	usdmInstrument := normalize.InstrumentIdentity{NativeID: "BTCUSDT", BaseAssetID: "BTC", QuoteAssetID: "USDT"}
	usdm := []struct {
		suffix  string
		payload string
	}{
		{"@aggTrade", `{"e":"aggTrade","E":1787492322656,"a":3422444173,"s":"BTCUSDT","p":"77490.00","q":"0.003","nq":"0.003","f":8006170903,"l":8006170903,"T":1787492322555,"m":false,"st":1}`},
		{"@ticker", `{"e":"24hrTicker","E":1787492324172,"s":"BTCUSDT","ps":"BTCUSDT","p":"355.50","P":"0.461","w":"76764.07","c":"77490.00","Q":"0.001","o":"77134.50","h":"77771.60","l":"75588.00","v":"115619.606","q":"8875431314.20","O":1787405880000,"C":1787492324171,"F":8003360342,"L":8006170931,"n":2798861,"st":1}`},
		{"@markPrice@1s", `{"e":"markPriceUpdate","E":1787492323000,"s":"BTCUSDT","p":"77488.24528925","ap":"77488.24528925","P":"77435.97981966","i":"77485.96456522","r":"0.00010000","T":1787500800000,"st":1}`},
		{"@indexPrice", `{"E":1787492323001,"s":"BTCUSDT","p":"77488.68739130","e":"IndexUpdate"}`},
	}
	for _, test := range usdm {
		if adjusted := derivativeSchemaValidationReceivedTime([]byte(test.payload), receivedTimeNS); adjusted <= receivedTimeNS {
			t.Errorf("USD-M %s schema validation time = %d, want later source time", test.suffix, adjusted)
		}
		if err := validateUSDMStreamPayload(test.suffix, []byte(test.payload), receivedTimeNS, usdmInstrument); err != nil {
			t.Errorf("USD-M %s current wire: %v", test.suffix, err)
		}
	}

	coinMInstrument := normalize.InstrumentIdentity{NativeID: "BTCUSD_PERP", BaseAssetID: "BTC", QuoteAssetID: "USD"}
	coinM := []struct {
		suffix  string
		payload string
	}{
		{"@aggTrade", `{"e":"aggTrade","E":1787492348372,"a":494118434,"s":"BTCUSD_PERP","p":"77505.5","q":"38","f":1149067072,"l":1149067074,"T":1787492348362,"m":true,"st":2}`},
		{"@bookTicker", `{"e":"bookTicker","u":11364820130989,"s":"BTCUSD_PERP","ps":"BTCUSD","b":"77509.2","B":"1512","a":"77509.3","A":"833","T":1787492347571,"E":1787492347571,"st":2}`},
		{"@depth@100ms", `{"e":"depthUpdate","E":1787492347503,"T":1787492347501,"s":"BTCUSD_PERP","ps":"BTCUSD","U":11364820101197,"u":11364820116395,"pu":11364820101191,"b":[["69758.2","1"],["77464.6","0"]],"a":[["77527.8","2"],["77530.2","288"]],"st":2}`},
		{"@ticker", `{"e":"24hrTicker","E":1787493134982,"s":"BTCUSD_PERP","ps":"BTCUSD","p":"70.4","P":"0.091","w":"76731.03358407","c":"77398.4","Q":"1","o":"77328.0","h":"77757.5","l":"75547.9","v":"5894737","q":"7682.3","O":1787406720000,"C":1787493134940,"F":1148905983,"L":1149069061,"n":163079,"st":2}`},
		{"@markPrice@1s", `{"e":"markPriceUpdate","E":1787492348000,"s":"BTCUSD_PERP","p":"77509.30000000","ap":"77509.30000000","P":"77427.35627946","i":"77515.59065002","r":"0.00010000","T":1787500800000,"st":2}`},
	}
	for _, test := range coinM {
		if adjusted := derivativeSchemaValidationReceivedTime([]byte(test.payload), receivedTimeNS); adjusted <= receivedTimeNS {
			t.Errorf("COIN-M %s schema validation time = %d, want later source time", test.suffix, adjusted)
		}
		if err := validateCoinMStreamPayload(test.suffix, []byte(test.payload), receivedTimeNS, coinMInstrument); err != nil {
			t.Errorf("COIN-M %s current wire: %v", test.suffix, err)
		}
	}
}
