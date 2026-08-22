package binance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

func TestSpotControlACKPingDisconnectAndRotation(t *testing.T) {
	t.Run("correct ACK and deterministic writes", func(t *testing.T) {
		connection := &spotTestConnection{frames: []SpotWSFrame{{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)}}}
		adapter, _, _, connector := newSpotTestCapture(t, []string{"ETHUSDT", "BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		result := mustSpotStep(t, adapter)
		if len(result.Faults) != 0 || len(result.Opportunities) == 0 {
			t.Fatalf("correct ACK result = %+v", result)
		}
		plan := adapter.SubscriptionPlan()
		if len(connection.writes) != len(plan.Requests) {
			t.Fatalf("subscription writes = %d, want %d", len(connection.writes), len(plan.Requests))
		}
		for i, request := range plan.Requests {
			if connection.writes[i].kind != SpotWSWriteText || !bytes.Equal(connection.writes[i].payload, request.Raw) {
				t.Fatalf("subscription write %d changed", i)
			}
		}
		if len(connector.requests) != 1 || connector.requests[0].Endpoint != SpotWSEndpoint || connector.requests[0].TimeUnit != "MICROSECOND" {
			t.Fatalf("connect request = %+v", connector.requests)
		}
	})

	for _, test := range []struct {
		name    string
		acks    []string
		want    capture.FaultKind
		symbols []string
	}{
		{name: "wrong", acks: []string{`{"result":null,"id":9}`}, want: capture.FaultACKWrong, symbols: []string{"BTCUSDT"}},
		{name: "duplicate", acks: []string{`{"result":null,"id":1}`, `{"result":null,"id":1}`}, want: capture.FaultACKDuplicate, symbols: manySpotSymbols(17)},
		{name: "partial", acks: []string{`{"result":null,"id":2}`}, want: capture.FaultACKPartial, symbols: manySpotSymbols(17)},
	} {
		t.Run(test.name+" ACK fails closed", func(t *testing.T) {
			frames := make([]SpotWSFrame, len(test.acks))
			for i, ack := range test.acks {
				frames[i] = SpotWSFrame{Kind: SpotWSFrameText, Payload: []byte(ack)}
			}
			connection := &spotTestConnection{frames: frames}
			adapter, _, sink, _ := newSpotTestCapture(t, test.symbols, []*spotTestConnection{connection}, 1)
			startSpotSubscribed(t, adapter)
			var result capture.StepResult
			for range test.acks {
				result = mustSpotStep(t, adapter)
			}
			if !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == test.want }) {
				t.Fatalf("ACK faults = %+v, want %d", result.Faults, test.want)
			}
			if len(sink.closes) != 1 || sink.closes[0].Reason != capture.CloseACKRejected {
				t.Fatalf("ACK close evidence = %+v", sink.closes)
			}
		})
	}

	t.Run("ping payload echoed after durable raw", func(t *testing.T) {
		ping := []byte{0x00, 0x7f, 0xff, 'p'}
		connection := &spotTestConnection{frames: []SpotWSFrame{
			{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
			{Kind: SpotWSFramePing, Payload: ping},
		}}
		adapter, clock, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		connection.beforeReturn = []func(){nil, func() {
			if err := clock.Advance(SpotPongDeadlineNS + 1); err != nil {
				t.Fatal(err)
			}
		}}
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		if err := clock.Advance(SpotPingIntervalNS); err != nil {
			t.Fatal(err)
		}
		mustSpotStep(t, adapter)
		if !sink.hasPayload(ping) {
			t.Fatal("ping was not durable before pong")
		}
		last := connection.writes[len(connection.writes)-1]
		if last.kind != SpotWSWritePong || !bytes.Equal(last.payload, ping) {
			t.Fatalf("pong = %+v, want exact ping payload", last)
		}
	})

	t.Run("empty ping payload is captured and echoed", func(t *testing.T) {
		connection := &spotTestConnection{frames: []SpotWSFrame{
			{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
			{Kind: SpotWSFramePing, Payload: []byte{}},
		}}
		adapter, clock, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		if err := clock.Advance(SpotPingIntervalNS); err != nil {
			t.Fatal(err)
		}
		mustSpotStep(t, adapter)
		lastWrite := connection.writes[len(connection.writes)-1]
		lastRecord := sink.records[len(sink.records)-1]
		if lastWrite.kind != SpotWSWritePong || len(lastWrite.payload) != 0 ||
			!lastRecord.ControlKind.Valid || lastRecord.ControlKind.Value != capture.ControlHeartbeat || len(lastRecord.RawPayload) != 0 {
			t.Fatalf("empty ping/pong evidence = write %+v, record %+v", lastWrite, lastRecord)
		}
	})

	t.Run("read cancellation remains cancellation", func(t *testing.T) {
		connection := &spotTestConnection{
			frames:  []SpotWSFrame{{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)}},
			readErr: context.Canceled,
		}
		adapter, _, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		result, err := adapter.Step(t.Context())
		if !errors.Is(err, context.Canceled) || !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultCanceled }) ||
			len(sink.closes) != 1 || sink.closes[0].Reason != capture.CloseCanceled {
			t.Fatalf("read cancellation result = %+v, err=%v, closes=%+v", result, err, sink.closes)
		}
	})

	t.Run("missed ping deadline closes", func(t *testing.T) {
		connection := &spotTestConnection{frames: []SpotWSFrame{{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)}}}
		adapter, clock, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		if err := clock.Advance(SpotPingIntervalNS + SpotPongDeadlineNS); err != nil {
			t.Fatal(err)
		}
		result := mustSpotStep(t, adapter)
		if !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultHeartbeatMissed }) || len(sink.closes) != 1 {
			t.Fatalf("missed ping result = %+v, closes = %+v", result, sink.closes)
		}
	})

	t.Run("abrupt disconnect marks blindness", func(t *testing.T) {
		connection := &spotTestConnection{frames: []SpotWSFrame{
			{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
			{Kind: SpotWSFrameClose},
		}}
		adapter, _, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		result := mustSpotStep(t, adapter)
		if !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultDisconnectAbrupt }) {
			t.Fatalf("disconnect faults = %+v", result.Faults)
		}
		if len(sink.closes) != 1 || sink.closes[0].BlindInterval == nil {
			t.Fatalf("abrupt close evidence = %+v", sink.closes)
		}
	})

	t.Run("24 hour planned rotation closes before reconnect", func(t *testing.T) {
		first := &spotTestConnection{frames: []SpotWSFrame{{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)}}}
		second := &spotTestConnection{}
		adapter, clock, sink, connector := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{first, second}, 2)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		if err := clock.Advance(SpotSocketLifetimeNS); err != nil {
			t.Fatal(err)
		}
		result := mustSpotStep(t, adapter)
		if result.State != capture.RunnerClosed || len(sink.closes) != 1 || sink.closes[0].Reason != capture.ClosePlanned || len(connector.requests) != 1 ||
			len(first.closeReasons) != 1 || first.closeReasons[0] != capture.ClosePlanned || len(second.closeReasons) != 0 {
			t.Fatalf("planned close did not finish before reconnect: result=%+v closes=%+v connects=%d first=%v second=%v", result, sink.closes, len(connector.requests), first.closeReasons, second.closeReasons)
		}
		result = mustSpotStep(t, adapter)
		if len(result.Controls) != 1 || result.Controls[0].Envelope.ControlKind.Value != capture.ControlReconnect || len(connector.requests) != 1 ||
			!result.Controls[0].Envelope.ConnectionEpoch.Valid || result.Controls[0].Envelope.ConnectionEpoch.Value != spotEpoch(capture.EpochConnection, 2).ID {
			t.Fatalf("reconnect evidence/order = %+v, connects=%d", result, len(connector.requests))
		}
		result = mustSpotStep(t, adapter)
		if len(result.Controls) != 1 || result.Controls[0].Envelope.ControlKind.Value != capture.ControlConnectAttempt ||
			len(connector.requests) != 2 || len(first.closeReasons) != 1 {
			t.Fatalf("new connection attempt order = %+v, attempts=%d, old closes=%v", result, len(connector.requests), first.closeReasons)
		}
	})

	t.Run("rotation drains a backpressured subscription without sending it", func(t *testing.T) {
		connection := &spotTestConnection{}
		adapter, clock, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		if _, err := adapter.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		mustSpotStep(t, adapter)
		sink.full = true
		blocked := mustSpotStep(t, adapter)
		if !blocked.Blocked || len(connection.writes) != 0 {
			t.Fatalf("subscription backpressure = %+v, writes=%+v", blocked, connection.writes)
		}
		if err := clock.Advance(SpotSocketLifetimeNS); err != nil {
			t.Fatal(err)
		}
		stillBlocked := mustSpotStep(t, adapter)
		if !stillBlocked.Blocked || connection.next != 0 || len(connection.writes) != 0 {
			t.Fatalf("rotation read/sent while durable control was blocked: result=%+v reads=%d writes=%d", stillBlocked, connection.next, len(connection.writes))
		}
		sink.full = false
		drained := mustSpotStep(t, adapter)
		if len(drained.Controls) != 2 ||
			drained.Controls[0].Envelope.ControlKind.Value != capture.ControlSubscribeRequest ||
			drained.Controls[1].Envelope.ControlKind.Value != capture.ControlDisconnect ||
			drained.State != capture.RunnerClosed ||
			len(sink.closes) != 1 || sink.closes[0].Reason != capture.CloseQueuePressure ||
			len(connection.closeReasons) != 1 || connection.closeReasons[0] != capture.CloseQueuePressure ||
			connection.next != 0 || len(connection.writes) != 0 {
			t.Fatalf("rotation queue-pressure drain = %+v, closes=%+v transport_closes=%+v reads=%d writes=%d", drained, sink.closes, connection.closeReasons, connection.next, len(connection.writes))
		}
	})

	t.Run("rotation drains complete buffered application bytes before close", func(t *testing.T) {
		trade := spotFixture(t, "trade.json")
		connection := &spotTestConnection{frames: []SpotWSFrame{
			{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
			{Kind: SpotWSFrameText, Payload: trade},
		}}
		adapter, clock, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		if err := clock.Advance(SpotSocketLifetimeNS); err != nil {
			t.Fatal(err)
		}
		drained := mustSpotStep(t, adapter)
		if drained.State == capture.RunnerClosed || !sink.hasPayload(trade) || connection.next != 2 || len(sink.closes) != 0 {
			t.Fatalf("buffered rotation drain = %+v, body=%v reads=%d closes=%+v", drained, sink.hasPayload(trade), connection.next, sink.closes)
		}
		closed := mustSpotStep(t, adapter)
		if closed.State != capture.RunnerClosed || len(sink.closes) != 1 || sink.closes[0].Reason != capture.ClosePlanned {
			t.Fatalf("buffered rotation close = %+v, closes=%+v", closed, sink.closes)
		}
	})

	t.Run("unsupported control traffic closes", func(t *testing.T) {
		connection := &spotTestConnection{frames: []SpotWSFrame{
			{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
			{Kind: SpotWSFramePong, Payload: []byte("unexpected")},
		}}
		adapter, _, _, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		mustSpotStep(t, adapter)
		result := mustSpotStep(t, adapter)
		if !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool {
			return fault.Kind == capture.FaultSchemaUnknownRole || fault.Kind == capture.FaultDisconnectAbrupt
		}) {
			t.Fatalf("unsupported control faults = %+v", result.Faults)
		}
	})
}

func TestSpotControlDeterministicSubscriptionOrdering(t *testing.T) {
	first, err := NewSpotSubscriptionPlan([]string{"ethusdt", "BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSpotSubscriptionPlan([]string{"btcusdt", "ETHUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.Inventory, second.Inventory) || !bytes.Equal(first.Evidence, second.Evidence) {
		t.Fatalf("subscription plan depends on caller order: %v / %v", first.Inventory, second.Inventory)
	}
	want := []string{
		"btcusdt@trade", "btcusdt@depth@100ms", "btcusdt@bookTicker", "btcusdt@ticker",
		"ethusdt@trade", "ethusdt@depth@100ms", "ethusdt@bookTicker", "ethusdt@ticker",
	}
	if !slices.Equal(first.Inventory, want) {
		t.Fatalf("inventory = %v, want %v", first.Inventory, want)
	}
	for i, request := range first.Requests {
		if request.ID != int64(i+1) {
			t.Fatalf("request %d ID = %d, want %d", i, request.ID, i+1)
		}
	}
}

func manySpotSymbols(count int) []string {
	symbols := make([]string, count)
	for i := range count {
		symbols[i] = fmt.Sprintf("SYM%02dUSDT", i)
	}
	return symbols
}
