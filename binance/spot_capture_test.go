package binance

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

type spotTestWrite struct {
	kind    SpotWSWriteKind
	payload []byte
}

type spotTestConnection struct {
	frames       []SpotWSFrame
	beforeReturn []func()
	readErr      error
	next         int
	writes       []spotTestWrite
	closeReasons []capture.CloseReason
}

func (c *spotTestConnection) Read(ctx context.Context, maximum uint32) (SpotWSFrame, error) {
	if err := ctx.Err(); err != nil {
		return SpotWSFrame{}, err
	}
	if c.next == len(c.frames) {
		if c.readErr != nil {
			return SpotWSFrame{}, c.readErr
		}
		return SpotWSFrame{}, io.EOF
	}
	index := c.next
	frame := c.frames[index]
	c.next++
	if index < len(c.beforeReturn) && c.beforeReturn[index] != nil {
		c.beforeReturn[index]()
	}
	if len(frame.Payload) > int(maximum) {
		return SpotWSFrame{}, ErrSpotBounds
	}
	frame.Payload = append([]byte(nil), frame.Payload...)
	return frame, nil
}

func (c *spotTestConnection) ReadBuffered(ctx context.Context, maximum uint32) (SpotWSFrame, bool, error) {
	if err := ctx.Err(); err != nil {
		return SpotWSFrame{}, false, err
	}
	if c.next == len(c.frames) {
		return SpotWSFrame{}, false, nil
	}
	frame, err := c.Read(ctx, maximum)
	return frame, err == nil, err
}

func (c *spotTestConnection) Write(ctx context.Context, kind SpotWSWriteKind, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writes = append(c.writes, spotTestWrite{kind: kind, payload: append([]byte(nil), payload...)})
	return nil
}

func (c *spotTestConnection) Close(ctx context.Context, reason capture.CloseReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.closeReasons = append(c.closeReasons, reason)
	return nil
}

type spotTestConnector struct {
	connections []*spotTestConnection
	requests    []SpotWSConnectRequest
	next        int
}

func (c *spotTestConnector) Connect(ctx context.Context, request SpotWSConnectRequest) (SpotWSConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.requests = append(c.requests, request)
	if c.next == len(c.connections) {
		return nil, errors.New("scripted connector exhausted")
	}
	connection := c.connections[c.next]
	c.next++
	return connection, nil
}

type spotTestSink struct {
	records []capture.EnvelopeV1
	closes  []capture.EpochClose
	commits []capture.EpochCommit
	full    bool
}

func (s *spotTestSink) WriteRaw(ctx context.Context, envelope capture.EnvelopeV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.full {
		return capture.ErrSinkFull
	}
	envelope.RawPayload = append([]byte(nil), envelope.RawPayload...)
	envelope.Extensions = append([]byte(nil), envelope.Extensions...)
	s.records = append(s.records, envelope)
	return nil
}

func (s *spotTestSink) Commit(_ context.Context, commit capture.EpochCommit) error {
	s.commits = append(s.commits, commit)
	return nil
}

func (s *spotTestSink) CloseEpoch(_ context.Context, closeRecord capture.EpochClose) error {
	s.closes = append(s.closes, closeRecord)
	return nil
}

func (s *spotTestSink) hasPayload(payload []byte) bool {
	for _, record := range s.records {
		if bytes.Equal(record.RawPayload, payload) {
			return true
		}
	}
	return false
}

func TestSpotCaptureRawBeforeParseAndRoles(t *testing.T) {
	trade := spotFixture(t, "trade.json")
	depth := spotFixture(t, "depth.json")
	book := spotFixture(t, "book-ticker.json")
	malformed := spotFixture(t, "malformed-ticker.json")
	connection := &spotTestConnection{frames: []SpotWSFrame{
		{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
		{Kind: SpotWSFrameText, Payload: trade},
		{Kind: SpotWSFrameText, Payload: depth},
		{Kind: SpotWSFrameText, Payload: book},
		{Kind: SpotWSFrameText, Payload: malformed},
	}}
	adapter, _, sink, _ := newSpotTestCapture(t, []string{"BNBBTC"}, []*spotTestConnection{connection}, 1)
	startSpotSubscribed(t, adapter)
	mustSpotStep(t, adapter)
	mustSpotStep(t, adapter)
	mustSpotStep(t, adapter)
	mustSpotStep(t, adapter)
	result := mustSpotStep(t, adapter)
	if !sink.hasPayload(malformed) {
		t.Fatal("malformed application bytes were not durably captured before observation")
	}
	if len(result.Faults) != 0 {
		t.Fatalf("parse fault was emitted before its already-captured raw record: %+v", result.Faults)
	}
	result = mustSpotStep(t, adapter)
	if !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultSchemaTypeChanged }) {
		t.Fatalf("semantic schema mutation faults = %+v, want FaultSchemaTypeChanged", result.Faults)
	}
	for name, payload := range map[string][]byte{"trade": trade, "depth": depth, "book ticker": book} {
		if !sink.hasPayload(payload) {
			t.Fatalf("%s raw payload was not captured exactly", name)
		}
	}
}

func TestSpotTickerCaptureExactBytes(t *testing.T) {
	ticker := spotFixture(t, "ticker.json")
	connection := &spotTestConnection{frames: []SpotWSFrame{
		{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
		{Kind: SpotWSFrameText, Payload: ticker},
	}}
	adapter, _, sink, _ := newSpotTestCapture(t, []string{"BNBBTC"}, []*spotTestConnection{connection}, 1)
	startSpotSubscribed(t, adapter)
	mustSpotStep(t, adapter)
	mustSpotStep(t, adapter)
	if !sink.hasPayload(ticker) {
		t.Fatalf("exact @ticker bytes were not preserved: %q", ticker)
	}
	observer := NewSpotRawObserver(adapter.SubscriptionPlan())
	observation, err := observer.Observe(t.Context(), capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindWebSocket,
		SourceID:                   SpotSourceID,
		ChannelOrEndpoint:          SpotRawChannel,
		ConnectionEpoch:            capture.OptionalEpoch{Value: spotEpoch(capture.EpochConnection, 9).ID, Valid: true},
		ArrivalOrdinal:             1,
		ReceivedWallTimeNS:         1,
		ClockEpochID:               "ticker-observer",
		MonotonicNSSinceClockEpoch: 1,
		PayloadEncoding:            capture.PayloadEncodingJSON,
		RawPayload:                 ticker,
		RawPayloadSHA256:           capture.PayloadHash(ticker),
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "test",
	})
	if err != nil || observation.Role != capture.MessageData || observation.Schema != capture.SchemaAccepted {
		t.Fatalf("ticker observation = %+v, %v", observation, err)
	}
}

func newSpotTestCapture(t *testing.T, symbols []string, connections []*spotTestConnection, epochCount int) (*SpotCapture, *capture.ManualClock, *spotTestSink, *spotTestConnector) {
	t.Helper()
	clock, err := capture.NewManualClock(1_000_000_000, "spot-test-clock")
	if err != nil {
		t.Fatal(err)
	}
	budget, err := NewSpotVenueRateBudget(0)
	if err != nil {
		t.Fatal(err)
	}
	epochs := make([]capture.StreamEpoch, epochCount)
	for i := range epochCount {
		epochs[i] = spotEpoch(capture.EpochConnection, byte(i+1))
	}
	connector := &spotTestConnector{connections: connections}
	sink := &spotTestSink{}
	adapter, err := NewSpotCapture(SpotWSConfig{
		Symbols:         symbols,
		MicrosecondTime: true,
		RecorderVersion: "spot-test",
		Epochs:          epochs,
	}, connector, clock, budget, sink)
	if err != nil {
		t.Fatal(err)
	}
	return adapter, clock, sink, connector
}

func startSpotSubscribed(t *testing.T, adapter *SpotCapture) {
	t.Helper()
	if _, err := adapter.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustSpotStep(t, adapter)
	mustSpotStep(t, adapter)
}

func mustSpotStep(t *testing.T, adapter *SpotCapture) capture.StepResult {
	t.Helper()
	result, err := adapter.Step(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func spotFixture(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := os.ReadFile("../testdata/binance/spot/synthetic/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func spotEpoch(kind capture.EpochKind, value byte) capture.StreamEpoch {
	return capture.StreamEpoch{Kind: kind, ID: [16]byte{value}}
}
