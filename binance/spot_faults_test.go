package binance

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

type spotTestRESTStep struct {
	response SpotRESTResponse
	err      error
}

type spotTestRESTClient struct {
	responses []SpotRESTResponse
	steps     []spotTestRESTStep
	requests  []SpotDepthRequest
	next      int
}

func (c *spotTestRESTClient) Do(ctx context.Context, request SpotDepthRequest, maximum uint32) (SpotRESTResponse, error) {
	if err := ctx.Err(); err != nil {
		return SpotRESTResponse{}, err
	}
	c.requests = append(c.requests, request)
	if len(c.steps) != 0 {
		if c.next == len(c.steps) {
			return SpotRESTResponse{}, ErrSpotRESTState
		}
		step := c.steps[c.next]
		c.next++
		if step.err != nil {
			return SpotRESTResponse{}, step.err
		}
		response := step.response
		if len(response.Body) > int(maximum) {
			return SpotRESTResponse{}, ErrSpotBounds
		}
		response.Body = append([]byte(nil), response.Body...)
		response.Headers = slices.Clone(response.Headers)
		return response, nil
	}
	if c.next == len(c.responses) {
		return SpotRESTResponse{}, ErrSpotRESTState
	}
	response := c.responses[c.next]
	c.next++
	if len(response.Body) > int(maximum) {
		return SpotRESTResponse{}, ErrSpotBounds
	}
	response.Body = append([]byte(nil), response.Body...)
	response.Headers = slices.Clone(response.Headers)
	return response, nil
}

type spotCountingBudget struct {
	inner              capture.RateBudget
	requestAcquires    int
	responseStatuses   []int
	responseRetryAfter []uint64
}

func (b *spotCountingBudget) Acquire(now uint64, cost uint32) (capture.BudgetDecision, error) {
	if cost > 1 {
		b.requestAcquires++
	}
	return b.inner.Acquire(now, cost)
}

func (b *spotCountingBudget) ObserveResponse(now uint64, status int, retryAfter uint64) (capture.ResponseDecision, error) {
	b.responseStatuses = append(b.responseStatuses, status)
	b.responseRetryAfter = append(b.responseRetryAfter, retryAfter)
	return b.inner.ObserveResponse(now, status, retryAfter)
}

type spotScriptClock struct {
	readings []capture.ClockReading
	next     int
}

func (c *spotScriptClock) Read() capture.ClockReading {
	if c.next >= len(c.readings) {
		return c.readings[len(c.readings)-1]
	}
	reading := c.readings[c.next]
	c.next++
	return reading
}

func (c *spotScriptClock) NewTimer(uint64) (capture.Timer, error) {
	return nil, errors.New("spot scripted clock does not support timers")
}

func TestSpotFaultsSnapshotRateRetrySchemaQueueClockAndCancellation(t *testing.T) {
	t.Run("snapshot cap weights and evidence", func(t *testing.T) {
		for _, test := range []struct {
			limit int
			want  uint32
		}{{1, 5}, {100, 5}, {101, 25}, {500, 25}, {501, 50}, {1000, 50}, {1001, 250}, {5000, 250}} {
			weight, err := SpotDepthRequestWeight(test.limit)
			if err != nil || weight != test.want {
				t.Fatalf("limit %d weight = %d, %v; want %d", test.limit, weight, err, test.want)
			}
		}
		for _, limit := range []int{-1, 0, 5001} {
			if _, err := SpotDepthRequestWeight(limit); !errors.Is(err, ErrSpotBounds) {
				t.Fatalf("limit %d error = %v, want ErrSpotBounds", limit, err)
			}
		}
		requestHeader := []capture.RESTHeader{{Kind: capture.RESTHeaderTimeUnit, Value: "MICROSECOND"}}
		if err := (capture.RESTRequestEvidenceV1{
			Version: 1, Kind: "request", RequestID: "header-boundary", Method: capture.RESTMethodGET,
			Headers: requestHeader, ScheduledAtNS: 1, StartedAtNS: 1,
		}).Validate(); err != nil {
			t.Fatalf("request-only time-unit header rejected: %v", err)
		}
		if err := (capture.RESTResponseEvidenceV1{
			Version: 1, Kind: "response", RequestID: "header-boundary", CompletedAtNS: 1,
			Status: 200, Headers: requestHeader,
		}).Validate(); !errors.Is(err, capture.ErrInvalidRESTEvidence) {
			t.Fatalf("response time-unit header error = %v, want ErrInvalidRESTEvidence", err)
		}
		body := spotFixture(t, "depth-snapshot.json")
		client := &spotTestRESTClient{responses: []SpotRESTResponse{{
			Status:  200,
			Headers: []SpotHTTPHeader{{Name: "X-MBX-USED-WEIGHT-1M", Value: "250"}, {Name: "Content-Type", Value: "application/json"}},
			Body:    body,
		}}}
		adapter, _, sink := newSpotDepthTestCapture(t, client, 5000, true)
		if _, err := adapter.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		requestResult := mustSpotDepthStep(t, adapter)
		if len(requestResult.Controls) != 1 {
			t.Fatalf("request controls = %+v", requestResult.Controls)
		}
		evidence, err := capture.UnmarshalRESTRequestEvidence(requestResult.Controls[0].Envelope.Extensions)
		if err != nil || len(evidence.Headers) != 1 || evidence.Headers[0].Kind != capture.RESTHeaderTimeUnit {
			t.Fatalf("microsecond request evidence = %+v, %v", evidence, err)
		}
		mustSpotDepthStep(t, adapter)
		if !sink.hasPayload(body) {
			t.Fatal("REST response body was not captured exactly before schema observation")
		}
	})

	t.Run("invalid response metadata preserves body and rate accounting before quarantine", func(t *testing.T) {
		body := spotFixture(t, "depth-snapshot.json")
		headers := make([]SpotHTTPHeader, SpotMaxResponseHeaders+1)
		headers[0] = SpotHTTPHeader{Name: "Content-Type", Value: "application/json"}
		headers[1] = SpotHTTPHeader{Name: "X-MBX-USED-WEIGHT-1M", Value: "5"}
		headers[2] = SpotHTTPHeader{Name: "Retry-After", Value: "not-a-valid-delay"}
		for i := 3; i < len(headers); i++ {
			headers[i] = SpotHTTPHeader{Name: fmt.Sprintf("X-Synthetic-%d", i), Value: "bounded"}
		}
		client := &spotTestRESTClient{responses: []SpotRESTResponse{{Status: 418, Headers: headers, Body: body}}}
		venueBudget, err := NewSpotVenueRateBudget(0)
		if err != nil {
			t.Fatal(err)
		}
		budget := &spotCountingBudget{inner: venueBudget}
		adapter, clock, sink := newSpotDepthTestCaptureWithBudget(t, client, 100, false, budget)
		if _, err := adapter.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		mustSpotDepthStep(t, adapter)
		rawResult := mustSpotDepthStep(t, adapter)
		if !sink.hasPayload(body) || rawResult.State == capture.RunnerClosed || len(client.requests) != 1 ||
			len(rawResult.Envelopes) != 1 || !rawResult.Envelopes[0].HTTPStatusOrWSState.Valid ||
			rawResult.Envelopes[0].HTTPStatusOrWSState.Value != "418" {
			t.Fatalf("invalid metadata raw boundary = %+v, body=%v calls=%d", rawResult, sink.hasPayload(body), len(client.requests))
		}
		evidence, err := capture.UnmarshalRESTResponseEvidence(rawResult.Envelopes[0].Extensions)
		if err != nil || len(evidence.Headers) != 2 {
			t.Fatalf("independently valid response metadata = %+v, err=%v", evidence, err)
		}
		if len(budget.responseStatuses) != 1 || budget.responseStatuses[0] != 418 ||
			len(budget.responseRetryAfter) != 1 || budget.responseRetryAfter[0] != 0 ||
			!slices.ContainsFunc(rawResult.Faults, func(fault capture.Fault) bool {
				return fault.Kind == capture.FaultRateCircuit && fault.HTTPStatus == 418
			}) {
			t.Fatalf("invalid metadata rate accounting = statuses=%v retry=%v result=%+v", budget.responseStatuses, budget.responseRetryAfter, rawResult)
		}
		quarantine := mustSpotDepthStep(t, adapter)
		if !slices.ContainsFunc(quarantine.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultResponseEvidence }) ||
			len(quarantine.Controls) != 2 ||
			quarantine.Controls[0].Envelope.ControlKind.Value != capture.ControlParseQuarantine ||
			quarantine.Controls[1].Envelope.ControlKind.Value != capture.ControlShutdown ||
			quarantine.State != capture.RunnerClosed || len(client.requests) != 1 ||
			len(sink.closes) != 1 || sink.closes[0].Reason != capture.CloseSchemaRejected {
			t.Fatalf("invalid metadata quarantine = %+v, calls=%d closes=%+v", quarantine, len(client.requests), sink.closes)
		}
		decision, err := budget.Acquire(clock.Read().MonotonicNS, 5)
		if err != nil || decision.Allowed {
			t.Fatalf("malformed 418 did not open venue circuit: decision=%+v err=%v", decision, err)
		}
	})

	for _, test := range []struct {
		status    int
		wantFault capture.FaultKind
		retry     bool
	}{
		{status: 429, wantFault: capture.FaultRateRetryable, retry: true},
		{status: 500, wantFault: capture.FaultRateRetryable, retry: true},
		{status: 599, wantFault: capture.FaultRateRetryable, retry: true},
		{status: 403, wantFault: capture.FaultRateTerminal},
		{status: 418, wantFault: capture.FaultRateCircuit},
	} {
		t.Run("status classification", func(t *testing.T) {
			body := []byte(`{"code":-1003,"msg":"bounded synthetic error"}`)
			responses := []SpotRESTResponse{{Status: test.status, Body: body}}
			if test.retry {
				responses = append(responses, SpotRESTResponse{Status: 200, Body: spotFixture(t, "depth-snapshot.json")})
			}
			client := &spotTestRESTClient{responses: responses}
			adapter, clock, _ := newSpotDepthTestCapture(t, client, 100, false)
			if _, err := adapter.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			mustSpotDepthStep(t, adapter)
			mustSpotDepthStep(t, adapter)
			faultResult := mustSpotDepthStep(t, adapter)
			if !slices.ContainsFunc(faultResult.Faults, func(fault capture.Fault) bool { return fault.Kind == test.wantFault && fault.HTTPStatus == test.status }) {
				t.Fatalf("status %d faults = %+v", test.status, faultResult.Faults)
			}
			if test.retry {
				if err := clock.Advance(uint64(1_000_000_000)); err != nil {
					t.Fatal(err)
				}
				mustSpotDepthStep(t, adapter)
				mustSpotDepthStep(t, adapter)
				if len(client.requests) != 2 || client.requests[0].RequestID != client.requests[1].RequestID {
					t.Fatalf("retry request identities = %+v", client.requests)
				}
			}
		})
	}

	t.Run("REST transport failures reacquire and terminate durably", func(t *testing.T) {
		clock, err := capture.NewManualClock(3_000_000_000, "spot-rest-failure-clock")
		if err != nil {
			t.Fatal(err)
		}
		venueBudget, err := NewSpotVenueRateBudget(0)
		if err != nil {
			t.Fatal(err)
		}
		budget := &spotCountingBudget{inner: venueBudget}
		request, err := NewSpotDepthRequest("depth-transport-stable", "BTCUSDT", 100, false)
		if err != nil {
			t.Fatal(err)
		}
		client := &spotTestRESTClient{steps: []spotTestRESTStep{
			{err: errors.New("synthetic transport drop 1")},
			{err: errors.New("synthetic transport drop 2")},
			{err: errors.New("synthetic transport drop 3")},
		}}
		sink := &spotTestSink{}
		adapter, err := NewSpotDepthCapture(SpotDepthConfig{
			Request: request, RecorderVersion: "spot-rest-failure",
			Epoch: spotEpoch(capture.EpochPollCycle, 0x45), ScheduledAtNS: 2_500_000_000,
		}, client, clock, budget, sink)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		for attempt := range 3 {
			requestResult := mustSpotDepthStep(t, adapter)
			if len(requestResult.Controls) != 1 || requestResult.Controls[0].Envelope.ControlKind.Value != capture.ControlRequestStarted ||
				requestResult.Controls[0].Envelope.SubscriptionOrRequestID.Value != request.RequestID {
				t.Fatalf("attempt %d request-start evidence = %+v", attempt+1, requestResult)
			}
			failureResult := mustSpotDepthStep(t, adapter)
			wantControls := 1
			if attempt == 2 {
				wantControls = 2
			}
			if !slices.ContainsFunc(failureResult.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultRequest }) ||
				len(failureResult.Controls) != wantControls ||
				failureResult.Controls[0].Envelope.ControlKind.Value != capture.ControlTimeout ||
				failureResult.Controls[0].Envelope.SubscriptionOrRequestID.Value != request.RequestID ||
				(attempt == 2 && failureResult.Controls[1].Envelope.ControlKind.Value != capture.ControlShutdown) {
				t.Fatalf("attempt %d failure evidence = %+v", attempt+1, failureResult)
			}
			if attempt < 2 && failureResult.State == capture.RunnerClosed {
				t.Fatalf("attempt %d closed before retry policy exhaustion", attempt+1)
			}
			if attempt == 2 {
				if failureResult.State != capture.RunnerClosed || len(failureResult.Opportunities) != 1 ||
					failureResult.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeCollectorFailed ||
					len(sink.closes) != 1 || sink.closes[0].Reason != capture.CloseTransportFailure {
					t.Fatalf("terminal transport failure = %+v, closes=%+v", failureResult, sink.closes)
				}
			}
		}
		if budget.requestAcquires != 3 || len(client.requests) != 3 {
			t.Fatalf("request rate acquisitions/calls = %d/%d, want 3/3", budget.requestAcquires, len(client.requests))
		}
		for _, retried := range client.requests {
			if retried.RequestID != request.RequestID {
				t.Fatalf("REST retry changed request ID: %+v", client.requests)
			}
		}
	})

	t.Run("schema oversize and malformed are bounded", func(t *testing.T) {
		oversize := make([]byte, SpotMaxRawPayloadBytes+1)
		if _, err := spotResponseEvent("oversize", SpotRESTResponse{Status: 200, Body: oversize}); !errors.Is(err, ErrSpotBounds) {
			t.Fatalf("oversize response error = %v, want ErrSpotBounds", err)
		}
		observer := NewSpotRawObserver(SpotSubscriptionPlan{})
		observation, err := observer.Observe(t.Context(), capture.EnvelopeV1{RecordKind: capture.RecordKindWebSocket, PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":`)})
		if err != nil || observation.Schema != capture.SchemaMalformed {
			t.Fatalf("malformed observation = %+v, %v", observation, err)
		}
	})

	t.Run("queue pressure preserves pending raw and closes", func(t *testing.T) {
		trade := spotFixture(t, "trade.json")
		connection := &spotTestConnection{frames: []SpotWSFrame{
			{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
			{Kind: SpotWSFrameText, Payload: trade},
		}}
		adapter, _, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		sink.full = true
		result := mustSpotStep(t, adapter)
		if !result.Blocked || !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultQueuePressure }) {
			t.Fatalf("queue pressure result = %+v", result)
		}
		sink.full = false
		mustSpotStep(t, adapter)
		if !sink.hasPayload(trade) || len(sink.closes) != 1 || sink.closes[0].Reason != capture.CloseQueuePressure {
			t.Fatalf("pending raw/close = %v / %+v", sink.hasPayload(trade), sink.closes)
		}
	})

	t.Run("wall clock regression preserves ordinal order", func(t *testing.T) {
		trade := spotFixture(t, "trade.json")
		connection := &spotTestConnection{frames: []SpotWSFrame{
			{Kind: SpotWSFrameText, Payload: []byte(`{"result":null,"id":1}`)},
			{Kind: SpotWSFrameText, Payload: trade},
		}}
		adapter, clock, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		startSpotSubscribed(t, adapter)
		mustSpotStep(t, adapter)
		clock.SetWall(-1)
		mustSpotStep(t, adapter)
		if len(sink.records) < 2 || sink.records[len(sink.records)-1].ReceivedWallTimeNS != -1 || sink.records[len(sink.records)-1].ArrivalOrdinal <= sink.records[len(sink.records)-2].ArrivalOrdinal {
			t.Fatalf("clock-regression records = %+v", sink.records)
		}
	})

	t.Run("transport carries a consumed frame through monotonic regression", func(t *testing.T) {
		payload := []byte("p")
		connection := &spotTestConnection{frames: []SpotWSFrame{{Kind: SpotWSFramePing, Payload: payload}}}
		transport := &spotWSTransport{
			clock: &spotScriptClock{readings: []capture.ClockReading{
				{ClockEpochID: "regression", MonotonicNS: 100},
				{ClockEpochID: "regression", MonotonicNS: 99},
			}},
			connection:    connection,
			connectedAtNS: 100,
		}
		event, err := transport.Next(t.Context())
		if err != nil || event.Kind != capture.TransportEventHeartbeat || !slices.Equal(event.Raw, payload) ||
			event.AfterRawFailure != capture.TransportFailureClockRegression || connection.next != 1 {
			t.Fatalf("transport read regression event = %+v, err=%v, reads=%d", event, err, connection.next)
		}
	})

	t.Run("cancellation closes epoch", func(t *testing.T) {
		connection := &spotTestConnection{}
		adapter, _, sink, _ := newSpotTestCapture(t, []string{"BTCUSDT"}, []*spotTestConnection{connection}, 1)
		if _, err := adapter.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := adapter.Step(ctx)
		if !errors.Is(err, context.Canceled) || !slices.ContainsFunc(result.Faults, func(fault capture.Fault) bool { return fault.Kind == capture.FaultCanceled }) || len(sink.closes) != 1 {
			t.Fatalf("cancellation result = %+v, err=%v, closes=%+v", result, err, sink.closes)
		}
	})
}

func newSpotDepthTestCapture(t *testing.T, client *spotTestRESTClient, limit int, microseconds bool) (*SpotDepthCapture, *capture.ManualClock, *spotTestSink) {
	t.Helper()
	budget, err := NewSpotVenueRateBudget(0)
	if err != nil {
		t.Fatal(err)
	}
	return newSpotDepthTestCaptureWithBudget(t, client, limit, microseconds, budget)
}

func newSpotDepthTestCaptureWithBudget(t *testing.T, client *spotTestRESTClient, limit int, microseconds bool, budget capture.RateBudget) (*SpotDepthCapture, *capture.ManualClock, *spotTestSink) {
	t.Helper()
	clock, err := capture.NewManualClock(2_000_000_000, "spot-rest-test-clock")
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewSpotDepthRequest("depth-request-stable", "BTCUSDT", limit, microseconds)
	if err != nil {
		t.Fatal(err)
	}
	sink := &spotTestSink{}
	adapter, err := NewSpotDepthCapture(SpotDepthConfig{
		Request:         request,
		RecorderVersion: "spot-rest-test",
		Epoch:           spotEpoch(capture.EpochPollCycle, 0x44),
		ScheduledAtNS:   1_500_000_000,
	}, client, clock, budget, sink)
	if err != nil {
		t.Fatal(err)
	}
	return adapter, clock, sink
}

func mustSpotDepthStep(t *testing.T, adapter *SpotDepthCapture) capture.StepResult {
	t.Helper()
	result, err := adapter.Step(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSpotFaultsOfficialFixtureDigests(t *testing.T) {
	fixtures := []struct {
		path   string
		length int
		digest string
	}{
		{"trade-example.js", 385, "25bc1caa02c3905a15eccb04078a73d114ecd32d3834c5ff4fae18f37cc756a8"},
		{"depth-example.js", 589, "11b794dac753c42fca68a65957be259723263fcc0214cf7fd686d4309f4d78a4"},
		{"book-ticker-example.js", 273, "3846e8bcbcd3c93da802ff23554d11c7fdac724a863a54f8e45ff2405f4adb2c"},
		{"ticker-example.js", 1152, "ae735d6e1e16dc47504ddce87343083f97535596009738e579d46f734dd4e2de"},
		{"subscribe-ack.json", 42, "5139da7abc80668135104d2665b92797a714725f4c840b281350a1c26cd680c8"},
		{"depth-snapshot-example.js", 196, "93190c63bd51815bd5b1662882e24796583ca2f2e3da06a08a61fb50e066fee3"},
	}
	manifestBytes, err := os.ReadFile("../testdata/binance/spot/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Commit   string `json:"commit"`
		Official []struct {
			File       string `json:"file"`
			ByteLength int    `json:"byte_length"`
			SHA256     string `json:"sha256"`
		} `json:"official"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Commit != "976cc580553890e92031b77306147c0ed1de5a46" {
		t.Fatalf("official fixture commit = %q", manifest.Commit)
	}
	for _, fixture := range fixtures {
		payload, err := os.ReadFile("../testdata/binance/spot/official/" + fixture.path)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(payload)
		if len(payload) != fixture.length || fmt.Sprintf("%x", digest) != fixture.digest {
			t.Fatalf("official fixture %s identity changed", fixture.path)
		}
		manifestFound := false
		for _, entry := range manifest.Official {
			if entry.File != "official/"+fixture.path {
				continue
			}
			manifestFound = true
			if entry.ByteLength != fixture.length || entry.SHA256 != fixture.digest {
				t.Fatalf("official fixture %s manifest identity changed: %+v", fixture.path, entry)
			}
		}
		if !manifestFound {
			t.Fatalf("official fixture %s is missing from manifest", fixture.path)
		}
	}
}
