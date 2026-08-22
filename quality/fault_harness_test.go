package quality

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

func TestFaultHarnessACK(t *testing.T) {
	t.Parallel()
	wantFault := map[string]capture.FaultKind{
		"partial":   capture.FaultACKPartial,
		"wrong":     capture.FaultACKWrong,
		"rejected":  capture.FaultACKRejected,
		"duplicate": capture.FaultACKDuplicate,
	}
	for _, script := range ACKFaultScripts() {
		t.Run(script.Name, func(t *testing.T) {
			t.Parallel()
			events := append([]capture.TransportEvent{{Kind: capture.TransportEventConnected, WSState: "open"}}, script.Events...)
			h := newWSHarness(t, true, true, false, 1024, events, script.Observations, 32)
			h.start(t)
			h.step(t)
			h.step(t)
			result := h.step(t)
			if script.Name == "duplicate" {
				result = h.step(t)
			}
			if script.Name == "success" {
				if len(result.Opportunities) != 1 || result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeObserved {
					t.Fatalf("success opportunities = %#v", result.Opportunities)
				}
			} else if !hasFault(result, wantFault[script.Name]) {
				t.Fatalf("faults = %#v, want %d", result.Faults, wantFault[script.Name])
			}
			for _, opportunity := range result.Opportunities {
				if err := opportunity.Validate(); err != nil {
					t.Fatalf("opportunity Validate() error = %v", err)
				}
			}
			assertHarnessEvidence(t, h, events)
		})
	}
}

func TestFaultHarnessACKTimeout(t *testing.T) {
	t.Parallel()
	h := newWSHarness(t, true, true, false, 1024, []capture.TransportEvent{{Kind: capture.TransportEventConnected}}, nil, 32)
	h.start(t)
	h.step(t)
	h.step(t)
	if err := h.clock.Advance(100); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	result := h.step(t)
	if !hasFault(result, capture.FaultACKTimeout) || result.State != capture.RunnerClosed {
		t.Fatalf("timeout result = %#v", result)
	}
	if len(result.Opportunities) != 1 || result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeSourceStale {
		t.Fatalf("timeout opportunities = %#v", result.Opportunities)
	}
	operations := h.sink.Operations()
	if err := AssertOperationOrder(operations); err != nil {
		t.Fatal(err)
	}
	closeOperation := operations[len(operations)-1]
	if got := closeOperation.Close.Reason; got != capture.CloseACKTimeout {
		t.Fatalf("ACK timeout close reason = %d, want %d", got, capture.CloseACKTimeout)
	}
	if closeOperation.Close.BlindInterval == nil {
		t.Fatal("ACK timeout close omitted blind interval")
	}
}

func TestFaultHarnessBatchedACK(t *testing.T) {
	t.Parallel()
	inventory := []string{"book", "ticker", "trades", "liquidations", "funding", "mark", "index", "status"}
	batches := [][]string{
		{"book", "ticker"},
		{"trades", "liquidations"},
		{"funding", "mark"},
		{"index", "status"},
	}

	t.Run("eight_subscriptions_two_per_ACK", func(t *testing.T) {
		t.Parallel()
		events, observations := batchedACKScript(batches, 3)
		h := newBatchedACKHarness(t, inventory, events, observations)
		h.start(t)
		h.step(t)
		h.step(t)
		for i := range batches {
			result := h.step(t)
			if i < len(batches)-1 && len(result.Opportunities) != 0 {
				t.Fatalf("batch %d prematurely closed ACK opportunity: %#v", i, result.Opportunities)
			}
			if i == len(batches)-1 {
				if result.State != capture.RunnerRunning || len(result.Opportunities) != 1 ||
					result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeObserved {
					t.Fatalf("final ACK batch result = %#v", result)
				}
			}
		}
	})

	t.Run("pending_bound", func(t *testing.T) {
		t.Parallel()
		events, observations := batchedACKScript(batches, -1)
		events = append(events, rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-batch","final":true}`)))
		observations = append(observations, ObserverStep{Observation: capture.Observation{
			Role:   capture.MessageAcknowledgement,
			Schema: capture.SchemaAccepted,
			ACK:    capture.ACKObservation{RequestID: "sub-batch", Accepted: true, FinalBatch: true},
		}})
		h := newBatchedACKHarness(t, inventory, events, observations)
		h.start(t)
		h.step(t)
		h.step(t)
		var result capture.StepResult
		for range events {
			result = h.step(t)
		}
		if !hasFault(result, capture.FaultACKOverflow) || result.State != capture.RunnerClosed {
			t.Fatalf("pending-bound result = %#v", result)
		}
	})

	t.Run("duplicate_across_batches", func(t *testing.T) {
		t.Parallel()
		duplicateBatches := [][]string{{"book", "ticker"}, {"ticker", "trades"}}
		events, observations := batchedACKScript(duplicateBatches, -1)
		h := newBatchedACKHarness(t, inventory, events, observations)
		h.start(t)
		h.step(t)
		h.step(t)
		h.step(t)
		result := h.step(t)
		if !hasFault(result, capture.FaultACKDuplicate) || result.State != capture.RunnerClosed {
			t.Fatalf("duplicate batch result = %#v", result)
		}
	})

	t.Run("wrong_subscription", func(t *testing.T) {
		t.Parallel()
		events, observations := batchedACKScript([][]string{{"book", "unknown"}}, -1)
		h := newBatchedACKHarness(t, inventory, events, observations)
		h.start(t)
		h.step(t)
		h.step(t)
		result := h.step(t)
		if !hasFault(result, capture.FaultACKWrong) || result.State != capture.RunnerClosed {
			t.Fatalf("wrong subscription result = %#v", result)
		}
	})

	t.Run("batch_size_overflow", func(t *testing.T) {
		t.Parallel()
		events, observations := batchedACKScript([][]string{{"book", "ticker", "trades"}}, -1)
		h := newBatchedACKHarness(t, inventory, events, observations)
		h.start(t)
		h.step(t)
		h.step(t)
		result := h.step(t)
		if !hasFault(result, capture.FaultACKOverflow) || result.State != capture.RunnerClosed {
			t.Fatalf("batch-size overflow result = %#v", result)
		}
	})

	t.Run("timeout_partial", func(t *testing.T) {
		t.Parallel()
		events, observations := batchedACKScript(batches[:1], -1)
		h := newBatchedACKHarness(t, inventory, events, observations)
		h.start(t)
		h.step(t)
		h.step(t)
		partial := h.step(t)
		if len(partial.Opportunities) != 0 {
			t.Fatalf("partial batch prematurely closed ACK opportunity: %#v", partial.Opportunities)
		}
		if err := h.clock.Advance(100); err != nil {
			t.Fatalf("Advance() error = %v", err)
		}
		result := h.step(t)
		if !hasFault(result, capture.FaultACKTimeout) || result.State != capture.RunnerClosed ||
			len(result.Opportunities) != 1 || result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeSourceStale {
			t.Fatalf("partial timeout result = %#v", result)
		}
		operations := h.sink.Operations()
		if got := operations[len(operations)-1].Close.Reason; got != capture.CloseACKTimeout {
			t.Fatalf("partial timeout close reason = %d, want %d", got, capture.CloseACKTimeout)
		}
	})
}

func TestFaultHarnessHeartbeatCadenceAndUsefulDataPolicy(t *testing.T) {
	t.Parallel()
	heartbeatEvent := rawEvent(capture.TransportEventHeartbeat, []byte(`{"pong":true}`))
	heartbeatObservation := ObserverStep{Observation: capture.Observation{
		Role: capture.MessageHeartbeat, Schema: capture.SchemaAccepted, Unchanged: true,
	}}

	t.Run("early_heartbeat_accepted", func(t *testing.T) {
		t.Parallel()
		h := newWSHealthHarness(t, 0,
			[]capture.TransportEvent{{Kind: capture.TransportEventConnected}, heartbeatEvent},
			[]ObserverStep{heartbeatObservation},
		)
		h.start(t)
		h.step(t)
		result := h.step(t)
		if result.State != capture.RunnerRunning || len(result.Opportunities) != 1 ||
			result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeObservedUnchanged ||
			hasFault(result, capture.FaultHeartbeatEarly) {
			t.Fatalf("early heartbeat result = %#v", result)
		}
	})

	t.Run("missed_heartbeat", func(t *testing.T) {
		t.Parallel()
		h := newWSHealthHarness(t, 0,
			[]capture.TransportEvent{{Kind: capture.TransportEventConnected}, heartbeatEvent},
			[]ObserverStep{heartbeatObservation},
		)
		h.start(t)
		h.step(t)
		if err := h.clock.Advance(150); err != nil {
			t.Fatalf("Advance() error = %v", err)
		}
		result := h.step(t)
		if !hasFault(result, capture.FaultHeartbeatMissed) || result.State != capture.RunnerClosed {
			t.Fatalf("missed heartbeat result = %#v", result)
		}
		if remaining := h.transport.Remaining(); remaining != 1 {
			t.Fatalf("missed heartbeat consumed %d scripted events, want none", 1-remaining)
		}
		operations := h.sink.Operations()
		if got := operations[len(operations)-1].Close.Reason; got != capture.CloseHeartbeatMissed {
			t.Fatalf("missed heartbeat close reason = %d, want %d", got, capture.CloseHeartbeatMissed)
		}
	})

	t.Run("stochastic_useful_data_disabled", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}, heartbeatEvent, heartbeatEvent, heartbeatEvent}
		observations := []ObserverStep{heartbeatObservation, heartbeatObservation, heartbeatObservation}
		h := newWSHealthHarness(t, 0, events, observations)
		h.start(t)
		h.step(t)
		for i := range observations {
			if err := h.clock.Advance(50); err != nil {
				t.Fatalf("Advance() error = %v", err)
			}
			result := h.step(t)
			if result.State != capture.RunnerRunning || len(result.Opportunities) != 1 ||
				hasFault(result, capture.FaultUsefulDataMissed) {
				t.Fatalf("heartbeat %d result = %#v", i, result)
			}
		}
	})

	t.Run("explicit_useful_data_deadline", func(t *testing.T) {
		t.Parallel()
		h := newWSHealthHarness(t, 150,
			[]capture.TransportEvent{{Kind: capture.TransportEventConnected}, heartbeatEvent},
			[]ObserverStep{heartbeatObservation},
		)
		h.start(t)
		h.step(t)
		if err := h.clock.Advance(50); err != nil {
			t.Fatalf("Advance() error = %v", err)
		}
		heartbeat := h.step(t)
		if heartbeat.State != capture.RunnerRunning || len(heartbeat.Opportunities) != 1 {
			t.Fatalf("valid heartbeat result = %#v", heartbeat)
		}
		if err := h.clock.Advance(100); err != nil {
			t.Fatalf("Advance() error = %v", err)
		}
		result := h.step(t)
		if !hasFault(result, capture.FaultUsefulDataMissed) || result.State != capture.RunnerClosed {
			t.Fatalf("useful-data timeout result = %#v", result)
		}
		operations := h.sink.Operations()
		if got := operations[len(operations)-1].Close.Reason; got != capture.CloseUsefulDataMissed {
			t.Fatalf("useful-data close reason = %d, want %d", got, capture.CloseUsefulDataMissed)
		}
	})
}

func TestFaultHarnessHeartbeatAndDisconnect(t *testing.T) {
	t.Parallel()
	for _, script := range HeartbeatFaultScripts() {
		t.Run(script.Name, func(t *testing.T) {
			t.Parallel()
			events := append([]capture.TransportEvent{{Kind: capture.TransportEventConnected}}, script.Events...)
			h := newWSHarness(t, false, true, false, 1024, events, script.Observations, 32)
			h.start(t)
			h.step(t)
			if script.Name == "missed" {
				if err := h.clock.Advance(150); err != nil {
					t.Fatalf("Advance() error = %v", err)
				}
				result := h.step(t)
				if !hasFault(result, capture.FaultHeartbeatMissed) || result.State != capture.RunnerClosed {
					t.Fatalf("missed heartbeat result = %#v", result)
				}
				return
			}
			for range script.Events {
				if err := h.clock.Advance(50); err != nil {
					t.Fatalf("Advance() error = %v", err)
				}
				result := h.step(t)
				if len(result.Opportunities) != 1 || result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeObservedUnchanged {
					t.Fatalf("heartbeat result = %#v", result)
				}
			}
			assertHarnessEvidence(t, h, events)
		})
	}

	for _, script := range DisconnectFaultScripts() {
		t.Run("disconnect_"+script.Name, func(t *testing.T) {
			t.Parallel()
			events := append([]capture.TransportEvent{{Kind: capture.TransportEventConnected}}, script.Events...)
			h := newWSHarness(t, false, false, false, 1024, events, nil, 16)
			h.start(t)
			h.step(t)
			result := h.step(t)
			if result.State != capture.RunnerClosed {
				t.Fatalf("disconnect state = %d", result.State)
			}
			if script.Name == "abrupt" && !hasFault(result, capture.FaultDisconnectAbrupt) {
				t.Fatalf("abrupt disconnect faults = %#v", result.Faults)
			}
			operations := h.sink.Operations()
			last := operations[len(operations)-1]
			if last.Kind != SinkClose || (script.Name == "abrupt") != (last.Close.BlindInterval != nil) {
				t.Fatalf("disconnect close = %#v", last)
			}
		})
	}
}

func TestFaultHarnessTransportFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		failure capture.TransportFailureKind
		fault   capture.FaultKind
	}{
		{name: "dns", failure: capture.TransportFailureDNS, fault: capture.FaultDNS},
		{name: "tls", failure: capture.TransportFailureTLS, fault: capture.FaultTLS},
		{name: "connect", failure: capture.TransportFailureConnect, fault: capture.FaultConnect},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := []capture.TransportEvent{{Kind: capture.TransportEventFailure, Failure: test.failure}}
			h := newWSHarness(t, false, false, false, 1024, events, nil, 8)
			h.start(t)
			result := h.step(t)
			if !hasFault(result, test.fault) || result.State != capture.RunnerClosed {
				t.Fatalf("failure result = %#v", result)
			}
			operations := h.sink.Operations()
			if err := AssertOperationOrder(operations); err != nil {
				t.Fatal(err)
			}
			closeOperation := operations[len(operations)-1]
			if closeOperation.Close.BlindInterval == nil || closeOperation.Close.Reason != capture.CloseTransportFailure {
				t.Fatalf("failure close = %#v", closeOperation.Close)
			}
		})
	}
}

func TestFaultHarnessSchema(t *testing.T) {
	t.Parallel()
	wantFault := map[string]capture.FaultKind{
		"malformed":    capture.FaultSchemaMalformed,
		"additive":     capture.FaultSchemaAdditive,
		"type_changed": capture.FaultSchemaTypeChanged,
		"unknown_role": capture.FaultSchemaUnknownRole,
	}
	for _, script := range SchemaFaultScripts() {
		t.Run(script.Name, func(t *testing.T) {
			t.Parallel()
			events := append([]capture.TransportEvent{{Kind: capture.TransportEventConnected}}, script.Events...)
			h := newWSHarness(t, false, false, false, 1024, events, script.Observations, 16)
			h.start(t)
			h.step(t)
			result := h.step(t)
			if script.Name != "additive" {
				result = h.step(t)
			}
			if !hasFault(result, wantFault[script.Name]) {
				t.Fatalf("schema faults = %#v, want %d", result.Faults, wantFault[script.Name])
			}
			assertHarnessEvidence(t, h, events)
		})
	}

	t.Run("oversized", func(t *testing.T) {
		t.Parallel()
		raw := []byte(`{"padding":"0123456789"}`)
		events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}, {Kind: capture.TransportEventApplication, Raw: raw, Encoding: capture.PayloadEncodingJSON}}
		h := newWSHarness(t, false, false, false, 8, events, nil, 16)
		h.start(t)
		h.step(t)
		h.step(t)
		result := h.step(t)
		if !hasFault(result, capture.FaultSchemaOversized) {
			t.Fatalf("oversized faults = %#v", result.Faults)
		}
		if len(h.observer.Calls()) != 0 {
			t.Fatal("observer parsed a contract-oversized payload")
		}
		if err := AssertExactRaw(events, h.sink.Operations()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFaultHarnessControlSchemaBeforeHealth(t *testing.T) {
	t.Parallel()
	t.Run("malformed ACK cannot establish health", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{
			{Kind: capture.TransportEventConnected},
			{Kind: capture.TransportEventAcknowledgement, Raw: []byte(`{"id":"sub-1"}`), Encoding: capture.PayloadEncodingJSON},
		}
		observations := []ObserverStep{{Observation: capture.Observation{
			Role:   capture.MessageAcknowledgement,
			Schema: capture.SchemaMalformed,
			ACK:    capture.ACKObservation{RequestID: "sub-1", Subscriptions: []string{"trades", "ticker"}, Accepted: true},
		}}}
		h := newWSHarness(t, true, true, false, 1024, events, observations, 16)
		h.start(t)
		h.step(t)
		h.step(t)
		h.step(t)
		result := h.step(t)
		if result.State != capture.RunnerClosed || !hasFault(result, capture.FaultSchemaMalformed) ||
			len(result.Opportunities) != 1 || result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeMalformed {
			t.Fatalf("malformed ACK result = %#v", result)
		}
		assertHarnessEvidence(t, h, events)
	})

	t.Run("type-changed heartbeat cannot reset health", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{
			{Kind: capture.TransportEventConnected},
			{Kind: capture.TransportEventHeartbeat, Raw: []byte(`{"pong":"changed"}`), Encoding: capture.PayloadEncodingJSON},
		}
		observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageHeartbeat, Schema: capture.SchemaSemanticChanged}}}
		h := newWSHarness(t, false, true, false, 1024, events, observations, 16)
		h.start(t)
		h.step(t)
		h.step(t)
		result := h.step(t)
		if result.State != capture.RunnerClosed || !hasFault(result, capture.FaultSchemaTypeChanged) ||
			len(result.Opportunities) != 1 || result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeSchemaRejected {
			t.Fatalf("type-changed heartbeat result = %#v", result)
		}
		assertHarnessEvidence(t, h, events)
	})
}

func TestFaultHarnessQueuePressure(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"trade":1}`)
	events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}, {Kind: capture.TransportEventApplication, Raw: raw, Encoding: capture.PayloadEncodingJSON}}
	observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
	h := newWSHarness(t, false, false, false, 1024, events, observations, 2)
	h.start(t)
	h.step(t)
	blocked := h.step(t)
	if !blocked.Blocked || !hasFault(blocked, capture.FaultQueuePressure) {
		t.Fatalf("queue pressure result = %#v", blocked)
	}
	if len(h.observer.Calls()) != 0 {
		t.Fatal("observer ran before the full durable queue accepted raw")
	}
	closed, reason := h.transport.Closed()
	if !closed || reason != capture.CloseQueuePressure {
		t.Fatalf("transport close = %v, %d", closed, reason)
	}
	h.sink.Drain(1)
	flushed := h.step(t)
	if flushed.State != capture.RunnerClosed || len(h.observer.Calls()) != 1 {
		t.Fatalf("flushed result = %#v, observer calls %d", flushed, len(h.observer.Calls()))
	}
	operations := h.sink.Operations()
	last := operations[len(operations)-1]
	if last.Kind != SinkClose || last.Close.Reason != capture.CloseQueuePressure || last.Close.BlindInterval == nil {
		t.Fatalf("queue close = %#v", last)
	}
	assertHarnessEvidence(t, h, events)
}

func TestFaultHarnessThrottleableQueueRecovery(t *testing.T) {
	t.Parallel()
	events := []capture.TransportEvent{
		{Kind: capture.TransportEventConnected},
		{Kind: capture.TransportEventApplication, Raw: []byte(`{"trade":2}`), Encoding: capture.PayloadEncodingJSON},
	}
	observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
	h := newWSHarness(t, false, false, true, 1024, events, observations, 2)
	h.start(t)
	h.step(t)
	blocked := h.step(t)
	if !blocked.Blocked || !hasFault(blocked, capture.FaultQueuePressure) {
		t.Fatalf("throttleable pressure = %#v", blocked)
	}
	if closed, _ := h.transport.Closed(); closed {
		t.Fatal("throttleable transport closed under recoverable pressure")
	}
	h.sink.Drain(1)
	recovered := h.step(t)
	if recovered.State != capture.RunnerRunning || len(h.observer.Calls()) != 1 {
		t.Fatalf("throttleable recovery = %#v", recovered)
	}
	for _, operation := range h.sink.Operations() {
		if operation.Kind == SinkClose {
			t.Fatal("throttleable recovery closed the epoch")
		}
	}
	assertHarnessEvidence(t, h, events)
}

func TestFaultHarnessSinkFailureAndCancellation(t *testing.T) {
	t.Parallel()
	t.Run("write failure never observes", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}, {Kind: capture.TransportEventApplication, Raw: []byte(`{"trade":1}`), Encoding: capture.PayloadEncodingJSON}}
		observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
		h := newWSHarness(t, false, false, false, 1024, events, observations, 8)
		h.start(t)
		h.step(t)
		h.sink.FailNextWrite(ErrInjectedSink)
		result, err := h.runner.Step(t.Context())
		if !errors.Is(err, ErrInjectedSink) || !hasFault(result, capture.FaultSinkFailure) {
			t.Fatalf("sink failure result/error = %#v, %v", result, err)
		}
		if len(h.observer.Calls()) != 0 {
			t.Fatal("observer ran after failed durable write")
		}
		operations := h.sink.Operations()
		if err := AssertOperationOrder(operations); err != nil {
			t.Fatal(err)
		}
		closeOperation := operations[len(operations)-1]
		if closeOperation.Kind != SinkClose || closeOperation.Close.Reason != capture.CloseSinkFailure ||
			closeOperation.Close.UnresolvedPending == nil {
			t.Fatalf("write failure close = %#v", closeOperation)
		}
		if err := AssertExactRaw(events, operations); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("commit failure is explicit", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}, {Kind: capture.TransportEventDisconnected, Planned: true}}
		h := newWSHarness(t, false, false, false, 1024, events, nil, 8)
		h.start(t)
		h.step(t)
		h.sink.FailNextCommit(ErrInjectedSink)
		result, err := h.runner.Step(t.Context())
		if !errors.Is(err, ErrInjectedSink) || result.State != capture.RunnerClosed || !hasFault(result, capture.FaultSinkFailure) {
			t.Fatalf("commit failure result/error = %#v, %v", result, err)
		}
	})

	t.Run("close failure is explicit", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}, {Kind: capture.TransportEventDisconnected, Planned: true}}
		h := newWSHarness(t, false, false, false, 1024, events, nil, 8)
		h.start(t)
		h.step(t)
		h.sink.FailNextClose(ErrInjectedSink)
		result, err := h.runner.Step(t.Context())
		if !errors.Is(err, ErrInjectedSink) || result.State != capture.RunnerClosed || !hasFault(result, capture.FaultSinkFailure) {
			t.Fatalf("close failure result/error = %#v, %v", result, err)
		}
	})

	t.Run("pending full cancellation closes with unresolved evidence", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{
			{Kind: capture.TransportEventConnected},
			{Kind: capture.TransportEventApplication, Raw: []byte(`{"trade":3}`), Encoding: capture.PayloadEncodingJSON},
		}
		observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
		h := newWSHarness(t, false, false, false, 1024, events, observations, 2)
		h.start(t)
		h.step(t)
		if blocked := h.step(t); !blocked.Blocked {
			t.Fatalf("pending setup = %#v", blocked)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := h.runner.Step(ctx)
		if !errors.Is(err, context.Canceled) || result.State != capture.RunnerClosed ||
			!hasFault(result, capture.FaultCanceled) || !hasFault(result, capture.FaultQueuePressure) ||
			len(result.Unresolved) != 1 {
			t.Fatalf("pending cancellation result/error = %#v, %v", result, err)
		}
		if len(h.observer.Calls()) != 0 {
			t.Fatal("pending canceled raw reached observer")
		}
		operations := h.sink.Operations()
		if err := AssertOperationOrder(operations); err != nil {
			t.Fatal(err)
		}
		closeOperation := operations[len(operations)-1]
		if closeOperation.Close.BlindInterval == nil || !closeOperation.Close.BlindInterval.QueuePressure ||
			closeOperation.Close.Reason != capture.CloseCanceled || closeOperation.Close.UnresolvedPending == nil {
			t.Fatalf("pending cancellation close = %#v", closeOperation.Close)
		}
		if err := AssertExactRaw(events, operations); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cancellation closes without consuming transport", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}}
		h := newWSHarness(t, false, false, false, 1024, events, nil, 8)
		h.start(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := h.runner.Step(ctx)
		if !errors.Is(err, context.Canceled) || !hasFault(result, capture.FaultCanceled) || result.State != capture.RunnerClosed {
			t.Fatalf("cancellation result/error = %#v, %v", result, err)
		}
		if h.transport.Remaining() != 1 {
			t.Fatalf("cancellation consumed %d scripted events", 1-h.transport.Remaining())
		}
	})

	t.Run("transport read cancellation finalizes", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		transport := &cancelOnNextTransport{cancel: cancel}
		contract := baseContract(capture.TransportWebSocket, 1024)
		contract.Subscription = capture.SubscriptionPolicy{ACKMode: capture.ACKNone}
		contract.Heartbeat = capture.HeartbeatPolicy{Mode: capture.HeartbeatNone}
		config := capture.RunnerConfig{
			Epoch:             capture.StreamEpoch{Kind: capture.EpochConnection, ID: [16]byte{9}},
			ChannelOrEndpoint: "synthetic.trades.v1",
			DataFamily:        "trades",
			RecorderVersion:   "fault-harness",
		}
		clock, err := capture.NewManualClock(100, "cancel-clock")
		if err != nil {
			t.Fatal(err)
		}
		rate, err := capture.NewTokenRateBudget(contract.Rate, 0)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := NewTrace(16)
		if err != nil {
			t.Fatal(err)
		}
		sink, err := NewMemorySink(8, 16, trace)
		if err != nil {
			t.Fatal(err)
		}
		observer, err := NewScriptedObserver(nil, 1, trace)
		if err != nil {
			t.Fatal(err)
		}
		runner, err := capture.NewRunner(contract, config, transport, clock, rate, sink, observer)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runner.Start(ctx); err != nil {
			t.Fatal(err)
		}
		result, err := runner.Step(ctx)
		if !errors.Is(err, context.Canceled) || result.State != capture.RunnerClosed ||
			!hasFault(result, capture.FaultCanceled) || !transport.closed {
			t.Fatalf("transport cancellation result/error = %#v, %v", result, err)
		}
		if operations := sink.Operations(); operations[len(operations)-1].Close.BlindInterval == nil {
			t.Fatal("transport cancellation omitted blindness")
		}
	})
}

func TestFaultHarnessRatePolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     int
		retryAfter uint64
		want       capture.FaultKind
	}{
		{name: "429", status: 429, retryAfter: 50, want: capture.FaultRateRetryable},
		{name: "403", status: 403, want: capture.FaultRateCircuit},
		{name: "418", status: 418, want: capture.FaultRateCircuit},
		{name: "500", status: 500, want: capture.FaultRateRetryable},
		{name: "502", status: 502, want: capture.FaultRateRetryable},
		{name: "503", status: 503, want: capture.FaultRateRetryable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := []capture.TransportEvent{
				restRequest("poll-1"),
				restResponse("poll-1", test.status, test.retryAfter, []byte(`{"error":"synthetic"}`)),
			}
			observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
			h := newRESTHarness(t, events, observations, 1)
			h.start(t)
			h.step(t)
			h.step(t)
			result := h.step(t)
			if !hasFault(result, test.want) || result.State != capture.RunnerClosed {
				t.Fatalf("status %d result = %#v", test.status, result)
			}
			if test.status == 429 && result.Faults[0].RetryAtMonotonic != 50 {
				t.Fatalf("429 retry deadline = %d", result.Faults[0].RetryAtMonotonic)
			}
			assertHarnessEvidence(t, h, events)
		})
	}

	t.Run("venue budget exhaustion is stepwise", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{
			restRequest("poll-1"),
			restResponse("poll-1", 500, 0, []byte(`{"error":"synthetic"}`)),
			restRequest("poll-1"),
		}
		observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
		h := newRESTHarness(t, events, observations, 2)
		h.start(t)
		h.step(t)
		h.step(t)
		h.step(t)
		result := h.step(t)
		if !hasFault(result, capture.FaultRateExhausted) || !result.Progressed {
			t.Fatalf("rate exhaustion result = %#v", result)
		}
		if h.clock.Read().MonotonicNS != 0 {
			t.Fatal("rate handling advanced ambient time")
		}
	})
}

func TestFaultHarnessRESTLifecycle(t *testing.T) {
	t.Parallel()
	t.Run("successful request and response evidence closes once", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{
			restRequest("poll-stable"),
			restResponse("poll-stable", 200, 0, []byte(`{"ticker":"ok"}`)),
		}
		observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
		h := newRESTHarness(t, events, observations, 2)
		h.start(t)
		requestResult := h.step(t)
		if len(requestResult.Controls) != 1 || requestResult.Controls[0].Envelope.ControlKind.Value != capture.ControlRequestStarted {
			t.Fatalf("request boundary = %#v", requestResult)
		}
		result := h.step(t)
		if result.State != capture.RunnerClosed || len(result.Opportunities) != 1 ||
			result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeObserved {
			t.Fatalf("successful REST result = %#v", result)
		}
		operations := h.sink.Operations()
		if err := AssertOperationOrder(operations); err != nil {
			t.Fatal(err)
		}
		var requestEvidence capture.RESTRequestEvidenceV1
		var responseEvidence capture.RESTResponseEvidenceV1
		for _, operation := range operations {
			if operation.Kind != SinkWrite {
				continue
			}
			if operation.Envelope.ControlKind.Valid && operation.Envelope.ControlKind.Value == capture.ControlRequestStarted {
				requestEvidence, _ = capture.UnmarshalRESTRequestEvidence(operation.Envelope.Extensions)
			}
			if operation.Envelope.RecordKind == capture.RecordKindREST {
				responseEvidence, _ = capture.UnmarshalRESTResponseEvidence(operation.Envelope.Extensions)
			}
		}
		if requestEvidence.RequestID != "poll-stable" || requestEvidence.Method != capture.RESTMethodGET ||
			len(requestEvidence.Parameters) != 1 || requestEvidence.ScheduledAtNS != 10 || requestEvidence.StartedAtNS != 100 {
			t.Fatalf("request evidence = %#v", requestEvidence)
		}
		if responseEvidence.RequestID != "poll-stable" || responseEvidence.Status != 200 ||
			len(responseEvidence.Headers) != 3 || responseEvidence.CompletedAtNS != 100 {
			t.Fatalf("response evidence = %#v", responseEvidence)
		}
		assertHarnessEvidence(t, h, events)
	})

	t.Run("explicit retry preserves identity then succeeds", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{
			restRequest("poll-retry"),
			restResponse("poll-retry", 429, 50, []byte(`{"retry":true}`)),
			restRequest("poll-retry"),
			restResponse("poll-retry", 200, 0, []byte(`{"ticker":"ok"}`)),
		}
		observations := []ObserverStep{
			{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}},
			{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}},
		}
		h := newRESTHarness(t, events, observations, 2)
		h.start(t)
		h.step(t)
		firstResponse := h.step(t)
		if len(firstResponse.Opportunities) != 0 || firstResponse.State != capture.RunnerRunning {
			t.Fatalf("retryable response terminalized = %#v", firstResponse)
		}
		retryControl := h.step(t)
		if !hasFault(retryControl, capture.FaultRateRetryable) {
			t.Fatalf("retry control = %#v", retryControl)
		}
		if err := h.clock.Advance(100); err != nil {
			t.Fatal(err)
		}
		h.step(t)
		result := h.step(t)
		if result.State != capture.RunnerClosed || len(result.Opportunities) != 1 ||
			result.Opportunities[0].SubscriptionOrRequestID.Value != "poll-retry" {
			t.Fatalf("retried REST result = %#v", result)
		}
		assertHarnessEvidence(t, h, events)
	})

	t.Run("default non-2xx terminalizes", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{restRequest("poll-404"), restResponse("poll-404", 404, 0, []byte(`{"missing":true}`))}
		observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
		h := newRESTHarness(t, events, observations, 2)
		h.start(t)
		h.step(t)
		result := h.step(t)
		if result.State != capture.RunnerClosed || len(result.Opportunities) != 1 ||
			result.Opportunities[0].TerminalOutcome != capture.OpportunityOutcomeVenueUnavailable {
			t.Fatalf("default non-2xx result = %#v", result)
		}
	})

	t.Run("retry request identity cannot change", func(t *testing.T) {
		t.Parallel()
		events := []capture.TransportEvent{
			restRequest("poll-original"),
			restResponse("poll-original", 500, 0, []byte(`{"retry":true}`)),
			restRequest("poll-changed"),
		}
		observations := []ObserverStep{{Observation: capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}}}
		h := newRESTHarness(t, events, observations, 2)
		h.start(t)
		h.step(t)
		h.step(t)
		h.step(t)
		result, err := h.runner.Step(t.Context())
		if !errors.Is(err, capture.ErrRESTRequestIDChanged) || result.State != capture.RunnerClosed {
			t.Fatalf("changed request identity result/error = %#v, %v", result, err)
		}
	})
}

func TestFaultHarnessClockRegression(t *testing.T) {
	t.Parallel()
	events := []capture.TransportEvent{{Kind: capture.TransportEventConnected}}
	observations := make([]ObserverStep, 3)
	for i := range observations {
		events = append(events, capture.TransportEvent{Kind: capture.TransportEventApplication, Raw: []byte{byte(i + 1)}, Encoding: capture.PayloadEncodingBinary})
		observations[i].Observation = capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}
	}
	h := newWSHarness(t, false, false, false, 1024, events, observations, 16)
	h.start(t)
	h.step(t)
	walls := []int64{100, 100, 50}
	var ordinals []uint64
	for _, wall := range walls {
		h.clock.SetWall(wall)
		result := h.step(t)
		ordinals = append(ordinals, result.Envelopes[0].ArrivalOrdinal)
		if result.Envelopes[0].ReceivedWallTimeNS != wall {
			t.Fatalf("wall time = %d, want %d", result.Envelopes[0].ReceivedWallTimeNS, wall)
		}
	}
	if !slices.Equal(ordinals, []uint64{3, 4, 5}) {
		t.Fatalf("ordinals under equal/regressing wall time = %v", ordinals)
	}
	assertHarnessEvidence(t, h, events)
}

type cancelOnNextTransport struct {
	cancel context.CancelFunc
	closed bool
}

func (t *cancelOnNextTransport) Next(ctx context.Context) (capture.TransportEvent, error) {
	t.cancel()
	return capture.TransportEvent{}, ctx.Err()
}

func (t *cancelOnNextTransport) Close(_ context.Context, _ capture.CloseReason) error {
	t.closed = true
	return nil
}

type testHarness struct {
	runner    *capture.Runner
	clock     *capture.ManualClock
	transport *capture.ScriptedTransport
	sink      *MemorySink
	observer  *ScriptedObserver
	trace     *Trace
}

func newWSHarness(t *testing.T, exactACK, heartbeat, throttleable bool, maxRaw uint32, events []capture.TransportEvent, observations []ObserverStep, queueCapacity int) *testHarness {
	t.Helper()
	contract := baseContract(capture.TransportWebSocket, maxRaw)
	contract.Topology.Throttleable = throttleable
	config := capture.RunnerConfig{
		Epoch:             capture.StreamEpoch{Kind: capture.EpochConnection, ID: [16]byte{1}},
		ChannelOrEndpoint: "synthetic.trades.v1",
		DataFamily:        "trades",
		RecorderVersion:   "fault-harness",
	}
	if exactACK {
		contract.Topology.MaxSubscriptionsPerACK = 8
		contract.Subscription = capture.SubscriptionPolicy{ACKMode: capture.ACKExact, ACKTimeoutNS: 100, MaxPendingACK: 1}
		config.SubscriptionRequestID = "sub-1"
		config.ExpectedSubscriptions = []string{"trades", "ticker"}
	} else {
		contract.Subscription = capture.SubscriptionPolicy{ACKMode: capture.ACKNone}
	}
	if heartbeat {
		contract.Heartbeat = capture.HeartbeatPolicy{Mode: capture.HeartbeatPingPong, IntervalNS: 50, TimeoutNS: 100}
	} else {
		contract.Heartbeat = capture.HeartbeatPolicy{Mode: capture.HeartbeatNone}
	}
	return buildHarness(t, contract, config, events, observations, queueCapacity)
}

func newBatchedACKHarness(t *testing.T, inventory []string, events []capture.TransportEvent, observations []ObserverStep) *testHarness {
	t.Helper()
	contract := baseContract(capture.TransportWebSocket, 1024)
	contract.Topology.MaxSubscriptionsPerACK = 2
	contract.Subscription = capture.SubscriptionPolicy{
		ACKMode:       capture.ACKExact,
		ACKTimeoutNS:  100,
		MaxPendingACK: 4,
	}
	contract.Heartbeat = capture.HeartbeatPolicy{Mode: capture.HeartbeatNone}
	config := capture.RunnerConfig{
		Epoch:                 capture.StreamEpoch{Kind: capture.EpochConnection, ID: [16]byte{3}},
		ChannelOrEndpoint:     "synthetic.trades.v1",
		DataFamily:            "trades",
		RecorderVersion:       "fault-harness",
		SubscriptionRequestID: "sub-batch",
		ExpectedSubscriptions: slices.Clone(inventory),
	}
	allEvents := append([]capture.TransportEvent{{Kind: capture.TransportEventConnected}}, events...)
	return buildHarness(t, contract, config, allEvents, observations, 32)
}

func batchedACKScript(batches [][]string, finalBatch int) ([]capture.TransportEvent, []ObserverStep) {
	events := make([]capture.TransportEvent, len(batches))
	observations := make([]ObserverStep, len(batches))
	for i, batch := range batches {
		events[i] = rawEvent(capture.TransportEventAcknowledgement, []byte(`{"id":"sub-batch","ack":true}`))
		observations[i] = ObserverStep{Observation: capture.Observation{
			Role:   capture.MessageAcknowledgement,
			Schema: capture.SchemaAccepted,
			ACK: capture.ACKObservation{
				RequestID:     "sub-batch",
				Subscriptions: slices.Clone(batch),
				Accepted:      true,
				FinalBatch:    i == finalBatch,
			},
		}}
	}
	return events, observations
}

func newWSHealthHarness(t *testing.T, usefulDataMaxSilenceNS uint64, events []capture.TransportEvent, observations []ObserverStep) *testHarness {
	t.Helper()
	contract := baseContract(capture.TransportWebSocket, 1024)
	contract.Subscription = capture.SubscriptionPolicy{ACKMode: capture.ACKNone}
	contract.Heartbeat = capture.HeartbeatPolicy{
		Mode:       capture.HeartbeatPingPong,
		IntervalNS: 50,
		TimeoutNS:  100,
	}
	contract.UsefulData = capture.UsefulDataPolicy{MaxSilenceNS: usefulDataMaxSilenceNS}
	config := capture.RunnerConfig{
		Epoch:             capture.StreamEpoch{Kind: capture.EpochConnection, ID: [16]byte{4}},
		ChannelOrEndpoint: "synthetic.trades.v1",
		DataFamily:        "trades",
		RecorderVersion:   "fault-harness",
	}
	return buildHarness(t, contract, config, events, observations, 32)
}

func newRESTHarness(t *testing.T, events []capture.TransportEvent, observations []ObserverStep, maxAttempts uint16) *testHarness {
	t.Helper()
	contract := baseContract(capture.TransportREST, 1024)
	contract.Rate.MaxAttempts = maxAttempts
	config := capture.RunnerConfig{
		Epoch:             capture.StreamEpoch{Kind: capture.EpochPollCycle, ID: [16]byte{2}},
		ChannelOrEndpoint: "synthetic.poll.v1",
		DataFamily:        "ticker",
		RecorderVersion:   "fault-harness",
		ScheduledAtNS:     capture.OptionalInt64{Value: 10, Valid: true},
	}
	return buildHarness(t, contract, config, events, observations, 32)
}

func buildHarness(t *testing.T, contract capture.SourceContract, config capture.RunnerConfig, events []capture.TransportEvent, observations []ObserverStep, queueCapacity int) *testHarness {
	t.Helper()
	clock, err := capture.NewManualClock(100, "fault-clock")
	if err != nil {
		t.Fatalf("NewManualClock() error = %v", err)
	}
	transport, err := capture.NewScriptedTransport(events)
	if err != nil {
		t.Fatalf("NewScriptedTransport() error = %v", err)
	}
	rate, err := capture.NewTokenRateBudget(contract.Rate, 0)
	if err != nil {
		t.Fatalf("NewTokenRateBudget() error = %v", err)
	}
	trace, err := NewTrace(256)
	if err != nil {
		t.Fatalf("NewTrace() error = %v", err)
	}
	sink, err := NewMemorySink(queueCapacity, 256, trace)
	if err != nil {
		t.Fatalf("NewMemorySink() error = %v", err)
	}
	maximumCalls := max(1, len(observations))
	observer, err := NewScriptedObserver(observations, maximumCalls, trace)
	if err != nil {
		t.Fatalf("NewScriptedObserver() error = %v", err)
	}
	runner, err := capture.NewRunner(contract, config, transport, clock, rate, sink, observer)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}
	return &testHarness{runner: runner, clock: clock, transport: transport, sink: sink, observer: observer, trace: trace}
}

func (h *testHarness) start(t *testing.T) capture.StepResult {
	t.Helper()
	result, err := h.runner.Start(t.Context())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	return result
}

func (h *testHarness) step(t *testing.T) capture.StepResult {
	t.Helper()
	result, err := h.runner.Step(t.Context())
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	return result
}

func baseContract(transport capture.TransportKind, maxRaw uint32) capture.SourceContract {
	fixture := []byte(`{"synthetic":true}`)
	contract := capture.SourceContract{
		Version:       capture.SourceContractVersion,
		SourceID:      "synthetic-source",
		ContractID:    "synthetic.contract.v1",
		APIVersion:    "v1",
		Documentation: []capture.DocumentationRef{{URL: "https://example.test/synthetic-contract", AccessedAtNS: 1, Authority: capture.RuleAdapterPolicyInference}},
		Rate: capture.RatePolicy{
			Capacity:             1,
			RefillTokens:         1,
			RefillIntervalNS:     100,
			ConnectionCost:       1,
			RequestCost:          1,
			MaxAttempts:          2,
			DefaultRetryAfterNS:  10,
			MaxRetryAfterNS:      100,
			CircuitOpenNS:        1_000,
			RetryableStatusCodes: []int{429, 500, 502, 503},
			TerminalStatusCodes:  []int{401},
			CircuitStatusCodes:   []int{403, 418},
		},
		Payload:           capture.PayloadPolicy{MaxRawBytes: maxRaw, MaxSchemaDepth: 8, MaxSchemaFields: 64, MaxArrayElements: 256},
		FixtureIdentities: []capture.FixtureIdentity{{ID: "synthetic.fault.inline.v1", SHA256: sha256.Sum256(fixture), ByteLength: uint32(len(fixture)), Provenance: capture.FixtureSynthetic}},
	}
	if transport == capture.TransportWebSocket {
		contract.Capabilities = []capture.Capability{{ChannelOrEndpoint: "synthetic.trades.v1", DataFamily: "trades", Entitlement: "public", Support: capture.SupportAvailable}}
		contract.Topology = capture.ConnectionTopology{Transport: transport, MaxConnections: 1, MaxSubscriptions: 8}
	} else {
		contract.Capabilities = []capture.Capability{{ChannelOrEndpoint: "synthetic.poll.v1", DataFamily: "ticker", Entitlement: "public", Support: capture.SupportAvailable}}
		contract.Topology = capture.ConnectionTopology{Transport: transport, MaxConnections: 1, Throttleable: true}
	}
	return contract
}

func restRequest(requestID string) capture.TransportEvent {
	return capture.TransportEvent{
		Kind:                capture.TransportEventRequest,
		RequestID:           requestID,
		Method:              capture.RESTMethodGET,
		SanitizedParameters: []capture.SanitizedParameter{{Name: "symbol", Value: "SYNTH"}},
	}
}

func restResponse(requestID string, status int, retryAfterNS uint64, raw []byte) capture.TransportEvent {
	return capture.TransportEvent{
		Kind:         capture.TransportEventHTTPResponse,
		RequestID:    requestID,
		HTTPStatus:   status,
		RetryAfterNS: retryAfterNS,
		ResponseHeaders: []capture.RESTHeader{
			{Kind: capture.RESTHeaderContentType, Value: "application/json"},
			{Kind: capture.RESTHeaderRateRemaining, Value: "0"},
			{Kind: capture.RESTHeaderRetryAfter, Value: "synthetic"},
		},
		Raw:      append([]byte(nil), raw...),
		Encoding: capture.PayloadEncodingJSON,
	}
}

func hasFault(result capture.StepResult, want capture.FaultKind) bool {
	for _, fault := range result.Faults {
		if fault.Kind == want {
			return true
		}
	}
	return false
}

func assertHarnessEvidence(t *testing.T, h *testHarness, events []capture.TransportEvent) {
	t.Helper()
	if err := AssertRawBeforeObserve(h.trace.Events()); err != nil {
		t.Fatal(err)
	}
	if err := AssertOperationOrder(h.sink.Operations()); err != nil {
		t.Fatal(err)
	}
	if err := AssertExactRaw(events, h.sink.Operations()); err != nil {
		t.Fatal(err)
	}
}
