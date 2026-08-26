package capture

import (
	"context"
	"errors"
	"slices"
	"testing"
)

type pressureDurable struct {
	messages []RawMessage
	events   *[]string
}

func (d *pressureDurable) WriteRaw(_ context.Context, message RawMessage) error {
	d.messages = append(d.messages, RawMessage{
		Stream: message.Stream, Coordinate: message.Coordinate, Payload: slices.Clone(message.Payload), FrameComplete: message.FrameComplete,
	})
	*d.events = append(*d.events, "write:"+message.Coordinate)
	return nil
}

func (d *pressureDurable) FlushCommit(context.Context) (DurableCommit, error) {
	*d.events = append(*d.events, "flush")
	return DurableCommit{SegmentID: "segment-1", LastCoordinate: d.messages[len(d.messages)-1].Coordinate}, nil
}

func TestWriterPressure(t *testing.T) {
	clock, err := NewManualClock(1_000, "pressure-clock")
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	durable := &pressureDurable{events: &events}
	writer, err := NewWriterPressure(WriterPressureConfig{
		Transport: PressureTransportWebSocket, DecodeQueueCapacity: 4, DurableQueueCapacity: 4,
		DecodeHighWater: 2, DurableHighWater: 3, DecodeLowWater: 0, DurableLowWater: 0,
		MaxRawMessageBytes: 1024, PendingRESTCapacity: 4,
	}, clock, durable, PressureHooks{
		RecordRESTOutcome: func(_ context.Context, id string, outcome PressureOutcome) error {
			events = append(events, "outcome:"+id+":"+string(outcome))
			return nil
		},
		CloseWebSocket: func(context.Context) error {
			events = append(events, "close")
			return nil
		},
		OpenBlindGap: func(_ context.Context, gap BlindGap) error {
			if gap.LastCommit.SegmentID != "segment-1" || gap.LastCommit.LastCoordinate != "coordinate-2" || gap.DetectedWallTimeNS != 1_000 {
				t.Fatalf("blind gap = %+v", gap)
			}
			events = append(events, "gap")
			return nil
		},
		Reconnect: func(context.Context) error {
			events = append(events, "reconnect")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.ScheduleREST("rest-opportunity-1"); err != nil {
		t.Fatal(err)
	}
	first := RawMessage{Stream: "canonical-trades", Coordinate: "coordinate-1", Payload: []byte("first-complete-frame"), FrameComplete: true}
	second := RawMessage{Stream: "canonical-book", Coordinate: "coordinate-2", Payload: []byte("second-complete-frame"), FrameComplete: true}
	if err := writer.EnqueueDecoded(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if snapshot := writer.Snapshot(); snapshot.State != PressureRunning || !snapshot.Complete || snapshot.DecodeDepth != 1 {
		t.Fatalf("pre-pressure snapshot = %s", snapshot)
	}
	if err := writer.EnqueueDecoded(t.Context(), second); err != nil {
		t.Fatal(err)
	}

	snapshot := writer.Snapshot()
	wantActions := []PressureAction{
		PressureStopREST, PressureRecordOutcomes, PressureCloseWebSocket, PressureFlushCommit, PressureOpenBlindGap, PressureAwaitCapacity,
	}
	if snapshot.State != PressureBlindGap || snapshot.Complete || snapshot.DecodeDepth != 0 || snapshot.DurableDepth != 0 ||
		snapshot.PendingREST != 0 || !slices.Equal(snapshot.LastActions, wantActions) {
		t.Fatalf("high-water snapshot = %+v, want actions %v", snapshot, wantActions)
	}
	wantEvents := []string{
		"outcome:rest-opportunity-1:backpressure_delayed", "close", "write:coordinate-1", "write:coordinate-2", "flush", "gap",
	}
	if !slices.Equal(events, wantEvents) {
		t.Fatalf("high-water event order = %v, want %v", events, wantEvents)
	}
	if len(durable.messages) != 2 || durable.messages[0].Stream != first.Stream || durable.messages[1].Stream != second.Stream ||
		string(durable.messages[0].Payload) != string(first.Payload) || string(durable.messages[1].Payload) != string(second.Payload) {
		t.Fatalf("canonical raw messages were dropped, reordered, or coalesced: %+v", durable.messages)
	}
	if err := writer.EnqueueDecoded(t.Context(), RawMessage{Stream: "canonical-trades", Coordinate: "coordinate-3", Payload: []byte("must-not-be-accepted"), FrameComplete: true}); !errors.Is(err, ErrPressureIncomplete) {
		t.Fatalf("enqueue during blind gap error = %v, want ErrPressureIncomplete", err)
	}
	if err := writer.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	snapshot = writer.Snapshot()
	if snapshot.State != PressureRunning || !snapshot.Complete || snapshot.LastActions[len(snapshot.LastActions)-1] != PressureReconnect {
		t.Fatalf("recovered snapshot = %+v", snapshot)
	}
	if events[len(events)-1] != "reconnect" {
		t.Fatalf("reconnect did not occur after flush and gap: %v", events)
	}

	var restEvents []string
	restDurable := &pressureDurable{events: &restEvents}
	resumed := false
	restWriter, err := NewWriterPressure(WriterPressureConfig{
		Transport: PressureTransportREST, DecodeQueueCapacity: 4, DurableQueueCapacity: 4,
		DecodeHighWater: 2, DurableHighWater: 3, DecodeLowWater: 0, DurableLowWater: 0,
		MaxRawMessageBytes: 64, PendingRESTCapacity: 2,
	}, clock, restDurable, PressureHooks{
		RecordRESTOutcome: func(context.Context, string, PressureOutcome) error { return nil },
		ResumeREST: func(context.Context) error {
			resumed = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, coordinate := range []string{"rest-coordinate-1", "rest-coordinate-2"} {
		if err := restWriter.EnqueueDecoded(t.Context(), RawMessage{
			Stream: "canonical-rest", Coordinate: coordinate, Payload: []byte{byte(index + 1)}, FrameComplete: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := restWriter.EnqueueDecoded(t.Context(), RawMessage{
		Stream: "canonical-rest", Coordinate: "rest-coordinate-in-flight", Payload: []byte{3}, FrameComplete: true,
	}); err != nil {
		t.Fatalf("started REST response rejected after scheduling throttle: %v", err)
	}
	if err := restWriter.Recover(t.Context()); !errors.Is(err, ErrPressureNotRecovered) {
		t.Fatalf("early REST recovery error = %v, want ErrPressureNotRecovered", err)
	}
	if resumed {
		t.Fatal("REST scheduling resumed above the low-water marks")
	}
	for range 3 {
		if err := restWriter.AdvanceDecode(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := restWriter.CommitOne(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := restWriter.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !resumed || restWriter.Snapshot().State != PressureRunning {
		t.Fatalf("REST did not resume after low-water recovery: %+v", restWriter.Snapshot())
	}

	partial, err := NewWriterPressure(WriterPressureConfig{
		Transport: PressureTransportWebSocket, DecodeQueueCapacity: 3, DurableQueueCapacity: 3,
		DecodeHighWater: 2, DurableHighWater: 2, DecodeLowWater: 0, DurableLowWater: 0,
		MaxRawMessageBytes: 64, PendingRESTCapacity: 1,
	}, clock, durable, PressureHooks{
		RecordRESTOutcome: func(context.Context, string, PressureOutcome) error { return nil },
		CloseWebSocket:    func(context.Context) error { return nil }, OpenBlindGap: func(context.Context, BlindGap) error { return nil },
		Reconnect: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := partial.EnqueueDecoded(t.Context(), RawMessage{Stream: "canonical", Coordinate: "partial", Payload: []byte("partial"), FrameComplete: false}); !errors.Is(err, ErrPressureIncomplete) {
		t.Fatalf("partial frame error = %v, want ErrPressureIncomplete", err)
	}
	if partial.Snapshot().Complete {
		t.Fatal("partial frame left connection marked complete")
	}
}

func TestWriterPressureFailureReconciliation(t *testing.T) {
	clock, err := NewManualClock(1_000, "pressure-failure-clock")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("REST outcomes remain pending until durable", func(t *testing.T) {
		var events []string
		durable := &pressureDurable{events: &events}
		recordAttempts := 0
		resumed := false
		writer, err := NewWriterPressure(WriterPressureConfig{
			Transport: PressureTransportREST, DecodeQueueCapacity: 4, DurableQueueCapacity: 4,
			DecodeHighWater: 2, DurableHighWater: 3, DecodeLowWater: 0, DurableLowWater: 0,
			MaxRawMessageBytes: 64, PendingRESTCapacity: 2,
		}, clock, durable, PressureHooks{
			RecordRESTOutcome: func(context.Context, string, PressureOutcome) error {
				recordAttempts++
				if recordAttempts == 1 {
					return errors.New("synthetic ledger unavailable")
				}
				return nil
			},
			ResumeREST: func(context.Context) error {
				resumed = true
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.ScheduleREST("rest-opportunity"); err != nil {
			t.Fatal(err)
		}
		for index := range 2 {
			err := writer.EnqueueDecoded(t.Context(), RawMessage{
				Stream: "rest", Coordinate: "coordinate-" + string(rune('a'+index)),
				Payload: []byte{byte(index + 1)}, FrameComplete: true,
			})
			if index == 1 && err == nil {
				t.Fatal("high-water transition hid outcome recording failure")
			}
		}
		if snapshot := writer.Snapshot(); snapshot.State != PressureRESTThrottled || snapshot.PendingREST != 1 {
			t.Fatalf("failed outcome was not retained: %+v", snapshot)
		}
		for range 2 {
			if err := writer.AdvanceDecode(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := writer.CommitOne(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Recover(t.Context()); err != nil {
			t.Fatal(err)
		}
		if !resumed || recordAttempts != 2 || writer.Snapshot().PendingREST != 0 {
			t.Fatalf("REST outcome reconciliation = resumed %t attempts %d snapshot %+v", resumed, recordAttempts, writer.Snapshot())
		}
	})

	t.Run("failed socket close cannot reconnect", func(t *testing.T) {
		var events []string
		durable := &pressureDurable{events: &events}
		reconnected := false
		writer, err := NewWriterPressure(WriterPressureConfig{
			Transport: PressureTransportWebSocket, DecodeQueueCapacity: 4, DurableQueueCapacity: 4,
			DecodeHighWater: 2, DurableHighWater: 3, DecodeLowWater: 0, DurableLowWater: 0,
			MaxRawMessageBytes: 64, PendingRESTCapacity: 1,
		}, clock, durable, PressureHooks{
			RecordRESTOutcome: func(context.Context, string, PressureOutcome) error { return nil },
			CloseWebSocket:    func(context.Context) error { return errors.New("synthetic close failure") },
			OpenBlindGap:      func(context.Context, BlindGap) error { t.Fatal("gap opened after failed close"); return nil },
			Reconnect: func(context.Context) error {
				reconnected = true
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		for index := range 2 {
			err := writer.EnqueueDecoded(t.Context(), RawMessage{
				Stream: "websocket", Coordinate: "coordinate-" + string(rune('a'+index)),
				Payload: []byte{byte(index + 1)}, FrameComplete: true,
			})
			if index == 1 && err == nil {
				t.Fatal("failed socket close was hidden")
			}
		}
		if snapshot := writer.Snapshot(); snapshot.State != PressureFaulted || snapshot.Complete {
			t.Fatalf("failed close state = %+v", snapshot)
		}
		if err := writer.Recover(t.Context()); !errors.Is(err, ErrWriterPressure) {
			t.Fatalf("faulted connection recovery error = %v", err)
		}
		if reconnected {
			t.Fatal("faulted connection created a second socket")
		}
	})
}

func TestWriterPressureClearsConsumedPayloads(t *testing.T) {
	clock, err := NewManualClock(1_000, "pressure-memory-clock")
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	writer, err := NewWriterPressure(WriterPressureConfig{
		Transport: PressureTransportREST, DecodeQueueCapacity: 4, DurableQueueCapacity: 4,
		DecodeHighWater: 3, DurableHighWater: 3, DecodeLowWater: 0, DurableLowWater: 0,
		MaxRawMessageBytes: 64, PendingRESTCapacity: 1,
	}, clock, &pressureDurable{events: &events}, PressureHooks{
		RecordRESTOutcome: func(context.Context, string, PressureOutcome) error { return nil },
		ResumeREST:        func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.EnqueueDecoded(t.Context(), RawMessage{Stream: "rest", Coordinate: "coordinate", Payload: []byte("payload"), FrameComplete: true}); err != nil {
		t.Fatal(err)
	}
	decodeBacking := writer.decode[:cap(writer.decode)]
	if err := writer.AdvanceDecode(t.Context()); err != nil {
		t.Fatal(err)
	}
	if decodeBacking[0].Payload != nil {
		t.Fatal("consumed decode slot retained payload")
	}
	durableBacking := writer.durableQueue[:cap(writer.durableQueue)]
	if err := writer.CommitOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if durableBacking[0].Payload != nil {
		t.Fatal("committed durable slot retained payload")
	}
}

func TestRateOwnerFakeClock(t *testing.T) {
	clock, err := NewManualClock(0, "rate-owner-clock")
	if err != nil {
		t.Fatal(err)
	}
	config := RateOwnerConfig{
		Identity: RateOwnerIdentity{Venue: "synthetic-venue", API: "public-v1", ScopeKind: RateScopeSharedPool, ScopeID: "public-pool"},
		Policy: RatePolicy{
			Capacity: 1, RefillTokens: 1, RefillIntervalNS: 100, RequestCost: 1, MaxAttempts: 3,
			DefaultRetryAfterNS: 20, MaxRetryAfterNS: 50, CircuitOpenNS: 100,
			RetryableStatusCodes: []int{429}, TerminalStatusCodes: []int{403}, CircuitStatusCodes: []int{418},
		},
		MaxOperations: 8,
	}
	owner, err := NewRateOwner(config, clock)
	if err != nil {
		t.Fatal(err)
	}
	operation := RateOperation{ID: "stable-operation-1", Kind: RateOperationRequest, Cost: 1, MaximumAttempts: 3, DeadlineMonotonicNS: 1_000}
	if attempt, err := owner.Attempt(operation); err != nil || !attempt.Allowed || attempt.Attempt != 1 {
		t.Fatalf("first attempt = %+v, %v", attempt, err)
	}
	completion, err := owner.Complete(operation.ID, RateResponse{Status: 429, RetryAfterNS: 50, UsedWeight: 77, Remaining: 23, Certainty: RateOutcomeKnown})
	if err != nil || completion.Disposition != ResponseRetryable || completion.RetryAtMonotonic != 50 {
		t.Fatalf("retryable completion = %+v, %v", completion, err)
	}
	if snapshot, err := owner.Snapshot(operation.ID); err != nil || snapshot.LastResponse.UsedWeight != 77 || snapshot.LastResponse.Remaining != 23 {
		t.Fatalf("typed response-header ownership = %+v, %v", snapshot, err)
	}
	if attempt, err := owner.Attempt(operation); err != nil || attempt.Allowed || attempt.RetryAtMonotonic != 50 {
		t.Fatalf("cooldown attempt = %+v, %v", attempt, err)
	}
	if err := clock.Advance(50); err != nil {
		t.Fatal(err)
	}
	if attempt, err := owner.Attempt(operation); err != nil || attempt.Allowed || attempt.Reason != BudgetExhausted || attempt.RetryAtMonotonic != 100 {
		t.Fatalf("token cooldown attempt = %+v, %v", attempt, err)
	}
	if err := clock.Advance(50); err != nil {
		t.Fatal(err)
	}
	if attempt, err := owner.Attempt(operation); err != nil || !attempt.Allowed || attempt.Attempt != 2 {
		t.Fatalf("second attempt = %+v, %v", attempt, err)
	}
	if completion, err := owner.Complete(operation.ID, RateResponse{Status: 0, Certainty: RateOutcomeUnknown}); err != nil || !completion.Reconcile {
		t.Fatalf("unknown completion = %+v, %v", completion, err)
	}
	if _, err := owner.Attempt(operation); !errors.Is(err, ErrRateReconcileRequired) {
		t.Fatalf("blind unknown retry error = %v, want ErrRateReconcileRequired", err)
	}
	if err := owner.Reconcile(operation.ID, ReconcileNotApplied); err != nil {
		t.Fatal(err)
	}
	if err := clock.Advance(100); err != nil {
		t.Fatal(err)
	}
	if attempt, err := owner.Attempt(operation); err != nil || !attempt.Allowed || attempt.Attempt != 3 {
		t.Fatalf("reconciled third attempt = %+v, %v", attempt, err)
	}
	if _, err := owner.Complete(operation.ID, RateResponse{Status: 429, RetryAfterNS: 20, Certainty: RateOutcomeKnown}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Attempt(operation); !errors.Is(err, ErrRateOperationTerminal) {
		t.Fatalf("attempt ceiling error = %v, want ErrRateOperationTerminal", err)
	}

	deadline := RateOperation{ID: "deadline-operation", Kind: RateOperationConnection, Cost: 1, MaximumAttempts: 1, DeadlineMonotonicNS: clock.Read().MonotonicNS + 10}
	if err := clock.Advance(10); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Attempt(deadline); !errors.Is(err, ErrRateOperationTerminal) {
		t.Fatalf("deadline attempt error = %v, want ErrRateOperationTerminal", err)
	}
	if _, err := NewRateOwners([]RateOwnerConfig{config, config}, clock); err == nil {
		t.Fatal("duplicate venue/API budget owners were accepted")
	}
}

func TestRateOwnerAuthoritativeHeadersGovernAdmission(t *testing.T) {
	clock, err := NewManualClock(0, "rate-header-clock")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewRateOwner(RateOwnerConfig{
		Identity: RateOwnerIdentity{Venue: "synthetic-venue", API: "public-v1", ScopeKind: RateScopeIP, ScopeID: "public-ip-pool"},
		Policy: RatePolicy{
			Capacity: 10, RefillTokens: 10, RefillIntervalNS: 100, RequestCost: 1, MaxAttempts: 2,
			DefaultRetryAfterNS: 10, MaxRetryAfterNS: 100, CircuitOpenNS: 100,
			RetryableStatusCodes: []int{429}, TerminalStatusCodes: []int{403}, CircuitStatusCodes: []int{418},
		},
		MaxOperations: 4,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	first := RateOperation{ID: "first", Kind: RateOperationRequest, Cost: 1, MaximumAttempts: 1, DeadlineMonotonicNS: 1_000}
	if attempt, err := owner.Attempt(first); err != nil || !attempt.Allowed {
		t.Fatalf("first admission = %+v, %v", attempt, err)
	}
	if _, err := owner.Complete(first.ID, RateResponse{
		Status: 200, Remaining: 0, RemainingPresent: true, ResetAfterNS: 100, Certainty: RateOutcomeKnown,
	}); err != nil {
		t.Fatal(err)
	}
	second := RateOperation{ID: "second", Kind: RateOperationRequest, Cost: 1, MaximumAttempts: 1, DeadlineMonotonicNS: 1_000}
	if attempt, err := owner.Attempt(second); err != nil || attempt.Allowed || attempt.Reason != BudgetRetryAfter || attempt.RetryAtMonotonic != 100 {
		t.Fatalf("authoritative zero remaining was ignored: %+v, %v", attempt, err)
	}
	if err := clock.Advance(100); err != nil {
		t.Fatal(err)
	}
	if attempt, err := owner.Attempt(second); err != nil || !attempt.Allowed {
		t.Fatalf("admission did not resume after authoritative reset: %+v, %v", attempt, err)
	}
}
