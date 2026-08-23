package binance

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/orderbook"
)

type spotBookTestFetchStep struct {
	observation orderbook.SnapshotObservation
	err         error
	after       func()
}

type spotBookTestFetcher struct {
	steps []spotBookTestFetchStep
	calls int
}

func (f *spotBookTestFetcher) Fetch(ctx context.Context) (orderbook.SnapshotObservation, error) {
	if err := ctx.Err(); err != nil {
		return orderbook.SnapshotObservation{}, err
	}
	if f.calls >= len(f.steps) {
		return orderbook.SnapshotObservation{}, errors.New("snapshot fake exhausted")
	}
	step := f.steps[f.calls]
	f.calls++
	if step.after != nil {
		step.after()
	}
	return step.observation, step.err
}

type spotBookTestLevel struct {
	price  string
	amount string
}

func TestBinanceSpotBookSnapshotBehindBoundaryOverflowSafe(t *testing.T) {
	policy := spotBookSequencePolicy{}
	maximum := ^uint64(0)
	for _, test := range []struct {
		name          string
		snapshotLast  uint64
		firstBuffered uint64
		wantBehind    bool
	}{
		{name: "adjacent is eligible", snapshotLast: 100, firstBuffered: 101, wantBehind: false},
		{name: "strictly behind refetches", snapshotLast: 100, firstBuffered: 102, wantBehind: true},
		{name: "maximum cannot overflow behind", snapshotLast: maximum, firstBuffered: maximum, wantBehind: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := policy.SnapshotBehind(test.snapshotLast, test.firstBuffered); got != test.wantBehind {
				t.Fatalf("SnapshotBehind(%d, %d) = %t, want %t", test.snapshotLast, test.firstBuffered, got, test.wantBehind)
			}
		})
	}
	maximumUpdate := normalize.BookUpdateV1{FirstSequence: maximum, LastSequence: maximum}
	if got := policy.First(maximum, maximumUpdate); got != orderbook.SequenceStale {
		t.Fatalf("First at MaxUint64 = %v, want stale", got)
	}
}

func TestBinanceSpotBookSnapshotBehindRefetchSpanningAndExactRawRange(t *testing.T) {
	instrument := spotBookTestInstrument()
	behind := spotBookTestSnapshot(t, instrument, 100,
		[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"111", "1"}}, 1, 1)
	bridge := spotBookTestSnapshot(t, instrument, 107,
		[]spotBookTestLevel{{"99", "2"}}, []spotBookTestLevel{{"111", "2"}}, 2, 1)
	fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: behind}, {observation: bridge}}}
	book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
	first := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 105, 110,
		[]spotBookTestLevel{{"100", "3"}}, nil)
	second := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_100, 111, 111,
		nil, []spotBookTestLevel{{"110", "4"}})
	if result, err := book.Accept(t.Context(), first, 250); err != nil || result.State != orderbook.StateBuffering {
		t.Fatalf("first buffer result = %+v, %v", result, err)
	}
	if _, err := book.Accept(t.Context(), second, 260); err != nil {
		t.Fatal(err)
	}
	result, err := book.Seed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fetcher.calls != 2 || result.SnapshotFetches != 2 || result.State != orderbook.StateLive || result.Output == nil {
		t.Fatalf("seed result = %+v; fetches = %d", result, fetcher.calls)
	}
	output := result.Output
	if output.LastSequence != 111 || output.InitialSnapshotID != bridge.Identity ||
		output.InitialSnapshotCoordinate != bridge.RawCoordinate || output.InputRange.Count != 2 ||
		output.InputRange.First != metadataBookCoordinate(first.Metadata) ||
		output.InputRange.Last != metadataBookCoordinate(second.Metadata) ||
		output.CatalogSnapshotID != spotBookTestCatalogID() || output.MapperVersion != SpotMapperVersion ||
		output.MapperBindingID != spotBookTestBindingID() || output.PolicyID != book.PolicyID() ||
		output.InputRange.Hash == (normalize.Hash{}) {
		t.Fatalf("output lineage = %+v", output)
	}
	if err := output.Validate(); err != nil {
		t.Fatalf("BookSnapshotV1 validation: %v", err)
	}
	evidence, ok := book.CurrentEvidence()
	if !ok || len(evidence.SnapshotAttempts) != 2 || !evidence.SnapshotAttempts[0].Behind || evidence.SnapshotAttempts[1].Behind ||
		!slices.Equal(spotBookTestStates(evidence.Transitions), []orderbook.State{
			orderbook.StateUnseeded, orderbook.StateBuffering, orderbook.StateSeeded, orderbook.StateLive,
		}) {
		t.Fatalf("epoch evidence = %+v", evidence)
	}
}

func TestBinanceSpotBookStaleDuplicateAndForwardGap(t *testing.T) {
	instrument := spotBookTestInstrument()
	snapshot := spotBookTestSnapshot(t, instrument, 100,
		[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
	fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}}
	book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
	seed := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 102, nil, nil)
	if _, err := book.Accept(t.Context(), seed, 100); err != nil {
		t.Fatal(err)
	}
	seeded, err := book.Seed(t.Context())
	if err != nil || seeded.Output == nil {
		t.Fatalf("seed = %+v, %v", seeded, err)
	}
	stale := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_100, 99, 101,
		[]spotBookTestLevel{{"99", "999"}}, nil)
	staleResult, err := book.Accept(t.Context(), stale, 100)
	if err != nil || !staleResult.IgnoredStale || staleResult.Output != nil {
		t.Fatalf("stale result = %+v, %v", staleResult, err)
	}
	duplicate := spotBookTestUpdate(t, instrument, [16]byte{1}, 3, 1_200, 101, 102,
		[]spotBookTestLevel{{"99", "888"}}, nil)
	duplicateResult, err := book.Accept(t.Context(), duplicate, 100)
	if err != nil || !duplicateResult.IgnoredStale || duplicateResult.Output != nil {
		t.Fatalf("duplicate result = %+v, %v", duplicateResult, err)
	}
	gap := spotBookTestUpdate(t, instrument, [16]byte{1}, 4, 1_300, 105, 105,
		[]spotBookTestLevel{{"100", "5"}}, nil)
	gapResult, err := book.Accept(t.Context(), gap, 100)
	if !errors.Is(err, orderbook.ErrSequenceGap) || gapResult.State != orderbook.StateBuffering || gapResult.Output != nil ||
		len(gapResult.ClosedEpochs) != 1 || gapResult.ClosedEpochs[0].CloseReason != orderbook.CloseForwardGap ||
		gapResult.ClosedEpochs[0].LastAcceptedRaw != metadataBookCoordinate(duplicate.Metadata) {
		t.Fatalf("gap result = %+v, %v", gapResult, err)
	}
	current, ok := book.CurrentEvidence()
	if !ok || current.ParentEpochID != gapResult.ClosedEpochs[0].ReconstructionEpochID ||
		current.FirstObservedRaw != metadataBookCoordinate(gap.Metadata) || current.State != orderbook.StateBuffering {
		t.Fatalf("post-gap epoch = %+v", current)
	}
}

func TestBinanceSpotBookSnapshotRaceSourceOrderAndZeroDelete(t *testing.T) {
	instrument := spotBookTestInstrument()
	snapshot := spotBookTestSnapshot(t, instrument, 100,
		[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
	fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}}
	book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
	first := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101,
		[]spotBookTestLevel{{"99", "2"}, {"99", "0"}, {"98", "3"}}, nil)
	second := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_100, 102, 103,
		nil, []spotBookTestLevel{{"101", "0"}, {"102", "4"}})
	if _, err := book.Accept(t.Context(), first, 200); err != nil {
		t.Fatal(err)
	}
	if _, err := book.Accept(t.Context(), second, 200); err != nil {
		t.Fatal(err)
	}
	result, err := book.Seed(t.Context())
	if err != nil || result.Output == nil {
		t.Fatalf("seed = %+v, %v", result, err)
	}
	if result.Output.InputRange.Count != 2 || result.Output.LastSequence != 103 ||
		spotBookTestAmount(result.Output.Bids, "99") != "" || spotBookTestAmount(result.Output.Bids, "98") != spotBookTestCoefficient(t, "3", normalize.CanonicalAmountScale) ||
		spotBookTestAmount(result.Output.Asks, "101") != "" || spotBookTestAmount(result.Output.Asks, "102") != spotBookTestCoefficient(t, "4", normalize.CanonicalAmountScale) {
		t.Fatalf("source-order/zero-delete output = %+v", result.Output)
	}
}

func TestBinanceSpotBookBufferSnapshotLevelAndOutputBounds(t *testing.T) {
	instrument := spotBookTestInstrument()
	t.Run("message overflow", func(t *testing.T) {
		bounds := DefaultSpotBookBounds()
		bounds.MaxBufferedMessages = 1
		book := spotBookTestNew(t, instrument, bounds, &spotBookTestFetcher{})
		first := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 1, 1, nil, nil)
		trigger := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_001, 2, 2, nil, nil)
		if _, err := book.Accept(t.Context(), first, 10); err != nil {
			t.Fatal(err)
		}
		result, err := book.Accept(t.Context(), trigger, 10)
		if !errors.Is(err, orderbook.ErrBufferMessages) || result.State != orderbook.StateClosed ||
			len(result.ClosedEpochs) != 1 || result.ClosedEpochs[0].CloseReason != orderbook.CloseBufferOverflow ||
			!result.ClosedEpochs[0].HasObservedRaw ||
			result.ClosedEpochs[0].FirstObservedRaw != metadataBookCoordinate(first.Metadata) ||
			result.ClosedEpochs[0].LastObservedRaw != metadataBookCoordinate(trigger.Metadata) ||
			result.ClosedEpochs[0].HasAcceptedRaw || result.ClosedEpochs[0].AppliedMessages != 0 {
			t.Fatalf("message overflow evidence = %+v, %v", result, err)
		}
	})
	t.Run("byte overflow", func(t *testing.T) {
		bounds := DefaultSpotBookBounds()
		bounds.MaxBufferedBytes = 9
		book := spotBookTestNew(t, instrument, bounds, &spotBookTestFetcher{})
		trigger := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 1, 1, nil, nil)
		result, err := book.Accept(t.Context(), trigger, 10)
		if !errors.Is(err, orderbook.ErrBufferBytes) || result.State != orderbook.StateClosed ||
			len(result.ClosedEpochs) != 1 || !result.ClosedEpochs[0].HasObservedRaw ||
			result.ClosedEpochs[0].FirstObservedRaw != metadataBookCoordinate(trigger.Metadata) ||
			result.ClosedEpochs[0].LastObservedRaw != metadataBookCoordinate(trigger.Metadata) ||
			result.ClosedEpochs[0].HasAcceptedRaw {
			t.Fatalf("byte overflow evidence = %+v, %v", result, err)
		}
	})
	t.Run("time overflow", func(t *testing.T) {
		bounds := DefaultSpotBookBounds()
		bounds.MaxBufferSpanNS = 10
		book := spotBookTestNew(t, instrument, bounds, &spotBookTestFetcher{})
		first := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 1, 1, nil, nil)
		trigger := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_011, 2, 2, nil, nil)
		if _, err := book.Accept(t.Context(), first, 1); err != nil {
			t.Fatal(err)
		}
		result, err := book.Accept(t.Context(), trigger, 1)
		if !errors.Is(err, orderbook.ErrBufferTime) || result.State != orderbook.StateClosed ||
			len(result.ClosedEpochs) != 1 ||
			result.ClosedEpochs[0].FirstObservedRaw != metadataBookCoordinate(first.Metadata) ||
			result.ClosedEpochs[0].LastObservedRaw != metadataBookCoordinate(trigger.Metadata) ||
			result.ClosedEpochs[0].HasAcceptedRaw {
			t.Fatalf("time overflow evidence = %+v, %v", result, err)
		}
	})
	t.Run("snapshot refetch overflow", func(t *testing.T) {
		bounds := DefaultSpotBookBounds()
		bounds.MaxSnapshotFetches = 1
		behind := spotBookTestSnapshot(t, instrument, 1, nil, nil, 1, 1)
		fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: behind}}}
		book := spotBookTestNew(t, instrument, bounds, fetcher)
		if _, err := book.Accept(t.Context(), spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 5, 5, nil, nil), 1); err != nil {
			t.Fatal(err)
		}
		result, err := book.Seed(t.Context())
		if !errors.Is(err, orderbook.ErrSnapshotLimit) || result.State != orderbook.StateClosed || fetcher.calls != 1 {
			t.Fatalf("refetch overflow = %+v, %v; calls = %d", result, err, fetcher.calls)
		}
	})
	t.Run("snapshot byte overflow", func(t *testing.T) {
		snapshot := spotBookTestSnapshot(t, instrument, 100,
			[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
		bounds := DefaultSpotBookBounds()
		bounds.MaxSnapshotBytes = snapshot.PayloadBytes - 1
		book := spotBookTestNew(t, instrument, bounds, &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}})
		update := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil)
		if _, err := book.Accept(t.Context(), update, 1); err != nil {
			t.Fatal(err)
		}
		result, err := book.Seed(t.Context())
		if !errors.Is(err, orderbook.ErrSnapshotBytes) || errors.Is(err, orderbook.ErrLevelLimit) ||
			result.State != orderbook.StateClosed || len(result.ClosedEpochs) != 1 ||
			result.ClosedEpochs[0].CloseReason != orderbook.CloseSnapshotBytes {
			t.Fatalf("snapshot byte overflow = %+v, %v", result, err)
		}
	})
	t.Run("level overflow", func(t *testing.T) {
		bounds := DefaultSpotBookBounds()
		bounds.MaxLevelsPerSide = 1
		snapshot := spotBookTestSnapshot(t, instrument, 100,
			[]spotBookTestLevel{{"98", "1"}, {"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
		book := spotBookTestNew(t, instrument, bounds, &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}})
		if _, err := book.Accept(t.Context(), spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil), 1); err != nil {
			t.Fatal(err)
		}
		result, err := book.Seed(t.Context())
		if !errors.Is(err, orderbook.ErrLevelLimit) || result.State != orderbook.StateClosed {
			t.Fatalf("level overflow = %+v, %v", result, err)
		}
	})
	t.Run("update level validation closes as level limit", func(t *testing.T) {
		bounds := DefaultSpotBookBounds()
		bounds.MaxLevelsPerSide = 1
		snapshot := spotBookTestSnapshot(t, instrument, 100,
			[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 2, 1)
		book := spotBookTestNew(t, instrument, bounds, &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}})
		seed := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil)
		if _, err := book.Accept(t.Context(), seed, 1); err != nil {
			t.Fatal(err)
		}
		if result, err := book.Seed(t.Context()); err != nil || result.Output == nil {
			t.Fatalf("seed = %+v, %v", result, err)
		}
		trigger := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_100, 102, 102,
			[]spotBookTestLevel{{"98", "1"}, {"99", "2"}}, nil)
		result, err := book.Accept(t.Context(), trigger, 1)
		if !errors.Is(err, orderbook.ErrLevelLimit) || result.State != orderbook.StateClosed ||
			len(result.ClosedEpochs) != 1 || result.ClosedEpochs[0].CloseReason != orderbook.CloseLevelLimit {
			t.Fatalf("update level bound = %+v, %v", result, err)
		}
	})
	t.Run("output overflow", func(t *testing.T) {
		bounds := DefaultSpotBookBounds()
		bounds.MaxOutputs = 1
		snapshot := spotBookTestSnapshot(t, instrument, 100,
			[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
		book := spotBookTestNew(t, instrument, bounds, &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}})
		seedUpdate := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil)
		if _, err := book.Accept(t.Context(), seedUpdate, 1); err != nil {
			t.Fatal(err)
		}
		initial, err := book.Seed(t.Context())
		if err != nil || initial.Output == nil {
			t.Fatalf("initial output = %+v, %v", initial, err)
		}
		trigger := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_100, 102, 102,
			[]spotBookTestLevel{{"99", "2"}}, nil)
		result, err := book.Accept(t.Context(), trigger, 1)
		if !errors.Is(err, orderbook.ErrOutputLimit) || result.State != orderbook.StateClosed || result.Output != nil ||
			len(result.ClosedEpochs) != 1 || result.ClosedEpochs[0].CloseReason != orderbook.CloseOutputLimit ||
			result.ClosedEpochs[0].LastObservedRaw != metadataBookCoordinate(trigger.Metadata) ||
			result.ClosedEpochs[0].LastAcceptedRaw != metadataBookCoordinate(seedUpdate.Metadata) ||
			result.ClosedEpochs[0].AcceptedMessages != 1 || result.ClosedEpochs[0].AppliedMessages != 1 ||
			result.ClosedEpochs[0].LastSequence != 101 || result.ClosedEpochs[0].OutputCount != 1 ||
			result.ClosedEpochs[0].LastOutputHash != initial.Output.ProjectionHash {
			t.Fatalf("output overflow evidence = %+v, %v", result, err)
		}
	})
}

func TestBinanceSpotBookReconnectCreatesEpochAndReplacesSnapshot(t *testing.T) {
	instrument := spotBookTestInstrument()
	firstSnapshot := spotBookTestSnapshot(t, instrument, 100,
		[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
	secondSnapshot := spotBookTestSnapshot(t, instrument, 200,
		[]spotBookTestLevel{{"90", "2"}}, []spotBookTestLevel{{"110", "2"}}, 2, 1)
	fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: firstSnapshot}, {observation: secondSnapshot}}}
	book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
	first := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil)
	if _, err := book.Accept(t.Context(), first, 1); err != nil {
		t.Fatal(err)
	}
	firstSeed, err := book.Seed(t.Context())
	if err != nil || firstSeed.Output == nil {
		t.Fatalf("first seed = %+v, %v", firstSeed, err)
	}
	second := spotBookTestUpdate(t, instrument, [16]byte{2}, 1, 2_000, 201, 201,
		[]spotBookTestLevel{{"91", "3"}}, nil)
	reconnect, err := book.Accept(t.Context(), second, 1)
	if err != nil || reconnect.Output != nil || reconnect.State != orderbook.StateBuffering || len(reconnect.ClosedEpochs) != 1 ||
		reconnect.ClosedEpochs[0].CloseReason != orderbook.CloseReconnect {
		t.Fatalf("reconnect = %+v, %v", reconnect, err)
	}
	secondSeed, err := book.Seed(t.Context())
	if err != nil || secondSeed.Output == nil {
		t.Fatalf("second seed = %+v, %v", secondSeed, err)
	}
	if secondSeed.Output.ReconstructionEpochID == firstSeed.Output.ReconstructionEpochID ||
		spotBookTestAmount(secondSeed.Output.Bids, "99") != "" || spotBookTestAmount(secondSeed.Output.Bids, "90") == "" ||
		spotBookTestAmount(secondSeed.Output.Bids, "91") == "" {
		t.Fatalf("snapshot replacement = %+v", secondSeed.Output)
	}
	current, ok := book.CurrentEvidence()
	if !ok || current.ParentEpochID != reconnect.ClosedEpochs[0].ReconstructionEpochID ||
		current.FirstObservedRaw != metadataBookCoordinate(second.Metadata) {
		t.Fatalf("reconnect epoch evidence = %+v", current)
	}
}

func TestBinanceSpotBookCancellationClosesWithoutFetchOrStaleOutput(t *testing.T) {
	instrument := spotBookTestInstrument()
	fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: spotBookTestSnapshot(t, instrument, 100, nil, nil, 1, 1)}}}
	book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
	if _, err := book.Accept(t.Context(), spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := book.Seed(ctx)
	if !errors.Is(err, context.Canceled) || result.State != orderbook.StateClosed || result.Output != nil || fetcher.calls != 0 ||
		len(result.ClosedEpochs) != 1 || result.ClosedEpochs[0].CloseReason != orderbook.CloseCancelled {
		t.Fatalf("cancel = %+v, %v; calls = %d", result, err, fetcher.calls)
	}
	if _, err := book.Seed(t.Context()); !errors.Is(err, orderbook.ErrClosed) {
		t.Fatalf("post-cancel seed error = %v", err)
	}
}

func TestBinanceSpotBookCancellationAfterFetchClosesBeforeSnapshotApply(t *testing.T) {
	instrument := spotBookTestInstrument()
	ctx, cancel := context.WithCancel(t.Context())
	snapshot := spotBookTestSnapshot(t, instrument, 100,
		[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
	fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{
		observation: snapshot,
		after:       cancel,
	}}}
	book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
	update := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil)
	if _, err := book.Accept(t.Context(), update, 1); err != nil {
		t.Fatal(err)
	}
	result, err := book.Seed(ctx)
	if !errors.Is(err, context.Canceled) || result.State != orderbook.StateClosed || result.Output != nil ||
		len(result.ClosedEpochs) != 1 || result.ClosedEpochs[0].CloseReason != orderbook.CloseCancelled ||
		result.ClosedEpochs[0].HasInitialSnapshot || result.ClosedEpochs[0].OutputCount != 0 ||
		result.ClosedEpochs[0].SnapshotFetches != 1 || len(result.ClosedEpochs[0].SnapshotAttempts) != 0 {
		t.Fatalf("post-fetch cancellation = %+v, %v", result, err)
	}
}

func TestBinanceSpotBookExplicitRawDiscontinuityStartsNewEpoch(t *testing.T) {
	instrument := spotBookTestInstrument()
	firstSnapshot := spotBookTestSnapshot(t, instrument, 100,
		[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
	secondSnapshot := spotBookTestSnapshot(t, instrument, 200,
		[]spotBookTestLevel{{"90", "1"}}, []spotBookTestLevel{{"110", "1"}}, 2, 1)
	fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: firstSnapshot}, {observation: secondSnapshot}}}
	book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
	first := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil)
	if _, err := book.Accept(t.Context(), first, 1); err != nil {
		t.Fatal(err)
	}
	firstSeed, err := book.Seed(t.Context())
	if err != nil || firstSeed.Output == nil {
		t.Fatalf("first seed = %+v, %v", firstSeed, err)
	}
	discontinuity, err := book.Discontinuity(t.Context())
	if err != nil || discontinuity.State != orderbook.StateClosed || len(discontinuity.ClosedEpochs) != 1 ||
		discontinuity.ClosedEpochs[0].CloseReason != orderbook.CloseRawDiscontinuity {
		t.Fatalf("discontinuity = %+v, %v", discontinuity, err)
	}
	afterBlindInterval := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 2_000, 201, 201,
		[]spotBookTestLevel{{"91", "2"}}, nil)
	buffered, err := book.Accept(t.Context(), afterBlindInterval, 1)
	if err != nil || buffered.State != orderbook.StateBuffering || buffered.Output != nil {
		t.Fatalf("post-discontinuity buffer = %+v, %v", buffered, err)
	}
	secondSeed, err := book.Seed(t.Context())
	if err != nil || secondSeed.Output == nil {
		t.Fatalf("second seed = %+v, %v", secondSeed, err)
	}
	if secondSeed.Output.ReconstructionEpochID == firstSeed.Output.ReconstructionEpochID ||
		secondSeed.Output.InputRange.First != metadataBookCoordinate(afterBlindInterval.Metadata) ||
		secondSeed.Output.InputRange.Count != 1 {
		t.Fatalf("blind interval was filled or epoch reused: %+v", secondSeed.Output)
	}
	current, ok := book.CurrentEvidence()
	if !ok || current.ParentEpochID != discontinuity.ClosedEpochs[0].ReconstructionEpochID {
		t.Fatalf("post-discontinuity evidence = %+v", current)
	}
}

func TestBinanceSpotBookRejectsCrossedAndNegativeLevelsAtomically(t *testing.T) {
	instrument := spotBookTestInstrument()
	t.Run("crossed snapshot", func(t *testing.T) {
		snapshot := spotBookTestSnapshot(t, instrument, 100,
			[]spotBookTestLevel{{"102", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
		book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}})
		if _, err := book.Accept(t.Context(), spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil), 1); err != nil {
			t.Fatal(err)
		}
		result, err := book.Seed(t.Context())
		if !errors.Is(err, orderbook.ErrCrossedBook) || result.Output != nil || result.State != orderbook.StateClosed ||
			len(result.ClosedEpochs) != 1 || result.ClosedEpochs[0].CloseReason != orderbook.CloseCrossedBook {
			t.Fatalf("crossed snapshot = %+v, %v", result, err)
		}
	})
	t.Run("negative normalized update", func(t *testing.T) {
		book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), &spotBookTestFetcher{})
		negative := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 1, 1,
			[]spotBookTestLevel{{"99", "-1"}}, nil)
		result, err := book.Accept(t.Context(), negative, 1)
		if !errors.Is(err, orderbook.ErrNegativeLevel) || result.Output != nil || result.State != orderbook.StateClosed {
			t.Fatalf("negative update = %+v, %v", result, err)
		}
	})
	t.Run("crossed live event", func(t *testing.T) {
		snapshot := spotBookTestSnapshot(t, instrument, 100,
			[]spotBookTestLevel{{"99", "1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1)
		book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}})
		if _, err := book.Accept(t.Context(), spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 101, nil, nil), 1); err != nil {
			t.Fatal(err)
		}
		seed, err := book.Seed(t.Context())
		if err != nil || seed.Output == nil {
			t.Fatalf("seed = %+v, %v", seed, err)
		}
		cross := spotBookTestUpdate(t, instrument, [16]byte{1}, 2, 1_100, 102, 102,
			[]spotBookTestLevel{{"102", "1"}}, nil)
		result, err := book.Accept(t.Context(), cross, 1)
		if !errors.Is(err, orderbook.ErrCrossedBook) || result.Output != nil || result.State != orderbook.StateClosed {
			t.Fatalf("crossed live = %+v, %v", result, err)
		}
	})
	t.Run("negative REST snapshot is rejected before observation", func(t *testing.T) {
		_, _, err := spotBookTestParseSnapshot(t, instrument, 100,
			[]spotBookTestLevel{{"99", "-1"}}, []spotBookTestLevel{{"101", "1"}}, 1, 1, true)
		if err == nil {
			t.Fatal("negative REST snapshot unexpectedly accepted")
		}
	})
}

func TestBinanceSpotBookDeterministicHashesAndImmutableProjection(t *testing.T) {
	instrument := spotBookTestInstrument()
	snapshot := spotBookTestSnapshot(t, instrument, 100,
		[]spotBookTestLevel{{"98", "1"}, {"99", "2"}}, []spotBookTestLevel{{"102", "1"}, {"101", "2"}}, 1, 1)
	update := spotBookTestUpdate(t, instrument, [16]byte{1}, 1, 1_000, 101, 103,
		[]spotBookTestLevel{{"99", "3"}}, []spotBookTestLevel{{"101", "4"}})
	build := func() *orderbook.BookSnapshotV1 {
		fetcher := &spotBookTestFetcher{steps: []spotBookTestFetchStep{{observation: snapshot}}}
		book := spotBookTestNew(t, instrument, DefaultSpotBookBounds(), fetcher)
		if _, err := book.Accept(t.Context(), update, 123); err != nil {
			t.Fatal(err)
		}
		result, err := book.Seed(t.Context())
		if err != nil || result.Output == nil {
			t.Fatalf("build = %+v, %v", result, err)
		}
		return result.Output
	}
	first := build()
	second := build()
	if first.ProjectionHash != second.ProjectionHash || first.InputRange.Hash != second.InputRange.Hash ||
		first.ReconstructionEpochID != second.ReconstructionEpochID || first.PolicyID != second.PolicyID ||
		!slices.Equal(first.Bids, second.Bids) || !slices.Equal(first.Asks, second.Asks) {
		t.Fatalf("non-deterministic outputs:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if spotBookTestAmount(second.Bids, "99") != spotBookTestCoefficient(t, "3", normalize.CanonicalAmountScale) {
		t.Fatalf("absolute replacement amount = %s", spotBookTestAmount(second.Bids, "99"))
	}
	original := second.Bids[0].Amount.Coefficient
	first.Bids[0].Amount.Coefficient = "999"
	if second.Bids[0].Amount.Coefficient != original {
		t.Fatal("returned output aliases another reconstruction")
	}
}

func TestBinanceSpotBookSnapshotParserPreservesExactRawCoordinate(t *testing.T) {
	instrument := spotBookTestInstrument()
	observation, record, err := spotBookTestParseSnapshot(t, instrument, 500,
		[]spotBookTestLevel{{"499", "2"}}, []spotBookTestLevel{{"501", "3"}}, 9, 7, false)
	if err != nil {
		t.Fatal(err)
	}
	if observation.RawCoordinate != record.Coordinate || observation.LastSequence != 500 ||
		observation.Identity == (normalize.Hash{}) || observation.PayloadBytes != uint64(len(record.Envelope.RawPayload)) {
		t.Fatalf("snapshot observation = %+v; raw = %+v", observation, record.Coordinate)
	}
	missingSymbol := record
	missingSymbol.Envelope.NativeSymbol = capture.OptionalString{}
	if _, err := ParseSpotBookSnapshot(missingSymbol, instrument); !errors.Is(err, ErrSpotBook) {
		t.Fatalf("missing native symbol error = %v", err)
	}
	mismatchedSymbol := record
	mismatchedSymbol.Envelope.NativeSymbol = capture.OptionalString{Value: "ETHUSDT", Valid: true}
	if _, err := ParseSpotBookSnapshot(mismatchedSymbol, instrument); !errors.Is(err, ErrSpotBook) {
		t.Fatalf("mismatched native symbol error = %v", err)
	}
	emptyNativeID := instrument
	emptyNativeID.NativeID = ""
	if _, err := ParseSpotBookSnapshot(record, emptyNativeID); !errors.Is(err, ErrSpotBook) {
		t.Fatalf("empty instrument native ID error = %v", err)
	}
	copyOfObservation := observation
	copyOfObservation.Bids = slices.Clone(observation.Bids)
	copyOfObservation.Bids[0].Amount.Decimal.Coefficient = "7"
	if copyOfObservation.Identity == observation.Identity {
		if err := copyOfObservation.Validate(); !errors.Is(err, orderbook.ErrInvalidSnapshot) {
			t.Fatalf("mutated snapshot validation = %v", err)
		}
	}
}

func spotBookTestNew(t *testing.T, instrument normalize.InstrumentIdentity, bounds orderbook.Bounds, fetcher orderbook.SnapshotFetcher) *SpotBook {
	t.Helper()
	book, err := NewSpotBook(SpotBookConfig{
		Instrument:        instrument,
		CatalogSnapshotID: spotBookTestCatalogID(),
		MapperVersion:     SpotMapperVersion,
		MapperBindingID:   spotBookTestBindingID(),
		Bounds:            bounds,
	}, fetcher)
	if err != nil {
		t.Fatal(err)
	}
	return book
}

func spotBookTestInstrument() normalize.InstrumentIdentity {
	return normalize.InstrumentIdentity{
		InstrumentUID: "binance-spot-btcusdt-generation-1",
		NativeID:      "BTCUSDT",
		BaseAssetID:   "BTC",
		QuoteAssetID:  "USDT",
	}
}

func spotBookTestCatalogID() normalize.Hash {
	return normalize.Hash(sha256.Sum256([]byte("spot-book-catalog-v1")))
}

func spotBookTestBindingID() normalize.Hash {
	return normalize.Hash(sha256.Sum256([]byte("spot-book-mapper-binding-v1")))
}

func spotBookTestUpdate(t *testing.T, instrument normalize.InstrumentIdentity, epoch [16]byte, arrival uint64, received int64,
	first, last uint64, bids, asks []spotBookTestLevel) normalize.BookUpdateV1 {
	t.Helper()
	wireBids := make([][2]string, len(bids))
	for index, level := range bids {
		wireBids[index] = [2]string{level.price, level.amount}
	}
	wireAsks := make([][2]string, len(asks))
	for index, level := range asks {
		wireAsks[index] = [2]string{level.price, level.amount}
	}
	payload, err := json.Marshal(struct {
		Event string      `json:"e"`
		First uint64      `json:"U"`
		Last  uint64      `json:"u"`
		Bids  [][2]string `json:"b"`
		Asks  [][2]string `json:"a"`
	}{Event: "depthUpdate", First: first, Last: last, Bids: wireBids, Asks: wireAsks})
	if err != nil {
		t.Fatal(err)
	}
	envelope := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindWebSocket,
		SourceID:                   SpotSourceID,
		ChannelOrEndpoint:          SpotRawChannel,
		ConnectionEpoch:            capture.OptionalEpoch{Value: epoch, Valid: true},
		ArrivalOrdinal:             arrival,
		MessageOrdinal:             0,
		ReceivedWallTimeNS:         received,
		ClockEpochID:               "spot-book-test-clock",
		MonotonicNSSinceClockEpoch: arrival,
		PayloadEncoding:            capture.PayloadEncodingJSON,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "spot-book-test-recorder-v1",
	}
	envelope.SetRawPayload(payload)
	segment := normalize.Hash(sha256.Sum256([]byte(fmt.Sprintf("spot-book-update-segment-%x-%d", epoch, arrival))))
	record, err := normalize.BindRawRecord(envelope, segment, arrival, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{
		Record:                  record,
		SchemaName:              normalize.BookUpdateSchemaName,
		SchemaVersion:           normalize.BookUpdateSchemaVersion,
		InstrumentUID:           instrument.InstrumentUID,
		ExchangeTimeNS:          normalize.OptionalInt64{Value: received, Valid: true},
		ExchangeTimeResolution:  normalize.ResolutionMillisecond,
		SourceEventTimeNS:       normalize.OptionalInt64{Value: received, Valid: true},
		SourceTimeResolution:    normalize.ResolutionMillisecond,
		SourceSchemaFingerprint: normalize.Hash(sha256.Sum256([]byte("spot-book-depth-schema-v1"))),
		MapperVersion:           SpotMapperVersion,
		MapperBindingID:         spotBookTestBindingID(),
		CatalogSnapshotID:       spotBookTestCatalogID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	event := normalize.BookUpdateV1{
		Metadata:                  metadata,
		UpdateKind:                normalize.UpdateDelta,
		DepthContract:             "diff_depth",
		AggregationContract:       "100ms",
		FirstSequence:             first,
		LastSequence:              last,
		Checksum:                  normalize.SourceMissing,
		Bids:                      spotBookTestNormalizedLevels(t, instrument, normalize.SideBuy, bids),
		Asks:                      spotBookTestNormalizedLevels(t, instrument, normalize.SideSell, asks),
		AmountSemantics:           "absolute_base_asset_quantity",
		ReconstructionEligibility: "eligible_with_rest_snapshot_bridge",
	}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	return event
}

func spotBookTestNormalizedLevels(t *testing.T, instrument normalize.InstrumentIdentity, side normalize.Side, levels []spotBookTestLevel) []normalize.BookLevel {
	t.Helper()
	result := make([]normalize.BookLevel, len(levels))
	for index, level := range levels {
		price := spotBookTestDecimal(t, level.price, normalize.CanonicalPriceScale)
		amount := spotBookTestDecimal(t, level.amount, normalize.CanonicalAmountScale)
		action := normalize.LevelUpsert
		if amount.IsZero() {
			action = normalize.LevelDelete
		}
		result[index] = normalize.BookLevel{
			Side:         side,
			LevelOrdinal: uint32(index),
			Action:       action,
			Price:        normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)},
			Amount:       normalize.Numeric{Decimal: amount, Unit: normalize.BaseAssetUnit(instrument.BaseAssetID)},
		}
	}
	return result
}

func spotBookTestSnapshot(t *testing.T, instrument normalize.InstrumentIdentity, last uint64,
	bids, asks []spotBookTestLevel, pollByte byte, arrival uint64) orderbook.SnapshotObservation {
	t.Helper()
	observation, _, err := spotBookTestParseSnapshot(t, instrument, last, bids, asks, pollByte, arrival, false)
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

func spotBookTestParseSnapshot(t *testing.T, instrument normalize.InstrumentIdentity, last uint64,
	bids, asks []spotBookTestLevel, pollByte byte, arrival uint64, wantError bool) (orderbook.SnapshotObservation, normalize.RawRecord, error) {
	t.Helper()
	wireBids := make([][2]string, len(bids))
	for index, level := range bids {
		wireBids[index] = [2]string{level.price, level.amount}
	}
	wireAsks := make([][2]string, len(asks))
	for index, level := range asks {
		wireAsks[index] = [2]string{level.price, level.amount}
	}
	payload, err := json.Marshal(struct {
		Last uint64      `json:"lastUpdateId"`
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
	}{Last: last, Bids: wireBids, Asks: wireAsks})
	if err != nil {
		t.Fatal(err)
	}
	poll := [16]byte{pollByte}
	received := int64(10_000 + arrival)
	envelope := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindREST,
		SourceID:                   SpotSourceID,
		ChannelOrEndpoint:          SpotDepthChannel,
		NativeSymbol:               capture.OptionalString{Value: instrument.NativeID, Valid: true},
		PollCycleID:                capture.OptionalEpoch{Value: poll, Valid: true},
		ArrivalOrdinal:             arrival,
		MessageOrdinal:             0,
		ScheduledAtNS:              capture.OptionalInt64{Value: received - 2, Valid: true},
		RequestStartedAtNS:         capture.OptionalInt64{Value: received - 1, Valid: true},
		RequestCompletedAtNS:       capture.OptionalInt64{Value: received, Valid: true},
		ReceivedWallTimeNS:         received,
		ClockEpochID:               "spot-book-test-clock",
		MonotonicNSSinceClockEpoch: arrival,
		SubscriptionOrRequestID:    capture.OptionalString{Value: fmt.Sprintf("depth-%d-%d", pollByte, arrival), Valid: true},
		HTTPStatusOrWSState:        capture.OptionalString{Value: "200", Valid: true},
		PayloadEncoding:            capture.PayloadEncodingJSON,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "spot-book-test-recorder-v1",
	}
	envelope.SetRawPayload(payload)
	segment := normalize.Hash(sha256.Sum256([]byte(fmt.Sprintf("spot-book-snapshot-segment-%d-%d", pollByte, arrival))))
	record, bindErr := normalize.BindRawRecord(envelope, segment, arrival, nil)
	if bindErr != nil {
		if wantError {
			return orderbook.SnapshotObservation{}, normalize.RawRecord{}, bindErr
		}
		t.Fatal(bindErr)
	}
	observation, parseErr := ParseSpotBookSnapshot(record, instrument)
	return observation, record, parseErr
}

func spotBookTestDecimal(t *testing.T, value string, scale uint8) normalize.Decimal {
	t.Helper()
	decimal, err := normalize.ParseDecimal(value, scale, normalize.DefaultDecimalBounds())
	if err != nil {
		t.Fatal(err)
	}
	return decimal
}

func spotBookTestCoefficient(t *testing.T, value string, scale uint8) string {
	t.Helper()
	return spotBookTestDecimal(t, value, scale).Coefficient
}

func spotBookTestAmount(levels []orderbook.Level, price string) string {
	for _, level := range levels {
		if level.Price.Coefficient == normalizeCoefficient(price, normalize.CanonicalPriceScale) {
			return level.Amount.Coefficient
		}
	}
	return ""
}

func normalizeCoefficient(value string, scale uint8) string {
	decimal, _ := normalize.ParseDecimal(value, scale, normalize.DefaultDecimalBounds())
	return decimal.Coefficient
}

func spotBookTestStates(transitions []orderbook.Transition) []orderbook.State {
	states := make([]orderbook.State, len(transitions))
	for index, transition := range transitions {
		states[index] = transition.State
	}
	return states
}

func metadataBookCoordinate(metadata normalize.Metadata) normalize.RawCoordinate {
	return normalize.RawCoordinate{
		SourceID:         metadata.SourceID,
		ChannelID:        metadata.ChannelID,
		EpochKind:        metadata.EpochKind,
		EpochID:          metadata.EpochID,
		ArrivalOrdinal:   metadata.ArrivalOrdinal,
		MessageOrdinal:   metadata.MessageOrdinal,
		RawSegmentSHA256: metadata.RawSegmentSHA256,
		RawRecordOrdinal: metadata.RawRecordOrdinal,
		RawPayloadSHA256: metadata.RawPayloadSHA256,
	}
}
