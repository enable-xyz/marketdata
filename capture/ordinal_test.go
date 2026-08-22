package capture

import (
	"errors"
	"slices"
	"sync"
	"testing"
)

func TestOrdinal(t *testing.T) {
	t.Run("duplicates retain distinct ordinals", func(t *testing.T) {
		epoch := StreamEpoch{Kind: EpochConnection, ID: epochValue(0x11)}
		assigner := mustOrdinalAssigner(t, "source-api-v1", epoch)
		first := validWebSocketEnvelope()
		second := validWebSocketEnvelope()
		first.ArrivalOrdinal = 0
		second.ArrivalOrdinal = 0
		if first.RawPayloadSHA256 != second.RawPayloadSHA256 {
			t.Fatal("test duplicate payload hashes differ")
		}
		if err := assigner.Assign(&first); err != nil {
			t.Fatalf("first Assign() error = %v", err)
		}
		if err := assigner.Assign(&second); err != nil {
			t.Fatalf("second Assign() error = %v", err)
		}
		if first.ArrivalOrdinal != 1 || second.ArrivalOrdinal != 2 {
			t.Fatalf("duplicate ordinals = (%d, %d), want (1, 2)", first.ArrivalOrdinal, second.ArrivalOrdinal)
		}
		if first.RawPayloadSHA256 != second.RawPayloadSHA256 {
			t.Fatal("assignment changed duplicate hashes")
		}
	})

	t.Run("new source and epoch restart at one", func(t *testing.T) {
		tests := []struct {
			source string
			epoch  StreamEpoch
		}{
			{"source-a", StreamEpoch{Kind: EpochConnection, ID: epochValue(0xf0)}},
			{"source-a", StreamEpoch{Kind: EpochConnection, ID: epochValue(0x01)}},
			{"source-a", StreamEpoch{Kind: EpochPollCycle, ID: epochValue(0xf0)}},
			{"source-b", StreamEpoch{Kind: EpochConnection, ID: epochValue(0xf0)}},
		}
		for i, test := range tests {
			assigner := mustOrdinalAssigner(t, test.source, test.epoch)
			record := ordinalEnvelope(test.source, test.epoch.Kind, test.epoch.ID)
			if err := assigner.Assign(&record); err != nil {
				t.Fatalf("case %d Assign() error = %v", i, err)
			}
			if record.ArrivalOrdinal != 1 {
				t.Fatalf("case %d ordinal = %d, want 1", i, record.ArrivalOrdinal)
			}
		}
	})

	t.Run("batch assignment is transactional", func(t *testing.T) {
		epoch := StreamEpoch{Kind: EpochConnection, ID: epochValue(0x11)}
		assigner := mustOrdinalAssigner(t, "source-a", epoch)
		invalid := []EnvelopeV1{
			ordinalEnvelope("source-a", EpochConnection, epoch.ID),
			ordinalEnvelope("source-a", EpochConnection, epochValue(0x12)),
		}
		if err := assigner.AssignBatch(invalid); !errors.Is(err, ErrInvalidEpoch) {
			t.Fatalf("cross-epoch AssignBatch() error = %v, want ErrInvalidEpoch", err)
		}
		if invalid[0].ArrivalOrdinal != 0 || invalid[1].ArrivalOrdinal != 0 {
			t.Fatalf("failed batch mutated records: (%d, %d)", invalid[0].ArrivalOrdinal, invalid[1].ArrivalOrdinal)
		}

		records := []EnvelopeV1{
			ordinalEnvelope("source-a", EpochConnection, epoch.ID),
			ordinalEnvelope("source-a", EpochConnection, epoch.ID),
			ordinalEnvelope("source-a", EpochConnection, epoch.ID),
		}
		if err := assigner.AssignBatch(records); err != nil {
			t.Fatalf("AssignBatch() error = %v", err)
		}
		for i := range records {
			if records[i].ArrivalOrdinal != uint64(i+1) {
				t.Fatalf("record %d arrival ordinal = %d, want %d", i, records[i].ArrivalOrdinal, i+1)
			}
			if records[i].MessageOrdinal != uint32(i) {
				t.Fatalf("record %d message ordinal = %d, want %d", i, records[i].MessageOrdinal, i)
			}
		}
	})

	t.Run("concurrent assignment is unique and contiguous", func(t *testing.T) {
		const count = 256
		epoch := StreamEpoch{Kind: EpochConnection, ID: epochValue(0x44)}
		assigner := mustOrdinalAssigner(t, "source-concurrent", epoch)
		ordinals := make(chan uint64, count)
		errs := make(chan error, count)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range count {
			workers.Go(func() {
				<-start
				record := ordinalEnvelope("source-concurrent", EpochConnection, epoch.ID)
				if err := assigner.Assign(&record); err != nil {
					errs <- err
					return
				}
				ordinals <- record.ArrivalOrdinal
			})
		}
		close(start)
		workers.Wait()
		close(errs)
		close(ordinals)
		for err := range errs {
			t.Fatalf("concurrent Assign() error = %v", err)
		}
		got := make([]uint64, 0, count)
		for ordinal := range ordinals {
			got = append(got, ordinal)
		}
		slices.Sort(got)
		if len(got) != count {
			t.Fatalf("assigned %d ordinals, want %d", len(got), count)
		}
		for i := range got {
			if got[i] != uint64(i+1) {
				t.Fatalf("sorted ordinal %d = %d, want %d", i, got[i], i+1)
			}
		}
	})

	t.Run("overflow and preassignment do not mutate", func(t *testing.T) {
		epoch := StreamEpoch{Kind: EpochConnection, ID: epochValue(0x55)}
		assigner, err := ResumeOrdinalAssigner("source-overflow", epoch, ^uint64(0))
		if err != nil {
			t.Fatalf("ResumeOrdinalAssigner() error = %v", err)
		}
		record := ordinalEnvelope("source-overflow", EpochConnection, epoch.ID)
		if err := assigner.Assign(&record); !errors.Is(err, ErrOrdinalExhausted) {
			t.Fatalf("Assign() error = %v, want ErrOrdinalExhausted", err)
		}
		if record.ArrivalOrdinal != 0 {
			t.Fatalf("overflow assigned ordinal %d", record.ArrivalOrdinal)
		}

		freshEpoch := StreamEpoch{Kind: EpochConnection, ID: epochValue(0x66)}
		fresh := mustOrdinalAssigner(t, "source-new", freshEpoch)
		already := ordinalEnvelope("source-new", EpochConnection, freshEpoch.ID)
		already.ArrivalOrdinal = 9
		if err := fresh.Assign(&already); !errors.Is(err, ErrOrdinalAlreadyAssigned) {
			t.Fatalf("preassigned Assign() error = %v, want ErrOrdinalAlreadyAssigned", err)
		}
		if already.ArrivalOrdinal != 9 {
			t.Fatalf("failed assignment changed ordinal to %d", already.ArrivalOrdinal)
		}
	})

	t.Run("poll finalization releases identity and rejects reuse", func(t *testing.T) {
		const cycles = 128
		for cycle := range cycles {
			epoch := StreamEpoch{Kind: EpochPollCycle, ID: epochValue(byte(cycle + 1))}
			assigner := mustOrdinalAssigner(t, "scheduled-rest", epoch)
			record := ordinalEnvelope("scheduled-rest", EpochPollCycle, epoch.ID)
			if err := assigner.Assign(&record); err != nil {
				t.Fatalf("cycle %d Assign() error = %v", cycle, err)
			}
			mismatch := EpochCommit{SourceID: "scheduled-rest", Epoch: epoch, LastArrivalOrdinal: 0}
			if err := assigner.Finalize(mismatch); !errors.Is(err, ErrOrdinalCommitMismatch) {
				t.Fatalf("cycle %d mismatched Finalize() error = %v, want ErrOrdinalCommitMismatch", cycle, err)
			}
			commit := EpochCommit{SourceID: "scheduled-rest", Epoch: epoch, LastArrivalOrdinal: 1}
			if err := assigner.Finalize(commit); err != nil {
				t.Fatalf("cycle %d Finalize() error = %v", cycle, err)
			}
			if assigner.source != "" || assigner.epoch != (StreamEpoch{}) || assigner.last != 0 || assigner.initialized || !assigner.finalized {
				t.Fatalf("cycle %d finalized assigner retained active epoch state: %#v", cycle, assigner)
			}
			reused := ordinalEnvelope("scheduled-rest", EpochPollCycle, epochValue(byte(cycle+2)))
			if err := assigner.Assign(&reused); !errors.Is(err, ErrOrdinalFinalized) {
				t.Fatalf("cycle %d reused Assign() error = %v, want ErrOrdinalFinalized", cycle, err)
			}
			if reused.ArrivalOrdinal != 0 {
				t.Fatalf("cycle %d finalized assigner mutated reused ordinal to %d", cycle, reused.ArrivalOrdinal)
			}
			if err := assigner.Finalize(commit); !errors.Is(err, ErrOrdinalFinalized) {
				t.Fatalf("cycle %d repeated Finalize() error = %v, want ErrOrdinalFinalized", cycle, err)
			}
		}
	})

	t.Run("zero value cannot become an untracked epoch", func(t *testing.T) {
		var assigner OrdinalAssigner
		record := ordinalEnvelope("source-a", EpochConnection, epochValue(0x77))
		if err := assigner.Assign(&record); !errors.Is(err, ErrOrdinalUninitialized) {
			t.Fatalf("zero-value Assign() error = %v, want ErrOrdinalUninitialized", err)
		}
	})
}

func mustOrdinalAssigner(t *testing.T, source string, epoch StreamEpoch) *OrdinalAssigner {
	t.Helper()
	assigner, err := NewOrdinalAssigner(source, epoch)
	if err != nil {
		t.Fatalf("NewOrdinalAssigner() error = %v", err)
	}
	return assigner
}

func ordinalEnvelope(source string, kind EpochKind, id [16]byte) EnvelopeV1 {
	envelope := EnvelopeV1{SourceID: source}
	if kind == EpochConnection {
		envelope.ConnectionEpoch = OptionalEpoch{Value: id, Valid: true}
	} else {
		envelope.PollCycleID = OptionalEpoch{Value: id, Valid: true}
	}
	return envelope
}
