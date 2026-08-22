package capture

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrOrdinalExhausted       = errors.New("capture: arrival ordinal exhausted")
	ErrOrdinalAlreadyAssigned = errors.New("capture: arrival ordinal already assigned")
	ErrOrdinalFinalized       = errors.New("capture: ordinal epoch finalized")
	ErrOrdinalCommitMismatch  = errors.New("capture: ordinal epoch commit mismatch")
	ErrOrdinalUninitialized   = errors.New("capture: ordinal assigner is uninitialized")
)

type EpochKind uint8

const (
	EpochConnection EpochKind = iota + 1
	EpochPollCycle
)

// StreamEpoch is an opaque epoch identity. ID bytes are equality keys only;
// ordinal assignment never orders epoch identities or ordinals across them.
type StreamEpoch struct {
	Kind EpochKind
	ID   [16]byte
}

func (e StreamEpoch) Validate() error {
	if e.Kind != EpochConnection && e.Kind != EpochPollCycle {
		return &ValidationError{
			Field:   "epoch kind",
			Problem: fmt.Sprintf("unsupported value %d", e.Kind),
			Cause:   ErrInvalidEpoch,
		}
	}
	if e.ID == [16]byte{} {
		return &ValidationError{
			Field:   "epoch ID",
			Problem: "must not be all zero",
			Cause:   ErrInvalidEpoch,
		}
	}
	return nil
}

// EpochCommit is the committed high-water evidence required to permanently
// finalize one assigner. Finalization succeeds only when the complete assigned
// range has committed for this exact source and epoch.
type EpochCommit struct {
	SourceID           string
	Epoch              StreamEpoch
	LastArrivalOrdinal uint64
}

// OrdinalAssigner owns exactly one active source epoch. State is therefore
// bounded regardless of how many poll cycles a recorder completes. Once
// finalized it clears the source and epoch identity and can never be reused.
type OrdinalAssigner struct {
	mu          sync.Mutex
	source      string
	epoch       StreamEpoch
	last        uint64
	initialized bool
	finalized   bool
}

// NewOrdinalAssigner creates a fresh epoch whose first assignment is one.
func NewOrdinalAssigner(source string, epoch StreamEpoch) (*OrdinalAssigner, error) {
	return newOrdinalAssigner(source, epoch, 0)
}

// ResumeOrdinalAssigner restores the committed high-water mark of one active
// epoch. It never changes the epoch-local numbering rule.
func ResumeOrdinalAssigner(source string, epoch StreamEpoch, last uint64) (*OrdinalAssigner, error) {
	return newOrdinalAssigner(source, epoch, last)
}

func newOrdinalAssigner(source string, epoch StreamEpoch, last uint64) (*OrdinalAssigner, error) {
	if err := validateRequiredString("source_id", source, MaxSourceIDBytes); err != nil {
		return nil, err
	}
	if err := epoch.Validate(); err != nil {
		return nil, err
	}
	return &OrdinalAssigner{
		source:      source,
		epoch:       epoch,
		last:        last,
		initialized: true,
	}, nil
}

// Assign writes the next epoch-local ordinal before returning. Callers must
// enqueue the record only after Assign succeeds.
func (a *OrdinalAssigner) Assign(record *EnvelopeV1) error {
	if record == nil {
		return &ValidationError{Field: "envelope", Problem: "must not be nil", Cause: ErrInvalidEnvelope}
	}
	if err := validateRequiredString("source_id", record.SourceID, MaxSourceIDBytes); err != nil {
		return err
	}
	epoch, err := record.StreamEpoch()
	if err != nil {
		return err
	}
	if err := epoch.Validate(); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkActiveLocked(record.SourceID, epoch); err != nil {
		return err
	}
	if record.ArrivalOrdinal != 0 {
		return &ValidationError{
			Field:   "arrival_ordinal",
			Problem: fmt.Sprintf("already set to %d", record.ArrivalOrdinal),
			Cause:   ErrOrdinalAlreadyAssigned,
		}
	}
	if a.last == ^uint64(0) {
		return &ValidationError{Field: "arrival_ordinal", Problem: "uint64 epoch range is exhausted", Cause: ErrOrdinalExhausted}
	}
	a.last++
	record.ArrivalOrdinal = a.last
	return nil
}

// AssignBatch assigns consecutive arrival ordinals and zero-based message
// ordinals to one received batch as a single transaction. It mutates no record
// unless every record belongs to this assigner's source epoch and the complete
// range is available.
func (a *OrdinalAssigner) AssignBatch(records []EnvelopeV1) error {
	if len(records) == 0 {
		return nil
	}
	if uint64(len(records)) > uint64(^uint32(0))+1 {
		return &ValidationError{Field: "message_ordinal", Problem: "batch has more than uint32 positions", Cause: ErrInvalidEnvelope}
	}
	if err := validateRequiredString("source_id", records[0].SourceID, MaxSourceIDBytes); err != nil {
		return err
	}
	epoch, err := records[0].StreamEpoch()
	if err != nil {
		return err
	}
	if err := epoch.Validate(); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkActiveLocked(records[0].SourceID, epoch); err != nil {
		return err
	}
	for i := range records {
		if records[i].ArrivalOrdinal != 0 {
			return &ValidationError{
				Field:   "arrival_ordinal",
				Problem: fmt.Sprintf("batch record %d is already set to %d", i, records[i].ArrivalOrdinal),
				Cause:   ErrOrdinalAlreadyAssigned,
			}
		}
		candidateEpoch, candidateErr := records[i].StreamEpoch()
		if candidateErr != nil {
			return candidateErr
		}
		if records[i].SourceID != a.source || candidateEpoch != a.epoch {
			return &ValidationError{
				Field:   "stream epoch",
				Problem: fmt.Sprintf("batch record %d crosses the source/epoch boundary", i),
				Cause:   ErrInvalidEpoch,
			}
		}
	}
	count := uint64(len(records))
	if a.last > ^uint64(0)-count {
		return &ValidationError{Field: "arrival_ordinal", Problem: "uint64 epoch range is exhausted", Cause: ErrOrdinalExhausted}
	}
	for i := range records {
		records[i].ArrivalOrdinal = a.last + uint64(i) + 1
		records[i].MessageOrdinal = uint32(i)
	}
	a.last += count
	return nil
}

// Finalize permanently closes the assigner after its exact assigned high-water
// mark has committed. A mismatch leaves the assigner active. Success releases
// all retained source and epoch identity bytes.
func (a *OrdinalAssigner) Finalize(commit EpochCommit) error {
	if err := validateRequiredString("source_id", commit.SourceID, MaxSourceIDBytes); err != nil {
		return err
	}
	if err := commit.Epoch.Validate(); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finalized {
		return &ValidationError{Field: "stream epoch", Problem: "assigner has already been finalized", Cause: ErrOrdinalFinalized}
	}
	if !a.initialized {
		return &ValidationError{Field: "stream epoch", Problem: "assigner was not constructed", Cause: ErrOrdinalUninitialized}
	}
	if commit.SourceID != a.source || commit.Epoch != a.epoch || commit.LastArrivalOrdinal != a.last {
		return &ValidationError{
			Field:   "epoch commit",
			Problem: "source, epoch, or committed high-water mark differs from active assignment state",
			Cause:   ErrOrdinalCommitMismatch,
		}
	}
	a.source = ""
	a.epoch = StreamEpoch{}
	a.last = 0
	a.initialized = false
	a.finalized = true
	return nil
}

func (a *OrdinalAssigner) checkActiveLocked(source string, epoch StreamEpoch) error {
	if a.finalized {
		return &ValidationError{Field: "stream epoch", Problem: "assigner has been finalized", Cause: ErrOrdinalFinalized}
	}
	if !a.initialized {
		return &ValidationError{Field: "stream epoch", Problem: "assigner was not constructed", Cause: ErrOrdinalUninitialized}
	}
	if source != a.source || epoch != a.epoch {
		return &ValidationError{Field: "stream epoch", Problem: "record does not belong to this assigner", Cause: ErrInvalidEpoch}
	}
	return nil
}
