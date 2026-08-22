package segment

import "time"

const (
	DefaultFrameBytes          = 4 << 20
	DefaultSegmentBytes        = 256 << 20
	DefaultSegmentAge          = 5 * time.Minute
	X1CorpusBytes        int64 = 10 << 30
	X1AlternateThreshold       = 0.15
	FullX1Consent              = "RUN_FIXED_10_GIB_X1"
)

var X1FrameSizes = [...]int{1 << 20, 4 << 20, 16 << 20}
var X1Concurrencies = [...]int{1, 2}

type DecisionRecord struct {
	DecisionID           string        `json:"decision_id"`
	FormatVersion        uint16        `json:"format_version"`
	Encoding             string        `json:"encoding"`
	FrameBytes           int           `json:"frame_bytes"`
	SegmentBytes         int           `json:"segment_bytes"`
	SegmentAge           time.Duration `json:"segment_age_ns"`
	AlternateThreshold   float64       `json:"alternate_threshold"`
	EvidenceStatus       string        `json:"evidence_status"`
	ObservedMeasurements bool          `json:"observed_measurements"`
	SelectionBasis       string        `json:"selection_basis"`
}

// FrozenDecision distinguishes the binding default from experiment evidence.
// Measurements are attached only to reports produced by an authorized X1 run.
func FrozenDecision() DecisionRecord {
	return DecisionRecord{
		DecisionID:           "ELMD-003-X1-v1",
		FormatVersion:        FormatVersion,
		Encoding:             "project-binary-crc32c-concatenated-independent-zstd",
		FrameBytes:           DefaultFrameBytes,
		SegmentBytes:         DefaultSegmentBytes,
		SegmentAge:           DefaultSegmentAge,
		AlternateThreshold:   X1AlternateThreshold,
		EvidenceStatus:       "frozen-default-no-qualifying-observed-alternate",
		ObservedMeasurements: false,
		SelectionBasis:       "an alternate requires exact payloads, evolution and unambiguous recovery, plus >=15% compressed throughput gain or byte reduction without greater peak RSS",
	}
}

type AlternateEvidence struct {
	ExactPayloads           bool
	EvolutionCompatible     bool
	RecoveryUnambiguous     bool
	ThroughputImprovement   float64
	CompressedByteReduction float64
	PeakRSSIncrease         int64
}

func (e AlternateEvidence) Qualifies() bool {
	return e.ExactPayloads && e.EvolutionCompatible && e.RecoveryUnambiguous &&
		e.PeakRSSIncrease <= 0 &&
		(e.ThroughputImprovement >= X1AlternateThreshold || e.CompressedByteReduction >= X1AlternateThreshold)
}

type RotationReason uint8

const (
	RotationNone RotationReason = iota
	RotationSize
	RotationAge
	RotationEpochEnd
	RotationShutdown
)

type RotationPolicy struct {
	MaxUncompressedBytes int
	MaxAge               time.Duration
}

func DefaultRotationPolicy() RotationPolicy {
	return RotationPolicy{MaxUncompressedBytes: DefaultSegmentBytes, MaxAge: DefaultSegmentAge}
}

// Reason reports the first binding rotation condition in deterministic priority
// order. It contains no file, publication, writer, or recovery behavior.
func (p RotationPolicy) Reason(uncompressedBytes int, startedAt, now time.Time, epochEnded, shuttingDown bool) RotationReason {
	if p.MaxUncompressedBytes == 0 {
		p.MaxUncompressedBytes = DefaultSegmentBytes
	}
	if p.MaxAge == 0 {
		p.MaxAge = DefaultSegmentAge
	}
	if uncompressedBytes >= p.MaxUncompressedBytes {
		return RotationSize
	}
	if !startedAt.IsZero() && !now.Before(startedAt.Add(p.MaxAge)) {
		return RotationAge
	}
	if epochEnded {
		return RotationEpochEnd
	}
	if shuttingDown {
		return RotationShutdown
	}
	return RotationNone
}
