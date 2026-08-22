package replay

import (
	"context"
	"fmt"
	"io"

	"github.com/enable-xyz/marketdata/segment"
)

const (
	MaximumWorkers       = 64
	MaximumPrefetch      = 64
	LogicalHashVersionV1 = uint16(1)

	defaultMaxSources           = 64
	defaultMaxSegments          = 1_000_000
	defaultMaxSegmentBytes      = int64(320 << 20)
	defaultMaxRecordsPerSegment = uint64(4_000_000)
	defaultMaxFramesPerSegment  = 4_096
)

// Config bounds every replay-owned queue, worker set, and decoded segment.
type Config struct {
	Concurrency          int
	Prefetch             int
	MaxSources           int
	MaxSegments          int
	MaxSegmentBytes      int64
	MaxRecordsPerSegment uint64
	MaxFramesPerSegment  int
}

func DefaultConfig() Config {
	return Config{
		Concurrency:          1,
		Prefetch:             2,
		MaxSources:           defaultMaxSources,
		MaxSegments:          defaultMaxSegments,
		MaxSegmentBytes:      defaultMaxSegmentBytes,
		MaxRecordsPerSegment: defaultMaxRecordsPerSegment,
		MaxFramesPerSegment:  defaultMaxFramesPerSegment,
	}
}

func (c Config) normalized() (Config, error) {
	defaults := DefaultConfig()
	if c.Concurrency == 0 {
		c.Concurrency = defaults.Concurrency
	}
	if c.Prefetch == 0 {
		c.Prefetch = defaults.Prefetch
	}
	if c.MaxSources == 0 {
		c.MaxSources = defaults.MaxSources
	}
	if c.MaxSegments == 0 {
		c.MaxSegments = defaults.MaxSegments
	}
	if c.MaxSegmentBytes == 0 {
		c.MaxSegmentBytes = defaults.MaxSegmentBytes
	}
	if c.MaxRecordsPerSegment == 0 {
		c.MaxRecordsPerSegment = defaults.MaxRecordsPerSegment
	}
	if c.MaxFramesPerSegment == 0 {
		c.MaxFramesPerSegment = defaults.MaxFramesPerSegment
	}
	if c.Concurrency < 1 || c.Concurrency > MaximumWorkers {
		return Config{}, fmt.Errorf("%w: concurrency must be from 1 through %d", ErrInputBound, MaximumWorkers)
	}
	if c.Prefetch < 1 || c.Prefetch > MaximumPrefetch {
		return Config{}, fmt.Errorf("%w: prefetch must be from 1 through %d", ErrInputBound, MaximumPrefetch)
	}
	if c.MaxSources < 1 || c.MaxSegments < 1 || c.MaxSegmentBytes < 1 || c.MaxRecordsPerSegment < 1 || c.MaxFramesPerSegment < 1 {
		return Config{}, fmt.Errorf("%w: replay limits must be positive", ErrInputBound)
	}
	return c, nil
}

// ObjectReader is the smallest immutable object-store contract native replay
// consumes. objectstore.Client satisfies it.
type ObjectReader interface {
	Get(context.Context, string) (io.ReadCloser, error)
}

type OrderLabel string

const (
	SourceNativeOrder      OrderLabel = "source-native"
	CollectorObservedOrder OrderLabel = "collector-observed"
)

type EventKind uint8

const (
	EventRecord EventKind = iota + 1
	EventDiscontinuity
)

// Coordinate is also the exact collector-observed merge key, in field order.
type Coordinate struct {
	ReceivedWallTimeNS           int64
	SourceID                     string
	EpochFirstReceivedWallTimeNS int64
	StreamEpochID                string
	ArrivalOrdinal               uint64
	MessageOrdinal               uint32
}

type DiscontinuityKind uint8

const (
	DiscontinuityEpochBoundary DiscontinuityKind = iota + 1
	DiscontinuityDisconnect
	DiscontinuityQuarantinedFrame
	DiscontinuityMissingSegment
	DiscontinuityOrdinalGap
	DiscontinuityOrdinalOverlap
)

type IntegrityReason uint8

const (
	IntegrityReasonNone IntegrityReason = iota
	IntegrityReasonObjectLength
	IntegrityReasonObjectSHA256
	IntegrityReasonFrame
	IntegrityReasonRecord
	IntegrityReasonRecordCount
	IntegrityReasonIdentity
	IntegrityReasonOrdinalOrder
)

// Discontinuity contains only deterministic catalog/framing coordinates. It
// deliberately excludes provider errors, payload bytes, clocks, and paths.
type Discontinuity struct {
	Kind                  DiscontinuityKind
	Reason                IntegrityReason
	SegmentID             string
	PreviousStreamEpochID string
	FirstOrdinal          uint64
	LastOrdinal           uint64
	FrameOrdinal          uint32
	CompressedOffset      uint64
}

type Event struct {
	Kind          EventKind
	Order         OrderLabel
	Coordinate    Coordinate
	Record        segment.Envelope
	Discontinuity Discontinuity
}

type EmitFunc func(Event) error

type Result struct {
	Order              OrderLabel
	LogicalHashVersion uint16
	LogicalHash        [32]byte
	EventCount         uint64
}
