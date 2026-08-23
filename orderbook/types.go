package orderbook

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

const (
	BookSnapshotSchemaName    = "BookSnapshotV1"
	BookSnapshotSchemaVersion = 1
	PolicyEncodingVersion     = 1
	SnapshotEncodingVersion   = 1
	ProjectionEncodingVersion = 1
)

var (
	ErrInvalidConfig         = errors.New("orderbook: invalid configuration")
	ErrInvalidUpdate         = errors.New("orderbook: invalid normalized update")
	ErrInvalidSnapshot       = errors.New("orderbook: invalid snapshot observation")
	ErrInvalidTransition     = errors.New("orderbook: invalid lifecycle transition")
	ErrClosed                = errors.New("orderbook: reconstruction is closed")
	ErrBufferMessages        = errors.New("orderbook: buffered message bound exceeded")
	ErrBufferBytes           = errors.New("orderbook: buffered byte bound exceeded")
	ErrBufferTime            = errors.New("orderbook: buffered time bound exceeded")
	ErrLevelLimit            = errors.New("orderbook: level bound exceeded")
	ErrSnapshotLimit         = errors.New("orderbook: snapshot/refetch bound exceeded")
	ErrSnapshotBytes         = errors.New("orderbook: snapshot byte bound exceeded")
	ErrSnapshotBehind        = errors.New("orderbook: snapshot remains behind buffered updates")
	ErrBoundary              = errors.New("orderbook: first retained update does not span snapshot boundary")
	ErrSequenceGap           = errors.New("orderbook: forward sequence gap")
	ErrCrossedBook           = errors.New("orderbook: venue contract forbids crossed book")
	ErrNegativeLevel         = errors.New("orderbook: negative level")
	ErrInvalidLevel          = errors.New("orderbook: invalid level")
	ErrOutputLimit           = errors.New("orderbook: output bound exceeded")
	ErrRawDiscontinuity      = errors.New("orderbook: raw coordinate discontinuity")
	ErrIdentityDiscontinuity = errors.New("orderbook: immutable input identity changed")
)

type State string

const (
	StateUnseeded  State = "UNSEEDED"
	StateBuffering State = "BUFFERING"
	StateSeeded    State = "SEEDED"
	StateLive      State = "LIVE"
	StateGap       State = "GAP"
	StateClosed    State = "CLOSED"
)

type CloseReason string

const (
	CloseExplicit         CloseReason = "explicit_close"
	CloseReconnect        CloseReason = "raw_connection_epoch_changed"
	CloseRawDiscontinuity CloseReason = "raw_discontinuity"
	CloseForwardGap       CloseReason = "forward_sequence_gap"
	CloseSeedBoundary     CloseReason = "seed_boundary_not_spanned"
	CloseBufferOverflow   CloseReason = "buffer_overflow"
	CloseSnapshotLimit    CloseReason = "snapshot_limit"
	CloseSnapshotBytes    CloseReason = "snapshot_byte_limit"
	CloseMalformedInput   CloseReason = "malformed_input"
	CloseLevelLimit       CloseReason = "level_limit"
	CloseCrossedBook      CloseReason = "crossed_book"
	CloseOutputLimit      CloseReason = "output_limit"
	CloseCancelled        CloseReason = "cancelled"
)

type Bounds struct {
	MaxBufferedMessages uint32
	MaxBufferedBytes    uint64
	MaxBufferSpanNS     int64
	MaxLevelsPerSide    uint32
	MaxSnapshotBytes    uint64
	MaxSnapshotFetches  uint32
	MaxOutputs          uint32
}

type Config struct {
	SourceID          string
	UpdateChannelID   string
	SnapshotChannelID string
	Instrument        normalize.InstrumentIdentity
	CatalogSnapshotID normalize.Hash
	MapperVersion     string
	MapperBindingID   normalize.Hash
	RejectCrossed     bool
	Bounds            Bounds
}

type SequenceAction uint8

const (
	SequenceApply SequenceAction = iota + 1
	SequenceStale
	SequenceGap
)

// SequencePolicy supplies only venue sequence semantics. Engine owns buffering,
// immutable evidence, atomic level application, bounds, cancellation, and hashes.
type SequencePolicy interface {
	Name() string
	Version() string
	SnapshotBehind(snapshotLast, firstBuffered uint64) bool
	First(snapshotLast uint64, update normalize.BookUpdateV1) SequenceAction
	Next(localLast uint64, update normalize.BookUpdateV1) SequenceAction
}

type SnapshotFetcher interface {
	Fetch(context.Context) (SnapshotObservation, error)
}

type SnapshotLevel struct {
	Price  normalize.Numeric
	Amount normalize.Numeric
}

type SnapshotObservation struct {
	EncodingVersion uint16
	Identity        normalize.Hash
	SourceID        string
	ChannelID       string
	InstrumentUID   string
	LastSequence    uint64
	Bids            []SnapshotLevel
	Asks            []SnapshotLevel
	RawCoordinate   normalize.RawCoordinate
	PayloadBytes    uint64
}

func NewSnapshotObservation(sourceID, channelID, instrumentUID string, lastSequence uint64, bids, asks []SnapshotLevel, coordinate normalize.RawCoordinate, payloadBytes uint64) (SnapshotObservation, error) {
	observation := SnapshotObservation{
		EncodingVersion: SnapshotEncodingVersion,
		SourceID:        sourceID,
		ChannelID:       channelID,
		InstrumentUID:   instrumentUID,
		LastSequence:    lastSequence,
		Bids:            slices.Clone(bids),
		Asks:            slices.Clone(asks),
		RawCoordinate:   coordinate,
		PayloadBytes:    payloadBytes,
	}
	observation.Identity = snapshotIdentity(observation)
	if err := observation.Validate(); err != nil {
		return SnapshotObservation{}, err
	}
	return observation, nil
}

func (s SnapshotObservation) Validate() error {
	if s.EncodingVersion != SnapshotEncodingVersion || s.SourceID == "" || s.ChannelID == "" ||
		s.InstrumentUID == "" || s.PayloadBytes == 0 || !validRawCoordinate(s.RawCoordinate) ||
		s.RawCoordinate.SourceID != s.SourceID || s.RawCoordinate.ChannelID != s.ChannelID {
		return fmt.Errorf("%w: identity or raw coordinate", ErrInvalidSnapshot)
	}
	for _, levels := range [][]SnapshotLevel{s.Bids, s.Asks} {
		for _, level := range levels {
			if err := level.Price.Validate(); err != nil {
				return fmt.Errorf("%w: price: %v", ErrInvalidSnapshot, err)
			}
			if err := level.Amount.Validate(); err != nil {
				return fmt.Errorf("%w: amount: %v", ErrInvalidSnapshot, err)
			}
		}
	}
	if s.Identity != snapshotIdentity(s) {
		return fmt.Errorf("%w: identity hash mismatch", ErrInvalidSnapshot)
	}
	return nil
}

func cloneSnapshot(s SnapshotObservation) SnapshotObservation {
	s.Bids = slices.Clone(s.Bids)
	s.Asks = slices.Clone(s.Asks)
	return s
}

type Level struct {
	Price  normalize.Decimal
	Amount normalize.Decimal
}

type InputRange struct {
	First normalize.RawCoordinate
	Last  normalize.RawCoordinate
	Count uint64
	Hash  normalize.Hash
}

type BookSnapshotV1 struct {
	SchemaName                string
	SchemaVersion             uint16
	ProjectionEncodingVersion uint16
	ProjectionHash            normalize.Hash
	SourceID                  string
	UpdateChannelID           string
	SnapshotChannelID         string
	InstrumentUID             string
	BaseAssetID               string
	QuoteAssetID              string
	CatalogSnapshotID         normalize.Hash
	MapperVersion             string
	MapperBindingID           normalize.Hash
	PolicyName                string
	PolicyVersion             string
	PolicyID                  normalize.Hash
	ReconstructionEpochID     normalize.Hash
	InitialSnapshotID         normalize.Hash
	InitialSnapshotCoordinate normalize.RawCoordinate
	LastSequence              uint64
	OutputOrdinal             uint32
	InputRange                InputRange
	Bids                      []Level
	Asks                      []Level
}

func (s BookSnapshotV1) Validate() error {
	if s.SchemaName != BookSnapshotSchemaName || s.SchemaVersion != BookSnapshotSchemaVersion ||
		s.ProjectionEncodingVersion != ProjectionEncodingVersion || s.SourceID == "" ||
		s.UpdateChannelID == "" || s.SnapshotChannelID == "" || s.InstrumentUID == "" ||
		s.BaseAssetID == "" || s.QuoteAssetID == "" || s.BaseAssetID == s.QuoteAssetID ||
		s.MapperVersion == "" || s.OutputOrdinal == 0 || s.InputRange.Count == 0 ||
		s.CatalogSnapshotID == (normalize.Hash{}) || s.MapperBindingID == (normalize.Hash{}) ||
		s.PolicyID == (normalize.Hash{}) || s.ReconstructionEpochID == (normalize.Hash{}) ||
		s.InitialSnapshotID == (normalize.Hash{}) || !validRawCoordinate(s.InitialSnapshotCoordinate) ||
		!validRawCoordinate(s.InputRange.First) || !validRawCoordinate(s.InputRange.Last) {
		return fmt.Errorf("%w: invalid BookSnapshotV1 identity", ErrInvalidLevel)
	}
	if s.ProjectionHash != projectionIdentity(s) {
		return fmt.Errorf("%w: projection hash mismatch", ErrInvalidLevel)
	}
	return nil
}

func cloneProjection(s BookSnapshotV1) BookSnapshotV1 {
	s.Bids = slices.Clone(s.Bids)
	s.Asks = slices.Clone(s.Asks)
	return s
}

type Transition struct {
	State         State
	HasCoordinate bool
	Coordinate    normalize.RawCoordinate
}

type SnapshotAttemptEvidence struct {
	Identity      normalize.Hash
	RawCoordinate normalize.RawCoordinate
	LastSequence  uint64
	Behind        bool
}

type EpochEvidence struct {
	ReconstructionEpochID normalize.Hash
	ParentEpochID         normalize.Hash
	SourceID              string
	UpdateChannelID       string
	SnapshotChannelID     string
	InstrumentUID         string
	CatalogSnapshotID     normalize.Hash
	MapperVersion         string
	MapperBindingID       normalize.Hash
	PolicyName            string
	PolicyVersion         string
	PolicyID              normalize.Hash
	State                 State
	CloseReason           CloseReason
	Transitions           []Transition
	SnapshotAttempts      []SnapshotAttemptEvidence
	SnapshotFetches       uint32
	InitialSnapshotID     normalize.Hash
	InitialSnapshotRaw    normalize.RawCoordinate
	HasInitialSnapshot    bool
	FirstObservedRaw      normalize.RawCoordinate
	LastObservedRaw       normalize.RawCoordinate
	HasObservedRaw        bool
	FirstAcceptedRaw      normalize.RawCoordinate
	LastAcceptedRaw       normalize.RawCoordinate
	HasAcceptedRaw        bool
	AcceptedMessages      uint64
	AppliedMessages       uint64
	StaleMessages         uint64
	LastSequence          uint64
	OutputCount           uint32
	OutputHash            normalize.Hash
	LastOutputHash        normalize.Hash
}

func cloneEvidence(e EpochEvidence) EpochEvidence {
	e.Transitions = slices.Clone(e.Transitions)
	e.SnapshotAttempts = slices.Clone(e.SnapshotAttempts)
	return e
}

type Result struct {
	State           State
	Output          *BookSnapshotV1
	ClosedEpochs    []EpochEvidence
	IgnoredStale    bool
	SnapshotFetches uint32
}

func (r Result) clone() Result {
	if r.Output != nil {
		output := cloneProjection(*r.Output)
		r.Output = &output
	}
	r.ClosedEpochs = slices.Clone(r.ClosedEpochs)
	for i := range r.ClosedEpochs {
		r.ClosedEpochs[i] = cloneEvidence(r.ClosedEpochs[i])
	}
	return r
}

func validateConfig(config Config, policy SequencePolicy, fetcher SnapshotFetcher) error {
	if policy == nil || fetcher == nil || config.SourceID == "" || config.UpdateChannelID == "" ||
		config.SnapshotChannelID == "" || config.UpdateChannelID == config.SnapshotChannelID ||
		config.Instrument.InstrumentUID == "" || config.Instrument.BaseAssetID == "" ||
		config.Instrument.QuoteAssetID == "" || config.Instrument.BaseAssetID == config.Instrument.QuoteAssetID ||
		config.CatalogSnapshotID == (normalize.Hash{}) || config.MapperBindingID == (normalize.Hash{}) ||
		config.MapperVersion == "" || policy.Name() == "" || policy.Version() == "" {
		return fmt.Errorf("%w: immutable identity", ErrInvalidConfig)
	}
	for _, value := range []string{config.SourceID, config.UpdateChannelID, config.SnapshotChannelID,
		config.Instrument.InstrumentUID, config.Instrument.BaseAssetID, config.Instrument.QuoteAssetID,
		config.MapperVersion, policy.Name(), policy.Version()} {
		if len(value) > 256 || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: oversized or invalid string", ErrInvalidConfig)
		}
	}
	bounds := config.Bounds
	if bounds.MaxBufferedMessages == 0 || bounds.MaxBufferedBytes == 0 || bounds.MaxBufferSpanNS <= 0 ||
		bounds.MaxLevelsPerSide == 0 || bounds.MaxSnapshotBytes == 0 || bounds.MaxSnapshotFetches == 0 ||
		bounds.MaxOutputs == 0 || bounds.MaxLevelsPerSide > normalize.MaxBookLevelsPerSide {
		return fmt.Errorf("%w: bounds", ErrInvalidConfig)
	}
	return nil
}

func validRawCoordinate(coordinate normalize.RawCoordinate) bool {
	return coordinate.SourceID != "" && coordinate.ChannelID != "" &&
		(coordinate.EpochKind == normalize.ConnectionEpoch || coordinate.EpochKind == normalize.PollCycleEpoch) &&
		coordinate.EpochID != ([16]byte{}) && coordinate.ArrivalOrdinal != 0 &&
		coordinate.RawSegmentSHA256 != (normalize.Hash{}) && coordinate.RawPayloadSHA256 != (normalize.Hash{})
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	return ctx.Err()
}
