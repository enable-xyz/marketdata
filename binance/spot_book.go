package binance

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/orderbook"
)

const (
	SpotBookPolicyName    = "binance-spot-diff-depth"
	SpotBookPolicyVersion = "binance.spot.diff-depth.reconstruction.v1"
)

var ErrSpotBook = errors.New("binance: Spot order-book reconstruction rejected")

type SpotBookConfig struct {
	Instrument        normalize.InstrumentIdentity
	CatalogSnapshotID normalize.Hash
	MapperVersion     string
	MapperBindingID   normalize.Hash
	Bounds            orderbook.Bounds
}

func DefaultSpotBookBounds() orderbook.Bounds {
	return orderbook.Bounds{
		MaxBufferedMessages: 4_096,
		MaxBufferedBytes:    32 << 20,
		MaxBufferSpanNS:     10_000_000_000,
		MaxLevelsPerSide:    SpotDepthLimitMaximum,
		MaxSnapshotBytes:    SpotMaxRawPayloadBytes,
		MaxSnapshotFetches:  3,
		MaxOutputs:          1_000_000,
	}
}

type SpotBook struct {
	engine *orderbook.Engine
}

func NewSpotBook(config SpotBookConfig, fetcher orderbook.SnapshotFetcher) (*SpotBook, error) {
	if config.Bounds == (orderbook.Bounds{}) {
		config.Bounds = DefaultSpotBookBounds()
	}
	engine, err := orderbook.New(orderbook.Config{
		SourceID:          SpotSourceID,
		UpdateChannelID:   SpotRawChannel,
		SnapshotChannelID: SpotDepthChannel,
		Instrument:        config.Instrument,
		CatalogSnapshotID: config.CatalogSnapshotID,
		MapperVersion:     config.MapperVersion,
		MapperBindingID:   config.MapperBindingID,
		RejectCrossed:     true,
		Bounds:            config.Bounds,
	}, spotBookSequencePolicy{}, fetcher)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSpotBook, err)
	}
	return &SpotBook{engine: engine}, nil
}

func (b *SpotBook) State() orderbook.State {
	if b == nil || b.engine == nil {
		return orderbook.StateClosed
	}
	return b.engine.State()
}

func (b *SpotBook) PolicyID() normalize.Hash {
	if b == nil || b.engine == nil {
		return normalize.Hash{}
	}
	return b.engine.PolicyID()
}

func (b *SpotBook) Buffered() (uint32, uint64) {
	if b == nil || b.engine == nil {
		return 0, 0
	}
	return b.engine.Buffered()
}

func (b *SpotBook) CurrentEvidence() (orderbook.EpochEvidence, bool) {
	if b == nil || b.engine == nil {
		return orderbook.EpochEvidence{}, false
	}
	return b.engine.CurrentEvidence()
}

func (b *SpotBook) Accept(ctx context.Context, update normalize.BookUpdateV1, encodedBytes uint64) (orderbook.Result, error) {
	if b == nil || b.engine == nil {
		return orderbook.Result{State: orderbook.StateClosed}, orderbook.ErrClosed
	}
	return b.engine.Accept(ctx, update, encodedBytes)
}

func (b *SpotBook) Seed(ctx context.Context) (orderbook.Result, error) {
	if b == nil || b.engine == nil {
		return orderbook.Result{State: orderbook.StateClosed}, orderbook.ErrClosed
	}
	return b.engine.Seed(ctx)
}

func (b *SpotBook) Discontinuity(ctx context.Context) (orderbook.Result, error) {
	if b == nil || b.engine == nil {
		return orderbook.Result{State: orderbook.StateClosed}, orderbook.ErrClosed
	}
	return b.engine.Discontinuity(ctx)
}

func (b *SpotBook) Close(ctx context.Context) (orderbook.Result, error) {
	if b == nil || b.engine == nil {
		return orderbook.Result{State: orderbook.StateClosed}, orderbook.ErrClosed
	}
	return b.engine.Close(ctx)
}

type spotBookSequencePolicy struct{}

func (spotBookSequencePolicy) Name() string    { return SpotBookPolicyName }
func (spotBookSequencePolicy) Version() string { return SpotBookPolicyVersion }

func (spotBookSequencePolicy) SnapshotBehind(snapshotLast, firstBuffered uint64) bool {
	return snapshotLast != math.MaxUint64 && snapshotLast+1 < firstBuffered
}

func (spotBookSequencePolicy) First(snapshotLast uint64, update normalize.BookUpdateV1) orderbook.SequenceAction {
	if update.LastSequence <= snapshotLast {
		return orderbook.SequenceStale
	}
	if snapshotLast == math.MaxUint64 {
		return orderbook.SequenceStale
	}
	boundary := snapshotLast + 1
	if update.FirstSequence <= boundary && boundary <= update.LastSequence {
		return orderbook.SequenceApply
	}
	return orderbook.SequenceGap
}

func (spotBookSequencePolicy) Next(localLast uint64, update normalize.BookUpdateV1) orderbook.SequenceAction {
	if update.LastSequence <= localLast {
		return orderbook.SequenceStale
	}
	if localLast == math.MaxUint64 {
		return orderbook.SequenceStale
	}
	if update.FirstSequence > localLast+1 {
		return orderbook.SequenceGap
	}
	return orderbook.SequenceApply
}

var spotBookSnapshotFields = []string{"asks", "bids", "lastUpdateId"}

// ParseSpotBookSnapshot converts one already-captured REST observation. It does
// not perform network access and binds every parsed level to the exact raw
// coordinate and payload hash supplied by capture.
func ParseSpotBookSnapshot(record normalize.RawRecord, instrument normalize.InstrumentIdentity) (orderbook.SnapshotObservation, error) {
	if err := record.Validate(); err != nil {
		return orderbook.SnapshotObservation{}, fmt.Errorf("%w: raw record: %w", ErrSpotBook, err)
	}
	envelope := record.Envelope
	if record.Coordinate.SourceID != SpotSourceID || record.Coordinate.ChannelID != SpotDepthChannel ||
		record.Coordinate.EpochKind != normalize.PollCycleEpoch || envelope.RecordKind != capture.RecordKindREST ||
		envelope.PayloadEncoding != capture.PayloadEncodingJSON || !envelope.HTTPStatusOrWSState.Valid ||
		envelope.HTTPStatusOrWSState.Value != "200" || !envelope.NativeSymbol.Valid ||
		instrument.NativeID == "" || envelope.NativeSymbol.Value != instrument.NativeID ||
		instrument.InstrumentUID == "" || instrument.BaseAssetID == "" || instrument.QuoteAssetID == "" {
		return orderbook.SnapshotObservation{}, fmt.Errorf("%w: snapshot boundary", ErrSpotBook)
	}
	object, err := decodeSpotObject(envelope.RawPayload)
	if err != nil {
		return orderbook.SnapshotObservation{}, fmt.Errorf("%w: %w", ErrSpotBook, err)
	}
	if err := validateSpotFields(object, spotBookSnapshotFields, normalize.FingerprintRule{}); err != nil {
		return orderbook.SnapshotObservation{}, fmt.Errorf("%w: %w", ErrSpotBook, err)
	}
	lastSequence, err := spotUint(object, "lastUpdateId")
	if err != nil {
		return orderbook.SnapshotObservation{}, fmt.Errorf("%w: %w", ErrSpotBook, err)
	}
	bids, err := mappedSpotLevels(object, "bids", normalize.SideBuy, instrument)
	if err != nil {
		return orderbook.SnapshotObservation{}, fmt.Errorf("%w: %w", ErrSpotBook, err)
	}
	asks, err := mappedSpotLevels(object, "asks", normalize.SideSell, instrument)
	if err != nil {
		return orderbook.SnapshotObservation{}, fmt.Errorf("%w: %w", ErrSpotBook, err)
	}
	return orderbook.NewSnapshotObservation(
		SpotSourceID,
		SpotDepthChannel,
		instrument.InstrumentUID,
		lastSequence,
		spotSnapshotLevels(bids),
		spotSnapshotLevels(asks),
		record.Coordinate,
		uint64(len(envelope.RawPayload)),
	)
}

func spotSnapshotLevels(levels []normalize.BookLevel) []orderbook.SnapshotLevel {
	result := make([]orderbook.SnapshotLevel, len(levels))
	for index, level := range levels {
		result[index] = orderbook.SnapshotLevel{Price: level.Price, Amount: level.Amount}
	}
	return result
}
