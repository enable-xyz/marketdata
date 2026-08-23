package binance

import (
	"encoding/json"
	"fmt"

	"github.com/enable-xyz/marketdata/normalize"
)

type CoinMRawLevel struct {
	PriceText     string
	ContractsText string
	Price         normalize.Numeric
	Contracts     normalize.NativeValue
}

type CoinMDepthSnapshot struct {
	SourceID          string
	Symbol            string
	LastUpdateID      uint64
	EventTimeMS       int64
	TransactionTimeMS int64
	Bids              []CoinMRawLevel
	Asks              []CoinMRawLevel
}

type CoinMDepthUpdate struct {
	SourceID          string
	NativeSymbolType  uint8
	EventType         string
	EventTimeMS       int64
	TransactionTimeMS int64
	Symbol            string
	Pair              string
	FirstUpdateID     uint64
	FinalUpdateID     uint64
	PreviousFinalID   uint64
	Bids              []CoinMRawLevel
	Asks              []CoinMRawLevel
}

func ParseCoinMDepthSnapshot(raw []byte, instrument normalize.InstrumentIdentity) (CoinMDepthSnapshot, error) {
	var wire struct {
		LastUpdateID      uint64            `json:"lastUpdateId"`
		EventTimeMS       int64             `json:"E"`
		TransactionTimeMS int64             `json:"T"`
		Bids              []json.RawMessage `json:"bids"`
		Asks              []json.RawMessage `json:"asks"`
	}
	if err := coinMUnmarshalBoundedStrict(raw, &wire); err != nil {
		return CoinMDepthSnapshot{}, err
	}
	if wire.LastUpdateID == 0 || wire.EventTimeMS < 0 || wire.TransactionTimeMS < 0 || len(wire.Bids) > CoinMMaxMergedRecords || len(wire.Asks) > CoinMMaxMergedRecords {
		return CoinMDepthSnapshot{}, fmt.Errorf("%w: malformed depth snapshot identity", ErrCoinMInvalidMarketPayload)
	}
	bids, err := parseCoinMRawLevels(wire.Bids, instrument)
	if err != nil {
		return CoinMDepthSnapshot{}, err
	}
	asks, err := parseCoinMRawLevels(wire.Asks, instrument)
	if err != nil {
		return CoinMDepthSnapshot{}, err
	}
	return CoinMDepthSnapshot{SourceID: CoinMSourceID, Symbol: instrument.NativeID, LastUpdateID: wire.LastUpdateID, EventTimeMS: wire.EventTimeMS, TransactionTimeMS: wire.TransactionTimeMS, Bids: bids, Asks: asks}, nil
}

func ParseCoinMDepthUpdate(raw []byte, instrument normalize.InstrumentIdentity) (CoinMDepthUpdate, error) {
	var wire struct {
		EventType         string            `json:"e"`
		EventTimeMS       int64             `json:"E"`
		TransactionTimeMS int64             `json:"T"`
		Symbol            string            `json:"s"`
		Pair              string            `json:"ps"`
		SymbolType        uint8             `json:"st"`
		FirstUpdateID     uint64            `json:"U"`
		FinalUpdateID     uint64            `json:"u"`
		PreviousFinalID   uint64            `json:"pu"`
		Bids              []json.RawMessage `json:"b"`
		Asks              []json.RawMessage `json:"a"`
	}
	if err := coinMUnmarshalBoundedStrict(raw, &wire); err != nil {
		return CoinMDepthUpdate{}, err
	}
	if wire.EventType != "depthUpdate" || wire.SymbolType != 2 || wire.Symbol != instrument.NativeID || !coinMSymbolShape(wire.Symbol, wire.Pair) ||
		wire.EventTimeMS < 0 || wire.TransactionTimeMS < 0 || wire.FirstUpdateID == 0 || wire.FinalUpdateID < wire.FirstUpdateID ||
		len(wire.Bids) > CoinMMaxMergedRecords || len(wire.Asks) > CoinMMaxMergedRecords {
		return CoinMDepthUpdate{}, fmt.Errorf("%w: malformed depth update identity", ErrCoinMInvalidMarketPayload)
	}
	bids, err := parseCoinMRawLevels(wire.Bids, instrument)
	if err != nil {
		return CoinMDepthUpdate{}, err
	}
	asks, err := parseCoinMRawLevels(wire.Asks, instrument)
	if err != nil {
		return CoinMDepthUpdate{}, err
	}
	return CoinMDepthUpdate{
		SourceID:          CoinMSourceID,
		NativeSymbolType:  wire.SymbolType,
		EventType:         wire.EventType,
		EventTimeMS:       wire.EventTimeMS,
		TransactionTimeMS: wire.TransactionTimeMS,
		Symbol:            wire.Symbol,
		Pair:              wire.Pair,
		FirstUpdateID:     wire.FirstUpdateID,
		FinalUpdateID:     wire.FinalUpdateID,
		PreviousFinalID:   wire.PreviousFinalID,
		Bids:              bids,
		Asks:              asks,
	}, nil
}

func parseCoinMRawLevels(rawLevels []json.RawMessage, instrument normalize.InstrumentIdentity) ([]CoinMRawLevel, error) {
	levels := make([]CoinMRawLevel, len(rawLevels))
	for i, rawLevel := range rawLevels {
		var values []string
		if len(rawLevel) == 0 || len(rawLevel) > 512 || json.Unmarshal(rawLevel, &values) != nil || len(values) != 2 {
			return nil, fmt.Errorf("%w: malformed depth level %d", ErrCoinMInvalidMarketPayload, i)
		}
		price, err := parseCoinMPositiveDecimal(values[0], normalize.CanonicalPriceScale, false)
		if err != nil {
			return nil, err
		}
		contracts, err := parseCoinMPositiveDecimal(values[1], normalize.CanonicalAmountScale, true)
		if err != nil {
			return nil, err
		}
		levels[i] = CoinMRawLevel{
			PriceText:     values[0],
			ContractsText: values[1],
			Price:         normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)},
			Contracts:     normalize.NativeValue{Decimal: contracts, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrument.InstrumentUID}},
		}
	}
	return levels, nil
}

func (u CoinMDepthUpdate) Validate() error {
	if u.SourceID != CoinMSourceID || u.NativeSymbolType != 2 || u.EventType != "depthUpdate" || u.Symbol == "" || u.FirstUpdateID == 0 || u.FinalUpdateID < u.FirstUpdateID || len(u.Bids) > CoinMMaxMergedRecords || len(u.Asks) > CoinMMaxMergedRecords {
		return fmt.Errorf("%w: invalid typed depth update", ErrCoinMInvalidMarketPayload)
	}
	return nil
}

type CoinMBookState string

const (
	CoinMBookAwaitingSnapshot CoinMBookState = "awaiting_snapshot"
	CoinMBookBridging         CoinMBookState = "bridging"
	CoinMBookLive             CoinMBookState = "live"
	CoinMBookClosed           CoinMBookState = "closed"
)

type CoinMBookTransition struct {
	State           CoinMBookState
	Epoch           uint64
	LastUpdateID    uint64
	Applied         bool
	Stale           bool
	NeedsSnapshot   bool
	ContinuityError error
}

type CoinMBookSynchronizer struct {
	inner *USDMBookSynchronizer
}

func NewCoinMBookSynchronizer(symbol string, maxBuffered int) (*CoinMBookSynchronizer, error) {
	inner, err := NewUSDMBookSynchronizer(symbol, maxBuffered)
	if err != nil {
		return nil, err
	}
	return &CoinMBookSynchronizer{inner: inner}, nil
}

func (b *CoinMBookSynchronizer) State() CoinMBookState {
	if b == nil || b.inner == nil {
		return CoinMBookClosed
	}
	return coinMBookState(b.inner.State())
}

func (b *CoinMBookSynchronizer) Epoch() uint64 {
	if b == nil || b.inner == nil {
		return 0
	}
	return b.inner.Epoch()
}

func (b *CoinMBookSynchronizer) LastUpdateID() uint64 {
	if b == nil || b.inner == nil {
		return 0
	}
	return b.inner.LastUpdateID()
}

func (b *CoinMBookSynchronizer) ApplyUpdate(update CoinMDepthUpdate) (CoinMBookTransition, error) {
	if b == nil || b.inner == nil {
		return CoinMBookTransition{}, fmt.Errorf("%w: nil COIN-M book synchronizer", ErrCoinMInvalidMarketPayload)
	}
	if err := update.Validate(); err != nil {
		return CoinMBookTransition{}, err
	}
	transition, err := b.inner.ApplyUpdate(USDMDepthUpdate{
		EventType:         update.EventType,
		EventTimeMS:       update.EventTimeMS,
		TransactionTimeMS: update.TransactionTimeMS,
		Symbol:            update.Symbol,
		FirstUpdateID:     update.FirstUpdateID,
		FinalUpdateID:     update.FinalUpdateID,
		PreviousFinalID:   update.PreviousFinalID,
		Bids:              coinMUSDMLevels(update.Bids),
		Asks:              coinMUSDMLevels(update.Asks),
	})
	return coinMBookTransition(transition), err
}

func (b *CoinMBookSynchronizer) Seed(snapshot CoinMDepthSnapshot) (CoinMBookTransition, error) {
	if b == nil || b.inner == nil {
		return CoinMBookTransition{}, fmt.Errorf("%w: nil COIN-M book synchronizer", ErrCoinMInvalidMarketPayload)
	}
	if snapshot.SourceID != CoinMSourceID || snapshot.Symbol == "" || snapshot.LastUpdateID == 0 {
		return CoinMBookTransition{}, fmt.Errorf("%w: invalid typed depth snapshot", ErrCoinMInvalidMarketPayload)
	}
	transition, err := b.inner.Seed(USDMDepthSnapshot{
		LastUpdateID:      snapshot.LastUpdateID,
		EventTimeMS:       snapshot.EventTimeMS,
		TransactionTimeMS: snapshot.TransactionTimeMS,
		Bids:              coinMUSDMLevels(snapshot.Bids),
		Asks:              coinMUSDMLevels(snapshot.Asks),
	})
	return coinMBookTransition(transition), err
}

func (b *CoinMBookSynchronizer) Level(side normalize.Side, price string) (string, bool) {
	if b == nil || b.inner == nil {
		return "", false
	}
	return b.inner.Level(side, price)
}

func coinMUSDMLevels(levels []CoinMRawLevel) []USDMRawLevel {
	converted := make([]USDMRawLevel, len(levels))
	for i, level := range levels {
		converted[i] = USDMRawLevel{Price: level.PriceText, Quantity: level.ContractsText}
	}
	return converted
}

func coinMBookState(state USDMBookState) CoinMBookState {
	switch state {
	case USDMBookAwaitingSnapshot:
		return CoinMBookAwaitingSnapshot
	case USDMBookBridging:
		return CoinMBookBridging
	case USDMBookLive:
		return CoinMBookLive
	default:
		return CoinMBookClosed
	}
}

func coinMBookTransition(transition USDMBookTransition) CoinMBookTransition {
	return CoinMBookTransition{
		State:           coinMBookState(transition.State),
		Epoch:           transition.Epoch,
		LastUpdateID:    transition.LastUpdateID,
		Applied:         transition.Applied,
		Stale:           transition.Stale,
		NeedsSnapshot:   transition.NeedsSnapshot,
		ContinuityError: transition.ContinuityError,
	}
}
