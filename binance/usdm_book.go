package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

var (
	ErrUSDMBookGap        = errors.New("binance: USD-M book continuity gap")
	ErrUSDMBufferOverflow = errors.New("binance: USD-M book buffer overflow")
	ErrUSDMInvalidBook    = errors.New("binance: invalid USD-M book payload")
)

type USDMBookState string

const (
	USDMBookAwaitingSnapshot USDMBookState = "awaiting_snapshot"
	USDMBookBridging         USDMBookState = "bridging_snapshot"
	USDMBookLive             USDMBookState = "live"
	USDMBookGap              USDMBookState = "gap_epoch_closed"
)

type USDMRawLevel struct {
	Price    string
	Quantity string
}

type USDMDepthSnapshot struct {
	LastUpdateID      uint64
	EventTimeMS       int64
	TransactionTimeMS int64
	Bids              []USDMRawLevel
	Asks              []USDMRawLevel
}

type USDMDepthUpdate struct {
	EventType         string
	EventTimeMS       int64
	TransactionTimeMS int64
	Symbol            string
	FirstUpdateID     uint64
	FinalUpdateID     uint64
	PreviousFinalID   uint64
	Bids              []USDMRawLevel
	Asks              []USDMRawLevel
}

func ParseUSDMDepthSnapshot(raw []byte) (USDMDepthSnapshot, error) {
	var wire struct {
		LastUpdateID      uint64            `json:"lastUpdateId"`
		EventTimeMS       int64             `json:"E"`
		TransactionTimeMS int64             `json:"T"`
		Bids              []json.RawMessage `json:"bids"`
		Asks              []json.RawMessage `json:"asks"`
	}
	if len(raw) == 0 || len(raw) > USDMMaxRawPayloadBytes || json.Unmarshal(raw, &wire) != nil {
		return USDMDepthSnapshot{}, ErrUSDMInvalidBook
	}
	bids, err := parseUSDMRawLevels(wire.Bids)
	if err != nil {
		return USDMDepthSnapshot{}, err
	}
	asks, err := parseUSDMRawLevels(wire.Asks)
	if err != nil {
		return USDMDepthSnapshot{}, err
	}
	if wire.LastUpdateID == 0 {
		return USDMDepthSnapshot{}, fmt.Errorf("%w: snapshot lastUpdateId is required", ErrUSDMInvalidBook)
	}
	return USDMDepthSnapshot{LastUpdateID: wire.LastUpdateID, EventTimeMS: wire.EventTimeMS, TransactionTimeMS: wire.TransactionTimeMS, Bids: bids, Asks: asks}, nil
}

func ParseUSDMDepthUpdate(raw []byte) (USDMDepthUpdate, error) {
	var wire struct {
		EventType         string            `json:"e"`
		EventTimeMS       int64             `json:"E"`
		TransactionTimeMS int64             `json:"T"`
		Symbol            string            `json:"s"`
		FirstUpdateID     uint64            `json:"U"`
		FinalUpdateID     uint64            `json:"u"`
		PreviousFinalID   uint64            `json:"pu"`
		Bids              []json.RawMessage `json:"b"`
		Asks              []json.RawMessage `json:"a"`
	}
	if len(raw) == 0 || len(raw) > USDMMaxRawPayloadBytes || json.Unmarshal(raw, &wire) != nil {
		return USDMDepthUpdate{}, ErrUSDMInvalidBook
	}
	bids, err := parseUSDMRawLevels(wire.Bids)
	if err != nil {
		return USDMDepthUpdate{}, err
	}
	asks, err := parseUSDMRawLevels(wire.Asks)
	if err != nil {
		return USDMDepthUpdate{}, err
	}
	update := USDMDepthUpdate{EventType: wire.EventType, EventTimeMS: wire.EventTimeMS, TransactionTimeMS: wire.TransactionTimeMS, Symbol: wire.Symbol, FirstUpdateID: wire.FirstUpdateID, FinalUpdateID: wire.FinalUpdateID, PreviousFinalID: wire.PreviousFinalID, Bids: bids, Asks: asks}
	if err := update.Validate(); err != nil {
		return USDMDepthUpdate{}, err
	}
	return update, nil
}

func parseUSDMRawLevels(rawLevels []json.RawMessage) ([]USDMRawLevel, error) {
	if len(rawLevels) > normalize.MaxBookLevelsPerSide {
		return nil, fmt.Errorf("%w: too many levels", ErrUSDMInvalidBook)
	}
	levels := make([]USDMRawLevel, len(rawLevels))
	for i, raw := range rawLevels {
		var fields []json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 || json.Unmarshal(fields[0], &levels[i].Price) != nil || json.Unmarshal(fields[1], &levels[i].Quantity) != nil {
			return nil, fmt.Errorf("%w: level %d must be exactly [price,quantity] strings", ErrUSDMInvalidBook, i)
		}
		price, err := normalize.ParseDecimal(levels[i].Price, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
		if err != nil || strings.HasPrefix(price.Coefficient, "-") || price.IsZero() {
			return nil, fmt.Errorf("%w: invalid level price", ErrUSDMInvalidBook)
		}
		quantity, err := normalize.ParseDecimal(levels[i].Quantity, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
		if err != nil || strings.HasPrefix(quantity.Coefficient, "-") {
			return nil, fmt.Errorf("%w: invalid level quantity", ErrUSDMInvalidBook)
		}
	}
	return levels, nil
}

func (u USDMDepthUpdate) Validate() error {
	if u.EventType != "depthUpdate" || u.Symbol == "" || u.EventTimeMS < 0 || u.TransactionTimeMS < 0 || u.FirstUpdateID == 0 || u.FinalUpdateID < u.FirstUpdateID {
		return fmt.Errorf("%w: malformed update identity", ErrUSDMInvalidBook)
	}
	if len(u.Bids) > normalize.MaxBookLevelsPerSide || len(u.Asks) > normalize.MaxBookLevelsPerSide {
		return fmt.Errorf("%w: update level bound exceeded", ErrUSDMInvalidBook)
	}
	return nil
}

type USDMBookTransition struct {
	State           USDMBookState
	Epoch           uint64
	ClosedEpoch     uint64
	LastUpdateID    uint64
	Applied         bool
	Stale           bool
	NeedsSnapshot   bool
	ContinuityError error
}

type USDMBookSynchronizer struct {
	symbol      string
	maxBuffered int
	state       USDMBookState
	epoch       uint64
	lastUpdate  uint64
	buffer      []USDMDepthUpdate
	bids        map[string]string
	asks        map[string]string
}

func NewUSDMBookSynchronizer(symbol string, maxBuffered int) (*USDMBookSynchronizer, error) {
	if symbol == "" || maxBuffered <= 0 {
		return nil, fmt.Errorf("%w: symbol and positive buffer bound are required", ErrUSDMInvalidBook)
	}
	return &USDMBookSynchronizer{symbol: symbol, maxBuffered: maxBuffered, state: USDMBookAwaitingSnapshot, epoch: 1}, nil
}

func (b *USDMBookSynchronizer) State() USDMBookState { return b.state }
func (b *USDMBookSynchronizer) Epoch() uint64        { return b.epoch }
func (b *USDMBookSynchronizer) LastUpdateID() uint64 { return b.lastUpdate }

func (b *USDMBookSynchronizer) ApplyUpdate(update USDMDepthUpdate) (USDMBookTransition, error) {
	if err := b.validateUpdate(update); err != nil {
		return b.transition(false, false, err), err
	}
	switch b.state {
	case USDMBookAwaitingSnapshot, USDMBookGap:
		if len(b.buffer) == b.maxBuffered {
			err := ErrUSDMBufferOverflow
			return b.closeEpoch(err), err
		}
		b.buffer = append(b.buffer, cloneUSDMDepthUpdate(update))
		return b.transition(false, false, nil), nil
	case USDMBookBridging:
		return b.applyBridge(update)
	case USDMBookLive:
		return b.applyLive(update)
	default:
		err := fmt.Errorf("%w: unknown book state", ErrUSDMInvalidBook)
		return b.transition(false, false, err), err
	}
}

func (b *USDMBookSynchronizer) Seed(snapshot USDMDepthSnapshot) (USDMBookTransition, error) {
	if snapshot.LastUpdateID == 0 || b.state == USDMBookLive {
		return b.transition(false, false, ErrUSDMInvalidBook), ErrUSDMInvalidBook
	}
	bids := make(map[string]string, len(snapshot.Bids))
	asks := make(map[string]string, len(snapshot.Asks))
	if err := replaceUSDMSide(bids, snapshot.Bids); err != nil {
		return b.transition(false, false, err), err
	}
	if err := replaceUSDMSide(asks, snapshot.Asks); err != nil {
		return b.transition(false, false, err), err
	}
	b.bids, b.asks = bids, asks
	b.lastUpdate = snapshot.LastUpdateID
	b.state = USDMBookBridging
	buffered := b.buffer
	b.buffer = nil
	for _, update := range buffered {
		transition, err := b.ApplyUpdate(update)
		if err != nil {
			return transition, err
		}
	}
	return b.transition(false, false, nil), nil
}

func (b *USDMBookSynchronizer) applyBridge(update USDMDepthUpdate) (USDMBookTransition, error) {
	if update.FinalUpdateID < b.lastUpdate {
		return b.transition(false, true, nil), nil
	}
	if update.FirstUpdateID > b.lastUpdate || update.FinalUpdateID < b.lastUpdate {
		err := fmt.Errorf("%w: first bridge [%d,%d] does not span snapshot %d", ErrUSDMBookGap, update.FirstUpdateID, update.FinalUpdateID, b.lastUpdate)
		return b.closeEpoch(err), err
	}
	applyUSDMSide(b.bids, update.Bids)
	applyUSDMSide(b.asks, update.Asks)
	b.lastUpdate = update.FinalUpdateID
	b.state = USDMBookLive
	return b.transition(true, false, nil), nil
}

func (b *USDMBookSynchronizer) applyLive(update USDMDepthUpdate) (USDMBookTransition, error) {
	if update.FinalUpdateID <= b.lastUpdate {
		return b.transition(false, true, nil), nil
	}
	if update.PreviousFinalID != b.lastUpdate {
		err := fmt.Errorf("%w: pu=%d previous_u=%d", ErrUSDMBookGap, update.PreviousFinalID, b.lastUpdate)
		return b.closeEpoch(err), err
	}
	applyUSDMSide(b.bids, update.Bids)
	applyUSDMSide(b.asks, update.Asks)
	b.lastUpdate = update.FinalUpdateID
	return b.transition(true, false, nil), nil
}

func (b *USDMBookSynchronizer) validateUpdate(update USDMDepthUpdate) error {
	if err := update.Validate(); err != nil {
		return err
	}
	if update.Symbol != b.symbol {
		return fmt.Errorf("%w: update symbol %q does not match %q", ErrUSDMInvalidBook, update.Symbol, b.symbol)
	}
	return nil
}

func (b *USDMBookSynchronizer) closeEpoch(err error) USDMBookTransition {
	closed := b.epoch
	b.epoch++
	b.state = USDMBookGap
	b.lastUpdate = 0
	b.buffer = nil
	b.bids = nil
	b.asks = nil
	return USDMBookTransition{State: b.state, Epoch: b.epoch, ClosedEpoch: closed, NeedsSnapshot: true, ContinuityError: err}
}

func (b *USDMBookSynchronizer) transition(applied, stale bool, err error) USDMBookTransition {
	return USDMBookTransition{State: b.state, Epoch: b.epoch, LastUpdateID: b.lastUpdate, Applied: applied, Stale: stale, NeedsSnapshot: b.state != USDMBookLive, ContinuityError: err}
}

func (b *USDMBookSynchronizer) Level(side normalize.Side, price string) (string, bool) {
	if b.state != USDMBookLive {
		return "", false
	}
	levels := b.asks
	if side == normalize.SideBuy {
		levels = b.bids
	} else if side != normalize.SideSell {
		return "", false
	}
	quantity, ok := levels[price]
	return quantity, ok
}

func replaceUSDMSide(destination map[string]string, levels []USDMRawLevel) error {
	for _, level := range levels {
		if level.Quantity == "0" || normalizeCoefficientIsZero(level.Quantity) {
			continue
		}
		destination[level.Price] = level.Quantity
	}
	return nil
}

func applyUSDMSide(destination map[string]string, levels []USDMRawLevel) {
	for _, level := range levels {
		if level.Quantity == "0" || normalizeCoefficientIsZero(level.Quantity) {
			delete(destination, level.Price)
			continue
		}
		destination[level.Price] = level.Quantity
	}
}

func normalizeCoefficientIsZero(value string) bool {
	decimal, err := normalize.ParseDecimal(value, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	return err == nil && decimal.IsZero()
}

func cloneUSDMDepthUpdate(update USDMDepthUpdate) USDMDepthUpdate {
	update.Bids = append([]USDMRawLevel(nil), update.Bids...)
	update.Asks = append([]USDMRawLevel(nil), update.Asks...)
	return update
}
