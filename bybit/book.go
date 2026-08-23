package bybit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type BookMessageKind string

const (
	BookSnapshot BookMessageKind = "snapshot"
	BookDelta    BookMessageKind = "delta"
)

type PriceLevel struct {
	Price  string
	Amount string
}

type RPIPriceLevel struct {
	Price        string
	NonRPIAmount string
	RPIAmount    string
}

type BoundedBookMessage struct {
	Category      Category
	Topic         string
	Symbol        string
	Depth         int
	Kind          BookMessageKind
	SystemTimeMS  int64
	EngineTimeMS  int64
	UpdateID      uint64
	CrossSequence uint64
	Bids          []PriceLevel
	Asks          []PriceLevel
}

type FullBookDelta struct {
	Category      Category
	Topic         string
	Symbol        string
	SystemTimeMS  int64
	EngineTimeMS  int64
	UpdateID      uint64
	CrossSequence uint64
	Bids          []PriceLevel
	Asks          []PriceLevel
}

type RPIBookMessage struct {
	Category      Category
	Topic         string
	Symbol        string
	Kind          BookMessageKind
	SystemTimeMS  int64
	EngineTimeMS  int64
	UpdateID      uint64
	CrossSequence uint64
	Bids          []RPIPriceLevel
	Asks          []RPIPriceLevel
}

type nativeBookPayload struct {
	Topic string          `json:"topic"`
	Type  BookMessageKind `json:"type"`
	TS    int64           `json:"ts"`
	CTS   int64           `json:"cts"`
	Data  struct {
		Symbol string          `json:"s"`
		Bids   [][]string      `json:"b"`
		Asks   [][]string      `json:"a"`
		U      json.RawMessage `json:"u"`
		Seq    json.RawMessage `json:"seq"`
	} `json:"data"`
}

func ParseBoundedOrderbook(category Category, payload []byte) (BoundedBookMessage, error) {
	if err := category.Validate(); err != nil {
		return BoundedBookMessage{}, err
	}
	var native nativeBookPayload
	if err := decodeBoundedJSON(payload, &native); err != nil {
		return BoundedBookMessage{}, err
	}
	parts := strings.Split(native.Topic, ".")
	if len(parts) != 3 || parts[0] != "orderbook" || parts[1] == "full" || parts[1] == "rpi" || parts[2] != native.Data.Symbol {
		return BoundedBookMessage{}, ErrInvalidPayload
	}
	depth, err := strconv.Atoi(parts[1])
	if err != nil || !validBoundedDepth(depth) || !validSymbol(parts[2]) {
		return BoundedBookMessage{}, ErrInvalidPayload
	}
	if native.Type != BookSnapshot && native.Type != BookDelta {
		return BoundedBookMessage{}, ErrInvalidPayload
	}
	if depth == 1 && native.Type != BookSnapshot {
		return BoundedBookMessage{}, fmt.Errorf("%w: level-1 orderbook is snapshot-only", ErrInvalidPayload)
	}
	u, err := decodeUint(native.Data.U)
	if err != nil {
		return BoundedBookMessage{}, err
	}
	seq, err := decodeUint(native.Data.Seq)
	if err != nil {
		return BoundedBookMessage{}, err
	}
	bids, asks, err := decodeRegularLevels(native.Data.Bids, native.Data.Asks)
	if err != nil {
		return BoundedBookMessage{}, err
	}
	return BoundedBookMessage{Category: category, Topic: native.Topic, Symbol: native.Data.Symbol, Depth: depth, Kind: native.Type, SystemTimeMS: native.TS, EngineTimeMS: native.CTS, UpdateID: u, CrossSequence: seq, Bids: bids, Asks: asks}, nil
}

func ParseFullOrderbookDelta(category Category, payload []byte) (FullBookDelta, error) {
	if err := category.Validate(); err != nil {
		return FullBookDelta{}, err
	}
	var native nativeBookPayload
	if err := decodeBoundedJSON(payload, &native); err != nil {
		return FullBookDelta{}, err
	}
	if native.Type != BookDelta || native.Topic != "orderbook.full."+native.Data.Symbol || !validSymbol(native.Data.Symbol) {
		return FullBookDelta{}, fmt.Errorf("%w: full book is a delta-only distinct topic", ErrInvalidPayload)
	}
	u, err := decodeUint(native.Data.U)
	if err != nil || u == 0 {
		return FullBookDelta{}, ErrInvalidPayload
	}
	seq, err := decodeUint(native.Data.Seq)
	if err != nil || seq == 0 {
		return FullBookDelta{}, ErrInvalidPayload
	}
	bids, asks, err := decodeRegularLevels(native.Data.Bids, native.Data.Asks)
	if err != nil {
		return FullBookDelta{}, err
	}
	return FullBookDelta{Category: category, Topic: native.Topic, Symbol: native.Data.Symbol, SystemTimeMS: native.TS, EngineTimeMS: native.CTS, UpdateID: u, CrossSequence: seq, Bids: bids, Asks: asks}, nil
}

func ParseRPIOrderbook(category Category, payload []byte) (RPIBookMessage, error) {
	if err := category.Validate(); err != nil {
		return RPIBookMessage{}, err
	}
	var native nativeBookPayload
	if err := decodeBoundedJSON(payload, &native); err != nil {
		return RPIBookMessage{}, err
	}
	if native.Topic != "orderbook.rpi."+native.Data.Symbol || !validSymbol(native.Data.Symbol) || (native.Type != BookSnapshot && native.Type != BookDelta) {
		return RPIBookMessage{}, fmt.Errorf("%w: RPI source role mismatch", ErrInvalidPayload)
	}
	u, err := decodeUint(native.Data.U)
	if err != nil {
		return RPIBookMessage{}, err
	}
	seq, err := decodeUint(native.Data.Seq)
	if err != nil {
		return RPIBookMessage{}, err
	}
	bids, err := decodeRPILevels(native.Data.Bids)
	if err != nil {
		return RPIBookMessage{}, err
	}
	asks, err := decodeRPILevels(native.Data.Asks)
	if err != nil {
		return RPIBookMessage{}, err
	}
	return RPIBookMessage{Category: category, Topic: native.Topic, Symbol: native.Data.Symbol, Kind: native.Type, SystemTimeMS: native.TS, EngineTimeMS: native.CTS, UpdateID: u, CrossSequence: seq, Bids: bids, Asks: asks}, nil
}

func decodeBoundedJSON(payload []byte, destination any) error {
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return ErrInvalidPayload
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	return nil
}

func decodeUint(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 {
		return 0, ErrInvalidPayload
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, ErrInvalidPayload
	}
	value, err := strconv.ParseUint(string(number), 10, 64)
	if err != nil {
		return 0, ErrInvalidPayload
	}
	return value, nil
}

func decodeRegularLevels(bidsNative, asksNative [][]string) ([]PriceLevel, []PriceLevel, error) {
	bids := make([]PriceLevel, len(bidsNative))
	for i, level := range bidsNative {
		if len(level) != 2 || !validDecimalText(level[0]) || !validDecimalText(level[1]) {
			return nil, nil, ErrInvalidPayload
		}
		bids[i] = PriceLevel{Price: level[0], Amount: level[1]}
	}
	asks := make([]PriceLevel, len(asksNative))
	for i, level := range asksNative {
		if len(level) != 2 || !validDecimalText(level[0]) || !validDecimalText(level[1]) {
			return nil, nil, ErrInvalidPayload
		}
		asks[i] = PriceLevel{Price: level[0], Amount: level[1]}
	}
	return bids, asks, nil
}

func decodeRPILevels(native [][]string) ([]RPIPriceLevel, error) {
	levels := make([]RPIPriceLevel, len(native))
	for i, level := range native {
		if len(level) != 3 || !validDecimalText(level[0]) || !validDecimalText(level[1]) || !validDecimalText(level[2]) {
			return nil, ErrInvalidPayload
		}
		levels[i] = RPIPriceLevel{Price: level[0], NonRPIAmount: level[1], RPIAmount: level[2]}
	}
	return levels, nil
}

func validDecimalText(text string) bool {
	if text == "" || len(text) > 128 || text[0] == '+' {
		return false
	}
	digits, dots := 0, 0
	for i, r := range text {
		if r == '-' && i == 0 {
			continue
		}
		if r == '.' {
			dots++
			continue
		}
		if r < '0' || r > '9' {
			return false
		}
		digits++
	}
	return digits > 0 && dots <= 1 && text != "." && text != "-." && text[len(text)-1] != '.'
}

type BoundedBook struct {
	category Category
	symbol   string
	depth    int
	seeded   bool
	updateID uint64
	sequence uint64
	bids     map[string]string
	asks     map[string]string
}

func NewBoundedBook(category Category, symbol string, depth int) (*BoundedBook, error) {
	if err := category.Validate(); err != nil {
		return nil, err
	}
	if !validSymbol(symbol) || !validBoundedDepth(depth) {
		return nil, ErrInvalidTopic
	}
	return &BoundedBook{category: category, symbol: symbol, depth: depth, bids: make(map[string]string), asks: make(map[string]string)}, nil
}

func (b *BoundedBook) Apply(message BoundedBookMessage) error {
	if b == nil || message.Category != b.category || message.Symbol != b.symbol || message.Depth != b.depth || message.Topic != fmt.Sprintf("orderbook.%d.%s", b.depth, b.symbol) {
		return ErrInvalidPayload
	}
	if message.Kind == BookSnapshot {
		b.bids = make(map[string]string, len(message.Bids))
		b.asks = make(map[string]string, len(message.Asks))
		applyLevels(b.bids, message.Bids)
		applyLevels(b.asks, message.Asks)
		b.seeded = true
		b.updateID = message.UpdateID
		b.sequence = message.CrossSequence
		return nil
	}
	if message.Kind != BookDelta || b.depth == 1 {
		return ErrInvalidPayload
	}
	if message.UpdateID == 1 {
		b.Reset()
		return ErrBookNeedsSnapshot
	}
	if !b.seeded {
		return ErrBookNeedsSnapshot
	}
	applyLevels(b.bids, message.Bids)
	applyLevels(b.asks, message.Asks)
	b.updateID = message.UpdateID
	b.sequence = message.CrossSequence
	return nil
}

func (b *BoundedBook) Reset() {
	if b == nil {
		return
	}
	b.seeded = false
	b.updateID = 0
	b.sequence = 0
	b.bids = make(map[string]string)
	b.asks = make(map[string]string)
}

func (b *BoundedBook) Snapshot() BoundedBookSnapshot {
	if b == nil {
		return BoundedBookSnapshot{}
	}
	return BoundedBookSnapshot{Category: b.category, Symbol: b.symbol, Depth: b.depth, Seeded: b.seeded, UpdateID: b.updateID, CrossSequence: b.sequence, Bids: cloneLevelMap(b.bids), Asks: cloneLevelMap(b.asks), RPIIncluded: false}
}

type BoundedBookSnapshot struct {
	Category      Category
	Symbol        string
	Depth         int
	Seeded        bool
	UpdateID      uint64
	CrossSequence uint64
	Bids          map[string]string
	Asks          map[string]string
	RPIIncluded   bool
}

type RPIBook struct {
	category Category
	symbol   string
	seeded   bool
	updateID uint64
	sequence uint64
	bids     map[string]RPIPriceLevel
	asks     map[string]RPIPriceLevel
}

func NewRPIBook(category Category, symbol string) (*RPIBook, error) {
	if err := category.Validate(); err != nil {
		return nil, err
	}
	if !validSymbol(symbol) {
		return nil, ErrInvalidTopic
	}
	return &RPIBook{category: category, symbol: symbol, bids: make(map[string]RPIPriceLevel), asks: make(map[string]RPIPriceLevel)}, nil
}

func (b *RPIBook) Apply(message RPIBookMessage) error {
	if b == nil || message.Category != b.category || message.Symbol != b.symbol || message.Topic != "orderbook.rpi."+b.symbol {
		return ErrInvalidPayload
	}
	if message.Kind == BookSnapshot {
		b.bids = make(map[string]RPIPriceLevel, len(message.Bids))
		b.asks = make(map[string]RPIPriceLevel, len(message.Asks))
		applyRPILevels(b.bids, message.Bids)
		applyRPILevels(b.asks, message.Asks)
		b.seeded = true
		b.updateID = message.UpdateID
		b.sequence = message.CrossSequence
		return nil
	}
	if message.Kind != BookDelta || !b.seeded {
		return ErrBookNeedsSnapshot
	}
	if message.UpdateID == 1 {
		b.Reset()
		return ErrBookNeedsSnapshot
	}
	applyRPILevels(b.bids, message.Bids)
	applyRPILevels(b.asks, message.Asks)
	b.updateID = message.UpdateID
	b.sequence = message.CrossSequence
	return nil
}

func (b *RPIBook) Reset() {
	if b == nil {
		return
	}
	b.seeded = false
	b.updateID = 0
	b.sequence = 0
	b.bids = make(map[string]RPIPriceLevel)
	b.asks = make(map[string]RPIPriceLevel)
}

func (b *RPIBook) Snapshot() RPIBookSnapshot {
	if b == nil {
		return RPIBookSnapshot{}
	}
	return RPIBookSnapshot{Category: b.category, Symbol: b.symbol, Seeded: b.seeded, UpdateID: b.updateID, CrossSequence: b.sequence, Bids: cloneRPILevelMap(b.bids), Asks: cloneRPILevelMap(b.asks), SourceRole: RoleRPIOrderbook}
}

type RPIBookSnapshot struct {
	Category      Category
	Symbol        string
	Seeded        bool
	UpdateID      uint64
	CrossSequence uint64
	Bids          map[string]RPIPriceLevel
	Asks          map[string]RPIPriceLevel
	SourceRole    SourceRole
}

type FullBookState string

const (
	FullBookBuffering   FullBookState = "buffering"
	FullBookLive        FullBookState = "live"
	FullBookNeedsResync FullBookState = "needs_snapshot"
	FullBookUnavailable FullBookState = "unavailable"
)

type FullBookSnapshot struct {
	Category             Category
	Symbol               string
	UpdateID             uint64
	CrossSequence        uint64
	Bids                 []PriceLevel
	Asks                 []PriceLevel
	MaximumLevelsPerSide uint32
}

type FullBook struct {
	category Category
	symbol   string
	state    FullBookState
	updateID uint64
	sequence uint64
	buffer   []FullBookDelta
	bids     map[string]string
	asks     map[string]string
}

func NewFullBook(category Category, symbol string) (*FullBook, error) {
	if err := category.Validate(); err != nil {
		return nil, err
	}
	if !validSymbol(symbol) {
		return nil, ErrInvalidTopic
	}
	return &FullBook{category: category, symbol: symbol, state: FullBookBuffering, bids: make(map[string]string), asks: make(map[string]string)}, nil
}

func (b *FullBook) Accept(delta FullBookDelta) error {
	if b == nil || delta.Category != b.category || delta.Symbol != b.symbol || delta.Topic != "orderbook.full."+b.symbol || delta.UpdateID == 0 || delta.CrossSequence == 0 {
		return ErrInvalidPayload
	}
	if delta.UpdateID == 1 {
		b.clearForResync(FullBookNeedsResync)
		b.buffer = append(b.buffer, cloneFullDelta(delta))
		return ErrBookNeedsSnapshot
	}
	if b.state == FullBookLive {
		if delta.CrossSequence < b.sequence {
			return ErrFullSequence
		}
		if delta.UpdateID <= b.updateID {
			return nil
		}
		if delta.UpdateID != b.updateID+1 {
			b.clearForResync(FullBookNeedsResync)
			b.buffer = append(b.buffer, cloneFullDelta(delta))
			return ErrFullBookGap
		}
		applyLevels(b.bids, delta.Bids)
		applyLevels(b.asks, delta.Asks)
		b.updateID = delta.UpdateID
		b.sequence = delta.CrossSequence
		return nil
	}
	if len(b.buffer) != 0 {
		prior := b.buffer[len(b.buffer)-1]
		if delta.CrossSequence < prior.CrossSequence {
			return ErrFullSequence
		}
		if delta.UpdateID != prior.UpdateID+1 {
			b.buffer = b.buffer[:0]
			b.buffer = append(b.buffer, cloneFullDelta(delta))
			b.state = FullBookNeedsResync
			return ErrFullBookGap
		}
	}
	b.buffer = append(b.buffer, cloneFullDelta(delta))
	if b.state == FullBookUnavailable {
		b.state = FullBookBuffering
	}
	return nil
}

func (b *FullBook) Seed(snapshot FullBookSnapshot) error {
	if b == nil || snapshot.Category != b.category || snapshot.Symbol != b.symbol || snapshot.UpdateID == 0 || snapshot.CrossSequence == 0 || snapshot.MaximumLevelsPerSide == 0 || snapshot.MaximumLevelsPerSide > 10000 || len(snapshot.Bids) > int(snapshot.MaximumLevelsPerSide) || len(snapshot.Asks) > int(snapshot.MaximumLevelsPerSide) || len(b.buffer) == 0 {
		return ErrFullSnapshotStale
	}
	match := -1
	for i, delta := range b.buffer {
		if delta.CrossSequence < snapshot.CrossSequence {
			continue
		}
		if delta.CrossSequence == snapshot.CrossSequence {
			if delta.UpdateID != snapshot.UpdateID {
				return ErrFullSnapshotStale
			}
			match = i
		}
		break
	}
	if match < 0 {
		return ErrFullSnapshotStale
	}
	b.bids = make(map[string]string, len(snapshot.Bids))
	b.asks = make(map[string]string, len(snapshot.Asks))
	applyLevels(b.bids, snapshot.Bids)
	applyLevels(b.asks, snapshot.Asks)
	b.updateID = snapshot.UpdateID
	b.sequence = snapshot.CrossSequence
	for _, delta := range b.buffer[match+1:] {
		if delta.UpdateID != b.updateID+1 || delta.CrossSequence < b.sequence {
			b.clearForResync(FullBookNeedsResync)
			return ErrFullBookGap
		}
		applyLevels(b.bids, delta.Bids)
		applyLevels(b.asks, delta.Asks)
		b.updateID = delta.UpdateID
		b.sequence = delta.CrossSequence
	}
	b.buffer = nil
	b.state = FullBookLive
	return nil
}

func (b *FullBook) MarkUnavailable() {
	if b == nil {
		return
	}
	b.clearForResync(FullBookUnavailable)
}

func (b *FullBook) Snapshot() FullBookView {
	if b == nil {
		return FullBookView{}
	}
	return FullBookView{Category: b.category, Symbol: b.symbol, State: b.state, UpdateID: b.updateID, CrossSequence: b.sequence, BufferedDeltas: len(b.buffer), Bids: cloneLevelMap(b.bids), Asks: cloneLevelMap(b.asks), SnapshotMaximumLevelsPerSide: 10000, CompleteOutsideSnapshotRange: false, RPIIncluded: false}
}

type FullBookView struct {
	Category                     Category
	Symbol                       string
	State                        FullBookState
	UpdateID                     uint64
	CrossSequence                uint64
	BufferedDeltas               int
	Bids                         map[string]string
	Asks                         map[string]string
	SnapshotMaximumLevelsPerSide uint32
	CompleteOutsideSnapshotRange bool
	RPIIncluded                  bool
}

func (b *FullBook) clearForResync(state FullBookState) {
	b.state = state
	b.updateID = 0
	b.sequence = 0
	b.buffer = nil
	b.bids = make(map[string]string)
	b.asks = make(map[string]string)
}

func applyLevels(book map[string]string, levels []PriceLevel) {
	for _, level := range levels {
		if isZeroDecimal(level.Amount) {
			delete(book, level.Price)
			continue
		}
		book[level.Price] = level.Amount
	}
}

func applyRPILevels(book map[string]RPIPriceLevel, levels []RPIPriceLevel) {
	for _, level := range levels {
		if isZeroDecimal(level.NonRPIAmount) && isZeroDecimal(level.RPIAmount) {
			delete(book, level.Price)
			continue
		}
		book[level.Price] = level
	}
}

func isZeroDecimal(value string) bool {
	trimmed := strings.TrimLeft(value, "-0")
	trimmed = strings.TrimLeft(trimmed, ".0")
	return trimmed == ""
}

func cloneLevelMap(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for price, amount := range source {
		clone[price] = amount
	}
	return clone
}

func cloneRPILevelMap(source map[string]RPIPriceLevel) map[string]RPIPriceLevel {
	clone := make(map[string]RPIPriceLevel, len(source))
	for price, level := range source {
		clone[price] = level
	}
	return clone
}

func cloneFullDelta(delta FullBookDelta) FullBookDelta {
	delta.Bids = append([]PriceLevel(nil), delta.Bids...)
	delta.Asks = append([]PriceLevel(nil), delta.Asks...)
	return delta
}
