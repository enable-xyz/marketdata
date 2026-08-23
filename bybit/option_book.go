package bybit

import (
	"fmt"
	"strconv"
	"strings"
)

type OptionBookMessage struct {
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

func ParseOptionOrderbook(payload []byte) (OptionBookMessage, error) {
	if err := validateOptionPayload(payload, optionWSPayloadPolicy()); err != nil {
		return OptionBookMessage{}, err
	}
	var native nativeBookPayload
	if err := decodeBoundedJSON(payload, &native); err != nil {
		return OptionBookMessage{}, err
	}
	parts := strings.Split(native.Topic, ".")
	if len(parts) != 3 || parts[0] != "orderbook" || parts[2] != native.Data.Symbol {
		return OptionBookMessage{}, ErrInvalidPayload
	}
	if parts[1] == "full" || parts[1] == "rpi" {
		return OptionBookMessage{}, fmt.Errorf("%w: option book role %s", ErrUnsupportedRole, parts[1])
	}
	depth, err := strconv.Atoi(parts[1])
	if err != nil {
		return OptionBookMessage{}, ErrInvalidPayload
	}
	if depth == 1 {
		return OptionBookMessage{}, fmt.Errorf("%w: option public L1/BBO", ErrUnsupportedRole)
	}
	if !validOptionBookDepth(depth) || !validOptionSymbol(native.Data.Symbol) ||
		(native.Type != BookSnapshot && native.Type != BookDelta) || native.TS < 0 || native.CTS < 0 {
		return OptionBookMessage{}, ErrInvalidPayload
	}
	if len(native.Data.Bids) > depth || len(native.Data.Asks) > depth {
		return OptionBookMessage{}, fmt.Errorf("%w: option book exceeds declared depth", ErrInvalidPayload)
	}
	updateID, err := decodeUint(native.Data.U)
	if err != nil {
		return OptionBookMessage{}, err
	}
	sequence, err := decodeUint(native.Data.Seq)
	if err != nil {
		return OptionBookMessage{}, err
	}
	bids, asks, err := decodeRegularLevels(native.Data.Bids, native.Data.Asks)
	if err != nil {
		return OptionBookMessage{}, err
	}
	return OptionBookMessage{
		Topic: native.Topic, Symbol: native.Data.Symbol, Depth: depth, Kind: native.Type,
		SystemTimeMS: native.TS, EngineTimeMS: native.CTS, UpdateID: updateID, CrossSequence: sequence,
		Bids: bids, Asks: asks,
	}, nil
}

type OptionBook struct {
	symbol   string
	depth    int
	seeded   bool
	updateID uint64
	sequence uint64
	bids     map[string]string
	asks     map[string]string
}

func NewOptionBook(symbol string, depth int) (*OptionBook, error) {
	if !validOptionSymbol(symbol) || !validOptionBookDepth(depth) {
		return nil, ErrInvalidTopic
	}
	return &OptionBook{symbol: symbol, depth: depth, bids: make(map[string]string), asks: make(map[string]string)}, nil
}

func (b *OptionBook) Apply(message OptionBookMessage) error {
	if b == nil || message.Symbol != b.symbol || message.Depth != b.depth || message.Topic != fmt.Sprintf("orderbook.%d.%s", b.depth, b.symbol) {
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
	if message.Kind != BookDelta {
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

func (b *OptionBook) Reset() {
	if b == nil {
		return
	}
	b.seeded = false
	b.updateID = 0
	b.sequence = 0
	b.bids = make(map[string]string)
	b.asks = make(map[string]string)
}

func (b *OptionBook) Snapshot() OptionBookSnapshot {
	if b == nil {
		return OptionBookSnapshot{}
	}
	return OptionBookSnapshot{
		Symbol: b.symbol, Depth: b.depth, Seeded: b.seeded, UpdateID: b.updateID, CrossSequence: b.sequence,
		Bids: cloneLevelMap(b.bids), Asks: cloneLevelMap(b.asks),
	}
}

type OptionBookSnapshot struct {
	Symbol        string
	Depth         int
	Seeded        bool
	UpdateID      uint64
	CrossSequence uint64
	Bids          map[string]string
	Asks          map[string]string
}
