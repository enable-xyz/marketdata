package bybit

import (
	"encoding/json"
	"fmt"
)

type PublicTrade struct {
	Category      Category
	Topic         string
	Symbol        string
	TradeID       string
	TakerSide     string
	Price         string
	Size          string
	TickDirection string
	BlockTrade    *bool
	RPITrade      *bool
	TradeTimeMS   int64
	SystemTimeMS  int64
	CrossSequence uint64
}

func ParsePublicTrades(category Category, payload []byte) ([]PublicTrade, error) {
	if err := category.Validate(); err != nil {
		return nil, err
	}
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return nil, ErrInvalidPayload
	}
	var message struct {
		Topic string `json:"topic"`
		Type  string `json:"type"`
		TS    int64  `json:"ts"`
		Data  []struct {
			TradeTime     int64           `json:"T"`
			Symbol        string          `json:"s"`
			Side          string          `json:"S"`
			Size          string          `json:"v"`
			Price         string          `json:"p"`
			TickDirection string          `json:"L"`
			TradeID       string          `json:"i"`
			BlockTrade    *bool           `json:"BT"`
			RPITrade      *bool           `json:"RPI"`
			Sequence      json.RawMessage `json:"seq"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &message); err != nil || message.Type != "snapshot" || message.TS < 0 || len(message.Data) == 0 || len(message.Data) > 1024 {
		return nil, ErrInvalidPayload
	}
	trades := make([]PublicTrade, len(message.Data))
	for i, native := range message.Data {
		if message.Topic != "publicTrade."+native.Symbol || !validSymbol(native.Symbol) || native.TradeID == "" || len(native.TradeID) > 128 || (native.Side != "Buy" && native.Side != "Sell") || !validDecimalText(native.Price) || !validDecimalText(native.Size) || native.TradeTime < 0 {
			return nil, fmt.Errorf("%w: trade at ordinal %d", ErrInvalidPayload, i)
		}
		sequence, err := decodeUint(native.Sequence)
		if err != nil {
			return nil, fmt.Errorf("%w: trade sequence at ordinal %d", ErrInvalidPayload, i)
		}
		trades[i] = PublicTrade{Category: category, Topic: message.Topic, Symbol: native.Symbol, TradeID: native.TradeID, TakerSide: native.Side, Price: native.Price, Size: native.Size, TickDirection: native.TickDirection, BlockTrade: cloneBool(native.BlockTrade), RPITrade: cloneBool(native.RPITrade), TradeTimeMS: native.TradeTime, SystemTimeMS: message.TS, CrossSequence: sequence}
	}
	return trades, nil
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
