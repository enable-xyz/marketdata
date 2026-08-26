package bybit

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/enable-xyz/marketdata/normalize"
)

type OptionTrade struct {
	Topic         string
	BaseCoin      string
	Symbol        string
	TradeID       string
	TakerSide     string
	Price         string
	Size          string
	TickDirection string
	BlockTrade    *bool
	RPITrade      *bool
	MarkPrice     NativeField
	IndexPrice    NativeField
	MarkIV        NativeField
	TradeIV       NativeField
	TradeTimeMS   int64
	SystemTimeMS  int64
	CrossSequence uint64
}

func ParseOptionTrades(payload []byte) ([]OptionTrade, error) {
	if err := validateOptionPayload(payload, optionWSPayloadPolicy()); err != nil {
		return nil, err
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
			MarkPrice     json.RawMessage `json:"mP"`
			IndexPrice    json.RawMessage `json:"iP"`
			MarkIV        json.RawMessage `json:"mIv"`
			TradeIV       json.RawMessage `json:"iv"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &message); err != nil || message.Type != "snapshot" || message.TS < 0 || len(message.Data) == 0 || len(message.Data) > 1024 {
		return nil, ErrInvalidPayload
	}
	const prefix = "publicTrade."
	if !stringsHasPrefixExact(message.Topic, prefix) {
		return nil, ErrInvalidPayload
	}
	baseCoin := message.Topic[len(prefix):]
	if !validBaseCoin(baseCoin) {
		return nil, ErrInvalidPayload
	}
	trades := make([]OptionTrade, len(message.Data))
	for ordinal, native := range message.Data {
		identity, ok := parseOptionSymbol(native.Symbol)
		if !ok || identity.base != baseCoin || native.TradeID == "" || len(native.TradeID) > 128 ||
			(native.Side != "Buy" && native.Side != "Sell") || !validDecimalText(native.Price) || !validDecimalText(native.Size) ||
			native.TradeTime < 0 || native.TradeTime > (1<<63-1)/int64(1e6) {
			return nil, fmt.Errorf("%w: option trade at ordinal %d", ErrInvalidPayload, ordinal)
		}
		sequence, err := decodeUint(native.Sequence)
		if err != nil {
			return nil, fmt.Errorf("%w: option trade sequence at ordinal %d", ErrInvalidPayload, ordinal)
		}
		sourceTimeNS := native.TradeTime * int64(1e6)
		markPrice, err := decodeOptionNativeField(native.MarkPrice, sourceTimeNS)
		if err != nil {
			return nil, fmt.Errorf("%w: option trade mark price at ordinal %d", ErrInvalidPayload, ordinal)
		}
		indexPrice, err := decodeOptionNativeField(native.IndexPrice, sourceTimeNS)
		if err != nil {
			return nil, fmt.Errorf("%w: option trade index price at ordinal %d", ErrInvalidPayload, ordinal)
		}
		markIV, err := decodeOptionNativeField(native.MarkIV, sourceTimeNS)
		if err != nil {
			return nil, fmt.Errorf("%w: option trade mark IV at ordinal %d", ErrInvalidPayload, ordinal)
		}
		tradeIV, err := decodeOptionNativeField(native.TradeIV, sourceTimeNS)
		if err != nil {
			return nil, fmt.Errorf("%w: option trade IV at ordinal %d", ErrInvalidPayload, ordinal)
		}
		trades[ordinal] = OptionTrade{
			Topic: message.Topic, BaseCoin: baseCoin, Symbol: native.Symbol, TradeID: native.TradeID, TakerSide: native.Side,
			Price: native.Price, Size: native.Size, TickDirection: native.TickDirection, BlockTrade: cloneBool(native.BlockTrade), RPITrade: cloneBool(native.RPITrade),
			MarkPrice: markPrice, IndexPrice: indexPrice, MarkIV: markIV, TradeIV: tradeIV,
			TradeTimeMS: native.TradeTime, SystemTimeMS: message.TS, CrossSequence: sequence,
		}
	}
	return trades, nil
}

func decodeOptionNativeField(raw json.RawMessage, sourceTimeNS int64) (NativeField, error) {
	if len(raw) == 0 {
		return NativeField{State: normalize.SourceMissing}, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return NativeField{State: normalize.SourceNull, SourceTimeNS: sourceTimeNS}, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return NativeField{}, ErrInvalidPayload
	}
	if text == "" {
		return NativeField{State: normalize.SourceEmpty, SourceTimeNS: sourceTimeNS}, nil
	}
	if !validDecimalText(text) {
		return NativeField{}, ErrInvalidPayload
	}
	return NativeField{State: normalize.SourceValue, Text: text, SourceTimeNS: sourceTimeNS}, nil
}

func stringsHasPrefixExact(value, prefix string) bool {
	return len(value) > len(prefix) && value[:len(prefix)] == prefix
}
