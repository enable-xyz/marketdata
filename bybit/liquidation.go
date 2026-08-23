package bybit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/enable-xyz/marketdata/normalize"
)

type LiquidationDataShape string

const (
	LiquidationDataObject LiquidationDataShape = "object"
	LiquidationDataArray  LiquidationDataShape = "array"
)

type AllLiquidation struct {
	UpdatedTimeMS   int64
	Symbol          string
	PositionSide    string
	ExecutedSize    string
	BankruptcyPrice string
}

type AllLiquidationBatch struct {
	Category     Category
	Topic        string
	MessageType  string
	SystemTimeMS int64
	DataShape    LiquidationDataShape
	Events       []AllLiquidation
}

func ParseAllLiquidation(category Category, payload []byte) (AllLiquidationBatch, error) {
	if err := category.Validate(); err != nil {
		return AllLiquidationBatch{}, err
	}
	if category == Spot {
		return AllLiquidationBatch{}, fmt.Errorf("%w: Spot liquidation is explicitly unsupported", ErrUnsupportedRole)
	}
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return AllLiquidationBatch{}, ErrInvalidPayload
	}
	var envelope struct {
		Topic string          `json:"topic"`
		Type  string          `json:"type"`
		TS    int64           `json:"ts"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope.Type != "snapshot" || envelope.TS < 0 || envelope.TS > (1<<63-1)/int64(1e6) {
		return AllLiquidationBatch{}, ErrInvalidPayload
	}
	trimmed := bytes.TrimSpace(envelope.Data)
	var native []struct {
		UpdatedTime int64  `json:"T"`
		Symbol      string `json:"s"`
		Side        string `json:"S"`
		Size        string `json:"v"`
		Price       string `json:"p"`
	}
	shape := LiquidationDataArray
	switch {
	case len(trimmed) > 0 && trimmed[0] == '[':
		if err := json.Unmarshal(trimmed, &native); err != nil {
			return AllLiquidationBatch{}, ErrInvalidPayload
		}
	case len(trimmed) > 0 && trimmed[0] == '{':
		shape = LiquidationDataObject
		var event struct {
			UpdatedTime int64  `json:"T"`
			Symbol      string `json:"s"`
			Side        string `json:"S"`
			Size        string `json:"v"`
			Price       string `json:"p"`
		}
		if err := json.Unmarshal(trimmed, &event); err != nil {
			return AllLiquidationBatch{}, ErrInvalidPayload
		}
		native = append(native, event)
	default:
		return AllLiquidationBatch{}, ErrInvalidPayload
	}
	if len(native) == 0 || len(native) > 1<<16 {
		return AllLiquidationBatch{}, ErrInvalidPayload
	}
	events := make([]AllLiquidation, len(native))
	for i, event := range native {
		if envelope.Topic != "allLiquidation."+event.Symbol || !validSymbol(event.Symbol) || (event.Side != "Buy" && event.Side != "Sell") || !validDecimalText(event.Size) || !validDecimalText(event.Price) || event.UpdatedTime < 0 || event.UpdatedTime > (1<<63-1)/int64(1e6) {
			return AllLiquidationBatch{}, fmt.Errorf("%w: liquidation at ordinal %d", ErrInvalidPayload, i)
		}
		events[i] = AllLiquidation{UpdatedTimeMS: event.UpdatedTime, Symbol: event.Symbol, PositionSide: event.Side, ExecutedSize: event.Size, BankruptcyPrice: event.Price}
	}
	return AllLiquidationBatch{Category: category, Topic: envelope.Topic, MessageType: envelope.Type, SystemTimeMS: envelope.TS, DataShape: shape, Events: events}, nil
}

type LiquidationUnitContract struct {
	Amount normalize.NativeUnit
	Price  normalize.Unit
}

func (c LiquidationUnitContract) Validate() error {
	if err := c.Amount.Validate(); err != nil {
		return err
	}
	return c.Price.Validate()
}

func (b AllLiquidationBatch) Normalized(metadata []normalize.Metadata, units LiquidationUnitContract) ([]normalize.LiquidationV1, error) {
	if b.Category != Linear && b.Category != Inverse {
		return nil, ErrUnsupportedRole
	}
	if len(metadata) != len(b.Events) || len(b.Events) == 0 {
		return nil, ErrInvalidPayload
	}
	if err := units.Validate(); err != nil {
		return nil, err
	}
	result := make([]normalize.LiquidationV1, len(b.Events))
	for i, event := range b.Events {
		amountDecimal, err := normalize.ParseDecimal(event.ExecutedSize, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
		if err != nil {
			return nil, err
		}
		price, err := makeNumericField(NativeField{State: normalize.SourceValue, Text: event.BankruptcyPrice, SourceTimeNS: event.UpdatedTimeMS * int64(1e6)}, units.Price, metadata[i].ReceivedTimeNS)
		if err != nil {
			return nil, err
		}
		side := normalize.SideBuy
		if event.PositionSide == "Sell" {
			side = normalize.SideSell
		}
		normalized := normalize.LiquidationV1{
			Metadata:         metadata[i],
			NativeSourceRole: "bybit_v5_all_liquidation",
			NativeRole:       normalize.LiquidationNativeEvent,
			Side:             side,
			SideSemantics:    normalize.LiquidationLiquidatedPositionSide,
			Amount:           normalize.NativeValue{Decimal: amountDecimal, Unit: units.Amount},
			Price:            price,
			PriceType:        normalize.LiquidationBankruptcyPrice,
			Completeness:     normalize.LiquidationComplete,
			Window: normalize.LiquidationWindow{
				Selection: normalize.LiquidationAllObserved,
				BatchID:   fmt.Sprintf("%s:%d", b.Topic, b.SystemTimeMS),
			},
		}
		if err := normalized.Validate(); err != nil {
			return nil, err
		}
		result[i] = normalized
	}
	return slices.Clone(result), nil
}
