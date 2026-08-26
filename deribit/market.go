package deribit

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type MappedNumber struct {
	State normalize.SourceState
	Value normalize.Numeric
}

type MappedNativeNumber struct {
	State normalize.SourceState
	Value normalize.NativeValue
}

type NativeBookLevel struct {
	Action string
	Price  SourceNumber
	Amount SourceNumber
}

type BookMessage struct {
	Channel        string
	InstrumentName string
	Kind           normalize.DeribitBookUpdateKind
	ChangeID       uint64
	PreviousID     normalize.OptionalUint64
	TimestampMS    int64
	GroupedView    bool
	Bids           []NativeBookLevel
	Asks           []NativeBookLevel
}

func ParseBook(payload []byte) (BookMessage, error) {
	channel, raw, err := notificationData(payload)
	if err != nil || !strings.HasPrefix(channel, "book.") {
		return BookMessage{}, ErrInvalidRPC
	}
	parts := strings.Split(channel, ".")
	if len(parts) != 3 && len(parts) != 5 {
		return BookMessage{}, ErrInvalidRPC
	}
	intervalIndex := 2
	if len(parts) == 5 {
		depth, parseErr := strconv.Atoi(parts[3])
		if parseErr != nil || !validGroup(parts[2]) || (depth != 1 && depth != 10 && depth != 20) {
			return BookMessage{}, ErrInvalidRPC
		}
		intervalIndex = 4
	}
	if err := Cadence(parts[intervalIndex]).Validate(); err != nil {
		return BookMessage{}, ErrInvalidRPC
	}
	object, err := decodeObject(raw)
	if err != nil {
		return BookMessage{}, err
	}
	instrument, err := requiredString(object, "instrument_name")
	if err != nil || instrument != parts[1] {
		return BookMessage{}, ErrInvalidRPC
	}
	kindText, err := requiredString(object, "type")
	if err != nil {
		return BookMessage{}, err
	}
	kind := normalize.DeribitBookUpdateKind(kindText)
	if kind != normalize.DeribitBookSnapshot && kind != normalize.DeribitBookChange {
		return BookMessage{}, ErrInvalidRPC
	}
	changeID, err := requiredUint64(object, "change_id")
	if err != nil || changeID == 0 {
		return BookMessage{}, ErrInvalidRPC
	}
	timestamp, err := requiredInt64(object, "timestamp")
	if err != nil {
		return BookMessage{}, err
	}
	previous := normalize.OptionalUint64{}
	if rawPrevious, exists := object["prev_change_id"]; exists {
		previousObject := map[string]json.RawMessage{"value": rawPrevious}
		value, parseErr := requiredUint64(previousObject, "value")
		if parseErr != nil || value == 0 {
			return BookMessage{}, ErrInvalidRPC
		}
		previous = normalize.OptionalUint64{Value: value, Valid: true}
	}
	if (kind == normalize.DeribitBookSnapshot) == previous.Valid {
		return BookMessage{}, ErrInvalidRPC
	}
	bids, err := parseBookLevels(object["bids"])
	if err != nil {
		return BookMessage{}, err
	}
	asks, err := parseBookLevels(object["asks"])
	if err != nil {
		return BookMessage{}, err
	}
	return BookMessage{
		Channel: channel, InstrumentName: instrument, Kind: kind, ChangeID: changeID,
		PreviousID: previous, TimestampMS: timestamp, GroupedView: len(parts) == 5,
		Bids: bids, Asks: asks,
	}, nil
}

func parseBookLevels(raw json.RawMessage) ([]NativeBookLevel, error) {
	if len(raw) == 0 {
		return nil, ErrInvalidRPC
	}
	var tuples [][]json.RawMessage
	if err := decodeRaw(raw, &tuples); err != nil || len(tuples) > MaxArrayElements {
		return nil, ErrInvalidRPC
	}
	levels := make([]NativeBookLevel, 0, len(tuples))
	for _, tuple := range tuples {
		if len(tuple) != 3 {
			return nil, ErrInvalidRPC
		}
		var action string
		if err := json.Unmarshal(tuple[0], &action); err != nil || (action != "new" && action != "change" && action != "delete") {
			return nil, ErrInvalidRPC
		}
		object := map[string]json.RawMessage{"price": tuple[1], "amount": tuple[2]}
		price, err := requiredNumber(object, "price")
		if err != nil {
			return nil, err
		}
		amount, err := requiredNumber(object, "amount")
		if err != nil {
			return nil, err
		}
		levels = append(levels, NativeBookLevel{Action: action, Price: price, Amount: amount})
	}
	return levels, nil
}

func (m BookMessage) Normalized(terms normalize.DeribitInstrumentTerms) (normalize.DeribitBookUpdate, error) {
	if m.InstrumentName != terms.InstrumentName {
		return normalize.DeribitBookUpdate{}, ErrInvalidRPC
	}
	timestampNS, err := millisecondsToNanoseconds(m.TimestampMS)
	if err != nil {
		return normalize.DeribitBookUpdate{}, err
	}
	inference, err := normalize.InferDeribitAmountUnit(terms, timestampNS)
	if err != nil {
		return normalize.DeribitBookUpdate{}, err
	}
	priceUnit, err := terms.PremiumPriceUnit()
	if err != nil {
		return normalize.DeribitBookUpdate{}, err
	}
	mapSide := func(source []NativeBookLevel) ([]normalize.DeribitBookLevel, error) {
		levels := make([]normalize.DeribitBookLevel, 0, len(source))
		for index, native := range source {
			price, err := mappedNumeric(native.Price, priceUnit)
			if err != nil || price.State != normalize.SourceValue {
				return nil, ErrInvalidRPC
			}
			amount, err := normalize.DeribitNativeAmount(native.Amount.Text, inference)
			if err != nil {
				return nil, err
			}
			action := normalize.DeribitBookAction(native.Action)
			if (action == normalize.DeribitBookDelete) != amount.Decimal.IsZero() {
				return nil, ErrInvalidRPC
			}
			levels = append(levels, normalize.DeribitBookLevel{
				Action: action, Price: price.Value, Amount: amount, SourceOrdinal: uint32(index),
			})
		}
		return levels, nil
	}
	bids, err := mapSide(m.Bids)
	if err != nil {
		return normalize.DeribitBookUpdate{}, err
	}
	asks, err := mapSide(m.Asks)
	if err != nil {
		return normalize.DeribitBookUpdate{}, err
	}
	update := normalize.DeribitBookUpdate{
		InstrumentUID: terms.InstrumentUID, Channel: m.Channel, Kind: m.Kind,
		ChangeID: m.ChangeID, PreviousID: m.PreviousID, SourceTimeNS: timestampNS,
		GroupedView: m.GroupedView, PriceUnit: priceUnit, UnitInference: inference, Bids: bids, Asks: asks,
	}
	if err := update.Validate(); err != nil {
		return normalize.DeribitBookUpdate{}, err
	}
	return update, nil
}

type Trade struct {
	Channel           string
	InstrumentName    string
	TradeID           string
	TradeSequence     uint64
	TimestampMS       int64
	Direction         string
	Cadence           Cadence
	Price             SourceNumber
	Amount            SourceNumber
	MarkPrice         SourceNumber
	IndexPrice        SourceNumber
	ImpliedVolatility SourceNumber
	LiquidationFlag   string
}

type TradeLiquidation struct {
	Flag             string
	NativeSourceRole string
	Completeness     normalize.LiquidationCompleteness
	PublicChannel    bool
}

type MappedTrade struct {
	Channel           string
	InstrumentUID     string
	TradeID           string
	TradeSequence     uint64
	SourceAggregation string
	SourceTimeNS      int64
	AggressorSide     normalize.Side
	Price             normalize.Numeric
	Amount            normalize.NativeValue
	UnitInference     normalize.DeribitUnitInference
	MarkPrice         MappedNumber
	IndexPrice        MappedNumber
	ImpliedVolatility MappedNumber
	Liquidation       *TradeLiquidation
}

func ParseTrades(payload []byte) ([]Trade, error) {
	channel, raw, err := notificationData(payload)
	if err != nil {
		return nil, ErrInvalidRPC
	}
	parts := strings.Split(channel, ".")
	if len(parts) != 3 || parts[0] != "trades" {
		return nil, ErrInvalidRPC
	}
	cadence := Cadence(parts[2])
	if err := cadence.Validate(); err != nil {
		return nil, ErrInvalidRPC
	}
	var entries []json.RawMessage
	if err := decodeRaw(raw, &entries); err != nil || len(entries) == 0 || len(entries) > MaxArrayElements {
		return nil, ErrInvalidRPC
	}
	trades := make([]Trade, 0, len(entries))
	for _, entry := range entries {
		object, err := decodeObject(entry)
		if err != nil {
			return nil, err
		}
		instrument, err := requiredString(object, "instrument_name")
		if err != nil || instrument != parts[1] {
			return nil, ErrInvalidRPC
		}
		tradeID, err := requiredString(object, "trade_id")
		if err != nil {
			return nil, err
		}
		sequence, err := requiredUint64(object, "trade_seq")
		if err != nil {
			return nil, err
		}
		timestamp, err := requiredInt64(object, "timestamp")
		if err != nil {
			return nil, err
		}
		direction, err := requiredString(object, "direction")
		if err != nil || (direction != "buy" && direction != "sell") {
			return nil, ErrInvalidRPC
		}
		price, err := requiredNumber(object, "price")
		if err != nil {
			return nil, err
		}
		amount, err := requiredNumber(object, "amount")
		if err != nil {
			return nil, err
		}
		mark, err := sourceNumber(object, "mark_price")
		if err != nil {
			return nil, err
		}
		index, err := sourceNumber(object, "index_price")
		if err != nil {
			return nil, err
		}
		iv, err := sourceNumber(object, "iv")
		if err != nil {
			return nil, err
		}
		liquidationState, liquidation, err := sourceString(object, "liquidation")
		if err != nil {
			return nil, err
		}
		if liquidationState == normalize.SourceValue && liquidation != "M" && liquidation != "T" && liquidation != "MT" {
			return nil, ErrInvalidRPC
		}
		trades = append(trades, Trade{
			Channel: channel, InstrumentName: instrument, TradeID: tradeID, TradeSequence: sequence,
			TimestampMS: timestamp, Direction: direction, Cadence: cadence, Price: price, Amount: amount,
			MarkPrice: mark, IndexPrice: index, ImpliedVolatility: iv, LiquidationFlag: liquidation,
		})
	}
	return trades, nil
}

func (t Trade) Normalized(terms normalize.DeribitInstrumentTerms) (MappedTrade, error) {
	if t.InstrumentName != terms.InstrumentName || t.Cadence.Validate() != nil ||
		t.Channel != "trades."+t.InstrumentName+"."+string(t.Cadence) {
		return MappedTrade{}, ErrInvalidRPC
	}
	timestampNS, err := millisecondsToNanoseconds(t.TimestampMS)
	if err != nil {
		return MappedTrade{}, err
	}
	inference, err := normalize.InferDeribitAmountUnit(terms, timestampNS)
	if err != nil {
		return MappedTrade{}, err
	}
	priceUnit, err := terms.PremiumPriceUnit()
	if err != nil {
		return MappedTrade{}, err
	}
	referenceUnit, err := terms.ReferencePriceUnit()
	if err != nil {
		return MappedTrade{}, err
	}
	price, err := mappedNumeric(t.Price, priceUnit)
	if err != nil || price.State != normalize.SourceValue {
		return MappedTrade{}, ErrInvalidRPC
	}
	amount, err := normalize.DeribitNativeAmount(t.Amount.Text, inference)
	if err != nil {
		return MappedTrade{}, err
	}
	mark, err := mappedNumeric(t.MarkPrice, priceUnit)
	if err != nil {
		return MappedTrade{}, err
	}
	index, err := mappedNumeric(t.IndexPrice, referenceUnit)
	if err != nil {
		return MappedTrade{}, err
	}
	iv, err := mappedRate(t.ImpliedVolatility, normalize.ImpliedVolatilityUnit())
	if err != nil {
		return MappedTrade{}, err
	}
	side := normalize.SideBuy
	if t.Direction == "sell" {
		side = normalize.SideSell
	}
	mapped := MappedTrade{
		Channel: t.Channel, InstrumentUID: terms.InstrumentUID, TradeID: t.TradeID, TradeSequence: t.TradeSequence,
		SourceAggregation: sourceAggregationContract(t.Cadence), SourceTimeNS: timestampNS, AggressorSide: side,
		Price: price.Value, Amount: amount, UnitInference: inference,
		MarkPrice: mark, IndexPrice: index, ImpliedVolatility: iv,
	}
	if t.LiquidationFlag != "" {
		mapped.Liquidation = &TradeLiquidation{
			Flag: t.LiquidationFlag, NativeSourceRole: "deribit_public_trade_liquidation_flag",
			Completeness: normalize.LiquidationTradeFlagOnly, PublicChannel: false,
		}
	}
	return mapped, nil
}

func sourceAggregationContract(cadence Cadence) string {
	switch cadence {
	case CadenceRaw:
		return "authorized_1ms_aggregation_not_per_match"
	case Cadence100MS:
		return "100ms"
	case CadenceAgg2:
		return "agg2"
	default:
		return ""
	}
}
