package okx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type NativeField struct {
	State normalize.SourceState
	Text  string
}

func field(object map[string]json.RawMessage, name string) (NativeField, error) {
	raw, exists := object[name]
	if !exists {
		return NativeField{State: normalize.SourceMissing}, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return NativeField{State: normalize.SourceNull}, nil
	}
	var text string
	if err := json.Unmarshal(trimmed, &text); err == nil {
		if text == "" {
			return NativeField{State: normalize.SourceEmpty}, nil
		}
		if len(text) > normalize.MaxDecimalInputBytes*2 || strings.IndexByte(text, 0) >= 0 {
			return NativeField{}, ErrInvalidPayload
		}
		return NativeField{State: normalize.SourceValue, Text: text}, nil
	}
	if len(trimmed) == 0 || len(trimmed) > normalize.MaxDecimalInputBytes*2 || bytes.IndexByte(trimmed, 0) >= 0 || trimmed[0] == '{' || trimmed[0] == '[' || trimmed[0] == '"' {
		return NativeField{}, ErrInvalidPayload
	}
	return NativeField{State: normalize.SourceValue, Text: string(trimmed)}, nil
}

func requiredField(object map[string]json.RawMessage, name string) (NativeField, error) {
	value, err := field(object, name)
	if err != nil || value.State != normalize.SourceValue {
		return NativeField{}, fmt.Errorf("%w: required field %s", ErrInvalidPayload, name)
	}
	return value, nil
}

type Trade struct {
	InstrumentID string
	TradeID      string
	Price        string
	Size         string
	Side         string
	TimestampMS  int64
	Count        NativeField
	Source       string
	Raw          json.RawMessage
}

func ParseTradesAll(payload []byte) ([]Trade, error) {
	if !validPayload(payload) {
		return nil, ErrInvalidPayload
	}
	var message struct {
		Argument struct {
			Channel      string `json:"channel"`
			InstrumentID string `json:"instId"`
		} `json:"arg"`
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &message); err != nil || message.Argument.Channel != "trades-all" || !validIdentifier(message.Argument.InstrumentID, 128) || len(message.Data) == 0 || len(message.Data) > 4096 {
		return nil, ErrInvalidPayload
	}
	trades := make([]Trade, len(message.Data))
	for index, object := range message.Data {
		instrument, err := requiredField(object, "instId")
		if err != nil || instrument.Text != message.Argument.InstrumentID {
			return nil, fmt.Errorf("%w: trade instrument at ordinal %d", ErrInvalidPayload, index)
		}
		tradeID, err := requiredField(object, "tradeId")
		if err != nil {
			return nil, err
		}
		if _, err := strconv.ParseUint(tradeID.Text, 10, 64); err != nil {
			return nil, fmt.Errorf("%w: trade ID at ordinal %d", ErrInvalidPayload, index)
		}
		price, err := requiredField(object, "px")
		if err != nil || !validDecimalText(price.Text, false) {
			return nil, fmt.Errorf("%w: trade price at ordinal %d", ErrInvalidPayload, index)
		}
		size, err := requiredField(object, "sz")
		if err != nil || !validDecimalText(size.Text, false) {
			return nil, fmt.Errorf("%w: trade size at ordinal %d", ErrInvalidPayload, index)
		}
		side, err := requiredField(object, "side")
		if err != nil || (side.Text != "buy" && side.Text != "sell") {
			return nil, fmt.Errorf("%w: trade side at ordinal %d", ErrInvalidPayload, index)
		}
		timestamp, err := requiredField(object, "ts")
		if err != nil {
			return nil, err
		}
		timestampMS, err := strconv.ParseInt(timestamp.Text, 10, 64)
		if err != nil || timestampMS < 0 || timestampMS > (1<<63-1)/1_000_000 {
			return nil, fmt.Errorf("%w: trade timestamp at ordinal %d", ErrInvalidPayload, index)
		}
		count, err := field(object, "count")
		if err != nil || count.State != normalize.SourceValue || count.Text != "1" {
			return nil, fmt.Errorf("%w: trades-all record is not one maker match at ordinal %d", ErrAmbiguousProjection, index)
		}
		trades[index] = Trade{InstrumentID: instrument.Text, TradeID: tradeID.Text, Price: price.Text, Size: size.Text, Side: side.Text, TimestampMS: timestampMS, Count: count, Source: "trades-all", Raw: slices.Clone(payload)}
	}
	return trades, nil
}

type BookLevel struct {
	Price            string
	Size             string
	DeprecatedOrders string
	OrderCount       string
}

type BookMessage struct {
	Channel      string
	InstrumentID string
	Action       string
	TimestampMS  int64
	PreviousSeq  int64
	Sequence     int64
	Checksum     int32
	Bids         []BookLevel
	Asks         []BookLevel
	Raw          json.RawMessage
}

func ParseBook(payload []byte) (BookMessage, error) {
	if !validPayload(payload) {
		return BookMessage{}, ErrInvalidPayload
	}
	var message struct {
		Argument struct {
			Channel      string `json:"channel"`
			InstrumentID string `json:"instId"`
		} `json:"arg"`
		Action string                       `json:"action"`
		Data   []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &message); err != nil || !validBookChannel(message.Argument.Channel) || !validIdentifier(message.Argument.InstrumentID, 128) ||
		(message.Action != "snapshot" && message.Action != "update") || len(message.Data) != 1 {
		return BookMessage{}, ErrInvalidPayload
	}
	object := message.Data[0]
	timestamp, err := requiredInt64(object, "ts")
	if err != nil || timestamp < 0 || timestamp > (1<<63-1)/1_000_000 {
		return BookMessage{}, ErrInvalidPayload
	}
	previous, err := requiredInt64(object, "prevSeqId")
	if err != nil || previous < -1 {
		return BookMessage{}, ErrInvalidPayload
	}
	sequence, err := requiredInt64(object, "seqId")
	if err != nil || sequence < 0 {
		return BookMessage{}, ErrInvalidPayload
	}
	checksumValue, err := requiredInt64(object, "checksum")
	if err != nil || checksumValue < -1<<31 || checksumValue > 1<<31-1 {
		return BookMessage{}, ErrInvalidPayload
	}
	bids, err := decodeBookSide(object["bids"])
	if err != nil {
		return BookMessage{}, err
	}
	asks, err := decodeBookSide(object["asks"])
	if err != nil {
		return BookMessage{}, err
	}
	if message.Action == "snapshot" && previous != -1 {
		return BookMessage{}, fmt.Errorf("%w: snapshot prevSeqId must be -1", ErrInvalidPayload)
	}
	return BookMessage{Channel: message.Argument.Channel, InstrumentID: message.Argument.InstrumentID, Action: message.Action, TimestampMS: timestamp, PreviousSeq: previous, Sequence: sequence, Checksum: int32(checksumValue), Bids: bids, Asks: asks, Raw: slices.Clone(payload)}, nil
}

func validBookChannel(channel string) bool {
	switch channel {
	case "books", "books5", "bbo-tbt", "books50-l2-tbt", "books-l2-tbt", "books-rpi-tbt":
		return true
	default:
		return false
	}
}

func decodeBookSide(raw json.RawMessage) ([]BookLevel, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: missing book side", ErrInvalidPayload)
	}
	var native [][]string
	if err := json.Unmarshal(raw, &native); err != nil || len(native) > 400 {
		return nil, ErrInvalidPayload
	}
	levels := make([]BookLevel, len(native))
	for index, level := range native {
		if len(level) != 4 || !validDecimalText(level[0], false) || !validDecimalText(level[1], true) || !validDecimalText(level[2], true) || !validDecimalText(level[3], true) {
			return nil, fmt.Errorf("%w: book level at ordinal %d", ErrInvalidPayload, index)
		}
		levels[index] = BookLevel{Price: level[0], Size: level[1], DeprecatedOrders: level[2], OrderCount: level[3]}
	}
	return levels, nil
}

func requiredInt64(object map[string]json.RawMessage, name string) (int64, error) {
	value, err := requiredField(object, name)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value.Text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: signed integer field %s", ErrInvalidPayload, name)
	}
	return parsed, nil
}

type MarketObservation struct {
	Channel          string
	InstrumentID     NativeField
	InstrumentType   NativeField
	InstrumentFamily NativeField
	TimestampMS      NativeField
	Fields           map[string]NativeField
	Raw              json.RawMessage
}

func ParseMarketObservations(payload []byte) ([]MarketObservation, error) {
	if !validPayload(payload) {
		return nil, ErrInvalidPayload
	}
	var message struct {
		Argument map[string]json.RawMessage   `json:"arg"`
		Data     []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &message); err != nil || len(message.Data) == 0 || len(message.Data) > 4096 {
		return nil, ErrInvalidPayload
	}
	channel, err := requiredField(message.Argument, "channel")
	if err != nil || !supportedObservationChannel(channel.Text) {
		return nil, ErrInvalidPayload
	}
	observations := make([]MarketObservation, len(message.Data))
	for index, object := range message.Data {
		fields := make(map[string]NativeField, len(object))
		for name := range object {
			value, err := field(object, name)
			if err != nil {
				return nil, fmt.Errorf("%w: field %s at ordinal %d", ErrInvalidPayload, name, index)
			}
			fields[name] = value
		}
		instrument, err := field(object, "instId")
		if err != nil {
			return nil, err
		}
		instrumentType, err := field(object, "instType")
		if err != nil {
			return nil, err
		}
		instrumentFamily, err := field(object, "instFamily")
		if err != nil {
			return nil, err
		}
		timestamp, err := field(object, "ts")
		if err != nil {
			return nil, err
		}
		observations[index] = MarketObservation{Channel: channel.Text, InstrumentID: instrument, InstrumentType: instrumentType, InstrumentFamily: instrumentFamily, TimestampMS: timestamp, Fields: fields, Raw: slices.Clone(payload)}
	}
	return observations, nil
}

func supportedObservationChannel(channel string) bool {
	switch channel {
	case "tickers", "open-interest", "funding-rate", "mark-price", "index-tickers", "opt-summary":
		return true
	default:
		return false
	}
}

type LiquidationDetail struct {
	Side            NativeField
	PositionSide    NativeField
	BankruptcyPrice NativeField
	Size            NativeField
	BankruptcyLoss  NativeField
	Currency        NativeField
	TimestampMS     NativeField
	Raw             json.RawMessage
}

type LiquidationBatch struct {
	InstrumentType   NativeField
	InstrumentFamily NativeField
	InstrumentID     NativeField
	Underlying       NativeField
	Details          []LiquidationDetail
	Raw              json.RawMessage
}

func ParseLiquidations(payload []byte) ([]LiquidationBatch, error) {
	if !validPayload(payload) {
		return nil, ErrInvalidPayload
	}
	var message struct {
		Argument struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &message); err != nil || message.Argument.Channel != "liquidation-orders" || len(message.Data) == 0 || len(message.Data) > 4096 {
		return nil, ErrInvalidPayload
	}
	batches := make([]LiquidationBatch, len(message.Data))
	for batchOrdinal, object := range message.Data {
		var detailsNative []map[string]json.RawMessage
		if err := json.Unmarshal(object["details"], &detailsNative); err != nil || len(detailsNative) == 0 || len(detailsNative) > 4096 {
			return nil, fmt.Errorf("%w: liquidation details at batch %d", ErrInvalidPayload, batchOrdinal)
		}
		details := make([]LiquidationDetail, len(detailsNative))
		for detailOrdinal, native := range detailsNative {
			values := make(map[string]NativeField, 7)
			for _, name := range []string{"side", "posSide", "bkPx", "sz", "bkLoss", "ccy", "ts"} {
				value, err := field(native, name)
				if err != nil {
					return nil, fmt.Errorf("%w: liquidation field %s at %d/%d", ErrInvalidPayload, name, batchOrdinal, detailOrdinal)
				}
				values[name] = value
			}
			details[detailOrdinal] = LiquidationDetail{Side: values["side"], PositionSide: values["posSide"], BankruptcyPrice: values["bkPx"], Size: values["sz"], BankruptcyLoss: values["bkLoss"], Currency: values["ccy"], TimestampMS: values["ts"], Raw: slices.Clone(payload)}
		}
		instType, err := field(object, "instType")
		if err != nil {
			return nil, err
		}
		instFamily, err := field(object, "instFamily")
		if err != nil {
			return nil, err
		}
		instID, err := field(object, "instId")
		if err != nil {
			return nil, err
		}
		underlying, err := field(object, "uly")
		if err != nil {
			return nil, err
		}
		batches[batchOrdinal] = LiquidationBatch{InstrumentType: instType, InstrumentFamily: instFamily, InstrumentID: instID, Underlying: underlying, Details: details, Raw: slices.Clone(payload)}
	}
	return batches, nil
}

type InstrumentObservation struct {
	Fields map[string]NativeField
	Raw    json.RawMessage
}

func ParseInstruments(payload []byte) ([]InstrumentObservation, error) {
	if !validPayload(payload) {
		return nil, ErrInvalidPayload
	}
	var response struct {
		Code string                       `json:"code"`
		Msg  string                       `json:"msg"`
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &response); err != nil || response.Code != "0" || response.Msg != "" || len(response.Data) == 0 || len(response.Data) > 10000 {
		return nil, ErrInvalidPayload
	}
	observations := make([]InstrumentObservation, len(response.Data))
	for index, object := range response.Data {
		fields := make(map[string]NativeField, len(object))
		for name := range object {
			value, err := field(object, name)
			if err != nil {
				return nil, fmt.Errorf("%w: instrument field %s at ordinal %d", ErrInvalidPayload, name, index)
			}
			fields[name] = value
		}
		if fields["instId"].State != normalize.SourceValue || fields["instType"].State != normalize.SourceValue || fields["state"].State != normalize.SourceValue {
			return nil, fmt.Errorf("%w: incomplete instrument identity at ordinal %d", ErrInvalidPayload, index)
		}
		observations[index] = InstrumentObservation{Fields: fields, Raw: slices.Clone(payload)}
	}
	return observations, nil
}

func validPayload(payload []byte) bool {
	return len(payload) > 0 && len(payload) <= MaxRawPayloadBytes && json.Valid(payload)
}

func validDecimalText(text string, zeroAllowed bool) bool {
	if text == "" || len(text) > normalize.MaxDecimalInputBytes || text[0] == '+' {
		return false
	}
	if text[0] == '-' {
		return false
	}
	start := 0
	dot := -1
	digits := 0
	for index := start; index < len(text); index++ {
		switch {
		case text[index] >= '0' && text[index] <= '9':
			digits++
		case text[index] == '.' && dot < 0:
			dot = index
		default:
			return false
		}
	}
	if digits == 0 || dot == start || dot == len(text)-1 {
		return false
	}
	if !zeroAllowed {
		trimmed := strings.Trim(text, "0.")
		return trimmed != ""
	}
	return true
}
