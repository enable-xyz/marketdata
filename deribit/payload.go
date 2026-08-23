package deribit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type SourceNumber struct {
	State normalize.SourceState
	Text  string
}

func (n SourceNumber) Decimal(scale uint8) (normalize.Decimal, error) {
	if n.State != normalize.SourceValue {
		return normalize.Decimal{}, fmt.Errorf("%w: numeric source field is %s", ErrInvalidRPC, n.State)
	}
	value, err := normalize.ParseDecimal(n.Text, scale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.Decimal{}, fmt.Errorf("%w: invalid decimal field", ErrInvalidRPC)
	}
	return value, nil
}

func notificationData(payload []byte) (string, json.RawMessage, error) {
	var notification struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		} `json:"params"`
	}
	if err := decodeRPC(payload, &notification); err != nil || notification.JSONRPC != JSONRPCVersion ||
		notification.Method != "subscription" || notification.Params.Channel == "" || len(notification.Params.Data) == 0 {
		return "", nil, ErrInvalidRPC
	}
	return notification.Params.Channel, notification.Params.Data, nil
}

func decodeObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := decodeRaw(raw, &object); err != nil || object == nil {
		return nil, ErrInvalidRPC
	}
	return object, nil
}

func sourceNumber(object map[string]json.RawMessage, name string) (SourceNumber, error) {
	raw, exists := object[name]
	if !exists {
		return SourceNumber{State: normalize.SourceMissing}, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return SourceNumber{State: normalize.SourceNull}, nil
	}
	if len(trimmed) == 0 || len(trimmed) > normalize.MaxDecimalInputBytes {
		return SourceNumber{}, ErrInvalidRPC
	}
	text := string(trimmed)
	if !validSourceDecimal(text) {
		return SourceNumber{}, ErrInvalidRPC
	}
	return SourceNumber{State: normalize.SourceValue, Text: text}, nil
}

func validSourceDecimal(text string) bool {
	start := 0
	if text[0] == '-' {
		start = 1
		if start == len(text) {
			return false
		}
	}
	digits := 0
	dot := -1
	for index := start; index < len(text); index++ {
		switch character := text[index]; {
		case character >= '0' && character <= '9':
			digits++
		case character == '.' && dot < 0:
			dot = index
		default:
			return false
		}
	}
	return digits > 0 && digits <= normalize.MaxCanonicalCoefficientDigits &&
		dot != start && dot != len(text)-1
}

func requiredNumber(object map[string]json.RawMessage, name string) (SourceNumber, error) {
	value, err := sourceNumber(object, name)
	if err != nil || value.State != normalize.SourceValue {
		return SourceNumber{}, ErrInvalidRPC
	}
	return value, nil
}

func sourceString(object map[string]json.RawMessage, name string) (normalize.SourceState, string, error) {
	raw, exists := object[name]
	if !exists {
		return normalize.SourceMissing, "", nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return normalize.SourceNull, "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", "", ErrInvalidRPC
	}
	if value == "" {
		return normalize.SourceEmpty, "", nil
	}
	if len(value) > 512 || strings.IndexByte(value, 0) >= 0 {
		return "", "", ErrInvalidRPC
	}
	return normalize.SourceValue, value, nil
}

func requiredString(object map[string]json.RawMessage, name string) (string, error) {
	state, value, err := sourceString(object, name)
	if err != nil || state != normalize.SourceValue {
		return "", ErrInvalidRPC
	}
	return value, nil
}

func sourceBool(object map[string]json.RawMessage, name string) (normalize.SourceState, bool, error) {
	raw, exists := object[name]
	if !exists {
		return normalize.SourceMissing, false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return normalize.SourceNull, false, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false, ErrInvalidRPC
	}
	return normalize.SourceValue, value, nil
}

func requiredUint64(object map[string]json.RawMessage, name string) (uint64, error) {
	number, err := requiredNumber(object, name)
	if err != nil || strings.Contains(number.Text, ".") || strings.HasPrefix(number.Text, "-") {
		return 0, ErrInvalidRPC
	}
	value, err := strconv.ParseUint(number.Text, 10, 64)
	if err != nil {
		return 0, ErrInvalidRPC
	}
	return value, nil
}

func requiredInt64(object map[string]json.RawMessage, name string) (int64, error) {
	number, err := requiredNumber(object, name)
	if err != nil || strings.Contains(number.Text, ".") {
		return 0, ErrInvalidRPC
	}
	value, err := strconv.ParseInt(number.Text, 10, 64)
	if err != nil || value < 0 {
		return 0, ErrInvalidRPC
	}
	return value, nil
}

func millisecondsToNanoseconds(milliseconds int64) (int64, error) {
	const multiplier = int64(1_000_000)
	if milliseconds < 0 || milliseconds > (1<<63-1)/multiplier {
		return 0, ErrInvalidRPC
	}
	return milliseconds * multiplier, nil
}

func mappedPrice(number SourceNumber, baseAssetID, quoteAssetID string) (MappedNumber, error) {
	return mappedNumeric(number, normalize.SpotPriceUnit(baseAssetID, quoteAssetID))
}

func mappedNumeric(number SourceNumber, unit normalize.Unit) (MappedNumber, error) {
	mapped := MappedNumber{State: number.State}
	if number.State != normalize.SourceValue {
		return mapped, nil
	}
	decimal, err := number.Decimal(normalize.CanonicalPriceScale)
	if err != nil || strings.HasPrefix(decimal.Coefficient, "-") {
		return MappedNumber{}, ErrInvalidRPC
	}
	mapped.Value = normalize.Numeric{Decimal: decimal, Unit: unit}
	if err := mapped.Value.Validate(); err != nil {
		return MappedNumber{}, ErrInvalidRPC
	}
	return mapped, nil
}

func mappedRate(number SourceNumber, unit normalize.Unit) (MappedNumber, error) {
	mapped := MappedNumber{State: number.State}
	if number.State != normalize.SourceValue {
		return mapped, nil
	}
	decimal, err := number.Decimal(normalize.CanonicalPriceScale)
	if err != nil {
		return MappedNumber{}, ErrInvalidRPC
	}
	mapped.Value = normalize.Numeric{Decimal: decimal, Unit: unit}
	if err := mapped.Value.Validate(); err != nil {
		return MappedNumber{}, ErrInvalidRPC
	}
	return mapped, nil
}

func mappedNative(number SourceNumber, inference normalize.DeribitUnitInference) (MappedNativeNumber, error) {
	mapped := MappedNativeNumber{State: number.State}
	if number.State != normalize.SourceValue {
		return mapped, nil
	}
	value, err := normalize.DeribitNativeAmount(number.Text, inference)
	if err != nil {
		return MappedNativeNumber{}, err
	}
	mapped.Value = value
	return mapped, nil
}

func mappedVenueNative(number SourceNumber, label string) (MappedNativeNumber, error) {
	mapped := MappedNativeNumber{State: number.State}
	if number.State != normalize.SourceValue {
		return mapped, nil
	}
	decimal, err := number.Decimal(normalize.CanonicalAmountScale)
	if err != nil {
		return MappedNativeNumber{}, err
	}
	mapped.Value = normalize.NativeValue{
		Decimal: decimal,
		Unit:    normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: label},
	}
	if err := mapped.Value.Validate(); err != nil {
		return MappedNativeNumber{}, ErrInvalidRPC
	}
	return mapped, nil
}
