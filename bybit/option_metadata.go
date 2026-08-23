package bybit

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/normalize"
)

type OptionInstrumentRequest struct {
	Path  string
	Query string
}

func NewOptionInstrumentRequest(baseCoin, symbol, cursor string, limit int) (OptionInstrumentRequest, error) {
	if baseCoin != "" && !validBaseCoin(baseCoin) {
		return OptionInstrumentRequest{}, ErrInvalidTopic
	}
	if symbol != "" {
		identity, ok := parseOptionSymbol(symbol)
		if !ok || (baseCoin != "" && identity.base != baseCoin) {
			return OptionInstrumentRequest{}, ErrInvalidTopic
		}
	}
	if limit < 0 || limit > 1000 {
		return OptionInstrumentRequest{}, ErrInvalidTopic
	}
	query := make(url.Values)
	query.Set("category", "option")
	if baseCoin != "" {
		query.Set("baseCoin", baseCoin)
	}
	if symbol != "" {
		query.Set("symbol", symbol)
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if limit != 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	return OptionInstrumentRequest{Path: InstrumentInfoPath, Query: query.Encode()}, nil
}

type OptionInstrument struct {
	Symbol             string
	Status             string
	BaseCoin           string
	QuoteCoin          string
	SettleCoin         string
	OptionsType        string
	LaunchTimeMS       int64
	DeliveryTimeMS     int64
	DeliveryFeeRate    string
	StrikePrice        string
	TickSize           string
	MinimumPrice       string
	MaximumPrice       string
	MinimumOrderQty    string
	MaximumOrderQty    string
	QuantityStep       string
	UnifiedMarginTrade bool
	Raw                json.RawMessage
}

func (i OptionInstrument) Kind() normalize.OptionKind {
	switch i.OptionsType {
	case "Call":
		return normalize.OptionCall
	case "Put":
		return normalize.OptionPut
	default:
		return ""
	}
}

func (i OptionInstrument) ExpiryTimeNS() (int64, error) {
	if i.DeliveryTimeMS < 0 || i.DeliveryTimeMS > (1<<63-1)/int64(1e6) {
		return 0, ErrInvalidPayload
	}
	return i.DeliveryTimeMS * int64(1e6), nil
}

type OptionInstrumentPage struct {
	ObservedTimeMS int64
	NextCursor     string
	Instruments    []OptionInstrument
}

func ParseOptionInstrumentInfo(payload []byte) (OptionInstrumentPage, error) {
	if err := validateOptionPayload(payload, optionRESTPayloadPolicy()); err != nil {
		return OptionInstrumentPage{}, err
	}
	var response struct {
		ReturnCode *int64 `json:"retCode"`
		ReturnMsg  string `json:"retMsg"`
		Time       *int64 `json:"time"`
		Result     struct {
			Category       string            `json:"category"`
			NextPageCursor string            `json:"nextPageCursor"`
			List           []json.RawMessage `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(payload, &response); err != nil || response.ReturnCode == nil || *response.ReturnCode != 0 ||
		response.Time == nil || *response.Time < 0 || response.Result.Category != "option" || len(response.Result.List) > 1000 {
		return OptionInstrumentPage{}, ErrInvalidPayload
	}
	instruments := make([]OptionInstrument, len(response.Result.List))
	seen := make(map[string]struct{}, len(instruments))
	for ordinal, raw := range response.Result.List {
		var native struct {
			Symbol             string `json:"symbol"`
			Status             string `json:"status"`
			BaseCoin           string `json:"baseCoin"`
			QuoteCoin          string `json:"quoteCoin"`
			SettleCoin         string `json:"settleCoin"`
			OptionsType        string `json:"optionsType"`
			LaunchTime         string `json:"launchTime"`
			DeliveryTime       string `json:"deliveryTime"`
			DeliveryFeeRate    string `json:"deliveryFeeRate"`
			UnifiedMarginTrade *bool  `json:"unifiedMarginTrade"`
			PriceFilter        struct {
				Minimum string `json:"minPrice"`
				Maximum string `json:"maxPrice"`
				Tick    string `json:"tickSize"`
			} `json:"priceFilter"`
			LotSizeFilter struct {
				MinimumQty   string `json:"minOrderQty"`
				MaximumQty   string `json:"maxOrderQty"`
				QuantityStep string `json:"qtyStep"`
			} `json:"lotSizeFilter"`
		}
		if err := json.Unmarshal(raw, &native); err != nil {
			return OptionInstrumentPage{}, fmt.Errorf("%w: option instrument type drift at ordinal %d", ErrInvalidPayload, ordinal)
		}
		identity, ok := parseOptionSymbol(native.Symbol)
		if !ok || native.Status == "" || native.BaseCoin != identity.base || !validBaseCoin(native.QuoteCoin) || !validBaseCoin(native.SettleCoin) ||
			(identity.quote != "" && native.QuoteCoin != identity.quote) ||
			(native.OptionsType != "Call" && native.OptionsType != "Put") || (identity.kind == normalize.OptionCall) != (native.OptionsType == "Call") ||
			native.UnifiedMarginTrade == nil || !validDecimalText(native.DeliveryFeeRate) || !validDecimalText(native.PriceFilter.Tick) ||
			!validDecimalText(native.PriceFilter.Minimum) || !validDecimalText(native.PriceFilter.Maximum) ||
			!validDecimalText(native.LotSizeFilter.MinimumQty) || !validDecimalText(native.LotSizeFilter.MaximumQty) ||
			!validDecimalText(native.LotSizeFilter.QuantityStep) {
			return OptionInstrumentPage{}, fmt.Errorf("%w: option instrument at ordinal %d", ErrInvalidPayload, ordinal)
		}
		launchTime, err := parseNonNegativeMilliseconds(native.LaunchTime)
		if err != nil {
			return OptionInstrumentPage{}, fmt.Errorf("%w: option launch time at ordinal %d", ErrInvalidPayload, ordinal)
		}
		deliveryTime, err := parseNonNegativeMilliseconds(native.DeliveryTime)
		if err != nil || !sameUTCDate(deliveryTime, identity.expiry) {
			return OptionInstrumentPage{}, fmt.Errorf("%w: option delivery time at ordinal %d", ErrInvalidPayload, ordinal)
		}
		if _, exists := seen[native.Symbol]; exists {
			return OptionInstrumentPage{}, fmt.Errorf("%w: duplicate option instrument %s", ErrInvalidPayload, native.Symbol)
		}
		seen[native.Symbol] = struct{}{}
		instruments[ordinal] = OptionInstrument{
			Symbol: native.Symbol, Status: native.Status, BaseCoin: native.BaseCoin, QuoteCoin: native.QuoteCoin, SettleCoin: native.SettleCoin,
			OptionsType: native.OptionsType, LaunchTimeMS: launchTime, DeliveryTimeMS: deliveryTime, DeliveryFeeRate: native.DeliveryFeeRate,
			StrikePrice: identity.strike, TickSize: native.PriceFilter.Tick, MinimumPrice: native.PriceFilter.Minimum, MaximumPrice: native.PriceFilter.Maximum,
			MinimumOrderQty: native.LotSizeFilter.MinimumQty, MaximumOrderQty: native.LotSizeFilter.MaximumQty, QuantityStep: native.LotSizeFilter.QuantityStep,
			UnifiedMarginTrade: *native.UnifiedMarginTrade, Raw: slices.Clone(raw),
		}
	}
	return OptionInstrumentPage{ObservedTimeMS: *response.Time, NextCursor: response.Result.NextPageCursor, Instruments: instruments}, nil
}

type optionSymbol struct {
	base   string
	expiry time.Time
	strike string
	kind   normalize.OptionKind
	quote  string
}

func validOptionSymbol(symbol string) bool {
	_, ok := parseOptionSymbol(symbol)
	return ok
}

// parseOptionSymbol accepts Bybit's documented four-field native identity and
// the quote-qualified five-field identity returned by the current V5 API.
func parseOptionSymbol(symbol string) (optionSymbol, bool) {
	if symbol == "" || len(symbol) > 64 || strings.IndexByte(symbol, 0) >= 0 {
		return optionSymbol{}, false
	}
	parts := strings.Split(symbol, "-")
	if (len(parts) != 4 && len(parts) != 5) || !validBaseCoin(parts[0]) || !validDecimalText(parts[2]) {
		return optionSymbol{}, false
	}
	expiry, err := time.Parse("02Jan06", parts[1])
	if err != nil || strings.ToUpper(expiry.Format("02Jan06")) != parts[1] {
		return optionSymbol{}, false
	}
	var kind normalize.OptionKind
	switch parts[3] {
	case "C":
		kind = normalize.OptionCall
	case "P":
		kind = normalize.OptionPut
	default:
		return optionSymbol{}, false
	}
	quote := ""
	if len(parts) == 5 {
		quote = parts[4]
		if !validBaseCoin(quote) {
			return optionSymbol{}, false
		}
	}
	return optionSymbol{base: parts[0], expiry: expiry.UTC(), strike: parts[2], kind: kind, quote: quote}, true
}

func parseNonNegativeMilliseconds(text string) (int64, error) {
	milliseconds, err := strconv.ParseInt(text, 10, 64)
	if err != nil || milliseconds < 0 {
		return 0, ErrInvalidPayload
	}
	return milliseconds, nil
}

func sameUTCDate(milliseconds int64, date time.Time) bool {
	if milliseconds < 0 || milliseconds > (1<<63-1)/int64(1e6) {
		return false
	}
	observed := time.UnixMilli(milliseconds).UTC()
	return observed.Year() == date.Year() && observed.Month() == date.Month() && observed.Day() == date.Day()
}
