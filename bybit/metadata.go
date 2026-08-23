package bybit

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
)

type InstrumentRequest struct {
	Path  string
	Query string
}

func NewInstrumentRequest(category Category, symbol, cursor string, limit int) (InstrumentRequest, error) {
	if err := category.Validate(); err != nil {
		return InstrumentRequest{}, err
	}
	if symbol != "" && !validSymbol(symbol) {
		return InstrumentRequest{}, ErrInvalidTopic
	}
	if category == Spot && (cursor != "" || limit != 0) {
		return InstrumentRequest{}, fmt.Errorf("%w: Spot instruments do not support cursor or limit", ErrInvalidTopic)
	}
	if category != Spot && (limit < 0 || limit > 1000) {
		return InstrumentRequest{}, ErrInvalidTopic
	}
	query := make(url.Values)
	query.Set("category", string(category))
	if symbol != "" {
		query.Set("symbol", symbol)
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if limit != 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	return InstrumentRequest{Path: InstrumentInfoPath, Query: query.Encode()}, nil
}

type Instrument struct {
	Category           Category
	Symbol             string
	SymbolID           int64
	ContractType       string
	Status             string
	BaseCoin           string
	QuoteCoin          string
	SettleCoin         string
	SymbolType         string
	LaunchTimeMS       string
	DeliveryTimeMS     string
	DeliveryFeeRate    string
	PriceScale         string
	TickSize           string
	MinimumPrice       string
	MaximumPrice       string
	MinimumOrderQty    string
	MaximumOrderQty    string
	MaximumMarketQty   string
	QuantityStep       string
	MinimumNotional    string
	FundingIntervalMin int64
	UpperFundingRate   string
	LowerFundingRate   string
	UnifiedMarginTrade bool
	PreListing         bool
	PreListingInfo     json.RawMessage
	RiskParameters     json.RawMessage
	Raw                json.RawMessage
}

type InstrumentPage struct {
	Category       Category
	ObservedTimeMS int64
	NextCursor     string
	Instruments    []Instrument
}

func ParseInstrumentInfo(expected Category, payload []byte) (InstrumentPage, error) {
	if err := expected.Validate(); err != nil {
		return InstrumentPage{}, err
	}
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return InstrumentPage{}, ErrInvalidPayload
	}
	var response struct {
		ReturnCode int64  `json:"retCode"`
		ReturnMsg  string `json:"retMsg"`
		Time       int64  `json:"time"`
		Result     struct {
			Category       Category          `json:"category"`
			NextPageCursor string            `json:"nextPageCursor"`
			List           []json.RawMessage `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(payload, &response); err != nil || response.ReturnCode != 0 || response.Result.Category != expected || response.Time < 0 || len(response.Result.List) > 1000 || (expected == Spot && response.Result.NextPageCursor != "") {
		return InstrumentPage{}, ErrInvalidPayload
	}
	instruments := make([]Instrument, len(response.Result.List))
	seen := make(map[string]struct{}, len(instruments))
	for i, raw := range response.Result.List {
		var native struct {
			Symbol             string          `json:"symbol"`
			SymbolID           int64           `json:"symbolId"`
			ContractType       string          `json:"contractType"`
			Status             string          `json:"status"`
			BaseCoin           string          `json:"baseCoin"`
			QuoteCoin          string          `json:"quoteCoin"`
			SettleCoin         string          `json:"settleCoin"`
			SymbolType         string          `json:"symbolType"`
			LaunchTime         string          `json:"launchTime"`
			DeliveryTime       string          `json:"deliveryTime"`
			DeliveryFeeRate    string          `json:"deliveryFeeRate"`
			PriceScale         string          `json:"priceScale"`
			UnifiedMarginTrade bool            `json:"unifiedMarginTrade"`
			FundingInterval    int64           `json:"fundingInterval"`
			UpperFundingRate   string          `json:"upperFundingRate"`
			LowerFundingRate   string          `json:"lowerFundingRate"`
			IsPreListing       bool            `json:"isPreListing"`
			PreListingInfo     json.RawMessage `json:"preListingInfo"`
			RiskParameters     json.RawMessage `json:"riskParameters"`
			PriceFilter        struct {
				Minimum string `json:"minPrice"`
				Maximum string `json:"maxPrice"`
				Tick    string `json:"tickSize"`
			} `json:"priceFilter"`
			LotSizeFilter struct {
				MinimumQty      string `json:"minOrderQty"`
				MaximumQty      string `json:"maxOrderQty"`
				MaximumMarket   string `json:"maxMktOrderQty"`
				QuantityStep    string `json:"qtyStep"`
				MinimumNotional string `json:"minNotionalValue"`
			} `json:"lotSizeFilter"`
		}
		if err := json.Unmarshal(raw, &native); err != nil || !validSymbol(native.Symbol) || native.Status == "" || native.BaseCoin == "" || native.QuoteCoin == "" {
			return InstrumentPage{}, fmt.Errorf("%w: instrument at ordinal %d", ErrInvalidPayload, i)
		}
		if _, ok := seen[native.Symbol]; ok {
			return InstrumentPage{}, fmt.Errorf("%w: duplicate instrument %s", ErrInvalidPayload, native.Symbol)
		}
		seen[native.Symbol] = struct{}{}
		instruments[i] = Instrument{
			Category: expected, Symbol: native.Symbol, SymbolID: native.SymbolID, ContractType: native.ContractType, Status: native.Status,
			BaseCoin: native.BaseCoin, QuoteCoin: native.QuoteCoin, SettleCoin: native.SettleCoin, SymbolType: native.SymbolType,
			LaunchTimeMS: native.LaunchTime, DeliveryTimeMS: native.DeliveryTime, DeliveryFeeRate: native.DeliveryFeeRate, PriceScale: native.PriceScale,
			TickSize: native.PriceFilter.Tick, MinimumPrice: native.PriceFilter.Minimum, MaximumPrice: native.PriceFilter.Maximum,
			MinimumOrderQty: native.LotSizeFilter.MinimumQty, MaximumOrderQty: native.LotSizeFilter.MaximumQty, MaximumMarketQty: native.LotSizeFilter.MaximumMarket,
			QuantityStep: native.LotSizeFilter.QuantityStep, MinimumNotional: native.LotSizeFilter.MinimumNotional,
			FundingIntervalMin: native.FundingInterval, UpperFundingRate: native.UpperFundingRate, LowerFundingRate: native.LowerFundingRate,
			UnifiedMarginTrade: native.UnifiedMarginTrade, PreListing: native.IsPreListing,
			PreListingInfo: slices.Clone(native.PreListingInfo), RiskParameters: slices.Clone(native.RiskParameters), Raw: slices.Clone(raw),
		}
	}
	return InstrumentPage{Category: expected, ObservedTimeMS: response.Time, NextCursor: response.Result.NextPageCursor, Instruments: instruments}, nil
}
