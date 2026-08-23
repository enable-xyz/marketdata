package binance

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

var ErrUSDMInvalidMetadata = errors.New("binance: invalid USD-M metadata")

type USDMRateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   uint32 `json:"intervalNum"`
	Limit         uint32 `json:"limit"`
}

type USDMFilter struct {
	FilterType string
	Raw        json.RawMessage
}

type USDMInstrumentMetadata struct {
	Symbol                  string
	Pair                    string
	ContractType            string
	DeliveryDateMS          int64
	OnboardDateMS           int64
	Status                  string
	BaseAsset               string
	QuoteAsset              string
	MarginAsset             string
	PricePrecision          int
	QuantityPrecision       int
	BaseAssetPrecision      int
	QuotePrecision          int
	UnderlyingType          string
	UnderlyingSubType       []string
	SettlePlan              int
	TriggerProtect          string
	LiquidationFee          string
	MarketTakeBound         string
	Filters                 []USDMFilter
	OrderTypes              []string
	TimeInForce             []string
	NativeQuantityUnitClaim string
	Raw                     json.RawMessage
	RawSHA256               [sha256.Size]byte
}

type USDMExchangeInfo struct {
	ServerTimeMS int64
	Timezone     string
	RateLimits   []USDMRateLimit
	Instruments  []USDMInstrumentMetadata
}

func ParseUSDMExchangeInfo(raw []byte) (USDMExchangeInfo, error) {
	var wire struct {
		ServerTimeMS int64             `json:"serverTime"`
		Timezone     string            `json:"timezone"`
		RateLimits   []USDMRateLimit   `json:"rateLimits"`
		Symbols      []json.RawMessage `json:"symbols"`
	}
	if len(raw) == 0 || len(raw) > USDMMaxRawPayloadBytes || json.Unmarshal(raw, &wire) != nil || wire.ServerTimeMS < 0 || wire.Timezone == "" || len(wire.Symbols) == 0 {
		return USDMExchangeInfo{}, ErrUSDMInvalidMetadata
	}
	result := USDMExchangeInfo{ServerTimeMS: wire.ServerTimeMS, Timezone: wire.Timezone, RateLimits: slices.Clone(wire.RateLimits), Instruments: make([]USDMInstrumentMetadata, len(wire.Symbols))}
	seen := make(map[string]struct{}, len(wire.Symbols))
	for i, rawSymbol := range wire.Symbols {
		instrument, err := parseUSDMInstrumentMetadata(rawSymbol)
		if err != nil {
			return USDMExchangeInfo{}, fmt.Errorf("%w: symbol %d: %v", ErrUSDMInvalidMetadata, i, err)
		}
		if _, ok := seen[instrument.Symbol]; ok {
			return USDMExchangeInfo{}, fmt.Errorf("%w: duplicate symbol %q", ErrUSDMInvalidMetadata, instrument.Symbol)
		}
		seen[instrument.Symbol] = struct{}{}
		result.Instruments[i] = instrument
	}
	for _, rate := range result.RateLimits {
		if rate.RateLimitType == "" || rate.Interval == "" || rate.IntervalNum == 0 || rate.Limit == 0 {
			return USDMExchangeInfo{}, fmt.Errorf("%w: malformed rate limit", ErrUSDMInvalidMetadata)
		}
	}
	return result, nil
}

func parseUSDMInstrumentMetadata(raw json.RawMessage) (USDMInstrumentMetadata, error) {
	var wire struct {
		Symbol             string            `json:"symbol"`
		Pair               string            `json:"pair"`
		ContractType       string            `json:"contractType"`
		DeliveryDateMS     int64             `json:"deliveryDate"`
		OnboardDateMS      int64             `json:"onboardDate"`
		Status             string            `json:"status"`
		BaseAsset          string            `json:"baseAsset"`
		QuoteAsset         string            `json:"quoteAsset"`
		MarginAsset        string            `json:"marginAsset"`
		PricePrecision     int               `json:"pricePrecision"`
		QuantityPrecision  int               `json:"quantityPrecision"`
		BaseAssetPrecision int               `json:"baseAssetPrecision"`
		QuotePrecision     int               `json:"quotePrecision"`
		UnderlyingType     string            `json:"underlyingType"`
		UnderlyingSubType  []string          `json:"underlyingSubType"`
		SettlePlan         int               `json:"settlePlan"`
		TriggerProtect     string            `json:"triggerProtect"`
		LiquidationFee     string            `json:"liquidationFee"`
		MarketTakeBound    string            `json:"marketTakeBound"`
		Filters            []json.RawMessage `json:"filters"`
		OrderTypes         []string          `json:"orderTypes"`
		TimeInForce        []string          `json:"timeInForce"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.Symbol == "" || wire.Pair == "" || wire.ContractType == "" || wire.Status == "" || wire.BaseAsset == "" || wire.QuoteAsset == "" || wire.MarginAsset == "" || wire.BaseAsset == wire.QuoteAsset || wire.DeliveryDateMS < 0 || wire.OnboardDateMS < 0 || wire.PricePrecision < 0 || wire.QuantityPrecision < 0 || wire.BaseAssetPrecision < 0 || wire.QuotePrecision < 0 {
		return USDMInstrumentMetadata{}, ErrUSDMInvalidMetadata
	}
	filters := make([]USDMFilter, len(wire.Filters))
	seenFilters := make(map[string]struct{}, len(wire.Filters))
	for i, rawFilter := range wire.Filters {
		var identity struct {
			FilterType string `json:"filterType"`
		}
		if json.Unmarshal(rawFilter, &identity) != nil || identity.FilterType == "" {
			return USDMInstrumentMetadata{}, fmt.Errorf("%w: filter %d", ErrUSDMInvalidMetadata, i)
		}
		if _, ok := seenFilters[identity.FilterType]; ok {
			return USDMInstrumentMetadata{}, fmt.Errorf("%w: duplicate filter %s", ErrUSDMInvalidMetadata, identity.FilterType)
		}
		seenFilters[identity.FilterType] = struct{}{}
		filters[i] = USDMFilter{FilterType: identity.FilterType, Raw: slices.Clone(rawFilter)}
	}
	return USDMInstrumentMetadata{
		Symbol: wire.Symbol, Pair: wire.Pair, ContractType: wire.ContractType, DeliveryDateMS: wire.DeliveryDateMS, OnboardDateMS: wire.OnboardDateMS, Status: wire.Status,
		BaseAsset: wire.BaseAsset, QuoteAsset: wire.QuoteAsset, MarginAsset: wire.MarginAsset, PricePrecision: wire.PricePrecision, QuantityPrecision: wire.QuantityPrecision,
		BaseAssetPrecision: wire.BaseAssetPrecision, QuotePrecision: wire.QuotePrecision, UnderlyingType: wire.UnderlyingType, UnderlyingSubType: slices.Clone(wire.UnderlyingSubType),
		SettlePlan: wire.SettlePlan, TriggerProtect: wire.TriggerProtect, LiquidationFee: wire.LiquidationFee, MarketTakeBound: wire.MarketTakeBound,
		Filters: filters, OrderTypes: slices.Clone(wire.OrderTypes), TimeInForce: slices.Clone(wire.TimeInForce),
		NativeQuantityUnitClaim: "venue_native_contract_quantity_unspecified_without_temporal_multiplier_formula", Raw: slices.Clone(raw), RawSHA256: sha256.Sum256(raw),
	}, nil
}

func NewUSDMRatePoolFromExchangeInfo(info USDMExchangeInfo, initialMonotonicNS uint64) (*USDMRatePool, error) {
	for _, limit := range info.RateLimits {
		if limit.RateLimitType == "REQUEST_WEIGHT" && limit.Interval == "MINUTE" && limit.IntervalNum == 1 {
			return NewUSDMRatePool(limit.Limit, USDMFAPIRefillIntervalNS, initialMonotonicNS)
		}
	}
	return nil, fmt.Errorf("%w: one-minute REQUEST_WEIGHT limit is required for the shared FAPI pool", ErrUSDMInvalidMetadata)
}

type USDMPollObservation struct {
	OperationID     string
	PollCycleID     [16]byte
	Method          string
	Path            string
	Symbol          string
	ScheduledTimeNS int64
	RequestTimeNS   int64
	ReceivedTimeNS  int64
}

func (p USDMPollObservation) Validate() error {
	if p.OperationID == "" || len(p.OperationID) > 256 || strings.IndexByte(p.OperationID, 0) >= 0 || p.PollCycleID == ([16]byte{}) || p.Method != "GET" || p.Path != USDMOpenInterestPath || p.Symbol == "" || p.ScheduledTimeNS < 0 || p.RequestTimeNS < p.ScheduledTimeNS || p.ReceivedTimeNS < p.RequestTimeNS {
		return ErrUSDMInvalidPoll
	}
	return nil
}

type USDMOpenInterestObservation struct {
	Poll            USDMPollObservation
	Symbol          string
	NativeValueText string
	SourceTimeMS    int64
	Normalized      normalize.OpenInterestObservation
}

func ParseUSDMOpenInterest(raw []byte, poll USDMPollObservation) (USDMOpenInterestObservation, error) {
	if err := poll.Validate(); err != nil {
		return USDMOpenInterestObservation{}, err
	}
	var wire struct {
		OpenInterest string `json:"openInterest"`
		Symbol       string `json:"symbol"`
		TimeMS       int64  `json:"time"`
	}
	if err := unmarshalUSDMBounded(raw, &wire); err != nil {
		return USDMOpenInterestObservation{}, err
	}
	if wire.Symbol != poll.Symbol || wire.OpenInterest == "" || wire.TimeMS < 0 {
		return USDMOpenInterestObservation{}, fmt.Errorf("%w: response does not match poll", ErrUSDMInvalidPoll)
	}
	decimal, err := normalize.ParseDecimal(wire.OpenInterest, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil || strings.HasPrefix(decimal.Coefficient, "-") {
		return USDMOpenInterestObservation{}, fmt.Errorf("%w: invalid native openInterest", ErrUSDMInvalidPoll)
	}
	provenance, err := usdmFieldProvenance(wire.TimeMS, poll.ReceivedTimeNS)
	if err != nil {
		return USDMOpenInterestObservation{}, err
	}
	normalized := normalize.OpenInterestObservation{
		State: normalize.SourceValue, Variant: "openInterest", Native: normalize.NativeValue{Decimal: decimal, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "openInterest"}},
		Sidedness: normalize.OpenInterestUnspecified, Provenance: provenance,
		DerivedBase: missingUSDMDerivedValue(), DerivedQuote: missingUSDMDerivedValue(), DerivedUSD: missingUSDMDerivedValue(),
	}
	if err := normalized.Validate(); err != nil {
		return USDMOpenInterestObservation{}, err
	}
	return USDMOpenInterestObservation{Poll: poll, Symbol: wire.Symbol, NativeValueText: wire.OpenInterest, SourceTimeMS: wire.TimeMS, Normalized: normalized}, nil
}

func (o USDMOpenInterestObservation) DerivativeTicker(metadata normalize.Metadata) (normalize.DerivativeTickerV1, error) {
	event := normalize.DerivativeTickerV1{
		Metadata: metadata, NativeSourceRole: "rest_openInterest_current_poll_observation",
		LastPrice: missingUSDMNumericField(), MarkPrice: missingUSDMNumericField(), IndexPrice: missingUSDMNumericField(), FundingRate: missingUSDMNumericField(), NextFundingTime: missingUSDMTimeField(),
		OpenInterest: []normalize.OpenInterestObservation{o.Normalized}, SettlementPrice: missingUSDMNumericField(), Basis: missingUSDMNumericField(), Premium: missingUSDMNumericField(),
	}
	if err := event.Validate(); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	return event, nil
}
