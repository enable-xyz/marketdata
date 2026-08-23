package binance

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

var ErrCoinMInvalidMetadata = errors.New("binance: invalid COIN-M metadata")

type CoinMRateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   uint32 `json:"intervalNum"`
	Limit         uint32 `json:"limit"`
}

type CoinMFilter struct {
	FilterType string
	Raw        json.RawMessage
}

type CoinMInstrumentMetadata struct {
	Symbol             string
	Pair               string
	ContractType       string
	DeliveryDateMS     int64
	OnboardDateMS      int64
	ContractStatus     string
	ContractSize       normalize.Decimal
	BaseAsset          string
	QuoteAsset         string
	MarginAsset        string
	PricePrecision     int
	QuantityPrecision  int
	BaseAssetPrecision int
	QuotePrecision     int
	EqualQtyPrecision  int
	UnderlyingType     string
	UnderlyingSubType  []string
	TriggerProtect     string
	Filters            []CoinMFilter
	OrderTypes         []string
	TimeInForce        []string
	NativeQuantityUnit normalize.NativeUnitKind
	Raw                json.RawMessage
	RawSHA256          [sha256.Size]byte
}

type CoinMExchangeInfo struct {
	ServerTimeMS int64
	Timezone     string
	RateLimits   []CoinMRateLimit
	Instruments  []CoinMInstrumentMetadata
}

func ParseCoinMExchangeInfo(raw []byte) (CoinMExchangeInfo, error) {
	if len(raw) == 0 || len(raw) > CoinMMaxRawPayloadBytes || !json.Valid(raw) {
		return CoinMExchangeInfo{}, ErrCoinMInvalidMetadata
	}
	var wire struct {
		ServerTimeMS int64             `json:"serverTime"`
		Timezone     string            `json:"timezone"`
		RateLimits   []CoinMRateLimit  `json:"rateLimits"`
		Symbols      []json.RawMessage `json:"symbols"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.ServerTimeMS < 0 || wire.Timezone == "" || len(wire.Symbols) == 0 || len(wire.Symbols) > CoinMMaxMergedRecords {
		return CoinMExchangeInfo{}, ErrCoinMInvalidMetadata
	}
	result := CoinMExchangeInfo{
		ServerTimeMS: wire.ServerTimeMS,
		Timezone:     wire.Timezone,
		RateLimits:   slices.Clone(wire.RateLimits),
		Instruments:  make([]CoinMInstrumentMetadata, len(wire.Symbols)),
	}
	seen := make(map[string]struct{}, len(wire.Symbols))
	for i, rawSymbol := range wire.Symbols {
		instrument, err := parseCoinMInstrumentMetadata(rawSymbol)
		if err != nil {
			return CoinMExchangeInfo{}, fmt.Errorf("%w: symbol %d: %v", ErrCoinMInvalidMetadata, i, err)
		}
		if _, ok := seen[instrument.Symbol]; ok {
			return CoinMExchangeInfo{}, fmt.Errorf("%w: duplicate symbol %q", ErrCoinMInvalidMetadata, instrument.Symbol)
		}
		seen[instrument.Symbol] = struct{}{}
		result.Instruments[i] = instrument
	}
	for _, rate := range result.RateLimits {
		if rate.RateLimitType == "" || rate.Interval == "" || rate.IntervalNum == 0 || rate.Limit == 0 {
			return CoinMExchangeInfo{}, fmt.Errorf("%w: malformed rate limit", ErrCoinMInvalidMetadata)
		}
	}
	return result, nil
}

func parseCoinMInstrumentMetadata(raw json.RawMessage) (CoinMInstrumentMetadata, error) {
	var wire struct {
		Symbol             string            `json:"symbol"`
		Pair               string            `json:"pair"`
		ContractType       string            `json:"contractType"`
		DeliveryDateMS     int64             `json:"deliveryDate"`
		OnboardDateMS      int64             `json:"onboardDate"`
		ContractStatus     string            `json:"contractStatus"`
		ContractSize       json.RawMessage   `json:"contractSize"`
		BaseAsset          string            `json:"baseAsset"`
		QuoteAsset         string            `json:"quoteAsset"`
		MarginAsset        string            `json:"marginAsset"`
		PricePrecision     int               `json:"pricePrecision"`
		QuantityPrecision  int               `json:"quantityPrecision"`
		BaseAssetPrecision int               `json:"baseAssetPrecision"`
		QuotePrecision     int               `json:"quotePrecision"`
		EqualQtyPrecision  int               `json:"equalQtyPrecision"`
		UnderlyingType     string            `json:"underlyingType"`
		UnderlyingSubType  []string          `json:"underlyingSubType"`
		TriggerProtect     string            `json:"triggerProtect"`
		Filters            []json.RawMessage `json:"filters"`
		OrderTypes         []string          `json:"orderTypes"`
		TimeInForce        []string          `json:"timeInForce"`
	}
	if len(raw) == 0 || len(raw) > CoinMMaxRawPayloadBytes || json.Unmarshal(raw, &wire) != nil ||
		wire.Symbol == "" || wire.Pair == "" || wire.ContractType == "" || wire.ContractStatus == "" ||
		wire.BaseAsset == "" || wire.QuoteAsset == "" || wire.MarginAsset == "" || wire.BaseAsset == wire.QuoteAsset ||
		wire.MarginAsset != wire.BaseAsset || wire.QuoteAsset != "USD" || wire.DeliveryDateMS < 0 || wire.OnboardDateMS < 0 ||
		wire.PricePrecision < 0 || wire.QuantityPrecision < 0 || wire.BaseAssetPrecision < 0 || wire.QuotePrecision < 0 || wire.EqualQtyPrecision < 0 ||
		len(wire.Filters) > 128 || len(wire.OrderTypes) > 128 || len(wire.TimeInForce) > 128 || len(wire.UnderlyingSubType) > 128 {
		return CoinMInstrumentMetadata{}, ErrCoinMInvalidMetadata
	}
	contractSizeRaw := bytes.TrimSpace(wire.ContractSize)
	if len(contractSizeRaw) == 0 || contractSizeRaw[0] == '"' || bytes.Equal(contractSizeRaw, []byte("null")) {
		return CoinMInstrumentMetadata{}, fmt.Errorf("%w: contractSize must be a JSON number", ErrCoinMInvalidMetadata)
	}
	contractSize, err := normalize.ParseDecimal(string(contractSizeRaw), normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil || strings.HasPrefix(contractSize.Coefficient, "-") || contractSize.IsZero() {
		return CoinMInstrumentMetadata{}, fmt.Errorf("%w: invalid contractSize", ErrCoinMInvalidMetadata)
	}
	filters := make([]CoinMFilter, len(wire.Filters))
	seenFilters := make(map[string]struct{}, len(wire.Filters))
	for i, rawFilter := range wire.Filters {
		var identity struct {
			FilterType string `json:"filterType"`
		}
		if json.Unmarshal(rawFilter, &identity) != nil || identity.FilterType == "" {
			return CoinMInstrumentMetadata{}, fmt.Errorf("%w: filter %d", ErrCoinMInvalidMetadata, i)
		}
		if _, ok := seenFilters[identity.FilterType]; ok {
			return CoinMInstrumentMetadata{}, fmt.Errorf("%w: duplicate filter %s", ErrCoinMInvalidMetadata, identity.FilterType)
		}
		seenFilters[identity.FilterType] = struct{}{}
		filters[i] = CoinMFilter{FilterType: identity.FilterType, Raw: slices.Clone(rawFilter)}
	}
	return CoinMInstrumentMetadata{
		Symbol:             wire.Symbol,
		Pair:               wire.Pair,
		ContractType:       wire.ContractType,
		DeliveryDateMS:     wire.DeliveryDateMS,
		OnboardDateMS:      wire.OnboardDateMS,
		ContractStatus:     wire.ContractStatus,
		ContractSize:       contractSize,
		BaseAsset:          wire.BaseAsset,
		QuoteAsset:         wire.QuoteAsset,
		MarginAsset:        wire.MarginAsset,
		PricePrecision:     wire.PricePrecision,
		QuantityPrecision:  wire.QuantityPrecision,
		BaseAssetPrecision: wire.BaseAssetPrecision,
		QuotePrecision:     wire.QuotePrecision,
		EqualQtyPrecision:  wire.EqualQtyPrecision,
		UnderlyingType:     wire.UnderlyingType,
		UnderlyingSubType:  slices.Clone(wire.UnderlyingSubType),
		TriggerProtect:     wire.TriggerProtect,
		Filters:            filters,
		OrderTypes:         slices.Clone(wire.OrderTypes),
		TimeInForce:        slices.Clone(wire.TimeInForce),
		NativeQuantityUnit: normalize.NativeUnitContracts,
		Raw:                slices.Clone(raw),
		RawSHA256:          sha256.Sum256(raw),
	}, nil
}

// ContractTerms requires explicit temporal validity, catalog identity and payoff
// metadata. The exchangeInfo parser supplies contractSize but does not silently
// invent a payoff formula from the symbol.
func (m CoinMInstrumentMetadata) ContractTerms(instrumentUID, catalogVersion string, validFromNS int64, validUntilNS normalize.OptionalInt64, payoff normalize.CoinMPayoffKind) (normalize.CoinMContractTerms, error) {
	terms := normalize.CoinMContractTerms{
		InstrumentUID:     instrumentUID,
		BaseAssetID:       m.BaseAsset,
		QuoteAssetID:      m.QuoteAsset,
		SettlementAssetID: m.MarginAsset,
		CatalogVersion:    catalogVersion,
		ValidFromNS:       validFromNS,
		ValidUntilNS:      validUntilNS,
		ContractSize:      m.ContractSize,
		Payoff:            payoff,
	}
	if err := terms.Validate(); err != nil {
		return normalize.CoinMContractTerms{}, err
	}
	return terms, nil
}

type CoinMPollObservation struct {
	OperationID     string
	PollCycleID     [16]byte
	Method          string
	Path            string
	Symbol          string
	ScheduledTimeNS int64
	RequestTimeNS   int64
	ReceivedTimeNS  int64
}

func (p CoinMPollObservation) Validate() error {
	if p.OperationID == "" || len(p.OperationID) > 256 || strings.IndexByte(p.OperationID, 0) >= 0 ||
		p.PollCycleID == ([16]byte{}) || p.Method != "GET" || p.Path != CoinMOpenInterestPath || p.Symbol == "" ||
		p.ScheduledTimeNS < 0 || p.RequestTimeNS < p.ScheduledTimeNS || p.ReceivedTimeNS < p.RequestTimeNS {
		return ErrCoinMInvalidPoll
	}
	return nil
}

type CoinMOpenInterestObservation struct {
	Poll            CoinMPollObservation
	Symbol          string
	Pair            string
	ContractType    string
	NativeValueText string
	SourceTimeMS    int64
	Normalized      normalize.OpenInterestObservation
}

func ParseCoinMOpenInterest(raw []byte, poll CoinMPollObservation, instrument normalize.InstrumentIdentity, terms normalize.CoinMContractTerms) (CoinMOpenInterestObservation, error) {
	if err := poll.Validate(); err != nil {
		return CoinMOpenInterestObservation{}, err
	}
	var wire struct {
		Symbol       string `json:"symbol"`
		Pair         string `json:"pair"`
		OpenInterest string `json:"openInterest"`
		ContractType string `json:"contractType"`
		TimeMS       int64  `json:"time"`
	}
	if err := coinMUnmarshalBoundedStrict(raw, &wire); err != nil {
		return CoinMOpenInterestObservation{}, err
	}
	if wire.Symbol != poll.Symbol || wire.Symbol != instrument.NativeID || wire.Pair == "" || wire.OpenInterest == "" || wire.ContractType == "" || wire.TimeMS < 0 {
		return CoinMOpenInterestObservation{}, fmt.Errorf("%w: response does not match poll or instrument", ErrCoinMInvalidPoll)
	}
	contracts, err := normalize.ParseDecimal(wire.OpenInterest, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil || strings.HasPrefix(contracts.Coefficient, "-") {
		return CoinMOpenInterestObservation{}, fmt.Errorf("%w: invalid native openInterest", ErrCoinMInvalidPoll)
	}
	provenance, err := coinMFieldProvenance(wire.TimeMS, poll.ReceivedTimeNS)
	if err != nil {
		return CoinMOpenInterestObservation{}, err
	}
	conversion, err := normalize.ConvertCoinMContractsWithoutPrice(contracts, poll.ReceivedTimeNS, provenance, terms)
	if err != nil {
		return CoinMOpenInterestObservation{}, err
	}
	if terms.InstrumentUID != instrument.InstrumentUID || terms.BaseAssetID != instrument.BaseAssetID || terms.QuoteAssetID != instrument.QuoteAssetID {
		return CoinMOpenInterestObservation{}, fmt.Errorf("%w: temporal contract terms do not match instrument", ErrCoinMInvalidPoll)
	}
	normalized := normalize.OpenInterestObservation{
		State:                    normalize.SourceValue,
		Variant:                  "openInterest",
		Native:                   conversion.NativeContracts,
		Sidedness:                normalize.OpenInterestUnspecified,
		ReportedVariants:         []normalize.ReportedValue{{Name: "openInterest_contracts", Value: conversion.NativeContracts}},
		MultiplierCatalogVersion: terms.CatalogVersion,
		DerivedBase:              conversion.DerivedBase,
		DerivedQuote:             conversion.DerivedQuote,
		DerivedUSD:               conversion.DerivedUSD,
		Provenance:               provenance,
	}
	if err := normalized.Validate(); err != nil {
		return CoinMOpenInterestObservation{}, err
	}
	return CoinMOpenInterestObservation{
		Poll:            poll,
		Symbol:          wire.Symbol,
		Pair:            wire.Pair,
		ContractType:    wire.ContractType,
		NativeValueText: wire.OpenInterest,
		SourceTimeMS:    wire.TimeMS,
		Normalized:      normalized,
	}, nil
}

func (o CoinMOpenInterestObservation) DerivativeTicker(metadata normalize.Metadata) (normalize.DerivativeTickerV1, error) {
	event := normalize.DerivativeTickerV1{
		Metadata:         metadata,
		NativeSourceRole: "dapi_openInterest_current_poll_observation",
		LastPrice:        missingCoinMNumericField(),
		MarkPrice:        missingCoinMNumericField(),
		IndexPrice:       missingCoinMNumericField(),
		FundingRate:      missingCoinMNumericField(),
		NextFundingTime:  missingCoinMTimeField(),
		OpenInterest:     []normalize.OpenInterestObservation{o.Normalized},
		SettlementPrice:  missingCoinMNumericField(),
		Basis:            missingCoinMNumericField(),
		Premium:          missingCoinMNumericField(),
	}
	if err := event.Validate(); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	return event, nil
}
