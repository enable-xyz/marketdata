package binance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
)

const (
	MaxExchangeInfoBytes       = capture.MaxPayloadBytes
	MaxExchangeInfoPages       = 64
	MaxExchangeInfoSymbols     = 100_000
	MaxFiltersPerSymbol        = 64
	MaxJSONFieldsPerObject     = 256
	MaxJSONItemsPerArray       = 100_000
	MaxJSONTokens              = 2_000_000
	MaxExchangeInfoStringBytes = 4096
	MaxJSONNumberBytes         = 128
	MaxJSONNesting             = 32
)

var ErrInvalidExchangeInfo = errors.New("binance: invalid exchangeInfo")

type ParserLimits struct {
	MaxResponseBytes   int
	MaxPages           int
	MaxSymbols         int
	MaxFilters         int
	MaxFieldsPerObject int
	MaxItemsPerArray   int
	MaxTokens          int
	MaxStringBytes     int
	MaxNumberBytes     int
	MaxNesting         int
}

func DefaultParserLimits() ParserLimits {
	return ParserLimits{
		MaxResponseBytes:   MaxExchangeInfoBytes,
		MaxPages:           MaxExchangeInfoPages,
		MaxSymbols:         MaxExchangeInfoSymbols,
		MaxFilters:         MaxFiltersPerSymbol,
		MaxFieldsPerObject: MaxJSONFieldsPerObject,
		MaxItemsPerArray:   MaxJSONItemsPerArray,
		MaxTokens:          MaxJSONTokens,
		MaxStringBytes:     MaxExchangeInfoStringBytes,
		MaxNumberBytes:     MaxJSONNumberBytes,
		MaxNesting:         MaxJSONNesting,
	}
}

type ExactField struct {
	Name  string
	Kind  string
	Value string
}

type Filter struct {
	Type          string
	Known         bool
	ExactFields   []ExactField
	UnknownFields []string
	Raw           json.RawMessage
}

type Symbol struct {
	NativeID   string
	Status     string
	BaseAsset  string
	QuoteAsset string
	Filters    []Filter
	Raw        json.RawMessage
	Candidate  catalog.InstrumentCandidate
}

type ExchangeInfoPage struct {
	PageIndex      int
	PageCount      int
	Timezone       string
	ServerTime     string
	HeaderIdentity json.RawMessage
	Symbols        []Symbol
	RawSHA256      [sha256.Size]byte
	Raw            []byte
}

type CapturedPage struct {
	PageIndex int
	PageCount int
	Request   capture.RESTRequestEvidenceV1
	Response  capture.RESTResponseEvidenceV1
	RawRecord catalog.RawRecordCoordinate
	Raw       []byte
	RawSHA256 [sha256.Size]byte
}

type CommittedRawSegment struct {
	SegmentID     string
	ObjectKey     string
	ContentSHA256 [sha256.Size]byte
	ByteLength    int64
}

type ComposeOptions struct {
	ExplicitLifecycleClosures []string
}

type ComposedExchangeInfo struct {
	Timezone      string
	ServerTime    string
	CompletedAtNS int64
	Symbols       []Symbol
	Candidates    []catalog.InstrumentCandidate
	Evidence      []catalog.SyncPageEvidence
}

func NewInMemoryCapturedPage(pageIndex, pageCount int, request capture.RESTRequestEvidenceV1, response capture.RESTResponseEvidenceV1, raw []byte) CapturedPage {
	copied := append([]byte(nil), raw...)
	rawHash := sha256.Sum256(copied)
	segmentID := deterministicBinanceUUID("fixture-segment", request.RequestID+"\x00"+hex.EncodeToString(rawHash[:]))
	return CapturedPage{
		PageIndex: pageIndex,
		PageCount: pageCount,
		Request:   request,
		Response:  response,
		RawRecord: catalog.RawRecordCoordinate{
			EvidenceScope:        catalog.RawEvidenceInMemoryProjection,
			RawSegmentID:         segmentID,
			ObjectKey:            "in-memory://fixture-projection/" + segmentID,
			RawSegmentSHA256:     hex.EncodeToString(rawHash[:]),
			RawSegmentByteLength: int64(len(copied)),
			PollCycleID:          deterministicBinanceUUID("fixture-poll-cycle", fmt.Sprintf("%s\x00%d", request.RequestID, request.ScheduledAtNS)),
			ArrivalOrdinal:       uint64(pageIndex + 1),
			MessageOrdinal:       0,
			EnvelopeVersion:      capture.EnvelopeVersion,
		},
		Raw:       copied,
		RawSHA256: rawHash,
	}
}

func CapturedPageFromEnvelopes(pageIndex, pageCount int, segment CommittedRawSegment, requestEnvelope, responseEnvelope capture.EnvelopeV1) (CapturedPage, error) {
	if err := requestEnvelope.Validate(); err != nil {
		return CapturedPage{}, fmt.Errorf("%w: request envelope: %v", ErrInvalidExchangeInfo, err)
	}
	if requestEnvelope.RecordKind != capture.RecordKindControl || !requestEnvelope.ControlKind.Valid || requestEnvelope.ControlKind.Value != capture.ControlRequestStarted {
		return CapturedPage{}, fmt.Errorf("%w: request envelope is not request_started evidence", ErrInvalidExchangeInfo)
	}
	request, err := capture.UnmarshalRESTRequestEvidence(requestEnvelope.Extensions)
	if err != nil {
		return CapturedPage{}, fmt.Errorf("%w: request evidence: %v", ErrInvalidExchangeInfo, err)
	}
	if err := responseEnvelope.Validate(); err != nil {
		return CapturedPage{}, fmt.Errorf("%w: response envelope: %v", ErrInvalidExchangeInfo, err)
	}
	if responseEnvelope.RecordKind != capture.RecordKindREST || responseEnvelope.PayloadEncoding != capture.PayloadEncodingJSON {
		return CapturedPage{}, fmt.Errorf("%w: response envelope is not a JSON REST response", ErrInvalidExchangeInfo)
	}
	response, err := capture.UnmarshalRESTResponseEvidence(responseEnvelope.Extensions)
	if err != nil {
		return CapturedPage{}, fmt.Errorf("%w: response evidence: %v", ErrInvalidExchangeInfo, err)
	}
	if requestEnvelope.TerminalOutcome != capture.TerminalObserved ||
		responseEnvelope.TerminalOutcome != capture.TerminalObserved {
		return CapturedPage{}, fmt.Errorf("%w: request or response envelope is not an observed terminal outcome", ErrInvalidExchangeInfo)
	}
	if requestEnvelope.SourceID != SpotSourceID || responseEnvelope.SourceID != SpotSourceID ||
		requestEnvelope.ChannelOrEndpoint != SpotExchangeInfoChannel || responseEnvelope.ChannelOrEndpoint != SpotExchangeInfoChannel {
		return CapturedPage{}, fmt.Errorf("%w: envelope source or endpoint is not Binance Spot exchangeInfo", ErrInvalidExchangeInfo)
	}
	if !requestEnvelope.PollCycleID.Valid || !responseEnvelope.PollCycleID.Valid ||
		requestEnvelope.PollCycleID.Value != responseEnvelope.PollCycleID.Value {
		return CapturedPage{}, fmt.Errorf("%w: request and response do not share one poll cycle", ErrInvalidExchangeInfo)
	}
	if responseEnvelope.ArrivalOrdinal <= requestEnvelope.ArrivalOrdinal {
		return CapturedPage{}, fmt.Errorf("%w: response arrival ordinal does not follow request_started", ErrInvalidExchangeInfo)
	}
	if !requestEnvelope.SubscriptionOrRequestID.Valid || requestEnvelope.SubscriptionOrRequestID.Value != request.RequestID ||
		!responseEnvelope.SubscriptionOrRequestID.Valid || responseEnvelope.SubscriptionOrRequestID.Value != request.RequestID ||
		response.RequestID != request.RequestID {
		return CapturedPage{}, fmt.Errorf("%w: request identity differs across envelope boundaries", ErrInvalidExchangeInfo)
	}
	if !requestEnvelope.ScheduledAtNS.Valid || requestEnvelope.ScheduledAtNS.Value != request.ScheduledAtNS ||
		!requestEnvelope.RequestStartedAtNS.Valid || requestEnvelope.RequestStartedAtNS.Value != request.StartedAtNS ||
		requestEnvelope.RequestCompletedAtNS.Valid ||
		!responseEnvelope.ScheduledAtNS.Valid || responseEnvelope.ScheduledAtNS.Value != request.ScheduledAtNS ||
		!responseEnvelope.RequestStartedAtNS.Valid || responseEnvelope.RequestStartedAtNS.Value != request.StartedAtNS ||
		!responseEnvelope.RequestCompletedAtNS.Valid || responseEnvelope.RequestCompletedAtNS.Value != response.CompletedAtNS {
		return CapturedPage{}, fmt.Errorf("%w: request timing differs across envelope boundaries", ErrInvalidExchangeInfo)
	}
	if !responseEnvelope.HTTPStatusOrWSState.Valid ||
		responseEnvelope.HTTPStatusOrWSState.Value != strconv.Itoa(response.Status) {
		return CapturedPage{}, fmt.Errorf("%w: response status differs across envelope boundaries", ErrInvalidExchangeInfo)
	}
	pollCycleID := opaqueUUID(responseEnvelope.PollCycleID.Value)
	rawRecord := catalog.RawRecordCoordinate{
		EvidenceScope:        catalog.RawEvidenceCommitted,
		RawSegmentID:         segment.SegmentID,
		ObjectKey:            segment.ObjectKey,
		RawSegmentSHA256:     hex.EncodeToString(segment.ContentSHA256[:]),
		RawSegmentByteLength: segment.ByteLength,
		PollCycleID:          pollCycleID,
		ArrivalOrdinal:       responseEnvelope.ArrivalOrdinal,
		MessageOrdinal:       responseEnvelope.MessageOrdinal,
		EnvelopeVersion:      responseEnvelope.EnvelopeVersion,
	}
	if err := rawRecord.Validate(); err != nil {
		return CapturedPage{}, fmt.Errorf("%w: committed raw segment coordinate: %v", ErrInvalidExchangeInfo, err)
	}
	page := CapturedPage{
		PageIndex: pageIndex,
		PageCount: pageCount,
		Request:   request,
		Response:  response,
		RawRecord: rawRecord,
		Raw:       append([]byte(nil), responseEnvelope.RawPayload...),
		RawSHA256: responseEnvelope.RawPayloadSHA256,
	}
	if sha256.Sum256(page.Raw) != page.RawSHA256 {
		return CapturedPage{}, fmt.Errorf("%w: response envelope raw hash mismatch", ErrInvalidExchangeInfo)
	}
	return page, nil
}

func ParseExchangeInfoPage(page CapturedPage, limits ParserLimits) (ExchangeInfoPage, error) {
	if err := validateParserLimits(limits); err != nil {
		return ExchangeInfoPage{}, err
	}
	if err := validateCapturedPage(page, limits); err != nil {
		return ExchangeInfoPage{}, err
	}
	if err := validateJSONStructure(page.Raw, limits); err != nil {
		return ExchangeInfoPage{}, err
	}
	object, err := decodeObject(page.Raw)
	if err != nil {
		return ExchangeInfoPage{}, fmt.Errorf("%w: response object: %v", ErrInvalidExchangeInfo, err)
	}
	timezone, err := requiredString(object, "timezone", limits.MaxStringBytes)
	if err != nil {
		return ExchangeInfoPage{}, err
	}
	if timezone != "UTC" {
		return ExchangeInfoPage{}, fmt.Errorf("%w: timezone %q is not UTC", ErrInvalidExchangeInfo, timezone)
	}
	serverTime, err := requiredInteger(object, "serverTime")
	if err != nil {
		return ExchangeInfoPage{}, err
	}
	symbolBytes, ok := object["symbols"]
	if !ok {
		return ExchangeInfoPage{}, fmt.Errorf("%w: missing symbols", ErrInvalidExchangeInfo)
	}
	var rawSymbols []json.RawMessage
	if err := json.Unmarshal(symbolBytes, &rawSymbols); err != nil {
		return ExchangeInfoPage{}, fmt.Errorf("%w: symbols must be an array", ErrInvalidExchangeInfo)
	}
	if len(rawSymbols) > limits.MaxSymbols {
		return ExchangeInfoPage{}, fmt.Errorf("%w: symbol count %d exceeds %d", ErrInvalidExchangeInfo, len(rawSymbols), limits.MaxSymbols)
	}
	headerObject := make(map[string]json.RawMessage, len(object)-1)
	for name, raw := range object {
		if name != "symbols" {
			headerObject[name] = raw
		}
	}
	headerBytes, err := json.Marshal(headerObject)
	if err != nil {
		return ExchangeInfoPage{}, fmt.Errorf("%w: encode page identity: %v", ErrInvalidExchangeInfo, err)
	}
	headerIdentity, err := catalog.CanonicalJSON(headerBytes)
	if err != nil {
		return ExchangeInfoPage{}, fmt.Errorf("%w: canonical page identity: %v", ErrInvalidExchangeInfo, err)
	}
	result := ExchangeInfoPage{
		PageIndex:      page.PageIndex,
		PageCount:      page.PageCount,
		Timezone:       timezone,
		ServerTime:     serverTime,
		HeaderIdentity: headerIdentity,
		RawSHA256:      page.RawSHA256,
		Raw:            append([]byte(nil), page.Raw...),
		Symbols:        make([]Symbol, 0, len(rawSymbols)),
	}
	for i, rawSymbol := range rawSymbols {
		symbol, err := parseSymbol(rawSymbol, limits)
		if err != nil {
			return ExchangeInfoPage{}, fmt.Errorf("%w: symbol %d: %v", ErrInvalidExchangeInfo, i, err)
		}
		result.Symbols = append(result.Symbols, symbol)
	}
	return result, nil
}

func ComposeExchangeInfo(pages []CapturedPage, options ComposeOptions, limits ParserLimits) (ComposedExchangeInfo, error) {
	if err := validateParserLimits(limits); err != nil {
		return ComposedExchangeInfo{}, err
	}
	if len(pages) == 0 || len(pages) > limits.MaxPages {
		return ComposedExchangeInfo{}, fmt.Errorf("%w: page count %d is outside bounds", ErrInvalidExchangeInfo, len(pages))
	}
	pages = slices.Clone(pages)
	slices.SortFunc(pages, func(a, b CapturedPage) int { return a.PageIndex - b.PageIndex })
	pageCount := pages[0].PageCount
	if pageCount != len(pages) {
		return ComposedExchangeInfo{}, fmt.Errorf("%w: incomplete page set: got %d of %d", ErrInvalidExchangeInfo, len(pages), pageCount)
	}
	closures := make(map[string]struct{}, len(options.ExplicitLifecycleClosures))
	for _, nativeID := range options.ExplicitLifecycleClosures {
		if nativeID == "" || len(nativeID) > limits.MaxStringBytes {
			return ComposedExchangeInfo{}, fmt.Errorf("%w: invalid explicit lifecycle closure identity", ErrInvalidExchangeInfo)
		}
		if _, exists := closures[nativeID]; exists {
			return ComposedExchangeInfo{}, fmt.Errorf("%w: duplicate explicit lifecycle closure %q", ErrInvalidExchangeInfo, nativeID)
		}
		closures[nativeID] = struct{}{}
	}
	result := ComposedExchangeInfo{}
	seenRequests := make(map[string]struct{}, len(pages))
	seenResponses := make(map[[sha256.Size]byte]struct{}, len(pages))
	seenSymbols := make(map[string]struct{})
	foundClosures := make(map[string]struct{}, len(closures))
	var headerIdentity []byte
	for expectedIndex, page := range pages {
		if page.PageIndex != expectedIndex || page.PageCount != pageCount {
			return ComposedExchangeInfo{}, fmt.Errorf("%w: conflicting page identity at position %d", ErrInvalidExchangeInfo, expectedIndex)
		}
		if _, exists := seenRequests[page.Request.RequestID]; exists {
			return ComposedExchangeInfo{}, fmt.Errorf("%w: duplicate request identity %q", ErrInvalidExchangeInfo, page.Request.RequestID)
		}
		seenRequests[page.Request.RequestID] = struct{}{}
		if _, exists := seenResponses[page.RawSHA256]; exists {
			return ComposedExchangeInfo{}, fmt.Errorf("%w: duplicate response identity %s", ErrInvalidExchangeInfo, hex.EncodeToString(page.RawSHA256[:]))
		}
		seenResponses[page.RawSHA256] = struct{}{}
		parsed, err := ParseExchangeInfoPage(page, limits)
		if err != nil {
			return ComposedExchangeInfo{}, err
		}
		if expectedIndex == 0 {
			result.Timezone = parsed.Timezone
			result.ServerTime = parsed.ServerTime
			headerIdentity = parsed.HeaderIdentity
		} else if parsed.Timezone != result.Timezone || parsed.ServerTime != result.ServerTime || !bytes.Equal(parsed.HeaderIdentity, headerIdentity) {
			return ComposedExchangeInfo{}, fmt.Errorf("%w: conflicting full response identity on page %d", ErrInvalidExchangeInfo, page.PageIndex)
		}
		requestBytes, err := capture.MarshalRESTRequestEvidence(page.Request)
		if err != nil {
			return ComposedExchangeInfo{}, err
		}
		responseBytes, err := capture.MarshalRESTResponseEvidence(page.Response)
		if err != nil {
			return ComposedExchangeInfo{}, err
		}
		result.CompletedAtNS = max(result.CompletedAtNS, page.Response.CompletedAtNS)
		result.Evidence = append(result.Evidence, catalog.SyncPageEvidence{
			PageIndex:        page.PageIndex,
			PageCount:        page.PageCount,
			ChannelID:        SpotExchangeInfoChannel,
			RawRecord:        page.RawRecord,
			RequestEvidence:  requestBytes,
			ResponseEvidence: responseBytes,
			RawSHA256:        hex.EncodeToString(page.RawSHA256[:]),
			RawByteLength:    len(page.Raw),
		})
		for _, symbol := range parsed.Symbols {
			if _, exists := seenSymbols[symbol.NativeID]; exists {
				return ComposedExchangeInfo{}, fmt.Errorf("%w: duplicate symbol %q across pages", ErrInvalidExchangeInfo, symbol.NativeID)
			}
			seenSymbols[symbol.NativeID] = struct{}{}
			if _, closeObserved := closures[symbol.NativeID]; closeObserved {
				if symbol.Status != "HALT" && symbol.Status != "BREAK" {
					return ComposedExchangeInfo{}, fmt.Errorf("%w: closure %q lacks observed HALT or BREAK status", ErrInvalidExchangeInfo, symbol.NativeID)
				}
				symbol.Candidate.LifecycleClosure = true
				foundClosures[symbol.NativeID] = struct{}{}
			}
			result.Symbols = append(result.Symbols, symbol)
			result.Candidates = append(result.Candidates, symbol.Candidate)
		}
	}
	for nativeID := range closures {
		if _, found := foundClosures[nativeID]; !found {
			return ComposedExchangeInfo{}, fmt.Errorf("%w: closure %q is absent from the captured response", ErrInvalidExchangeInfo, nativeID)
		}
	}
	slices.SortFunc(result.Symbols, func(a, b Symbol) int { return strings.Compare(a.NativeID, b.NativeID) })
	slices.SortFunc(result.Candidates, func(a, b catalog.InstrumentCandidate) int { return strings.Compare(a.NativeID, b.NativeID) })
	return result, nil
}

func parseSymbol(raw json.RawMessage, limits ParserLimits) (Symbol, error) {
	object, err := decodeObject(raw)
	if err != nil {
		return Symbol{}, err
	}
	nativeID, err := requiredString(object, "symbol", limits.MaxStringBytes)
	if err != nil {
		return Symbol{}, err
	}
	status, err := requiredString(object, "status", limits.MaxStringBytes)
	if err != nil {
		return Symbol{}, err
	}
	baseAsset, err := requiredString(object, "baseAsset", limits.MaxStringBytes)
	if err != nil {
		return Symbol{}, err
	}
	quoteAsset, err := requiredString(object, "quoteAsset", limits.MaxStringBytes)
	if err != nil {
		return Symbol{}, err
	}
	filterBytes, ok := object["filters"]
	if !ok {
		return Symbol{}, errors.New("missing filters")
	}
	var rawFilters []json.RawMessage
	if err := json.Unmarshal(filterBytes, &rawFilters); err != nil {
		return Symbol{}, errors.New("filters must be an array")
	}
	if len(rawFilters) > limits.MaxFilters {
		return Symbol{}, fmt.Errorf("filter count %d exceeds %d", len(rawFilters), limits.MaxFilters)
	}
	filters := make([]Filter, 0, len(rawFilters))
	seenFilterTypes := make(map[string]struct{}, len(rawFilters))
	var priceFilter, lotSize, marketLotSize json.RawMessage
	for i, rawFilter := range rawFilters {
		filter, err := parseFilter(rawFilter, limits)
		if err != nil {
			return Symbol{}, fmt.Errorf("filter %d: %w", i, err)
		}
		if _, exists := seenFilterTypes[filter.Type]; exists {
			return Symbol{}, fmt.Errorf("duplicate filter type %q", filter.Type)
		}
		seenFilterTypes[filter.Type] = struct{}{}
		filters = append(filters, filter)
		switch filter.Type {
		case "PRICE_FILTER":
			priceFilter = filter.Raw
		case "LOT_SIZE":
			lotSize = filter.Raw
		case "MARKET_LOT_SIZE":
			marketLotSize = filter.Raw
		}
	}
	canonicalRaw, err := catalog.CanonicalJSON(raw)
	if err != nil {
		return Symbol{}, err
	}
	tickRules, err := namedRules(map[string]json.RawMessage{"price_filter": priceFilter})
	if err != nil {
		return Symbol{}, err
	}
	lotRules, err := namedRules(map[string]json.RawMessage{"lot_size": lotSize, "market_lot_size": marketLotSize})
	if err != nil {
		return Symbol{}, err
	}
	lifecycle := "quarantined"
	switch status {
	case "TRADING":
		lifecycle = "active"
	case "HALT", "BREAK":
		lifecycle = "disabled"
	}
	payoff, _ := catalog.CanonicalJSON([]byte(`{"price_unit":"quote_asset_per_base_asset","quantity_unit":"base_asset","type":"spot"}`))
	candidate := catalog.InstrumentCandidate{
		NativeID:                nativeID,
		Aliases:                 []string{nativeID},
		Lifecycle:               lifecycle,
		BaseAsset:               baseAsset,
		QuoteAsset:              quoteAsset,
		SettlementAsset:         quoteAsset,
		Kind:                    "spot",
		Payoff:                  payoff,
		Multiplier:              "1",
		TickRules:               tickRules,
		LotRules:                lotRules,
		RawMetadata:             canonicalRaw,
		RawMetadataSHA256:       sha256.Sum256(canonicalRaw),
		NormalizedSchemaVersion: "binance-spot-instrument-v1",
	}
	return Symbol{
		NativeID:   nativeID,
		Status:     status,
		BaseAsset:  baseAsset,
		QuoteAsset: quoteAsset,
		Filters:    filters,
		Raw:        canonicalRaw,
		Candidate:  candidate,
	}, nil
}

type fieldKind string

const (
	fieldDecimal fieldKind = "decimal"
	fieldInteger fieldKind = "integer"
	fieldBoolean fieldKind = "boolean"
)

var filterSpecifications = map[string]map[string]fieldKind{
	"PRICE_FILTER":           {"minPrice": fieldDecimal, "maxPrice": fieldDecimal, "tickSize": fieldDecimal},
	"PERCENT_PRICE":          {"multiplierUp": fieldDecimal, "multiplierDown": fieldDecimal, "avgPriceMins": fieldInteger},
	"PERCENT_PRICE_BY_SIDE":  {"bidMultiplierUp": fieldDecimal, "bidMultiplierDown": fieldDecimal, "askMultiplierUp": fieldDecimal, "askMultiplierDown": fieldDecimal, "avgPriceMins": fieldInteger},
	"LOT_SIZE":               {"minQty": fieldDecimal, "maxQty": fieldDecimal, "stepSize": fieldDecimal},
	"MIN_NOTIONAL":           {"minNotional": fieldDecimal, "applyToMarket": fieldBoolean, "avgPriceMins": fieldInteger},
	"NOTIONAL":               {"minNotional": fieldDecimal, "applyMinToMarket": fieldBoolean, "maxNotional": fieldDecimal, "applyMaxToMarket": fieldBoolean, "avgPriceMins": fieldInteger},
	"ICEBERG_PARTS":          {"limit": fieldInteger},
	"MARKET_LOT_SIZE":        {"minQty": fieldDecimal, "maxQty": fieldDecimal, "stepSize": fieldDecimal},
	"MAX_NUM_ORDERS":         {"maxNumOrders": fieldInteger},
	"MAX_NUM_ALGO_ORDERS":    {"maxNumAlgoOrders": fieldInteger},
	"MAX_NUM_ICEBERG_ORDERS": {"maxNumIcebergOrders": fieldInteger},
	"MAX_POSITION":           {"maxPosition": fieldDecimal},
	"TRAILING_DELTA":         {"minTrailingAboveDelta": fieldInteger, "maxTrailingAboveDelta": fieldInteger, "minTrailingBelowDelta": fieldInteger, "maxTrailingBelowDelta": fieldInteger},
	"MAX_NUM_ORDER_AMENDS":   {"maxNumOrderAmends": fieldInteger},
	"MAX_NUM_ORDER_LISTS":    {"maxNumOrderLists": fieldInteger},
}

func parseFilter(raw json.RawMessage, limits ParserLimits) (Filter, error) {
	object, err := decodeObject(raw)
	if err != nil {
		return Filter{}, err
	}
	filterType, err := requiredString(object, "filterType", limits.MaxStringBytes)
	if err != nil {
		return Filter{}, err
	}
	canonical, err := catalog.CanonicalJSON(raw)
	if err != nil {
		return Filter{}, err
	}
	result := Filter{Type: filterType, Raw: canonical}
	specification, known := filterSpecifications[filterType]
	result.Known = known
	if !known {
		for name := range object {
			if name != "filterType" {
				result.UnknownFields = append(result.UnknownFields, name)
			}
		}
		slices.Sort(result.UnknownFields)
		return result, nil
	}
	for name, kind := range specification {
		rawValue, exists := object[name]
		if !exists {
			return Filter{}, fmt.Errorf("known filter %s is missing %s", filterType, name)
		}
		value, err := exactFieldValue(rawValue, kind, limits)
		if err != nil {
			return Filter{}, fmt.Errorf("known filter %s field %s: %w", filterType, name, err)
		}
		result.ExactFields = append(result.ExactFields, ExactField{Name: name, Kind: string(kind), Value: value})
	}
	for name := range object {
		if name == "filterType" {
			continue
		}
		if _, exists := specification[name]; !exists {
			result.UnknownFields = append(result.UnknownFields, name)
		}
	}
	slices.SortFunc(result.ExactFields, func(a, b ExactField) int { return strings.Compare(a.Name, b.Name) })
	slices.Sort(result.UnknownFields)
	return result, nil
}

func exactFieldValue(raw json.RawMessage, kind fieldKind, limits ParserLimits) (string, error) {
	switch kind {
	case fieldDecimal:
		var value string
		if err := json.Unmarshal(raw, &value); err != nil || !validUnsignedDecimal(value) || len(value) > limits.MaxNumberBytes {
			return "", errors.New("must be an exact decimal JSON string")
		}
		return value, nil
	case fieldInteger:
		var value json.Number
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil || !validUnsignedInteger(value.String()) || len(value.String()) > limits.MaxNumberBytes {
			return "", errors.New("must be a bounded non-negative JSON integer")
		}
		return value.String(), nil
	case fieldBoolean:
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", errors.New("must be a JSON boolean")
		}
		if value {
			return "true", nil
		}
		return "false", nil
	default:
		return "", errors.New("unsupported exact field kind")
	}
}

func namedRules(values map[string]json.RawMessage) (json.RawMessage, error) {
	for key, value := range values {
		if len(value) == 0 {
			delete(values, key)
		}
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return catalog.CanonicalJSON(encoded)
}

func validateCapturedPage(page CapturedPage, limits ParserLimits) error {
	if page.PageCount < 1 || page.PageCount > limits.MaxPages || page.PageIndex < 0 || page.PageIndex >= page.PageCount {
		return fmt.Errorf("%w: invalid page identity %d/%d", ErrInvalidExchangeInfo, page.PageIndex, page.PageCount)
	}
	if err := page.Request.Validate(); err != nil {
		return fmt.Errorf("%w: request evidence: %v", ErrInvalidExchangeInfo, err)
	}
	if err := page.Response.Validate(); err != nil {
		return fmt.Errorf("%w: response evidence: %v", ErrInvalidExchangeInfo, err)
	}
	if page.Request.Method != capture.RESTMethodGET || page.Request.RequestID != page.Response.RequestID {
		return fmt.Errorf("%w: request/response identity mismatch", ErrInvalidExchangeInfo)
	}
	if page.Request.ScheduledAtNS < 0 || page.Request.StartedAtNS < page.Request.ScheduledAtNS || page.Response.CompletedAtNS < page.Request.StartedAtNS {
		return fmt.Errorf("%w: request timing evidence regresses", ErrInvalidExchangeInfo)
	}
	if page.Response.Status != 200 {
		return fmt.Errorf("%w: exchangeInfo response status %d", ErrInvalidExchangeInfo, page.Response.Status)
	}
	for _, parameter := range page.Request.Parameters {
		if parameter.Name != "showPermissionSets" || parameter.Value != "true" {
			return fmt.Errorf("%w: filtered exchangeInfo request %q cannot produce a complete snapshot", ErrInvalidExchangeInfo, parameter.Name)
		}
	}
	if len(page.Raw) == 0 || len(page.Raw) > limits.MaxResponseBytes {
		return fmt.Errorf("%w: raw response length %d is outside bounds", ErrInvalidExchangeInfo, len(page.Raw))
	}
	if sha256.Sum256(page.Raw) != page.RawSHA256 {
		return fmt.Errorf("%w: raw response SHA-256 mismatch", ErrInvalidExchangeInfo)
	}
	if err := page.RawRecord.Validate(); err != nil {
		return fmt.Errorf("%w: raw record coordinate: %v", ErrInvalidExchangeInfo, err)
	}
	if page.RawRecord.EvidenceScope == catalog.RawEvidenceInMemoryProjection &&
		(page.RawRecord.RawSegmentSHA256 != hex.EncodeToString(page.RawSHA256[:]) ||
			page.RawRecord.RawSegmentByteLength != int64(len(page.Raw))) {
		return fmt.Errorf("%w: in-memory raw projection differs from fixture bytes", ErrInvalidExchangeInfo)
	}
	return nil
}

func validateParserLimits(limits ParserLimits) error {
	defaults := DefaultParserLimits()
	values := []struct {
		name    string
		value   int
		maximum int
	}{
		{"response bytes", limits.MaxResponseBytes, defaults.MaxResponseBytes},
		{"pages", limits.MaxPages, defaults.MaxPages},
		{"symbols", limits.MaxSymbols, defaults.MaxSymbols},
		{"filters", limits.MaxFilters, defaults.MaxFilters},
		{"fields per object", limits.MaxFieldsPerObject, defaults.MaxFieldsPerObject},
		{"items per array", limits.MaxItemsPerArray, defaults.MaxItemsPerArray},
		{"tokens", limits.MaxTokens, defaults.MaxTokens},
		{"string bytes", limits.MaxStringBytes, defaults.MaxStringBytes},
		{"number bytes", limits.MaxNumberBytes, defaults.MaxNumberBytes},
		{"nesting", limits.MaxNesting, defaults.MaxNesting},
	}
	for _, bound := range values {
		if bound.value < 1 || bound.value > bound.maximum {
			return fmt.Errorf("%w: parser %s bound %d is invalid", ErrInvalidExchangeInfo, bound.name, bound.value)
		}
	}
	return nil
}

type jsonScanState struct {
	tokens int
}

func validateJSONStructure(raw []byte, limits ParserLimits) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	state := &jsonScanState{}
	if err := scanJSONValue(decoder, 0, limits, state); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExchangeInfo, err)
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("%w: trailing JSON: %v", ErrInvalidExchangeInfo, err)
		}
		return fmt.Errorf("%w: trailing JSON token %v", ErrInvalidExchangeInfo, token)
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int, limits ParserLimits, state *jsonScanState) error {
	if depth > limits.MaxNesting {
		return fmt.Errorf("JSON nesting exceeds %d", limits.MaxNesting)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	state.tokens++
	if state.tokens > limits.MaxTokens {
		return fmt.Errorf("JSON token count exceeds %d", limits.MaxTokens)
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			seen := make(map[string]struct{})
			fields := 0
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				state.tokens++
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				if err := validateJSONString(key, limits.MaxStringBytes); err != nil {
					return fmt.Errorf("object key: %w", err)
				}
				fields++
				if fields > limits.MaxFieldsPerObject {
					return fmt.Errorf("object field count exceeds %d", limits.MaxFieldsPerObject)
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate object key %q", key)
				}
				seen[key] = struct{}{}
				if err := scanJSONValue(decoder, depth+1, limits, state); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("unterminated object")
			}
			state.tokens++
		case '[':
			items := 0
			for decoder.More() {
				items++
				if items > limits.MaxItemsPerArray {
					return fmt.Errorf("array item count exceeds %d", limits.MaxItemsPerArray)
				}
				if err := scanJSONValue(decoder, depth+1, limits, state); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("unterminated array")
			}
			state.tokens++
		default:
			return fmt.Errorf("unexpected delimiter %q", value)
		}
	case string:
		return validateJSONString(value, limits.MaxStringBytes)
	case json.Number:
		if len(value.String()) == 0 || len(value.String()) > limits.MaxNumberBytes {
			return fmt.Errorf("JSON number exceeds %d bytes", limits.MaxNumberBytes)
		}
	case bool, nil:
		return nil
	default:
		return fmt.Errorf("unsupported JSON token %T", value)
	}
	if state.tokens > limits.MaxTokens {
		return fmt.Errorf("JSON token count exceeds %d", limits.MaxTokens)
	}
	return nil
}

func decodeObject(raw []byte) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("must be a JSON object")
	}
	return object, nil
}

func requiredString(object map[string]json.RawMessage, name string, maximum int) (string, error) {
	raw, ok := object[name]
	if !ok {
		return "", fmt.Errorf("%w: missing %s", ErrInvalidExchangeInfo, name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", fmt.Errorf("%w: %s must be a non-empty string", ErrInvalidExchangeInfo, name)
	}
	if err := validateJSONString(value, maximum); err != nil {
		return "", fmt.Errorf("%w: %s: %v", ErrInvalidExchangeInfo, name, err)
	}
	return value, nil
}

func requiredInteger(object map[string]json.RawMessage, name string) (string, error) {
	raw, ok := object[name]
	if !ok {
		return "", fmt.Errorf("%w: missing %s", ErrInvalidExchangeInfo, name)
	}
	var value json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil || !validUnsignedInteger(value.String()) {
		return "", fmt.Errorf("%w: %s must be a non-negative integer", ErrInvalidExchangeInfo, name)
	}
	if _, err := strconv.ParseInt(value.String(), 10, 64); err != nil {
		return "", fmt.Errorf("%w: %s exceeds signed 64-bit bounds", ErrInvalidExchangeInfo, name)
	}
	return value.String(), nil
}

func validateJSONString(value string, maximum int) error {
	if len(value) > maximum || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return errors.New("string is oversized, invalid UTF-8, or contains NUL")
	}
	return nil
}

func validUnsignedInteger(value string) bool {
	if value == "" {
		return false
	}
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func validUnsignedDecimal(value string) bool {
	if value == "" {
		return false
	}
	seenDot := false
	for i := range len(value) {
		switch c := value[i]; {
		case c >= '0' && c <= '9':
		case c == '.' && !seenDot && i > 0 && i < len(value)-1:
			seenDot = true
		default:
			return false
		}
	}
	return true
}

func stringsMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func deterministicBinanceUUID(namespace, identity string) string {
	digest := sha256.Sum256([]byte(namespace + "\x00" + identity))
	return opaqueUUID([16]byte(digest[:16]))
}

func opaqueUUID(value [16]byte) string {
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
