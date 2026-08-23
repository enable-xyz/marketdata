package hyperliquid

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/enable-xyz/marketdata/catalog"
)

type NativeFieldState string

const (
	NativeMissing NativeFieldState = "missing"
	NativeNull    NativeFieldState = "null"
	NativeEmpty   NativeFieldState = "empty"
	NativeValue   NativeFieldState = "value"
)

type NativeTextField struct {
	State NativeFieldState
	Text  string
}

func (f NativeTextField) Validate() error {
	switch f.State {
	case NativeMissing, NativeNull, NativeEmpty:
		if f.Text != "" {
			return ErrInvalidPayload
		}
	case NativeValue:
		if !validDecimalText(f.Text) {
			return ErrInvalidPayload
		}
	default:
		return ErrInvalidPayload
	}
	return nil
}

type PerpDEX struct {
	Index                    uint32
	Name                     string
	FullName                 string
	Deployer                 string
	OracleUpdater            NativeTextField
	FeeRecipient             NativeTextField
	AssetToStreamingOICap    json.RawMessage
	AssetToFundingMultiplier json.RawMessage
	Raw                      json.RawMessage
	Evidence                 *RawEvidence
}

func (d PerpDEX) Family() Family {
	if d.Index == 0 && d.Name == "" && d.Deployer == catalog.HyperliquidProtocolDeployer {
		return MainPerpetual
	}
	return HIP3
}

func (d PerpDEX) WireDEXName() string { return d.Name }

func ParsePerpDEXs(payload []byte) ([]PerpDEX, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return nil, err
	}
	var entries []json.RawMessage
	if json.Unmarshal(payload, &entries) != nil || len(entries) == 0 || len(entries) > 1024 || string(entries[0]) != "null" {
		return nil, ErrInvalidPayload
	}
	dexs := make([]PerpDEX, len(entries))
	seen := map[string]struct{}{catalog.HyperliquidMainDEX: {}}
	for index, raw := range entries {
		dex, err := parsePerpDEXEntry(uint32(index), raw, evidence)
		if err != nil {
			return nil, fmt.Errorf("%w: perp DEX at ordinal %d", ErrInvalidPayload, index)
		}
		if index > 0 {
			if _, duplicate := seen[dex.Name]; duplicate {
				return nil, fmt.Errorf("%w: duplicate perp DEX %q", ErrInvalidPayload, dex.Name)
			}
			seen[dex.Name] = struct{}{}
		}
		dexs[index] = dex
	}
	return dexs, nil
}

func (d PerpDEX) ValidateEvidence() error {
	if !d.Evidence.Valid() {
		return ErrInvalidPayload
	}
	var entries []json.RawMessage
	if json.Unmarshal(d.Evidence.Bytes(), &entries) != nil || uint64(d.Index) >= uint64(len(entries)) {
		return ErrInvalidPayload
	}
	reparsed, err := parsePerpDEXEntry(d.Index, entries[d.Index], d.Evidence)
	if err != nil || !equalPerpDEX(d, reparsed) {
		return ErrInvalidPayload
	}
	return nil
}

func parsePerpDEXEntry(index uint32, raw json.RawMessage, evidence *RawEvidence) (PerpDEX, error) {
	if index == 0 {
		if string(raw) != "null" {
			return PerpDEX{}, ErrInvalidPayload
		}
		return PerpDEX{Index: 0, Name: "", FullName: "Hyperliquid main perpetual DEX", Deployer: catalog.HyperliquidProtocolDeployer, Raw: slices.Clone(raw), Evidence: evidence}, nil
	}
	var native struct {
		Name                     string          `json:"name"`
		FullName                 string          `json:"fullName"`
		Deployer                 string          `json:"deployer"`
		OracleUpdater            json.RawMessage `json:"oracleUpdater"`
		FeeRecipient             json.RawMessage `json:"feeRecipient"`
		AssetToStreamingOICap    json.RawMessage `json:"assetToStreamingOiCap"`
		AssetToFundingMultiplier json.RawMessage `json:"assetToFundingMultiplier"`
	}
	if json.Unmarshal(raw, &native) != nil || native.Name == "" || len(native.Name) > 64 || !validHyperliquidAddress(native.Deployer) {
		return PerpDEX{}, ErrInvalidPayload
	}
	oracleUpdater, err := parseNullableText(native.OracleUpdater)
	if err != nil {
		return PerpDEX{}, err
	}
	feeRecipient, err := parseNullableText(native.FeeRecipient)
	if err != nil {
		return PerpDEX{}, err
	}
	if len(native.AssetToStreamingOICap) == 0 {
		native.AssetToStreamingOICap = json.RawMessage("null")
	}
	if len(native.AssetToFundingMultiplier) == 0 {
		native.AssetToFundingMultiplier = json.RawMessage("null")
	}
	return PerpDEX{
		Index: index, Name: native.Name, FullName: native.FullName, Deployer: native.Deployer,
		OracleUpdater: oracleUpdater, FeeRecipient: feeRecipient,
		AssetToStreamingOICap: slices.Clone(native.AssetToStreamingOICap), AssetToFundingMultiplier: slices.Clone(native.AssetToFundingMultiplier),
		Raw: slices.Clone(raw), Evidence: evidence,
	}, nil
}

func equalPerpDEX(left, right PerpDEX) bool {
	return left.Index == right.Index && left.Name == right.Name && left.FullName == right.FullName && left.Deployer == right.Deployer &&
		left.OracleUpdater == right.OracleUpdater && left.FeeRecipient == right.FeeRecipient &&
		slices.Equal(left.AssetToStreamingOICap, right.AssetToStreamingOICap) &&
		slices.Equal(left.AssetToFundingMultiplier, right.AssetToFundingMultiplier) &&
		slices.Equal(left.Raw, right.Raw) && left.Evidence.SHA256() == right.Evidence.SHA256()
}

type PerpInstrument struct {
	Identity                 catalog.HyperliquidInstrumentIdentity
	Name                     string
	SizeDecimals             uint8
	MaximumLeverage          uint32
	MarginTableID            uint32
	OnlyIsolated             *bool
	IsDelisted               *bool
	MarginMode               string
	GrowthMode               string
	LastGrowthModeChangeTime string
	Raw                      json.RawMessage
	Evidence                 *RawEvidence
}

type PerpAssetContext struct {
	Identity          catalog.HyperliquidInstrumentIdentity
	Funding           NativeTextField
	OpenInterest      NativeTextField
	PrevDayPrice      NativeTextField
	DayNotionalVolume NativeTextField
	Premium           NativeTextField
	OraclePrice       NativeTextField
	MarkPrice         NativeTextField
	MidPrice          NativeTextField
	ImpactPrices      []NativeTextField
	Raw               json.RawMessage
	Evidence          *RawEvidence
	binding           [sha256.Size]byte
}

type PerpMetadata struct {
	DEX             PerpDEX
	Generation      [sha256.Size]byte
	CollateralToken uint32
	Universe        []PerpInstrument
	Contexts        []PerpAssetContext
	MarginTables    json.RawMessage
	RawMetadata     json.RawMessage
	RawContexts     json.RawMessage
	Evidence        *RawEvidence
}

func ParsePerpMetadata(network Network, dex PerpDEX, generationEvidence catalog.HyperliquidGenerationEvidence, payload []byte) (PerpMetadata, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return PerpMetadata{}, err
	}
	if !validMetadataGenerationEvidence(generationEvidence, payload) {
		return PerpMetadata{}, ErrInvalidPayload
	}
	return parsePerpMetadataObject(network, dex, generationEvidence, payload, evidence)
}

func ParsePerpMetadataAndContexts(network Network, dex PerpDEX, generationEvidence catalog.HyperliquidGenerationEvidence, payload []byte) (PerpMetadata, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return PerpMetadata{}, err
	}
	var pair []json.RawMessage
	if json.Unmarshal(payload, &pair) != nil || len(pair) != 2 {
		return PerpMetadata{}, ErrPositionalMismatch
	}
	if !validPairedGenerationEvidence(generationEvidence, payload, pair) {
		return PerpMetadata{}, ErrInvalidPayload
	}
	metadata, err := parsePerpMetadataObject(network, dex, generationEvidence, pair[0], evidence)
	if err != nil {
		return PerpMetadata{}, err
	}
	var contextRaw []json.RawMessage
	if json.Unmarshal(pair[1], &contextRaw) != nil || len(metadata.Universe) != len(contextRaw) {
		return PerpMetadata{}, ErrPositionalMismatch
	}
	contexts := make([]PerpAssetContext, len(contextRaw))
	for index, raw := range contextRaw {
		context, err := parsePerpContext(metadata.Universe[index].Identity, raw)
		if err != nil {
			return PerpMetadata{}, fmt.Errorf("%w: context ordinal %d: %v", ErrPositionalMismatch, index, err)
		}
		context.Evidence = evidence
		context.binding = perpContextBinding(context)
		contexts[index] = context
	}
	metadata.Contexts = contexts
	metadata.RawContexts = slices.Clone(pair[1])
	return metadata, nil
}

func parsePerpMetadataObject(network Network, dex PerpDEX, generationEvidence catalog.HyperliquidGenerationEvidence, raw json.RawMessage, evidence *RawEvidence) (PerpMetadata, error) {
	if err := network.Validate(); err != nil {
		return PerpMetadata{}, err
	}
	if dex.ValidateEvidence() != nil || !evidence.Valid() || generationEvidence.Validate() != nil {
		return PerpMetadata{}, ErrInvalidPayload
	}
	var meta struct {
		Universe        []json.RawMessage `json:"universe"`
		MarginTables    json.RawMessage   `json:"marginTables"`
		CollateralToken json.RawMessage   `json:"collateralToken"`
	}
	if json.Unmarshal(raw, &meta) != nil || len(meta.Universe) == 0 {
		return PerpMetadata{}, ErrInvalidPayload
	}
	collateral, err := parseUint32(meta.CollateralToken)
	if err != nil {
		return PerpMetadata{}, ErrInvalidPayload
	}
	generation, err := catalog.HyperliquidMetadataGeneration(raw, generationEvidence)
	if err != nil {
		return PerpMetadata{}, err
	}
	universe := make([]PerpInstrument, len(meta.Universe))
	seen := make(map[string]struct{}, len(universe))
	for index, instrumentRaw := range meta.Universe {
		instrument, err := parsePerpInstrument(network, dex, generation, collateral, uint32(index), instrumentRaw)
		if err != nil {
			return PerpMetadata{}, fmt.Errorf("%w: universe ordinal %d: %v", ErrInvalidPayload, index, err)
		}
		if _, duplicate := seen[instrument.Identity.NativeID]; duplicate {
			return PerpMetadata{}, fmt.Errorf("%w: duplicate instrument identity", ErrInvalidPayload)
		}
		seen[instrument.Identity.NativeID] = struct{}{}
		instrument.Evidence = evidence
		universe[index] = instrument
	}
	return PerpMetadata{
		DEX: dex, Generation: generation, CollateralToken: collateral, Universe: universe,
		MarginTables: slices.Clone(meta.MarginTables), RawMetadata: slices.Clone(raw), Evidence: evidence,
	}, nil
}

func parsePerpInstrument(network Network, dex PerpDEX, generation [sha256.Size]byte, collateral, index uint32, raw json.RawMessage) (PerpInstrument, error) {
	var native struct {
		Name                     string  `json:"name"`
		SizeDecimals             *uint8  `json:"szDecimals"`
		MaximumLeverage          *uint32 `json:"maxLeverage"`
		MarginTableID            *uint32 `json:"marginTableId"`
		OnlyIsolated             *bool   `json:"onlyIsolated"`
		IsDelisted               *bool   `json:"isDelisted"`
		MarginMode               string  `json:"marginMode"`
		GrowthMode               string  `json:"growthMode"`
		LastGrowthModeChangeTime string  `json:"lastGrowthModeChangeTime"`
	}
	if json.Unmarshal(raw, &native) != nil || !validCoin(native.Name) || native.SizeDecimals == nil || native.MaximumLeverage == nil || *native.MaximumLeverage == 0 {
		return PerpInstrument{}, ErrInvalidPayload
	}
	family := dex.Family()
	dexIdentity := catalog.HyperliquidMainDEX
	deployer := catalog.HyperliquidProtocolDeployer
	if family == HIP3 {
		dexIdentity = dex.Name
		deployer = dex.Deployer
	}
	identity, err := catalog.NewHyperliquidInstrumentIdentity(catalog.HyperliquidIdentityInput{
		Network: string(network), Family: family, DEXName: dexIdentity, WireCoin: native.Name,
		MetadataGeneration: generation, Deployer: deployer, CollateralToken: strconv.FormatUint(uint64(collateral), 10), UniverseIndex: index,
	})
	if err != nil {
		return PerpInstrument{}, err
	}
	marginTableID := uint32(0)
	if native.MarginTableID != nil {
		marginTableID = *native.MarginTableID
	}
	return PerpInstrument{
		Identity: identity, Name: native.Name, SizeDecimals: *native.SizeDecimals, MaximumLeverage: *native.MaximumLeverage,
		MarginTableID: marginTableID, OnlyIsolated: cloneBool(native.OnlyIsolated), IsDelisted: cloneBool(native.IsDelisted),
		MarginMode: native.MarginMode, GrowthMode: native.GrowthMode, LastGrowthModeChangeTime: native.LastGrowthModeChangeTime, Raw: slices.Clone(raw),
	}, nil
}

func parsePerpContext(identity catalog.HyperliquidInstrumentIdentity, raw json.RawMessage) (PerpAssetContext, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return PerpAssetContext{}, err
	}
	context := PerpAssetContext{Identity: identity, Raw: slices.Clone(raw)}
	orderedFields := []struct {
		key         string
		destination *NativeTextField
	}{
		{key: "funding", destination: &context.Funding},
		{key: "openInterest", destination: &context.OpenInterest},
		{key: "prevDayPx", destination: &context.PrevDayPrice},
		{key: "dayNtlVlm", destination: &context.DayNotionalVolume},
		{key: "premium", destination: &context.Premium},
		{key: "oraclePx", destination: &context.OraclePrice},
		{key: "markPx", destination: &context.MarkPrice},
		{key: "midPx", destination: &context.MidPrice},
	}
	for _, field := range orderedFields {
		*field.destination, err = parseNativeDecimalField(fields, field.key)
		if err != nil {
			return PerpAssetContext{}, err
		}
	}
	if impact, ok := fields["impactPxs"]; ok && string(impact) != "null" {
		var values []json.RawMessage
		if json.Unmarshal(impact, &values) != nil || len(values) > 2 {
			return PerpAssetContext{}, ErrInvalidPayload
		}
		context.ImpactPrices = make([]NativeTextField, len(values))
		for index, value := range values {
			field, err := parseNativeText(value)
			if err != nil || field.Validate() != nil {
				return PerpAssetContext{}, ErrInvalidPayload
			}
			context.ImpactPrices[index] = field
		}
	}
	return context, nil
}

type SpotToken struct {
	Name         string
	SizeDecimals uint8
	WeiDecimals  uint8
	Index        uint32
	TokenID      string
	IsCanonical  bool
	EVMContract  NativeTextField
	FullName     NativeTextField
	Raw          json.RawMessage
	Evidence     *RawEvidence
}

type SpotPair struct {
	Identity     catalog.HyperliquidInstrumentIdentity
	Name         string
	Index        uint32
	TokenIndexes [2]uint32
	BaseToken    SpotToken
	QuoteToken   SpotToken
	IsCanonical  bool
	Raw          json.RawMessage
	Evidence     *RawEvidence
}

type SpotAssetContext struct {
	Identity          catalog.HyperliquidInstrumentIdentity
	DayNotionalVolume NativeTextField
	MarkPrice         NativeTextField
	MidPrice          NativeTextField
	PrevDayPrice      NativeTextField
	CirculatingSupply NativeTextField
	Raw               json.RawMessage
	Evidence          *RawEvidence
	binding           [sha256.Size]byte
}

type SpotMetadata struct {
	Generation  [sha256.Size]byte
	Tokens      []SpotToken
	Universe    []SpotPair
	Contexts    []SpotAssetContext
	RawMetadata json.RawMessage
	RawContexts json.RawMessage
	Evidence    *RawEvidence
}

func ParseSpotMetadata(network Network, generationEvidence catalog.HyperliquidGenerationEvidence, payload []byte) (SpotMetadata, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return SpotMetadata{}, err
	}
	if !validMetadataGenerationEvidence(generationEvidence, payload) {
		return SpotMetadata{}, ErrInvalidPayload
	}
	return parseSpotMetadataObject(network, generationEvidence, payload, evidence)
}

func ParseSpotMetadataAndContexts(network Network, generationEvidence catalog.HyperliquidGenerationEvidence, payload []byte) (SpotMetadata, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return SpotMetadata{}, err
	}
	var pair []json.RawMessage
	if json.Unmarshal(payload, &pair) != nil || len(pair) != 2 {
		return SpotMetadata{}, ErrPositionalMismatch
	}
	if !validPairedGenerationEvidence(generationEvidence, payload, pair) {
		return SpotMetadata{}, ErrInvalidPayload
	}
	metadata, err := parseSpotMetadataObject(network, generationEvidence, pair[0], evidence)
	if err != nil {
		return SpotMetadata{}, err
	}
	var contextRaw []json.RawMessage
	if json.Unmarshal(pair[1], &contextRaw) != nil || len(metadata.Universe) != len(contextRaw) {
		return SpotMetadata{}, ErrPositionalMismatch
	}
	contexts := make([]SpotAssetContext, len(contextRaw))
	for ordinal, raw := range contextRaw {
		context, err := parseSpotContext(metadata.Universe[ordinal].Identity, raw)
		if err != nil {
			return SpotMetadata{}, fmt.Errorf("%w: spot context ordinal %d: %v", ErrPositionalMismatch, ordinal, err)
		}
		context.Evidence = evidence
		context.binding = spotContextBinding(context)
		contexts[ordinal] = context
	}
	metadata.Contexts = contexts
	metadata.RawContexts = slices.Clone(pair[1])
	return metadata, nil
}

func parseSpotMetadataObject(network Network, generationEvidence catalog.HyperliquidGenerationEvidence, raw json.RawMessage, evidence *RawEvidence) (SpotMetadata, error) {
	if err := network.Validate(); err != nil {
		return SpotMetadata{}, err
	}
	if !evidence.Valid() || generationEvidence.Validate() != nil {
		return SpotMetadata{}, ErrInvalidPayload
	}
	var meta struct {
		Tokens   []json.RawMessage `json:"tokens"`
		Universe []json.RawMessage `json:"universe"`
	}
	if json.Unmarshal(raw, &meta) != nil || len(meta.Tokens) == 0 || len(meta.Universe) == 0 {
		return SpotMetadata{}, ErrInvalidPayload
	}
	generation, err := catalog.HyperliquidMetadataGeneration(raw, generationEvidence)
	if err != nil {
		return SpotMetadata{}, err
	}
	tokens := make([]SpotToken, len(meta.Tokens))
	byIndex := make(map[uint32]SpotToken, len(tokens))
	for index, tokenRaw := range meta.Tokens {
		token, err := parseSpotToken(tokenRaw)
		if err != nil || token.Index != uint32(index) {
			return SpotMetadata{}, fmt.Errorf("%w: token ordinal %d", ErrPositionalMismatch, index)
		}
		if _, duplicate := byIndex[token.Index]; duplicate {
			return SpotMetadata{}, ErrPositionalMismatch
		}
		token.Evidence = evidence
		tokens[index] = token
		byIndex[token.Index] = token
	}
	universe := make([]SpotPair, len(meta.Universe))
	for ordinal, pairRaw := range meta.Universe {
		spotPair, err := parseSpotPair(network, generation, uint32(ordinal), pairRaw, byIndex)
		if err != nil {
			return SpotMetadata{}, fmt.Errorf("%w: pair ordinal %d: %v", ErrPositionalMismatch, ordinal, err)
		}
		spotPair.Evidence = evidence
		universe[ordinal] = spotPair
	}
	return SpotMetadata{Generation: generation, Tokens: tokens, Universe: universe, RawMetadata: slices.Clone(raw), Evidence: evidence}, nil
}

func parseSpotToken(raw json.RawMessage) (SpotToken, error) {
	var native struct {
		Name         string          `json:"name"`
		SizeDecimals *uint8          `json:"szDecimals"`
		WeiDecimals  *uint8          `json:"weiDecimals"`
		Index        *uint32         `json:"index"`
		TokenID      string          `json:"tokenId"`
		IsCanonical  *bool           `json:"isCanonical"`
		EVMContract  json.RawMessage `json:"evmContract"`
		FullName     json.RawMessage `json:"fullName"`
	}
	if json.Unmarshal(raw, &native) != nil || !validCoin(native.Name) || native.SizeDecimals == nil || native.WeiDecimals == nil || native.Index == nil || native.IsCanonical == nil || native.TokenID == "" {
		return SpotToken{}, ErrInvalidPayload
	}
	evm, err := parseNullableText(native.EVMContract)
	if err != nil {
		return SpotToken{}, err
	}
	fullName, err := parseNullableText(native.FullName)
	if err != nil {
		return SpotToken{}, err
	}
	return SpotToken{Name: native.Name, SizeDecimals: *native.SizeDecimals, WeiDecimals: *native.WeiDecimals, Index: *native.Index, TokenID: native.TokenID, IsCanonical: *native.IsCanonical, EVMContract: evm, FullName: fullName, Raw: slices.Clone(raw)}, nil
}

func parseSpotPair(network Network, generation [sha256.Size]byte, ordinal uint32, raw json.RawMessage, tokens map[uint32]SpotToken) (SpotPair, error) {
	var native struct {
		Name        string   `json:"name"`
		Tokens      []uint32 `json:"tokens"`
		Index       *uint32  `json:"index"`
		IsCanonical *bool    `json:"isCanonical"`
	}
	if json.Unmarshal(raw, &native) != nil || !validCoin(native.Name) || len(native.Tokens) != 2 || native.Index == nil || *native.Index != ordinal || native.IsCanonical == nil || native.Tokens[0] == native.Tokens[1] {
		return SpotPair{}, ErrInvalidPayload
	}
	base, baseOK := tokens[native.Tokens[0]]
	quote, quoteOK := tokens[native.Tokens[1]]
	if !baseOK || !quoteOK {
		return SpotPair{}, ErrInvalidPayload
	}
	identity, err := catalog.NewHyperliquidInstrumentIdentity(catalog.HyperliquidIdentityInput{
		Network: string(network), Family: Spot, DEXName: catalog.HyperliquidSpotDEX, WireCoin: native.Name,
		MetadataGeneration: generation, Deployer: catalog.HyperliquidProtocolDeployer,
		CollateralToken: strconv.FormatUint(uint64(quote.Index), 10), UniverseIndex: *native.Index,
	})
	if err != nil {
		return SpotPair{}, err
	}
	return SpotPair{Identity: identity, Name: native.Name, Index: *native.Index, TokenIndexes: [2]uint32{native.Tokens[0], native.Tokens[1]}, BaseToken: base, QuoteToken: quote, IsCanonical: *native.IsCanonical, Raw: slices.Clone(raw)}, nil
}

func parseSpotContext(identity catalog.HyperliquidInstrumentIdentity, raw json.RawMessage) (SpotAssetContext, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return SpotAssetContext{}, err
	}
	context := SpotAssetContext{Identity: identity, Raw: slices.Clone(raw)}
	orderedFields := []struct {
		key         string
		destination *NativeTextField
	}{
		{key: "dayNtlVlm", destination: &context.DayNotionalVolume},
		{key: "markPx", destination: &context.MarkPrice},
		{key: "midPx", destination: &context.MidPrice},
		{key: "prevDayPx", destination: &context.PrevDayPrice},
		{key: "circulatingSupply", destination: &context.CirculatingSupply},
	}
	for _, field := range orderedFields {
		*field.destination, err = parseNativeDecimalField(fields, field.key)
		if err != nil {
			return SpotAssetContext{}, err
		}
	}
	return context, nil
}

func decodeObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var values map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &values) != nil || values == nil {
		return nil, ErrInvalidPayload
	}
	return values, nil
}

func parseNativeDecimalField(values map[string]json.RawMessage, key string) (NativeTextField, error) {
	raw, ok := values[key]
	if !ok {
		return NativeTextField{State: NativeMissing}, nil
	}
	field, err := parseNativeText(raw)
	if err != nil || field.Validate() != nil {
		return NativeTextField{}, ErrInvalidPayload
	}
	return field, nil
}

func parseNativeText(raw json.RawMessage) (NativeTextField, error) {
	if len(raw) == 0 {
		return NativeTextField{State: NativeMissing}, nil
	}
	if string(raw) == "null" {
		return NativeTextField{State: NativeNull}, nil
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return NativeTextField{}, ErrInvalidPayload
	}
	if text == "" {
		return NativeTextField{State: NativeEmpty}, nil
	}
	return NativeTextField{State: NativeValue, Text: text}, nil
}

func parseNullableText(raw json.RawMessage) (NativeTextField, error) {
	field, err := parseNativeText(raw)
	if err != nil {
		return NativeTextField{}, err
	}
	if field.State == NativeValue && (len(field.Text) > 256 || strings.IndexByte(field.Text, 0) >= 0) {
		return NativeTextField{}, ErrInvalidPayload
	}
	return field, nil
}

func parseUint32(raw json.RawMessage) (uint32, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, ErrInvalidPayload
	}
	value, err := strconv.ParseUint(string(raw), 10, 32)
	if err != nil {
		return 0, ErrInvalidPayload
	}
	return uint32(value), nil
}

func validDecimalText(text string) bool {
	if text == "" || len(text) > 128 {
		return false
	}
	start := 0
	if text[0] == '-' {
		start = 1
	}
	if start == len(text) || text[start] == '.' || text[len(text)-1] == '.' {
		return false
	}
	dot := false
	for index := start; index < len(text); index++ {
		if text[index] == '.' {
			if dot {
				return false
			}
			dot = true
			continue
		}
		if text[index] < '0' || text[index] > '9' {
			return false
		}
	}
	return true
}

func validHyperliquidAddress(value string) bool {
	if len(value) != 42 || !strings.HasPrefix(value, "0x") || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value[2:] {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func validMetadataGenerationEvidence(evidence catalog.HyperliquidGenerationEvidence, payload []byte) bool {
	return evidence.Validate() == nil &&
		evidence.RawPayloadSHA256 == sha256.Sum256(payload) &&
		evidence.ContextPayloadSHA256 == ([sha256.Size]byte{}) &&
		evidence.EnvelopePayloadSHA256 == ([sha256.Size]byte{})
}

func validPairedGenerationEvidence(evidence catalog.HyperliquidGenerationEvidence, payload []byte, pair []json.RawMessage) bool {
	return len(pair) == 2 && evidence.Validate() == nil &&
		evidence.RawPayloadSHA256 == sha256.Sum256(pair[0]) &&
		evidence.ContextPayloadSHA256 == sha256.Sum256(pair[1]) &&
		evidence.EnvelopePayloadSHA256 == sha256.Sum256(payload)
}
