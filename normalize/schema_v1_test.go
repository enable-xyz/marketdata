package normalize

import (
	"encoding/hex"
	"slices"
	"strings"
	"testing"
)

func TestSchemaV1RegistryAndRows(t *testing.T) {
	registry := SchemaV1Registry()
	if len(registry) != 9 {
		t.Fatalf("SchemaV1Registry() returned %d descriptors, want 9", len(registry))
	}
	wantNames := []string{
		TradeSchemaName, BookUpdateSchemaName, QuoteSchemaName, TickerSchemaName,
		DerivativeTickerSchemaName, LiquidationSchemaName, OptionSummarySchemaName,
		InstrumentEventSchemaName, SourceHealthSchemaName,
	}
	for i, descriptor := range registry {
		if descriptor.Name != wantNames[i] || descriptor.Version != 1 || descriptor.LogicalEncodingVersion != LogicalEncodingVersion ||
			descriptor.Fingerprint == (Hash{}) || descriptor.Contract == "" {
			t.Fatalf("descriptor %d = %#v", i, descriptor)
		}
		lookedUp, ok := LookupSchema(descriptor.Name, descriptor.Version)
		if !ok || lookedUp != descriptor {
			t.Fatalf("LookupSchema(%q, %d) = %#v, %v", descriptor.Name, descriptor.Version, lookedUp, ok)
		}
	}
	mutated := SchemaV1Registry()
	mutated[0].Name = "mutated"
	if SchemaV1Registry()[0].Name != TradeSchemaName {
		t.Fatal("caller mutated the package schema registry")
	}
	if err := ValidateSchemaRegistry(append(registry, registry[0])); err == nil {
		t.Fatal("duplicate schema identity was accepted")
	}
	if _, ok := LookupSchema(TradeSchemaName, 2); ok {
		t.Fatal("unregistered schema version was accepted")
	}

	rows := schemaV1Rows(t)
	if len(rows) != len(registry) {
		t.Fatalf("constructed %d rows, want %d", len(rows), len(registry))
	}
	seenHashes := make(map[Hash]struct{}, len(rows))
	for i, row := range rows {
		if err := row.Validate(); err != nil {
			t.Fatalf("row %d (%s): %v", i, row.Kind, err)
		}
		common := row.Common()
		if common.SchemaName != registry[i].Name || common.SchemaVersion != registry[i].Version ||
			common.RawSegmentSHA256 != schemaTestHash(0x11) || common.RawRecordOrdinal != 17 ||
			common.RawPayloadSHA256 != schemaTestHash(0x22) || common.SourceSchemaFingerprint != schemaTestHash(0x33) {
			t.Fatalf("row %d lost common raw provenance: %#v", i, common)
		}
		if _, duplicate := seenHashes[row.LogicalHash]; duplicate {
			t.Fatalf("row %d reused logical hash %x", i, row.LogicalHash)
		}
		seenHashes[row.LogicalHash] = struct{}{}
		copy := row
		if err := copy.Validate(); err != nil || copy.LogicalHash != row.LogicalHash {
			t.Fatalf("row %d did not round-trip by value: hash=%x err=%v", i, copy.LogicalHash, err)
		}
	}

	commonCopy := rows[0].Common()
	commonCopy.QualityFlags[0] = QualityFlag("caller_mutation")
	if err := rows[0].Validate(); err != nil || rows[0].Common().QualityFlags[0] == QualityFlag("caller_mutation") {
		t.Fatalf("Common exposed row-owned quality flags: %v", err)
	}

	badMetadata := schemaTestMetadata(TradeSchemaName, 2, "instrument-v1")
	if err := badMetadata.Validate(); err == nil {
		t.Fatal("metadata with unregistered schema version was accepted")
	}
	mismatch := rows[0]
	mismatch.Kind = EventTicker
	if err := mismatch.validateEvent(); err == nil {
		t.Fatal("row kind/event mismatch was accepted")
	}
}

func TestSchemaV1PartialStateAgeAndSourceDistinctions(t *testing.T) {
	provenance := schemaTestProvenance()
	metadata := schemaTestMetadata(DerivativeTickerSchemaName, DerivativeTickerSchemaVersion, "instrument-v1")
	event := DerivativeTickerV1{
		Metadata: metadata, NativeSourceRole: "partial_snapshot",
		LastPrice:       NumericField{State: SourceNull, Provenance: provenance},
		MarkPrice:       NumericField{State: SourceEmpty, Provenance: provenance},
		IndexPrice:      NumericField{State: SourceMissing},
		FundingRate:     NumericField{State: SourceValue, Value: Numeric{Decimal: Decimal{Coefficient: "0", Scale: 18}, Unit: RateUnit()}, Provenance: provenance},
		NextFundingTime: TimeField{State: SourceMissing, Resolution: ResolutionAbsent},
		SettlementPrice: NumericField{State: SourceMissing},
		Basis:           NumericField{State: SourceMissing},
		Premium:         NumericField{State: SourceMissing},
	}
	row, err := NewDerivativeTickerRow(event)
	if err != nil {
		t.Fatal(err)
	}
	if !event.LastPrice.Provenance.AgeNS.Valid || event.LastPrice.Provenance.AgeNS.Value != 7 ||
		row.DerivativeTicker.LastPrice.State != SourceNull || row.DerivativeTicker.MarkPrice.State != SourceEmpty ||
		row.DerivativeTicker.IndexPrice.State != SourceMissing || row.DerivativeTicker.FundingRate.State != SourceValue ||
		!row.DerivativeTicker.FundingRate.Value.Decimal.IsZero() {
		t.Fatalf("partial state or age was lost: %#v", row.DerivativeTicker)
	}
	variants := []DerivativeTickerV1{event, event, event, event}
	variants[0].LastPrice = NumericField{State: SourceMissing}
	variants[1].LastPrice = NumericField{State: SourceNull, Provenance: provenance}
	variants[2].LastPrice = NumericField{State: SourceEmpty, Provenance: provenance}
	variants[3].LastPrice = NumericField{State: SourceValue, Value: Numeric{Decimal: Decimal{Coefficient: "0", Scale: 18}, Unit: SpotPriceUnit("BTC", "USD")}, Provenance: provenance}
	hashes := make([]Hash, len(variants))
	for i, variant := range variants {
		variant.Metadata = schemaTestMetadata(DerivativeTickerSchemaName, DerivativeTickerSchemaVersion, "instrument-v1")
		got, err := NewDerivativeTickerRow(variant)
		if err != nil {
			t.Fatalf("variant %d: %v", i, err)
		}
		hashes[i] = got.LogicalHash
	}
	for i := range hashes {
		for j := i + 1; j < len(hashes); j++ {
			if hashes[i] == hashes[j] {
				t.Fatalf("source states %d and %d hashed identically", i, j)
			}
		}
	}
	bad := event
	bad.LastPrice = NumericField{State: SourceNull}
	if _, err := NewDerivativeTickerRow(bad); err == nil {
		t.Fatal("observed partial field without source time/age was accepted")
	}
}

func TestDecimalVectors(t *testing.T) {
	type vector struct {
		name   string
		text   string
		scale  uint8
		unit   Unit
		native NativeUnit
		want   string
	}
	vectors := []vector{
		{name: "spot-quote-per-base", text: "1.25", scale: 18, unit: SpotPriceUnit("BTC", "USDT"), want: "0000001331323530303030303030303030303030303030120000001a71756f74655f61737365745f7065725f626173655f617373657400000000000000034254430000000455534454"},
		{name: "base-asset", text: "2", scale: 18, unit: BaseAssetUnit("BTC"), want: "0000001332303030303030303030303030303030303030120000000a626173655f6173736574000000034254430000000000000000"},
		{name: "quote-asset-usd", text: "3.5", scale: 18, unit: QuoteAssetUnit("USD"), want: "0000001333353030303030303030303030303030303030120000000b71756f74655f6173736574000000035553440000000000000000"},
		{name: "rate", text: "-0.0001", scale: 18, unit: RateUnit(), want: "000000102d313030303030303030303030303030120000000472617465000000000000000000000000"},
		{name: "implied-volatility", text: "0.625", scale: 18, unit: ImpliedVolatilityUnit(), want: "000000123632353030303030303030303030303030301200000012696d706c6965645f766f6c6174696c697479000000000000000000000000"},
		{name: "contracts", text: "7", scale: 18, native: NativeUnit{Kind: NativeUnitContracts, InstrumentUID: "BTC-OPTION"}, want: "00000013373030303030303030303030303030303030301200000009636f6e747261637473000000000000000a4254432d4f5054494f4e00000000"},
		{name: "native-usd", text: "8.125", scale: 18, native: NativeUnit{Kind: NativeUnitUSD, AssetID: "USD"}, want: "00000013383132353030303030303030303030303030301200000003757364000000035553440000000000000000"},
		{name: "native-base", text: "9", scale: 18, native: NativeUnit{Kind: NativeUnitBaseAsset, AssetID: "BTC"}, want: "0000001339303030303030303030303030303030303030120000000a626173655f6173736574000000034254430000000000000000"},
		{name: "native-quote", text: "10", scale: 18, native: NativeUnit{Kind: NativeUnitQuoteAsset, AssetID: "USDT"}, want: "000000143130303030303030303030303030303030303030120000000b71756f74655f617373657400000004555344540000000000000000"},
		{name: "native-rate", text: "0.01", scale: 18, native: NativeUnit{Kind: NativeUnitRate}, want: "000000113130303030303030303030303030303030120000000472617465000000000000000000000000"},
		{name: "native-iv", text: "0.55", scale: 18, native: NativeUnit{Kind: NativeUnitImpliedVolatility}, want: "000000123535303030303030303030303030303030301200000012696d706c6965645f766f6c6174696c697479000000000000000000000000"},
		{name: "venue-native", text: "11", scale: 18, native: NativeUnit{Kind: NativeUnitVenueUnspecified, VenueLabel: "venue.contractValue"}, want: "000000143131303030303030303030303030303030303030120000001876656e75655f6e61746976655f756e73706563696669656400000000000000000000001376656e75652e636f6e747261637456616c7565"},
	}
	for _, test := range vectors {
		t.Run(test.name, func(t *testing.T) {
			decimal, err := ParseDecimal(test.text, test.scale, DefaultDecimalBounds())
			if err != nil {
				t.Fatal(err)
			}
			var encoder canonicalEncoder
			if test.unit.Kind != "" {
				value := Numeric{Decimal: decimal, Unit: test.unit}
				if err := value.Validate(); err != nil {
					t.Fatal(err)
				}
				encoder.numeric(value)
			} else {
				value := NativeValue{Decimal: decimal, Unit: test.native}
				if err := value.Validate(); err != nil {
					t.Fatal(err)
				}
				encodeNativeValue(&encoder, value)
			}
			got := hex.EncodeToString(encoder.bytes)
			if got != test.want {
				t.Fatalf("canonical encoding = %q, want %q", got, test.want)
			}
		})
	}
	t.Run("leading-zeroes-canonicalize-before-coefficient-bound", func(t *testing.T) {
		got, err := ParseDecimal(strings.Repeat("0", 80)+"1", 0, DefaultDecimalBounds())
		if err != nil {
			t.Fatal(err)
		}
		want := Decimal{Coefficient: "1", Scale: 0}
		if got != want {
			t.Fatalf("ParseDecimal() = %#v, want %#v", got, want)
		}
	})
	t.Run("post-canonical-coefficient-overflow", func(t *testing.T) {
		if _, err := ParseDecimal(strings.Repeat("9", MaxCanonicalCoefficientDigits+1), 0, DefaultDecimalBounds()); err == nil {
			t.Fatal("post-canonical coefficient overflow was accepted")
		}
	})

	for _, text := range []string{"0.0000000000000000001", "123456789012345678901", "NaN", "1e3", "+1", "", ".1", "1."} {
		t.Run("reject-"+text, func(t *testing.T) {
			if _, err := ParseDecimal(text, 18, DefaultDecimalBounds()); err == nil {
				t.Fatalf("ParseDecimal(%q) succeeded", text)
			}
		})
	}
}

func schemaV1Rows(t *testing.T) []Row {
	t.Helper()
	price := Numeric{Decimal: Decimal{Coefficient: "1000000000000000000", Scale: 18}, Unit: SpotPriceUnit("BTC", "USD")}
	amount := Numeric{Decimal: Decimal{Coefficient: "2000000000000000000", Scale: 18}, Unit: BaseAssetUnit("BTC")}
	quoteAmount := Numeric{Decimal: Decimal{Coefficient: "3000000000000000000", Scale: 18}, Unit: QuoteAssetUnit("USD")}
	percent := Numeric{Decimal: Decimal{Coefficient: "400000000", Scale: 8}, Unit: PercentUnit()}
	provenance := schemaTestProvenance()
	missingNumeric := NumericField{State: SourceMissing}
	missingTime := TimeField{State: SourceMissing, Resolution: ResolutionAbsent}
	missingNative := NativeNumericField{State: SourceMissing}
	missingText := TextField{State: SourceMissing}

	rows := make([]Row, 0, 9)
	appendRow := func(row Row, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	appendRow(NewTradeRow(TradeV1{Metadata: schemaTestMetadata(TradeSchemaName, 1, "instrument-v1"), NativeTradeID: 1, AggressorSide: SideBuy, Price: price, Amount: amount, AggregationKind: AggregationSingleMatch, NativeDuplicateStatus: DuplicateUnassessed}))
	appendRow(NewBookUpdateRow(BookUpdateV1{Metadata: schemaTestMetadata(BookUpdateSchemaName, 1, "instrument-v1"), UpdateKind: UpdateDelta, DepthContract: "diff_depth", AggregationContract: "100ms", FirstSequence: 1, LastSequence: 1, Checksum: SourceMissing, AmountSemantics: "absolute_base_asset_quantity", ReconstructionEligibility: "eligible_with_rest_snapshot_bridge"}))
	appendRow(NewQuoteRow(QuoteV1{Metadata: schemaTestMetadata(QuoteSchemaName, 1, "instrument-v1"), NativeSourceRole: "bookTicker_native_bbo", UpdateID: 1, BidPrice: price, BidAmount: amount, AskPrice: price, AskAmount: amount, RPIInclusionState: RPINotApplicable}))
	appendRow(NewTickerRow(TickerV1{Metadata: schemaTestMetadata(TickerSchemaName, 1, "instrument-v1"), NativeSourceRole: "24hrTicker_statistics_not_bbo", WindowKind: WindowRolling24Hours, WindowOpenSemantics: "native_statistics_open_time", WindowCloseSemantics: "native_statistics_close_time", WindowOpenTimeNS: 1, WindowCloseTimeNS: 86_400_000_000_001, WindowTimeResolution: ResolutionMillisecond, NominalWindowDurationNS: 86_400_000_000_000, PriceChange: price, PriceChangePercent: percent, WeightedAveragePrice: price, FirstTradeBeforeWindowPrice: price, LastPrice: price, LastAmount: amount, NativeBestBidPrice: price, NativeBestBidAmount: amount, NativeBestAskPrice: price, NativeBestAskAmount: amount, OpenPrice: price, HighPrice: price, LowPrice: price, BaseVolume: amount, QuoteVolume: quoteAmount, FirstTradeID: 1, LastTradeID: 1, TradeCount: 1}))
	appendRow(NewDerivativeTickerRow(DerivativeTickerV1{Metadata: schemaTestMetadata(DerivativeTickerSchemaName, 1, "instrument-v1"), NativeSourceRole: "state_snapshot", LastPrice: NumericField{State: SourceValue, Value: price, Provenance: provenance}, MarkPrice: missingNumeric, IndexPrice: missingNumeric, FundingRate: NumericField{State: SourceValue, Value: Numeric{Decimal: Decimal{Coefficient: "0", Scale: 18}, Unit: RateUnit()}, Provenance: provenance}, NextFundingTime: missingTime, SettlementPrice: missingNumeric, Basis: missingNumeric, Premium: missingNumeric}))
	appendRow(NewLiquidationRow(LiquidationV1{Metadata: schemaTestMetadata(LiquidationSchemaName, 1, "instrument-v1"), NativeSourceRole: "native_liquidation", NativeRole: LiquidationNativeEvent, Side: SideSell, SideSemantics: LiquidationOrderSide, Amount: NativeValue{Decimal: amount.Decimal, Unit: NativeUnit{Kind: NativeUnitContracts, InstrumentUID: "instrument-v1"}}, Price: NumericField{State: SourceValue, Value: price, Provenance: provenance}, PriceType: LiquidationOrderPrice, Completeness: LiquidationComplete, Window: LiquidationWindow{Selection: LiquidationAllObserved}}))
	appendRow(NewOptionSummaryRow(OptionSummaryV1{Metadata: schemaTestMetadata(OptionSummarySchemaName, 1, "instrument-v1"), NativeSourceRole: "option_snapshot", Instrument: TextField{State: SourceValue, Value: "instrument-v1", Provenance: provenance}, Underlying: TextField{State: SourceValue, Value: "BTC", Provenance: provenance}, Index: TextField{State: SourceValue, Value: "BTC-USD", Provenance: provenance}, Expiry: TimeField{State: SourceValue, ValueNS: 2_000, Resolution: ResolutionMillisecond, Provenance: provenance}, Strike: NumericField{State: SourceValue, Value: price, Provenance: provenance}, CallPut: OptionKindField{State: SourceValue, Value: OptionCall, Provenance: provenance}, BidPrice: NumericField{State: SourceNull, Provenance: provenance}, AskPrice: NumericField{State: SourceEmpty, Provenance: provenance}, LastPrice: NumericField{State: SourceValue, Value: price, Provenance: provenance}, MarkPrice: missingNumeric, BidIV: NumericField{State: SourceValue, Value: Numeric{Decimal: Decimal{Coefficient: "500000000000000000", Scale: 18}, Unit: ImpliedVolatilityUnit()}, Provenance: provenance}, AskIV: missingNumeric, MarkIV: missingNumeric, Delta: missingNative, Gamma: missingNative, Vega: missingNative, Theta: missingNative, Rho: missingNative, OpenInterest: NativeNumericField{State: SourceValue, Value: NativeValue{Decimal: amount.Decimal, Unit: NativeUnit{Kind: NativeUnitContracts, InstrumentUID: "instrument-v1"}}, Provenance: provenance}, Volume: missingNative, ForwardPrice: missingNumeric, UnderlyingPrice: missingNumeric, IndexPrice: missingNumeric}))
	appendRow(NewInstrumentEventRow(InstrumentEventV1{Metadata: schemaTestMetadata(InstrumentEventSchemaName, 1, "instrument-v1"), MetadataGeneration: Uint64Field{State: SourceValue, Value: 0, Provenance: provenance}, NativeStateBefore: InstrumentStateField{State: SourceValue, Value: InstrumentStatePreListing, Provenance: provenance}, NativeStateAfter: InstrumentStateField{State: SourceValue, Value: InstrumentStateListed, Provenance: provenance}, ListingTime: TimeField{State: SourceValue, ValueNS: 1_000, Resolution: ResolutionMillisecond, Provenance: provenance}, ContinuousTradingTime: missingTime, ExpiryTime: missingTime, DeliveryTime: missingTime, DelistingTime: missingTime, TickSize: NumericChange{Old: missingNumeric, New: NumericField{State: SourceValue, Value: price, Provenance: provenance}}, LotSize: NativeNumericChange{Old: missingNative, New: NativeNumericField{State: SourceValue, Value: NativeValue{Decimal: amount.Decimal, Unit: NativeUnit{Kind: NativeUnitBaseAsset, AssetID: "BTC"}}, Provenance: provenance}}, ContractMultiplier: NativeNumericChange{Old: missingNative, New: NativeNumericField{State: SourceValue, Value: NativeValue{Decimal: amount.Decimal, Unit: NativeUnit{Kind: NativeUnitContracts, InstrumentUID: "instrument-v1"}}, Provenance: provenance}}, Payoff: TextChange{Old: missingText, New: TextField{State: SourceValue, Value: "linear", Provenance: provenance}}, OldRawHash: HashField{State: SourceMissing}, NewRawHash: HashField{State: SourceValue, Value: schemaTestHash(0x99), Provenance: provenance}, ResolutionStatus: InstrumentResolutionField{State: SourceValue, Value: InstrumentResolved, Provenance: provenance}}))
	appendRow(NewSourceHealthRow(SourceHealthV1{Metadata: schemaTestMetadata(SourceHealthSchemaName, 1, ""), Dimension: HealthConnect, Scope: HealthScopeSource, Component: "websocket", NativeRole: "market_stream", PreviousStatus: HealthStatusField{State: SourceValue, Value: HealthStatusUp, Provenance: provenance}, CurrentStatus: HealthStatusField{State: SourceValue, Value: HealthStatusDown, Provenance: provenance}, NativePreviousState: HealthTextField{State: SourceMissing}, NativeCurrentState: HealthTextField{State: SourceMissing}, PreviousMeasurement: HealthMeasurementField{State: SourceMissing}, CurrentMeasurement: HealthMeasurementField{State: SourceValue, Value: HealthMeasurement{Decimal: Decimal{Coefficient: "1000000", Scale: 0}, Unit: HealthUnitNanoseconds}, Provenance: provenance}, WindowStart: missingTime, WindowEnd: missingTime, NativeCode: HealthTextField{State: SourceNull, Provenance: provenance}, Detail: HealthTextField{State: SourceEmpty, Provenance: provenance}}))
	return rows
}

func schemaTestMetadata(name string, version uint16, instrument string) Metadata {
	metadata := Metadata{
		EventIDEncodingVersion: EventIDEncodingVersion, SchemaName: name, SchemaVersion: version,
		SourceID: "source-v1", ChannelID: "channel-v1", InstrumentUID: instrument,
		EpochKind: ConnectionEpoch, EpochID: [16]byte{1, 2, 3, 4}, ArrivalOrdinal: 9, MessageOrdinal: 2,
		ExchangeTimeNS: OptionalInt64{Value: 1_000, Valid: true}, ExchangeTimeResolution: ResolutionMillisecond,
		SourceEventTimeNS: OptionalInt64{Value: 1_000, Valid: true}, SourceTimeResolution: ResolutionMillisecond,
		ReceivedTimeNS: 1_007, RawSegmentSHA256: schemaTestHash(0x11), RawRecordOrdinal: 17,
		RawPayloadSHA256: schemaTestHash(0x22), SourceSchemaFingerprint: schemaTestHash(0x33),
		MapperVersion: "mapper-v1", MapperBindingID: schemaTestHash(0x44), CatalogSnapshotID: schemaTestHash(0x55),
		QualityFlags: []QualityFlag{QualityClockUncertain, QualitySourceDuplicateCandidate},
	}
	slices.Sort(metadata.QualityFlags)
	metadata.EventID = eventID(metadata)
	return metadata
}

func schemaTestProvenance() FieldProvenance {
	return FieldProvenance{SourceTimeNS: OptionalInt64{Value: 1_000, Valid: true}, SourceTimeResolution: ResolutionMillisecond, AgeNS: OptionalUint64{Value: 7, Valid: true}}
}

func schemaTestHash(value byte) Hash {
	var hash Hash
	for i := range hash {
		hash[i] = value
	}
	return hash
}
