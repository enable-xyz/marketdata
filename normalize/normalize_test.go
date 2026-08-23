package normalize

import (
	"encoding/hex"
	"testing"
)

func TestSpotMapCanonicalDecimalCheckedBounds(t *testing.T) {
	for _, test := range []struct {
		name    string
		text    string
		scale   uint8
		want    Decimal
		wantErr bool
	}{
		{name: "signed exact", text: "-1.2300", scale: 8, want: Decimal{Coefficient: "-123000000", Scale: 8}},
		{name: "zero canonical", text: "-0", scale: 18, want: Decimal{Coefficient: "0", Scale: 18}},
		{name: "wide checked coefficient", text: "12345678901234567890", scale: 18, want: Decimal{Coefficient: "12345678901234567890000000000000000000", Scale: 18}},
		{name: "excess scale", text: "0.0000000000000000001", scale: 18, wantErr: true},
		{name: "coefficient overflow", text: "123456789012345678901", scale: 18, wantErr: true},
		{name: "binary exponent forbidden", text: "1e-3", scale: 18, wantErr: true},
		{name: "leading plus forbidden", text: "+1", scale: 18, wantErr: true},
		{name: "empty distinct invalid", text: "", scale: 18, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseDecimal(test.text, test.scale, DefaultDecimalBounds())
			if (err != nil) != test.wantErr {
				t.Fatalf("ParseDecimal() error = %v, wantErr %v", err, test.wantErr)
			}
			if err == nil && got != test.want {
				t.Fatalf("ParseDecimal() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestSpotMapFixedEventAndLogicalHashes(t *testing.T) {
	metadata := Metadata{
		EventIDEncodingVersion: EventIDEncodingVersion, SchemaName: TradeSchemaName, SchemaVersion: TradeSchemaVersion,
		SourceID: "source-fixed", ChannelID: "channel-fixed", InstrumentUID: "instrument-fixed",
		EpochKind: ConnectionEpoch, EpochID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		ArrivalOrdinal: 7, MessageOrdinal: 2,
		ExchangeTimeNS: OptionalInt64{Value: 1_234_567_890, Valid: true}, ExchangeTimeResolution: ResolutionMillisecond,
		SourceEventTimeNS: OptionalInt64{Value: 1_234_567_000, Valid: true}, SourceTimeResolution: ResolutionMillisecond,
		ReceivedTimeNS:   1_234_567_999,
		RawSegmentSHA256: repeatedHash(0x11), RawRecordOrdinal: 5, RawPayloadSHA256: repeatedHash(0x22),
		SourceSchemaFingerprint: repeatedHash(0x33), MapperVersion: "mapper-fixed-v1",
		MapperBindingID: repeatedHash(0x44), CatalogSnapshotID: repeatedHash(0x55),
		QualityFlags: []QualityFlag{QualityClockUncertain, QualitySourceDuplicateCandidate},
	}
	metadata.EventID = eventID(metadata)
	row, err := NewTradeRow(TradeV1{
		Metadata: metadata, NativeTradeID: 42, AggressorSide: SideSell, BuyerIsMaker: true,
		Price:           Numeric{Decimal: Decimal{Coefficient: "1234500000000000000", Scale: 18}, Unit: SpotPriceUnit("BTC", "USDT")},
		Amount:          Numeric{Decimal: Decimal{Coefficient: "2500000000000000000", Scale: 18}, Unit: BaseAssetUnit("BTC")},
		AggregationKind: AggregationSingleMatch, NativeDuplicateStatus: DuplicateUnassessed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(metadata.EventID[:]); got != "12958bb7c1f1942fcff250159da30b2979203e37db748d8f5663500fe79b945b" {
		t.Fatalf("event ID = %s", got)
	}
	if got := hex.EncodeToString(row.LogicalHash[:]); got != "5bed7791403f6825b05c9b6db937ff105036d73ef25170d94581002ca02e0d2c" {
		t.Fatalf("logical row hash = %s", got)
	}
}

func TestLiquidationWindowSelectionIsClosed(t *testing.T) {
	window := LiquidationWindow{Selection: LiquidationWindowSelection("typo")}
	if err := window.Validate(LiquidationPartialNonchronological); err == nil {
		t.Fatal("undefined liquidation window selection was accepted")
	}
	window.Selection = LiquidationWindowSelectionUnknown
	if err := window.Validate(LiquidationPartialNonchronological); err != nil {
		t.Fatalf("explicit unknown source selection was rejected: %v", err)
	}
}

func repeatedHash(value byte) Hash {
	var hash Hash
	for i := range hash {
		hash[i] = value
	}
	return hash
}
