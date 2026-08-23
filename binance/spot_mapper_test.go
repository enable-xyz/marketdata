package binance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	spotTradeProjectionSHA  = "eb15d0dcb17a8c58b42bf5b223c3482ea4a8617d41a6840096a0d205b960c085"
	spotDepthProjectionSHA  = "585c65c80a05e917c2cc35825a6f8d7494afa045772fb516b360a6dfc22a0717"
	spotBookProjectionSHA   = "f30a0ec792c5ca26fe3fff792ea138b272dce6028dbdbdc8c210eb01110a4c1f"
	spotTickerProjectionSHA = "a05bcf6c9a06d2d9267704ee529238a72dd1ca80d43b5a747e48ad1ce089ac5c"
)

func TestSpotMapTradeDepthQuoteAndRawCoordinates(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	tradeBytes := spotMapperFixture(t, "trade.json", spotTradeProjectionSHA)
	depthBytes := spotMapperFixture(t, "depth.json", spotDepthProjectionSHA)
	quoteBytes := spotMapperFixture(t, "book-ticker.json", spotBookProjectionSHA)
	records := []normalize.RawRecord{
		spotMapperRecord(t, tradeBytes, 1, 1_000, []normalize.QualityFlag{normalize.QualitySourceDuplicateCandidate, normalize.QualityClockUncertain}),
		spotMapperRecord(t, depthBytes, 2, 2_000, nil),
		spotMapperRecord(t, quoteBytes, 3, 3_000, nil),
	}
	batch := spotMapperBatch(t, snapshot, records, nil)
	if len(batch.Quarantines) != 0 || len(batch.Rows) != 3 {
		t.Fatalf("Normalize() rows/quarantines = %d/%d, want 3/0", len(batch.Rows), len(batch.Quarantines))
	}
	trade := batch.Rows[0].Trade
	if trade == nil || trade.NativeTradeID != 12345 || trade.AggressorSide != normalize.SideSell || !trade.BuyerIsMaker || !trade.NativeIgnoreFlag {
		t.Fatalf("trade native semantics = %#v", trade)
	}
	if trade.Metadata.InstrumentUID != "8de57b8c-3b83-5332-829f-c9e061bf0879" ||
		trade.Price.Decimal != (normalize.Decimal{Coefficient: "1000000000000000", Scale: 18}) ||
		trade.Amount.Decimal != (normalize.Decimal{Coefficient: "100000000000000000000", Scale: 18}) ||
		trade.Price.Unit != normalize.SpotPriceUnit("BNB", "BTC") || trade.Amount.Unit != normalize.BaseAssetUnit("BNB") {
		t.Fatalf("trade identity/numerics = %#v", trade)
	}
	if trade.Metadata.ExchangeTimeNS != (normalize.OptionalInt64{Value: 1_672_515_782_136_000_000, Valid: true}) ||
		trade.Metadata.SourceEventTimeNS != trade.Metadata.ExchangeTimeNS || trade.Metadata.ExchangeTimeResolution != normalize.ResolutionMillisecond {
		t.Fatalf("trade times = %#v", trade.Metadata)
	}
	wantFlags := []normalize.QualityFlag{normalize.QualityClockUncertain, normalize.QualitySourceDuplicateCandidate}
	if !slices.Equal(trade.Metadata.QualityFlags, wantFlags) {
		t.Fatalf("quality flags = %v, want %v", trade.Metadata.QualityFlags, wantFlags)
	}
	if got := hex.EncodeToString(trade.Metadata.SourceSchemaFingerprint[:]); got != "7105453a70d2005ebc3a228d5e6d667dcb4d2f17353795705ed05c356da0ab10" {
		t.Fatalf("trade structural fingerprint = %s", got)
	}
	assertSpotRawCoordinate(t, trade.Metadata, records[0])

	book := batch.Rows[1].BookUpdate
	if book == nil || book.FirstSequence != 157 || book.LastSequence != 160 || book.PreviousSequence.Valid ||
		book.UpdateKind != normalize.UpdateDelta || book.ReconstructionEligibility != "eligible_with_rest_snapshot_bridge" ||
		len(book.Bids) != 1 || len(book.Asks) != 1 {
		t.Fatalf("book contract = %#v", book)
	}
	if book.Bids[0].LevelOrdinal != 0 || book.Bids[0].Side != normalize.SideBuy || book.Bids[0].Action != normalize.LevelUpsert ||
		book.Bids[0].Price.Decimal.Coefficient != "2400000000000000" || book.Bids[0].Amount.Decimal.Coefficient != "10000000000000000000" ||
		book.Asks[0].Side != normalize.SideSell || book.Asks[0].Price.Decimal.Coefficient != "2600000000000000" ||
		book.Asks[0].Amount.Decimal.Coefficient != "100000000000000000000" {
		t.Fatalf("book source levels = %#v / %#v", book.Bids, book.Asks)
	}
	assertSpotRawCoordinate(t, book.Metadata, records[1])

	quote := batch.Rows[2].Quote
	if quote == nil || quote.NativeSourceRole != "bookTicker_native_bbo" || quote.UpdateID != 400900217 ||
		quote.Metadata.ExchangeTimeNS.Valid || quote.Metadata.ExchangeTimeResolution != normalize.ResolutionAbsent ||
		quote.BidPrice.Decimal.Coefficient != "25351900000000000000" || quote.BidAmount.Decimal.Coefficient != "31210000000000000000" ||
		quote.AskPrice.Decimal.Coefficient != "25365200000000000000" || quote.AskAmount.Decimal.Coefficient != "40660000000000000000" ||
		quote.Metadata.InstrumentUID != "b476f919-650a-501a-bc5c-19339d698f77" {
		t.Fatalf("quote semantics = %#v", quote)
	}
	assertSpotRawCoordinate(t, quote.Metadata, records[2])
}

func TestSpotMapDuplicatePreservationOrderingAndZeroDelete(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	tradeBytes := spotMapperFixture(t, "trade.json", spotTradeProjectionSHA)
	deleteBytes := spotMapperMutation(t, "depth-zero-delete.json", "78828bb57e02af3e4e2ae4418da3e2097738902d9a1de0caf49dc94c2e226db0")
	first := spotMapperRecord(t, tradeBytes, 10, 10_000, nil)
	duplicate := spotMapperRecord(t, tradeBytes, 11, 10_000, nil)
	deletion := spotMapperRecord(t, deleteBytes, 12, 10_000, nil)
	batch := spotMapperBatch(t, snapshot, []normalize.RawRecord{first, duplicate, deletion}, nil)
	if len(batch.Quarantines) != 0 || len(batch.Rows) != 3 {
		t.Fatalf("Normalize() rows/quarantines = %d/%d", len(batch.Rows), len(batch.Quarantines))
	}
	if batch.Rows[0].Common().ArrivalOrdinal != 10 || batch.Rows[1].Common().ArrivalOrdinal != 11 || batch.Rows[2].Common().ArrivalOrdinal != 12 {
		t.Fatalf("stable output arrival order = %d,%d,%d", batch.Rows[0].Common().ArrivalOrdinal, batch.Rows[1].Common().ArrivalOrdinal, batch.Rows[2].Common().ArrivalOrdinal)
	}
	if batch.Rows[0].Common().RawPayloadSHA256 != batch.Rows[1].Common().RawPayloadSHA256 ||
		batch.Rows[0].Common().EventID == batch.Rows[1].Common().EventID || batch.Rows[0].LogicalHash == batch.Rows[1].LogicalHash {
		t.Fatal("duplicate raw payloads did not remain distinct coordinate-owned rows")
	}
	book := batch.Rows[2].BookUpdate
	if book == nil || len(book.Bids) != 1 || len(book.Asks) != 0 || book.Bids[0].Action != normalize.LevelDelete || !book.Bids[0].Amount.Decimal.IsZero() {
		t.Fatalf("zero-delete level = %#v", book)
	}
}

func TestSpotTickerMapRollingWindowStatistics(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	payload := spotMapperFixture(t, "ticker.json", spotTickerProjectionSHA)
	record := spotMapperRecord(t, payload, 4, 4_000, nil)
	batch := spotMapperBatch(t, snapshot, []normalize.RawRecord{record}, nil)
	if len(batch.Quarantines) != 0 || len(batch.Rows) != 1 || batch.Rows[0].Ticker == nil {
		t.Fatalf("Normalize() rows/quarantines = %d/%d", len(batch.Rows), len(batch.Quarantines))
	}
	ticker := batch.Rows[0].Ticker
	if ticker.NativeSourceRole != "24hrTicker_statistics_not_bbo" || ticker.WindowKind != normalize.WindowRolling24Hours ||
		ticker.WindowOpenSemantics != "native_statistics_open_time" || ticker.WindowCloseSemantics != "native_statistics_close_time" ||
		ticker.WindowOpenTimeNS != 0 || ticker.WindowCloseTimeNS != 86_400_000_000_000 || ticker.NominalWindowDurationNS != 86_400_000_000_000 {
		t.Fatalf("ticker window semantics = %#v", ticker)
	}
	want := map[string]string{
		"change": "1500000000000000", "percent": "25000000000", "weighted": "1800000000000000",
		"before": "900000000000000", "last": "2500000000000000", "last_amount": "10000000000000000000",
		"bid": "2400000000000000", "bid_amount": "10000000000000000000", "ask": "2600000000000000",
		"ask_amount": "100000000000000000000", "open": "1000000000000000", "high": "2500000000000000",
		"low": "1000000000000000", "base_volume": "10000000000000000000000", "quote_volume": "18000000000000000000",
	}
	got := map[string]string{
		"change": ticker.PriceChange.Decimal.Coefficient, "percent": ticker.PriceChangePercent.Decimal.Coefficient,
		"weighted": ticker.WeightedAveragePrice.Decimal.Coefficient, "before": ticker.FirstTradeBeforeWindowPrice.Decimal.Coefficient,
		"last": ticker.LastPrice.Decimal.Coefficient, "last_amount": ticker.LastAmount.Decimal.Coefficient,
		"bid": ticker.NativeBestBidPrice.Decimal.Coefficient, "bid_amount": ticker.NativeBestBidAmount.Decimal.Coefficient,
		"ask": ticker.NativeBestAskPrice.Decimal.Coefficient, "ask_amount": ticker.NativeBestAskAmount.Decimal.Coefficient,
		"open": ticker.OpenPrice.Decimal.Coefficient, "high": ticker.HighPrice.Decimal.Coefficient,
		"low": ticker.LowPrice.Decimal.Coefficient, "base_volume": ticker.BaseVolume.Decimal.Coefficient,
		"quote_volume": ticker.QuoteVolume.Decimal.Coefficient,
	}
	for field, expected := range want {
		if got[field] != expected {
			t.Fatalf("ticker %s coefficient = %s, want %s", field, got[field], expected)
		}
	}
	if ticker.FirstTradeID != 0 || ticker.LastTradeID != 18150 || ticker.TradeCount != 18151 ||
		ticker.PriceChangePercent.Decimal.Scale != normalize.CanonicalPercentScale || ticker.QuoteVolume.Unit != normalize.QuoteAssetUnit("BTC") {
		t.Fatalf("ticker IDs/scales/units = %#v", ticker)
	}
	assertSpotRawCoordinate(t, ticker.Metadata, record)
}

func TestSpotMapSourceStatesScaleCoefficientAndArrayBounds(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	fixtures := []struct {
		name, hash string
		class      normalize.FingerprintClass
		wantState  normalize.SourceState
		wantCode   normalize.QuarantineCode
	}{
		{"trade-missing-price.json", "5757377148145d7e426f43e9d6387494fcbd9d8e3a82b8056f9e76660b0dda8f", normalize.FingerprintExact, normalize.SourceMissing, normalize.QuarantineInvalidField},
		{"trade-null-price.json", "b92891e7652b917628a4b7809bd6cc3ba578b0fc7a16513b486b927d35820714", normalize.FingerprintExact, normalize.SourceNull, normalize.QuarantineInvalidField},
		{"trade-empty-price.json", "e3c55cd5076a857db3c935f4172d3bf97232ba2895a371533b826b994969df6c", normalize.FingerprintExact, normalize.SourceEmpty, normalize.QuarantineInvalidField},
		{"trade-excess-scale.json", "b46dfeaeaf1f5bbe64a5d719f9ee6269f98a62a4966f1c82bcb0e8c5c52d1a61", normalize.FingerprintExact, normalize.SourceValue, normalize.QuarantineBounds},
		{"trade-overflow.json", "213b95cc1cd41564c4c7a073693516208f44595160e842114644d046d643a256", normalize.FingerprintExact, normalize.SourceValue, normalize.QuarantineBounds},
		{"depth-malformed-level.json", "2d52e39c2159d71e5c4dc6ccfb16380cccc2320c98072a83399bb0d0dedd2b50", normalize.FingerprintExact, normalize.SourceValue, normalize.QuarantineInvalidField},
	}
	additional := make([]normalize.FingerprintRule, 0, 2)
	for _, fixture := range fixtures[:2] {
		payload := spotMapperMutation(t, fixture.name, fixture.hash)
		additional = appendUniqueSpotRule(t, additional, payload, fixture.class, nil)
	}
	for i, fixture := range fixtures {
		payload := spotMapperMutation(t, fixture.name, fixture.hash)
		record := spotMapperRecord(t, payload, uint64(20+i), int64(20_000+i), nil)
		batch := spotMapperBatch(t, snapshot, []normalize.RawRecord{record}, additional)
		if len(batch.Rows) != 0 || len(batch.Quarantines) != 1 {
			t.Fatalf("%s rows/quarantines = %d/%d", fixture.name, len(batch.Rows), len(batch.Quarantines))
		}
		quarantine := batch.Quarantines[0]
		if quarantine.Code != fixture.wantCode || quarantine.SourceState != fixture.wantState || quarantine.Coordinate != record.Coordinate {
			t.Fatalf("%s quarantine = %#v", fixture.name, quarantine)
		}
	}
	zero := spotMapperMutation(t, "trade-zero.json", "2229eb499d4d6e561707ed37b554fb39a9f54320e0c80841ebcc54a192cb39d0")
	batch := spotMapperBatch(t, snapshot, []normalize.RawRecord{spotMapperRecord(t, zero, 30, 30_000, nil)}, additional)
	if len(batch.Quarantines) != 0 || len(batch.Rows) != 1 || batch.Rows[0].Trade == nil ||
		batch.Rows[0].Trade.NativeTradeID != 0 || !batch.Rows[0].Trade.Price.Decimal.IsZero() || !batch.Rows[0].Trade.Amount.Decimal.IsZero() {
		t.Fatalf("valid source zero = %#v / %#v", batch.Rows, batch.Quarantines)
	}
}

func TestSchemaQuarantineClassificationAdditiveAndReceiveTimeCutover(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	additive := spotMapperMutation(t, "trade-additive.json", "51ea7640b7a9847dfb115b5c41fcc4a0ce389ce101538d0bebb0a4bb4f6edece")
	typeChange := spotMapperMutation(t, "trade-type-change.json", "dbef52052eb76ae8c0d3b0acef6c84abc5c45a8bcf28a215f26d67f91ccea10c")
	unknownRole := spotMapperMutation(t, "unknown-role.json", "834806ca84201010b3b666cd4be9d545ed5ecb324fb8f7efecbdace1b6ab6f68")
	additional := []normalize.FingerprintRule{}
	additional = appendUniqueSpotRule(t, additional, additive, normalize.FingerprintAdditiveHarmless, []string{"z"})
	additional = appendUniqueSpotRule(t, additional, typeChange, normalize.FingerprintTypeOrMeaningChange, nil)
	additional = appendUniqueSpotRule(t, additional, unknownRole, normalize.FingerprintSemanticAdditive, nil)

	mapped := spotMapperBatch(t, snapshot, []normalize.RawRecord{spotMapperRecord(t, additive, 40, 40_000, nil)}, additional)
	if len(mapped.Quarantines) != 0 || len(mapped.Rows) != 1 || !slices.Contains(mapped.Rows[0].Common().QualityFlags, normalize.QualitySchemaAdditiveField) {
		t.Fatalf("classified additive result = %#v", mapped)
	}
	for i, test := range []struct {
		payload []byte
		code    normalize.QuarantineCode
		class   normalize.FingerprintClass
	}{
		{typeChange, normalize.QuarantineTypeMeaningChange, normalize.FingerprintTypeOrMeaningChange},
		{unknownRole, normalize.QuarantineSemanticChange, normalize.FingerprintSemanticAdditive},
		{[]byte(`{"e":"trade","E":1`), normalize.QuarantineSchemaMalformed, normalize.FingerprintUnknown},
		{[]byte(`{"e":"trade","E":1672515782136,"s":"BNBBTC","t":12345,"p":"0.001","q":"100","T":1672515782136,"m":true,"M":true,"y":0}`), normalize.QuarantineSchemaUnknown, normalize.FingerprintUnknown},
	} {
		batch := spotMapperBatch(t, snapshot, []normalize.RawRecord{spotMapperRecord(t, test.payload, uint64(41+i), int64(41_000+i), nil)}, additional)
		if len(batch.Rows) != 0 || len(batch.Quarantines) != 1 || batch.Quarantines[0].Code != test.code || batch.Quarantines[0].FingerprintClass != test.class {
			t.Fatalf("quarantine case %d = %#v", i, batch)
		}
	}

	trade := spotMapperFixture(t, "trade.json", spotTradeProjectionSHA)
	cutover := int64(50_000)
	oldBinding, err := NewSpotMapperBinding(normalize.Hash(snapshot.SHA256), "binance.spot.mapper.cutover.old", 0, normalize.OptionalInt64{Value: cutover, Valid: true}, normalize.ResolutionMillisecond, additional)
	if err != nil {
		t.Fatal(err)
	}
	newBinding, err := NewSpotMapperBinding(normalize.Hash(snapshot.SHA256), "binance.spot.mapper.cutover.new", cutover, normalize.OptionalInt64{}, normalize.ResolutionMillisecond, additional)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := normalize.NewOrchestrator(snapshot, []normalize.BoundMapper{newBinding, oldBinding})
	if err != nil {
		t.Fatal(err)
	}
	before := spotMapperRecord(t, trade, 60, cutover-1, nil)
	after := spotMapperRecord(t, trade, 61, cutover, nil)
	cutoverBatch, err := orchestrator.Normalize([]normalize.RawRecord{before, after})
	if err != nil {
		t.Fatal(err)
	}
	if len(cutoverBatch.Quarantines) != 0 || len(cutoverBatch.Rows) != 2 ||
		cutoverBatch.Rows[0].Common().MapperVersion != "binance.spot.mapper.cutover.old" ||
		cutoverBatch.Rows[1].Common().MapperVersion != "binance.spot.mapper.cutover.new" ||
		cutoverBatch.Rows[0].Common().ExchangeTimeNS != cutoverBatch.Rows[1].Common().ExchangeTimeNS {
		t.Fatalf("receive-time cutover = %#v", cutoverBatch)
	}
}

func TestSpotMapPreservesNativeOrderAcrossWallRegressionAndOpaqueEpochs(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	payload := spotMapperFixture(t, "trade.json", spotTradeProjectionSHA)
	binding, err := NewSpotMapperBinding(normalize.Hash(snapshot.SHA256), SpotMapperVersion, 0, normalize.OptionalInt64{}, normalize.ResolutionMillisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := normalize.NewOrchestrator(snapshot, []normalize.BoundMapper{binding})
	if err != nil {
		t.Fatal(err)
	}
	highEpoch := [16]byte{0xff, 0xee}
	lowEpoch := [16]byte{0x01}
	first := spotMapperRecordEpoch(t, payload, highEpoch, 1, 100, nil)
	equalTimeOtherEpoch := spotMapperRecordEpoch(t, payload, lowEpoch, 1, 100, nil)
	regressedWallTime := spotMapperRecordEpoch(t, payload, highEpoch, 2, 50, nil)
	batch, err := orchestrator.Normalize([]normalize.RawRecord{first, equalTimeOtherEpoch, regressedWallTime})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Quarantines) != 0 || len(batch.Rows) != 3 ||
		batch.Rows[0].Common().EpochID != highEpoch ||
		batch.Rows[1].Common().EpochID != lowEpoch ||
		batch.Rows[2].Common().EpochID != highEpoch ||
		batch.Rows[0].Common().ReceivedTimeNS != 100 ||
		batch.Rows[1].Common().ReceivedTimeNS != 100 ||
		batch.Rows[2].Common().ReceivedTimeNS != 50 {
		t.Fatalf("native replay order changed = %#v", batch)
	}
	if _, err := orchestrator.Normalize([]normalize.RawRecord{regressedWallTime, first}); err == nil {
		t.Fatal("ordinal regression within one source epoch was accepted")
	}
	equalArrival := first
	equalArrival.Envelope.MessageOrdinal++
	equalArrival.Coordinate.MessageOrdinal++
	if _, err := orchestrator.Normalize([]normalize.RawRecord{first, equalArrival}); err == nil {
		t.Fatal("equal arrival ordinal with a different message ordinal was accepted")
	}
}

func TestSpotMapTimestampResolutionBindingDefaultMicrosecondAndOverflow(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	payload := spotMapperFixture(t, "trade.json", spotTradeProjectionSHA)
	record := spotMapperRecord(t, payload, 70, 70_000, nil)
	defaultBatch := spotMapperBatchResolution(t, snapshot, []normalize.RawRecord{record}, SpotMapperTimeResolution(SpotWSConfig{}), nil)
	microBatch := spotMapperBatchResolution(t, snapshot, []normalize.RawRecord{record}, SpotMapperTimeResolution(SpotWSConfig{MicrosecondTime: true}), nil)
	if len(defaultBatch.Rows) != 1 || len(microBatch.Rows) != 1 ||
		defaultBatch.Rows[0].Common().ExchangeTimeNS.Value != 1_672_515_782_136_000_000 ||
		microBatch.Rows[0].Common().ExchangeTimeNS.Value != 1_672_515_782_136_000 ||
		defaultBatch.Rows[0].Common().SourceTimeResolution != normalize.ResolutionMillisecond ||
		microBatch.Rows[0].Common().SourceTimeResolution != normalize.ResolutionMicrosecond ||
		defaultBatch.Rows[0].Common().EventID == microBatch.Rows[0].Common().EventID ||
		defaultBatch.Rows[0].LogicalHash == microBatch.Rows[0].LogicalHash {
		t.Fatalf("timestamp binding semantics default=%#v micro=%#v", defaultBatch, microBatch)
	}
	overflow := bytes.ReplaceAll(payload, []byte("1672515782136"), []byte("18446744073709551615"))
	overflowBatch := spotMapperBatchResolution(t, snapshot, []normalize.RawRecord{spotMapperRecord(t, overflow, 71, 71_000, nil)}, normalize.ResolutionMillisecond, nil)
	if len(overflowBatch.Rows) != 0 || len(overflowBatch.Quarantines) != 1 || overflowBatch.Quarantines[0].Code != normalize.QuarantineBounds {
		t.Fatalf("timestamp overflow = %#v", overflowBatch)
	}
}

func TestSpotTickerMapNativeRollingWindowDurationAndResolution(t *testing.T) {
	snapshot := spotMapperSnapshot(t)
	payload := spotMapperFixture(t, "ticker.json", spotTickerProjectionSHA)
	short := bytes.Replace(payload, []byte(`"C":86400000`), []byte(`"C":86399999`), 1)
	long := bytes.Replace(payload, []byte(`"C":86400000`), []byte(`"C":86400001`), 1)
	for i, value := range [][]byte{short, long} {
		batch := spotMapperBatch(t, snapshot, []normalize.RawRecord{spotMapperRecord(t, value, uint64(80+i), int64(80_000+i), nil)}, nil)
		if len(batch.Rows) != 0 || len(batch.Quarantines) != 1 || batch.Quarantines[0].Code != normalize.QuarantineInvalidField {
			t.Fatalf("ticker non-24h window %d = %#v", i, batch)
		}
	}
	micro := bytes.ReplaceAll(payload, []byte("1672515782136"), []byte("1672515782136000"))
	micro = bytes.Replace(micro, []byte(`"C":86400000`), []byte(`"C":86400000000`), 1)
	microBatch := spotMapperBatchResolution(t, snapshot, []normalize.RawRecord{spotMapperRecord(t, micro, 82, 82_000, nil)}, normalize.ResolutionMicrosecond, nil)
	if len(microBatch.Quarantines) != 0 || len(microBatch.Rows) != 1 || microBatch.Rows[0].Ticker == nil ||
		microBatch.Rows[0].Ticker.WindowCloseTimeNS != 86_400_000_000_000 ||
		microBatch.Rows[0].Ticker.WindowTimeResolution != normalize.ResolutionMicrosecond {
		t.Fatalf("microsecond ticker = %#v", microBatch)
	}
	overflow := bytes.Replace(payload, []byte(`"O":0,"C":86400000`), []byte(`"O":18446744073623151615,"C":18446744073709551615`), 1)
	overflowBatch := spotMapperBatch(t, snapshot, []normalize.RawRecord{spotMapperRecord(t, overflow, 83, 83_000, nil)}, nil)
	if len(overflowBatch.Rows) != 0 || len(overflowBatch.Quarantines) != 1 || overflowBatch.Quarantines[0].Code != normalize.QuarantineBounds {
		t.Fatalf("ticker scaling overflow = %#v", overflowBatch)
	}
}

func spotMapperSnapshot(t *testing.T) catalog.Snapshot {
	t.Helper()
	source, version, channels := SpotCatalogContract()
	candidate := func(native, base, quote string) catalog.InstrumentCandidate {
		raw := json.RawMessage(`{"symbol":"` + native + `"}`)
		return catalog.InstrumentCandidate{
			NativeID: native, Aliases: []string{native}, Lifecycle: "active", BaseAsset: base, QuoteAsset: quote,
			SettlementAsset: quote, Kind: "spot", Payoff: json.RawMessage(`{"kind":"spot"}`), Multiplier: "1",
			TickRules: json.RawMessage(`{}`), LotRules: json.RawMessage(`{}`), RawMetadata: raw,
			RawMetadataSHA256: sha256.Sum256(raw), NormalizedSchemaVersion: "candidate-v1",
		}
	}
	snapshot, err := catalog.BuildFreshSnapshot(source, version, channels, []catalog.InstrumentCandidate{
		candidate("BNBBTC", "BNB", "BTC"), candidate("BNBUSDT", "BNB", "USDT"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func spotMapperRecord(t *testing.T, payload []byte, arrival uint64, received int64, flags []normalize.QualityFlag) normalize.RawRecord {
	return spotMapperRecordEpoch(t, payload, [16]byte{1, 2, 3, 4}, arrival, received, flags)
}

func spotMapperRecordEpoch(t *testing.T, payload []byte, epoch [16]byte, arrival uint64, received int64, flags []normalize.QualityFlag) normalize.RawRecord {
	t.Helper()
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: SpotSourceID, ChannelOrEndpoint: SpotRawChannel,
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true},
		ArrivalOrdinal:  arrival, MessageOrdinal: uint32(arrival % 3), ReceivedWallTimeNS: received,
		ClockEpochID: "spot-mapper-fixture-clock", MonotonicNSSinceClockEpoch: arrival,
		PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved,
		RecorderVersion: "spot-mapper-fixture-recorder-v1",
	}
	envelope.SetRawPayload(payload)
	segment := normalize.Hash(sha256.Sum256([]byte("spot-fixture-segment-v1")))
	record, err := normalize.BindRawRecord(envelope, segment, arrival-1, flags)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func spotMapperBatch(t *testing.T, snapshot catalog.Snapshot, records []normalize.RawRecord, additional []normalize.FingerprintRule) normalize.Batch {
	return spotMapperBatchResolution(t, snapshot, records, normalize.ResolutionMillisecond, additional)
}

func spotMapperBatchResolution(t *testing.T, snapshot catalog.Snapshot, records []normalize.RawRecord, resolution normalize.TimeResolution, additional []normalize.FingerprintRule) normalize.Batch {
	t.Helper()
	binding, err := NewSpotMapperBinding(normalize.Hash(snapshot.SHA256), SpotMapperVersion, 0, normalize.OptionalInt64{}, resolution, additional)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := normalize.NewOrchestrator(snapshot, []normalize.BoundMapper{binding})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := orchestrator.Normalize(records)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func appendUniqueSpotRule(t *testing.T, rules []normalize.FingerprintRule, payload []byte, class normalize.FingerprintClass, fields []string) []normalize.FingerprintRule {
	t.Helper()
	observation, err := normalize.StructuralFingerprint(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range rules {
		if rule.Fingerprint == observation.Fingerprint {
			return rules
		}
	}
	return append(rules, normalize.FingerprintRule{Fingerprint: observation.Fingerprint, Class: class, AllowedUnknownFields: fields})
}

func spotMapperFixture(t *testing.T, name, wantHash string) []byte {
	t.Helper()
	return spotMapperRead(t, "../testdata/binance/spot/synthetic/"+name, wantHash)
}

func spotMapperMutation(t *testing.T, name, wantHash string) []byte {
	t.Helper()
	return spotMapperRead(t, "../testdata/binance/spot/synthetic/mutations/"+name, wantHash)
}

func spotMapperRead(t *testing.T, path, wantHash string) []byte {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := sha256.Sum256(payload)
	if hex.EncodeToString(got[:]) != wantHash {
		t.Fatalf("fixture %s SHA-256 = %x, want %s", path, got, wantHash)
	}
	return payload
}

func assertSpotRawCoordinate(t *testing.T, metadata normalize.Metadata, record normalize.RawRecord) {
	t.Helper()
	if metadata.RawSegmentSHA256 != record.Coordinate.RawSegmentSHA256 || metadata.RawRecordOrdinal != record.Coordinate.RawRecordOrdinal ||
		metadata.RawPayloadSHA256 != record.Coordinate.RawPayloadSHA256 || metadata.EpochID != record.Coordinate.EpochID ||
		metadata.ArrivalOrdinal != record.Coordinate.ArrivalOrdinal || metadata.MessageOrdinal != record.Coordinate.MessageOrdinal ||
		metadata.SourceID != record.Coordinate.SourceID || metadata.ChannelID != record.Coordinate.ChannelID {
		t.Fatalf("normalized raw coordinate = %#v, want %#v", metadata, record.Coordinate)
	}
}
