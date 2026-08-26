package binance

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

func TestUSDMRouting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		stream string
		route  USDMRoute
	}{
		{"btcusdt@depth@100ms", USDMRoutePublic},
		{"btcusdt@bookTicker", USDMRoutePublic},
		{"btcusdt@aggTrade", USDMRouteMarket},
		{"btcusdt@ticker", USDMRouteMarket},
		{"btcusdt@markPrice", USDMRouteMarket},
		{"btcusdt@indexPrice", USDMRouteMarket},
		{"btcusdt@forceOrder", USDMRouteMarket},
	}
	for _, test := range tests {
		t.Run(test.stream, func(t *testing.T) {
			t.Parallel()
			contract, err := USDMValidateRoutedStream(test.route, test.stream)
			if err != nil || contract.Route != test.route || contract.Support != capture.SupportAvailable {
				t.Fatalf("USDMValidateRoutedStream() = %#v, %v", contract, err)
			}
			wrong := USDMRoutePublic
			if test.route == USDMRoutePublic {
				wrong = USDMRouteMarket
			}
			if _, err := USDMValidateRoutedStream(wrong, test.stream); !errors.Is(err, ErrUSDMWrongRoute) {
				t.Fatalf("wrong route error = %v", err)
			}
		})
	}
	if _, err := USDMWebSocketURL(USDMRoutePublic, []string{"btcusdt@markPrice"}); !errors.Is(err, ErrUSDMWrongRoute) {
		t.Fatalf("wrong-route URL error = %v", err)
	}
	if _, err := USDMWebSocketURL(USDMRoute("private"), []string{"btcusdt@depth"}); !errors.Is(err, ErrUSDMPrivateEndpoint) {
		t.Fatalf("private route error = %v", err)
	}
}

func TestUSDMSourceContractsAndSharedRatePool(t *testing.T) {
	t.Parallel()
	if err := USDMPublicSourceContract().Validate(); err != nil {
		t.Fatalf("USDMPublicSourceContract.Validate() error = %v", err)
	}
	if err := USDMMarketSourceContract().Validate(); err != nil {
		t.Fatalf("USDMMarketSourceContract.Validate() error = %v", err)
	}
	for _, operation := range []USDMRESTOperation{USDMRESTExchangeInfo, USDMRESTDepth, USDMRESTOpenInterest} {
		contract, err := USDMRESTSourceContract(operation, 100)
		if err != nil || contract.Validate() != nil {
			t.Fatalf("USDMRESTSourceContract(%s) = %v, validation %v", operation, err, contract.Validate())
		}
	}
	pool, err := NewUSDMRatePool(10, USDMFAPIRefillIntervalNS, 0)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := pool.Acquire(0, USDMRESTExchangeInfo, 0)
	second, _ := pool.Acquire(0, USDMRESTOpenInterest, 0)
	if !first.Allowed || !second.Allowed || first.RemainingTokens != 9 || second.RemainingTokens != 8 {
		t.Fatalf("shared rate decisions = %#v then %#v", first, second)
	}
}

func TestUSDMBookPUContinuity(t *testing.T) {
	t.Parallel()
	book, err := NewUSDMBookSynchronizer("BTCUSDT", 4)
	if err != nil {
		t.Fatal(err)
	}
	first := USDMDepthUpdate{EventType: "depthUpdate", EventTimeMS: 1, TransactionTimeMS: 1, Symbol: "BTCUSDT", FirstUpdateID: 10, FinalUpdateID: 12, PreviousFinalID: 9, Bids: []USDMRawLevel{{Price: "100", Quantity: "2"}}}
	if _, err := book.ApplyUpdate(first); err != nil {
		t.Fatal(err)
	}
	transition, err := book.Seed(USDMDepthSnapshot{LastUpdateID: 10, Bids: []USDMRawLevel{{Price: "100", Quantity: "1"}}})
	if err != nil || transition.State != USDMBookLive {
		t.Fatalf("Seed() = %#v, %v", transition, err)
	}
	gap := USDMDepthUpdate{EventType: "depthUpdate", EventTimeMS: 2, TransactionTimeMS: 2, Symbol: "BTCUSDT", FirstUpdateID: 13, FinalUpdateID: 13, PreviousFinalID: 11}
	transition, err = book.ApplyUpdate(gap)
	if !errors.Is(err, ErrUSDMBookGap) || transition.State != USDMBookGap || transition.ClosedEpoch != 1 {
		t.Fatalf("gap transition = %#v, %v", transition, err)
	}
}

func TestUSDMQuantityDirectionRetention(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"100","q":"0.900","nq":"0.125","f":2,"l":3,"T":1,"m":false}`)
	event, err := ParseUSDMAggregateTrade(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.Quantity != "0.900" || event.NormalQuantityExcludingRPI.Text != "0.125" || event.AggressorSide() != normalize.SideBuy || event.AggregationCadenceCeilingNS != USDMAggregateTradeCeilingNS {
		t.Fatalf("aggregate trade mapping = %#v", event)
	}
}

func TestUSDMTickerAllowsSignedChangeFieldsOnly(t *testing.T) {
	t.Parallel()
	const ticker = `{"e":"24hrTicker","E":1767225600300,"s":"BTCUSDT","p":"-1000.00","P":"-1.075","w":"93500.00","c":"94000.10","Q":"0.010","o":"93000.10","h":"95000.00","l":"92000.00","v":"10000.000","q":"935000000.00","O":1767139200300,"C":1767225600300,"F":100000,"L":200000,"n":100001}`
	event, err := ParseUSDMTicker24h([]byte(ticker))
	if err != nil {
		t.Fatalf("signed 24-hour change fields: %v", err)
	}
	if event.PriceChange != "-1000.00" || event.PriceChangePercent != "-1.075" {
		t.Fatalf("signed 24-hour change fields were not retained: %#v", event)
	}
	negativeVolume := strings.Replace(ticker, `"v":"10000.000"`, `"v":"-10000.000"`, 1)
	if _, err := ParseUSDMTicker24h([]byte(negativeVolume)); !errors.Is(err, ErrUSDMInvalidMarketPayload) {
		t.Fatalf("negative volume error = %v", err)
	}
}

func TestUSDMIndexPriceUpdate(t *testing.T) {
	t.Parallel()
	const receivedTimeNS = int64(1767225600500000000)
	raw := []byte(`{"e":"indexPriceUpdate","E":1767225600400,"s":"BTCUSDT","p":"93998.60000000"}`)
	identity := normalize.InstrumentIdentity{InstrumentUID: "binance-usdm-btcusdt", NativeID: "BTCUSDT", BaseAssetID: "BTC", QuoteAssetID: "USDT"}
	event, err := ParseUSDMIndexPriceUpdate(raw, receivedTimeNS, identity)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := event.Normalized(usdmTestDerivativeMetadata(t, raw, receivedTimeNS))
	if err != nil {
		t.Fatal(err)
	}
	if normalized.NativeSourceRole != string(USDMRoleIndexPrice) || normalized.IndexPrice.State != normalize.SourceValue ||
		normalized.MarkPrice.State != normalize.SourceMissing || normalized.FundingRate.State != normalize.SourceMissing ||
		normalized.SettlementPrice.State != normalize.SourceMissing {
		t.Fatalf("index-price normalization = %#v", normalized)
	}
	current := []byte(`{"E":1787492323001,"s":"BTCUSDT","p":"77488.68739130","e":"IndexUpdate"}`)
	currentEvent, err := ParseUSDMIndexPriceUpdate(current, 1787492323100000000, identity)
	if err != nil || currentEvent.Symbol != identity.NativeID || currentEvent.NativeSourceRole != string(USDMRoleIndexPrice) {
		t.Fatalf("current index-price event = %#v, %v", currentEvent, err)
	}
}

func TestParseUSDMIndexPriceUpdateRejectsMalformedAndSchemaDrift(t *testing.T) {
	t.Parallel()
	identity := normalize.InstrumentIdentity{NativeID: "BTCUSDT", BaseAssetID: "BTC", QuoteAssetID: "USDT"}
	tests := []struct {
		name string
		raw  string
	}{
		{name: "missing index price", raw: `{"e":"indexPriceUpdate","E":1767225600400,"s":"BTCUSDT"}`},
		{name: "numeric index price", raw: `{"e":"indexPriceUpdate","E":1767225600400,"s":"BTCUSDT","p":93998.6}`},
		{name: "zero index price", raw: `{"e":"indexPriceUpdate","E":1767225600400,"s":"BTCUSDT","p":"0"}`},
		{name: "wrong event", raw: `{"e":"markPriceUpdate","E":1767225600400,"s":"BTCUSDT","p":"93998.6"}`},
		{name: "wrong symbol", raw: `{"e":"indexPriceUpdate","E":1767225600400,"s":"ETHUSDT","p":"93998.6"}`},
		{name: "unknown field", raw: `{"e":"indexPriceUpdate","E":1767225600400,"s":"BTCUSDT","p":"93998.6","x":"drift"}`},
		{name: "trailing value", raw: `{"e":"indexPriceUpdate","E":1767225600400,"s":"BTCUSDT","p":"93998.6"} {}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseUSDMIndexPriceUpdate([]byte(test.raw), 1767225600500000000, identity); !errors.Is(err, ErrUSDMInvalidMarketPayload) {
				t.Fatalf("ParseUSDMIndexPriceUpdate() error = %v", err)
			}
		})
	}
	if _, err := ParseUSDMIndexPriceUpdate([]byte(strings.Repeat(" ", USDMMaxRawPayloadBytes+1)), 1767225600500000000, identity); !errors.Is(err, ErrUSDMInvalidMarketPayload) {
		t.Fatalf("oversized ParseUSDMIndexPriceUpdate() error = %v", err)
	}
}

func TestUSDMRPI(t *testing.T) {
	t.Parallel()
	contract, err := USDMStreamContractFor("btcusdt@rpiDepth@500ms")
	if err != nil {
		t.Fatal(err)
	}
	if contract.Route != USDMRoutePublic || contract.Support != capture.SupportAmbiguous || !strings.Contains(contract.CandidateDeclaration, "no live subscription") {
		t.Fatalf("RPI candidate contract = %#v", contract)
	}
	if _, err := USDMValidateRoutedStream(USDMRoutePublic, "btcusdt@rpiDepth@500ms"); !errors.Is(err, ErrUSDMCandidateStream) {
		t.Fatalf("RPI live validation error = %v", err)
	}
}

func TestUSDMFixtureVerifier(t *testing.T) {
	t.Parallel()
	evidence, err := VerifyUSDMFixtures("../testdata/binance/usdm")
	if err != nil {
		t.Fatal(err)
	}
	if evidence.OfficialDerivedCount != 11 || evidence.SyntheticMutationCount != 6 || !evidence.Routing.WrongRouteRejected || !evidence.Book.GapClosedEpoch || !evidence.QuantityQRetained || !evidence.QuantityNQRetained || !evidence.Ticker.IndexPriceStreamChecked || !evidence.OpenInterest.RESTPollObservationChecked || !evidence.Liquidation.CompleteTapeClaimAbsent || !evidence.RPI.SupportRemainsCandidate || evidence.RPI.RoutedPayloadProof {
		t.Fatalf("incomplete USD-M fixture evidence = %#v", evidence)
	}
}

func TestUSDMFixtureVerifierRejectsSemanticIndexPriceCorruption(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS("../testdata/binance/usdm")); err != nil {
		t.Fatal(err)
	}
	mutated := []byte(`{"e":"indexPriceUpdate","E":1767225600400,"s":"BTCUSDT","p":"00000.00000000"}`)
	if err := os.WriteFile(root+"/official/index_price.json", mutated, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := root + "/manifest.json"
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	const originalDigest = "76dcc5d10741656b66fc2b60ad12c03f0f5237ac8060d8cc0345f42397e64e9b"
	mutatedDigest := fmt.Sprintf("%x", sha256.Sum256(mutated))
	updated := strings.Replace(string(manifest), originalDigest, mutatedDigest, 1)
	if updated == string(manifest) {
		t.Fatal("index-price manifest digest was not replaced")
	}
	if err := os.WriteFile(manifestPath, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
	if evidence, err := VerifyUSDMFixtures(root); err == nil {
		t.Fatalf("VerifyUSDMFixtures() accepted semantic corruption: %#v", evidence)
	}
}

func usdmTestDerivativeMetadata(t *testing.T, raw []byte, receivedTimeNS int64) normalize.Metadata {
	t.Helper()
	epoch := [16]byte{1}
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: "binance-usdm", ChannelOrEndpoint: "btcusdt@indexPrice",
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true}, ArrivalOrdinal: 1,
		ExchangeTimeNS:         capture.OptionalInt64{Value: 1767225600400000000, Valid: true},
		ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: receivedTimeNS,
		ClockEpochID: "usdm-index-test", MonotonicNSSinceClockEpoch: 1,
		PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved,
		RecorderVersion: "usdm-test-recorder",
	}
	envelope.SetRawPayload(raw)
	hash := func(value string) normalize.Hash { return normalize.Hash(sha256.Sum256([]byte(value))) }
	record, err := normalize.BindRawRecord(envelope, hash("usdm-index-segment"), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{
		Record: record, SchemaName: normalize.DerivativeTickerSchemaName, SchemaVersion: normalize.DerivativeTickerSchemaVersion,
		InstrumentUID:  "binance-usdm-btcusdt",
		ExchangeTimeNS: normalize.OptionalInt64{Value: 1767225600400000000, Valid: true}, ExchangeTimeResolution: normalize.ResolutionMillisecond,
		SourceEventTimeNS: normalize.OptionalInt64{Value: 1767225600400000000, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond,
		SourceSchemaFingerprint: hash("usdm-index-schema"), MapperVersion: "usdm-index-mapper-v1",
		MapperBindingID: hash("usdm-index-binding"), CatalogSnapshotID: hash("usdm-index-catalog"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}
