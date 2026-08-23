package binance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/normalize"
)

func TestCoinMSourceIdentityAndDAPIContract(t *testing.T) {
	t.Parallel()
	if CoinMSourceID == USDMSourceID {
		t.Fatal("COIN-M source aliases USD-M")
	}
	contract := CoinMSourceContract()
	if err := contract.Validate(); err != nil {
		t.Fatalf("source contract: %v", err)
	}
	if contract.SourceID != CoinMSourceID || !strings.Contains(contract.APIVersion, "DAPI v1") {
		t.Fatalf("source contract identity = %#v", contract)
	}
	rest, err := CoinMRESTSourceContract(CoinMRESTOpenInterest, 0)
	if err != nil {
		t.Fatalf("REST contract: %v", err)
	}
	if rest.SourceID != CoinMSourceID || rest.ContractID != "binance.coinm.rest.current_open_interest_poll.v1" {
		t.Fatalf("REST identity = %#v", rest)
	}
	url, err := CoinMWebSocketURL([]string{"!ticker@arr", "btcusd_perp@aggTrade"})
	if err != nil || !strings.HasPrefix(url, CoinMWebSocketEndpoint+"/stream?") {
		t.Fatalf("websocket URL = %q, %v", url, err)
	}
}

func TestCoinMMergedST2RoutingAndInconsistency(t *testing.T) {
	t.Parallel()
	routes, err := RouteCoinMMergedRecords(coinMTestFixture(t, "official/merged_routing.json"))
	if err != nil || len(routes) != 2 {
		t.Fatalf("routes = %#v, %v", routes, err)
	}
	if routes[0].NativeSymbolType != 2 || routes[0].Route != CoinMMergedRouteCoinM || routes[0].SourceID != CoinMSourceID {
		t.Fatalf("st=2 route = %#v", routes[0])
	}
	if routes[1].NativeSymbolType != 1 || routes[1].Route != CoinMMergedRouteUSDM || routes[1].SourceID != USDMSourceID {
		t.Fatalf("st=1 route = %#v", routes[1])
	}
	inconsistentRaw := coinMTestFixture(t, "official/merged_inconsistency.json")
	inconsistent, err := RouteCoinMMergedRecords(inconsistentRaw)
	if err != nil || len(inconsistent) != 1 || inconsistent[0].Route != CoinMMergedRouteRejected || inconsistent[0].Rejection != "st_symbol_family_inconsistency" {
		t.Fatalf("inconsistency = %#v, %v", inconsistent, err)
	}
	var wrapper struct {
		Data []json.RawMessage `json:"data"`
	}
	if json.Unmarshal(inconsistentRaw, &wrapper) != nil || len(wrapper.Data) != 1 || string(inconsistent[0].Raw) != string(wrapper.Data[0]) {
		t.Fatal("rejected official bytes were not retained exactly")
	}
}
func TestCoinMMergedSelectorEventTypesRejectBeforeSTRouting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		selector string
		expected string
	}{
		{selector: "!ticker@arr", expected: "24hrTicker"},
		{selector: "!miniTicker@arr", expected: "24hrMiniTicker"},
		{selector: "!bookTicker", expected: "bookTicker"},
		{selector: "!forceOrder@arr", expected: "forceOrder"},
		{selector: "!contractInfo", expected: "contractInfo"},
		{selector: "!markPrice@arr", expected: "markPriceUpdate"},
		{selector: "!markPrice@arr@1s", expected: "markPriceUpdate"},
	}
	for _, test := range tests {
		t.Run(test.selector, func(t *testing.T) {
			record := map[string]any{"e": test.expected + "_wrong", "s": "BTCUSD_PERP", "ps": "BTCUSD", "st": 0}
			payload, err := json.Marshal(struct {
				Stream string           `json:"stream"`
				Data   []map[string]any `json:"data"`
			}{Stream: test.selector, Data: []map[string]any{record}})
			if err != nil {
				t.Fatal(err)
			}
			decisions, err := RouteCoinMMergedRecords(payload)
			if err != nil || len(decisions) != 1 {
				t.Fatalf("decisions = %#v, %v", decisions, err)
			}
			if decisions[0].Route != CoinMMergedRouteRejected || decisions[0].Rejection != "stream_event_type_inconsistency" {
				t.Fatalf("event mismatch decision = %#v", decisions[0])
			}
			var wrapper struct {
				Data []json.RawMessage `json:"data"`
			}
			if json.Unmarshal(payload, &wrapper) != nil || len(wrapper.Data) != 1 || string(decisions[0].Raw) != string(wrapper.Data[0]) {
				t.Fatal("event-mismatch bytes were not retained exactly")
			}
		})
	}
}

func TestCoinMNativeUnitsTemporalConversionAndFunding(t *testing.T) {
	t.Parallel()
	evidence, err := VerifyCoinMFixtures(filepath.Join("..", "testdata", "binance", "coinm"))
	if err != nil {
		t.Fatalf("VerifyCoinMFixtures: %v", err)
	}
	if !evidence.DistinctSource || !evidence.CoinMST2Routed || !evidence.InconsistencyRejected || !evidence.RejectedRawRetained {
		t.Fatalf("routing/source evidence = %#v", evidence)
	}
	if !evidence.TradeContracts || !evidence.BookContracts || !evidence.BookZeroDelete || !evidence.BBOContracts || !evidence.TickerContractsAndBase || !evidence.OpenInterestContracts {
		t.Fatalf("native-unit evidence = %#v", evidence)
	}
	if !evidence.TemporalVersionsChecked || !evidence.OldVersionRejected || !evidence.ContractSizeChanged || !evidence.InversePayoffRequired {
		t.Fatalf("temporal/payoff evidence = %#v", evidence)
	}
	if evidence.USDNotionalV1.Coefficient != "200000000000000000000" || evidence.BaseNotionalV1.Coefficient != "10000000000000000" || evidence.USDNotionalV2.Coefficient != "20000000000000000000" || evidence.BaseNotionalV2.Coefficient != "1000000000000000" {
		t.Fatalf("conversion evidence = %#v", evidence)
	}
	if !evidence.DeliveryFundingEmpty || !evidence.DeliveryFundingZero || !evidence.EmptyAndZeroDistinct || !evidence.ZeroFundingTimeRetained {
		t.Fatalf("funding evidence = %#v", evidence)
	}
}

func TestCoinMDeliveryFundingEmptyVersusNumericZero(t *testing.T) {
	t.Parallel()
	instrument := normalize.InstrumentIdentity{InstrumentUID: "binance-coinm:BTCUSD_201225:0", NativeID: "BTCUSD_201225", BaseAssetID: "BTC", QuoteAssetID: "USD"}
	receivedNS := int64(1596095725000) * 1_000_000
	empty, err := ParseCoinMDerivativeTicker(coinMTestFixture(t, "official/delivery_funding_empty.json"), receivedNS, instrument)
	if err != nil {
		t.Fatalf("empty funding: %v", err)
	}
	zero, err := ParseCoinMDerivativeTicker(coinMTestFixture(t, "synthetic/delivery_funding_zero.json"), receivedNS, instrument)
	if err != nil {
		t.Fatalf("zero funding: %v", err)
	}
	if empty.FundingRate.State != normalize.SourceEmpty || empty.FundingRate.Value != (normalize.Numeric{}) {
		t.Fatalf("empty funding = %#v", empty.FundingRate)
	}
	if zero.FundingRate.State != normalize.SourceValue || !zero.FundingRate.Value.Decimal.IsZero() {
		t.Fatalf("zero funding = %#v", zero.FundingRate)
	}
	if empty.NextFundingTime.State != normalize.SourceValue || zero.NextFundingTime.State != normalize.SourceValue || empty.NextFundingTime.ValueNS != 0 || zero.NextFundingTime.ValueNS != 0 {
		t.Fatalf("delivery funding times = %#v / %#v", empty.NextFundingTime, zero.NextFundingTime)
	}
}
func TestCoinMAggregateTradeRequiresExactCaseFields(t *testing.T) {
	t.Parallel()
	var base map[string]json.RawMessage
	if err := json.Unmarshal(coinMTestFixture(t, "official/agg_trade.json"), &base); err != nil {
		t.Fatal(err)
	}
	instrument := normalize.InstrumentIdentity{InstrumentUID: "binance-coinm:BTCUSD_PERP:0", NativeID: "BTCUSD_PERP", BaseAssetID: "BTC", QuoteAssetID: "USD"}
	for _, key := range []string{"e", "E", "s", "ps", "st", "a", "p", "q", "f", "l", "T", "m"} {
		t.Run("missing_"+key, func(t *testing.T) {
			object := cloneCoinMRawObject(base)
			delete(object, key)
			payload, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ParseCoinMAggregateTrade(payload, instrument); !errors.Is(err, ErrCoinMInvalidMarketPayload) {
				t.Fatalf("missing %s error = %v", key, err)
			}
		})
	}
	t.Run("E_e_collision", func(t *testing.T) {
		object := cloneCoinMRawObject(base)
		object["e"] = object["E"]
		object["E"] = json.RawMessage(`"aggTrade"`)
		payload, err := json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseCoinMAggregateTrade(payload, instrument); !errors.Is(err, ErrCoinMInvalidMarketPayload) {
			t.Fatalf("E/e collision error = %v", err)
		}
	})
	for _, rename := range []struct {
		name  string
		exact string
		wrong string
	}{{name: "T_to_t", exact: "T", wrong: "t"}, {name: "m_to_M", exact: "m", wrong: "M"}} {
		t.Run(rename.name, func(t *testing.T) {
			object := cloneCoinMRawObject(base)
			object[rename.wrong] = object[rename.exact]
			delete(object, rename.exact)
			payload, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ParseCoinMAggregateTrade(payload, instrument); !errors.Is(err, ErrCoinMInvalidMarketPayload) {
				t.Fatalf("case collision error = %v", err)
			}
		})
	}
}

func TestCoinMAggregateTradeConversionRejectsCrossInstrumentTerms(t *testing.T) {
	t.Parallel()
	instrument := normalize.InstrumentIdentity{InstrumentUID: "binance-coinm:BTCUSD_PERP:0", NativeID: "BTCUSD_PERP", BaseAssetID: "BTC", QuoteAssetID: "USD"}
	trade, err := ParseCoinMAggregateTrade(coinMTestFixture(t, "official/agg_trade.json"), instrument)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ParseCoinMExchangeInfo(coinMTestFixture(t, "official/exchange_info.json"))
	if err != nil {
		t.Fatal(err)
	}
	validFromNS := info.ServerTimeMS * 1_000_000
	terms, err := info.Instruments[0].ContractTerms(instrument.InstrumentUID, "coinm-catalog-v1", validFromNS, normalize.OptionalInt64{}, normalize.CoinMPayoffInverseQuote)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*normalize.CoinMContractTerms)
	}{
		{name: "instrument_uid", mutate: func(value *normalize.CoinMContractTerms) { value.InstrumentUID = "binance-coinm:OTHER:0" }},
		{name: "assets", mutate: func(value *normalize.CoinMContractTerms) { value.BaseAssetID, value.SettlementAssetID = "ETH", "ETH" }},
		{name: "payoff", mutate: func(value *normalize.CoinMContractTerms) { value.Payoff = normalize.CoinMPayoffKind("linear_quote") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			other := terms
			test.mutate(&other)
			if _, err := trade.ContractConversion(validFromNS, other); !errors.Is(err, normalize.ErrInvalidCoinMContract) {
				t.Fatalf("cross-contract error = %v", err)
			}
		})
	}
}

func TestCoinMFixtureVerifierRejectsModifiedReviewedManifestEvenWhenEntriesRehashed(t *testing.T) {
	root, manifest := copyCoinMFixtureCorpus(t)
	entry := coinMManifestEntry(t, manifest, "official.merged_routing")
	payloadPath := filepath.Join(root, filepath.FromSlash(entry.Path))
	payload := []byte(`{"stream":"!ticker@arr","data":[{"e":"24hrTicker","E":1591268262453,"s":"BTCUSD_PERP","ps":"BTCUSD","st":3},{"e":"24hrTicker","E":1591268262453,"s":"BTCUSDT","ps":"BTCUSDT","st":1}]}`)
	if err := os.WriteFile(payloadPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	entry.SHA256, entry.Bytes = hex.EncodeToString(digest[:]), uint32(len(payload))
	writeCoinMTestManifest(t, root, manifest)
	if _, err := VerifyCoinMFixtures(root); !errors.Is(err, ErrCoinMFixtureVerification) {
		t.Fatalf("modified reviewed-manifest error = %v", err)
	}
}

func TestCoinMFixtureVerifierRejectsTraversalSymlinkAndBounds(t *testing.T) {
	t.Run("traversal", func(t *testing.T) {
		root, manifest := copyCoinMFixtureCorpus(t)
		entry := coinMManifestEntry(t, manifest, "official.agg_trade")
		entry.Path = "../agg_trade.json"
		if _, _, err := loadCoinMFixture(root, *entry); !errors.Is(err, ErrCoinMFixtureVerification) {
			t.Fatalf("traversal error = %v", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		root, manifest := copyCoinMFixtureCorpus(t)
		entry := coinMManifestEntry(t, manifest, "official.agg_trade")
		path := filepath.Join(root, filepath.FromSlash(entry.Path))
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(root, "official", "ticker.json"), path); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadCoinMFixture(root, *entry); !errors.Is(err, ErrCoinMFixtureVerification) {
			t.Fatalf("symlink error = %v", err)
		}
	})
	t.Run("manifest_size", func(t *testing.T) {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte(strings.Repeat("x", CoinMMaxManifestBytes+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyCoinMFixtures(root); !errors.Is(err, ErrCoinMFixtureVerification) {
			t.Fatalf("size error = %v", err)
		}
	})
}

func coinMTestFixture(t *testing.T, relative string) []byte {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("..", "testdata", "binance", "coinm", filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func copyCoinMFixtureCorpus(t *testing.T) (string, *coinMFixtureManifest) {
	t.Helper()
	sourceRoot := filepath.Join("..", "testdata", "binance", "coinm")
	manifestBytes, err := os.ReadFile(filepath.Join(sourceRoot, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest coinMFixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, entry := range manifest.Fixtures {
		payload, err := os.ReadFile(filepath.Join(sourceRoot, filepath.FromSlash(entry.Path)))
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(root, filepath.FromSlash(entry.Path))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeCoinMTestManifest(t, root, &manifest)
	return root, &manifest
}

func coinMManifestEntry(t *testing.T, manifest *coinMFixtureManifest, id string) *coinMFixtureManifestEntry {
	t.Helper()
	for i := range manifest.Fixtures {
		if manifest.Fixtures[i].ID == id {
			return &manifest.Fixtures[i]
		}
	}
	t.Fatalf("fixture %s not found", id)
	return nil
}

func writeCoinMTestManifest(t *testing.T, root string, manifest *coinMFixtureManifest) {
	t.Helper()
	payload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
func cloneCoinMRawObject(source map[string]json.RawMessage) map[string]json.RawMessage {
	clone := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
