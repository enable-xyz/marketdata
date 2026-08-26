package binance

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

func TestDerivativeSubscriptionPlansAreDeterministicBoundedAndRoleComplete(t *testing.T) {
	first, err := NewUSDMDerivativeSubscriptionPlan(USDMMarketEndpoint, []string{"ethusdt", "BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewUSDMDerivativeSubscriptionPlan(USDMMarketEndpoint, []string{"btcusdt", "ETHUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.Symbols, []string{"btcusdt", "ethusdt"}) || !slices.Equal(first.Inventory, second.Inventory) || !bytes.Equal(first.Evidence, second.Evidence) {
		t.Fatalf("deterministic plans differ: %+v / %+v", first, second)
	}
	for _, request := range first.Requests {
		if len(request.Streams) == 0 || len(request.Streams) > DerivativeSubscriptionBatchLimit || len(request.Raw) > DerivativeMaxControlMessageBytes || !json.Valid(request.Raw) {
			t.Fatalf("unbounded request = %+v", request)
		}
	}

	public, err := NewUSDMDerivativeSubscriptionPlan(USDMPublicEndpoint, []string{"BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(public.Inventory, []string{"btcusdt@depth@100ms", "btcusdt@bookTicker"}) {
		t.Fatalf("USD-M public inventory = %v", public.Inventory)
	}
	market, err := NewUSDMDerivativeSubscriptionPlan(USDMMarketEndpoint, []string{"BTCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(market.Inventory, []string{
		"btcusdt@aggTrade", "btcusdt@ticker", "btcusdt@markPrice@1s", "btcusdt@indexPrice", "btcusdt@forceOrder",
	}) {
		t.Fatalf("USD-M market inventory = %v", market.Inventory)
	}
	for _, stream := range append(slices.Clone(public.Inventory), market.Inventory...) {
		contract, err := USDMStreamContractFor(stream)
		if err != nil || contract.Support != capture.SupportAvailable || contract.Role == USDMRoleRPIDepthCandidate {
			t.Fatalf("USD-M planned role %q = %+v, %v", stream, contract, err)
		}
	}

	coin, err := NewCoinMDerivativeSubscriptionPlan(CoinMWebSocketEndpoint, []string{"BTCUSD_201225"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(coin.Inventory, []string{
		"btcusd_201225@aggTrade", "btcusd_201225@depth@100ms", "btcusd_201225@bookTicker",
		"btcusd_201225@ticker", "btcusd_201225@markPrice@1s", "!forceOrder@arr",
	}) {
		t.Fatalf("COIN-M inventory = %v", coin.Inventory)
	}
	var evidence derivativePlanEvidence
	if err := json.Unmarshal(coin.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.Product != DerivativeProductCoinM || evidence.Endpoint != CoinMWebSocketEndpoint || evidence.SymbolCount != 1 || evidence.SubscriptionCount != len(coin.Inventory) || len(evidence.InventoryHash) != 64 {
		t.Fatalf("COIN-M evidence identity/count/hash = %+v", evidence)
	}
	merged := evidence.Streams[len(evidence.Streams)-1]
	if merged.Selector != "!forceOrder@arr" || !merged.MergedUniverse || merged.NativeSymbolType != 2 || merged.Role != string(CoinMRoleMergedAllMarket) {
		t.Fatalf("COIN-M merged route evidence = %+v", merged)
	}
	if bytes.Contains(coin.Evidence, []byte(`"payload"`)) {
		t.Fatalf("subscription evidence contains payload bytes: %s", coin.Evidence)
	}
}

func TestDerivativeSubscriptionsAndEndpointsFailClosed(t *testing.T) {
	if _, err := NewUSDMDerivativeSubscriptionPlan("wss://fstream.binance.com/private", []string{"BTCUSDT"}); !errors.Is(err, ErrDerivativeConfiguration) {
		t.Fatalf("private USD-M endpoint error = %v", err)
	}
	if _, err := NewCoinMDerivativeSubscriptionPlan("wss://dstream.binance.com/ws", []string{"BTCUSD_PERP"}); !errors.Is(err, ErrDerivativeConfiguration) {
		t.Fatalf("non-contract COIN-M endpoint error = %v", err)
	}
	if _, err := NewCoinMDerivativeSubscriptionPlan(CoinMWebSocketEndpoint, []string{"BTCUSD_PERP", "btcusd_perp"}); !errors.Is(err, ErrDerivativeConfiguration) {
		t.Fatalf("duplicate symbol error = %v", err)
	}
	tooMany := make([]string, DerivativeMaxSymbols+1)
	for i := range tooMany {
		tooMany[i] = "SYMBOL" + strings.Repeat("X", i)
	}
	if _, err := NewUSDMDerivativeSubscriptionPlan(USDMMarketEndpoint, tooMany); !errors.Is(err, ErrDerivativeBounds) {
		t.Fatalf("symbol bound error = %v", err)
	}

	for _, test := range []struct {
		product  DerivativeProduct
		endpoint string
		want     string
	}{
		{DerivativeProductUSDM, USDMPublicEndpoint, "wss://fstream.binance.com/public/stream"},
		{DerivativeProductUSDM, USDMMarketEndpoint, "wss://fstream.binance.com/market/stream"},
		{DerivativeProductCoinM, CoinMWebSocketEndpoint, "wss://dstream.binance.com/stream"},
	} {
		endpoint, err := derivativeDialEndpoint(test.product, test.endpoint)
		if err != nil || endpoint.String() != test.want {
			t.Fatalf("dial endpoint for %s/%s = %v, %v", test.product, test.endpoint, endpoint, err)
		}
	}
	if _, err := derivativeDialEndpoint(DerivativeProductCoinM, USDMPublicEndpoint); !errors.Is(err, ErrDerivativeConfiguration) {
		t.Fatalf("cross-product endpoint error = %v", err)
	}
}
