package catalog_test

import (
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/catalog"
)

func TestSpotCatalogSnapshotDeterministic(t *testing.T) {
	bundle, err := binance.LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	page, err := bundle.CapturedPage("active")
	if err != nil {
		t.Fatal(err)
	}
	composed, err := binance.ComposeExchangeInfo([]binance.CapturedPage{page}, binance.ComposeOptions{}, binance.DefaultParserLimits())
	if err != nil {
		t.Fatal(err)
	}
	source, version, channels := binance.SpotCatalogContract()
	first, err := catalog.BuildFreshSnapshot(source, version, channels, composed.Candidates)
	if err != nil {
		t.Fatal(err)
	}
	reversed := slices.Clone(composed.Candidates)
	slices.Reverse(reversed)
	second, err := catalog.BuildFreshSnapshot(source, version, channels, reversed)
	if err != nil {
		t.Fatal(err)
	}
	if first.SHA256 != second.SHA256 || string(first.Bytes) != string(second.Bytes) {
		t.Fatal("snapshot changes with candidate input order")
	}
	if got, want := catalog.HashHex(first.SHA256), "5df8bb05b91a29aa1905873f5e0ebad34a2ec44621d7f3754676dd758c9ca7f2"; got != want {
		t.Fatalf("snapshot hash = %s, want %s", got, want)
	}
}
