package bybit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

func TestBybitDistinctPublicContractsAndSockets(t *testing.T) {
	seenSources := make(map[string]struct{})
	seenEndpoints := make(map[string]struct{})
	for _, category := range []Category{Spot, Linear, Inverse} {
		contract, err := PublicSourceContract(category)
		if err != nil {
			t.Fatalf("PublicSourceContract(%s): %v", category, err)
		}
		if err := contract.Validate(); err != nil {
			t.Fatalf("contract %s: %v", category, err)
		}
		if _, duplicate := seenSources[contract.SourceID]; duplicate {
			t.Fatalf("source identity reused: %s", contract.SourceID)
		}
		seenSources[contract.SourceID] = struct{}{}
		if _, duplicate := seenEndpoints[category.PublicEndpoint()]; duplicate {
			t.Fatalf("socket endpoint reused: %s", category.PublicEndpoint())
		}
		seenEndpoints[category.PublicEndpoint()] = struct{}{}
	}
	spotLiquidation, ok := Supports(Spot, RoleAllLiquidation)
	if !ok || spotLiquidation.Support != capture.SupportUnsupported {
		t.Fatal("Spot liquidation must remain explicitly unsupported")
	}
	messages, err := SubscriptionMessages(Linear, []TopicRequest{{Role: RoleTrade, Symbol: "BTCUSDT"}, {Role: RoleFullOrderbook, Symbol: "BTCUSDT"}, {Role: RoleAllLiquidation, Symbol: "BTCUSDT"}})
	if err != nil || len(messages) != 1 {
		t.Fatalf("linear subscriptions: messages=%d err=%v", len(messages), err)
	}
	if _, err := SubscriptionMessages(Spot, []TopicRequest{{Role: RoleAllLiquidation, Symbol: "BTCUSDT"}}); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("Spot liquidation subscription error = %v", err)
	}
}

func TestBybitBoundedAndFullSequencingCannotSubstitute(t *testing.T) {
	boundedSnapshot, err := ParseBoundedOrderbook(Spot, bybitFixture(t, "synthetic/bounded-snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	boundedGap, err := ParseBoundedOrderbook(Spot, bybitFixture(t, "synthetic/bounded-gap-delta.json"))
	if err != nil {
		t.Fatal(err)
	}
	bounded, _ := NewBoundedBook(Spot, "BTCUSDT", 50)
	if err := bounded.Apply(boundedSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := bounded.Apply(boundedGap); err != nil {
		t.Fatalf("bounded u jump incorrectly used full-book continuity: %v", err)
	}
	if bounded.Snapshot().UpdateID != 500 {
		t.Fatal("bounded delta was not retained")
	}

	first, err := ParseFullOrderbookDelta(Linear, bybitFixture(t, "official/full-delta.json"))
	if err != nil {
		t.Fatal(err)
	}
	next, _ := ParseFullOrderbookDelta(Linear, bybitFixture(t, "synthetic/full-next-delta.json"))
	gap, _ := ParseFullOrderbookDelta(Linear, bybitFixture(t, "synthetic/full-gap-delta.json"))
	full, _ := NewFullBook(Linear, "MNTUSDT")
	if err := full.Accept(first); err != nil {
		t.Fatal(err)
	}
	if err := full.Accept(next); err != nil {
		t.Fatal(err)
	}
	if err := full.Seed(FullBookSnapshot{Category: Linear, Symbol: "MNTUSDT", UpdateID: first.UpdateID, CrossSequence: first.CrossSequence, Bids: first.Bids, Asks: first.Asks, MaximumLevelsPerSide: 10000}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(full.Accept(gap), ErrFullBookGap) || full.Snapshot().State != FullBookNeedsResync {
		t.Fatal("full-book gap did not force exact resynchronization")
	}
	if _, err := ParseBoundedOrderbook(Linear, bybitFixture(t, "official/full-delta.json")); err == nil {
		t.Fatal("full delta substituted for bounded contract")
	}
}

func TestBybitSparseTickerReconnectAndOpenInterestMeanings(t *testing.T) {
	state, _ := NewDerivativeTickerState(Linear, "BTCUSDT", "connection-1")
	if err := state.Apply(bybitFixture(t, "synthetic/ticker-oi-snapshot.json")); err != nil {
		t.Fatal(err)
	}
	mark := state.Fields().MarkPrice
	if err := state.Apply(bybitFixture(t, "synthetic/ticker-sparse-delta.json")); err != nil {
		t.Fatal(err)
	}
	fields := state.Fields()
	if fields.MarkPrice != mark {
		t.Fatal("omitted sparse field did not preserve prior value and source time")
	}
	if fields.FundingRate.State != normalize.SourceEmpty || fields.FundingRate.Text != "" {
		t.Fatal("explicit empty funding rate was not made unavailable")
	}
	units := TickerUnitContract{Price: normalize.SpotPriceUnit("BTC", "USDT"), OpenInterestSize: normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "bybit_open_interest_size"}, OpenInterestValue: normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "bybit_open_interest_value"}}
	observations, err := state.OpenInterest(1760325053000*int64(1e6), units)
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 4 || observations[0].Variant != "openInterest" || observations[0].Sidedness != normalize.OpenInterestBothSides || observations[2].Variant != "singleOpenInterest" || observations[2].Sidedness != normalize.OpenInterestSingleSide || observations[0].Native.Decimal == observations[2].Native.Decimal {
		t.Fatal("single-side and both-side OI meanings substituted")
	}
	if err := state.BeginConnection("connection-2"); err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(bybitFixture(t, "synthetic/ticker-sparse-delta.json")); !errors.Is(err, ErrUnseededTicker) || state.Seeded() {
		t.Fatalf("reconnect leaked sparse state: %v", err)
	}
}

func TestBybitTickerRejectsNonStringValuesAtomically(t *testing.T) {
	invalidValues := []string{"123", "true", `{"nested":"value"}`, `["value"]`}

	derivative, _ := NewDerivativeTickerState(Linear, "BTCUSDT", "connection-1")
	if err := derivative.Apply(bybitFixture(t, "synthetic/ticker-oi-snapshot.json")); err != nil {
		t.Fatal(err)
	}
	derivativeBefore := derivative.Fields()
	for _, messageType := range []string{"snapshot", "delta"} {
		for _, invalid := range invalidValues {
			payload := []byte(fmt.Sprintf(`{"topic":"tickers.BTCUSDT","type":%q,"data":{"symbol":"BTCUSDT","lastPrice":"70000","markPrice":%s},"ts":1760325052631}`, messageType, invalid))
			if err := derivative.Apply(payload); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("DerivativeTickerState.Apply(%s, %s) error = %v", messageType, invalid, err)
			}
			if derivative.Fields() != derivativeBefore || !derivative.Seeded() {
				t.Fatalf("malformed derivative %s %s partially mutated state", messageType, invalid)
			}
		}
	}
	if err := derivative.Apply([]byte(`{"topic":"tickers.BTCUSDT","type":"delta","data":{"symbol":"BTCUSDT","markPrice":null},"ts":1760325052631}`)); err != nil {
		t.Fatalf("literal null derivative field: %v", err)
	}
	if derivative.Fields().MarkPrice.State != normalize.SourceNull {
		t.Fatal("literal null derivative field was not retained as source null")
	}

	spot, _ := NewSpotTickerState("BTCUSDT", "connection-1")
	if err := spot.Apply([]byte(`{"topic":"tickers.BTCUSDT","type":"snapshot","data":{"symbol":"BTCUSDT","lastPrice":"66666.60"},"ts":1760325052630}`)); err != nil {
		t.Fatal(err)
	}
	spotBefore := spot.Fields()
	for _, invalid := range invalidValues {
		payload := []byte(fmt.Sprintf(`{"topic":"tickers.BTCUSDT","type":"snapshot","data":{"symbol":"BTCUSDT","lastPrice":"70000","bid1Price":%s},"ts":1760325052631}`, invalid))
		if err := spot.Apply(payload); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("SpotTickerState.Apply(%s) error = %v", invalid, err)
		}
		if !maps.Equal(spot.Fields(), spotBefore) || !spot.seeded {
			t.Fatalf("malformed Spot snapshot %s partially mutated state", invalid)
		}
	}
	if err := spot.Apply([]byte(`{"topic":"tickers.BTCUSDT","type":"snapshot","data":{"symbol":"BTCUSDT","bid1Price":null},"ts":1760325052631}`)); err != nil {
		t.Fatalf("literal null Spot field: %v", err)
	}
	if spot.Fields()["bid1Price"].State != normalize.SourceNull {
		t.Fatal("literal null Spot field was not retained as source null")
	}
}

func TestBybitRPIAndLiquidationSchemaDrift(t *testing.T) {
	rpiMessage, err := ParseRPIOrderbook(Spot, bybitFixture(t, "synthetic/rpi-snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	rpi, _ := NewRPIBook(Spot, "BTCUSDT")
	if err := rpi.Apply(rpiMessage); err != nil {
		t.Fatal(err)
	}
	if rpi.Snapshot().SourceRole != RoleRPIOrderbook || rpi.Snapshot().Bids["121975.1"].RPIAmount != "0.25" {
		t.Fatal("RPI quantity lost or relabelled as regular book")
	}
	bounded, _ := NewBoundedBook(Spot, "BTCUSDT", 50)
	boundedMessage, _ := ParseBoundedOrderbook(Spot, bybitFixture(t, "synthetic/bounded-snapshot.json"))
	_ = bounded.Apply(boundedMessage)
	if bounded.Snapshot().RPIIncluded {
		t.Fatal("regular book claims RPI inclusion")
	}
	arrayBatch, err := ParseAllLiquidation(Linear, bybitFixture(t, "official/all-liquidation-array.json"))
	if err != nil {
		t.Fatal(err)
	}
	objectBatch, err := ParseAllLiquidation(Linear, bybitFixture(t, "synthetic/all-liquidation-object.json"))
	if err != nil {
		t.Fatal(err)
	}
	if arrayBatch.DataShape != LiquidationDataArray || objectBatch.DataShape != LiquidationDataObject || len(arrayBatch.Events) != len(objectBatch.Events) {
		t.Fatal("documented liquidation object/array drift not preserved")
	}
	if _, err := ParseAllLiquidation(Spot, bybitFixture(t, "official/all-liquidation-array.json")); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatal("Spot liquidation was not rejected")
	}
}

func TestBybitMetadataAndOfflineFixtureEvidence(t *testing.T) {
	page, err := ParseInstrumentInfo(Linear, bybitFixture(t, "official/instrument-linear.json"))
	if err != nil || len(page.Instruments) != 1 || page.Instruments[0].TickSize != "0.10" {
		t.Fatalf("metadata page: %+v err=%v", page, err)
	}
	if _, err := NewInstrumentRequest(Spot, "BTCUSDT", "cursor", 0); err == nil {
		t.Fatal("Spot metadata accepted unsupported cursor")
	}
	evidence, err := VerifyFixtures(filepath.Join("..", "testdata", "bybit"))
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Version != EvidenceVersion || evidence.FixtureCount != 14 || evidence.PrimarySourceCount != 6 || evidence.SyntheticCount != 8 || len(evidence.Checks) != 8 || len(evidence.EvidenceSHA256) != 64 {
		t.Fatalf("unexpected evidence summary: %+v", evidence)
	}
	wantFixtureIDs := [][]string{
		nil,
		{"trade"},
		{"bounded-snapshot", "bounded-gap", "full-delta", "full-next", "full-gap"},
		{"rpi-snapshot", "rpi-delta"},
		{"ticker-official", "ticker-oi", "ticker-sparse"},
		{"ticker-oi"},
		{"liquidation-array", "liquidation-object"},
		{"instrument-linear"},
	}
	for i, check := range evidence.Checks {
		if !slices.Equal(check.FixtureIDs, wantFixtureIDs[i]) {
			t.Fatalf("evidence check %q fixture IDs = %v, want %v", check.Name, check.FixtureIDs, wantFixtureIDs[i])
		}
	}
	cited := make(map[string]bool)
	for _, check := range evidence.Checks {
		for _, id := range check.FixtureIDs {
			cited[id] = true
		}
	}
	if !cited["ticker-official"] || !cited["rpi-delta"] {
		t.Fatalf("official semantic fixture IDs missing from evidence checks: %+v", evidence.Checks)
	}
}

func TestBybitFixtureContainmentRejectsIntermediateSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	payload := []byte(`{"outside":"fixture"}`)
	if err := os.WriteFile(filepath.Join(outside, "trade.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "official")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	digest := sha256.Sum256(payload)
	manifest := FixtureManifest{
		Version:      FixtureManifestVersion,
		Venue:        "bybit-v5",
		AccessDate:   "2026-08-23",
		FixtureClaim: "intermediate symlink containment regression",
		Fixtures: []FixtureEntry{{
			ID:             "trade",
			File:           "official/trade.json",
			Role:           "trade",
			Classification: "primary_source_value_projection",
			SourceURL:      "https://example.invalid/bybit",
			ByteLength:     uint32(len(payload)),
			SHA256:         hex.EncodeToString(digest[:]),
		}},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifestBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("intermediate symlink escape error = %v", err)
	}
}

func TestBybitVerifierSemanticallyDependsOnOfficialFixtures(t *testing.T) {
	tests := []struct {
		name string
		id   string
		old  []byte
		new  []byte
	}{
		{name: "ticker", id: "ticker-official", old: []byte(`"lastPrice": "66666.60"`), new: []byte(`"lastPrice": "1"`)},
		{name: "RPI", id: "rpi-delta", old: []byte(`["121960.5", "0", "0.163986"]`), new: []byte(`["121960.5", "0", "9"]`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, manifest := copyBybitFixtureSet(t)
			entry := bybitFixtureEntry(t, &manifest, test.id)
			path := filepath.Join(root, filepath.FromSlash(entry.File))
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			mutated := bytes.Replace(payload, test.old, test.new, 1)
			if bytes.Equal(mutated, payload) {
				t.Fatalf("fixture mutation for %s did not match", test.id)
			}
			if err := os.WriteFile(path, mutated, 0o600); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(mutated)
			entry.ByteLength = uint32(len(mutated))
			entry.SHA256 = hex.EncodeToString(digest[:])
			manifestBytes, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifestBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
				t.Fatalf("semantically altered digest-valid %s fixture error = %v", test.id, err)
			}
		})
	}
}

func TestBybitVerifierRejectsDuplicateRoleSubstitution(t *testing.T) {
	root, manifest := copyBybitFixtureSet(t)
	bybitFixtureEntry(t, &manifest, "ticker-oi").Role = "ticker_official"
	writeBybitFixtureManifest(t, root, manifest)

	if evidence, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("VerifyFixtures() accepted duplicate role substitution: evidence=%+v err=%v", evidence, err)
	}
}

func TestBybitVerifierRejectsRenamedFixtureIDBeforeEvidence(t *testing.T) {
	root, manifest := copyBybitFixtureSet(t)
	bybitFixtureEntry(t, &manifest, "ticker-official").ID = "detached-ticker-evidence"
	writeBybitFixtureManifest(t, root, manifest)

	if evidence, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("VerifyFixtures() emitted evidence for a renamed fixture ID: evidence=%+v err=%v", evidence, err)
	}
}

func TestBybitVerifierRejectsSyntheticOfficialClassification(t *testing.T) {
	root, manifest := copyBybitFixtureSet(t)
	entry := bybitFixtureEntry(t, &manifest, "ticker-official")
	entry.Classification = "synthetic_parseable_projection"
	entry.DerivedFrom = "synthetic/ticker-oi-snapshot.json"
	writeBybitFixtureManifest(t, root, manifest)

	if evidence, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("VerifyFixtures() accepted synthetic official classification: evidence=%+v err=%v", evidence, err)
	}
}

func TestBybitVerifierRejectsNonNumericOfficialTickerFields(t *testing.T) {
	tests := []struct {
		name string
		old  []byte
		new  []byte
	}{
		{name: "last price", old: []byte(`"lastPrice": "66666.60"`), new: []byte(`"lastPrice": "not-a-number"`)},
		{name: "mark price", old: []byte(`"markPrice": "66666.60"`), new: []byte(`"markPrice": "not-a-number"`)},
		{name: "index price", old: []byte(`"indexPrice": "115418.19"`), new: []byte(`"indexPrice": "not-a-number"`)},
		{name: "open interest", old: []byte(`"openInterest": "492373.72"`), new: []byte(`"openInterest": "not-a-number"`)},
		{name: "open interest value", old: []byte(`"openInterestValue": "32824881841.75"`), new: []byte(`"openInterestValue": "not-a-number"`)},
		{name: "funding rate", old: []byte(`"fundingRate": "-0.005"`), new: []byte(`"fundingRate": "not-a-number"`)},
		{name: "next funding time", old: []byte(`"nextFundingTime": "1760342400000"`), new: []byte(`"nextFundingTime": "not-a-time"`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, manifest := copyBybitFixtureSet(t)
			mutateBybitFixture(t, root, &manifest, "ticker-official", test.old, test.new)
			writeBybitFixtureManifest(t, root, manifest)

			if evidence, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
				t.Fatalf("VerifyFixtures() accepted non-numeric official field: evidence=%+v err=%v", evidence, err)
			}
		})
	}
}

func bybitFixture(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("..", "testdata", "bybit", filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func copyBybitFixtureSet(t *testing.T) (string, FixtureManifest) {
	t.Helper()
	sourceRoot := filepath.Join("..", "testdata", "bybit")
	manifestBytes, err := os.ReadFile(filepath.Join(sourceRoot, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest FixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, entry := range manifest.Fixtures {
		payload, err := os.ReadFile(filepath.Join(sourceRoot, filepath.FromSlash(entry.File)))
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(root, filepath.FromSlash(entry.File))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifestBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	return root, manifest
}

func bybitFixtureEntry(t *testing.T, manifest *FixtureManifest, id string) *FixtureEntry {
	t.Helper()
	for i := range manifest.Fixtures {
		if manifest.Fixtures[i].ID == id {
			return &manifest.Fixtures[i]
		}
	}
	t.Fatalf("fixture ID %q not found", id)
	return nil
}

func mutateBybitFixture(t *testing.T, root string, manifest *FixtureManifest, id string, old, new []byte) {
	t.Helper()
	entry := bybitFixtureEntry(t, manifest, id)
	path := filepath.Join(root, filepath.FromSlash(entry.File))
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(payload, old, new, 1)
	if bytes.Equal(mutated, payload) {
		t.Fatalf("fixture mutation for %s did not match", id)
	}
	if err := os.WriteFile(path, mutated, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(mutated)
	entry.ByteLength = uint32(len(mutated))
	entry.SHA256 = hex.EncodeToString(digest[:])
}

func writeBybitFixtureManifest(t *testing.T, root string, manifest FixtureManifest) {
	t.Helper()
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifestBytes, 0o600); err != nil {
		t.Fatal(err)
	}
}
