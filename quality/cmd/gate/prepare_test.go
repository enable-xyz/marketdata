package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/quality"
)

func TestPaddedJSONProducesExactValidPayloads(t *testing.T) {
	original := []byte(`{"price":"123.45","quantity":7}`)
	var want any
	if err := json.Unmarshal(original, &want); err != nil {
		t.Fatal(err)
	}

	for _, target := range []int{len(original), 128, 1024} {
		payload, err := paddedJSON(original, target)
		if err != nil {
			t.Fatalf("paddedJSON(%d): %v", target, err)
		}
		if len(payload) != target {
			t.Fatalf("payload length = %d, want %d", len(payload), target)
		}
		if !json.Valid(payload) {
			t.Fatalf("payload of length %d is not valid JSON", target)
		}
		var got any
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("payload value = %#v, want %#v", got, want)
		}
	}

	if _, err := paddedJSON(original, len(original)-1); err == nil {
		t.Fatal("paddedJSON accepted a target smaller than the fixture")
	}
}

func TestLoadSigningPrivateKeyRequiresCanonicalFile(t *testing.T) {
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{4}, ed25519.SeedSize))
	path := filepath.Join(t.TempDir(), "signer.key")
	if err := os.WriteFile(path, []byte(hex.EncodeToString(private)), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadSigningPrivateKey(path)
	if err != nil {
		t.Fatalf("load private signing key: %v", err)
	}
	if !bytes.Equal(loaded, private) {
		t.Fatal("loaded signing key differs from provisioned key")
	}

	if err := os.WriteFile(path, []byte("malformed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSigningPrivateKey(path); err == nil {
		t.Fatal("malformed signing key was accepted")
	}
}

func TestLoadPrepareFixturesRequiresDeclaredBytesAndDigest(t *testing.T) {
	root := t.TempDir()
	payload := []byte(`{"event":"trade"}`)
	path := filepath.Join(root, "payload.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	sourceID, _ := deterministicUUID("fixture-digest-test")
	spec := prepareSpec{
		FixtureRoot:   root,
		MaxFileBytes:  1 << 20,
		MaxTotalBytes: 1 << 20,
		Fixtures: []prepareFixture{{
			VenueFamily:   "family-a",
			SourceID:      sourceID,
			Channel:       "trades",
			NativeSymbol:  "TEST",
			PayloadPath:   "payload.json",
			PayloadBytes:  int64(len(payload)),
			PayloadSHA256: hex.EncodeToString(digest[:]),
		}},
	}
	if _, err := loadPrepareFixtures(spec); err != nil {
		t.Fatalf("load exact fixture: %v", err)
	}

	spec.Fixtures[0].PayloadSHA256 = strings.Repeat("0", 64)
	if _, err := loadPrepareFixtures(spec); err == nil {
		t.Fatal("fixture with a mismatched declared digest was accepted")
	}
}

func TestLoadPrepareFixturesAcceptsRealBoundedSourceIDs(t *testing.T) {
	root := t.TempDir()
	payload := []byte(`{"event":"trade"}`)
	if err := os.WriteFile(filepath.Join(root, "payload.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	baseFixture := prepareFixture{
		VenueFamily:   "derivatives",
		Channel:       "trades",
		NativeSymbol:  "TEST",
		PayloadPath:   "payload.json",
		PayloadBytes:  int64(len(payload)),
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
	baseSpec := prepareSpec{
		FixtureRoot:   root,
		MaxFileBytes:  1 << 20,
		MaxTotalBytes: 1 << 20,
	}
	uuidSourceID, _ := deterministicUUID("fixture-source-id-test")
	valid := []struct {
		name     string
		sourceID string
	}{
		{name: "UUID", sourceID: uuidSourceID},
		{name: "Bybit V5 option", sourceID: "bybit-v5-option-public"},
		{name: "OKX V5 public", sourceID: "okx-v5-public"},
		{name: "Deribit JSON-RPC V2", sourceID: "deribit-json-rpc-v2"},
		{name: "Hyperliquid HIP-3", sourceID: "hyperliquid-mainnet-hip3_perpetual-xyz"},
		{name: "maximum length", sourceID: strings.Repeat("s", capture.MaxSourceIDBytes)},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			fixture := baseFixture
			fixture.SourceID = test.sourceID
			spec := baseSpec
			spec.Fixtures = []prepareFixture{fixture}
			prepared, err := loadPrepareFixtures(spec)
			if err != nil {
				t.Fatalf("load fixture with source ID %q: %v", test.sourceID, err)
			}
			if len(prepared) != 1 {
				t.Fatalf("prepared fixture count = %d, want 1", len(prepared))
			}
			if prepared[0].spec.SourceID != test.sourceID {
				t.Fatalf("prepared source ID = %q, want exact %q", prepared[0].spec.SourceID, test.sourceID)
			}
		})
	}

	invalid := []struct {
		name     string
		sourceID string
	}{
		{name: "empty", sourceID: ""},
		{name: "blank", sourceID: " \t"},
		{name: "leading whitespace", sourceID: " bybit-v5-option-public"},
		{name: "trailing whitespace", sourceID: "okx-v5-public "},
		{name: "malformed UTF-8", sourceID: string([]byte{'o', 'k', 'x', '-', 0xff})},
		{name: "oversized", sourceID: strings.Repeat("s", capture.MaxSourceIDBytes+1)},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			fixture := baseFixture
			fixture.SourceID = test.sourceID
			spec := baseSpec
			spec.Fixtures = []prepareFixture{fixture}
			if _, err := loadPrepareFixtures(spec); err == nil {
				t.Fatalf("fixture with invalid source ID %q was accepted", test.sourceID)
			}
		})
	}
}

func TestValidatePrepareFixtureDeclarationsAllowsDistinctContractsPerFamilyAndRequiresFiveFamilies(t *testing.T) {
	fixtures := []prepareFixture{
		{VenueFamily: "family-a", SourceID: "source-a", Channel: "trades", HighCardinalitySymbols: true},
		{VenueFamily: "family-b", SourceID: "source-b", Channel: "trades", LongBooks: true},
		{VenueFamily: "family-c", SourceID: "source-c", Channel: "trades", SparseTickerUpdates: true},
		{VenueFamily: "family-d", SourceID: "source-d", Channel: "trades", Reconnect: true},
		{VenueFamily: "family-e", SourceID: "source-e", Channel: "trades", LongHistory: true},
	}
	if err := validatePrepareFixtureDeclarations(fixtures); err != nil {
		t.Fatalf("complete fixture declarations: %v", err)
	}

	sameFamily := append(append([]prepareFixture(nil), fixtures...), prepareFixture{
		VenueFamily: "family-a", SourceID: "source-a", Channel: "book",
	})
	if err := validatePrepareFixtureDeclarations(sameFamily); err != nil {
		t.Fatalf("distinct contract in an existing venue family: %v", err)
	}

	missingShape := append([]prepareFixture(nil), fixtures...)
	missingShape[4].LongHistory = false
	if err := validatePrepareFixtureDeclarations(missingShape); err == nil {
		t.Fatal("fixture declarations without long-history coverage were accepted")
	}

	duplicate := append(append([]prepareFixture(nil), fixtures...), fixtures[0])
	if err := validatePrepareFixtureDeclarations(duplicate); err == nil {
		t.Fatal("duplicate exact fixture identity was accepted")
	}

	missingFamily := append([]prepareFixture(nil), fixtures...)
	missingFamily[4].VenueFamily = missingFamily[0].VenueFamily
	missingFamily[4].SourceID = "another-source"
	if err := validatePrepareFixtureDeclarations(missingFamily); err == nil {
		t.Fatal("fixture declarations with fewer than five venue families were accepted")
	}
}

func TestDeriveContractsBindsCanonicalHashesToFixtures(t *testing.T) {
	spec := contractPrepareSpec(t)
	first, err := deriveContracts(spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deriveContracts(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("contract hash derivation is not deterministic")
	}
	for _, contract := range first {
		for name, digest := range map[string]string{
			"contract": contract.ContractSHA256,
			"adapter":  contract.AdapterSHA256,
		} {
			decoded, err := hex.DecodeString(digest)
			if err != nil || len(decoded) != 32 || digest != strings.ToLower(digest) {
				t.Fatalf("%s hash for %q is not canonical SHA-256: %q", name, contract.ContractID, digest)
			}
		}
	}

	prefilled := contractPrepareSpec(t)
	prefilled.Contracts[0].ContractSHA256 = strings.Repeat("0", 64)
	if _, err := deriveContracts(prefilled); err == nil {
		t.Fatal("pre-filled contract hash was accepted")
	}

	unbound := contractPrepareSpec(t)
	unbound.Contracts[0].ChannelOrEndpoint = "other-channel"
	if _, err := deriveContracts(unbound); err == nil {
		t.Fatal("contract not bound to its exact fixture was accepted")
	}

	unboundSource := contractPrepareSpec(t)
	unboundSource.Contracts[0].SourceID += "-alias"
	if _, err := deriveContracts(unboundSource); err == nil {
		t.Fatal("contract with a synthetic source alias was accepted")
	}
}
func TestDeriveContractsBindsMultipleExactChannelsInOneVenueFamily(t *testing.T) {
	spec := contractPrepareSpec(t)
	fixture := spec.Fixtures[0]
	fixture.Channel += "-book"
	spec.Fixtures = append(spec.Fixtures, fixture)

	contract := spec.Contracts[0]
	contract.ContractID += "-book"
	contract.ChannelOrEndpoint = fixture.Channel
	contract.DataFamily = "book"
	spec.Contracts = append(spec.Contracts, contract)

	contracts, err := deriveContracts(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 6 {
		t.Fatalf("contract count = %d, want 6", len(contracts))
	}
}

func TestWritePreparedObjectAssignsOneBasedArrivalOrdinals(t *testing.T) {
	spec := prepareSpec{
		AdapterVersion:    "adapter-v1",
		RecordsPerVariant: 1,
		FrameBytes:        1 << 20,
		Replay:            replayLimits{Concurrency: 1},
	}
	fixture := preparedFixture{
		spec: prepareFixture{
			VenueFamily:  "bybit",
			SourceID:     "bybit-v5-linear-public",
			Channel:      "publicTrade.{symbol}",
			NativeSymbol: "BTCUSDT",
		},
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "objects"), 0o700); err != nil {
		t.Fatal(err)
	}
	object, err := writePreparedObject(
		spec,
		fixture,
		0,
		0,
		quality.PayloadTiny,
		[]byte(`{"event":"trade"}`),
		0,
		1,
		1_000,
		2_000,
		root,
		sha256.Sum256([]byte("one-based-arrival")),
	)
	if err != nil {
		t.Fatalf("write prepared object: %v", err)
	}
	if object.Publication.OrdinalStart != 1 || object.Publication.OrdinalEnd != 1 {
		t.Fatalf("publication ordinals = %d..%d, want 1..1", object.Publication.OrdinalStart, object.Publication.OrdinalEnd)
	}
}

func contractPrepareSpec(t *testing.T) prepareSpec {
	t.Helper()
	const startText = "2026-01-02T03:04:05Z"
	const endText = "2026-01-02T04:04:05Z"
	start, err := time.Parse(time.RFC3339Nano, startText)
	if err != nil {
		t.Fatal(err)
	}
	end, err := time.Parse(time.RFC3339Nano, endText)
	if err != nil {
		t.Fatal(err)
	}

	spec := prepareSpec{
		AdapterVersion:   "adapter-v1",
		CoverageStartUTC: startText,
		CoverageEndUTC:   endText,
	}
	uuidSourceID, _ := deterministicUUID("contract-test-source:uuid")
	sourceIDs := []string{
		uuidSourceID,
		"bybit-v5-option-public",
		"okx-v5-public",
		"deribit-json-rpc-v2",
		"hyperliquid-mainnet-hip3_perpetual-xyz",
	}
	for index := range 5 {
		family := "family-" + string(rune('a'+index))
		sourceID := sourceIDs[index]
		channel := "channel-" + family
		spec.Fixtures = append(spec.Fixtures, prepareFixture{
			VenueFamily: family,
			SourceID:    sourceID,
			Channel:     channel,
		})
		spec.Contracts = append(spec.Contracts, quality.ContractIdentity{
			ContractID:        "contract-" + family,
			SourceID:          sourceID,
			APIVersion:        "v1",
			Entitlement:       "public",
			ChannelOrEndpoint: channel,
			DataFamily:        "trades",
			NativeGranularity: "event",
			VenueFamily:       family,
			CoverageStartNS:   start.UnixNano(),
			CoverageEndNS:     end.UnixNano(),
			AdapterVersion:    spec.AdapterVersion,
			CanaryRequirement: quality.CanaryNotRequired,
		})
	}
	return spec
}
