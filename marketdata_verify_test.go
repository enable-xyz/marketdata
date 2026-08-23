package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/cmd"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/deployment"
)

var fixtureCoverageSelectors = []string{
	"binance-spot",
	"binance-usdm",
	"binance-coinm",
	"bybit-spot",
	"bybit-linear",
	"bybit-inverse",
	"bybit-option",
	"okx-v5-spot",
	"okx-v5-swap",
	"okx-v5-futures",
	"okx-v5-option",
	"deribit-v2",
	"hyperliquid-main",
	"hyperliquid-hip3",
}

func TestRunVerifyVenueExecutesEveryDeclaredFixtureSelectorTwice(t *testing.T) {
	tests := []struct {
		selector string
		venue    string
		product  string
	}{
		{selector: "binance-spot", venue: "binance", product: "spot"},
		{selector: "binance-usdm", venue: "binance", product: "usdm"},
		{selector: "binance-coinm", venue: "binance", product: "coinm"},
		{selector: "bybit-spot", venue: "bybit-v5", product: "spot"},
		{selector: "bybit-linear", venue: "bybit-v5", product: "linear"},
		{selector: "bybit-inverse", venue: "bybit-v5", product: "inverse"},
		{selector: "bybit-option", venue: "bybit-v5", product: "option"},
		{selector: "okx-v5-spot", venue: "okx-v5", product: "spot"},
		{selector: "okx-v5-swap", venue: "okx-v5", product: "swap"},
		{selector: "okx-v5-futures", venue: "okx-v5", product: "futures"},
		{selector: "okx-v5-option", venue: "okx-v5", product: "option"},
		{selector: "deribit-v2", venue: "deribit", product: "derivatives"},
		{selector: "hyperliquid-main", venue: "hyperliquid", product: "main-perpetual"},
		{selector: "hyperliquid-hip3", venue: "hyperliquid", product: "hip3-perpetual"},
	}
	for _, test := range tests {
		t.Run(test.selector, func(t *testing.T) {
			cfg := fixtureSelectorConfig(t, test.selector)
			run := func() []byte {
				t.Helper()
				var output bytes.Buffer
				if err := runVerifyVenue(t.Context(), test.selector, cfg, &verifierVenueRuntime{}, &output); err != nil {
					t.Fatal(err)
				}
				return bytes.Clone(output.Bytes())
			}
			first := run()
			second := run()
			if !bytes.Equal(first, second) || !bytes.HasSuffix(first, []byte("\n")) {
				t.Fatal("fixture selector did not return identical newline-terminated evidence bytes")
			}
			var identity struct {
				SchemaVersion  uint16          `json:"schema_version"`
				Selector       string          `json:"selector"`
				Venue          string          `json:"venue"`
				Product        string          `json:"product"`
				ProofScope     string          `json:"proof_scope"`
				Evidence       json.RawMessage `json:"evidence"`
				EvidenceSHA256 string          `json:"evidence_sha256"`
			}
			if err := json.Unmarshal(first, &identity); err != nil {
				t.Fatal(err)
			}
			if identity.SchemaVersion != 1 || identity.Selector != test.selector || identity.Venue != test.venue ||
				identity.Product != test.product || identity.ProofScope != "offline_repository_fixture" ||
				len(identity.Evidence) == 0 || len(identity.EvidenceSHA256) != sha256.Size*2 {
				t.Fatalf("fixture evidence identity = %+v", identity)
			}
			claimedHash := identity.EvidenceSHA256
			identity.EvidenceSHA256 = ""
			material, err := json.Marshal(identity)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(material)
			if claimedHash != hex.EncodeToString(digest[:]) {
				t.Fatal("scoped fixture evidence hash does not resolve to its deterministic content")
			}
		})
	}
}

func TestRunVerifyReplayReportsExactDeterministicBytes(t *testing.T) {
	source := []byte("{\"selector\":\"bybit-linear\",\"proof\":\"exact\"}\n")
	calls := 0
	var output bytes.Buffer
	err := runVerifyReplayWith(
		t.Context(), "bybit-linear", config.Config{}, &verifierVenueRuntime{}, &output,
		func(_ context.Context, selector string, _ config.Config, _ cmd.Runtime, destination io.Writer) error {
			calls++
			if selector != "bybit-linear" {
				t.Fatalf("selector = %q", selector)
			}
			_, err := destination.Write(source)
			return err
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("venue verification calls = %d, want 2", calls)
	}
	var evidence replayVerificationEvidence
	if err := json.Unmarshal(output.Bytes(), &evidence); err != nil {
		t.Fatal(err)
	}
	sourceDigest := sha256.Sum256(source)
	wantDigest := hex.EncodeToString(sourceDigest[:])
	if evidence.SchemaVersion != 1 || evidence.Selector != "bybit-linear" ||
		evidence.ProofScope != "exact_venue_evidence_replay" || !evidence.Deterministic ||
		evidence.FirstRunSHA256 != wantDigest || evidence.SecondRunSHA256 != wantDigest {
		t.Fatalf("replay evidence = %+v", evidence)
	}
	claimed := evidence.EvidenceSHA256
	evidence.EvidenceSHA256 = ""
	material, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	selfDigest := sha256.Sum256(material)
	if claimed != hex.EncodeToString(selfDigest[:]) {
		t.Fatal("replay self hash does not resolve")
	}
}

func TestRunVerifyReplayRejectsByteMismatch(t *testing.T) {
	calls := 0
	var output bytes.Buffer
	err := runVerifyReplayWith(
		t.Context(), "bybit-linear", config.Config{}, &verifierVenueRuntime{}, &output,
		func(_ context.Context, _ string, _ config.Config, _ cmd.Runtime, destination io.Writer) error {
			calls++
			_, writeErr := fmt.Fprintf(destination, "{\"run\":%d}\n", calls)
			return writeErr
		},
	)
	if err == nil || !strings.Contains(err.Error(), "not deterministic") {
		t.Fatalf("runVerifyReplayWith() error = %v, want mismatch", err)
	}
	if output.Len() != 0 {
		t.Fatalf("mismatched replay emitted evidence: %q", output.String())
	}
}

func TestRunVerifyCoverageValidatesEveryDeclaredFixtureSelector(t *testing.T) {
	for _, selector := range fixtureCoverageSelectors {
		t.Run(selector, func(t *testing.T) {
			var output bytes.Buffer
			if err := runVerifyCoverage(
				t.Context(),
				selector,
				fixtureSelectorConfig(t, selector),
				&verifierVenueRuntime{},
				&output,
			); err != nil {
				t.Fatal(err)
			}
			var evidence coverageVerificationEvidence
			if err := json.Unmarshal(output.Bytes(), &evidence); err != nil {
				t.Fatal(err)
			}
			if evidence.SchemaVersion != 1 || evidence.Selector != selector ||
				evidence.ProofScope != "offline_fixture_manifest_role_coverage" ||
				!evidence.ManifestCoverage || !evidence.RoleCoverage ||
				!validSHA256Hex(evidence.ExactEvidenceSHA256) ||
				!validSHA256Hex(evidence.EnvelopeSHA256) {
				t.Fatalf("coverage evidence = %+v", evidence)
			}
			claimed := evidence.EvidenceSHA256
			evidence.EvidenceSHA256 = ""
			material, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(material)
			if claimed != hex.EncodeToString(digest[:]) {
				t.Fatal("coverage self hash does not resolve")
			}
		})
	}
}

func TestRunVerifyCoverageRejectsMissingExtraAndDuplicateRoleEvidence(t *testing.T) {
	for _, selector := range fixtureCoverageSelectors {
		t.Run(selector, func(t *testing.T) {
			source := exactScopedCoverageSource(t, selector)
			for _, mutation := range []string{"missing", "extra", "duplicate"} {
				t.Run(mutation, func(t *testing.T) {
					assertCoverageSourceRejected(t, selector, mutateScopedCoverageRoleInventory(t, source, selector, mutation))
				})
			}
		})
	}
}

func TestRunVerifyCoverageRejectsCoinMNonemptyFixtureCollectionWithoutExactRoles(t *testing.T) {
	source := exactScopedCoverageSource(t, "binance-coinm")
	var envelope scopedFixtureEnvelope
	if err := json.Unmarshal(source, &envelope); err != nil {
		t.Fatal(err)
	}
	var evidence map[string]any
	if err := json.Unmarshal(envelope.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	fixtures, ok := evidence["Fixtures"].([]any)
	if !ok || len(fixtures) < 2 {
		t.Fatalf("Coin-M fixtures = %#v", evidence["Fixtures"])
	}
	evidence["Fixtures"] = fixtures[:1]
	assertCoverageSourceRejected(t, "binance-coinm", rehashScopedCoveragePacket(t, envelope, evidence))
}

func TestRunVerifyCoverageRejectsTamperedEnvelopeHash(t *testing.T) {
	source := exactScopedCoverageSource(t, "bybit-linear")
	var envelope scopedFixtureEnvelope
	if err := json.Unmarshal(source, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.EvidenceSHA256 = strings.Repeat("0", sha256.Size*2)
	tampered, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	assertCoverageSourceRejected(t, "bybit-linear", tampered)
}

func exactScopedCoverageSource(t *testing.T, selector string) []byte {
	t.Helper()
	var source bytes.Buffer
	if err := runVerifyVenue(
		t.Context(),
		selector,
		fixtureSelectorConfig(t, selector),
		&verifierVenueRuntime{},
		&source,
	); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(source.Bytes())
}

func assertCoverageSourceRejected(t *testing.T, selector string, source []byte) {
	t.Helper()
	var output bytes.Buffer
	err := runVerifyCoverageWith(
		t.Context(), selector, config.Config{}, &verifierVenueRuntime{}, &output,
		func(_ context.Context, _ string, _ config.Config, _ cmd.Runtime, destination io.Writer) error {
			_, writeErr := destination.Write(source)
			return writeErr
		},
	)
	if err == nil {
		t.Fatal("coverage accepted an inexact role inventory")
	}
	if output.Len() != 0 {
		t.Fatalf("invalid coverage emitted evidence: %q", output.String())
	}
}

func mutateScopedCoverageRoleInventory(t *testing.T, source []byte, selector, mutation string) []byte {
	t.Helper()
	var envelope scopedFixtureEnvelope
	if err := json.Unmarshal(source, &envelope); err != nil {
		t.Fatal(err)
	}
	var evidence map[string]any
	if err := json.Unmarshal(envelope.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	switch selector {
	case "binance-spot":
		mutateObjectRoleInventory(t, evidence, "components", "name", mutation)
	case "binance-usdm", "binance-coinm":
		mutateObjectRoleInventory(t, evidence, "Fixtures", "ID", mutation)
	case "bybit-spot", "bybit-linear", "bybit-inverse", "bybit-option",
		"hyperliquid-main", "hyperliquid-hip3":
		mutateCheckRoleInventory(t, evidence, mutation)
	case "okx-v5-spot", "okx-v5-swap", "okx-v5-futures", "okx-v5-option":
		mutateObjectRoleInventory(t, evidence, "fixtures", "role", mutation)
		if mutation == "extra" {
			fixtures := evidence["fixtures"].([]any)
			fixtures[len(fixtures)-1].(map[string]any)["id"] = "okx-unexpected"
		}
	case "deribit-v2":
		mutateStringRoleInventory(t, evidence, "roles", mutation)
	default:
		t.Fatalf("no coverage mutation for selector %q", selector)
	}
	return rehashScopedCoveragePacket(t, envelope, evidence)
}

func mutateObjectRoleInventory(t *testing.T, evidence map[string]any, key, roleKey, mutation string) {
	t.Helper()
	values, ok := evidence[key].([]any)
	if !ok || len(values) == 0 {
		t.Fatalf("%s role inventory = %#v", key, evidence[key])
	}
	switch mutation {
	case "missing":
		values = values[:len(values)-1]
	case "extra":
		item, ok := values[0].(map[string]any)
		if !ok {
			t.Fatalf("%s role evidence = %#v", key, values[0])
		}
		extra := make(map[string]any, len(item))
		for field, value := range item {
			extra[field] = value
		}
		extra[roleKey] = "unexpected"
		values = append(values, extra)
	case "duplicate":
		values = append(values, values[0])
	default:
		t.Fatalf("unknown role inventory mutation %q", mutation)
	}
	evidence[key] = values
}

func mutateCheckRoleInventory(t *testing.T, evidence map[string]any, mutation string) {
	t.Helper()
	checks, ok := evidence["checks"].([]any)
	if !ok || len(checks) == 0 {
		t.Fatalf("check inventory = %#v", evidence["checks"])
	}
	for _, value := range checks {
		check, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("check evidence = %#v", value)
		}
		fixtures, ok := check["fixture_ids"].([]any)
		if !ok || len(fixtures) == 0 {
			continue
		}
		switch mutation {
		case "missing":
			fixtures = fixtures[:len(fixtures)-1]
		case "extra":
			fixtures = append(fixtures, "unexpected")
		case "duplicate":
			fixtures = append(fixtures, fixtures[0])
		default:
			t.Fatalf("unknown role inventory mutation %q", mutation)
		}
		check["fixture_ids"] = fixtures
		return
	}
	t.Fatal("check evidence has no fixture role inventory")
}

func mutateStringRoleInventory(t *testing.T, evidence map[string]any, key, mutation string) {
	t.Helper()
	roles, ok := evidence[key].([]any)
	if !ok || len(roles) == 0 {
		t.Fatalf("%s role inventory = %#v", key, evidence[key])
	}
	switch mutation {
	case "missing":
		roles = roles[:len(roles)-1]
	case "extra":
		roles = append(roles, "unexpected")
	case "duplicate":
		roles = append(roles, roles[0])
	default:
		t.Fatalf("unknown role inventory mutation %q", mutation)
	}
	evidence[key] = roles
}

func rehashScopedCoveragePacket(t *testing.T, envelope scopedFixtureEnvelope, evidence any) []byte {
	t.Helper()
	var output bytes.Buffer
	if err := writeScopedFixtureEvidence(
		&output,
		envelope.Selector,
		envelope.Venue,
		envelope.Product,
		evidence,
	); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(output.Bytes())
}

func TestRunVerifyVenueFailsBeforeFixtureOrNetworkEffects(t *testing.T) {
	bogus := config.Config{Verify: config.VerifyConfig{
		Mode: config.VerifyModeFixture, FixtureRoot: "not-read", FixtureManifest: "not-read/manifest.json",
	}}
	for _, test := range []struct {
		name     string
		selector string
		cfg      config.Config
		runtime  cmd.Runtime
	}{
		{name: "unsupported selector", selector: "unknown", cfg: bogus, runtime: &verifierVenueRuntime{}},
		{name: "non-verifier role", selector: "bybit-spot", cfg: bogus, runtime: &collectorVenueRuntime{}},
		{name: "live mode", selector: "bybit-spot", cfg: config.Config{Verify: config.VerifyConfig{Mode: config.VerifyModeLive}}, runtime: &verifierVenueRuntime{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := runVerifyVenue(t.Context(), test.selector, test.cfg, test.runtime, &output); err == nil {
				t.Fatal("verification boundary accepted a fail-closed input")
			}
			if output.Len() != 0 {
				t.Fatalf("fail-closed verifier emitted output: %q", output.String())
			}
		})
	}
}

type collectorVenueRuntime struct{ verifierVenueRuntime }

func (*collectorVenueRuntime) DeploymentRole() deployment.Role { return deployment.RoleCollector }

func fixtureSelectorConfig(t *testing.T, selector string) config.Config {
	t.Helper()
	if selector == "binance-spot" {
		cfg, err := config.Load(commandFixtureConfig(t), config.Overrides{})
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	roots := map[string]string{
		"binance-usdm":     filepath.Join("testdata", "binance", "usdm"),
		"binance-coinm":    filepath.Join("testdata", "binance", "coinm"),
		"bybit-spot":       filepath.Join("testdata", "bybit"),
		"bybit-linear":     filepath.Join("testdata", "bybit"),
		"bybit-inverse":    filepath.Join("testdata", "bybit"),
		"bybit-option":     filepath.Join("testdata", "bybit", "option"),
		"okx-v5-spot":      filepath.Join("okx", "testdata"),
		"okx-v5-swap":      filepath.Join("okx", "testdata"),
		"okx-v5-futures":   filepath.Join("okx", "testdata"),
		"okx-v5-option":    filepath.Join("okx", "testdata"),
		"deribit-v2":       filepath.Join("deribit", "testdata"),
		"hyperliquid-main": filepath.Join("hyperliquid", "testdata"),
		"hyperliquid-hip3": filepath.Join("hyperliquid", "testdata"),
	}
	root, ok := roots[selector]
	if !ok {
		t.Fatalf("no repository fixture root for selector %q", selector)
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	return config.Config{Verify: config.VerifyConfig{
		Mode: config.VerifyModeFixture, FixtureRoot: root, FixtureManifest: filepath.Join(root, "manifest.json"),
	}}
}

func TestDerivativeFixtureManifestRejectsConfiguredRootMismatch(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(outside, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Verify: config.VerifyConfig{FixtureRoot: root, FixtureManifest: outside}}
	if _, err := derivativeFixtureManifest(cfg); err == nil {
		t.Fatal("fixture manifest outside the configured root was accepted")
	}
}

func TestVerifyVenueLiveFailsClosedBeforeNetwork(t *testing.T) {
	t.Setenv("ELMD014_POSTGRES_DSN", "")
	t.Setenv("ELMD014_S3_CREDENTIALS", "")
	command := newCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"verify", "venue", "--config", filepath.Join("testdata", "config", "binance-spot-live.yaml"), "--venue", "binance-spot"})
	if err := command.ExecuteContext(t.Context()); err == nil {
		t.Fatal("live verification accepted absent caller bindings")
	}
	if output.Len() != 0 {
		t.Fatalf("fail-closed command emitted output: %q", output.String())
	}
}

func TestLiveCredentialDecodeDoesNotEchoSecret(t *testing.T) {
	const secret = "DO-NOT-ECHO-THIS-CREDENTIAL"
	t.Setenv("ELMD014_TEST_CREDENTIALS", secret)
	_, err := loadAWSCredentials("ELMD014_TEST_CREDENTIALS")
	if err == nil {
		t.Fatal("malformed credential JSON was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("credential value appeared in error text")
	}
}

func commandFixtureConfig(t *testing.T) string {
	t.Helper()
	testdata, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	spool := filepath.Join(root, "spool")
	artifacts := filepath.Join(root, "artifacts")
	if err := os.Mkdir(spool, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Join("testdata", "config", "binance-spot-verify.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	contents = strings.Replace(contents, "  fixture_root: ..", "  fixture_root: "+testdata, 1)
	contents = strings.Replace(contents, "  fixture_manifest: ../verify/binance-spot/fixture.json", "  fixture_manifest: "+filepath.Join(testdata, "verify", "binance-spot", "fixture.json"), 1)
	contents = strings.Replace(contents, "  spool_root: ../verify/binance-spot/runtime/spool", "  spool_root: "+spool, 1)
	contents = strings.Replace(contents, "  artifact_root: ../verify/binance-spot/runtime/artifacts", "  artifact_root: "+artifacts, 1)
	path := filepath.Join(root, "verify.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
