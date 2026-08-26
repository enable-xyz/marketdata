package config

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

var exactVerifyVenueFixtures = []struct {
	selector string
	file     string
}{
	{selector: "binance-spot", file: "binance-spot-verify.yaml"},
	{selector: "binance-usdm", file: "binance-usdm-verify.yaml"},
	{selector: "binance-coinm", file: "binance-coinm-verify.yaml"},
	{selector: "bybit-spot", file: "bybit-spot-verify.yaml"},
	{selector: "bybit-linear", file: "bybit-linear-verify.yaml"},
	{selector: "bybit-inverse", file: "bybit-inverse-verify.yaml"},
	{selector: "bybit-option", file: "bybit-option-verify.yaml"},
	{selector: "okx-v5-spot", file: "okx-v5-spot-verify.yaml"},
	{selector: "okx-v5-swap", file: "okx-v5-swap-verify.yaml"},
	{selector: "okx-v5-futures", file: "okx-v5-futures-verify.yaml"},
	{selector: "okx-v5-option", file: "okx-v5-option-verify.yaml"},
	{selector: "deribit-v2", file: "deribit-v2-verify.yaml"},
	{selector: "hyperliquid-main", file: "hyperliquid-main-verify.yaml"},
	{selector: "hyperliquid-hip3", file: "hyperliquid-hip3-verify.yaml"},
}

func TestLiveVerifyRequiresExplicitLimitsAndExactSecretBindings(t *testing.T) {
	path := filepath.Join("..", "testdata", "config", "binance-spot-live.yaml")
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	var references []string
	if err := cfg.ValidateVerifyVenue(t.Context(), "binance-spot", func(_ context.Context, reference string) error {
		references = append(references, reference)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(references)
	if !slices.Equal(references, []string{"ELMD014_POSTGRES_DSN", "ELMD014_S3_CREDENTIALS"}) {
		t.Fatalf("resolved references = %q", references)
	}

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	withoutDuration := strings.Replace(string(encoded), "  max_duration: 10s\n", "", 1)
	missingPath := filepath.Join(t.TempDir(), "missing-limit.yaml")
	if err := os.WriteFile(missingPath, []byte(withoutDuration), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(missingPath, nil); err == nil || !strings.Contains(err.Error(), "verify.max_duration must be explicitly declared") {
		t.Fatalf("missing explicit live duration error = %v", err)
	}
}

func TestVerifyVenueExactFixtureContracts(t *testing.T) {
	type mutation struct {
		name   string
		change func(*Config)
	}
	mutations := []mutation{
		{name: "wrong API", change: func(cfg *Config) {
			if cfg.Sources[0].API == "binance-spot" {
				cfg.Sources[0].API = "bybit-spot"
			} else {
				cfg.Sources[0].API = "binance-spot"
			}
		}},
		{name: "endpoint omitted", change: func(cfg *Config) {
			cfg.Sources[0].Endpoints = cfg.Sources[0].Endpoints[:len(cfg.Sources[0].Endpoints)-1]
		}},
		{name: "endpoint added", change: func(cfg *Config) {
			cfg.Sources[0].Endpoints = append(slices.Clone(cfg.Sources[0].Endpoints), "https://api.binance.com")
		}},
		{name: "channel omitted", change: func(cfg *Config) { cfg.Sources[0].Channels = cfg.Sources[0].Channels[:len(cfg.Sources[0].Channels)-1] }},
		{name: "channel added", change: func(cfg *Config) {
			cfg.Sources[0].Channels = append(slices.Clone(cfg.Sources[0].Channels), "private.orders")
		}},
		{name: "method omitted", change: func(cfg *Config) { cfg.Sources[0].Methods = cfg.Sources[0].Methods[:len(cfg.Sources[0].Methods)-1] }},
		{name: "method added", change: func(cfg *Config) {
			cfg.Sources[0].Methods = append(slices.Clone(cfg.Sources[0].Methods), "trading:post-order")
		}},
		{name: "entitlement added", change: func(cfg *Config) {
			cfg.Sources[0].EntitlementRef = "SECRET"
			cfg.Sources[0].EntitlementScope = EntitlementScopeReadOnly
		}},
		{name: "zero symbols", change: func(cfg *Config) { cfg.Sources[0].Symbols = nil }},
		{name: "five symbols", change: func(cfg *Config) { cfg.Sources[0].Symbols = []string{"A", "B", "C", "D", "E"} }},
		{name: "source omitted", change: func(cfg *Config) { cfg.Sources = nil }},
		{name: "source added", change: func(cfg *Config) {
			cfg.Sources = append(slices.Clone(cfg.Sources), cfg.Sources[0])
		}},
	}

	for _, fixture := range exactVerifyVenueFixtures {
		t.Run(fixture.selector, func(t *testing.T) {
			cfg := loadVerifyVenueFixture(t, fixture.file)
			callbackCalled := false
			if err := cfg.ValidateVerifyVenue(t.Context(), fixture.selector, func(context.Context, string) error {
				callbackCalled = true
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if callbackCalled {
				t.Fatal("fixture verification invoked a secret or network callback")
			}

			cfg.Sources[0].Symbols = []string{"A", "B", "C", "D"}
			if err := cfg.ValidateVerifyVenue(t.Context(), fixture.selector, nil); err != nil {
				t.Fatalf("four-symbol boundary rejected: %v", err)
			}

			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					cfg := loadVerifyVenueFixture(t, fixture.file)
					mutation.change(&cfg)
					callbackCalled := false
					err := cfg.ValidateVerifyVenue(t.Context(), fixture.selector, func(context.Context, string) error {
						callbackCalled = true
						return nil
					})
					if err == nil {
						t.Fatalf("mutated fixture was accepted")
					}
					if callbackCalled {
						t.Fatal("invalid fixture reached a secret or network callback")
					}
				})
			}
		})
	}
}

func TestVerifyVenueFixtureSelectorsRejectLiveModeBeforeCallbacks(t *testing.T) {
	for _, fixture := range exactVerifyVenueFixtures {
		if fixture.selector == "binance-spot" {
			continue
		}
		t.Run(fixture.selector, func(t *testing.T) {
			cfg := loadVerifyVenueFixture(t, fixture.file)
			cfg.Verify.Mode = VerifyModeLive
			cfg.Verify.FixtureRoot = ""
			cfg.Verify.FixtureManifest = ""
			callbackCalled := false
			err := cfg.ValidateVerifyVenue(t.Context(), fixture.selector, func(context.Context, string) error {
				callbackCalled = true
				return nil
			})
			if err == nil {
				t.Fatal("live acquisition was accepted by the verifier role")
			}
			if callbackCalled {
				t.Fatal("live-mode rejection reached a secret or network callback")
			}
		})
	}
}

func TestVerifyVenueRejectsMalformedAndUnknownSelectors(t *testing.T) {
	cfg := loadVerifyVenueFixture(t, "binance-spot-verify.yaml")
	for _, selector := range []string{"", " binance-spot", "BINANCE-SPOT", "unknown", "bybit"} {
		t.Run(selector, func(t *testing.T) {
			callbackCalled := false
			err := cfg.ValidateVerifyVenue(t.Context(), selector, func(context.Context, string) error {
				callbackCalled = true
				return nil
			})
			if err == nil {
				t.Fatalf("selector %q was accepted", selector)
			}
			if callbackCalled {
				t.Fatal("unknown selector reached a secret or network callback")
			}
		})
	}
}

func TestVerifyVenueRejectsManifestEscapeAndSymlinkBoundary(t *testing.T) {
	cfg := loadVerifyVenueFixture(t, "bybit-spot-verify.yaml")
	root := t.TempDir()
	outside := t.TempDir()
	outsideManifest := filepath.Join(outside, "manifest.json")
	if err := os.WriteFile(outsideManifest, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg.Verify.FixtureRoot = root
	cfg.Verify.FixtureManifest = outsideManifest
	if err := cfg.ValidateVerifyVenue(t.Context(), "bybit-spot", nil); err == nil {
		t.Fatal("lexically escaped fixture manifest was accepted")
	}

	link := filepath.Join(root, "linked-outside")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	cfg.Verify.FixtureManifest = filepath.Join(link, "manifest.json")
	if err := cfg.ValidateVerifyVenue(t.Context(), "bybit-spot", nil); err == nil {
		t.Fatal("fixture manifest escaping through an intermediate symlink was accepted")
	}

	cfg.Verify.FixtureRoot = "relative"
	cfg.Verify.FixtureManifest = filepath.Join("relative", "manifest.json")
	if err := cfg.ValidateVerifyVenue(t.Context(), "bybit-spot", nil); err == nil {
		t.Fatal("relative fixture paths were accepted")
	}
}

func TestLegacyBybitAggregateRemainsFixtureOnly(t *testing.T) {
	cfg := loadVerifyVenueFixture(t, "bybit-v5-verify.yaml")
	if err := cfg.ValidateVerifyVenue(t.Context(), "bybit-v5", nil); err != nil {
		t.Fatal(err)
	}
	cfg.Verify.Mode = VerifyModeLive
	cfg.Verify.FixtureRoot = ""
	cfg.Verify.FixtureManifest = ""
	if err := cfg.ValidateVerifyVenue(t.Context(), "bybit-v5", nil); err == nil {
		t.Fatal("legacy Bybit fixture aggregate accepted live mode")
	}
}

func loadVerifyVenueFixture(t *testing.T, name string) Config {
	t.Helper()
	cfg, err := Load(filepath.Join("..", "testdata", "config", name), nil)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
