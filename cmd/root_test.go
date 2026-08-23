package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/config"
)

func TestNewReturnsFreshCommandTrees(t *testing.T) {
	first := New(testDependencies())
	second := New(testDependencies())

	if err := first.PersistentFlags().Set("log-level", "debug"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got := second.PersistentFlags().Lookup("log-level").Value.String(); got != "" {
		t.Fatalf("second tree inherited log-level %q", got)
	}

	for _, root := range []*Command{first, second} {
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs([]string{"--help"})
		if err := root.ExecuteContext(t.Context()); err != nil {
			t.Fatalf("ExecuteContext(--help) error = %v", err)
		}
		for _, command := range []string{"collect", "catalog", "replay", "export", "verify", "serve", "migrate", "load", "backup", "release", "smoke", "completion"} {
			if !strings.Contains(out.String(), command) {
				t.Errorf("help omits command %q", command)
			}
		}
	}
}

func TestVersion(t *testing.T) {
	root := New(Dependencies{Build: BuildInfo{Version: "1.2.3", Commit: "abc", Date: "today"}})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"--version"})
	if err := root.ExecuteContext(t.Context()); err != nil {
		t.Fatalf("ExecuteContext(--version) error = %v", err)
	}
	if got, want := strings.TrimSpace(out.String()), "1.2.3 (commit: abc, built: today)"; got != want {
		t.Fatalf("version = %q, want %q", got, want)
	}
}

func TestEffectCommandRequiresExplicitConfig(t *testing.T) {
	root := New(testDependencies())
	root.SetArgs([]string{"collect"})
	err := root.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), config.ErrPathRequired.Error()) {
		t.Fatalf("ExecuteContext(collect) error = %v, want explicit-config error", err)
	}
}

func TestEffectCommandRejectsMissingDestinationsBeforeRunner(t *testing.T) {
	called := false
	root := New(Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) {
			return config.Config{
				Deployment: config.DeploymentConfig{Role: "collector", WriterLeaseKey: "source/channel", WriterID: "writer"},
				Capture:    boundedCapture(),
			}, nil
		},
		Run: func(context.Context, string, config.Config, Runtime, io.Writer) error {
			called = true
			return nil
		},
	})
	root.SetArgs([]string{"collect", "--config", "declared.yaml"})
	err := root.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), "at least one source") {
		t.Fatalf("ExecuteContext(collect) error = %v, want missing-destination error", err)
	}
	if called {
		t.Fatal("effect runner was called with missing destinations")
	}
}

func TestEffectCommandRejectsUnboundSecretBeforeRunner(t *testing.T) {
	called := false
	cfg := config.Config{
		Deployment: config.DeploymentConfig{Role: "collector", WriterLeaseKey: "spot/trades", WriterID: "writer-a"},
		Capture:    boundedCapture(),
		ObjectStore: config.ObjectStoreConfig{
			Endpoint:      "https://objects.example.test",
			Region:        "test",
			Bucket:        "bucket",
			CredentialRef: "opaque-secret-reference",
		},
		Catalog: config.CatalogConfig{
			DSNRef:       "catalog-secret-reference",
			ServerMajors: []int{17},
		},
		Sources: []config.SourceConfig{{
			ID:        "spot",
			API:       "binance-spot",
			Endpoints: []string{"wss://stream.example.test"},
		}},
	}
	root := New(Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) {
			return cfg, nil
		},
		ValidateSecret: func(context.Context, string) error {
			return context.Canceled
		},
		Run: func(context.Context, string, config.Config, Runtime, io.Writer) error {
			called = true
			return nil
		},
	})
	root.SetArgs([]string{"collect", "--config", "declared.yaml"})
	err := root.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), "object_store.credential_ref is not bound") {
		t.Fatalf("ExecuteContext(collect) error = %v, want unbound-reference error", err)
	}
	if strings.Contains(err.Error(), "opaque-secret-reference") {
		t.Fatalf("error disclosed secret reference: %v", err)
	}
	if called {
		t.Fatal("effect runner was called with an unbound secret reference")
	}
}

type orderedRuntime struct{ order *[]string }

func (r *orderedRuntime) Shutdown(context.Context) error {
	*r.order = append(*r.order, "shutdown")
	return nil
}

func TestEffectCommandValidatesBeforeComposition(t *testing.T) {
	var order []string
	cfg := config.Config{
		Deployment: config.DeploymentConfig{Role: "collector", WriterLeaseKey: "source-a/trades", WriterID: "writer-a"},
		Capture:    boundedCapture(),
		Runtime:    config.RuntimeConfig{ShutdownTimeout: 1},
		ObjectStore: config.ObjectStoreConfig{
			Endpoint: "https://objects.example.test", Region: "test", Bucket: "bucket", CredentialRef: "object-ref",
		},
		Catalog: config.CatalogConfig{DSNRef: "catalog-ref", ServerMajors: []int{17}},
		Sources: []config.SourceConfig{{
			ID: "source-a", API: "binance-spot", Endpoints: []string{"wss://stream.example.test"},
			Channels: []string{"trades"}, Families: []string{"trade"},
		}},
	}
	root := New(Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) {
			order = append(order, "load")
			return cfg, nil
		},
		ValidateSecret: func(context.Context, string) error {
			order = append(order, "secret")
			return nil
		},
		Compose: func(context.Context, string, config.Config, BuildInfo, io.Writer) (Runtime, error) {
			order = append(order, "compose")
			return &orderedRuntime{order: &order}, nil
		},
		Run: func(context.Context, string, config.Config, Runtime, io.Writer) error {
			order = append(order, "run")
			return nil
		},
	})
	root.SetArgs([]string{"collect", "--config", "declared.yaml"})
	if err := root.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(order, ","), "load,secret,secret,compose,run,shutdown"; got != want {
		t.Fatalf("startup order = %q, want %q", got, want)
	}
}

func TestCollectRejectsMissingPressureBeforeComposition(t *testing.T) {
	composed := false
	cfg := config.Config{
		Deployment:  config.DeploymentConfig{Role: "collector", WriterLeaseKey: "source-a/trades", WriterID: "writer-a"},
		ObjectStore: config.ObjectStoreConfig{Endpoint: "https://objects.example.test", CredentialRef: "OBJECT_REF"},
		Catalog:     config.CatalogConfig{DSNRef: "CATALOG_REF", ServerMajors: []int{17}},
		Sources:     []config.SourceConfig{{ID: "source-a"}},
	}
	root := New(Dependencies{
		LoadConfig:     func(string, config.Overrides) (config.Config, error) { return cfg, nil },
		ValidateSecret: func(context.Context, string) error { return nil },
		Compose: func(context.Context, string, config.Config, BuildInfo, io.Writer) (Runtime, error) {
			composed = true
			return &orderedRuntime{}, nil
		},
		Run: func(context.Context, string, config.Config, Runtime, io.Writer) error { return nil },
	})
	root.SetArgs([]string{"collect", "--config", "declared.yaml"})
	err := root.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), "bounded capture pressure") {
		t.Fatalf("ExecuteContext(collect) error = %v, want pressure precondition", err)
	}
	if composed {
		t.Fatal("collector composition ran without bounded capture pressure")
	}
}

func TestVerifyVenueUsesVerifierLifecycle(t *testing.T) {
	var order []string
	cfg := verifyVenueTestConfig()
	root := New(Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) {
			order = append(order, "load")
			return cfg, nil
		},
		Compose: func(_ context.Context, operation string, _ config.Config, _ BuildInfo, _ io.Writer) (Runtime, error) {
			if operation != "verify venue" {
				t.Fatalf("composition operation = %q, want verify venue", operation)
			}
			order = append(order, "compose")
			return &orderedRuntime{order: &order}, nil
		},
		VerifyVenue: func(_ context.Context, venue string, _ config.Config, runtime Runtime, _ io.Writer) error {
			if venue != "bybit-v5" || runtime == nil {
				t.Fatalf("verifier runner received venue %q and runtime %T", venue, runtime)
			}
			order = append(order, "verify")
			return nil
		},
	})
	root.SetArgs([]string{"verify", "venue", "--config", "declared.yaml", "--venue", "bybit-v5"})
	if err := root.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(order, ","), "load,compose,verify,shutdown"; got != want {
		t.Fatalf("verify venue lifecycle = %q, want %q", got, want)
	}
}

func TestVerifyVenueRejectsWrongRoleBeforeComposition(t *testing.T) {
	cfg := verifyVenueTestConfig()
	cfg.Deployment.Role = "collector"
	composed := false
	verified := false
	root := New(Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) { return cfg, nil },
		Compose: func(context.Context, string, config.Config, BuildInfo, io.Writer) (Runtime, error) {
			composed = true
			return &orderedRuntime{}, nil
		},
		VerifyVenue: func(context.Context, string, config.Config, Runtime, io.Writer) error {
			verified = true
			return nil
		},
	})
	root.SetArgs([]string{"verify", "venue", "--config", "declared.yaml", "--venue", "bybit-v5"})
	err := root.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), "cannot run") {
		t.Fatalf("ExecuteContext(verify venue) error = %v, want role denial", err)
	}
	if composed || verified {
		t.Fatalf("wrong-role verifier reached effects: composed=%t verified=%t", composed, verified)
	}
}

func TestVerifyVenueRejectsLiveAcquisitionBeforeComposition(t *testing.T) {
	cfg := verifyVenueTestConfig()
	cfg.Verify.Mode = config.VerifyModeLive
	composed := false
	verified := false
	root := New(Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) { return cfg, nil },
		Compose: func(context.Context, string, config.Config, BuildInfo, io.Writer) (Runtime, error) {
			composed = true
			return &orderedRuntime{}, nil
		},
		VerifyVenue: func(context.Context, string, config.Config, Runtime, io.Writer) error {
			verified = true
			return nil
		},
	})
	root.SetArgs([]string{"verify", "venue", "--config", "declared.yaml", "--venue", "bybit-v5"})
	err := root.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), "collector role") {
		t.Fatalf("ExecuteContext(verify venue live) error = %v, want collector-role denial", err)
	}
	if composed || verified {
		t.Fatalf("live verifier reached effects: composed=%t verified=%t", composed, verified)
	}
}

func verifyVenueTestConfig() config.Config {
	return config.Config{
		Runtime:    config.RuntimeConfig{ShutdownTimeout: time.Second},
		Deployment: config.DeploymentConfig{Role: "verifier"},
		Verify: config.VerifyConfig{
			Mode:            config.VerifyModeFixture,
			FixtureRoot:     "/fixture",
			FixtureManifest: "/fixture/manifest.json",
			SpoolRoot:       "/spool",
			ArtifactRoot:    "/artifacts",
			MaxMessages:     64,
			MaxBytes:        4 << 20,
			MaxDuration:     10 * time.Second,
			DepthLimit:      100,
		},
		Sources: []config.SourceConfig{{
			API: "bybit-v5",
			Endpoints: []string{
				"https://api.bybit.com",
				"wss://stream.bybit.com/v5/public/inverse",
				"wss://stream.bybit.com/v5/public/linear",
				"wss://stream.bybit.com/v5/public/spot",
			},
			Channels: []string{
				"allLiquidation.{symbol}",
				"orderbook.1.{symbol}",
				"orderbook.full.{symbol}",
				"orderbook.rpi.{symbol}",
				"orderbook.{depth}.{symbol}",
				"publicTrade.{symbol}",
				"tickers.{symbol}",
			},
			Symbols: []string{"BTCUSDT"},
		}},
	}
}

func TestReleaseVerifyCommandWiring(t *testing.T) {
	var got ReleaseVerifyOptions
	root := New(Dependencies{
		VerifyRelease: func(_ context.Context, options ReleaseVerifyOptions, _ io.Writer) error {
			got = options
			return nil
		},
	})
	root.SetArgs([]string{
		"release", "verify",
		"--amd64", "dist/enable-market-linux-amd64",
		"--arm64", "dist/enable-market-linux-arm64",
		"--licenses", "release/license-policy.json",
		"--evidence", "dist/release-evidence.json",
	})
	if err := root.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got.AMD64Binary == "" || got.ARM64Binary == "" || got.LicensePolicy == "" || got.EvidenceOutput == "" {
		t.Fatalf("release options were not wired: %+v", got)
	}
}

func TestSmokeCommandWiring(t *testing.T) {
	var roles []string
	root := New(Dependencies{
		SmokeRole: func(_ context.Context, role string, _ io.Writer) error {
			roles = append(roles, role)
			return nil
		},
	})
	root.SetArgs([]string{"smoke", "--role", "all"})
	if err := root.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(roles) != 8 {
		t.Fatalf("smoke roles = %v, want all eight deployment roles", roles)
	}
}

func boundedCapture() config.CaptureConfig {
	return config.CaptureConfig{
		DecodeQueueCapacity: 16, DurableQueueCapacity: 16,
		DecodeHighWater: 12, DurableHighWater: 12,
		DecodeLowWater: 4, DurableLowWater: 4,
		MaxRawMessageBytes: 1 << 20, PendingRESTCapacity: 8,
	}
}

func testDependencies() Dependencies {
	return Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) {
			return config.Config{}, config.ErrPathRequired
		},
		Run: func(context.Context, string, config.Config, Runtime, io.Writer) error { return nil },
	}
}
