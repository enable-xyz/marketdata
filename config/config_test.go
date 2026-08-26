package config

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoadPrecedence(t *testing.T) {
	path := writeConfig(t, "runtime:\n  shutdown_timeout: 10s\n")
	t.Setenv("ENABLE_MARKET_RUNTIME_SHUTDOWN_TIMEOUT", "20s")

	cfg, err := Load(path, Overrides{"runtime.shutdown_timeout": 30 * time.Second})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.Runtime.ShutdownTimeout, 30*time.Second; got != want {
		t.Fatalf("flag precedence = %v, want %v", got, want)
	}

	cfg, err = Load(path, nil)
	if err != nil {
		t.Fatalf("Load() with environment error = %v", err)
	}
	if got, want := cfg.Runtime.ShutdownTimeout, 20*time.Second; got != want {
		t.Fatalf("environment precedence = %v, want %v", got, want)
	}

	if err := os.Unsetenv("ENABLE_MARKET_RUNTIME_SHUTDOWN_TIMEOUT"); err != nil {
		t.Fatalf("Unsetenv() error = %v", err)
	}
	cfg, err = Load(path, nil)
	if err != nil {
		t.Fatalf("Load() from file error = %v", err)
	}
	if got, want := cfg.Runtime.ShutdownTimeout, 10*time.Second; got != want {
		t.Fatalf("file precedence = %v, want %v", got, want)
	}

	cfg, err = Load(writeConfig(t, "{}\n"), nil)
	if err != nil {
		t.Fatalf("Load() defaults error = %v", err)
	}
	if got, want := cfg.Runtime.ShutdownTimeout, 15*time.Second; got != want {
		t.Fatalf("default = %v, want %v", got, want)
	}
}

func TestLoadRejectsUnknownKey(t *testing.T) {
	_, err := Load(writeConfig(t, "runtime:\n  shutdown_timeout: 10s\n  surprise: true\n"), nil)
	if err == nil || !strings.Contains(err.Error(), "surprise") {
		t.Fatalf("Load() error = %v, want unknown-key error", err)
	}
}

func TestLoadOpportunityPoliciesBySourceAndChannel(t *testing.T) {
	cfg, err := Load(writeConfig(t, `quality:
  opportunity_policies:
    - source_id: spot
      channel_id: trades
      hot_retention: 24h
      spill_batch_rows: 1000
    - source_id: spot
      channel_id: depth
      hot_retention: 72h
      spill_batch_rows: 5000
`), nil)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := len(cfg.Quality.OpportunityPolicies), 2; got != want {
		t.Fatalf("opportunity policies = %d, want %d", got, want)
	}
	if got, want := cfg.Quality.OpportunityPolicies[1].HotRetention, 72*time.Hour; got != want {
		t.Fatalf("depth hot retention = %v, want %v", got, want)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "duplicate source id",
			yaml: "sources:\n  - id: spot\n    api: binance-spot\n    endpoints: [wss://data-stream.binance.vision/ws]\n    methods: [market-data:websocket]\n  - id: spot\n    api: binance-spot\n    endpoints: [wss://data-stream.binance.vision/ws]\n    methods: [market-data:websocket]\n",
			want: "duplicate source id",
		},
		{
			name: "unknown enum",
			yaml: "telemetry:\n  log_level: noisy\n",
			want: "unknown telemetry.log_level",
		},
		{
			name: "malformed duration",
			yaml: "runtime:\n  shutdown_timeout: tomorrow\n",
			want: "decoding configuration strictly",
		},
		{
			name: "credential in URL",
			yaml: "object_store:\n  endpoint: https://user:pass@example.test\n  region: test\n  bucket: bucket\n  credential_ref: OBJECT_SECRET_REF\n",
			want: "object_store.endpoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.yaml), nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestLoadRequiresExplicitYAMLPath(t *testing.T) {
	if _, err := Load("", nil); err != ErrPathRequired {
		t.Fatalf("Load(\"\") error = %v, want %v", err, ErrPathRequired)
	}
	if _, err := Load(filepath.Join(t.TempDir(), "config.json"), nil); err == nil {
		t.Fatal("Load(.json) succeeded, want YAML-only error")
	}
}

func TestPublicDigestExcludesSecretReferences(t *testing.T) {
	first := Config{
		ObjectStore: ObjectStoreConfig{CredentialRef: "FIRST_OBJECT_SECRET"},
		Catalog:     CatalogConfig{DSNRef: "FIRST_DATABASE_SECRET"},
		Warehouse:   WarehouseConfig{DSNRef: "FIRST_WAREHOUSE_SECRET"},
		Sources:     []SourceConfig{{ID: "source-a", EntitlementRef: "FIRST_VENUE_SECRET"}},
		Serve: ServeConfig{
			TLSCertRef: "FIRST_CERT_SECRET", TLSKeyRef: "FIRST_KEY_SECRET",
			BearerTokenRefs: map[string]string{"metrics:read": "FIRST_BEARER_SECRET"},
		},
		Telemetry: TelemetryConfig{TraceExporterRef: "FIRST_TRACE_SECRET"},
	}
	second := first
	second.ObjectStore.CredentialRef = "SECOND_OBJECT_SECRET"
	second.Catalog.DSNRef = "SECOND_DATABASE_SECRET"
	second.Warehouse.DSNRef = "SECOND_WAREHOUSE_SECRET"
	second.Sources = slices.Clone(first.Sources)
	second.Sources[0].EntitlementRef = "SECOND_VENUE_SECRET"
	second.Serve.BearerTokenRefs = map[string]string{"metrics:read": "SECOND_BEARER_SECRET"}
	second.Serve.TLSCertRef = "SECOND_CERT_SECRET"
	second.Serve.TLSKeyRef = "SECOND_KEY_SECRET"
	second.Telemetry.TraceExporterRef = "SECOND_TRACE_SECRET"
	firstDigest, err := first.PublicDigest()
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := second.PublicDigest()
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest {
		t.Fatal("public config digest depends on caller-owned secret reference names")
	}
}

func TestSecretScan(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "inline credential value",
			yaml: "object_store:\n  endpoint: https://objects.example.test\n  region: test\n  bucket: bucket\n  credential_ref: sk-live-inline-secret\n",
			want: "opaque environment reference",
		},
		{
			name: "insecure market destination",
			yaml: "sources:\n  - id: spot\n    api: binance-spot\n    endpoints: [http://data-api.binance.vision]\n    methods: [market-data:http-get]\n",
			want: "valid https/wss URL",
		},
		{
			name: "nonallowlisted destination",
			yaml: "sources:\n  - id: spot\n    api: binance-spot\n    endpoints: [wss://attacker.example/ws]\n    methods: [market-data:websocket]\n",
			want: "not an allowlisted public market-data destination",
		},
		{
			name: "private endpoint path",
			yaml: "sources:\n  - id: bybit\n    api: bybit-v5\n    endpoints: [https://api.bybit.com/v5/order/create]\n    methods: [market-data:http-get]\n",
			want: "not an allowlisted public market-data endpoint path",
		},
		{
			name: "trading capability",
			yaml: "sources:\n  - id: spot\n    api: binance-spot\n    endpoints: [wss://data-stream.binance.vision/ws]\n    methods: [trading:order]\n",
			want: "private, trading, or unknown capability",
		},
		{
			name: "redirects enabled",
			yaml: "security:\n  redirect_policy: follow\n",
			want: "must deny redirects",
		},
		{
			name: "write entitlement scope",
			yaml: "sources:\n  - id: okx\n    api: okx-v5\n    endpoints: [wss://ws.okx.com:8443/ws/v5/public]\n    methods: [market-data:websocket]\n    entitlement_ref: OKX_ENTITLEMENT\n    entitlement_scope: trading:write\n",
			want: "must be \"market-data:read\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, tt.yaml), nil); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, tt.want)
			}
		})
	}

	source := SourceConfig{
		API:       "binance-spot",
		Endpoints: []string{"https://data-api.binance.vision"},
		Methods:   []string{MethodMarketDataHTTPGet},
	}
	if err := source.AuthorizeRequest("https://data-api.binance.vision/private", MethodMarketDataHTTPGet); err == nil {
		t.Fatal("AuthorizeRequest() accepted a payload-selected destination")
	}
	if err := (Config{}).AuthorizeRedirect("https://data-api.binance.vision", "https://data-api.binance.vision"); err == nil {
		t.Fatal("AuthorizeRedirect() accepted a redirect")
	}
}

func TestOKXV5SplitSourcesAcceptReadOnlyEntitlement(t *testing.T) {
	for _, selector := range []string{"okx-v5-spot", "okx-v5-swap", "okx-v5-futures", "okx-v5-option"} {
		t.Run(selector, func(t *testing.T) {
			err := validateSources([]SourceConfig{{
				ID:               selector,
				API:              selector,
				Endpoints:        []string{"wss://ws.okx.com:8443/ws/v5/public"},
				Methods:          []string{MethodMarketDataWebSocket},
				EntitlementRef:   "OKX_ENTITLEMENT",
				EntitlementScope: EntitlementScopeReadOnly,
			}})
			if err != nil {
				t.Fatalf("validateSources() error = %v", err)
			}
		})
	}

	t.Run("unrelated public source", func(t *testing.T) {
		err := validateSources([]SourceConfig{{
			ID:               "binance",
			API:              "binance-spot",
			Endpoints:        []string{"wss://data-stream.binance.vision/ws"},
			Methods:          []string{MethodMarketDataWebSocket},
			EntitlementRef:   "BINANCE_ENTITLEMENT",
			EntitlementScope: EntitlementScopeReadOnly,
		}})
		if err == nil || !strings.Contains(err.Error(), "non-entitlement public source") {
			t.Fatalf("validateSources() error = %v, want non-entitlement public source error", err)
		}
	})
}

func TestRoleResolvesOnlyRequiredSecrets(t *testing.T) {
	cfg := Config{
		Deployment:  DeploymentConfig{Role: "migration-job"},
		Catalog:     CatalogConfig{DSNRef: "CATALOG_DSN", ServerMajors: []int{17}},
		ObjectStore: ObjectStoreConfig{Endpoint: "https://objects.example.test", CredentialRef: "UNRELATED_OBJECT"},
		Warehouse:   WarehouseConfig{DSNRef: "UNRELATED_WAREHOUSE", Database: "market", ServerDigest: "sha256:synthetic"},
		Sources:     []SourceConfig{{EntitlementRef: "UNRELATED_ENTITLEMENT"}},
	}
	var resolved []string
	err := cfg.ValidateRole(t.Context(), "migration job", func(_ context.Context, reference string) error {
		resolved = append(resolved, reference)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(resolved, []string{"CATALOG_DSN"}) {
		t.Fatalf("resolved references = %v, want only catalog binding", resolved)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
