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

func TestLoadProductionSliceFromYAMLEnvironmentAndFlagOverrides(t *testing.T) {
	path := writeConfig(t, `capture:
  spool_root: runtime/spool
  frame_bytes: 4194304
  segment_max_bytes: 67108864
  segment_max_age: 30m
  depth_snapshot_limit: 100
  depth_snapshot_cadence: 5m
  reconnect_delay: 1s
  decode_queue_capacity: 16
  durable_queue_capacity: 16
  decode_high_water: 12
  durable_high_water: 12
  decode_low_water: 4
  durable_low_water: 4
  max_raw_message_bytes: 1048576
  pending_rest_capacity: 8
dataset:
  working_root: runtime/dataset
  build_cadence: 10m
serve:
  listener: query.invalid:8443
  tls_cert_ref: MARKETDATA_TLS_CERT
  tls_key_ref: MARKETDATA_TLS_KEY
  paging_key_ref: MARKETDATA_PAGING_KEY
  principals:
    - id: dashboard
      token_ref: MARKETDATA_DASHBOARD_TOKEN
      scopes: [catalog:read, coverage:read, query:read, replay:native, replay:normalized, metrics:read]
  max_query_interval: 12h
  page_token_ttl: 5m
  read_header_timeout: 3s
`)
	t.Setenv("ENABLE_MARKET_CAPTURE_SEGMENT_MAX_AGE", "20m")
	t.Setenv("ENABLE_MARKET_SERVE_PRINCIPALS", `[{"id":"environment-dashboard","token_ref":"ENV_DASHBOARD_TOKEN","scopes":["metrics:read"]}]`)

	cfg, err := Load(path, Overrides{
		"dataset.build_cadence": 15 * time.Minute,
		"serve.page_token_ttl":  3 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	configDir := filepath.Dir(path)
	if got, want := cfg.Capture.SpoolRoot, filepath.Join(configDir, "runtime", "spool"); got != want {
		t.Fatalf("capture spool root = %q, want %q", got, want)
	}
	if got, want := cfg.Dataset.WorkingRoot, filepath.Join(configDir, "runtime", "dataset"); got != want {
		t.Fatalf("dataset working root = %q, want %q", got, want)
	}
	if got, want := cfg.Capture.SegmentMaxAge, 20*time.Minute; got != want {
		t.Fatalf("environment segment age = %v, want %v", got, want)
	}
	if got, want := cfg.Dataset.BuildCadence, 15*time.Minute; got != want {
		t.Fatalf("flag dataset cadence = %v, want %v", got, want)
	}
	if got, want := cfg.Serve.PageTokenTTL, 3*time.Minute; got != want {
		t.Fatalf("flag page token TTL = %v, want %v", got, want)
	}
	if got, want := cfg.Serve.MaxQueryInterval, 12*time.Hour; got != want {
		t.Fatalf("YAML query interval = %v, want %v", got, want)
	}
	if len(cfg.Serve.Principals) != 1 || cfg.Serve.Principals[0].ID != "environment-dashboard" ||
		!slices.Equal(cfg.Serve.Principals[0].Scopes, []string{"metrics:read"}) {
		t.Fatalf("environment principals = %#v", cfg.Serve.Principals)
	}
	if err := os.Unsetenv("ENABLE_MARKET_SERVE_PRINCIPALS"); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path, nil)
	if err != nil {
		t.Fatalf("Load() YAML principal error = %v", err)
	}
	if len(cfg.Serve.Principals) != 1 || cfg.Serve.Principals[0].ID != "dashboard" ||
		!slices.Equal(cfg.Serve.Principals[0].Scopes, []string{"catalog:read", "coverage:read", "query:read", "replay:native", "replay:normalized", "metrics:read"}) {
		t.Fatalf("YAML principals = %#v", cfg.Serve.Principals)
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

func TestCaptureProductionBounds(t *testing.T) {
	base, err := Load(writeConfig(t, `capture:
  spool_root: spool
  frame_bytes: 4194304
  segment_max_bytes: 67108864
  segment_max_age: 5m
  depth_snapshot_limit: 100
  depth_snapshot_cadence: 1m
  reconnect_delay: 1s
  decode_queue_capacity: 16
  durable_queue_capacity: 16
  decode_high_water: 12
  durable_high_water: 12
  decode_low_water: 4
  durable_low_water: 4
  max_raw_message_bytes: 1048576
  pending_rest_capacity: 8
`), nil)
	if err != nil {
		t.Fatalf("Load() valid collector capture error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"caller-owned absolute spool path", func(c *Config) { c.Capture.SpoolRoot = "relative/spool" }, "absolute path"},
		{"frame below collector minimum", func(c *Config) { c.Capture.FrameBytes = 1 << 20 }, "capture.frame_bytes"},
		{"frame above format maximum", func(c *Config) { c.Capture.FrameBytes = 32 << 20 }, "capture.frame_bytes"},
		{"non-power-of-two frame", func(c *Config) { c.Capture.FrameBytes = 3 << 20 }, "capture.frame_bytes"},
		{"depth limit omitted", func(c *Config) { c.Capture.DepthSnapshotLimit = 0 }, "depth_snapshot_limit"},
		{"depth limit exceeds venue bound", func(c *Config) { c.Capture.DepthSnapshotLimit = 5_001 }, "depth_snapshot_limit"},
		{"depth cadence omitted", func(c *Config) { c.Capture.DepthSnapshotCadence = 0 }, "depth_snapshot_cadence"},
		{"reconnect delay too short", func(c *Config) { c.Capture.ReconnectDelay = 99 * time.Millisecond }, "reconnect_delay"},
		{"raw message exceeds framing headroom", func(c *Config) {
			c.Capture.MaxRawMessageBytes = c.Capture.FrameBytes - (128 << 10) + 1
		}, "framing headroom"},
		{"spool cannot reserve active epoch", func(c *Config) {
			c.Runtime.SpoolMaxBytes = 2*c.Capture.SegmentMaxBytes + 2*int64(c.Capture.FrameBytes) - 1
		}, "two capture segments and two framing buffers"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.want)
			}
		})
	}

	base.Runtime.SpoolMaxBytes = 2*base.Capture.SegmentMaxBytes + 2*int64(base.Capture.FrameBytes)
	if err := base.Validate(); err != nil {
		t.Fatalf("Validate() exact spool reservation error = %v", err)
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
			TLSCertRef: "FIRST_CERT_SECRET", TLSKeyRef: "FIRST_KEY_SECRET", PagingKeyRef: "FIRST_PAGING_SECRET",
			Principals: []ServePrincipalConfig{{ID: "dashboard", TokenRef: "FIRST_BEARER_SECRET", Scopes: []string{"metrics:read"}}},
		},
		Telemetry: TelemetryConfig{TraceExporterRef: "FIRST_TRACE_SECRET"},
	}
	second := first
	second.ObjectStore.CredentialRef = "SECOND_OBJECT_SECRET"
	second.Catalog.DSNRef = "SECOND_DATABASE_SECRET"
	second.Warehouse.DSNRef = "SECOND_WAREHOUSE_SECRET"
	second.Sources = slices.Clone(first.Sources)
	second.Sources[0].EntitlementRef = "SECOND_VENUE_SECRET"
	second.Serve.Principals = []ServePrincipalConfig{{ID: "dashboard", TokenRef: "SECOND_BEARER_SECRET", Scopes: []string{"metrics:read"}}}
	second.Serve.TLSCertRef = "SECOND_CERT_SECRET"
	second.Serve.TLSKeyRef = "SECOND_KEY_SECRET"
	second.Serve.PagingKeyRef = "SECOND_PAGING_SECRET"
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

func TestCollectorRoleRequiresProductionCaptureContract(t *testing.T) {
	base := Config{
		Runtime: RuntimeConfig{SpoolMaxBytes: 1 << 30},
		Deployment: DeploymentConfig{
			Role: "collector", WriterLeaseKey: "source/channel", WriterID: "writer",
		},
		Capture: CaptureConfig{
			SpoolRoot: t.TempDir(), FrameBytes: 4 << 20, SegmentMaxBytes: 64 << 20, SegmentMaxAge: 5 * time.Minute,
			DepthSnapshotLimit: 100, DepthSnapshotCadence: time.Minute, ReconnectDelay: time.Second,
			DecodeQueueCapacity: 16, DurableQueueCapacity: 16, DecodeHighWater: 12, DurableHighWater: 12,
			DecodeLowWater: 4, DurableLowWater: 4, MaxRawMessageBytes: 1 << 20, PendingRESTCapacity: 8,
		},
		ObjectStore: ObjectStoreConfig{
			Endpoint: "https://objects.example.test", Region: "test", Bucket: "market", CredentialRef: "OBJECT_CREDENTIAL",
		},
		Catalog: CatalogConfig{DSNRef: "CATALOG_DSN", ServerMajors: []int{17}},
		Sources: []SourceConfig{{
			ID: "binance-spot", API: "binance-spot",
			Endpoints: []string{"https://data-api.binance.vision", "wss://data-stream.binance.vision/ws"},
			Methods:   []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
			Symbols:   []string{"BTCUSDT"},
		}},
	}
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"framing", func(c *Config) { c.Capture.FrameBytes = 0 }, "capture.frame_bytes"},
		{"depth", func(c *Config) { c.Capture.DepthSnapshotLimit = 0 }, "depth_snapshot_limit"},
		{"reconnect", func(c *Config) { c.Capture.ReconnectDelay = 0 }, "reconnect_delay"},
		{"spool path", func(c *Config) { c.Capture.SpoolRoot = "relative" }, "absolute path"},
		{"single Binance Spot source", func(c *Config) { c.Sources = append(c.Sources, c.Sources[0]) }, "exactly one Binance Spot source"},
		{"explicit symbol", func(c *Config) { c.Sources[0].Symbols = nil }, "at least one explicit Binance Spot symbol"},
		{"unique symbol", func(c *Config) { c.Sources[0].Symbols = []string{"BTCUSDT", "BTCUSDT"} }, "duplicate symbol"},
		{"all live epochs", func(c *Config) {
			epochBytes := 2*c.Capture.SegmentMaxBytes + 2*int64(c.Capture.FrameBytes)
			c.Runtime.SpoolMaxBytes = 2*epochBytes - 1
		}, "websocket and every per-symbol depth epoch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			cfg.Sources = slices.Clone(base.Sources)
			cfg.Sources[0].Symbols = slices.Clone(base.Sources[0].Symbols)
			tt.mutate(&cfg)
			resolutions := 0
			err := cfg.ValidateRole(t.Context(), "collect", func(context.Context, string) error {
				resolutions++
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateRole() error = %v, want substring %q", err, tt.want)
			}
			if resolutions != 0 {
				t.Fatalf("secret resolver called %d times before structural validation", resolutions)
			}
		})
	}

	epochBytes := 2*base.Capture.SegmentMaxBytes + 2*int64(base.Capture.FrameBytes)
	base.Runtime.SpoolMaxBytes = int64(len(base.Sources[0].Symbols)+1) * epochBytes
	var resolved []string
	if err := base.ValidateRole(t.Context(), "collect", func(_ context.Context, reference string) error {
		resolved = append(resolved, reference)
		return nil
	}); err != nil {
		t.Fatalf("ValidateRole() complete collector error = %v", err)
	}
	if !slices.Equal(resolved, []string{"OBJECT_CREDENTIAL", "CATALOG_DSN"}) {
		t.Fatalf("resolved collector references = %v", resolved)
	}
}

func TestDatasetAndWarehouseRoleRequirementsFailClosed(t *testing.T) {
	base := Config{
		ObjectStore: ObjectStoreConfig{
			Endpoint: "https://objects.example.test", Region: "test", Bucket: "market", CredentialRef: "OBJECT_CREDENTIAL",
		},
		Catalog:   CatalogConfig{DSNRef: "CATALOG_DSN", ServerMajors: []int{17}},
		Warehouse: WarehouseConfig{DSNRef: "WAREHOUSE_DSN", Database: "market", ServerDigest: "sha256:declared"},
		Sources: []SourceConfig{{
			ID: "binance-spot", API: "binance-spot",
			Endpoints: []string{"https://data-api.binance.vision", "wss://data-stream.binance.vision/ws"},
			Methods:   []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		}},
		Dataset: DatasetConfig{WorkingRoot: t.TempDir(), BuildCadence: time.Minute},
	}
	tests := []struct {
		name      string
		role      string
		operation string
		mutate    func(*Config)
		want      string
	}{
		{"dataset builder source", "dataset-builder", "export parquet", func(c *Config) { c.Sources = nil }, "at least one source"},
		{"dataset builder catalog destination", "dataset-builder", "export parquet", func(c *Config) { c.Catalog.DSNRef = "" }, "catalog destination"},
		{"dataset builder catalog version", "dataset-builder", "export parquet", func(c *Config) { c.Catalog.ServerMajors = nil }, "server majors"},
		{"dataset builder absolute path", "dataset-builder", "export parquet", func(c *Config) { c.Dataset.WorkingRoot = "relative" }, "absolute dataset working_root"},
		{"warehouse loader catalog destination", "warehouse-loader", "warehouse load", func(c *Config) { c.Catalog.DSNRef = "" }, "catalog destination"},
		{"warehouse loader dataset path", "warehouse-loader", "warehouse load", func(c *Config) { c.Dataset.WorkingRoot = "" }, "dataset working_root"},
		{"warehouse loader dataset cadence", "warehouse-loader", "warehouse load", func(c *Config) { c.Dataset.BuildCadence = 0 }, "build_cadence"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			cfg.Deployment.Role = tt.role
			tt.mutate(&cfg)
			resolutions := 0
			err := cfg.ValidateRole(t.Context(), tt.operation, func(context.Context, string) error {
				resolutions++
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateRole() error = %v, want substring %q", err, tt.want)
			}
			if resolutions != 0 {
				t.Fatalf("secret resolver called %d times before structural validation", resolutions)
			}
		})
	}

	for _, tt := range []struct {
		role      string
		operation string
		want      []string
	}{
		{"dataset-builder", "export parquet", []string{"OBJECT_CREDENTIAL", "CATALOG_DSN"}},
		{"warehouse-loader", "warehouse load", []string{"OBJECT_CREDENTIAL", "CATALOG_DSN", "WAREHOUSE_DSN"}},
	} {
		t.Run(tt.role+" complete", func(t *testing.T) {
			cfg := base
			cfg.Deployment.Role = tt.role
			var resolved []string
			if err := cfg.ValidateRole(t.Context(), tt.operation, func(_ context.Context, reference string) error {
				resolved = append(resolved, reference)
				return nil
			}); err != nil {
				t.Fatalf("ValidateRole() complete config error = %v", err)
			}
			if !slices.Equal(resolved, tt.want) {
				t.Fatalf("resolved references = %v, want %v", resolved, tt.want)
			}
		})
	}
}

func TestServeRoleFailsClosedBeforeSecretResolution(t *testing.T) {
	base := Config{
		Deployment:  DeploymentConfig{Role: "query-replay-server"},
		ObjectStore: ObjectStoreConfig{Endpoint: "https://objects.example.test", Region: "test", Bucket: "market", CredentialRef: "OBJECT_CREDENTIAL"},
		Catalog:     CatalogConfig{DSNRef: "CATALOG_DSN", ServerMajors: []int{17}},
		Warehouse:   WarehouseConfig{DSNRef: "WAREHOUSE_DSN", Database: "market", ServerDigest: "sha256:declared"},
		Serve: ServeConfig{
			Listener: "query.invalid:8443", TLSCertRef: "TLS_CERT", TLSKeyRef: "TLS_KEY", PagingKeyRef: "PAGING_KEY",
			Principals:       []ServePrincipalConfig{{ID: "dashboard", TokenRef: "DASHBOARD_TOKEN", Scopes: []string{"query:read"}}},
			MaxQueryInterval: 24 * time.Hour, PageTokenTTL: 15 * time.Minute, ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
			DefaultPageRows: 1_000, MaxPageRows: 10_000, MaxResponseBytes: 16 << 20,
		},
	}
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"object binding", func(c *Config) { c.ObjectStore.CredentialRef = "" }, "object_store"},
		{"database binding", func(c *Config) { c.Catalog.DSNRef = "" }, "catalog destination"},
		{"warehouse binding", func(c *Config) { c.Warehouse.DSNRef = "" }, "warehouse destination"},
		{"TLS binding", func(c *Config) { c.Serve.TLSKeyRef = "" }, "TLS"},
		{"paging binding", func(c *Config) { c.Serve.PagingKeyRef = "" }, "paging key"},
		{"token binding", func(c *Config) { c.Serve.Principals = nil }, "principals"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			resolutions := 0
			err := cfg.ValidateRole(t.Context(), "serve", func(context.Context, string) error {
				resolutions++
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateRole() error = %v, want substring %q", err, tt.want)
			}
			if resolutions != 0 {
				t.Fatalf("secret resolver called %d times before structural validation", resolutions)
			}
		})
	}
	var resolved []string
	if err := base.ValidateRole(t.Context(), "serve", func(_ context.Context, reference string) error {
		resolved = append(resolved, reference)
		return nil
	}); err != nil {
		t.Fatalf("ValidateRole() complete config error = %v", err)
	}
	want := []string{"OBJECT_CREDENTIAL", "CATALOG_DSN", "WAREHOUSE_DSN", "TLS_CERT", "TLS_KEY", "PAGING_KEY", "DASHBOARD_TOKEN"}
	if !slices.Equal(resolved, want) {
		t.Fatalf("resolved serve references = %v, want %v", resolved, want)
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
