package config

import (
	"os"
	"path/filepath"
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
			yaml: "sources:\n  - id: spot\n    api: binance-spot\n    endpoints: [wss://example.test/ws]\n  - id: spot\n    api: binance-spot\n    endpoints: [wss://example.test/ws]\n",
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
			yaml: "object_store:\n  endpoint: https://user:pass@example.test\n  region: test\n  bucket: bucket\n  credential_ref: secret-ref\n",
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

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
