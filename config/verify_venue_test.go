package config

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

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

func TestVerifyVenueRejectsScopeWidening(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "testdata", "config", "binance-spot-verify.yaml"), nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Sources[0].Symbols = []string{"A", "B", "C", "D", "E"}
	if err := cfg.ValidateVerifyVenue(context.Background(), "binance-spot", nil); err == nil {
		t.Fatal("fixture accepted more than four symbols")
	}
	cfg, err = Load(filepath.Join("..", "testdata", "config", "binance-spot-verify.yaml"), nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Sources[0].Endpoints[0] = "https://api.binance.com"
	if err := cfg.ValidateVerifyVenue(context.Background(), "binance-spot", nil); err == nil {
		t.Fatal("fixture accepted a trading-capable endpoint")
	}
}
