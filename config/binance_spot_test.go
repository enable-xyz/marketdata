package config

import "testing"

func TestSpotCatalogCheckConfig(t *testing.T) {
	cfg, err := Load("../testdata/config/binance-spot.yaml", nil)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Catalog.Check.FixtureManifest != "../binance/manifest.json" || len(cfg.Catalog.Check.FixtureNames) != 1 || cfg.Catalog.Check.FixtureNames[0] != "active" {
		t.Fatalf("catalog check config = %+v", cfg.Catalog.Check)
	}
	if cfg.Catalog.DSNRef != "" || cfg.ObjectStore.CredentialRef != "" || cfg.Serve.Listener != "" || cfg.Telemetry.MetricsListener != "" {
		t.Fatal("offline check fixture unexpectedly declares a destination, credential, or listener")
	}
}
