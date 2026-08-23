package config

import (
	"strings"
	"testing"
)

func TestSpotCatalogCheckConfig(t *testing.T) {
	cfg, err := Load("../testdata/config/binance-spot.yaml", nil)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Catalog.Check.FixtureManifest != "../binance/manifest.json" || len(cfg.Catalog.Check.FixtureNames) != 1 || cfg.Catalog.Check.FixtureNames[0] != "active" {
		t.Fatalf("catalog check config = %+v", cfg.Catalog.Check)
	}
	if cfg.Catalog.DSNRef != "" || cfg.ObjectStore.CredentialRef != "" || cfg.Serve.Listener != "" || cfg.Telemetry.TraceExporterRef != "" {
		t.Fatal("offline check fixture unexpectedly declares a destination, credential, or listener")
	}
}

func TestPlatformCatalogCheckConfigIsPairedAndPinned(t *testing.T) {
	valid := CatalogCheckConfig{
		PlatformEvidence:             "platform-evidence.json",
		ExpectedPlatformReportSHA256: strings.Repeat("a", 64),
	}
	if err := validateOptionalCatalogCheck(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []CatalogCheckConfig{
		{PlatformEvidence: valid.PlatformEvidence},
		{ExpectedPlatformReportSHA256: valid.ExpectedPlatformReportSHA256},
		{PlatformEvidence: "platform-evidence.yaml", ExpectedPlatformReportSHA256: valid.ExpectedPlatformReportSHA256},
		{PlatformEvidence: valid.PlatformEvidence, ExpectedPlatformReportSHA256: strings.Repeat("A", 64)},
	} {
		if err := validateOptionalCatalogCheck(invalid); err == nil {
			t.Fatalf("invalid platform catalog config accepted: %#v", invalid)
		}
	}
}
