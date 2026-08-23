package main

import (
	"io"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/warehouse"
)

func TestDefaultModeUsesPinnedReleaseGate(t *testing.T) {
	config, err := parseOptions([]string{"-dsn=clickhouse://fixture.invalid/test", "-dataset-root=/synthetic"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	selection := warehouse.PinnedX5ProductionSelection()
	if config.mode != modeVerify || config.serverDigest != selection.ServerDigest || config.selection != selection {
		t.Fatalf("default command did not select the pinned release gate: %#v", config)
	}
}

func TestMeasureModeMustBeExplicit(t *testing.T) {
	config, err := parseOptions([]string{"-mode=measure", "-dsn=clickhouse://fixture.invalid/test",
		"-dataset-root=/synthetic", "-server-digest=synthetic-measurement"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if config.mode != modeMeasure || config.serverDigest != "synthetic-measurement" {
		t.Fatalf("explicit measure mode was not retained: %#v", config)
	}
}

func TestVerifyModeRejectsUnpinnedDigest(t *testing.T) {
	_, err := parseOptions([]string{"-dsn=clickhouse://fixture.invalid/test", "-dataset-root=/synthetic",
		"-server-digest=synthetic-measurement"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "requires pinned server digest") {
		t.Fatalf("unpinned default gate error = %v", err)
	}
}
