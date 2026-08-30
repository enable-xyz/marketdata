package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestSpotCatalogCheckCommand(t *testing.T) {
	root := New(Dependencies{})
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	root.SetArgs([]string{"catalog", "check", "--config", "../testdata/config/binance-spot.yaml"})
	if err := root.ExecuteContext(t.Context()); err != nil {
		t.Fatalf("catalog check error = %v", err)
	}
	want := `{"status":"verified","verification_scope":"in_memory_fixture_projection","source_id":"7d1e8644-35af-5fd7-b0ba-3bf1b59ac6fc","channel_count":1,"instrument_count":2,"page_count":1,"fixture_names":["active"],"raw_sha256":["94159f49ca322afa767719a8956cef7a9cea587fa1cc84dbfd94b2e1b0bc6517"],"snapshot_sha256":"1e22d5d631d4bc8142a5188665c4c6e0105918d228de719bd297454a838f006b","documentation_commit":"976cc580553890e92031b77306147c0ed1de5a46","official_fixture_sha256":"487b37f889c319f3e632ad0bd3d16557f2c8ef16ff30c19ca34a0414c8f36e2b","official_fixture_byte_length":2090,"unknown_filter_types":1,"unknown_additive_fields":3}`
	if got := strings.TrimSpace(output.String()); got != want {
		t.Fatalf("catalog check output = %s\nwant = %s", got, want)
	}
}
