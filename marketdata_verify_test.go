package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/config"
)

func TestVerifyVenueCommandExecutesFixtureTwice(t *testing.T) {
	configPath := commandFixtureConfig(t)
	run := func() []byte {
		t.Helper()
		command := newCommand()
		var output bytes.Buffer
		command.SetOut(&output)
		command.SetErr(&output)
		command.SetArgs([]string{"verify", "venue", "--config", configPath, "--venue", "binance-spot"})
		if err := command.ExecuteContext(t.Context()); err != nil {
			t.Fatal(err)
		}
		return bytes.Clone(output.Bytes())
	}
	first := run()
	second := run()
	if !bytes.Equal(first, second) || !bytes.HasSuffix(first, []byte("\n")) {
		t.Fatal("fixture command did not return identical canonical evidence bytes")
	}
}

func TestDerivativeVenueCommandsExecuteFixturesTwice(t *testing.T) {
	for _, test := range []struct {
		venue string
		file  string
	}{
		{venue: "binance-usdm", file: "binance-usdm-verify.yaml"},
		{venue: "binance-coinm", file: "binance-coinm-verify.yaml"},
		{venue: "bybit-v5", file: "bybit-v5-verify.yaml"},
	} {
		t.Run(test.venue, func(t *testing.T) {
			run := func() []byte {
				t.Helper()
				command := newCommand()
				var output bytes.Buffer
				command.SetOut(&output)
				command.SetErr(&output)
				command.SetArgs([]string{"verify", "venue", "--config", filepath.Join("testdata", "config", test.file), "--venue", test.venue})
				if err := command.ExecuteContext(t.Context()); err != nil {
					t.Fatal(err)
				}
				return bytes.Clone(output.Bytes())
			}
			first := run()
			second := run()
			if !bytes.Equal(first, second) || !bytes.HasSuffix(first, []byte("\n")) {
				t.Fatal("fixture command did not return identical canonical evidence bytes")
			}
		})
	}
}

func TestDerivativeFixtureManifestRejectsConfiguredRootMismatch(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(outside, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Verify: config.VerifyConfig{FixtureRoot: root, FixtureManifest: outside}}
	if _, err := derivativeFixtureManifest(cfg); err == nil {
		t.Fatal("fixture manifest outside the configured root was accepted")
	}
}

func TestVerifyVenueLiveFailsClosedBeforeNetwork(t *testing.T) {
	t.Setenv("ELMD014_POSTGRES_DSN", "")
	t.Setenv("ELMD014_S3_CREDENTIALS", "")
	command := newCommand()
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"verify", "venue", "--config", filepath.Join("testdata", "config", "binance-spot-live.yaml"), "--venue", "binance-spot"})
	if err := command.ExecuteContext(t.Context()); err == nil {
		t.Fatal("live verification accepted absent caller bindings")
	}
	if output.Len() != 0 {
		t.Fatalf("fail-closed command emitted output: %q", output.String())
	}
}

func TestLiveCredentialDecodeDoesNotEchoSecret(t *testing.T) {
	const secret = "DO-NOT-ECHO-THIS-CREDENTIAL"
	t.Setenv("ELMD014_TEST_CREDENTIALS", secret)
	_, err := loadAWSCredentials("ELMD014_TEST_CREDENTIALS")
	if err == nil {
		t.Fatal("malformed credential JSON was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("credential value appeared in error text")
	}
}

func commandFixtureConfig(t *testing.T) string {
	t.Helper()
	testdata, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	spool := filepath.Join(root, "spool")
	artifacts := filepath.Join(root, "artifacts")
	if err := os.Mkdir(spool, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Join("testdata", "config", "binance-spot-verify.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	contents = strings.Replace(contents, "  fixture_root: ..", "  fixture_root: "+testdata, 1)
	contents = strings.Replace(contents, "  fixture_manifest: ../verify/binance-spot/fixture.json", "  fixture_manifest: "+filepath.Join(testdata, "verify", "binance-spot", "fixture.json"), 1)
	contents = strings.Replace(contents, "  spool_root: ../verify/binance-spot/runtime/spool", "  spool_root: "+spool, 1)
	contents = strings.Replace(contents, "  artifact_root: ../verify/binance-spot/runtime/artifacts", "  artifact_root: "+artifacts, 1)
	path := filepath.Join(root, "verify.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
