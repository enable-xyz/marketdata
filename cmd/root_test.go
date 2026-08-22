package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

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
		for _, command := range []string{"collect", "catalog", "replay", "export", "verify", "serve", "completion"} {
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
			return config.Config{}, nil
		},
		Run: func(context.Context, string, config.Config, io.Writer) error {
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
		Run: func(context.Context, string, config.Config, io.Writer) error {
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

func testDependencies() Dependencies {
	return Dependencies{
		LoadConfig: func(string, config.Overrides) (config.Config, error) {
			return config.Config{}, config.ErrPathRequired
		},
		Run: func(context.Context, string, config.Config, io.Writer) error { return nil },
	}
}
