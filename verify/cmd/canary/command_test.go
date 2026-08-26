package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/verify"
)

func TestCanaryCommandRequiresExplicitIdentityAndOutput(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "selector", want: "selector"},
		{name: "instrument", args: []string{"--selector", verify.CanarySelectorBybitSpot}, want: "instrument"},
		{name: "output", args: []string{"--selector", verify.CanarySelectorBybitSpot, "--instrument", "BTCUSDT"}, want: "output"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
				called = true
				return verify.CanaryReceipt{}, nil
			})
			err := executeCanary(t, dependencies, test.args...)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want required %s error", err, test.want)
			}
			if called {
				t.Fatal("runner called with missing required flag")
			}
		})
	}
}

func TestCanaryCommandSuccessWritesCanonicalExclusiveReceipt(t *testing.T) {
	output := filepath.Join(t.TempDir(), "receipt.json")
	var observed verify.CanaryConfig
	dependencies := productionDependencies()
	dependencies.newEpochID = func() (string, error) { return "public-canary-test-epoch", nil }
	dependencies.run = func(_ context.Context, config verify.CanaryConfig) (verify.CanaryReceipt, error) {
		observed = config
		return validSuccessReceipt(config.Selector, config.DurationNS), nil
	}
	if err := executeCanary(t, dependencies,
		"--selector", verify.CanarySelectorBybitSpot,
		"--instrument", "BTCUSDT",
		"--output", output,
	); err != nil {
		t.Fatal(err)
	}
	if observed.Selector != verify.CanarySelectorBybitSpot || observed.Instrument != "BTCUSDT" || observed.DEX != "" {
		t.Fatalf("identity config = %#v", observed)
	}
	if observed.DurationNS != uint64(defaultCanaryDuration) || observed.Reconnect.MaxAttempts != defaultReconnectAttempts || observed.Reconnect.BackoffNS != uint64(defaultReconnectBackoff) {
		t.Fatalf("duration/reconnect config = %#v", observed)
	}
	if observed.MaxMessageBytes != bybit.MaxRawPayloadBytes || observed.Heartbeat.IntervalNS != bybit.HeartbeatIntervalNS ||
		observed.Heartbeat.TimeoutNS != bybit.HeartbeatTimeoutNS || observed.Heartbeat.ACKTimeoutNS != bybit.SubscriptionACKTimeoutNS {
		t.Fatalf("venue bounds = %#v", observed)
	}
	if observed.Clock == nil || observed.HTTPClient == nil || observed.HTTPClient.Transport == nil || observed.HTTPClient.Timeout <= 0 {
		t.Fatalf("runtime dependencies = %#v", observed)
	}
	transport, ok := observed.HTTPClient.Transport.(*http.Transport)
	if !ok || transport == http.DefaultTransport || transport.DialContext == nil || transport.TLSHandshakeTimeout <= 0 ||
		transport.ResponseHeaderTimeout <= 0 || transport.IdleConnTimeout <= 0 || transport.MaxConnsPerHost <= 0 || transport.MaxResponseHeaderBytes <= 0 {
		t.Fatalf("HTTP transport is not an explicit bounded clone: %#v", observed.HTTPClient.Transport)
	}
	if observed.RateBudgets.BinanceDerivatives == nil || observed.RateBudgets.OKXHandshake == nil ||
		observed.RateBudgets.HyperliquidMessages == nil || observed.RateBudgets.HyperliquidConnections == nil {
		t.Fatalf("rate budgets = %#v", observed.RateBudgets)
	}
	if observed.RawSink != nil {
		t.Fatal("command configured a raw payload sink")
	}
	receipt := validSuccessReceipt(verify.CanarySelectorBybitSpot, uint64(defaultCanaryDuration))
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	actual, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, encoded) {
		t.Fatalf("receipt bytes = %q, want %q", actual, encoded)
	}
	info, err := os.Lstat(output)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("receipt mode = %v", info.Mode())
	}
}

func TestCanaryCommandScopesDEXToHIP3(t *testing.T) {
	t.Run("HIP-3 DEX", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "receipt.json")
		dependencies := testDependencies(func(_ context.Context, config verify.CanaryConfig) (verify.CanaryReceipt, error) {
			if config.DEX != "dex-a" {
				t.Fatalf("DEX = %q", config.DEX)
			}
			return validSuccessReceipt(config.Selector, config.DurationNS), nil
		})
		if err := executeCanary(t, dependencies,
			"--selector", verify.CanarySelectorHyperliquidHIP3,
			"--instrument", "BTC",
			"--dex", "dex-a",
			"--output", output,
		); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("non-HIP-3 DEX", func(t *testing.T) {
		called := false
		dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
			called = true
			return verify.CanaryReceipt{}, nil
		})
		err := executeCanary(t, dependencies,
			"--selector", verify.CanarySelectorBybitSpot,
			"--instrument", "BTCUSDT",
			"--dex", "dex-a",
			"--output", filepath.Join(t.TempDir(), "receipt.json"),
		)
		if err == nil || called {
			t.Fatalf("error = %v, runner called = %t", err, called)
		}
	})
}

func TestCanaryCommandNeverOverwritesOutput(t *testing.T) {
	output := filepath.Join(t.TempDir(), "receipt.json")
	original := []byte("operator-owned\n")
	if err := os.WriteFile(output, original, 0o600); err != nil {
		t.Fatal(err)
	}
	called := false
	dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
		called = true
		return verify.CanaryReceipt{}, nil
	})
	err := executeCanary(t, dependencies,
		"--selector", verify.CanarySelectorBybitSpot,
		"--instrument", "BTCUSDT",
		"--output", output,
	)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want existing output failure", err)
	}
	if called {
		t.Fatal("runner called even though output was already occupied")
	}
	actual, readErr := os.ReadFile(output)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(actual, original) {
		t.Fatalf("existing output changed to %q", actual)
	}
}

func TestCanaryCommandFailsOnUnexplainedInterval(t *testing.T) {
	output := filepath.Join(t.TempDir(), "receipt.json")
	failure := validFailureReceipt(verify.CanarySelectorBybitSpot)
	dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
		return failure, nil
	})
	err := executeCanary(t, dependencies,
		"--selector", verify.CanarySelectorBybitSpot,
		"--instrument", "BTCUSDT",
		"--output", output,
	)
	if !errors.Is(err, errCanaryUnsuccessful) {
		t.Fatalf("error = %v, want unsuccessful canary", err)
	}
	assertReceiptFile(t, output, failure)
}

func TestCanaryCommandRuntimeErrorReceiptPolicy(t *testing.T) {
	runtimeErr := errors.New("synthetic runtime failure")
	t.Run("no meaningful receipt", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "receipt.json")
		dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
			return verify.CanaryReceipt{}, runtimeErr
		})
		err := executeCanary(t, dependencies,
			"--selector", verify.CanarySelectorBybitSpot,
			"--instrument", "BTCUSDT",
			"--output", output,
		)
		if !errors.Is(err, runtimeErr) {
			t.Fatalf("error = %v, want runtime error", err)
		}
		if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("non-meaningful runtime failure created output: %v", statErr)
		}
	})
	t.Run("meaningful failure receipt", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "receipt.json")
		failure := validFailureReceipt(verify.CanarySelectorBybitSpot)
		dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
			return failure, runtimeErr
		})
		err := executeCanary(t, dependencies,
			"--selector", verify.CanarySelectorBybitSpot,
			"--instrument", "BTCUSDT",
			"--output", output,
		)
		if !errors.Is(err, runtimeErr) {
			t.Fatalf("error = %v, want runtime error", err)
		}
		assertReceiptFile(t, output, failure)
	})
}

func TestCanaryCommandRejectsSymlinkedOutputDirectory(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(realDirectory, link); err != nil {
		t.Fatal(err)
	}
	called := false
	dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
		called = true
		return verify.CanaryReceipt{}, nil
	})
	err := executeCanary(t, dependencies,
		"--selector", verify.CanarySelectorBybitSpot,
		"--instrument", "BTCUSDT",
		"--output", filepath.Join(link, "receipt.json"),
	)
	if err == nil || !strings.Contains(err.Error(), "real directory") {
		t.Fatalf("error = %v, want symlink rejection", err)
	}
	if called {
		t.Fatal("runner called with symlinked output directory")
	}
}

func TestCanaryCommandRejectsNonpositiveOverflowingAndUnboundedValues(t *testing.T) {
	tests := []struct {
		name  string
		flag  string
		value string
	}{
		{name: "zero duration", flag: "--duration", value: "0s"},
		{name: "duration above bound", flag: "--duration", value: "745h"},
		{name: "duration parse overflow", flag: "--duration", value: "2562048h"},
		{name: "zero reconnects", flag: "--reconnect-max-attempts", value: "0"},
		{name: "excess reconnects", flag: "--reconnect-max-attempts", value: "4097"},
		{name: "zero reconnect delay", flag: "--reconnect-backoff", value: "0s"},
		{name: "excess reconnect delay", flag: "--reconnect-backoff", value: "11m"},
		{name: "zero message bound", flag: "--max-message-bytes", value: "0"},
		{name: "excess message bound", flag: "--max-message-bytes", value: "4194305"},
		{name: "zero heartbeat interval", flag: "--heartbeat-interval", value: "0s"},
		{name: "zero heartbeat timeout", flag: "--heartbeat-timeout", value: "0s"},
		{name: "zero ACK timeout", flag: "--subscription-ack-timeout", value: "0s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			dependencies := testDependencies(func(context.Context, verify.CanaryConfig) (verify.CanaryReceipt, error) {
				called = true
				return verify.CanaryReceipt{}, nil
			})
			err := executeCanary(t, dependencies,
				"--selector", verify.CanarySelectorBybitSpot,
				"--instrument", "BTCUSDT",
				"--output", filepath.Join(t.TempDir(), "receipt.json"),
				test.flag, test.value,
			)
			if err == nil {
				t.Fatal("invalid bound was accepted")
			}
			if called {
				t.Fatal("runner called with invalid bound")
			}
		})
	}
}

func testDependencies(runner canaryRunner) commandDependencies {
	dependencies := productionDependencies()
	dependencies.run = runner
	dependencies.newEpochID = func() (string, error) { return "public-canary-test-epoch", nil }
	return dependencies
}

func executeCanary(t *testing.T, dependencies commandDependencies, args ...string) error {
	t.Helper()
	command := newCanaryCommand(dependencies)
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)
	command.SetArgs(args)
	return command.ExecuteContext(t.Context())
}

func validSuccessReceipt(selector string, durationNS uint64) verify.CanaryReceipt {
	const startedAt = int64(1_000_000)
	receipt := verify.CanaryReceipt{
		Version:                verify.CanaryReceiptVersion,
		Selector:               selector,
		StartedAtUTCNS:         startedAt,
		EndedAtUTCNS:           startedAt + int64(durationNS),
		DurationNS:             durationNS,
		Messages:               1,
		Bytes:                  2,
		RollingSHA256:          strings.Repeat("0", 64),
		SubscriptionsRequested: 1,
		SubscriptionsACKed:     1,
		HeartbeatsSent:         1,
		HeartbeatsACKed:        1,
		TerminalReason:         verify.CanaryTerminalPlannedDuration,
	}
	receipt.ReceiptSHA256 = verify.CanaryReceiptSHA256(receipt)
	return receipt
}

func validFailureReceipt(selector string) verify.CanaryReceipt {
	const startedAt = int64(1_000_000)
	receipt := verify.CanaryReceipt{
		Version:        verify.CanaryReceiptVersion,
		Selector:       selector,
		StartedAtUTCNS: startedAt,
		EndedAtUTCNS:   startedAt + int64(time.Second),
		DurationNS:     uint64(time.Second),
		RollingSHA256:  strings.Repeat("0", 64),
		UnexplainedIntervals: []verify.CanaryInterval{{
			StartedAtUTCNS: startedAt,
			EndedAtUTCNS:   startedAt + int64(time.Second),
			Reason:         "synthetic transport failure",
		}},
		TerminalReason: verify.CanaryTerminalTransportFailure,
	}
	receipt.ReceiptSHA256 = verify.CanaryReceiptSHA256(receipt)
	return receipt
}

func assertReceiptFile(t *testing.T, path string, receipt verify.CanaryReceipt) {
	t.Helper()
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, encoded) {
		t.Fatalf("receipt bytes = %q, want %q", actual, encoded)
	}
}
