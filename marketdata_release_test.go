package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/deployment"
)

func TestRoleBoundSmoke(t *testing.T) {
	for _, role := range deployment.Roles() {
		var output bytes.Buffer
		if err := runSmokeRole(t.Context(), string(role), &output); err != nil {
			t.Fatalf("runSmokeRole(%s) error = %v", role, err)
		}
		if !strings.Contains(output.String(), deployment.SmokeFormat) || !strings.Contains(output.String(), string(role)) {
			t.Fatalf("runSmokeRole(%s) omitted policy evidence: %s", role, output.String())
		}
	}
}

func TestRoleDispatchRejectsNonDryRun(t *testing.T) {
	cfg := config.Config{Deployment: config.DeploymentConfig{Role: string(deployment.RoleVerifier)}}
	err := runRole(t.Context(), string(deployment.RoleVerifier), cfg, nil, new(bytes.Buffer))
	if err == nil || !strings.Contains(err.Error(), "configured production implementation") {
		t.Fatalf("runRole() error = %v, want fail-closed production dispatch", err)
	}
}

type verifierTestRuntime struct{}

func (verifierTestRuntime) DeploymentRole() deployment.Role { return deployment.RoleVerifier }

func (verifierTestRuntime) Shutdown(context.Context) error { return nil }

func TestVerifierRunnerRejectsLiveAcquisitionPreEffect(t *testing.T) {
	cfg := config.Config{Verify: config.VerifyConfig{Mode: config.VerifyModeLive}}
	var output bytes.Buffer
	err := runVerifyVenue(t.Context(), "binance-spot", cfg, verifierTestRuntime{}, &output)
	if err == nil || !strings.Contains(err.Error(), "collector role") {
		t.Fatalf("runVerifyVenue(live) error = %v, want collector-role boundary", err)
	}
	if output.Len() != 0 {
		t.Fatalf("live verifier emitted output before denial: %q", output.String())
	}
}

type unscopedVerifierTestRuntime struct{}

func (unscopedVerifierTestRuntime) Shutdown(context.Context) error { return nil }

func TestVerifierRunnerRejectsUnscopedRuntimePreEffect(t *testing.T) {
	cfg := config.Config{Verify: config.VerifyConfig{Mode: config.VerifyModeFixture}}
	var output bytes.Buffer
	err := runVerifyVenue(t.Context(), "binance-spot", cfg, unscopedVerifierTestRuntime{}, &output)
	if err == nil || !strings.Contains(err.Error(), "verifier-scoped") {
		t.Fatalf("runVerifyVenue(unscoped) error = %v, want role boundary", err)
	}
	if output.Len() != 0 {
		t.Fatalf("unscoped verifier emitted output before denial: %q", output.String())
	}
}
