package release

import (
	"strings"
	"testing"
)

// TestRelease validates typed release metadata only. The coordinator command
// inspects the real caller-built linux binaries and is the cross-build proof.
func TestRelease(t *testing.T) {
	identity, err := Identity()
	if err != nil {
		t.Fatal(err)
	}
	if identity.CertifiedTuples == nil || len(identity.CertifiedTuples) != 0 {
		t.Fatalf("embedded certified tuple set = %#v, want explicit empty set", identity.CertifiedTuples)
	}
	missingSet := identity
	missingSet.CertifiedTuples = nil
	if err := validateIdentity(missingSet); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("validateIdentity() error = %v, want missing certified tuple set rejection", err)
	}
	premature := identity
	premature.CertifiedTuples = []string{"binance-spot"}
	if err := validateIdentity(premature); err == nil || !strings.Contains(err.Error(), "must remain empty") {
		t.Fatalf("validateIdentity() error = %v, want premature support rejection", err)
	}

	provenance := Provenance{
		GoVersion: "go1.25.7", ModulePath: "github.com/enable-xyz/marketdata", ModuleValue: "(devel)",
		Revision: strings.Repeat("a", 40), RevisionAt: "2026-08-23T00:00:00Z", Trimpath: true,
		BuildMode: "exe", Compiler: "gc",
	}
	dependencies := []Module{
		{Path: "example.test/apache", Version: "v1.0.0", Sum: "h1:apache"},
		{Path: "example.test/mit", Version: "v2.0.0", Sum: "h1:mit"},
	}
	binary := func(target, architecture string) BinaryMetadata {
		return BinaryMetadata{
			Target: target, SHA256: strings.Repeat("b", 64), GOOS: "linux", GOARCH: architecture,
			CGOEnabled: "0", Identity: identity, Provenance: provenance,
			Dependencies: append([]Module(nil), dependencies...),
		}
	}
	policy := LicensePolicy{
		Format: PolicyFormat, AllowedExpressions: []string{"Apache-2.0", "MIT"},
		Modules: []LicenseRule{
			{Module: "example.test/apache", Version: "v1.0.0", Sum: "h1:apache", License: "Apache-2.0"},
			{Module: "example.test/mit", Version: "v2.0.0", Sum: "h1:mit", License: "MIT"},
		},
	}

	evidence, err := verifyMetadata(binary("linux/amd64", "amd64"), binary("linux/arm64", "arm64"), policy)
	if err != nil {
		t.Fatalf("verifyMetadata() error = %v", err)
	}
	if evidence.Format != EvidenceFormat || len(evidence.Binaries) != 2 || len(evidence.Dependencies) != 2 {
		t.Fatalf("release evidence is incomplete: %+v", evidence)
	}
	for i, licensed := range evidence.Dependencies {
		dependency := dependencies[i]
		if licensed.Path != dependency.Path || licensed.Version != dependency.Version || licensed.Sum != dependency.Sum {
			t.Fatalf("licensed dependency %d = %+v, want exact tuple %+v", i, licensed, dependency)
		}
	}

	t.Run("CGO release metadata", func(t *testing.T) {
		arm64 := binary("linux/arm64", "arm64")
		arm64.CGOEnabled = "1"
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), arm64, policy); err == nil || !strings.Contains(err.Error(), "CGO_ENABLED=0") {
			t.Fatalf("verifyMetadata() error = %v, want CGO rejection", err)
		}
	})

	t.Run("cross-architecture dependency drift", func(t *testing.T) {
		arm64 := binary("linux/arm64", "arm64")
		arm64.Dependencies[0].Version = "v1.0.1"
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), arm64, policy); err == nil || !strings.Contains(err.Error(), "dependency inventory differs") {
			t.Fatalf("verifyMetadata() error = %v, want dependency drift", err)
		}
	})

	t.Run("dependency version drifts from reviewed rule", func(t *testing.T) {
		amd64 := binary("linux/amd64", "amd64")
		arm64 := binary("linux/arm64", "arm64")
		amd64.Dependencies[0].Version = "v1.0.1"
		arm64.Dependencies[0].Version = "v1.0.1"
		if _, err := verifyMetadata(amd64, arm64, policy); err == nil || !strings.Contains(err.Error(), "does not match reviewed license tuple") {
			t.Fatalf("verifyMetadata() error = %v, want stale version rejection", err)
		}
	})

	t.Run("dependency checksum drifts from reviewed rule", func(t *testing.T) {
		amd64 := binary("linux/amd64", "amd64")
		arm64 := binary("linux/arm64", "arm64")
		amd64.Dependencies[0].Sum = "h1:changed-bytes"
		arm64.Dependencies[0].Sum = "h1:changed-bytes"
		if _, err := verifyMetadata(amd64, arm64, policy); err == nil || !strings.Contains(err.Error(), "does not match reviewed license tuple") {
			t.Fatalf("verifyMetadata() error = %v, want stale checksum rejection", err)
		}
	})

	t.Run("missing license rule", func(t *testing.T) {
		missing := policy
		missing.Modules = append([]LicenseRule(nil), policy.Modules[:1]...)
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), binary("linux/arm64", "arm64"), missing); err == nil || !strings.Contains(err.Error(), "no reviewed license rule") {
			t.Fatalf("verifyMetadata() error = %v, want missing license rejection", err)
		}
	})

	t.Run("extra license rule", func(t *testing.T) {
		extra := policy
		extra.Modules = append([]LicenseRule(nil), policy.Modules...)
		extra.Modules = append(extra.Modules, LicenseRule{
			Module: "example.test/extra", Version: "v3.0.0", Sum: "h1:extra", License: "MIT",
		})
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), binary("linux/arm64", "arm64"), extra); err == nil || !strings.Contains(err.Error(), "extra rule") {
			t.Fatalf("verifyMetadata() error = %v, want extra rule rejection", err)
		}
	})

	t.Run("duplicate license rule", func(t *testing.T) {
		duplicate := policy
		duplicate.Modules = append([]LicenseRule(nil), policy.Modules...)
		duplicate.Modules = append(duplicate.Modules, policy.Modules[0])
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), binary("linux/arm64", "arm64"), duplicate); err == nil || !strings.Contains(err.Error(), "duplicate license rule") {
			t.Fatalf("verifyMetadata() error = %v, want duplicate rule rejection", err)
		}
	})

	t.Run("incomplete license tuple", func(t *testing.T) {
		incomplete := policy
		incomplete.Modules = append([]LicenseRule(nil), policy.Modules...)
		incomplete.Modules[0].Sum = ""
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), binary("linux/arm64", "arm64"), incomplete); err == nil || !strings.Contains(err.Error(), "incomplete") {
			t.Fatalf("verifyMetadata() error = %v, want incomplete tuple rejection", err)
		}
	})

	t.Run("disallowed license", func(t *testing.T) {
		disallowed := policy
		disallowed.Modules = append([]LicenseRule(nil), policy.Modules...)
		disallowed.Modules[0].License = "GPL-3.0-only"
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), binary("linux/arm64", "arm64"), disallowed); err == nil || !strings.Contains(err.Error(), "disallowed license") {
			t.Fatalf("verifyMetadata() error = %v, want disallowed license", err)
		}
	})
}
