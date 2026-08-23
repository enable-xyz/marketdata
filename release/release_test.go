package release

import (
	"os"
	"strings"
	"testing"
)

// TestRelease validates typed release identity, embedded build provenance, and
// exact license tuples. The coordinator inspects the real caller-built Linux
// binaries and supplies the cross-build proof.
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

	build := BuildProvenance{
		Version: "v1.2.3", Commit: strings.Repeat("a", 40), BuildDate: "2026-08-23T00:00:00Z",
	}
	provenance := Provenance{
		Build: build, GoVersion: "go1.25.7",
		ModulePath: "github.com/enable-xyz/marketdata", ModuleValue: "(devel)",
		VCSPresent: true, VCS: "git", Revision: build.Commit, RevisionAt: build.BuildDate,
		Trimpath: true, BuildMode: "exe", Compiler: "gc",
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

	t.Run("missing Go VCS settings", func(t *testing.T) {
		amd64 := binary("linux/amd64", "amd64")
		arm64 := binary("linux/arm64", "arm64")
		for _, metadata := range []*BinaryMetadata{&amd64, &arm64} {
			metadata.Provenance.VCSPresent = false
			metadata.Provenance.VCS = ""
			metadata.Provenance.Revision = ""
			metadata.Provenance.RevisionAt = ""
		}
		if _, err := verifyMetadata(amd64, arm64, policy); err == nil ||
			!strings.Contains(err.Error(), "missing the required VCS tuple") {
			t.Fatalf("verifyMetadata() error = %v, want missing VCS tuple rejection", err)
		}
	})

	t.Run("complete matching Go VCS settings", func(t *testing.T) {
		if _, err := verifyMetadata(
			binary("linux/amd64", "amd64"),
			binary("linux/arm64", "arm64"),
			policy,
		); err != nil {
			t.Fatalf("verifyMetadata() error = %v, want matching marker and Go VCS tuple", err)
		}
	})

	t.Run("marker differs across architectures", func(t *testing.T) {
		arm64 := binary("linux/arm64", "arm64")
		arm64.Provenance.Build.Version = "v1.2.4"
		if _, err := verifyMetadata(binary("linux/amd64", "amd64"), arm64, policy); err == nil ||
			!strings.Contains(err.Error(), "provenance differs across architectures") {
			t.Fatalf("verifyMetadata() error = %v, want cross-architecture marker rejection", err)
		}
	})

	t.Run("Go VCS tuple validation", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(*Provenance)
			want   string
		}{
			{
				name: "marker commit mismatch",
				mutate: func(p *Provenance) {
					p.VCSPresent, p.VCS = true, "git"
					p.Revision, p.RevisionAt = strings.Repeat("b", 40), build.BuildDate
				},
				want: "differs from Go VCS",
			},
			{
				name: "marker date mismatch",
				mutate: func(p *Provenance) {
					p.VCSPresent, p.VCS = true, "git"
					p.Revision, p.RevisionAt = build.Commit, "2026-08-22T00:00:00Z"
				},
				want: "differs from Go VCS",
			},
			{
				name: "dirty",
				mutate: func(p *Provenance) {
					p.VCSPresent, p.VCS = true, "git"
					p.Revision, p.RevisionAt, p.Modified = build.Commit, build.BuildDate, true
				},
				want: "modified worktree",
			},
			{
				name: "declared tuple incomplete",
				mutate: func(p *Provenance) {
					p.VCSPresent, p.VCS = true, "git"
					p.Revision, p.RevisionAt = "", ""
				},
				want: "invalid VCS tuple",
			},
			{
				name: "undeclared tuple partial",
				mutate: func(p *Provenance) {
					p.VCSPresent, p.VCS = false, ""
					p.Revision, p.RevisionAt = build.Commit, ""
				},
				want: "partial VCS tuple",
			},
			{
				name: "wrong VCS",
				mutate: func(p *Provenance) {
					p.VCSPresent, p.VCS = true, "hg"
					p.Revision, p.RevisionAt = build.Commit, build.BuildDate
				},
				want: "invalid VCS tuple",
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				amd64 := binary("linux/amd64", "amd64")
				arm64 := binary("linux/arm64", "arm64")
				test.mutate(&amd64.Provenance)
				test.mutate(&arm64.Provenance)
				if _, err := verifyMetadata(amd64, arm64, policy); err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("verifyMetadata() error = %v, want %q", err, test.want)
				}
			})
		}
	})

	t.Run("embedded marker decoding", func(t *testing.T) {
		frame := func(payload string) []byte {
			prefix, suffix := buildProvenancePrefix(), buildProvenanceSuffix()
			marked := make([]byte, 0, len(prefix)+len(payload)+len(suffix))
			marked = append(marked, prefix...)
			marked = append(marked, payload...)
			return append(marked, suffix...)
		}
		validPayload := strings.Join([]string{build.Version, build.Commit, build.BuildDate}, buildProvenanceSeparator)
		decoded, err := decodeBuildProvenance(frame(validPayload))
		if err != nil || decoded != build {
			t.Fatalf("decodeBuildProvenance() = %+v, %v, want %+v", decoded, err, build)
		}
		tests := []struct {
			name    string
			marked  []byte
			wantErr string
		}{
			{name: "safe default", marked: frame("UNSET"), wantErr: "safe default"},
			{name: "missing prefix", marked: []byte("not-framed"), wantErr: "prefix is missing"},
			{name: "missing suffix", marked: append(buildProvenancePrefix(), []byte(validPayload)...), wantErr: "suffix is missing"},
			{name: "missing field", marked: frame(build.Version + buildProvenanceSeparator + build.Commit), wantErr: "exactly version"},
			{name: "extra field", marked: frame(validPayload + buildProvenanceSeparator + "extra"), wantErr: "exactly version"},
			{name: "development version", marked: frame("v1.2.3-dev|" + build.Commit + "|" + build.BuildDate), wantErr: "non-development"},
			{name: "non-semantic version", marked: frame("release-1|" + build.Commit + "|" + build.BuildDate), wantErr: "semantic version"},
			{name: "short commit", marked: frame(build.Version + "|abc|" + build.BuildDate), wantErr: "full 40-hex"},
			{name: "uppercase commit", marked: frame(build.Version + "|" + strings.Repeat("A", 40) + "|" + build.BuildDate), wantErr: "lowercase"},
			{name: "nonhex commit", marked: frame(build.Version + "|" + strings.Repeat("z", 40) + "|" + build.BuildDate), wantErr: "40-hex"},
			{name: "non-UTC date", marked: frame(build.Version + "|" + build.Commit + "|2026-08-23T01:00:00+01:00"), wantErr: "RFC3339 UTC"},
			{name: "malformed date", marked: frame(build.Version + "|" + build.Commit + "|today"), wantErr: "RFC3339 UTC"},
			{name: "nested frame", marked: frame(validPayload + string(buildProvenancePrefix())), wantErr: "nested framing"},
			{name: "oversized", marked: append(frame(validPayload), make([]byte, maxBuildProvenanceMarkerBytes)...), wantErr: "exceeds its bound"},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if _, err := decodeBuildProvenance(test.marked); err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("decodeBuildProvenance() error = %v, want %q", err, test.wantErr)
				}
			})
		}

		scan := func(t *testing.T, contents []byte) error {
			t.Helper()
			path := t.TempDir() + "/binary"
			if err := os.WriteFile(path, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := readEmbeddedBuildProvenance(path)
			return err
		}
		validMarker := frame(validPayload)
		if err := scan(t, append(append([]byte("prefix"), validMarker...), []byte("suffix")...)); err != nil {
			t.Fatalf("readEmbeddedBuildProvenance() error = %v, want one valid marker", err)
		}
		if err := scan(t, []byte("no marker")); err == nil || !strings.Contains(err.Error(), "marker is missing") {
			t.Fatalf("missing marker error = %v", err)
		}
		if err := scan(t, append([]byte(nil), buildProvenancePrefix()...)); err == nil || !strings.Contains(err.Error(), "missing its suffix") {
			t.Fatalf("malformed marker error = %v", err)
		}
		twoMarkers := append(append([]byte(nil), validMarker...), validMarker...)
		if err := scan(t, twoMarkers); err == nil || !strings.Contains(err.Error(), "multiple") {
			t.Fatalf("multiple marker error = %v", err)
		}
		if err := scan(t, frame("UNSET")); err == nil || !strings.Contains(err.Error(), "safe default") {
			t.Fatalf("default marker error = %v", err)
		}
	})

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

	t.Run("extra reviewed policy tuple is not emitted", func(t *testing.T) {
		extra := policy
		extra.Modules = append([]LicenseRule(nil), policy.Modules...)
		extra.Modules = append(extra.Modules, LicenseRule{
			Module: "example.test/extra", Version: "v3.0.0", Sum: "h1:extra", License: "MIT",
		})
		evidence, err := verifyMetadata(binary("linux/amd64", "amd64"), binary("linux/arm64", "arm64"), extra)
		if err != nil {
			t.Fatalf("verifyMetadata() error = %v", err)
		}
		if len(evidence.Dependencies) != len(dependencies) {
			t.Fatalf("dependency inventory has %d rows, want %d actual binary dependencies", len(evidence.Dependencies), len(dependencies))
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
