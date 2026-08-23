// Package release inspects caller-built binaries and emits deterministic,
// typed release evidence.
package release

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"
)

const (
	IdentityFormat = "enable-market-release-identity/v2"
	EvidenceFormat = "enable-market-release-evidence/v2"
	PolicyFormat   = "enable-market-license-policy/v2"

	identityPrefix = "\x00ENABLE_MARKET_RELEASE_IDENTITY_V2\x00"
	identitySuffix = "\x00END_ENABLE_MARKET_RELEASE_IDENTITY_V2\x00"
	identityJSON   = `{"format":"enable-market-release-identity/v2","certified_tuples":[],"schema":"normalized-schema/v1","mapper":"catalog-mapper/normalized-schema-v1"}`
	maxMarkerBytes = 8 << 10
)

// embeddedIdentity is read by Identity and therefore remains inspectable in a
// stripped release binary. Its payload is architecture-independent.
var embeddedIdentity = identityPrefix + identityJSON + identitySuffix

type IdentityManifest struct {
	Format          string   `json:"format"`
	CertifiedTuples []string `json:"certified_tuples"`
	Schema          string   `json:"schema"`
	Mapper          string   `json:"mapper"`
}

func Identity() (IdentityManifest, error) {
	return decodeIdentity([]byte(embeddedIdentity))
}

func WriteIdentity(output io.Writer) error {
	if output == nil {
		return errors.New("release metadata output is required")
	}
	identity, err := Identity()
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(identity)
}

type Module struct {
	Path    string `json:"path"`
	Version string `json:"version"`
	Sum     string `json:"sum"`
}

type Provenance struct {
	GoVersion   string `json:"go_version"`
	ModulePath  string `json:"module_path"`
	ModuleValue string `json:"module_version"`
	Revision    string `json:"vcs_revision"`
	RevisionAt  string `json:"vcs_time"`
	Modified    bool   `json:"vcs_modified"`
	Trimpath    bool   `json:"trimpath"`
	BuildMode   string `json:"build_mode"`
	Compiler    string `json:"compiler"`
	LDFlags     string `json:"ldflags,omitempty"`
	BuildTags   string `json:"build_tags,omitempty"`
}

type BinaryMetadata struct {
	Target       string           `json:"target"`
	SHA256       string           `json:"sha256"`
	GOOS         string           `json:"goos"`
	GOARCH       string           `json:"goarch"`
	CGOEnabled   string           `json:"cgo_enabled"`
	Identity     IdentityManifest `json:"identity"`
	Provenance   Provenance       `json:"provenance"`
	Dependencies []Module         `json:"dependencies"`
}

type LicenseRule struct {
	Module  string `json:"module"`
	Version string `json:"version"`
	Sum     string `json:"sum"`
	License string `json:"license"`
}

type LicensePolicy struct {
	Format             string        `json:"format"`
	AllowedExpressions []string      `json:"allowed_expressions"`
	Modules            []LicenseRule `json:"modules"`
}

type LicensedModule struct {
	Path    string `json:"path"`
	Version string `json:"version"`
	Sum     string `json:"sum"`
	License string `json:"license"`
}

type Evidence struct {
	Format       string           `json:"format"`
	Identity     IdentityManifest `json:"identity"`
	Binaries     []BinaryMetadata `json:"binaries"`
	Dependencies []LicensedModule `json:"dependency_inventory"`
}

type VerifyOptions struct {
	AMD64Binary    string
	ARM64Binary    string
	LicensePolicy  string
	EvidenceOutput string
}

func Verify(ctx context.Context, options VerifyOptions) (Evidence, error) {
	if err := ctx.Err(); err != nil {
		return Evidence{}, err
	}
	for field, value := range map[string]string{
		"amd64 binary":    options.AMD64Binary,
		"arm64 binary":    options.ARM64Binary,
		"license policy":  options.LicensePolicy,
		"evidence output": options.EvidenceOutput,
	} {
		if value == "" {
			return Evidence{}, fmt.Errorf("explicit %s path is required", field)
		}
	}
	amd64, err := InspectBinary(options.AMD64Binary, "linux/amd64")
	if err != nil {
		return Evidence{}, fmt.Errorf("inspecting linux/amd64 binary: %w", err)
	}
	arm64, err := InspectBinary(options.ARM64Binary, "linux/arm64")
	if err != nil {
		return Evidence{}, fmt.Errorf("inspecting linux/arm64 binary: %w", err)
	}
	policy, err := ReadLicensePolicy(options.LicensePolicy)
	if err != nil {
		return Evidence{}, err
	}
	evidence, err := verifyMetadata(amd64, arm64, policy)
	if err != nil {
		return Evidence{}, err
	}
	if err := WriteEvidence(options.EvidenceOutput, evidence); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

func InspectBinary(path, target string) (BinaryMetadata, error) {
	if path == "" {
		return BinaryMetadata{}, errors.New("binary path is required")
	}
	identity, err := readEmbeddedIdentity(path)
	if err != nil {
		return BinaryMetadata{}, err
	}
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return BinaryMetadata{}, fmt.Errorf("reading Go build information: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return BinaryMetadata{}, fmt.Errorf("opening binary for digest: %w", err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		_ = file.Close()
		return BinaryMetadata{}, fmt.Errorf("hashing binary: %w", err)
	}
	if err := file.Close(); err != nil {
		return BinaryMetadata{}, fmt.Errorf("closing binary after digest: %w", err)
	}
	settings := make(map[string]string, len(info.Settings))
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	dependencies := make([]Module, 0, len(info.Deps))
	for _, dependency := range info.Deps {
		if dependency.Replace != nil {
			return BinaryMetadata{}, fmt.Errorf("dependency %s uses a replacement", dependency.Path)
		}
		dependencies = append(dependencies, Module{Path: dependency.Path, Version: dependency.Version, Sum: dependency.Sum})
	}
	slices.SortFunc(dependencies, compareModules)
	return BinaryMetadata{
		Target: target,
		SHA256: hex.EncodeToString(hash.Sum(nil)),
		GOOS:   settings["GOOS"], GOARCH: settings["GOARCH"], CGOEnabled: settings["CGO_ENABLED"],
		Identity: identity,
		Provenance: Provenance{
			GoVersion: info.GoVersion, ModulePath: info.Path, ModuleValue: info.Main.Version,
			Revision: settings["vcs.revision"], RevisionAt: settings["vcs.time"],
			Modified: settings["vcs.modified"] == "true", Trimpath: settings["-trimpath"] == "true",
			BuildMode: settings["-buildmode"], Compiler: settings["-compiler"],
			LDFlags: settings["-ldflags"], BuildTags: settings["-tags"],
		},
		Dependencies: dependencies,
	}, nil
}

func ReadLicensePolicy(path string) (LicensePolicy, error) {
	file, err := os.Open(path)
	if err != nil {
		return LicensePolicy{}, fmt.Errorf("opening license policy: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	decoder.DisallowUnknownFields()
	var policy LicensePolicy
	if err := decoder.Decode(&policy); err != nil {
		return LicensePolicy{}, fmt.Errorf("decoding license policy: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return LicensePolicy{}, err
	}
	if policy.Format != PolicyFormat {
		return LicensePolicy{}, fmt.Errorf("unknown license policy format %q", policy.Format)
	}
	return policy, nil
}

func WriteEvidence(path string, evidence Evidence) error {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(evidence); err != nil {
		return fmt.Errorf("encoding release evidence: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("creating immutable release evidence: %w", err)
	}
	remove := true
	defer func() {
		_ = file.Close()
		if remove {
			_ = os.Remove(path)
		}
	}()
	if _, err := io.Copy(file, &encoded); err != nil {
		return fmt.Errorf("writing release evidence: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing release evidence: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing release evidence: %w", err)
	}
	remove = false
	return nil
}

func verifyMetadata(amd64, arm64 BinaryMetadata, policy LicensePolicy) (Evidence, error) {
	for _, binary := range []BinaryMetadata{amd64, arm64} {
		if binary.Target != binary.GOOS+"/"+binary.GOARCH || binary.GOOS != "linux" ||
			(binary.GOARCH != "amd64" && binary.GOARCH != "arm64") {
			return Evidence{}, fmt.Errorf("binary target metadata mismatch for %q", binary.Target)
		}
		if err := validateBinaryInventory(binary); err != nil {
			return Evidence{}, fmt.Errorf("%s inventory: %w", binary.Target, err)
		}
		if binary.CGOEnabled != "0" {
			return Evidence{}, fmt.Errorf("%s was not built with CGO_ENABLED=0", binary.Target)
		}
		if !binary.Provenance.Trimpath {
			return Evidence{}, fmt.Errorf("%s was not built with -trimpath", binary.Target)
		}
		if err := validateProvenance(binary.Provenance); err != nil {
			return Evidence{}, fmt.Errorf("%s provenance: %w", binary.Target, err)
		}
		if err := validateIdentity(binary.Identity); err != nil {
			return Evidence{}, fmt.Errorf("%s identity: %w", binary.Target, err)
		}
	}
	if amd64.Target != "linux/amd64" || arm64.Target != "linux/arm64" {
		return Evidence{}, errors.New("release verifier requires linux/amd64 and linux/arm64 in canonical order")
	}
	if amd64.Identity.Format != arm64.Identity.Format || amd64.Identity.Schema != arm64.Identity.Schema ||
		amd64.Identity.Mapper != arm64.Identity.Mapper ||
		!slices.Equal(amd64.Identity.CertifiedTuples, arm64.Identity.CertifiedTuples) {
		return Evidence{}, errors.New("certified tuple, schema, or mapper identity differs across architectures")
	}
	if amd64.Provenance != arm64.Provenance {
		return Evidence{}, errors.New("immutable build provenance differs across architectures")
	}
	if !slices.Equal(amd64.Dependencies, arm64.Dependencies) {
		return Evidence{}, errors.New("dependency inventory differs across architectures")
	}
	licenses, err := licenseDependencies(amd64.Dependencies, policy)
	if err != nil {
		return Evidence{}, err
	}
	return Evidence{
		Format:       EvidenceFormat,
		Identity:     amd64.Identity,
		Binaries:     []BinaryMetadata{amd64, arm64},
		Dependencies: licenses,
	}, nil
}

func validateBinaryInventory(binary BinaryMetadata) error {
	digest, err := hex.DecodeString(binary.SHA256)
	if err != nil || len(digest) != sha256.Size || binary.SHA256 != strings.ToLower(binary.SHA256) {
		return errors.New("binary SHA-256 is invalid")
	}
	if !slices.IsSortedFunc(binary.Dependencies, func(left, right Module) int {
		return compareModules(left, right)
	}) {
		return errors.New("dependency inventory is not sorted")
	}
	for i, dependency := range binary.Dependencies {
		if dependency.Path == "" || dependency.Version == "" || dependency.Sum == "" {
			return errors.New("dependency inventory contains an incomplete module")
		}
		if i > 0 && dependency.Path == binary.Dependencies[i-1].Path {
			return fmt.Errorf("dependency inventory contains duplicate module %s", dependency.Path)
		}
	}
	return nil
}

func validateIdentity(identity IdentityManifest) error {
	if identity.Format != IdentityFormat || identity.Schema == "" || identity.Mapper == "" ||
		identity.CertifiedTuples == nil {
		return errors.New("release identity is incomplete or unknown")
	}
	if len(identity.CertifiedTuples) != 0 {
		return errors.New("certified tuple set must remain empty until generated certification evidence exists")
	}
	return nil
}

func validateProvenance(provenance Provenance) error {
	if provenance.ModulePath != "github.com/enable-xyz/marketdata" || provenance.GoVersion == "" ||
		provenance.ModuleValue == "" || provenance.Revision == "" || len(provenance.Revision) != 40 ||
		provenance.Modified || provenance.BuildMode != "exe" || provenance.Compiler != "gc" {
		return errors.New("module, revision, Go version, build mode, compiler, or clean-tree evidence is invalid")
	}
	if _, err := hex.DecodeString(provenance.Revision); err != nil || provenance.Revision != strings.ToLower(provenance.Revision) {
		return errors.New("VCS revision must be lowercase full hexadecimal")
	}
	if _, err := time.Parse(time.RFC3339, provenance.RevisionAt); err != nil {
		return errors.New("VCS time must be immutable RFC3339")
	}
	return nil
}

func licenseDependencies(dependencies []Module, policy LicensePolicy) ([]LicensedModule, error) {
	if policy.Format != PolicyFormat || len(policy.AllowedExpressions) == 0 {
		return nil, errors.New("license policy format or allowlist is missing")
	}
	allowed := make(map[string]struct{}, len(policy.AllowedExpressions))
	for _, expression := range policy.AllowedExpressions {
		if expression == "" || expression == "UNKNOWN" || expression == "NOASSERTION" {
			return nil, fmt.Errorf("unknown license expression %q is disallowed", expression)
		}
		allowed[expression] = struct{}{}
	}
	rules := make(map[string]LicenseRule, len(policy.Modules))
	for _, rule := range policy.Modules {
		if rule.Module == "" || rule.Version == "" || rule.Sum == "" ||
			rule.License == "" || rule.License == "UNKNOWN" || rule.License == "NOASSERTION" {
			return nil, errors.New("license policy contains an incomplete or unknown module tuple")
		}
		if _, ok := allowed[rule.License]; !ok {
			return nil, fmt.Errorf("module %s@%s uses disallowed license %s", rule.Module, rule.Version, rule.License)
		}
		if _, exists := rules[rule.Module]; exists {
			return nil, fmt.Errorf("duplicate license rule for module %s", rule.Module)
		}
		rules[rule.Module] = rule
	}
	licensed := make([]LicensedModule, 0, len(dependencies))
	for _, dependency := range dependencies {
		rule, exists := rules[dependency.Path]
		if !exists {
			return nil, fmt.Errorf("dependency %s@%s with sum %s has no reviewed license rule", dependency.Path, dependency.Version, dependency.Sum)
		}
		if rule.Version != dependency.Version || rule.Sum != dependency.Sum {
			return nil, fmt.Errorf(
				"dependency %s@%s with sum %s does not match reviewed license tuple %s@%s with sum %s",
				dependency.Path, dependency.Version, dependency.Sum, rule.Module, rule.Version, rule.Sum,
			)
		}
		licensed = append(licensed, LicensedModule{
			Path: dependency.Path, Version: dependency.Version, Sum: dependency.Sum, License: rule.License,
		})
		delete(rules, dependency.Path)
	}
	if len(rules) != 0 {
		extra := make([]string, 0, len(rules))
		for module := range rules {
			extra = append(extra, module)
		}
		slices.Sort(extra)
		rule := rules[extra[0]]
		return nil, fmt.Errorf("license policy contains extra rule for %s@%s with sum %s", rule.Module, rule.Version, rule.Sum)
	}
	return licensed, nil
}

func readEmbeddedIdentity(path string) (IdentityManifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return IdentityManifest{}, fmt.Errorf("opening binary identity: %w", err)
	}
	defer file.Close()
	buffer := make([]byte, (64<<10)+maxMarkerBytes)
	carry := 0
	for {
		n, readErr := file.Read(buffer[carry:])
		data := buffer[:carry+n]
		if start := bytes.Index(data, []byte(identityPrefix)); start >= 0 {
			if end := bytes.Index(data[start+len(identityPrefix):], []byte(identitySuffix)); end >= 0 {
				end += start + len(identityPrefix) + len(identitySuffix)
				return decodeIdentity(data[start:end])
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return IdentityManifest{}, fmt.Errorf("scanning binary identity: %w", readErr)
		}
		carry = min(len(data), maxMarkerBytes)
		copy(buffer[:carry], data[len(data)-carry:])
	}
	return IdentityManifest{}, errors.New("release identity marker is missing")
}

func decodeIdentity(marked []byte) (IdentityManifest, error) {
	start := bytes.Index(marked, []byte(identityPrefix))
	if start < 0 {
		return IdentityManifest{}, errors.New("release identity prefix is missing")
	}
	payloadStart := start + len(identityPrefix)
	end := bytes.Index(marked[payloadStart:], []byte(identitySuffix))
	if end < 0 {
		return IdentityManifest{}, errors.New("release identity suffix is missing")
	}
	var identity IdentityManifest
	decoder := json.NewDecoder(bytes.NewReader(marked[payloadStart : payloadStart+end]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return IdentityManifest{}, fmt.Errorf("decoding release identity: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return IdentityManifest{}, err
	}
	if err := validateIdentity(identity); err != nil {
		return IdentityManifest{}, err
	}
	return identity, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON document contains multiple values")
		}
		return fmt.Errorf("reading JSON document boundary: %w", err)
	}
	return nil
}

func compareModules(left, right Module) int {
	if left.Path != right.Path {
		return strings.Compare(left.Path, right.Path)
	}
	if left.Version != right.Version {
		return strings.Compare(left.Version, right.Version)
	}
	return strings.Compare(left.Sum, right.Sum)
}
