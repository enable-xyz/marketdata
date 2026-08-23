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
	"regexp"
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

	buildProvenancePrefixFirst    = "@@ENABLE_MARKET_RELEASE_"
	buildProvenancePrefixSecond   = "PROVENANCE_V1@@"
	buildProvenanceSuffixFirst    = "@@END_ENABLE_MARKET_RELEASE_"
	buildProvenanceSuffixSecond   = "PROVENANCE_V1@@"
	buildProvenanceSeparator      = "|"
	maxBuildProvenanceMarkerBytes = 512
	maxReleaseVersionBytes        = 128
)

var releaseVersionPattern = regexp.MustCompile(
	`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?(\+[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$`,
)

// embeddedIdentity is read by Identity and therefore remains inspectable in a
// stripped release binary. Its payload is architecture-independent.
var embeddedIdentity = identityPrefix + identityJSON + identitySuffix

// embeddedBuildProvenance is replaced atomically by one linker -X value. The
// unframed default is deliberately unrecognizable as release evidence, while
// keeping the linker-set variable reachable through Identity.
var embeddedBuildProvenance = "UNSET"

type IdentityManifest struct {
	Format          string   `json:"format"`
	CertifiedTuples []string `json:"certified_tuples"`
	Schema          string   `json:"schema"`
	Mapper          string   `json:"mapper"`
}

func Identity() (IdentityManifest, error) {
	// This bound also keeps the linker-set provenance variable reachable from
	// every binary that exposes release identity.
	if len(embeddedBuildProvenance) > maxBuildProvenanceMarkerBytes {
		return IdentityManifest{}, errors.New("embedded build provenance exceeds its bound")
	}
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

type BuildProvenance struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
}

type Provenance struct {
	Build       BuildProvenance `json:"build"`
	GoVersion   string          `json:"go_version"`
	ModulePath  string          `json:"module_path"`
	ModuleValue string          `json:"module_version"`
	VCSPresent  bool            `json:"vcs_present"`
	VCS         string          `json:"vcs,omitempty"`
	Revision    string          `json:"vcs_revision,omitempty"`
	RevisionAt  string          `json:"vcs_time,omitempty"`
	Modified    bool            `json:"vcs_modified"`
	Trimpath    bool            `json:"trimpath"`
	BuildMode   string          `json:"build_mode"`
	Compiler    string          `json:"compiler"`
	LDFlags     string          `json:"ldflags,omitempty"`
	BuildTags   string          `json:"build_tags,omitempty"`
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
	provenanceMarker, err := readEmbeddedBuildProvenance(path)
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
	vcsKeys := [...]string{"vcs", "vcs.revision", "vcs.time", "vcs.modified"}
	vcsFields := 0
	for _, key := range vcsKeys {
		if _, ok := settings[key]; ok {
			vcsFields++
		}
	}
	if vcsFields != 0 && vcsFields != len(vcsKeys) {
		return BinaryMetadata{}, errors.New("Go build information contains a partial VCS tuple")
	}
	vcsPresent := vcsFields == len(vcsKeys)
	if vcsPresent && settings["vcs.modified"] != "true" && settings["vcs.modified"] != "false" {
		return BinaryMetadata{}, fmt.Errorf("Go build information has invalid vcs.modified value %q", settings["vcs.modified"])
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
			Build:     provenanceMarker,
			GoVersion: info.GoVersion, ModulePath: info.Path, ModuleValue: info.Main.Version,
			VCSPresent: vcsPresent, VCS: settings["vcs"],
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
		provenance.ModuleValue == "" || provenance.BuildMode != "exe" || provenance.Compiler != "gc" {
		return errors.New("module, Go version, build mode, or compiler evidence is invalid")
	}
	if err := validateBuildProvenance(provenance.Build); err != nil {
		return fmt.Errorf("embedded build marker: %w", err)
	}
	if !provenance.VCSPresent {
		if provenance.VCS != "" || provenance.Revision != "" || provenance.RevisionAt != "" || provenance.Modified {
			return errors.New("Go build information contains a partial VCS tuple")
		}
		return errors.New("Go build information is missing the required VCS tuple")
	}
	if provenance.VCS != "git" || provenance.Revision == "" || provenance.RevisionAt == "" {
		return errors.New("Go build information contains an invalid VCS tuple")
	}
	if provenance.Modified {
		return errors.New("Go VCS evidence reports a modified worktree")
	}
	if err := validateFullCommit(provenance.Revision); err != nil {
		return fmt.Errorf("Go VCS revision: %w", err)
	}
	if err := validateUTCBuildDate(provenance.RevisionAt); err != nil {
		return fmt.Errorf("Go VCS time: %w", err)
	}
	if provenance.Build.Commit != provenance.Revision || provenance.Build.BuildDate != provenance.RevisionAt {
		return errors.New("embedded build marker differs from Go VCS revision or time")
	}
	return nil
}

func validateBuildProvenance(provenance BuildProvenance) error {
	if provenance.Version == "" || len(provenance.Version) > maxReleaseVersionBytes ||
		!releaseVersionPattern.MatchString(provenance.Version) ||
		strings.Contains(strings.ToLower(provenance.Version), "dev") {
		return errors.New("version must be a bounded non-development semantic version")
	}
	if err := validateFullCommit(provenance.Commit); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	if err := validateUTCBuildDate(provenance.BuildDate); err != nil {
		return fmt.Errorf("build date: %w", err)
	}
	return nil
}

func validateFullCommit(commit string) error {
	if len(commit) != 40 || commit != strings.ToLower(commit) {
		return errors.New("must be a lowercase full 40-hex revision")
	}
	if _, err := hex.DecodeString(commit); err != nil {
		return errors.New("must be a lowercase full 40-hex revision")
	}
	return nil
}

func validateUTCBuildDate(value string) error {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || !strings.HasSuffix(value, "Z") || parsed.Format(time.RFC3339) != value {
		return errors.New("must be canonical RFC3339 UTC")
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
	}
	return licensed, nil
}

func readEmbeddedBuildProvenance(path string) (BuildProvenance, error) {
	file, err := os.Open(path)
	if err != nil {
		return BuildProvenance{}, fmt.Errorf("opening binary build provenance: %w", err)
	}
	defer file.Close()

	prefix := buildProvenancePrefix()
	suffix := buildProvenanceSuffix()
	buffer := make([]byte, 64<<10)
	pending := make([]byte, 0, len(buffer)+maxBuildProvenanceMarkerBytes)
	var marked []byte
	markerCount := 0
	incomplete := false
	for {
		n, readErr := file.Read(buffer)
		pending = append(pending, buffer[:n]...)
		for {
			start := bytes.Index(pending, prefix)
			if start < 0 {
				keep := min(len(pending), len(prefix)-1)
				copy(pending[:keep], pending[len(pending)-keep:])
				pending = pending[:keep]
				incomplete = false
				break
			}
			pending = pending[start:]
			incomplete = true
			end := bytes.Index(pending[len(prefix):], suffix)
			if end < 0 {
				if len(pending) > maxBuildProvenanceMarkerBytes {
					return BuildProvenance{}, errors.New("embedded build provenance marker exceeds its bound or is missing its suffix")
				}
				break
			}
			markerEnd := len(prefix) + end + len(suffix)
			if markerEnd > maxBuildProvenanceMarkerBytes {
				return BuildProvenance{}, errors.New("embedded build provenance marker exceeds its bound")
			}
			markerCount++
			if markerCount > 1 {
				return BuildProvenance{}, errors.New("multiple embedded build provenance markers found")
			}
			marked = append(marked[:0], pending[:markerEnd]...)
			pending = pending[markerEnd:]
			incomplete = false
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return BuildProvenance{}, fmt.Errorf("scanning binary build provenance: %w", readErr)
			}
			break
		}
	}
	if incomplete {
		return BuildProvenance{}, errors.New("embedded build provenance marker is missing its suffix")
	}
	if markerCount == 0 {
		return BuildProvenance{}, errors.New("embedded build provenance marker is missing")
	}
	return decodeBuildProvenance(marked)
}

func decodeBuildProvenance(marked []byte) (BuildProvenance, error) {
	if len(marked) > maxBuildProvenanceMarkerBytes {
		return BuildProvenance{}, errors.New("embedded build provenance marker exceeds its bound")
	}
	prefix := buildProvenancePrefix()
	suffix := buildProvenanceSuffix()
	if !bytes.HasPrefix(marked, prefix) {
		return BuildProvenance{}, errors.New("embedded build provenance prefix is missing")
	}
	if !bytes.HasSuffix(marked, suffix) {
		return BuildProvenance{}, errors.New("embedded build provenance suffix is missing")
	}
	payload := marked[len(prefix) : len(marked)-len(suffix)]
	if bytes.Equal(payload, []byte("UNSET")) {
		return BuildProvenance{}, errors.New("embedded build provenance marker has its safe default")
	}
	if bytes.Contains(payload, prefix) || bytes.Contains(payload, suffix) {
		return BuildProvenance{}, errors.New("embedded build provenance marker contains nested framing")
	}
	fields := strings.Split(string(payload), buildProvenanceSeparator)
	if len(fields) != 3 {
		return BuildProvenance{}, errors.New("embedded build provenance payload must contain exactly version, commit, and build date")
	}
	provenance := BuildProvenance{Version: fields[0], Commit: fields[1], BuildDate: fields[2]}
	if err := validateBuildProvenance(provenance); err != nil {
		return BuildProvenance{}, err
	}
	return provenance, nil
}

func buildProvenancePrefix() []byte {
	prefix := make([]byte, 0, len(buildProvenancePrefixFirst)+len(buildProvenancePrefixSecond))
	prefix = append(prefix, buildProvenancePrefixFirst...)
	return append(prefix, buildProvenancePrefixSecond...)
}

func buildProvenanceSuffix() []byte {
	suffix := make([]byte, 0, len(buildProvenanceSuffixFirst)+len(buildProvenanceSuffixSecond))
	suffix = append(suffix, buildProvenanceSuffixFirst...)
	return append(suffix, buildProvenanceSuffixSecond...)
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
