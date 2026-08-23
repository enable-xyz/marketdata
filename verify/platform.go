package verify

import (
	"bytes"
	"cmp"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/enable-xyz/marketdata/catalog"
)

const (
	PlatformBundleSchemaVersion uint16 = 1
	PlatformPlanSHA256                 = "30dea46540f14817624f92ad8e4671380408b0f67ded74849d90f923a6e96585"
)

// LifecycleStatus reuses the catalog's canonical tuple lifecycle vocabulary.
type LifecycleStatus = catalog.TupleLifecycle

const (
	LifecycleCandidate     = catalog.LifecycleCandidate
	LifecycleCaptured      = catalog.LifecycleCaptured
	LifecycleReplayable    = catalog.LifecycleReplayable
	LifecycleNormalized    = catalog.LifecycleNormalized
	LifecycleReconstructed = catalog.LifecycleReconstructed
	LifecycleSupported     = catalog.LifecycleSupported
	LifecycleUnsupported   = catalog.LifecycleUnsupported
	LifecycleAmbiguous     = catalog.LifecycleAmbiguous
)

// ParityStatus is the independently published parity claim for one tuple.
type ParityStatus string

const (
	ParityProved          ParityStatus = "proved"
	ParityPartiallyProved ParityStatus = "partially_proved"
	ParityUnsupported     ParityStatus = "unsupported"
	ParityAmbiguous       ParityStatus = "ambiguous"
)

// EvidenceKind states what an artifact proves. It does not describe storage.
type EvidenceKind string

const (
	EvidenceOfficialContract EvidenceKind = "official_contract"
	EvidenceSourceContract   EvidenceKind = "source_contract"
	EvidenceFixture          EvidenceKind = "fixture"
	EvidenceRaw              EvidenceKind = "raw"
	EvidenceDataset          EvidenceKind = "dataset"
	EvidenceReplay           EvidenceKind = "replay"
	EvidenceCatalog          EvidenceKind = "catalog"
)

// TupleIdentity is the complete identity of a support and parity claim.
// A nil CoverageEndNS denotes an explicitly open observed interval.
type TupleIdentity struct {
	SourceID          string `json:"source_id"`
	APIVersion        string `json:"api_version"`
	Entitlement       string `json:"entitlement"`
	Channel           string `json:"channel"`
	DataFamily        string `json:"data_family"`
	NativeGranularity string `json:"native_granularity"`
	CoverageStartNS   int64  `json:"coverage_start_ns"`
	CoverageEndNS     *int64 `json:"coverage_end_ns,omitempty"`
	AdapterVersion    string `json:"adapter_version"`
}

// TupleDeclaration is caller-owned scope. CanaryRequired distinguishes tuples
// whose operational gate includes the 26-hour canary from optional canaries.
type TupleDeclaration struct {
	Tuple          TupleIdentity   `json:"tuple"`
	Lifecycle      LifecycleStatus `json:"lifecycle"`
	CanaryRequired bool            `json:"canary_required"`
}

// ArtifactReference contains no payload or filesystem path. Locator must be a
// credential-free, non-file URI and must exactly match the inventory entry.
type ArtifactReference struct {
	Kind    EvidenceKind `json:"kind"`
	SHA256  string       `json:"sha256"`
	Locator string       `json:"locator"`
}

// OperationalGateValidator is supplied by the owner of the operational gate
// report. It must validate the report bytes, all required gates, the exact
// tuple identity, and the tuple's required/optional canary disposition.
type OperationalGateValidator func(reportSHA256 string, tuple TupleIdentity, canaryRequired bool) error

// ArtifactInventoryEntry resolves one lowercase SHA-256 to exactly one
// credential-free locator. OperationalGateValidator is required when this
// entry is used to certify a supported lifecycle; it is never serialized.
type ArtifactInventoryEntry struct {
	Locator                  string
	OperationalGateValidator OperationalGateValidator
}

// ArtifactInventory is caller-supplied evidence resolution. Map keys are
// lowercase SHA-256 digests and values carry the exact locator.
type ArtifactInventory map[string]ArtifactInventoryEntry

// ParityRow is the complete tuple-level certification statement.
// OperationalGateReportSHA256 is required for supported rows, optional for
// partial rows, and empty for unsupported or ambiguous rows.
type ParityRow struct {
	Tuple                       TupleIdentity       `json:"tuple"`
	Lifecycle                   LifecycleStatus     `json:"lifecycle"`
	Status                      ParityStatus        `json:"status"`
	CanaryRequired              bool                `json:"canary_required"`
	Limitation                  string              `json:"limitation"`
	Ambiguity                   string              `json:"ambiguity"`
	OperationalGateReportSHA256 string              `json:"operational_gate_report_sha256"`
	SourceEvidence              []ArtifactReference `json:"source_evidence"`
	FixtureEvidence             []ArtifactReference `json:"fixture_evidence"`
	RawEvidence                 []ArtifactReference `json:"raw_evidence"`
	DatasetEvidence             []ArtifactReference `json:"dataset_evidence"`
	ReplayEvidence              []ArtifactReference `json:"replay_evidence"`
	CatalogEvidence             []ArtifactReference `json:"catalog_evidence"`
}

// PlatformBundle is the canonical, content-hashed certification output.
type PlatformBundle struct {
	SchemaVersion          uint16      `json:"schema_version"`
	PlanSHA256             string      `json:"plan_sha256"`
	BuildIdentity          BuildInfo   `json:"build_identity"`
	HardwareIdentitySHA256 string      `json:"hardware_identity_sha256"`
	WorkloadIdentitySHA256 string      `json:"workload_identity_sha256"`
	X5EvidenceSHA256       string      `json:"x5_evidence_sha256"`
	Rows                   []ParityRow `json:"rows"`
	BodySHA256             string      `json:"body_sha256"`
}

// PlatformBundleInput contains only caller-declared identities and evidence.
// BuildPlatformBundle supplies the immutable schema and plan identities.
type PlatformBundleInput struct {
	BuildIdentity          BuildInfo
	HardwareIdentitySHA256 string
	WorkloadIdentitySHA256 string
	X5EvidenceSHA256       string
	Declarations           []TupleDeclaration
	Rows                   []ParityRow
}

// BuildPlatformBundle validates all declarations and evidence before returning
// canonical JSON. Input row and evidence ordering does not affect the bytes.
func BuildPlatformBundle(input PlatformBundleInput, inventory ArtifactInventory) ([]byte, error) {
	rows := canonicalPlatformRows(input.Rows)
	bundle := PlatformBundle{
		SchemaVersion:          PlatformBundleSchemaVersion,
		PlanSHA256:             PlatformPlanSHA256,
		BuildIdentity:          input.BuildIdentity,
		HardwareIdentitySHA256: input.HardwareIdentitySHA256,
		WorkloadIdentitySHA256: input.WorkloadIdentitySHA256,
		X5EvidenceSHA256:       input.X5EvidenceSHA256,
		Rows:                   rows,
	}
	if err := validatePlatformBundleContents(bundle, input.Declarations, inventory); err != nil {
		return nil, err
	}
	return marshalPlatformBundle(bundle)
}

// ValidatePlatformBundle strictly decodes, self-hash checks, and revalidates a
// bundle against the caller's complete declaration and artifact inventories.
func ValidatePlatformBundle(encoded []byte, declarations []TupleDeclaration, inventory ArtifactInventory) (PlatformBundle, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var bundle PlatformBundle
	if err := decoder.Decode(&bundle); err != nil {
		return PlatformBundle{}, fmt.Errorf("verify: decode platform bundle: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return PlatformBundle{}, errors.New("verify: platform bundle has trailing JSON")
	}

	claimed := bundle.BodySHA256
	bundle.BodySHA256 = ""
	body, err := json.Marshal(bundle)
	if err != nil || !validPlatformSHA256(claimed) || claimed != hashHex(body) {
		return PlatformBundle{}, errors.New("verify: platform bundle body hash mismatch")
	}
	bundle.BodySHA256 = claimed

	if err := validatePlatformBundleContents(bundle, declarations, inventory); err != nil {
		return PlatformBundle{}, err
	}
	canonical, err := json.Marshal(bundle)
	if err != nil {
		return PlatformBundle{}, fmt.Errorf("verify: encode canonical platform bundle: %w", err)
	}
	if !bytes.Equal(bytes.TrimSpace(encoded), canonical) {
		return PlatformBundle{}, errors.New("verify: platform bundle encoding is not canonical")
	}
	return bundle, nil
}

func marshalPlatformBundle(bundle PlatformBundle) ([]byte, error) {
	bundle.BodySHA256 = ""
	body, err := json.Marshal(bundle)
	if err != nil {
		return nil, fmt.Errorf("verify: encode platform bundle body: %w", err)
	}
	bundle.BodySHA256 = hashHex(body)
	encoded, err := json.Marshal(bundle)
	if err != nil {
		return nil, fmt.Errorf("verify: encode platform bundle: %w", err)
	}
	return append(encoded, '\n'), nil
}

func validatePlatformBundleContents(bundle PlatformBundle, declarations []TupleDeclaration, inventory ArtifactInventory) error {
	if bundle.SchemaVersion != PlatformBundleSchemaVersion {
		return fmt.Errorf("verify: platform bundle schema version %d is unsupported", bundle.SchemaVersion)
	}
	if bundle.PlanSHA256 != PlatformPlanSHA256 {
		return errors.New("verify: platform bundle plan hash mismatch")
	}
	if err := validatePlatformBuildIdentity(bundle.BuildIdentity); err != nil {
		return err
	}
	if err := validateArtifactInventory(inventory); err != nil {
		return err
	}
	for name, digest := range map[string]string{
		"hardware identity": bundle.HardwareIdentitySHA256,
		"workload identity": bundle.WorkloadIdentitySHA256,
		"X5 evidence":       bundle.X5EvidenceSHA256,
	} {
		if err := validateInventoryDigest(name, digest, inventory); err != nil {
			return err
		}
	}
	if len(declarations) == 0 {
		return errors.New("verify: platform declaration set is empty")
	}

	declared := make(map[platformTupleKey]TupleDeclaration, len(declarations))
	for i, declaration := range declarations {
		if err := validateTupleIdentity(declaration.Tuple); err != nil {
			return fmt.Errorf("verify: declaration %d: %w", i, err)
		}
		if !validLifecycleStatus(declaration.Lifecycle) {
			return fmt.Errorf("verify: declaration %d has invalid lifecycle %q", i, declaration.Lifecycle)
		}
		key := platformKey(declaration.Tuple)
		if _, exists := declared[key]; exists {
			return fmt.Errorf("verify: duplicate declared tuple identity for %q", declaration.Tuple.SourceID)
		}
		declared[key] = declaration
	}

	seen := make(map[platformTupleKey]struct{}, len(bundle.Rows))
	for i, row := range bundle.Rows {
		if i > 0 && compareTupleIdentity(bundle.Rows[i-1].Tuple, row.Tuple) >= 0 {
			return errors.New("verify: platform bundle rows are unsorted or duplicate a tuple identity")
		}
		if err := validateTupleIdentity(row.Tuple); err != nil {
			return fmt.Errorf("verify: parity row %d: %w", i, err)
		}
		if !validLifecycleStatus(row.Lifecycle) {
			return fmt.Errorf("verify: parity row %d has invalid lifecycle %q", i, row.Lifecycle)
		}
		if !validParityStatus(row.Status) {
			return fmt.Errorf("verify: parity row %d has invalid parity status %q", i, row.Status)
		}
		key := platformKey(row.Tuple)
		declaration, exists := declared[key]
		if !exists {
			return fmt.Errorf("verify: parity row %d is an extra undeclared tuple", i)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("verify: duplicate parity tuple identity for %q", row.Tuple.SourceID)
		}
		if row.Lifecycle != declaration.Lifecycle {
			return fmt.Errorf("verify: parity row %d lifecycle does not match its declaration", i)
		}
		if row.CanaryRequired != declaration.CanaryRequired {
			return fmt.Errorf("verify: parity row %d canary requirement does not match its declaration", i)
		}
		if err := validateParityRow(row, inventory); err != nil {
			return fmt.Errorf("verify: parity row %d: %w", i, err)
		}
		seen[key] = struct{}{}
	}
	if len(seen) != len(declared) {
		return errors.New("verify: platform bundle is missing one or more declared tuples")
	}
	return nil
}

func validateParityRow(row ParityRow, inventory ArtifactInventory) error {
	if err := validateTupleIdentity(row.Tuple); err != nil {
		return err
	}
	if !validLifecycleStatus(row.Lifecycle) {
		return fmt.Errorf("invalid lifecycle %q", row.Lifecycle)
	}
	if !validParityStatus(row.Status) {
		return fmt.Errorf("invalid parity status %q", row.Status)
	}
	if err := validateParityTransition(row.Lifecycle, row.Status); err != nil {
		return err
	}
	if err := validatePlatformText("limitation", row.Limitation); err != nil {
		return err
	}
	if row.Status == ParityAmbiguous {
		if err := validatePlatformText("ambiguity", row.Ambiguity); err != nil {
			return err
		}
	} else if row.Ambiguity != "" {
		return errors.New("ambiguity text is only valid for an ambiguous parity row")
	}
	switch row.Lifecycle {
	case LifecycleUnsupported, LifecycleAmbiguous:
		if row.OperationalGateReportSHA256 != "" {
			return errors.New("unsupported or ambiguous tuple must not reference an operational gate report")
		}
	case LifecycleSupported:
		if err := validateInventoryDigest("operational gate report", row.OperationalGateReportSHA256, inventory); err != nil {
			return err
		}
	default:
		if row.OperationalGateReportSHA256 != "" {
			if err := validateInventoryDigest("partial operational gate report", row.OperationalGateReportSHA256, inventory); err != nil {
				return err
			}
		}
	}

	lists := []struct {
		name    string
		refs    []ArtifactReference
		allowed []EvidenceKind
	}{
		{"source evidence", row.SourceEvidence, []EvidenceKind{EvidenceOfficialContract, EvidenceSourceContract}},
		{"fixture evidence", row.FixtureEvidence, []EvidenceKind{EvidenceFixture}},
		{"raw evidence", row.RawEvidence, []EvidenceKind{EvidenceRaw}},
		{"dataset evidence", row.DatasetEvidence, []EvidenceKind{EvidenceDataset}},
		{"replay evidence", row.ReplayEvidence, []EvidenceKind{EvidenceReplay}},
		{"catalog evidence", row.CatalogEvidence, []EvidenceKind{EvidenceCatalog}},
	}
	for _, list := range lists {
		if err := validateArtifactReferences(list.name, list.refs, list.allowed, inventory); err != nil {
			return err
		}
	}
	if err := validateRequiredEvidence(row); err != nil {
		return err
	}

	if row.Lifecycle == LifecycleSupported {
		entry := inventory[row.OperationalGateReportSHA256]
		if entry.OperationalGateValidator == nil {
			return errors.New("supported tuple lacks an operational gate validator")
		}
		if err := entry.OperationalGateValidator(row.OperationalGateReportSHA256, row.Tuple, row.CanaryRequired); err != nil {
			return fmt.Errorf("operational gate report does not prove every required gate: %w", err)
		}
	}
	return nil
}

func validateParityTransition(lifecycle LifecycleStatus, status ParityStatus) error {
	switch status {
	case ParityProved:
		if lifecycle != LifecycleSupported {
			return errors.New("proved parity requires the supported lifecycle")
		}
	case ParityPartiallyProved:
		switch lifecycle {
		case LifecycleCaptured, LifecycleReplayable, LifecycleNormalized, LifecycleReconstructed, LifecycleSupported:
		default:
			return fmt.Errorf("partially_proved parity is invalid for lifecycle %q", lifecycle)
		}
	case ParityUnsupported:
		if lifecycle != LifecycleUnsupported {
			return errors.New("unsupported parity requires the unsupported lifecycle")
		}
	case ParityAmbiguous:
		if lifecycle != LifecycleAmbiguous {
			return errors.New("ambiguous parity requires the ambiguous lifecycle")
		}
	default:
		return fmt.Errorf("invalid parity status %q", status)
	}
	return nil
}

func validateRequiredEvidence(row ParityRow) error {
	if row.Lifecycle == LifecycleUnsupported {
		if !referencesContainKind(row.SourceEvidence, EvidenceOfficialContract) {
			return errors.New("unsupported tuple lacks official_contract evidence")
		}
		return nil
	}
	if row.Lifecycle == LifecycleAmbiguous {
		if len(row.SourceEvidence) == 0 {
			return errors.New("ambiguous tuple lacks source evidence")
		}
		return nil
	}
	if !referencesContainKind(row.SourceEvidence, EvidenceSourceContract) {
		return errors.New("tuple lacks source_contract evidence")
	}
	if len(row.FixtureEvidence) == 0 || len(row.RawEvidence) == 0 {
		return errors.New("captured-or-later tuple lacks fixture or raw evidence")
	}
	if row.Lifecycle == LifecycleCaptured {
		return nil
	}
	if len(row.ReplayEvidence) == 0 {
		return errors.New("replayable-or-later tuple lacks replay evidence")
	}
	if row.Lifecycle == LifecycleReplayable {
		return nil
	}
	if len(row.DatasetEvidence) == 0 || len(row.CatalogEvidence) == 0 {
		return errors.New("normalized-or-later tuple lacks dataset or catalog evidence")
	}
	return nil
}

func validateArtifactReferences(name string, refs []ArtifactReference, allowed []EvidenceKind, inventory ArtifactInventory) error {
	if refs == nil {
		return fmt.Errorf("%s is null", name)
	}
	seen := make(map[string]struct{}, len(refs))
	for i, ref := range refs {
		if i > 0 && compareArtifactReference(refs[i-1], ref) >= 0 {
			return fmt.Errorf("%s is unsorted or duplicated", name)
		}
		if !slices.Contains(allowed, ref.Kind) {
			return fmt.Errorf("%s has invalid evidence kind %q", name, ref.Kind)
		}
		if !validPlatformSHA256(ref.SHA256) {
			return fmt.Errorf("%s has invalid SHA-256", name)
		}
		if err := validateArtifactLocator(ref.Locator); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		entry, exists := inventory[ref.SHA256]
		if !exists || entry.Locator != ref.Locator {
			return fmt.Errorf("%s checksum does not resolve exactly in the artifact inventory", name)
		}
		if _, duplicate := seen[ref.SHA256]; duplicate {
			return fmt.Errorf("%s repeats an artifact checksum", name)
		}
		seen[ref.SHA256] = struct{}{}
	}
	return nil
}

func validateArtifactInventory(inventory ArtifactInventory) error {
	if len(inventory) == 0 {
		return errors.New("verify: platform artifact inventory is empty")
	}
	locators := make(map[string]string, len(inventory))
	for digest, entry := range inventory {
		if !validPlatformSHA256(digest) {
			return errors.New("verify: platform artifact inventory has an invalid lowercase SHA-256 key")
		}
		if err := validateArtifactLocator(entry.Locator); err != nil {
			return fmt.Errorf("verify: platform artifact inventory: %w", err)
		}
		if prior, exists := locators[entry.Locator]; exists && prior != digest {
			return errors.New("verify: platform artifact inventory maps one locator to multiple checksums")
		}
		locators[entry.Locator] = digest
	}
	return nil
}

func validateInventoryDigest(name, digest string, inventory ArtifactInventory) error {
	if !validPlatformSHA256(digest) {
		return fmt.Errorf("verify: platform %s has invalid lowercase SHA-256", name)
	}
	if _, exists := inventory[digest]; !exists {
		return fmt.Errorf("verify: platform %s checksum is unresolved", name)
	}
	return nil
}

func validateArtifactLocator(locator string) error {
	if err := validatePlatformText("artifact locator", locator); err != nil {
		return err
	}
	parsed, err := url.Parse(locator)
	if err != nil || parsed.Scheme == "" || parsed.Scheme != strings.ToLower(parsed.Scheme) {
		return errors.New("artifact locator must be a canonical URI")
	}
	if parsed.Scheme == "file" {
		return errors.New("artifact locator must not be a filesystem path")
	}
	if parsed.User != nil || parsed.RawQuery != "" {
		return errors.New("artifact locator must not contain credentials or query parameters")
	}
	if parsed.Host == "" && parsed.Opaque == "" && parsed.Path == "" {
		return errors.New("artifact locator target is blank")
	}
	return nil
}

func validateTupleIdentity(tuple TupleIdentity) error {
	fields := []struct {
		name  string
		value string
	}{
		{"source_id", tuple.SourceID},
		{"api_version", tuple.APIVersion},
		{"entitlement", tuple.Entitlement},
		{"channel", tuple.Channel},
		{"data_family", tuple.DataFamily},
		{"native_granularity", tuple.NativeGranularity},
		{"adapter_version", tuple.AdapterVersion},
	}
	for _, field := range fields {
		if err := validatePlatformText(field.name, field.value); err != nil {
			return err
		}
	}
	if tuple.CoverageStartNS <= 0 {
		return errors.New("coverage_start_ns must be positive")
	}
	if tuple.CoverageEndNS != nil && *tuple.CoverageEndNS <= tuple.CoverageStartNS {
		return errors.New("coverage_end_ns must be greater than coverage_start_ns")
	}
	return nil
}

func validatePlatformBuildIdentity(build BuildInfo) error {
	for name, value := range map[string]string{
		"build version": build.Version,
		"build commit":  build.Commit,
		"build date":    build.Date,
	} {
		if err := validatePlatformText(name, value); err != nil {
			return fmt.Errorf("verify: platform %w", err)
		}
	}
	builtAt, err := time.Parse(time.RFC3339Nano, build.Date)
	if err != nil {
		return errors.New("verify: platform build date must be an RFC3339 timestamp")
	}
	_, offset := builtAt.Zone()
	if offset != 0 {
		return errors.New("verify: platform build date must be explicit UTC")
	}
	return nil
}

func validatePlatformText(name, value string) error {
	if value == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s is blank or has surrounding whitespace", name)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s contains control characters", name)
	}
	return nil
}

func validLifecycleStatus(status LifecycleStatus) bool {
	switch status {
	case LifecycleCandidate, LifecycleCaptured, LifecycleReplayable, LifecycleNormalized,
		LifecycleReconstructed, LifecycleSupported, LifecycleUnsupported, LifecycleAmbiguous:
		return true
	default:
		return false
	}
}

func validParityStatus(status ParityStatus) bool {
	switch status {
	case ParityProved, ParityPartiallyProved, ParityUnsupported, ParityAmbiguous:
		return true
	default:
		return false
	}
}

func validPlatformSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func referencesContainKind(refs []ArtifactReference, kind EvidenceKind) bool {
	return slices.ContainsFunc(refs, func(ref ArtifactReference) bool {
		return ref.Kind == kind
	})
}

func canonicalPlatformRows(rows []ParityRow) []ParityRow {
	canonical := make([]ParityRow, len(rows))
	for i, row := range rows {
		canonical[i] = row
		canonical[i].Tuple = cloneTupleIdentity(row.Tuple)
		canonical[i].SourceEvidence = canonicalArtifactReferences(row.SourceEvidence)
		canonical[i].FixtureEvidence = canonicalArtifactReferences(row.FixtureEvidence)
		canonical[i].RawEvidence = canonicalArtifactReferences(row.RawEvidence)
		canonical[i].DatasetEvidence = canonicalArtifactReferences(row.DatasetEvidence)
		canonical[i].ReplayEvidence = canonicalArtifactReferences(row.ReplayEvidence)
		canonical[i].CatalogEvidence = canonicalArtifactReferences(row.CatalogEvidence)
	}
	slices.SortFunc(canonical, func(a, b ParityRow) int {
		return compareTupleIdentity(a.Tuple, b.Tuple)
	})
	return canonical
}

func canonicalArtifactReferences(refs []ArtifactReference) []ArtifactReference {
	canonical := make([]ArtifactReference, len(refs))
	copy(canonical, refs)
	slices.SortFunc(canonical, compareArtifactReference)
	return canonical
}

func compareArtifactReference(a, b ArtifactReference) int {
	return cmp.Or(
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.SHA256, b.SHA256),
		cmp.Compare(a.Locator, b.Locator),
	)
}

func cloneTupleIdentity(tuple TupleIdentity) TupleIdentity {
	if tuple.CoverageEndNS == nil {
		return tuple
	}
	end := *tuple.CoverageEndNS
	tuple.CoverageEndNS = &end
	return tuple
}

func compareTupleIdentity(a, b TupleIdentity) int {
	if order := cmp.Or(
		cmp.Compare(a.SourceID, b.SourceID),
		cmp.Compare(a.APIVersion, b.APIVersion),
		cmp.Compare(a.Entitlement, b.Entitlement),
		cmp.Compare(a.Channel, b.Channel),
		cmp.Compare(a.DataFamily, b.DataFamily),
		cmp.Compare(a.NativeGranularity, b.NativeGranularity),
		cmp.Compare(a.CoverageStartNS, b.CoverageStartNS),
	); order != 0 {
		return order
	}
	if a.CoverageEndNS == nil && b.CoverageEndNS != nil {
		return -1
	}
	if a.CoverageEndNS != nil && b.CoverageEndNS == nil {
		return 1
	}
	if a.CoverageEndNS != nil {
		if order := cmp.Compare(*a.CoverageEndNS, *b.CoverageEndNS); order != 0 {
			return order
		}
	}
	return cmp.Compare(a.AdapterVersion, b.AdapterVersion)
}

type platformTupleKey struct {
	sourceID          string
	apiVersion        string
	entitlement       string
	channel           string
	dataFamily        string
	nativeGranularity string
	coverageStartNS   int64
	coverageEndNS     int64
	hasCoverageEnd    bool
	adapterVersion    string
}

func platformKey(tuple TupleIdentity) platformTupleKey {
	key := platformTupleKey{
		sourceID:          tuple.SourceID,
		apiVersion:        tuple.APIVersion,
		entitlement:       tuple.Entitlement,
		channel:           tuple.Channel,
		dataFamily:        tuple.DataFamily,
		nativeGranularity: tuple.NativeGranularity,
		coverageStartNS:   tuple.CoverageStartNS,
		adapterVersion:    tuple.AdapterVersion,
	}
	if tuple.CoverageEndNS != nil {
		key.coverageEndNS = *tuple.CoverageEndNS
		key.hasCoverageEnd = true
	}
	return key
}
