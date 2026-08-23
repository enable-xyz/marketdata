package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/verify"
)

const (
	maxInputBytes    int64 = 16 << 20
	maxArtifactBytes int64 = 64 << 20
)

type options struct {
	inputPath  string
	outputPath string
}

type publicationInput struct {
	PlatformInput verify.PlatformBundleInput `json:"platform_bundle_input"`
	Artifacts     []artifactInput            `json:"artifacts"`
}

type artifactInput struct {
	SHA256          string `json:"sha256"`
	Locator         string `json:"locator"`
	Path            string `json:"path"`
	OperationalGate bool   `json:"operational_gate"`
}

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
	config, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	return publish(config.inputPath, config.outputPath)
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	var result options
	flags := flag.NewFlagSet("platform", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&result.inputPath, "input", "", "strict JSON platform bundle input package")
	flags.StringVar(&result.outputPath, "output", "", "new platform bundle output file")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 || result.inputPath == "" || result.outputPath == "" {
		return options{}, errors.New("platform: --input and --output are required; positional arguments are not accepted")
	}
	return result, nil
}

func publish(inputPath, outputPath string) error {
	encodedInput, err := readRegularFile(inputPath, maxInputBytes)
	if err != nil {
		return fmt.Errorf("platform: read input: %w", err)
	}
	input, err := decodePublicationInput(encodedInput)
	if err != nil {
		return fmt.Errorf("platform: decode input: %w", err)
	}
	inventory, loaded, err := loadInventory(input)
	if err != nil {
		return err
	}
	bundle, err := verify.BuildPlatformBundle(input.PlatformInput, inventory)
	if err != nil {
		return fmt.Errorf("platform: build bundle: %w", err)
	}
	if _, err := verify.ValidatePlatformBundle(bundle, input.PlatformInput.Declarations, inventory); err != nil {
		return fmt.Errorf("platform: validate built bundle: %w", err)
	}
	if err := rejectSerializedPaths(bundle, inputPath, outputPath, loaded); err != nil {
		return err
	}
	if err := writeExclusive(outputPath, bundle); err != nil {
		return fmt.Errorf("platform: write output: %w", err)
	}
	return nil
}

func decodePublicationInput(data []byte) (publicationInput, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var input publicationInput
	if err := decoder.Decode(&input); err != nil {
		return publicationInput{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return publicationInput{}, errors.New("trailing JSON value")
		}
		return publicationInput{}, fmt.Errorf("trailing JSON: %w", err)
	}
	if input.Artifacts == nil {
		return publicationInput{}, errors.New("artifacts is null")
	}
	return input, nil
}

func loadInventory(input publicationInput) (verify.ArtifactInventory, []artifactInput, error) {
	used, gates := referencedDigests(input.PlatformInput)
	inventory := make(verify.ArtifactInventory, len(input.Artifacts))
	loaded := make([]artifactInput, 0, len(input.Artifacts))
	locators := make(map[string]string, len(input.Artifacts))
	for i, artifact := range input.Artifacts {
		if !validSHA256(artifact.SHA256) {
			return nil, nil, fmt.Errorf("platform: artifact %d has an invalid lowercase SHA-256", i)
		}
		if artifact.Path == "" {
			return nil, nil, fmt.Errorf("platform: artifact %d path is blank", i)
		}
		if _, exists := inventory[artifact.SHA256]; exists {
			return nil, nil, fmt.Errorf("platform: duplicate artifact digest %q", artifact.SHA256)
		}
		if prior, exists := locators[artifact.Locator]; exists {
			return nil, nil, fmt.Errorf("platform: duplicate artifact locator %q for %s and %s", artifact.Locator, prior, artifact.SHA256)
		}
		if _, exists := used[artifact.SHA256]; !exists {
			return nil, nil, fmt.Errorf("platform: artifact %s is unused", artifact.SHA256)
		}
		_, usedAsGate := gates[artifact.SHA256]
		if artifact.OperationalGate != usedAsGate {
			if usedAsGate {
				return nil, nil, fmt.Errorf("platform: operational gate artifact %s is not marked operational_gate", artifact.SHA256)
			}
			return nil, nil, fmt.Errorf("platform: artifact %s is marked operational_gate but is not referenced as one", artifact.SHA256)
		}

		data, err := readRegularFile(artifact.Path, maxArtifactBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("platform: read artifact %d: %w", i, err)
		}
		actual := sha256.Sum256(data)
		if hex.EncodeToString(actual[:]) != artifact.SHA256 {
			return nil, nil, fmt.Errorf("platform: artifact %d SHA-256 mismatch", i)
		}

		entry := verify.ArtifactInventoryEntry{Locator: artifact.Locator}
		item := artifact
		if artifact.OperationalGate {
			report, err := parseCanonicalReleaseGate(data)
			if err != nil {
				return nil, nil, fmt.Errorf("platform: operational gate artifact %d: %w", i, err)
			}
			entry.OperationalGateValidator = operationalGateValidator(artifact.SHA256, report)
		}
		inventory[artifact.SHA256] = entry
		locators[artifact.Locator] = artifact.SHA256
		loaded = append(loaded, item)
	}
	for digest := range used {
		if _, exists := inventory[digest]; !exists {
			return nil, nil, fmt.Errorf("platform: referenced artifact %s has no input entry", digest)
		}
	}
	for i, row := range input.PlatformInput.Rows {
		if row.OperationalGateReportSHA256 == "" {
			continue
		}
		validator := inventory[row.OperationalGateReportSHA256].OperationalGateValidator
		if validator == nil {
			return nil, nil, fmt.Errorf("platform: parity row %d has no operational gate validator", i)
		}
		if err := validator(row.OperationalGateReportSHA256, row.Tuple, row.CanaryRequired); err != nil {
			return nil, nil, fmt.Errorf("platform: parity row %d operational gate: %w", i, err)
		}
	}
	return inventory, loaded, nil
}

func referencedDigests(input verify.PlatformBundleInput) (map[string]struct{}, map[string]struct{}) {
	used := make(map[string]struct{})
	gates := make(map[string]struct{})
	add := func(digest string) {
		if digest != "" {
			used[digest] = struct{}{}
		}
	}
	add(input.HardwareIdentitySHA256)
	add(input.WorkloadIdentitySHA256)
	add(input.X5EvidenceSHA256)
	for _, row := range input.Rows {
		if row.OperationalGateReportSHA256 != "" {
			add(row.OperationalGateReportSHA256)
			gates[row.OperationalGateReportSHA256] = struct{}{}
		}
		lists := [][]verify.ArtifactReference{
			row.SourceEvidence,
			row.FixtureEvidence,
			row.RawEvidence,
			row.DatasetEvidence,
			row.ReplayEvidence,
			row.CatalogEvidence,
		}
		for _, list := range lists {
			for _, ref := range list {
				add(ref.SHA256)
			}
		}
	}
	return used, gates
}

func parseCanonicalReleaseGate(data []byte) (quality.ReleaseGateReport, error) {
	report, err := quality.ParseReleaseGateReport(data)
	if err != nil {
		return quality.ReleaseGateReport{}, err
	}
	canonical, err := json.Marshal(report)
	if err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("marshal canonical report: %w", err)
	}
	if !bytes.Equal(data, canonical) {
		return quality.ReleaseGateReport{}, errors.New("release gate report encoding is not canonical")
	}
	return report, nil
}

func operationalGateValidator(digest string, report quality.ReleaseGateReport) verify.OperationalGateValidator {
	return func(reportSHA256 string, tuple verify.TupleIdentity, canaryRequired bool) error {
		if reportSHA256 != digest {
			return errors.New("report digest does not match its inventory entry")
		}
		if !report.Passed {
			return errors.New("release gate report did not pass")
		}
		if tuple.CoverageEndNS == nil {
			return errors.New("release gate report cannot prove an open coverage interval")
		}
		var match *quality.ContractGateResult
		for i := range report.ContractResults {
			result := &report.ContractResults[i]
			if !sameTuple(result.Tuple, tuple) {
				continue
			}
			if match != nil {
				return errors.New("release gate report has ambiguous matching contract results")
			}
			match = result
		}
		if match == nil {
			return errors.New("release gate report has no exact tuple result")
		}
		if !match.Passed {
			return errors.New("release gate tuple result did not pass")
		}
		wantCanary := quality.CanaryNotRequired
		if canaryRequired {
			wantCanary = quality.CanaryRequired
		}
		if match.CanaryRequirement != wantCanary || match.Tuple.CanaryRequirement != wantCanary {
			return errors.New("release gate tuple canary requirement does not match")
		}
		return nil
	}
}

func sameTuple(contract quality.ContractIdentity, tuple verify.TupleIdentity) bool {
	return tuple.CoverageEndNS != nil &&
		contract.SourceID == tuple.SourceID &&
		contract.APIVersion == tuple.APIVersion &&
		contract.Entitlement == tuple.Entitlement &&
		contract.ChannelOrEndpoint == tuple.Channel &&
		contract.DataFamily == tuple.DataFamily &&
		contract.NativeGranularity == tuple.NativeGranularity &&
		contract.CoverageStartNS == tuple.CoverageStartNS &&
		contract.CoverageEndNS == *tuple.CoverageEndNS &&
		contract.AdapterVersion == tuple.AdapterVersion
}

func rejectSerializedPaths(bundle []byte, inputPath, outputPath string, artifacts []artifactInput) error {
	paths := make([]string, 0, len(artifacts)+2)
	paths = append(paths, inputPath, outputPath)
	for _, artifact := range artifacts {
		paths = append(paths, artifact.Path)
	}
	for _, path := range paths {
		candidates := []string{path}
		if absolute, err := filepath.Abs(path); err == nil && absolute != path {
			candidates = append(candidates, absolute)
		}
		for _, candidate := range candidates {
			if candidate != "" && bytes.Contains(bundle, []byte(candidate)) {
				return errors.New("platform: a filesystem path would enter the serialized bundle")
			}
		}
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func readRegularFile(path string, limit int64) ([]byte, error) {
	if path == "" {
		return nil, errors.New("path is blank")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("path is not a regular non-symlink file")
	}
	if before.Size() > limit {
		return nil, fmt.Errorf("file exceeds %d-byte limit", limit)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, errors.New("file changed identity while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("file exceeds %d-byte limit", limit)
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(opened, after) || opened.Size() != after.Size() || !opened.ModTime().Equal(after.ModTime()) || int64(len(data)) != after.Size() {
		return nil, errors.New("file changed while reading")
	}
	return data, nil
}

func writeExclusive(path string, data []byte) (err error) {
	if path == "" {
		return errors.New("output path is blank")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	created, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	complete := false
	defer func() {
		if complete {
			return
		}
		_ = file.Close()
		current, statErr := os.Lstat(path)
		if statErr == nil && os.SameFile(created, current) {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	complete = true
	return nil
}
