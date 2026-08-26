package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/enable-xyz/marketdata/quality"
)

type faultReceipt struct {
	Format         string                          `json:"format"`
	ManifestSHA256 string                          `json:"manifest_sha256"`
	Coverage       []quality.ContractFaultCoverage `json:"coverage"`
	BodySHA256     string                          `json:"body_sha256"`
}

type determinismReceipt struct {
	Format         string                                `json:"format"`
	ManifestSHA256 string                                `json:"manifest_sha256"`
	Coverage       []quality.ContractDeterminismCoverage `json:"coverage"`
	BodySHA256     string                                `json:"body_sha256"`
}

type canaryReceipt struct {
	Format         string                      `json:"format"`
	ManifestSHA256 string                      `json:"manifest_sha256"`
	Canaries       []quality.CanaryObservation `json:"canaries"`
	BodySHA256     string                      `json:"body_sha256"`
}

type x5Receipt struct {
	Format         string                           `json:"format"`
	ManifestSHA256 string                           `json:"manifest_sha256"`
	Observation    quality.X5QueryBudgetObservation `json:"observation"`
	BodySHA256     string                           `json:"body_sha256"`
}

type evidencePath struct {
	Path   string
	SHA256 string
}

type verifyInputs struct {
	Observation evidencePath
	Fault       evidencePath
	Determinism evidencePath
	Canary      evidencePath
	X5          evidencePath
}

func verifyReleaseGate(manifest loadedManifest, inputs verifyInputs) (quality.ReleaseGateReport, error) {
	observation, err := loadObservation(inputs.Observation, manifest)
	if err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("observation artifact: %w", err)
	}
	faults, err := loadFaultReceipt(inputs.Fault, manifest)
	if err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("fault receipt: %w", err)
	}
	determinism, err := loadDeterminismReceipt(inputs.Determinism, manifest)
	if err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("determinism receipt: %w", err)
	}
	canaries, err := loadCanaryReceipt(inputs.Canary, manifest)
	if err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("canary receipt: %w", err)
	}
	queries, err := loadX5Receipt(inputs.X5, manifest)
	if err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("X5 receipt: %w", err)
	}
	if err := validateFaultSet(manifest.value.Workload.Value.Contracts, faults.Coverage); err != nil {
		return quality.ReleaseGateReport{}, err
	}
	if err := validateDeterminismSet(manifest.value.Workload.Value.Contracts, determinism.Coverage); err != nil {
		return quality.ReleaseGateReport{}, err
	}
	if err := validateCanarySet(manifest.value.Workload.Value.Contracts, canaries.Canaries); err != nil {
		return quality.ReleaseGateReport{}, err
	}
	if err := validateX5Set(queries.Observation); err != nil {
		return quality.ReleaseGateReport{}, err
	}
	performance := observation.Performance
	performance.Queries = queries.Observation
	bundle := quality.ReleaseGateBundle{
		Hardware: manifest.value.Hardware.Value, Workload: manifest.value.Workload.Value,
		Corpus: manifest.value.FixedCorpus.Value, Performance: performance,
		FaultCoverage: faults.Coverage, DeterminismCoverage: determinism.Coverage, Canaries: canaries.Canaries,
	}
	report, err := quality.EvaluateReleaseGate(bundle)
	if err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("evaluate release gate: %w", err)
	}
	if _, err := report.ContentSHA256(); err != nil {
		return quality.ReleaseGateReport{}, fmt.Errorf("validate canonical release gate report: %w", err)
	}
	if err := report.Validate(); err != nil {
		if errors.Is(err, quality.ErrReleaseGateFailed) {
			return report, err
		}
		return quality.ReleaseGateReport{}, fmt.Errorf("validate release gate report: %w", err)
	}
	return report, nil
}

func loadObservation(input evidencePath, manifest loadedManifest) (observationArtifact, error) {
	data, err := loadEvidenceBytes(input)
	if err != nil {
		return observationArtifact{}, err
	}
	var artifact observationArtifact
	if err := decodeStrict(data, &artifact); err != nil {
		return observationArtifact{}, err
	}
	if artifact.Format != observationFormat || artifact.ManifestSHA256 != manifest.sha256 || artifact.HardwareManifestSHA256 != manifest.value.Hardware.Value.ManifestSHA256 || artifact.WorkloadManifestSHA256 != manifest.value.Workload.Value.ManifestSHA256 || artifact.CorpusManifestSHA256 != manifest.value.FixedCorpus.Value.ManifestSHA256 {
		return observationArtifact{}, errors.New("format or manifest identity mismatch")
	}
	rebuilt, err := buildObservation(manifest, artifact.Evidence.Sustained, artifact.Evidence.Burst, artifact.Evidence.Replay.Native, artifact.Evidence.Replay.Normalized, artifact.Evidence.Telemetry, artifact.Evidence.Corruption)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("recompute observation: %w", err)
	}
	actual, err := json.Marshal(artifact)
	if err != nil {
		return observationArtifact{}, err
	}
	expected, err := json.Marshal(rebuilt)
	if err != nil {
		return observationArtifact{}, err
	}
	if !slices.Equal(actual, expected) {
		return observationArtifact{}, errors.New("observation fields or evidence hashes are not canonical")
	}
	return artifact, nil
}

func loadFaultReceipt(input evidencePath, manifest loadedManifest) (faultReceipt, error) {
	data, err := loadEvidenceBytes(input)
	if err != nil {
		return faultReceipt{}, err
	}
	var receipt faultReceipt
	if err := decodeStrict(data, &receipt); err != nil {
		return faultReceipt{}, err
	}
	body := receipt
	body.BodySHA256 = ""
	if err := validateReceiptIdentity(receipt.Format, faultReceiptFormat, receipt.ManifestSHA256, manifest.sha256, receipt.BodySHA256, body); err != nil {
		return faultReceipt{}, err
	}
	return receipt, nil
}

func loadDeterminismReceipt(input evidencePath, manifest loadedManifest) (determinismReceipt, error) {
	data, err := loadEvidenceBytes(input)
	if err != nil {
		return determinismReceipt{}, err
	}
	var receipt determinismReceipt
	if err := decodeStrict(data, &receipt); err != nil {
		return determinismReceipt{}, err
	}
	body := receipt
	body.BodySHA256 = ""
	if err := validateReceiptIdentity(receipt.Format, determinismFormat, receipt.ManifestSHA256, manifest.sha256, receipt.BodySHA256, body); err != nil {
		return determinismReceipt{}, err
	}
	return receipt, nil
}

func loadCanaryReceipt(input evidencePath, manifest loadedManifest) (canaryReceipt, error) {
	data, err := loadEvidenceBytes(input)
	if err != nil {
		return canaryReceipt{}, err
	}
	var receipt canaryReceipt
	if err := decodeStrict(data, &receipt); err != nil {
		return canaryReceipt{}, err
	}
	body := receipt
	body.BodySHA256 = ""
	if err := validateReceiptIdentity(receipt.Format, canaryReceiptFormat, receipt.ManifestSHA256, manifest.sha256, receipt.BodySHA256, body); err != nil {
		return canaryReceipt{}, err
	}
	return receipt, nil
}

func loadX5Receipt(input evidencePath, manifest loadedManifest) (x5Receipt, error) {
	data, err := loadEvidenceBytes(input)
	if err != nil {
		return x5Receipt{}, err
	}
	var receipt x5Receipt
	if err := decodeStrict(data, &receipt); err != nil {
		return x5Receipt{}, err
	}
	body := receipt
	body.BodySHA256 = ""
	if err := validateReceiptIdentity(receipt.Format, x5ReceiptFormat, receipt.ManifestSHA256, manifest.sha256, receipt.BodySHA256, body); err != nil {
		return x5Receipt{}, err
	}
	return receipt, nil
}

func validateReceiptIdentity(actualFormat, expectedFormat, actualManifest, expectedManifest, actualBody string, body any) error {
	if actualFormat != expectedFormat {
		return fmt.Errorf("unsupported format %q", actualFormat)
	}
	if actualManifest != expectedManifest {
		return errors.New("input manifest SHA-256 mismatch")
	}
	digest, err := canonicalDigest(body)
	if err != nil {
		return err
	}
	if actualBody != digest {
		return errors.New("body SHA-256 mismatch")
	}
	return nil
}

func loadEvidenceBytes(input evidencePath) ([]byte, error) {
	if !validSHA256(input.SHA256) {
		return nil, errors.New("caller-supplied file SHA-256 is missing or invalid")
	}
	data, err := readTopLevelRegular(input.Path, maxEvidenceBytes)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != input.SHA256 {
		return nil, errors.New("caller-supplied file SHA-256 mismatch")
	}
	return data, nil
}

func validateFaultSet(contracts []quality.ContractIdentity, coverage []quality.ContractFaultCoverage) error {
	expectedContracts := contractIDSet(contracts)
	if len(coverage) != len(expectedContracts) {
		return errors.New("fault receipt has a missing or extra contract")
	}
	required := quality.RequiredFaultCategoryIDs()
	seenContracts := make(map[string]struct{}, len(coverage))
	for _, item := range coverage {
		if _, ok := expectedContracts[item.ContractID]; !ok {
			return fmt.Errorf("fault receipt contains undeclared contract %q", item.ContractID)
		}
		if _, duplicate := seenContracts[item.ContractID]; duplicate {
			return fmt.Errorf("fault receipt duplicates contract %q", item.ContractID)
		}
		seenContracts[item.ContractID] = struct{}{}
		if len(item.Categories) != len(required) {
			return fmt.Errorf("fault receipt contract %q has a missing or extra category", item.ContractID)
		}
		seen := make(map[quality.FaultCategoryID]struct{}, len(item.Categories))
		for _, category := range item.Categories {
			if !slices.Contains(required, category.CategoryID) {
				return fmt.Errorf("fault receipt contract %q has unknown category %q", item.ContractID, category.CategoryID)
			}
			if _, duplicate := seen[category.CategoryID]; duplicate {
				return fmt.Errorf("fault receipt contract %q duplicates category %q", item.ContractID, category.CategoryID)
			}
			seen[category.CategoryID] = struct{}{}
		}
	}
	return nil
}

func validateDeterminismSet(contracts []quality.ContractIdentity, coverage []quality.ContractDeterminismCoverage) error {
	expectedContracts := contractIDSet(contracts)
	if len(coverage) != len(expectedContracts) {
		return errors.New("determinism receipt has a missing or extra contract")
	}
	required := quality.RequiredDeterminismGateIDs()
	seenContracts := make(map[string]struct{}, len(coverage))
	for _, item := range coverage {
		if _, ok := expectedContracts[item.ContractID]; !ok {
			return fmt.Errorf("determinism receipt contains undeclared contract %q", item.ContractID)
		}
		if _, duplicate := seenContracts[item.ContractID]; duplicate {
			return fmt.Errorf("determinism receipt duplicates contract %q", item.ContractID)
		}
		seenContracts[item.ContractID] = struct{}{}
		if len(item.Gates) != len(required) {
			return fmt.Errorf("determinism receipt contract %q has a missing or extra gate", item.ContractID)
		}
		seen := make(map[quality.DeterminismGateID]struct{}, len(item.Gates))
		for _, gate := range item.Gates {
			if !slices.Contains(required, gate.GateID) {
				return fmt.Errorf("determinism receipt contract %q has unknown gate %q", item.ContractID, gate.GateID)
			}
			if _, duplicate := seen[gate.GateID]; duplicate {
				return fmt.Errorf("determinism receipt contract %q duplicates gate %q", item.ContractID, gate.GateID)
			}
			seen[gate.GateID] = struct{}{}
		}
	}
	return nil
}

func validateCanarySet(contracts []quality.ContractIdentity, canaries []quality.CanaryObservation) error {
	expected := contractIDSet(contracts)
	if len(canaries) != len(expected) {
		return errors.New("canary receipt has a missing or extra contract")
	}
	seen := make(map[string]struct{}, len(canaries))
	for _, canary := range canaries {
		if _, ok := expected[canary.ContractID]; !ok {
			return fmt.Errorf("canary receipt contains undeclared contract %q", canary.ContractID)
		}
		if _, duplicate := seen[canary.ContractID]; duplicate {
			return fmt.Errorf("canary receipt duplicates contract %q", canary.ContractID)
		}
		seen[canary.ContractID] = struct{}{}
	}
	return nil
}

func validateX5Set(observation quality.X5QueryBudgetObservation) error {
	if len(observation.RequiredIDs) == 0 || len(observation.RequiredIDs) != len(observation.Thresholds) || len(observation.RequiredIDs) > quality.MaxReleaseGateQueryThresholds {
		return errors.New("X5 receipt has an empty, missing, or extra query set")
	}
	required := make(map[string]struct{}, len(observation.RequiredIDs))
	for _, id := range observation.RequiredIDs {
		if id == "" {
			return errors.New("X5 receipt has an empty query ID")
		}
		if _, duplicate := required[id]; duplicate {
			return fmt.Errorf("X5 receipt duplicates required query %q", id)
		}
		required[id] = struct{}{}
	}
	seen := make(map[string]struct{}, len(observation.Thresholds))
	for _, threshold := range observation.Thresholds {
		if _, ok := required[threshold.ID]; !ok {
			return fmt.Errorf("X5 receipt contains undeclared query %q", threshold.ID)
		}
		if _, duplicate := seen[threshold.ID]; duplicate {
			return fmt.Errorf("X5 receipt duplicates threshold %q", threshold.ID)
		}
		seen[threshold.ID] = struct{}{}
	}
	return nil
}

func contractIDSet(contracts []quality.ContractIdentity) map[string]struct{} {
	result := make(map[string]struct{}, len(contracts))
	for _, contract := range contracts {
		result[contract.ContractID] = struct{}{}
	}
	return result
}

func marshalFullReport(report quality.ReleaseGateReport) ([]byte, error) {
	if _, err := report.ContentSHA256(); err != nil {
		return nil, err
	}
	return json.Marshal(report)
}

func writeExclusive(path string, data []byte) (err error) {
	if path == "" {
		return errors.New("output path is required")
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
