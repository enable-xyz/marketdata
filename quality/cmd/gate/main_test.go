package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/quality"
)

func TestBoundedLoaderRejectsTraversalSymlinkAndHashMismatch(t *testing.T) {
	root := t.TempDir()
	data := []byte("fixed-corpus")
	path := filepath.Join(root, "fixture.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	valid := fileRef{Path: "fixture.bin", Bytes: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}

	t.Run("traversal", func(t *testing.T) {
		if _, err := containedRegularPath(root, "../fixture.bin"); err == nil {
			t.Fatal("traversal path was accepted")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		if err := os.Symlink(path, filepath.Join(root, "link.bin")); err != nil {
			t.Fatal(err)
		}
		ref := valid
		ref.Path = "link.bin"
		loader := boundedLoader{base: root, fileLimit: 1 << 20, totalLimit: 1 << 20, cache: make(map[string]loadedFile)}
		if _, err := loader.read(ref); err == nil || !strings.Contains(err.Error(), "symbolic link") {
			t.Fatalf("symlink error = %v", err)
		}
	})

	t.Run("hash", func(t *testing.T) {
		ref := valid
		ref.SHA256 = strings.Repeat("0", sha256.Size*2)
		loader := boundedLoader{base: root, fileLimit: 1 << 20, totalLimit: 1 << 20, cache: make(map[string]loadedFile)}
		if _, err := loader.read(ref); err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
			t.Fatalf("hash error = %v", err)
		}
	})
}

func TestDecodeStrictRejectsUnknownAndDuplicateFields(t *testing.T) {
	var target struct {
		Value int `json:"value"`
	}
	for _, input := range []string{`{"value":1,"extra":2}`, `{"value":1,"value":2}`} {
		if err := decodeStrict([]byte(input), &target); err == nil {
			t.Fatalf("decodeStrict(%s) succeeded", input)
		}
	}
}

func TestManifestSignaturesRequireExternalTrustedSigner(t *testing.T) {
	trustedPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	unrelatedPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, ed25519.SeedSize))
	trustedPublic := trustedPrivate.Public().(ed25519.PublicKey)
	unrelatedPublic := unrelatedPrivate.Public().(ed25519.PublicKey)

	declaration, err := signHardware(quality.HardwareIdentity{ID: "hardware-a"}, trustedPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyHardwareDeclaration(declaration, trustedPublic); err != nil {
		t.Fatalf("trusted signer rejected: %v", err)
	}
	if err := verifyHardwareDeclaration(declaration, nil); err == nil || !strings.Contains(err.Error(), "trusted signer public key is required") {
		t.Fatalf("missing trust anchor error = %v", err)
	}
	if err := verifyHardwareDeclaration(declaration, unrelatedPublic); err == nil || !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("unrelated trust anchor error = %v", err)
	}

	encoded, err := json.Marshal(declaration)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(`"public_key"`)) {
		t.Fatalf("signed declaration embeds its own trust root: %s", encoded)
	}
	var decoded signedHardware
	embedded := append(encoded[:len(encoded)-1], []byte(`,"public_key":"`+hex.EncodeToString(unrelatedPublic)+`"}`)...)
	if err := decodeStrict(embedded, &decoded); err == nil {
		t.Fatal("embedded-only signer key was accepted")
	}
}

func TestTrustedSignerPublicKeyRequiresCanonicalExactHex(t *testing.T) {
	trustedPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{3}, ed25519.SeedSize))
	valid := hex.EncodeToString(trustedPrivate.Public().(ed25519.PublicKey))
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "missing", input: "", wantErr: true},
		{name: "short", input: valid[:len(valid)-2], wantErr: true},
		{name: "malformed", input: strings.Repeat("z", ed25519.PublicKeySize*2), wantErr: true},
		{name: "uppercase", input: strings.ToUpper(valid), wantErr: true},
		{name: "success", input: valid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeTrustedSignerPublicKey(test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("decodeTrustedSignerPublicKey() error = %v, wantErr %v", err, test.wantErr)
			}
			if err == nil && !bytes.Equal(got, trustedPrivate.Public().(ed25519.PublicKey)) {
				t.Fatalf("decoded key = %x, want %s", got, valid)
			}
		})
	}
	if _, err := loadInputManifest(filepath.Join(t.TempDir(), "manifest.json"), ""); err == nil || !strings.Contains(err.Error(), "trusted signer public key") {
		t.Fatalf("manifest loader missing trust anchor error = %v", err)
	}
}

func TestGateCommandsRequireExternalKeyFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		flag string
	}{
		{
			name: "prepare signing key file",
			args: []string{"prepare", "--spec", "spec.json", "--output-dir", "output"},
			flag: "signing-private-key-file",
		},
		{
			name: "measure trusted signer",
			args: []string{"measure", "--input-manifest", "manifest.json", "--output", "observation.json", "--duration", "60m", "--burst-duration", "30s"},
			flag: "trusted-signer-public-key",
		},
		{
			name: "verify trusted signer",
			args: []string{
				"verify", "--input-manifest", "manifest.json", "--output", "report.json",
				"--observation", "observation.json", "--observation-sha256", strings.Repeat("0", 64),
				"--fault-receipt", "fault.json", "--fault-receipt-sha256", strings.Repeat("0", 64),
				"--determinism-receipt", "determinism.json", "--determinism-receipt-sha256", strings.Repeat("0", 64),
				"--canary-receipt", "canary.json", "--canary-receipt-sha256", strings.Repeat("0", 64),
				"--x5-receipt", "x5.json", "--x5-receipt-sha256", strings.Repeat("0", 64),
			},
			flag: "trusted-signer-public-key",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := execute(t.Context(), new(bytes.Buffer), new(bytes.Buffer), test.args)
			if err == nil || !strings.Contains(err.Error(), `required flag(s) "`+test.flag+`" not set`) {
				t.Fatalf("missing %s error = %v", test.flag, err)
			}
		})
	}
}

type sequenceClock struct {
	values []int64
	next   int
}

func (c *sequenceClock) NowNS() int64 {
	value := c.values[c.next]
	c.next++
	return value
}

type sequenceRSS struct {
	values []uint64
	next   int
}

func (r *sequenceRSS) RSSBytes() (uint64, error) {
	value := r.values[r.next]
	r.next++
	return value, nil
}

type recordingProcessor struct {
	objects    int
	families   []string
	normalized []int
	seen       []int
	modes      []workloadMode
}

func (p *recordingProcessor) ObjectIndexes(mode workloadMode) ([]int, error) {
	switch mode {
	case modeNative, modeTelemetryBlackhole:
		indexes := make([]int, p.objects)
		for index := range p.objects {
			indexes[index] = index
		}
		return indexes, nil
	case modeNormalized:
		if len(p.normalized) == 0 {
			return nil, errUnsupportedWorkloadStep
		}
		return slices.Clone(p.normalized), nil
	default:
		return nil, errUnsupportedWorkloadStep
	}
}

func (p *recordingProcessor) Process(_ context.Context, index int, mode workloadMode, _ func() error) (workSample, error) {
	p.seen = append(p.seen, index)
	p.modes = append(p.modes, mode)
	return workSample{Expected: 1, Committed: 1, Bytes: 3, Digest: sha256.Sum256([]byte{byte(index)})}, nil
}

func (*recordingProcessor) Corruption(context.Context) (corruptionEvidence, error) {
	return corruptionEvidence{}, nil
}

type failingProcessor struct {
	*recordingProcessor
	failIndex int
}

func (p *failingProcessor) Process(ctx context.Context, index int, mode workloadMode, observe func() error) (workSample, error) {
	if index == p.failIndex {
		return workSample{}, errors.New("synthetic invalid corpus")
	}
	return p.recordingProcessor.Process(ctx, index, mode, observe)
}

func TestMeasurementUsesInjectedElapsedRateRSSAndDeterministicCycling(t *testing.T) {
	run := func() (runEvidence, []int) {
		processor := &recordingProcessor{objects: 2}
		engine := measurementEngine{
			clock:     &sequenceClock{values: []int64{0, 10, 20, 30}},
			rss:       &sequenceRSS{values: []uint64{100, 110, 120, 130}},
			processor: processor,
			rssPolicy: rssPolicy{MaximumSamples: 8, PlateauWindowSamples: 1, PlateauToleranceBytes: 10, SampleIntervalNS: 10},
		}
		evidence, err := engine.runFor(t.Context(), 30, modeNative, 4)
		if err != nil {
			t.Fatal(err)
		}
		return evidence, processor.seen
	}

	first, firstOrder := run()
	second, secondOrder := run()
	if !slices.Equal(firstOrder, []int{0, 1, 0}) || !slices.Equal(secondOrder, firstOrder) {
		t.Fatalf("cycling orders = %v and %v", firstOrder, secondOrder)
	}
	if first.DurationNS != 30 || first.ExpectedRecords != 3 || first.CommittedRecords != 3 || first.RawBytes != 9 || first.PeakRSSBytes != 130 {
		t.Fatalf("evidence = %+v", first)
	}
	if first.RawSHA256 != second.RawSHA256 {
		t.Fatalf("deterministic raw hashes differ: %s != %s", first.RawSHA256, second.RawSHA256)
	}
	rates, err := observedRates(first)
	if err != nil {
		t.Fatal(err)
	}
	if rates.records != 100_000_000 || rates.bytes != 300_000_000 {
		t.Fatalf("rates = %+v", rates)
	}
	if !slices.Equal(first.RSSSamples, []uint64{100, 110, 120, 130}) {
		t.Fatalf("RSS samples = %v", first.RSSSamples)
	}
}

func TestMeasurementSelectsNormalizedObjectsAndRawTelemetryCorpus(t *testing.T) {
	run := func(mode workloadMode, normalized []int) (*recordingProcessor, error) {
		processor := &recordingProcessor{objects: 3, families: []string{"venue-a", "venue-b", "venue-c"}, normalized: normalized}
		engine := measurementEngine{
			clock:     &sequenceClock{values: []int64{0, 10, 20}},
			rss:       &sequenceRSS{values: []uint64{100, 100, 100}},
			processor: processor,
			rssPolicy: rssPolicy{MaximumSamples: 8, PlateauWindowSamples: 1, SampleIntervalNS: 10},
		}
		_, err := engine.runFor(t.Context(), 20, mode, 3)
		return processor, err
	}
	normalized, err := run(modeNormalized, []int{0, 2})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(normalized.seen, []int{0, 2}) || !slices.Equal(normalized.modes, []workloadMode{modeNormalized, modeNormalized}) {
		t.Fatalf("normalized selection = %v, modes = %v", normalized.seen, normalized.modes)
	}
	if normalized.families[normalized.seen[0]] == normalized.families[normalized.seen[1]] {
		t.Fatalf("normalized selection did not span distinct venue families: %v", normalized.families)
	}
	telemetry, err := run(modeTelemetryBlackhole, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(telemetry.seen, []int{0, 1}) || !slices.Equal(telemetry.modes, []workloadMode{modeTelemetryBlackhole, modeTelemetryBlackhole}) {
		t.Fatalf("telemetry selection = %v, modes = %v", telemetry.seen, telemetry.modes)
	}
	if _, err := run(modeNormalized, nil); !errors.Is(err, errUnsupportedWorkloadStep) {
		t.Fatalf("missing normalized workload error = %v", err)
	}
}

func TestNormalizedCorpusPreflightRejectsBeforeTimedMeasurement(t *testing.T) {
	processor := &recordingProcessor{objects: 3, normalized: []int{2, 0}}
	if err := preflightNormalizedCorpus(t.Context(), processor); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(processor.seen, []int{2, 0}) ||
		!slices.Equal(processor.modes, []workloadMode{modeNormalized, modeNormalized}) {
		t.Fatalf("preflight selection = %v modes = %v", processor.seen, processor.modes)
	}

	failing := &failingProcessor{
		recordingProcessor: &recordingProcessor{objects: 3, normalized: []int{2, 0}},
		failIndex:          2,
	}
	if err := preflightNormalizedCorpus(t.Context(), failing); err == nil ||
		!strings.Contains(err.Error(), "normalized corpus object 2: synthetic invalid corpus") {
		t.Fatalf("preflight error = %v", err)
	}
}

func TestShortGateDurationFailsBeforeWorkload(t *testing.T) {
	_, err := runMeasurement(t.Context(), loadedManifest{}, measureConfig{
		SustainedDurationNS: quality.MinimumSustainedDurationNS - 1,
		BurstDurationNS:     quality.MinimumBurstDurationNS,
		EnforceGateMinimums: true,
	}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "shorter than the release-gate minimum") {
		t.Fatalf("error = %v", err)
	}
}

func TestReceiptExactSetsRejectMissingAndExtraValues(t *testing.T) {
	contracts := []quality.ContractIdentity{{ContractID: "alpha"}, {ContractID: "beta"}}
	requiredFaults := quality.RequiredFaultCategoryIDs()
	categories := make([]quality.FaultCategoryObservation, len(requiredFaults))
	for i, id := range requiredFaults {
		categories[i].CategoryID = id
	}
	validFaults := []quality.ContractFaultCoverage{
		{ContractID: "alpha", Categories: slices.Clone(categories)},
		{ContractID: "beta", Categories: slices.Clone(categories)},
	}
	if err := validateFaultSet(contracts, validFaults[:1]); err == nil || !strings.Contains(err.Error(), "missing or extra contract") {
		t.Fatalf("missing contract error = %v", err)
	}
	extraCategory := slices.Clone(validFaults)
	extraCategory[0].Categories = append(extraCategory[0].Categories, quality.FaultCategoryObservation{CategoryID: quality.FaultCategoryID("unknown")})
	if err := validateFaultSet(contracts, extraCategory); err == nil || !strings.Contains(err.Error(), "missing or extra category") {
		t.Fatalf("extra category error = %v", err)
	}

	requiredGates := quality.RequiredDeterminismGateIDs()
	gates := make([]quality.DeterminismObservation, len(requiredGates))
	for i, id := range requiredGates {
		gates[i].GateID = id
	}
	if err := validateDeterminismSet(contracts, []quality.ContractDeterminismCoverage{{ContractID: "alpha", Gates: gates}}); err == nil {
		t.Fatal("missing determinism contract was accepted")
	}
	if err := validateCanarySet(contracts, []quality.CanaryObservation{{ContractID: "alpha"}, {ContractID: "beta"}, {ContractID: "gamma"}}); err == nil {
		t.Fatal("extra canary contract was accepted")
	}
}

func TestWriteExclusiveNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := writeExclusive(path, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(path, []byte("second")); err == nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("second create error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "first" {
		t.Fatalf("output was overwritten: %q", data)
	}
}
