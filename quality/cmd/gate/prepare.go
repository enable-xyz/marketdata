package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/elastic/go-sysinfo"
	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/spf13/cobra"
)

const (
	prepareSpecFormat      = "enable-market-release-gate-prepare/v1"
	preparedManifestName   = "manifest.json"
	maximumPrepareFixtures = 64
	maximumPrepareRecords  = 100_000
)

type prepareRates struct {
	MaxObservedMessagesPerSecond uint64 `json:"max_observed_messages_per_second"`
	MaxObservedBytesPerSecond    uint64 `json:"max_observed_bytes_per_second"`
	AcquisitionRecordsPerSecond  uint64 `json:"acquisition_records_per_second"`
	AcquisitionBytesPerSecond    uint64 `json:"acquisition_bytes_per_second"`
}

type payloadTargets struct {
	Tiny   int `json:"tiny"`
	Median int `json:"median"`
	Max    int `json:"max"`
}

type prepareDatasetConfig struct {
	RowGroupTargetBytes int64  `json:"row_group_target_bytes"`
	PageBufferBytes     int    `json:"page_buffer_bytes"`
	Dictionary          bool   `json:"dictionary"`
	BloomFilter         bool   `json:"bloom_filter"`
	MaxInputRows        uint64 `json:"max_input_rows"`
	MaxParquetRows      uint64 `json:"max_parquet_rows"`
}

type prepareNormalizer struct {
	CatalogSnapshotPath   string `json:"catalog_snapshot_path"`
	CatalogSnapshotBytes  int64  `json:"catalog_snapshot_bytes"`
	CatalogSnapshotSHA256 string `json:"catalog_snapshot_sha256"`
	MapperVersion         string `json:"mapper_version"`
	EffectiveFromNS       int64  `json:"effective_from_ns"`
	EffectiveUntilNS      *int64 `json:"effective_until_ns,omitempty"`
	SourceTimeResolution  string `json:"source_time_resolution"`
}

type prepareFixture struct {
	VenueFamily            string             `json:"venue_family"`
	SourceID               string             `json:"source_id"`
	Channel                string             `json:"channel"`
	NativeSymbol           string             `json:"native_symbol"`
	PayloadPath            string             `json:"payload_path"`
	PayloadBytes           int64              `json:"payload_bytes"`
	PayloadSHA256          string             `json:"payload_sha256"`
	HighCardinalitySymbols bool               `json:"high_cardinality_symbols"`
	LongBooks              bool               `json:"long_books"`
	SparseTickerUpdates    bool               `json:"sparse_ticker_updates"`
	Reconnect              bool               `json:"reconnect"`
	LongHistory            bool               `json:"long_history"`
	BinanceSpot            *prepareNormalizer `json:"binance_spot,omitempty"`
}

type prepareSpec struct {
	Format               string                     `json:"format"`
	FixtureRoot          string                     `json:"fixture_root"`
	MaxFileBytes         int64                      `json:"max_file_bytes"`
	MaxTotalBytes        int64                      `json:"max_total_bytes"`
	AdapterVersion       string                     `json:"adapter_version"`
	CoverageStartUTC     string                     `json:"coverage_start_utc"`
	CoverageEndUTC       string                     `json:"coverage_end_utc"`
	ProductionEquivalent bool                       `json:"production_equivalent"`
	Rates                prepareRates               `json:"rates"`
	Contracts            []quality.ContractIdentity `json:"contracts"`
	PayloadBytes         payloadTargets             `json:"payload_bytes"`
	RecordsPerVariant    int                        `json:"records_per_variant"`
	FrameBytes           int                        `json:"frame_bytes"`
	Replay               replayLimits               `json:"replay"`
	Memory               memoryLimits               `json:"memory"`
	Durations            durationPolicy             `json:"durations"`
	RSS                  rssPolicy                  `json:"rss"`
	Dataset              prepareDatasetConfig       `json:"dataset"`
	Fixtures             []prepareFixture           `json:"fixtures"`
}

type preparedFixture struct {
	spec     prepareFixture
	payload  []byte
	snapshot []byte
}

type prepareResult struct {
	ManifestPath   string
	ManifestSHA256 string
}

func newPrepareCommand() *cobra.Command {
	var specPath, outputDir, signingPrivateKeyPath string
	command := &cobra.Command{
		Use:   "prepare",
		Short: "Build a strict fixed-corpus manifest and canonical measurement artifacts",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			result, err := prepareReleaseGate(command.Context(), specPath, outputDir, signingPrivateKeyPath)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "%s  %s\n", result.ManifestSHA256, result.ManifestPath)
			return err
		},
	}
	command.Flags().StringVar(&specPath, "spec", "", "strict JSON preparation specification")
	command.Flags().StringVar(&outputDir, "output-dir", "", "new directory for the manifest and generated fixed corpus")
	command.Flags().StringVar(&signingPrivateKeyPath, "signing-private-key-file", "", "non-symlink file containing the caller-provisioned lowercase hex Ed25519 private key")
	_ = command.MarkFlagRequired("spec")
	_ = command.MarkFlagRequired("output-dir")
	_ = command.MarkFlagRequired("signing-private-key-file")
	return command
}

func prepareReleaseGate(ctx context.Context, specPath, outputDir, signingPrivateKeyPath string) (prepareResult, error) {
	signingPrivateKey, err := loadSigningPrivateKey(signingPrivateKeyPath)
	if err != nil {
		return prepareResult{}, err
	}
	specBytes, err := readTopLevelRegular(specPath, maxManifestBytes)
	if err != nil {
		return prepareResult{}, fmt.Errorf("read prepare spec: %w", err)
	}
	var spec prepareSpec
	if err := decodeStrict(specBytes, &spec); err != nil {
		return prepareResult{}, fmt.Errorf("decode prepare spec: %w", err)
	}
	coverageStart, coverageEnd, err := validatePrepareSpec(&spec)
	if err != nil {
		return prepareResult{}, err
	}
	fixtures, err := loadPrepareFixtures(spec)
	if err != nil {
		return prepareResult{}, err
	}
	contracts, err := deriveContracts(spec)
	if err != nil {
		return prepareResult{}, err
	}
	absoluteOutput, err := filepath.Abs(outputDir)
	if err != nil {
		return prepareResult{}, fmt.Errorf("resolve output directory: %w", err)
	}
	if filepath.Clean(absoluteOutput) != absoluteOutput {
		return prepareResult{}, errors.New("output directory must be a clean path")
	}
	if err := os.Mkdir(absoluteOutput, 0o700); err != nil {
		return prepareResult{}, fmt.Errorf("create output directory exclusively: %w", err)
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(absoluteOutput)
		}
	}()
	for _, directory := range []string{"objects", "snapshots", "dataset-work"} {
		if err := os.Mkdir(filepath.Join(absoluteOutput, directory), 0o700); err != nil {
			return prepareResult{}, fmt.Errorf("create generated directory %q: %w", directory, err)
		}
	}
	manifest, err := buildPreparedManifest(ctx, specBytes, spec, contracts, fixtures, coverageStart, coverageEnd, absoluteOutput, signingPrivateKey)
	if err != nil {
		return prepareResult{}, err
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return prepareResult{}, fmt.Errorf("marshal prepared manifest: %w", err)
	}
	manifestPath := filepath.Join(absoluteOutput, preparedManifestName)
	if err := writeExclusive(manifestPath, manifestBytes); err != nil {
		return prepareResult{}, fmt.Errorf("write prepared manifest: %w", err)
	}
	for _, directory := range []string{filepath.Join(absoluteOutput, "objects"), filepath.Join(absoluteOutput, "snapshots"), filepath.Join(absoluteOutput, "dataset-work"), absoluteOutput} {
		if err := syncDirectoryEntry(directory); err != nil {
			return prepareResult{}, err
		}
	}
	trustedSigner := signingPrivateKey.Public().(ed25519.PublicKey)
	if _, err := loadInputManifest(manifestPath, hex.EncodeToString(trustedSigner)); err != nil {
		return prepareResult{}, fmt.Errorf("prepared manifest is not consumable: %w", err)
	}
	digest := sha256.Sum256(manifestBytes)
	complete = true
	return prepareResult{ManifestPath: manifestPath, ManifestSHA256: hex.EncodeToString(digest[:])}, nil
}

func validatePrepareSpec(spec *prepareSpec) (int64, int64, error) {
	if spec.Format != prepareSpecFormat {
		return 0, 0, fmt.Errorf("unsupported prepare spec format %q", spec.Format)
	}
	if !filepath.IsAbs(spec.FixtureRoot) || filepath.Clean(spec.FixtureRoot) != spec.FixtureRoot {
		return 0, 0, errors.New("fixture_root must be a clean absolute path")
	}
	info, err := os.Lstat(spec.FixtureRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0, 0, errors.New("fixture_root must be an existing non-symlink directory")
	}
	if spec.MaxFileBytes <= 0 || spec.MaxFileBytes > maxInputFileBytes || spec.MaxTotalBytes <= 0 || spec.MaxTotalBytes > maxTotalInputBytes || spec.MaxFileBytes > spec.MaxTotalBytes {
		return 0, 0, errors.New("prepare byte bounds are invalid")
	}
	if spec.AdapterVersion == "" || len(spec.AdapterVersion) > quality.MaxReleaseGateTextBytes {
		return 0, 0, errors.New("adapter version is required and bounded")
	}
	start, err := parseUTC(spec.CoverageStartUTC)
	if err != nil {
		return 0, 0, fmt.Errorf("coverage start: %w", err)
	}
	end, err := parseUTC(spec.CoverageEndUTC)
	if err != nil {
		return 0, 0, fmt.Errorf("coverage end: %w", err)
	}
	if end <= start {
		return 0, 0, errors.New("coverage interval is empty or regressing")
	}
	for name, value := range map[string]uint64{
		"max observed message rate": spec.Rates.MaxObservedMessagesPerSecond,
		"max observed byte rate":    spec.Rates.MaxObservedBytesPerSecond,
		"acquisition record rate":   spec.Rates.AcquisitionRecordsPerSecond,
		"acquisition byte rate":     spec.Rates.AcquisitionBytesPerSecond,
	} {
		if value == 0 || value > quality.MaxReleaseGateRate {
			return 0, 0, fmt.Errorf("%s is zero or unbounded", name)
		}
	}
	if len(spec.Contracts) == 0 || len(spec.Contracts) > quality.MaxReleaseGateContracts {
		return 0, 0, errors.New("contract count is zero or unbounded")
	}
	if spec.PayloadBytes.Tiny <= 0 || spec.PayloadBytes.Tiny >= spec.PayloadBytes.Median || spec.PayloadBytes.Median >= spec.PayloadBytes.Max || spec.PayloadBytes.Max > capture.MaxPayloadBytes {
		return 0, 0, errors.New("tiny, median, and max payload targets are invalid")
	}
	if spec.RecordsPerVariant < 1 || spec.RecordsPerVariant > maximumPrepareRecords || spec.FrameBytes < 1 || uint64(spec.FrameBytes) > spec.Memory.FrameBoundBytes {
		return 0, 0, errors.New("record count or frame bound is invalid")
	}
	if len(spec.Fixtures) != 5 || len(spec.Fixtures) > maximumPrepareFixtures {
		return 0, 0, errors.New("exactly five venue-family fixtures are required")
	}
	if err := validatePrepareFixtureDeclarations(spec.Fixtures); err != nil {
		return 0, 0, err
	}
	if spec.Replay.Concurrency < 1 || spec.Replay.Concurrency > replay.MaximumWorkers ||
		spec.Replay.Prefetch < 1 || spec.Replay.Prefetch > replay.MaximumPrefetch ||
		spec.Replay.MaxSources < 1 || spec.Replay.MaxSegments < 1 || spec.Replay.MaxSegmentBytes < 1 ||
		spec.Replay.MaxRecordsPerSegment < 1 || spec.Replay.MaxFramesPerSegment < 1 {
		return 0, 0, errors.New("explicit replay bounds are invalid")
	}
	if (spec.Dataset.RowGroupTargetBytes != 64<<20 && spec.Dataset.RowGroupTargetBytes != 256<<20 && spec.Dataset.RowGroupTargetBytes != 1024<<20) ||
		spec.Dataset.RowGroupTargetBytes > int64(spec.Memory.RowGroupBoundBytes) ||
		spec.Dataset.PageBufferBytes < 64<<10 || spec.Dataset.PageBufferBytes > 4<<20 ||
		spec.Dataset.MaxInputRows == 0 || spec.Dataset.MaxInputRows > dataset.MaxPartitionInputRows ||
		spec.Dataset.MaxParquetRows == 0 || spec.Dataset.MaxParquetRows > dataset.MaxPartitionParquetRows {
		return 0, 0, errors.New("explicit dataset bounds are invalid")
	}
	return start, end, nil
}

func validatePrepareFixtureDeclarations(fixtures []prepareFixture) error {
	families := make(map[string]struct{}, len(fixtures))
	var highCardinality, longBooks, sparse, reconnects, histories bool
	for _, fixture := range fixtures {
		if fixture.VenueFamily == "" {
			return errors.New("fixture venue family is required")
		}
		if _, duplicate := families[fixture.VenueFamily]; duplicate {
			return fmt.Errorf("duplicate venue family %q", fixture.VenueFamily)
		}
		families[fixture.VenueFamily] = struct{}{}
		highCardinality = highCardinality || fixture.HighCardinalitySymbols
		longBooks = longBooks || fixture.LongBooks
		sparse = sparse || fixture.SparseTickerUpdates
		reconnects = reconnects || fixture.Reconnect
		histories = histories || fixture.LongHistory
	}
	if !highCardinality || !longBooks || !sparse || !reconnects || !histories {
		return errors.New("fixtures do not cover all mandatory workload shapes")
	}
	return nil
}

func parseUTC(value string) (int64, error) {
	if !strings.HasSuffix(value, "Z") {
		return 0, errors.New("timestamp must use the UTC Z designator")
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return 0, err
	}
	if parsed.Location() != time.UTC || parsed.UnixNano() < 0 {
		return 0, errors.New("timestamp is not a non-negative UTC instant")
	}
	return parsed.UnixNano(), nil
}

func loadPrepareFixtures(spec prepareSpec) ([]preparedFixture, error) {
	loader := boundedLoader{base: spec.FixtureRoot, fileLimit: spec.MaxFileBytes, totalLimit: spec.MaxTotalBytes, frameLimit: spec.Memory.FrameBoundBytes, cache: make(map[string]loadedFile)}
	fixtures := slices.Clone(spec.Fixtures)
	slices.SortFunc(fixtures, func(left, right prepareFixture) int { return strings.Compare(left.VenueFamily, right.VenueFamily) })
	seenFamilies := make(map[string]struct{}, len(fixtures))
	result := make([]preparedFixture, 0, len(fixtures))
	for _, fixture := range fixtures {
		if fixture.VenueFamily == "" || fixture.Channel == "" || fixture.NativeSymbol == "" || fixture.PayloadPath == "" {
			return nil, errors.New("fixture identity, symbol, and payload path are required")
		}
		if _, duplicate := seenFamilies[fixture.VenueFamily]; duplicate {
			return nil, fmt.Errorf("duplicate venue family %q", fixture.VenueFamily)
		}
		seenFamilies[fixture.VenueFamily] = struct{}{}
		if err := validatePrepareSourceID(fixture.SourceID); err != nil {
			return nil, fmt.Errorf("fixture %q source ID: %w", fixture.VenueFamily, err)
		}
		payload, err := loader.read(fileRef{
			Path: fixture.PayloadPath, Bytes: fixture.PayloadBytes, SHA256: fixture.PayloadSHA256,
		})
		if err != nil {
			return nil, fmt.Errorf("fixture %q payload: %w", fixture.VenueFamily, err)
		}
		if err := validateJSONPayload(payload); err != nil {
			return nil, fmt.Errorf("fixture %q payload: %w", fixture.VenueFamily, err)
		}
		prepared := preparedFixture{spec: fixture, payload: payload}
		if fixture.BinanceSpot != nil {
			if fixture.SourceID != binance.SpotSourceID || fixture.Channel != binance.SpotRawChannel {
				return nil, fmt.Errorf("fixture %q Binance Spot source or channel does not match the real mapper", fixture.VenueFamily)
			}
			snapshot, err := loader.read(fileRef{
				Path:   fixture.BinanceSpot.CatalogSnapshotPath,
				Bytes:  fixture.BinanceSpot.CatalogSnapshotBytes,
				SHA256: fixture.BinanceSpot.CatalogSnapshotSHA256,
			})
			if err != nil {
				return nil, fmt.Errorf("fixture %q catalog snapshot: %w", fixture.VenueFamily, err)
			}
			if err := validateCatalogSnapshot(snapshot); err != nil {
				return nil, err
			}
			snapshotDigest := sha256.Sum256(snapshot)
			until := normalize.OptionalInt64{}
			if fixture.BinanceSpot.EffectiveUntilNS != nil {
				until = normalize.OptionalInt64{Valid: true, Value: *fixture.BinanceSpot.EffectiveUntilNS}
			}
			if _, err := binance.NewSpotMapperBinding(
				normalize.Hash(snapshotDigest),
				fixture.BinanceSpot.MapperVersion,
				fixture.BinanceSpot.EffectiveFromNS,
				until,
				normalize.TimeResolution(fixture.BinanceSpot.SourceTimeResolution),
				nil,
			); err != nil {
				return nil, fmt.Errorf("fixture %q Binance Spot mapper declaration: %w", fixture.VenueFamily, err)
			}
			prepared.snapshot = snapshot
		}
		result = append(result, prepared)
	}
	return result, nil
}
func validatePrepareSourceID(value string) error {
	if value == "" {
		return errors.New("must not be empty")
	}
	if !utf8.ValidString(value) {
		return errors.New("must be valid UTF-8")
	}
	if strings.TrimSpace(value) != value {
		return errors.New("must not have surrounding whitespace")
	}
	if len(value) > capture.MaxSourceIDBytes {
		return fmt.Errorf("has %d bytes, maximum is %d", len(value), capture.MaxSourceIDBytes)
	}
	return nil
}

func validateJSONPayload(payload []byte) error {
	if len(payload) == 0 || !json.Valid(payload) {
		return errors.New("payload is not one complete JSON value")
	}
	var value any
	if err := decodeStrict(payload, &value); err != nil {
		return err
	}
	return nil
}

func validateCatalogSnapshot(data []byte) error {
	var document struct {
		Version     uint16            `json:"version"`
		Instruments []json.RawMessage `json:"instruments"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("decode catalog snapshot: %w", err)
	}
	digest := sha256.Sum256(data)
	_, err := normalize.NewCatalogView(catalog.Snapshot{Version: document.Version, SHA256: digest, Bytes: slices.Clone(data), InstrumentCount: len(document.Instruments)})
	if err != nil {
		return fmt.Errorf("validate catalog snapshot: %w", err)
	}
	return nil
}

func deriveContracts(spec prepareSpec) ([]quality.ContractIdentity, error) {
	coverageStart, err := parseUTC(spec.CoverageStartUTC)
	if err != nil {
		return nil, err
	}
	coverageEnd, err := parseUTC(spec.CoverageEndUTC)
	if err != nil {
		return nil, err
	}
	contracts := slices.Clone(spec.Contracts)
	seenTuples := make(map[string]struct{}, len(contracts))
	seenIDs := make(map[string]struct{}, len(contracts))
	fixtureFamilies := make(map[string]prepareFixture, len(spec.Fixtures))
	for _, fixture := range spec.Fixtures {
		fixtureFamilies[fixture.VenueFamily] = fixture
	}
	for index := range contracts {
		contract := &contracts[index]
		if contract.ContractSHA256 != "" || contract.AdapterSHA256 != "" {
			return nil, fmt.Errorf("contract %q hash fields must be empty before derivation", contract.ContractID)
		}
		for name, value := range map[string]string{
			"contract_id": contract.ContractID, "source_id": contract.SourceID, "api_version": contract.APIVersion,
			"entitlement": contract.Entitlement, "channel_or_endpoint": contract.ChannelOrEndpoint,
			"data_family": contract.DataFamily, "native_granularity": contract.NativeGranularity,
			"venue_family": contract.VenueFamily, "adapter_version": contract.AdapterVersion,
		} {
			if value == "" || len(value) > quality.MaxReleaseGateTextBytes {
				return nil, fmt.Errorf("contract %q field %s is empty or unbounded", contract.ContractID, name)
			}
		}
		if contract.CoverageStartNS != coverageStart || contract.CoverageEndNS != coverageEnd {
			return nil, fmt.Errorf("contract %q coverage differs from the declared UTC interval", contract.ContractID)
		}
		if contract.CanaryRequirement != quality.CanaryRequired && contract.CanaryRequirement != quality.CanaryNotRequired {
			return nil, fmt.Errorf("contract %q has an unknown canary requirement", contract.ContractID)
		}
		if contract.AdapterVersion != spec.AdapterVersion {
			return nil, fmt.Errorf("contract %q adapter version differs from the declared adapter version", contract.ContractID)
		}
		fixture, ok := fixtureFamilies[contract.VenueFamily]
		if !ok || fixture.SourceID != contract.SourceID || fixture.Channel != contract.ChannelOrEndpoint {
			return nil, fmt.Errorf("contract %q does not bind its exact venue fixture", contract.ContractID)
		}
		if _, duplicate := seenIDs[contract.ContractID]; duplicate {
			return nil, fmt.Errorf("duplicate contract ID %q", contract.ContractID)
		}
		seenIDs[contract.ContractID] = struct{}{}
		tupleBytes, err := json.Marshal(struct {
			SourceID          string
			APIVersion        string
			Entitlement       string
			Channel           string
			DataFamily        string
			NativeGranularity string
			CoverageStartNS   int64
			CoverageEndNS     int64
			AdapterVersion    string
		}{contract.SourceID, contract.APIVersion, contract.Entitlement, contract.ChannelOrEndpoint, contract.DataFamily, contract.NativeGranularity, contract.CoverageStartNS, contract.CoverageEndNS, contract.AdapterVersion})
		if err != nil {
			return nil, err
		}
		tupleKey := string(tupleBytes)
		if _, duplicate := seenTuples[tupleKey]; duplicate {
			return nil, fmt.Errorf("duplicate contract tuple %q", contract.ContractID)
		}
		seenTuples[tupleKey] = struct{}{}
		contractBody, err := json.Marshal(*contract)
		if err != nil {
			return nil, err
		}
		contractHash := sha256.Sum256(contractBody)
		contract.ContractSHA256 = hex.EncodeToString(contractHash[:])
		adapterBody, err := json.Marshal(struct {
			AdapterVersion    string `json:"adapter_version"`
			ContractID        string `json:"contract_id"`
			SourceID          string `json:"source_id"`
			Channel           string `json:"channel"`
			DataFamily        string `json:"data_family"`
			NativeGranularity string `json:"native_granularity"`
		}{spec.AdapterVersion, contract.ContractID, contract.SourceID, contract.ChannelOrEndpoint, contract.DataFamily, contract.NativeGranularity})
		if err != nil {
			return nil, err
		}
		adapterHash := sha256.Sum256(adapterBody)
		contract.AdapterSHA256 = hex.EncodeToString(adapterHash[:])
	}
	for family := range fixtureFamilies {
		if !slices.ContainsFunc(contracts, func(contract quality.ContractIdentity) bool { return contract.VenueFamily == family }) {
			return nil, fmt.Errorf("venue family %q has no declared contract", family)
		}
	}
	return contracts, nil
}

func buildPreparedManifest(ctx context.Context, specBytes []byte, spec prepareSpec, contracts []quality.ContractIdentity, fixtures []preparedFixture, coverageStart, coverageEnd int64, outputRoot string, signingPrivateKey ed25519.PrivateKey) (inputManifest, error) {
	if err := ctx.Err(); err != nil {
		return inputManifest{}, err
	}
	if len(signingPrivateKey) != ed25519.PrivateKeySize {
		return inputManifest{}, errors.New("signing private key is required")
	}
	specDigest := sha256.Sum256(specBytes)
	hardware, err := actualHardwareIdentity(spec.ProductionEquivalent)
	if err != nil {
		return inputManifest{}, err
	}
	workload := quality.WorkloadIdentity{
		ID:                           "prepared-workload-" + hex.EncodeToString(specDigest[:8]),
		MaxObservedMessagesPerSecond: spec.Rates.MaxObservedMessagesPerSecond,
		MaxObservedBytesPerSecond:    spec.Rates.MaxObservedBytesPerSecond,
		AcquisitionRecordsPerSecond:  spec.Rates.AcquisitionRecordsPerSecond,
		AcquisitionBytesPerSecond:    spec.Rates.AcquisitionBytesPerSecond,
		Contracts:                    contracts,
	}
	families := make([]string, 0, len(fixtures))
	for _, fixture := range fixtures {
		families = append(families, fixture.spec.VenueFamily)
	}
	slices.Sort(families)
	corpus := quality.FixedCorpusIdentity{
		ID: "prepared-corpus-" + hex.EncodeToString(specDigest[8:16]), VenueFamilies: families,
		PayloadClasses: []quality.PayloadClass{quality.PayloadTiny, quality.PayloadMedian, quality.PayloadMax},
	}
	objects := make([]corpusObject, 0, len(fixtures)*3)
	totalRecords := len(fixtures) * 3 * spec.RecordsPerVariant
	globalRecord := 0
	for fixtureIndex, fixture := range fixtures {
		for classIndex, class := range []quality.PayloadClass{quality.PayloadTiny, quality.PayloadMedian, quality.PayloadMax} {
			if err := ctx.Err(); err != nil {
				return inputManifest{}, err
			}
			target := []int{spec.PayloadBytes.Tiny, spec.PayloadBytes.Median, spec.PayloadBytes.Max}[classIndex]
			payload, err := paddedJSON(fixture.payload, target)
			if err != nil {
				return inputManifest{}, fmt.Errorf("fixture %q %s: %w", fixture.spec.VenueFamily, class, err)
			}
			object, err := writePreparedObject(spec, fixture, fixtureIndex, classIndex, class, payload, globalRecord, totalRecords, coverageStart, coverageEnd, outputRoot, specDigest)
			if err != nil {
				return inputManifest{}, err
			}
			globalRecord += spec.RecordsPerVariant
			objects = append(objects, object)
		}
		corpus.HighCardinalitySymbols = corpus.HighCardinalitySymbols || fixture.spec.HighCardinalitySymbols
		corpus.LongBooks = corpus.LongBooks || fixture.spec.LongBooks
		corpus.SparseTickerUpdates = corpus.SparseTickerUpdates || fixture.spec.SparseTickerUpdates
		corpus.Reconnects = corpus.Reconnects || fixture.spec.Reconnect
		corpus.LongHistories = corpus.LongHistories || fixture.spec.LongHistory
	}
	if !corpus.HighCardinalitySymbols || !corpus.LongBooks || !corpus.SparseTickerUpdates || !corpus.Reconnects || !corpus.LongHistories {
		return inputManifest{}, errors.New("fixtures do not cover all mandatory workload shapes")
	}
	signedHardwareValue, err := signHardware(hardware, signingPrivateKey)
	if err != nil {
		return inputManifest{}, err
	}
	signedWorkloadValue, err := signWorkload(workload, signingPrivateKey)
	if err != nil {
		return inputManifest{}, err
	}
	signedCorpusValue, err := signCorpus(corpus, signingPrivateKey)
	if err != nil {
		return inputManifest{}, err
	}
	return inputManifest{
		Format: manifestFormat, MaxFileBytes: spec.MaxFileBytes, MaxTotalBytes: spec.MaxTotalBytes,
		Hardware: signedHardwareValue, Workload: signedWorkloadValue, FixedCorpus: signedCorpusValue,
		Replay: spec.Replay, Memory: spec.Memory, Durations: spec.Durations, RSS: spec.RSS, Objects: objects,
	}, nil
}

func writePreparedObject(spec prepareSpec, fixture preparedFixture, fixtureIndex, classIndex int, class quality.PayloadClass, payload []byte, globalRecord, totalRecords int, coverageStart, coverageEnd int64, outputRoot string, specDigest [32]byte) (corpusObject, error) {
	objectNumber := fixtureIndex*3 + classIndex
	objectID := fmt.Sprintf("fixture-%02d-%s", fixtureIndex, class)
	epochID, epochBytes := deterministicUUID(fmt.Sprintf("epoch:%x:%d", specDigest, objectNumber))
	segmentID, _ := deterministicUUID(fmt.Sprintf("segment:%x:%d", specDigest, objectNumber))
	records := make([]segment.Envelope, 0, spec.RecordsPerVariant)
	for ordinal := range spec.RecordsPerVariant {
		position := globalRecord + ordinal + 1
		offset := mulDiv(uint64(coverageEnd-coverageStart), uint64(position), uint64(totalRecords+1))
		received := coverageStart + int64(offset)
		symbol := fixture.spec.NativeSymbol
		if fixture.spec.HighCardinalitySymbols && fixture.spec.BinanceSpot == nil {
			symbol = fmt.Sprintf("%s-%06d", symbol, ordinal)
		}
		envelope := capture.EnvelopeV1{
			EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
			SourceID: fixture.spec.SourceID, ChannelOrEndpoint: fixture.spec.Channel,
			NativeSymbol:    capture.OptionalString{Value: symbol, Valid: true},
			ConnectionEpoch: capture.OptionalEpoch{Value: epochBytes, Valid: true},
			ArrivalOrdinal:  uint64(ordinal + 1), MessageOrdinal: uint32(ordinal),
			ExchangeTimeResolution: capture.ExchangeTimeAbsent, ReceivedWallTimeNS: received,
			ClockEpochID: epochID, MonotonicNSSinceClockEpoch: uint64(ordinal),
			PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved,
			RecorderVersion: spec.AdapterVersion,
		}
		envelope.SetRawPayload(payload)
		framed, err := envelope.ToSegment()
		if err != nil {
			return corpusObject{}, fmt.Errorf("build %s envelope %d: %w", objectID, ordinal, err)
		}
		records = append(records, framed)
	}
	encoded, err := segment.Encode(records, segment.EncodeOptions{FrameBytes: spec.FrameBytes, Concurrency: spec.Replay.Concurrency})
	if err != nil {
		return corpusObject{}, fmt.Errorf("encode %s: %w", objectID, err)
	}
	segmentRelative := filepath.ToSlash(filepath.Join("objects", objectID+".segment"))
	manifestRelative := filepath.ToSlash(filepath.Join("objects", objectID+".manifest.json"))
	objectKey := "prepared/" + objectID
	ready := segment.ReadyManifest{
		ManifestVersion: segment.SpoolManifestVersion, SourceID: fixture.spec.SourceID, ChannelID: fixture.spec.Channel,
		EpochKind: segment.EpochConnection, EpochID: epochID, WriterVersion: spec.AdapterVersion,
		RotationReason: segment.RotationEpochEnd, SegmentFile: filepath.Base(segmentRelative), ObjectKey: objectKey,
		Segment: encoded.Manifest,
	}
	readyBytes, err := json.Marshal(ready)
	if err != nil {
		return corpusObject{}, err
	}
	if err := writeExclusive(filepath.Join(outputRoot, filepath.FromSlash(segmentRelative)), encoded.Bytes); err != nil {
		return corpusObject{}, err
	}
	if err := writeExclusive(filepath.Join(outputRoot, filepath.FromSlash(manifestRelative)), readyBytes); err != nil {
		return corpusObject{}, err
	}
	segmentHash := sha256.Sum256(encoded.Bytes)
	manifestHash := sha256.Sum256(readyBytes)
	first, last := records[0], records[len(records)-1]
	object := corpusObject{
		ID: objectID, VenueFamily: fixture.spec.VenueFamily, PayloadClass: class,
		HighCardinalitySymbols: fixture.spec.HighCardinalitySymbols, LongBooks: fixture.spec.LongBooks,
		SparseTickerUpdates: fixture.spec.SparseTickerUpdates, Reconnect: fixture.spec.Reconnect, LongHistory: fixture.spec.LongHistory,
		Segment:       fileRef{Path: segmentRelative, Bytes: int64(len(encoded.Bytes)), SHA256: hex.EncodeToString(segmentHash[:])},
		ReadyManifest: fileRef{Path: manifestRelative, Bytes: int64(len(readyBytes)), SHA256: hex.EncodeToString(manifestHash[:])},
		Publication: publicationSpec{
			SegmentID: segmentID, SourceID: fixture.spec.SourceID, ChannelID: fixture.spec.Channel, EpochID: epochID,
			ReceivedStartNS: first.ReceivedWallTimeNS, ReceivedEndNS: last.ReceivedWallTimeNS,
			OrdinalStart: first.ArrivalOrdinal, OrdinalEnd: last.ArrivalOrdinal, ObjectKey: objectKey, ManifestVersion: segment.SpoolManifestVersion,
		},
	}
	if fixture.spec.BinanceSpot != nil {
		snapshotRelative := filepath.ToSlash(filepath.Join("snapshots", fmt.Sprintf("fixture-%02d.json", fixtureIndex)))
		if classIndex == 0 {
			if err := writeExclusive(filepath.Join(outputRoot, filepath.FromSlash(snapshotRelative)), fixture.snapshot); err != nil {
				return corpusObject{}, err
			}
		}
		snapshotHash := sha256.Sum256(fixture.snapshot)
		object.Normalizer = &normalizerSpec{
			Kind: "binance_spot", CatalogSnapshot: fileRef{Path: snapshotRelative, Bytes: int64(len(fixture.snapshot)), SHA256: hex.EncodeToString(snapshotHash[:])},
			MapperVersion: fixture.spec.BinanceSpot.MapperVersion, EffectiveFromNS: fixture.spec.BinanceSpot.EffectiveFromNS,
			EffectiveUntilNS: fixture.spec.BinanceSpot.EffectiveUntilNS, SourceTimeResolution: fixture.spec.BinanceSpot.SourceTimeResolution,
		}
		datasetPolicy, _ := canonicalDigest(spec.Dataset)
		replayConfig, _ := canonicalDigest(spec.Replay)
		inputSet := sha256.Sum256(append(append(slices.Clone(specDigest[:]), segmentHash[:]...), snapshotHash[:]...))
		object.Dataset = &datasetSpec{
			Root: filepath.Join(outputRoot, "dataset-work"), RowGroupTargetBytes: spec.Dataset.RowGroupTargetBytes,
			PageBufferBytes: spec.Dataset.PageBufferBytes, Dictionary: spec.Dataset.Dictionary, BloomFilter: spec.Dataset.BloomFilter,
			MaxInputRows: spec.Dataset.MaxInputRows, MaxParquetRows: spec.Dataset.MaxParquetRows,
			DatasetPolicyID: datasetPolicy, ReplayConfigID: replayConfig, InputManifestSetID: hex.EncodeToString(inputSet[:]),
		}
	}
	return object, nil
}

func paddedJSON(payload []byte, target int) ([]byte, error) {
	trimmed := slices.Clone(payload)
	if len(trimmed) > target {
		return nil, errors.New("payload exceeds declared byte target")
	}
	result := make([]byte, target)
	copy(result, trimmed)
	for index := len(trimmed); index < target; index++ {
		result[index] = ' '
	}
	if !json.Valid(result) {
		return nil, errors.New("whitespace-padded payload is not valid JSON")
	}
	return result, nil
}

func actualHardwareIdentity(productionEquivalent bool) (quality.HardwareIdentity, error) {
	host, err := sysinfo.Host()
	if err != nil {
		return quality.HardwareIdentity{}, fmt.Errorf("inspect host: %w", err)
	}
	memory, err := host.Memory()
	if err != nil {
		return quality.HardwareIdentity{}, fmt.Errorf("inspect host memory: %w", err)
	}
	info := host.Info()
	model := info.NativeArchitecture
	if model == "" {
		model = info.Architecture
	}
	if model == "" {
		model = runtime.GOARCH
	}
	body := struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
		CPUModel     string `json:"cpu_model"`
		LogicalCPUs  int    `json:"logical_cpus"`
		MemoryBytes  uint64 `json:"memory_bytes"`
	}{runtime.GOOS, runtime.GOARCH, model, runtime.NumCPU(), memory.Total}
	digest, err := canonicalDigest(body)
	if err != nil {
		return quality.HardwareIdentity{}, err
	}
	return quality.HardwareIdentity{
		ID: "prepared-hardware-" + digest[:16], OS: runtime.GOOS, Architecture: runtime.GOARCH, CPUModel: model,
		LogicalCPUs: uint32(runtime.NumCPU()), MemoryBytes: memory.Total, ProductionEquivalent: productionEquivalent,
	}, nil
}

func signHardware(value quality.HardwareIdentity, private ed25519.PrivateKey) (signedHardware, error) {
	body, hash, signature, err := signDeclaration(value, private)
	if err != nil {
		return signedHardware{}, err
	}
	body.ManifestSHA256 = hash
	return signedHardware{Value: body, Signature: signature}, nil
}

func signWorkload(value quality.WorkloadIdentity, private ed25519.PrivateKey) (signedWorkload, error) {
	body, hash, signature, err := signDeclaration(value, private)
	if err != nil {
		return signedWorkload{}, err
	}
	body.ManifestSHA256 = hash
	return signedWorkload{Value: body, Signature: signature}, nil
}

func signCorpus(value quality.FixedCorpusIdentity, private ed25519.PrivateKey) (signedCorpus, error) {
	body, hash, signature, err := signDeclaration(value, private)
	if err != nil {
		return signedCorpus{}, err
	}
	body.ManifestSHA256 = hash
	return signedCorpus{Value: body, Signature: signature}, nil
}

func signDeclaration[T any](value T, private ed25519.PrivateKey) (T, string, string, error) {
	if len(private) != ed25519.PrivateKeySize {
		return value, "", "", errors.New("signing private key is required")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return value, "", "", err
	}
	digest := sha256.Sum256(body)
	signature := ed25519.Sign(private, body)
	return value, hex.EncodeToString(digest[:]), hex.EncodeToString(signature), nil
}

func loadSigningPrivateKey(path string) (ed25519.PrivateKey, error) {
	data, err := readTopLevelRegular(path, ed25519.PrivateKeySize*2)
	if err != nil {
		return nil, fmt.Errorf("read signing private key file: %w", err)
	}
	return decodeSigningPrivateKey(string(data))
}

func decodeSigningPrivateKey(text string) (ed25519.PrivateKey, error) {
	decoded, err := decodeExactHex(text, ed25519.PrivateKeySize)
	if err != nil {
		return nil, fmt.Errorf("signing private key: %w", err)
	}
	private := ed25519.PrivateKey(decoded)
	if canonical := ed25519.NewKeyFromSeed(private.Seed()); !bytes.Equal(private, canonical) {
		return nil, errors.New("signing private key is not a canonical Ed25519 private key")
	}
	return private, nil
}

func deterministicUUID(seed string) (string, [16]byte) {
	digest := sha256.Sum256([]byte(seed))
	var value [16]byte
	copy(value[:], digest[:16])
	value[6] = (value[6] & 0x0f) | 0x50
	value[8] = (value[8] & 0x3f) | 0x80
	return formatUUID(value), value
}

func formatUUID(value [16]byte) string {
	text := hex.EncodeToString(value[:])
	return text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32]
}

func mulDiv(left, right, divisor uint64) uint64 {
	high, low := bits.Mul64(left, right)
	quotient, _ := bits.Div64(high, low, divisor)
	return quotient
}

func syncDirectoryEntry(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync directory %q: %w", filepath.Base(path), err)
	}
	return nil
}
