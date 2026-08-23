package dataset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/fileflow"
	"github.com/spf13/pathologize"
)

const (
	OpportunityArchiveManifestVersion uint16 = 1
	OpportunityArchiveSchemaVersion   uint16 = 1
	opportunityArchiveSchemaName             = "enable.opportunity_archive.parquet.v1"
)

type opportunityParquetRow struct {
	RowOrdinal       uint64   `parquet:"row_ordinal,uint(64)"`
	RowLogicalHash   [32]byte `parquet:"row_logical_hash"`
	OpportunityID    string   `parquet:"opportunity_id"`
	LedgerPartition  string   `parquet:"ledger_partition"`
	SourceID         string   `parquet:"source_id"`
	ChannelID        string   `parquet:"channel_id"`
	InstrumentUID    string   `parquet:"instrument_uid"`
	HasInstrumentUID bool     `parquet:"has_instrument_uid"`
	Expectation      string   `parquet:"expectation,enum"`
	ExpectedTimeNS   int64    `parquet:"expected_time_ns,timestamp(nanosecond:utc)"`
	WindowStartNS    int64    `parquet:"window_start_ns,timestamp(nanosecond:utc)"`
	WindowEndNS      int64    `parquet:"window_end_ns,timestamp(nanosecond:utc)"`
	ScopeJSON        []byte   `parquet:"scope_json"`
	TerminalTimeNS   int64    `parquet:"terminal_time_ns,timestamp(nanosecond:utc)"`
	TerminalOutcome  string   `parquet:"terminal_outcome,enum"`
	TerminalEvidence []byte   `parquet:"terminal_evidence_json"`
	CreatedTimeNS    int64    `parquet:"created_time_ns,timestamp(nanosecond:utc)"`
}

type OpportunityArchiveManifest struct {
	ManifestVersion              uint16          `json:"manifest_version"`
	DatasetVersion               string          `json:"dataset_version"`
	ParquetWriterVersion         string          `json:"parquet_writer_version"`
	ParquetFormatCompatibility   string          `json:"parquet_format_compatibility"`
	SchemaName                   string          `json:"schema_name"`
	SchemaVersion                uint16          `json:"schema_version"`
	SchemaSHA256                 string          `json:"schema_sha256"`
	Family                       Family          `json:"family"`
	GenerationID                 string          `json:"generation_id"`
	GenerationFingerprint        string          `json:"generation_fingerprint"`
	CatalogSnapshotSHA256        string          `json:"catalog_snapshot_sha256"`
	MapperSetSHA256              string          `json:"mapper_set_sha256"`
	LedgerPartition              string          `json:"ledger_partition"`
	BoundaryFromTimeNS           int64           `json:"boundary_from_time_ns"`
	BoundaryFromOpportunityID    string          `json:"boundary_from_opportunity_id"`
	BoundaryThroughTimeNS        int64           `json:"boundary_through_time_ns"`
	BoundaryThroughOpportunityID string          `json:"boundary_through_opportunity_id"`
	RowCount                     uint64          `json:"row_count"`
	LogicalSHA256                string          `json:"logical_sha256"`
	PhysicalSHA256               string          `json:"physical_sha256"`
	FileBytes                    int64           `json:"file_bytes"`
	RowGroups                    uint64          `json:"row_groups"`
	Pages                        uint64          `json:"pages"`
	Values                       uint64          `json:"values"`
	ParquetFile                  string          `json:"parquet_file"`
	DatasetPartitionID           string          `json:"dataset_partition_id"`
	DatasetPolicyID              string          `json:"dataset_policy_id"`
	ReplayConfigID               string          `json:"replay_config_id"`
	InputManifestSetID           string          `json:"input_manifest_set_id"`
	Options                      ManifestOptions `json:"options"`
}

type OpportunityArchiveResult struct {
	Manifest     OpportunityArchiveManifest
	ManifestPath string
	ParquetPath  string
	ManifestHash [sha256.Size]byte
	Rows         []quality.Opportunity
}

type OpportunityArchiveStore struct {
	root    string
	options WriterOptions
}

func NewOpportunityArchiveStore(root string, options WriterOptions) (*OpportunityArchiveStore, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: opportunity archive root", ErrInvalidInput)
	}
	if err := options.validate(); err != nil {
		return nil, err
	}
	return &OpportunityArchiveStore{root: root, options: options}, nil
}

func (s *OpportunityArchiveStore) LookupOpportunityArchive(ctx context.Context, generation quality.SpillGeneration) (quality.ArchiveCommit, bool, error) {
	manifestPath, err := opportunityManifestPath(s.root, generation)
	if err != nil {
		return quality.ArchiveCommit{}, false, err
	}
	if _, err := os.Stat(manifestPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return quality.ArchiveCommit{}, false, nil
		}
		return quality.ArchiveCommit{}, false, err
	}
	result, err := VerifyOpportunityArchive(ctx, s.root, manifestPath, generation)
	if err != nil {
		return quality.ArchiveCommit{}, false, err
	}
	return opportunityArchiveCommit(result), true, nil
}

func (s *OpportunityArchiveStore) WriteOpportunityArchive(ctx context.Context, generation quality.SpillGeneration) (quality.ArchiveCommit, error) {
	result, err := BuildOpportunityArchive(ctx, s.root, generation, s.options)
	if err != nil {
		return quality.ArchiveCommit{}, err
	}
	return opportunityArchiveCommit(result), nil
}

func BuildOpportunityArchive(ctx context.Context, root string, generation quality.SpillGeneration, options WriterOptions) (result OpportunityArchiveResult, err error) {
	if err := options.validate(); err != nil {
		return result, err
	}
	if err := generation.Validate(); err != nil {
		return result, err
	}
	if generation.State != quality.SpillPending || len(generation.Rows) == 0 {
		return result, fmt.Errorf("%w: archive requires a pending generation with rows", ErrInvalidInput)
	}
	staging, err := newStaging(root)
	if err != nil {
		return result, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(staging)
		}
	}()
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return result, fmt.Errorf("dataset: open opportunity staging parquet: %w", err)
	}
	schema := parquet.SchemaOf(new(opportunityParquetRow))
	writer := &genericPartitionWriter[opportunityParquetRow]{parquet.NewGenericWriter[opportunityParquetRow](
		file, parquetWriterOptions(schema, FamilyOpportunity, options, filepath.Dir(staging))...)}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
			_ = file.Close()
		}
	}()
	schemaHash := schemaDigest(opportunityArchiveSchemaName, OpportunityArchiveSchemaVersion, schema)
	setStaticMetadata(writer, opportunityArchiveSchemaName, OpportunityArchiveSchemaVersion, schemaHash, options)
	writer.SetKeyValueMetadata("enable.family", string(FamilyOpportunity))
	writer.SetKeyValueMetadata("enable.generation_id", generation.GenerationID)
	writer.SetKeyValueMetadata("enable.generation_fingerprint", hex.EncodeToString(generation.Fingerprint[:]))
	writer.SetKeyValueMetadata("enable.ledger_partition", generation.Partition)
	writer.SetKeyValueMetadata("enable.catalog_snapshot_sha256", hex.EncodeToString(generation.CatalogSnapshotHash[:]))
	writer.SetKeyValueMetadata("enable.mapper_set_sha256", hex.EncodeToString(generation.MapperSetHash[:]))

	lastFlushSize := int64(0)
	for ordinal, opportunity := range generation.Rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		digest, err := opportunity.LogicalHash()
		if err != nil {
			return result, err
		}
		row := opportunityParquetRow{
			RowOrdinal: uint64(ordinal), RowLogicalHash: digest, OpportunityID: opportunity.OpportunityID,
			LedgerPartition: opportunity.LedgerPartition, SourceID: opportunity.SourceID, ChannelID: opportunity.ChannelID,
			InstrumentUID: opportunity.InstrumentUID, HasInstrumentUID: opportunity.InstrumentUID != "",
			Expectation: opportunity.Expectation.String(), ExpectedTimeNS: opportunity.ExpectedTimeNS,
			WindowStartNS: opportunity.WindowStartNS, WindowEndNS: opportunity.WindowEndNS,
			ScopeJSON: append([]byte(nil), opportunity.Scope...), TerminalTimeNS: opportunity.TerminalTimeNS,
			TerminalOutcome: opportunity.TerminalOutcome.String(), TerminalEvidence: append([]byte(nil), opportunity.TerminalEvidence...),
			CreatedTimeNS: opportunity.CreatedTimeNS,
		}
		if _, err := writer.Write(row); err != nil {
			return result, fmt.Errorf("dataset: write opportunity row: %w", err)
		}
		if writer.Size() > MaxPartitionFileBytes {
			return result, fmt.Errorf("%w: opportunity archive byte bound", ErrInvalidInput)
		}
		if writer.Size()-lastFlushSize >= options.RowGroupTargetBytes {
			if err := writer.Flush(); err != nil {
				return result, err
			}
			lastFlushSize = writer.Size()
		}
	}
	logicalHash, err := quality.OpportunityArchiveLogicalHash(generation.Rows)
	if err != nil {
		return result, err
	}
	writer.SetKeyValueMetadata("enable.logical_sha256", hex.EncodeToString(logicalHash[:]))
	writer.SetKeyValueMetadata("enable.input_rows", strconv.Itoa(len(generation.Rows)))
	writer.SetKeyValueMetadata("enable.parquet_rows", strconv.Itoa(len(generation.Rows)))
	if err := writer.Close(); err != nil {
		return result, fmt.Errorf("dataset: close opportunity parquet writer: %w", err)
	}
	closed = true
	if err := file.Sync(); err != nil {
		return result, err
	}
	if err := file.Close(); err != nil {
		return result, err
	}

	physicalHash, fileBytes, err := hashFileContext(ctx, staging)
	if err != nil {
		return result, err
	}
	date := time.Unix(0, generation.From.TerminalTimeNS).UTC().Format("2006-01-02")
	partitionDir := pathologize.Join(root, "quality", "v1", "kind=opportunity", "date="+date)
	parquetPath := filepath.Join(partitionDir, "part="+hex.EncodeToString(logicalHash[:])+".parquet")
	published, err := fileflow.Move(staging, parquetPath)
	if err != nil {
		return result, fmt.Errorf("dataset: publish opportunity parquet: %w", err)
	}
	if published != parquetPath {
		return result, fmt.Errorf("dataset: opportunity content-address collision")
	}
	if err := syncPublicationDirectories(root, partitionDir, options.DirectorySync); err != nil {
		return result, err
	}
	stats, err := inspectParquet(ctx, parquetPath)
	if err != nil {
		return result, err
	}
	relativeParquet, err := filepath.Rel(root, parquetPath)
	if err != nil {
		return result, err
	}
	datasetPartitionID := opportunityDatasetPartitionID(generation.Fingerprint, logicalHash, physicalHash)
	manifest := OpportunityArchiveManifest{
		ManifestVersion: OpportunityArchiveManifestVersion, DatasetVersion: DatasetVersion,
		ParquetWriterVersion: ParquetWriterVersion, ParquetFormatCompatibility: ParquetFormatCompatibility,
		SchemaName: opportunityArchiveSchemaName, SchemaVersion: OpportunityArchiveSchemaVersion,
		SchemaSHA256: hex.EncodeToString(schemaHash[:]), Family: FamilyOpportunity,
		GenerationID: generation.GenerationID, GenerationFingerprint: hex.EncodeToString(generation.Fingerprint[:]),
		CatalogSnapshotSHA256: hex.EncodeToString(generation.CatalogSnapshotHash[:]),
		MapperSetSHA256:       hex.EncodeToString(generation.MapperSetHash[:]),
		LedgerPartition:       generation.Partition, BoundaryFromTimeNS: generation.From.TerminalTimeNS,
		BoundaryFromOpportunityID: generation.From.OpportunityID, BoundaryThroughTimeNS: generation.Through.TerminalTimeNS,
		BoundaryThroughOpportunityID: generation.Through.OpportunityID, RowCount: uint64(len(generation.Rows)),
		LogicalSHA256: hex.EncodeToString(logicalHash[:]), PhysicalSHA256: hex.EncodeToString(physicalHash[:]),
		FileBytes: fileBytes, RowGroups: stats.rowGroups, Pages: stats.pages, Values: stats.values,
		ParquetFile: filepath.ToSlash(relativeParquet), DatasetPartitionID: datasetPartitionID,
		DatasetPolicyID: hashHex(options.DatasetPolicyID), ReplayConfigID: hashHex(options.ReplayConfigID),
		InputManifestSetID: hashHex(options.InputManifestSetID), Options: ManifestOptions{RowGroupTargetBytes: options.RowGroupTargetBytes,
			PageBufferBytes: options.PageBufferBytes, Compression: options.Compression, Dictionary: options.Dictionary, BloomFilter: options.BloomFilter},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return result, err
	}
	manifestBytes = append(manifestBytes, '\n')
	manifestHash := sha256.Sum256(manifestBytes)
	manifestTemp, err := os.CreateTemp(root, ".opportunity-manifest-staging.*")
	if err != nil {
		return result, err
	}
	manifestTempPath := manifestTemp.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(manifestTempPath)
		}
	}()
	if _, err := manifestTemp.Write(manifestBytes); err != nil {
		_ = manifestTemp.Close()
		return result, err
	}
	if err := manifestTemp.Sync(); err != nil {
		_ = manifestTemp.Close()
		return result, err
	}
	if err := manifestTemp.Close(); err != nil {
		return result, err
	}
	manifestPath, err := opportunityManifestPath(root, generation)
	if err != nil {
		return result, err
	}
	publishedManifest, err := fileflow.Move(manifestTempPath, manifestPath)
	if err != nil {
		return result, fmt.Errorf("dataset: publish opportunity manifest last: %w", err)
	}
	if publishedManifest != manifestPath {
		return result, fmt.Errorf("dataset: opportunity generation manifest collision")
	}
	if err := syncPublicationDirectories(root, partitionDir, options.DirectorySync); err != nil {
		return result, err
	}
	result = OpportunityArchiveResult{Manifest: manifest, ManifestPath: manifestPath, ParquetPath: parquetPath,
		ManifestHash: manifestHash, Rows: append([]quality.Opportunity(nil), generation.Rows...)}
	verified, err := VerifyOpportunityArchive(ctx, root, manifestPath, generation)
	if err != nil {
		return OpportunityArchiveResult{}, err
	}
	return verified, nil
}

func VerifyOpportunityArchive(ctx context.Context, root, manifestPath string, generation quality.SpillGeneration) (OpportunityArchiveResult, error) {
	if err := generation.Validate(); err != nil {
		return OpportunityArchiveResult{}, err
	}
	resolvedManifest, err := resolveOpportunityArchiveTarget(root, manifestPath)
	if err != nil {
		return OpportunityArchiveResult{}, err
	}
	manifestFile, err := os.Open(resolvedManifest)
	if err != nil {
		return OpportunityArchiveResult{}, err
	}
	contents, readErr := io.ReadAll(io.LimitReader(manifestFile, MaxManifestBytes+1))
	closeErr := manifestFile.Close()
	if readErr != nil {
		return OpportunityArchiveResult{}, readErr
	}
	if closeErr != nil {
		return OpportunityArchiveResult{}, closeErr
	}
	if len(contents) == 0 || len(contents) > MaxManifestBytes {
		return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity manifest size", ErrManifestMismatch)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var manifest OpportunityArchiveManifest
	if err := decoder.Decode(&manifest); err != nil {
		return OpportunityArchiveResult{}, fmt.Errorf("%w: decode opportunity manifest: %v", ErrManifestMismatch, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return OpportunityArchiveResult{}, fmt.Errorf("%w: trailing opportunity manifest", ErrManifestMismatch)
	}
	if err := validateOpportunityManifest(manifest, generation); err != nil {
		return OpportunityArchiveResult{}, err
	}
	parquetPath, err := resolveOpportunityArchiveTarget(root, manifest.ParquetFile)
	if err != nil {
		return OpportunityArchiveResult{}, err
	}
	physicalHash, fileBytes, err := hashFileContext(ctx, parquetPath)
	if err != nil {
		return OpportunityArchiveResult{}, err
	}
	if hex.EncodeToString(physicalHash[:]) != manifest.PhysicalSHA256 || fileBytes != manifest.FileBytes {
		return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity physical identity", ErrManifestMismatch)
	}
	file, err := os.Open(parquetPath)
	if err != nil {
		return OpportunityArchiveResult{}, err
	}
	reader := parquet.NewGenericReader[opportunityParquetRow](file)
	defer reader.Close()
	defer file.Close()
	rows := make([]quality.Opportunity, 0, manifest.RowCount)
	logical := sha256.New()
	_, _ = logical.Write([]byte("dataset-opportunity-archive-logical-v1\x00"))
	batch := make([]opportunityParquetRow, 128)
	for {
		if err := ctx.Err(); err != nil {
			return OpportunityArchiveResult{}, err
		}
		count, readErr := reader.Read(batch)
		for index := range count {
			row := batch[index]
			if row.RowOrdinal != uint64(len(rows)) {
				return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity row ordinal", ErrCorruptDataset)
			}
			opportunity, err := opportunityFromParquet(row)
			if err != nil {
				return OpportunityArchiveResult{}, err
			}
			digest, _ := opportunity.LogicalHash()
			ordinal := len(rows)
			if ordinal >= len(generation.RowIdentities) {
				return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity row exceeds generation membership", ErrManifestMismatch)
			}
			if digest != row.RowLogicalHash {
				return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity row logical hash", ErrCorruptDataset)
			}
			if digest != generation.RowIdentities[ordinal].LogicalHash {
				return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity row differs from generation identity", ErrManifestMismatch)
			}
			_, _ = logical.Write(digest[:])
			rows = append(rows, opportunity)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return OpportunityArchiveResult{}, readErr
		}
	}
	logicalHash := sumHash(logical)
	if uint64(len(rows)) != manifest.RowCount || hex.EncodeToString(logicalHash[:]) != manifest.LogicalSHA256 || len(rows) != len(generation.RowIdentities) {
		return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity logical identity", ErrManifestMismatch)
	}
	for index, row := range rows {
		identity := generation.RowIdentities[index]
		if row.OpportunityID != identity.OpportunityID || row.TerminalTimeNS != identity.TerminalTimeNS || row.LedgerPartition != generation.Partition {
			return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity generation membership", ErrManifestMismatch)
		}
	}
	stats, err := inspectParquet(ctx, parquetPath)
	if err != nil {
		return OpportunityArchiveResult{}, err
	}
	if stats.rowGroups != manifest.RowGroups || stats.pages != manifest.Pages || stats.values != manifest.Values {
		return OpportunityArchiveResult{}, fmt.Errorf("%w: opportunity parquet statistics", ErrManifestMismatch)
	}
	return OpportunityArchiveResult{Manifest: manifest, ManifestPath: resolvedManifest, ParquetPath: parquetPath,
		ManifestHash: sha256.Sum256(contents), Rows: rows}, nil
}

func validateOpportunityManifest(manifest OpportunityArchiveManifest, generation quality.SpillGeneration) error {
	if manifest.ManifestVersion != OpportunityArchiveManifestVersion || manifest.DatasetVersion != DatasetVersion ||
		manifest.ParquetWriterVersion != ParquetWriterVersion || manifest.ParquetFormatCompatibility != ParquetFormatCompatibility ||
		manifest.SchemaName != opportunityArchiveSchemaName || manifest.SchemaVersion != OpportunityArchiveSchemaVersion ||
		manifest.Family != FamilyOpportunity || manifest.GenerationID != generation.GenerationID || manifest.LedgerPartition != generation.Partition ||
		manifest.GenerationFingerprint != hex.EncodeToString(generation.Fingerprint[:]) ||
		manifest.CatalogSnapshotSHA256 != hex.EncodeToString(generation.CatalogSnapshotHash[:]) ||
		manifest.MapperSetSHA256 != hex.EncodeToString(generation.MapperSetHash[:]) ||
		manifest.BoundaryFromTimeNS != generation.From.TerminalTimeNS ||
		manifest.BoundaryFromOpportunityID != generation.From.OpportunityID || manifest.BoundaryThroughTimeNS != generation.Through.TerminalTimeNS ||
		manifest.BoundaryThroughOpportunityID != generation.Through.OpportunityID || manifest.RowCount != uint64(len(generation.RowIdentities)) ||
		manifest.RowCount == 0 || manifest.FileBytes <= 0 || manifest.ParquetFile == "" || manifest.DatasetPartitionID == "" ||
		manifest.RowGroups == 0 || manifest.Pages == 0 || manifest.Values == 0 {
		return fmt.Errorf("%w: invalid opportunity manifest fields", ErrManifestMismatch)
	}
	schema := parquet.SchemaOf(new(opportunityParquetRow))
	expectedSchema := schemaDigest(opportunityArchiveSchemaName, OpportunityArchiveSchemaVersion, schema)
	if manifest.SchemaSHA256 != hex.EncodeToString(expectedSchema[:]) {
		return fmt.Errorf("%w: opportunity schema identity", ErrManifestMismatch)
	}
	for _, encoded := range []string{manifest.GenerationFingerprint, manifest.CatalogSnapshotSHA256, manifest.MapperSetSHA256,
		manifest.SchemaSHA256, manifest.LogicalSHA256, manifest.PhysicalSHA256,
		manifest.DatasetPolicyID, manifest.ReplayConfigID, manifest.InputManifestSetID} {
		if _, err := decodeHash(encoded); err != nil {
			return err
		}
	}
	if manifest.Options.RowGroupTargetBytes != 64<<20 && manifest.Options.RowGroupTargetBytes != 256<<20 && manifest.Options.RowGroupTargetBytes != 1024<<20 ||
		manifest.Options.PageBufferBytes < 64<<10 || manifest.Options.PageBufferBytes > 4<<20 || manifest.Options.Compression != "zstd" {
		return fmt.Errorf("%w: opportunity writer options", ErrManifestMismatch)
	}
	logical, _ := decodeHash(manifest.LogicalSHA256)
	physical, _ := decodeHash(manifest.PhysicalSHA256)
	if manifest.DatasetPartitionID != opportunityDatasetPartitionID(generation.Fingerprint, logical, physical) {
		return fmt.Errorf("%w: opportunity dataset partition identity", ErrManifestMismatch)
	}
	return nil
}

func opportunityFromParquet(row opportunityParquetRow) (quality.Opportunity, error) {
	if (row.InstrumentUID != "") != row.HasInstrumentUID {
		return quality.Opportunity{}, fmt.Errorf("%w: instrument UID state", ErrCorruptDataset)
	}
	expectation, err := quality.ParseOpportunityExpectation(row.Expectation)
	if err != nil {
		return quality.Opportunity{}, fmt.Errorf("%w: %v", ErrCorruptDataset, err)
	}
	outcome, err := captureOutcome(row.TerminalOutcome)
	if err != nil {
		return quality.Opportunity{}, err
	}
	opportunity := quality.Opportunity{OpportunityID: row.OpportunityID, LedgerPartition: row.LedgerPartition,
		SourceID: row.SourceID, ChannelID: row.ChannelID, InstrumentUID: row.InstrumentUID, Expectation: expectation,
		ExpectedTimeNS: row.ExpectedTimeNS, WindowStartNS: row.WindowStartNS, WindowEndNS: row.WindowEndNS,
		Scope: json.RawMessage(append([]byte(nil), row.ScopeJSON...)), Terminal: true, TerminalTimeNS: row.TerminalTimeNS,
		TerminalOutcome: outcome, TerminalEvidence: json.RawMessage(append([]byte(nil), row.TerminalEvidence...)), CreatedTimeNS: row.CreatedTimeNS}
	if err := opportunity.Validate(); err != nil {
		return quality.Opportunity{}, fmt.Errorf("%w: %v", ErrCorruptDataset, err)
	}
	return opportunity, nil
}

func captureOutcome(value string) (quality.OpportunityOutcome, error) {
	outcome, err := capture.ParseOpportunityOutcome(value)
	if err != nil {
		return 0, fmt.Errorf("%w: terminal outcome %q", ErrCorruptDataset, value)
	}
	return outcome, nil
}

func opportunityManifestPath(root string, generation quality.SpillGeneration) (string, error) {
	if err := generation.Validate(); err != nil {
		return "", err
	}
	date := time.Unix(0, generation.From.TerminalTimeNS).UTC().Format("2006-01-02")
	partitionDir := pathologize.Join(root, "quality", "v1", "kind=opportunity", "date="+date)
	identity := sha256.Sum256([]byte("dataset-opportunity-generation-manifest-v1\x00" + generation.GenerationID))
	return filepath.Join(partitionDir, "generation="+hex.EncodeToString(identity[:])+".manifest.json"), nil
}

func opportunityDatasetPartitionID(generation, logical, physical [sha256.Size]byte) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("dataset-opportunity-partition-uuid-v1\x00"))
	_, _ = hasher.Write(generation[:])
	_, _ = hasher.Write(logical[:])
	_, _ = hasher.Write(physical[:])
	value := hasher.Sum(nil)[:16]
	value[6] = (value[6] & 0x0f) | 0x80
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value)
	return strings.Join([]string{encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]}, "-")
}

func opportunityArchiveCommit(result OpportunityArchiveResult) quality.ArchiveCommit {
	manifest := result.Manifest
	generation, _ := decodeHash(manifest.GenerationFingerprint)
	inputSet := generation
	catalogSnapshot, _ := decodeHash(manifest.CatalogSnapshotSHA256)
	mapperSet, _ := decodeHash(manifest.MapperSetSHA256)
	logical, _ := decodeHash(manifest.LogicalSHA256)
	physical, _ := decodeHash(manifest.PhysicalSHA256)
	manifestRelative := manifestPathRelative(result.ManifestPath, manifest.ParquetFile)
	return quality.ArchiveCommit{GenerationID: manifest.GenerationID, GenerationFingerprint: generation,
		DatasetPartitionID: manifest.DatasetPartitionID, ManifestHash: result.ManifestHash, ManifestPath: manifestRelative,
		ParquetPath: manifest.ParquetFile, DatasetVersion: manifest.DatasetVersion,
		PartitionKey: strings.TrimSuffix(filepath.ToSlash(filepath.Dir(manifest.ParquetFile)), "/"),
		RangeStartNS: manifest.BoundaryFromTimeNS, RangeEndNS: manifest.BoundaryThroughTimeNS,
		InputSetHash: inputSet, CatalogSnapshotHash: catalogSnapshot, MapperSetHash: mapperSet,
		LogicalHash: logical, PhysicalHash: physical}
}

func manifestPathRelative(manifestPath, parquetRelative string) string {
	marker := filepath.FromSlash(strings.TrimSuffix(filepath.ToSlash(filepath.Dir(parquetRelative)), "/"))
	clean := filepath.Clean(manifestPath)
	index := strings.LastIndex(clean, marker)
	if index < 0 {
		return filepath.ToSlash(filepath.Base(clean))
	}
	return filepath.ToSlash(clean[index:])
}

func resolveOpportunityArchiveTarget(root, target string) (string, error) {
	if root == "" || target == "" {
		return "", fmt.Errorf("%w: empty opportunity archive path", ErrManifestMismatch)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolvedRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return "", fmt.Errorf("%w: resolve opportunity archive root: %v", ErrManifestMismatch, err)
	}
	lexicalTarget, err := containedPath(absoluteRoot, target)
	if err != nil {
		return "", err
	}
	resolvedTarget, err := filepath.EvalSymlinks(lexicalTarget)
	if err != nil {
		return "", fmt.Errorf("%w: resolve opportunity archive target: %v", ErrManifestMismatch, err)
	}
	relative, err := filepath.Rel(resolvedRoot, resolvedTarget)
	if err != nil || relative == "." || !filepath.IsLocal(relative) {
		return "", fmt.Errorf("%w: opportunity archive target escapes resolved root", ErrManifestMismatch)
	}
	return resolvedTarget, nil
}
