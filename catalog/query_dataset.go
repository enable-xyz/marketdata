package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func validateDatasetPublication(value DatasetPublication) error {
	for name, text := range map[string]string{
		"dataset_id": value.DatasetID, "dataset_family": value.DatasetFamily, "dataset_version": value.DatasetVersion,
		"source_id": value.SourceID, "schema_name": value.SchemaName, "partition_key": value.PartitionKey,
		"parquet_object_key": value.ParquetObjectKey, "manifest_object_key": value.ManifestObjectKey,
	} {
		if !validQueryText(text) {
			return fmt.Errorf("%w: %s", ErrInvalidQueryProjection, name)
		}
	}
	if _, err := uuid.Parse(value.DatasetID); err != nil {
		return fmt.Errorf("%w: dataset UUID", ErrInvalidQueryProjection)
	}
	if _, err := uuid.Parse(value.SourceID); err != nil {
		return fmt.Errorf("%w: source UUID", ErrInvalidQueryProjection)
	}
	if value.State != DatasetVerified || value.SchemaVersion == 0 || value.ManifestVersion == 0 ||
		value.RangeStartNS < 0 || value.RangeEndNS <= value.RangeStartNS || value.ParquetBytes <= 0 ||
		len(value.ManifestBytes) == 0 || len(value.ManifestBytes) > MaximumCheckpointBytes ||
		!nonzeroHash(value.InputSegmentSetHash) || !nonzeroHash(value.CatalogSnapshotHash) ||
		!nonzeroHash(value.MapperSetHash) || !nonzeroHash(value.LogicalHash) || !nonzeroHash(value.PhysicalHash) ||
		!nonzeroHash(value.ManifestHash) || sha256.Sum256(value.ManifestBytes) != value.ManifestHash {
		return fmt.Errorf("%w: incomplete verified dataset publication", ErrInvalidQueryProjection)
	}
	if len(value.InputSegmentIDs) == 0 || len(value.InputSegmentIDs) > MaximumDatasetLineage ||
		len(value.Coverage) == 0 || len(value.Coverage) > MaximumDatasetCoverage {
		return fmt.Errorf("%w: dataset lineage or coverage bound", ErrInvalidQueryProjection)
	}
	segments := make(map[string]struct{}, len(value.InputSegmentIDs))
	for _, id := range value.InputSegmentIDs {
		if _, err := uuid.Parse(id); err != nil {
			return fmt.Errorf("%w: raw segment UUID", ErrInvalidQueryProjection)
		}
		if _, exists := segments[id]; exists {
			return fmt.Errorf("%w: duplicate raw segment", ErrInvalidQueryProjection)
		}
		segments[id] = struct{}{}
	}
	previousCoverageID := ""
	for _, coverage := range value.Coverage {
		if _, err := uuid.Parse(coverage.ID); err != nil || coverage.ID <= previousCoverageID ||
			!validQueryText(coverage.Tuple.SourceID) || !validQueryText(coverage.Tuple.ChannelID) ||
			(coverage.Tuple.InstrumentUID != "" && !validQueryText(coverage.Tuple.InstrumentUID)) ||
			!validQueryText(coverage.State) || coverage.StartReceivedTimeNS < value.RangeStartNS ||
			coverage.EndReceivedTimeNS > value.RangeEndNS || coverage.StartReceivedTimeNS >= coverage.EndReceivedTimeNS {
			return fmt.Errorf("%w: invalid or unordered dataset coverage", ErrInvalidQueryProjection)
		}
		if _, err := uuid.Parse(coverage.Tuple.SourceID); err != nil {
			return fmt.Errorf("%w: coverage source UUID", ErrInvalidQueryProjection)
		}
		if coverage.Tuple.InstrumentUID != "" {
			if _, err := uuid.Parse(coverage.Tuple.InstrumentUID); err != nil {
				return fmt.Errorf("%w: coverage instrument UUID", ErrInvalidQueryProjection)
			}
		}
		previousCoverageID = coverage.ID
	}
	return nil
}

// RecordVerifiedDataset atomically records exact immutable publication bytes,
// ordered raw lineage, and tuple coverage. Exact verified/committed retries are
// idempotent; a changed identity never overwrites catalog evidence.
func (s *QueryStore) RecordVerifiedDataset(ctx context.Context, value DatasetPublication) (err error) {
	if err := validateDatasetPublication(value); err != nil {
		return err
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return s.unavailable("begin verified dataset", err)
	}
	defer rollbackQueryTx(ctx, tx, &err)

	_, err = tx.Exec(ctx, `
INSERT INTO dataset_partition (
    dataset_partition_id, dataset_family, dataset_version, partition_key,
    range_start_ns, range_end_ns, input_segment_set_hash, catalog_snapshot_hash,
    mapper_set_hash, logical_hash, physical_hash, object_key, canonical, state
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, false, 'verified')
ON CONFLICT (dataset_partition_id) DO NOTHING
`, value.DatasetID, value.DatasetFamily, value.DatasetVersion, value.PartitionKey,
		value.RangeStartNS, value.RangeEndNS, value.InputSegmentSetHash[:], value.CatalogSnapshotHash[:],
		value.MapperSetHash[:], value.LogicalHash[:], value.PhysicalHash[:], value.ParquetObjectKey)
	if err != nil {
		return fmt.Errorf("catalog: insert verified dataset partition: %w", err)
	}
	partitionState, err := verifyDatasetPartitionIdentity(ctx, tx, value)
	if err != nil {
		return err
	}

	if partitionState != DatasetCommitted {
		_, err = tx.Exec(ctx, `
INSERT INTO dataset_manifest (
    dataset_partition_id, source_id, schema_name, schema_version, manifest_version,
    manifest_object_key, manifest_hash, manifest_bytes, parquet_byte_length
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (dataset_partition_id) DO NOTHING
`, value.DatasetID, value.SourceID, value.SchemaName, value.SchemaVersion, value.ManifestVersion,
			value.ManifestObjectKey, value.ManifestHash[:], value.ManifestBytes, value.ParquetBytes)
		if err != nil {
			return fmt.Errorf("catalog: insert dataset manifest: %w", err)
		}
	}
	if err := verifyDatasetManifestIdentity(ctx, tx, value); err != nil {
		return err
	}

	for ordinal, segmentID := range value.InputSegmentIDs {
		var committed bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM raw_segment AS r
    JOIN raw_segment_manifest AS m ON m.raw_segment_id = r.raw_segment_id
    WHERE r.raw_segment_id = $1 AND r.state = 'committed' AND r.committed_at IS NOT NULL
)
`, segmentID).Scan(&committed); err != nil {
			return fmt.Errorf("catalog: verify dataset raw lineage: %w", err)
		}
		if !committed {
			return queryConflict("dataset lineage contains uncommitted raw segment")
		}
		if partitionState != DatasetCommitted {
			_, err := tx.Exec(ctx, `
INSERT INTO dataset_partition_segment (dataset_partition_id, raw_segment_id, input_ordinal)
VALUES ($1, $2, $3)
ON CONFLICT (dataset_partition_id, raw_segment_id) DO NOTHING
`, value.DatasetID, segmentID, ordinal)
			if err != nil {
				return fmt.Errorf("catalog: insert dataset lineage: %w", err)
			}
		}
		var existingOrdinal int
		if err := tx.QueryRow(ctx, `
SELECT input_ordinal FROM dataset_partition_segment
WHERE dataset_partition_id = $1 AND raw_segment_id = $2
`, value.DatasetID, segmentID).Scan(&existingOrdinal); err != nil || existingOrdinal != ordinal {
			if err != nil {
				return fmt.Errorf("catalog: verify dataset lineage: %w", err)
			}
			return queryConflict("dataset lineage ordinal")
		}
	}

	for _, coverage := range value.Coverage {
		var instrument any
		if coverage.Tuple.InstrumentUID != "" {
			instrument = coverage.Tuple.InstrumentUID
		}
		if partitionState != DatasetCommitted {
			_, err := tx.Exec(ctx, `
INSERT INTO dataset_coverage (
    coverage_id, dataset_partition_id, source_id, channel_id, instrument_uid,
    range_start_ns, range_end_ns, coverage_state
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (coverage_id) DO NOTHING
`, coverage.ID, value.DatasetID, coverage.Tuple.SourceID, coverage.Tuple.ChannelID, instrument,
				coverage.StartReceivedTimeNS, coverage.EndReceivedTimeNS, coverage.State)
			if err != nil {
				return fmt.Errorf("catalog: insert dataset coverage: %w", err)
			}
		}
		var existing DatasetCoverage
		if err := tx.QueryRow(ctx, `
SELECT coverage_id::text, source_id::text, channel_id, COALESCE(instrument_uid::text, ''),
       range_start_ns, range_end_ns, coverage_state
FROM dataset_coverage WHERE coverage_id = $1 AND dataset_partition_id = $2
`, coverage.ID, value.DatasetID).Scan(&existing.ID, &existing.Tuple.SourceID, &existing.Tuple.ChannelID,
			&existing.Tuple.InstrumentUID, &existing.StartReceivedTimeNS, &existing.EndReceivedTimeNS, &existing.State); err != nil {
			return fmt.Errorf("catalog: verify dataset coverage: %w", err)
		}
		if existing != coverage {
			return queryConflict("dataset coverage identity")
		}
	}
	var lineageCount, coverageCount int
	if err := tx.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM dataset_partition_segment WHERE dataset_partition_id = $1),
    (SELECT count(*) FROM dataset_coverage WHERE dataset_partition_id = $1)
`, value.DatasetID).Scan(&lineageCount, &coverageCount); err != nil {
		return fmt.Errorf("catalog: count dataset evidence: %w", err)
	}
	if lineageCount != len(value.InputSegmentIDs) || coverageCount != len(value.Coverage) {
		return queryConflict("dataset evidence set")
	}
	if err := tx.Commit(ctx); err != nil {
		return s.unavailable("commit verified dataset", err)
	}
	return nil
}

func verifyDatasetPartitionIdentity(ctx context.Context, tx pgx.Tx, value DatasetPublication) (DatasetState, error) {
	var family, version, partitionKey, objectKey, state string
	var start, end int64
	var input, catalogHash, mapper, logical, physical []byte
	if err := tx.QueryRow(ctx, `
SELECT dataset_family, dataset_version, partition_key, range_start_ns, range_end_ns,
       input_segment_set_hash, catalog_snapshot_hash, mapper_set_hash, logical_hash,
       physical_hash, object_key, state::text
FROM dataset_partition WHERE dataset_partition_id = $1 FOR UPDATE
`, value.DatasetID).Scan(&family, &version, &partitionKey, &start, &end, &input, &catalogHash,
		&mapper, &logical, &physical, &objectKey, &state); err != nil {
		return "", fmt.Errorf("catalog: verify dataset partition identity: %w", err)
	}
	if family != value.DatasetFamily || version != value.DatasetVersion || partitionKey != value.PartitionKey ||
		start != value.RangeStartNS || end != value.RangeEndNS || !bytes.Equal(input, value.InputSegmentSetHash[:]) ||
		!bytes.Equal(catalogHash, value.CatalogSnapshotHash[:]) || !bytes.Equal(mapper, value.MapperSetHash[:]) ||
		!bytes.Equal(logical, value.LogicalHash[:]) || !bytes.Equal(physical, value.PhysicalHash[:]) ||
		objectKey != value.ParquetObjectKey || (state != string(DatasetVerified) && state != string(DatasetCommitted)) {
		return "", queryConflict("dataset partition identity")
	}
	return DatasetState(state), nil
}

func verifyDatasetManifestIdentity(ctx context.Context, tx pgx.Tx, value DatasetPublication) error {
	var sourceID, schemaName, manifestKey string
	var schemaVersion, manifestVersion int16
	var manifestHash, manifestBytes []byte
	var parquetBytes int64
	if err := tx.QueryRow(ctx, `
SELECT source_id::text, schema_name, schema_version, manifest_version,
       manifest_object_key, manifest_hash, manifest_bytes, parquet_byte_length
FROM dataset_manifest WHERE dataset_partition_id = $1
`, value.DatasetID).Scan(&sourceID, &schemaName, &schemaVersion, &manifestVersion,
		&manifestKey, &manifestHash, &manifestBytes, &parquetBytes); err != nil {
		return fmt.Errorf("catalog: verify dataset manifest identity: %w", err)
	}
	if sourceID != value.SourceID || schemaName != value.SchemaName || schemaVersion != int16(value.SchemaVersion) ||
		manifestVersion != int16(value.ManifestVersion) || manifestKey != value.ManifestObjectKey ||
		!bytes.Equal(manifestHash, value.ManifestHash[:]) || !bytes.Equal(manifestBytes, value.ManifestBytes) ||
		parquetBytes != value.ParquetBytes {
		return queryConflict("dataset manifest identity")
	}
	return nil
}

func (s *QueryStore) CommitDataset(ctx context.Context, datasetID string) (err error) {
	if _, parseErr := uuid.Parse(datasetID); parseErr != nil {
		return fmt.Errorf("%w: dataset UUID", ErrInvalidQueryProjection)
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return s.unavailable("begin dataset commit", err)
	}
	defer rollbackQueryTx(ctx, tx, &err)
	var state string
	if err := tx.QueryRow(ctx, `
SELECT state::text FROM dataset_partition WHERE dataset_partition_id = $1 FOR UPDATE
`, datasetID).Scan(&state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return queryNotFound("dataset publication")
		}
		return fmt.Errorf("catalog: lock dataset publication: %w", err)
	}
	if state == string(DatasetCommitted) {
		return tx.Commit(ctx)
	}
	if state != string(DatasetVerified) {
		return queryConflict("dataset is not verified")
	}
	var manifestCount, lineageCount, coverageCount int
	if err := tx.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM dataset_manifest WHERE dataset_partition_id = $1),
    (SELECT count(*) FROM dataset_partition_segment WHERE dataset_partition_id = $1),
    (SELECT count(*) FROM dataset_coverage WHERE dataset_partition_id = $1)
`, datasetID).Scan(&manifestCount, &lineageCount, &coverageCount); err != nil {
		return fmt.Errorf("catalog: verify dataset commit evidence: %w", err)
	}
	if manifestCount != 1 || lineageCount == 0 || coverageCount == 0 {
		return queryConflict("dataset commit evidence is incomplete")
	}
	command, err := tx.Exec(ctx, `
UPDATE dataset_partition
SET state = 'committed', canonical = true, committed_at = now()
WHERE dataset_partition_id = $1 AND state = 'verified'
`, datasetID)
	if err != nil {
		return fmt.Errorf("catalog: commit dataset partition: %w", err)
	}
	if !commandChangedOne(command) {
		return queryConflict("dataset commit transition")
	}
	if err := tx.Commit(ctx); err != nil {
		return s.unavailable("commit dataset transaction", err)
	}
	return nil
}

func (s *QueryStore) FindDataset(ctx context.Context, datasetID string) (DatasetPublication, bool, error) {
	if _, err := uuid.Parse(datasetID); err != nil {
		return DatasetPublication{}, false, fmt.Errorf("%w: dataset UUID", ErrInvalidQueryProjection)
	}
	value, err := s.readDatasetPublication(ctx, datasetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return DatasetPublication{}, false, nil
	}
	if err != nil {
		return DatasetPublication{}, false, err
	}
	return value, true, nil
}

func (s *QueryStore) readDatasetPublication(ctx context.Context, datasetID string) (DatasetPublication, error) {
	var value DatasetPublication
	var input, catalogHash, mapper, logical, physical, manifestHash []byte
	var schemaVersion, manifestVersion int16
	err := s.database.QueryRow(ctx, `
SELECT p.dataset_partition_id::text, p.dataset_family, p.dataset_version,
       m.source_id::text, m.schema_name, m.schema_version, m.manifest_version,
       p.partition_key, p.range_start_ns, p.range_end_ns, p.input_segment_set_hash,
       p.catalog_snapshot_hash, p.mapper_set_hash, p.logical_hash, p.physical_hash,
       p.object_key, m.manifest_object_key, m.parquet_byte_length, m.manifest_hash,
       m.manifest_bytes, p.state::text
FROM dataset_partition AS p
JOIN dataset_manifest AS m ON m.dataset_partition_id = p.dataset_partition_id
WHERE p.dataset_partition_id = $1
`, datasetID).Scan(&value.DatasetID, &value.DatasetFamily, &value.DatasetVersion,
		&value.SourceID, &value.SchemaName, &schemaVersion, &manifestVersion, &value.PartitionKey,
		&value.RangeStartNS, &value.RangeEndNS, &input, &catalogHash, &mapper, &logical,
		&physical, &value.ParquetObjectKey, &value.ManifestObjectKey, &value.ParquetBytes,
		&manifestHash, &value.ManifestBytes, &value.State)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DatasetPublication{}, err
		}
		return DatasetPublication{}, s.unavailable("read dataset publication", err)
	}
	if schemaVersion <= 0 || manifestVersion <= 0 || len(input) != sha256.Size || len(catalogHash) != sha256.Size ||
		len(mapper) != sha256.Size || len(logical) != sha256.Size || len(physical) != sha256.Size || len(manifestHash) != sha256.Size {
		s.ready.Store(false)
		return DatasetPublication{}, fmt.Errorf("%w: malformed dataset publication", ErrInvalidQueryProjection)
	}
	value.SchemaVersion, value.ManifestVersion = uint16(schemaVersion), uint16(manifestVersion)
	copy(value.InputSegmentSetHash[:], input)
	copy(value.CatalogSnapshotHash[:], catalogHash)
	copy(value.MapperSetHash[:], mapper)
	copy(value.LogicalHash[:], logical)
	copy(value.PhysicalHash[:], physical)
	copy(value.ManifestHash[:], manifestHash)
	lineage, err := s.datasetPartitionLineage(ctx, datasetID)
	if err != nil {
		return DatasetPublication{}, err
	}
	value.InputSegmentIDs = make([]string, len(lineage))
	for index := range lineage {
		value.InputSegmentIDs[index] = lineage[index].RawSegmentID
	}
	value.Coverage, err = s.datasetPartitionCoverage(ctx, datasetID)
	if err != nil {
		return DatasetPublication{}, err
	}
	return value.clone(), nil
}

func (s *QueryStore) StreamCommittedDatasets(ctx context.Context, visit func(DatasetPublication) error) error {
	if visit == nil {
		return fmt.Errorf("%w: committed dataset visitor", ErrInvalidQueryProjection)
	}
	rows, err := s.database.Query(ctx, `
SELECT p.dataset_partition_id::text
FROM dataset_partition AS p
JOIN dataset_manifest AS m ON m.dataset_partition_id = p.dataset_partition_id
WHERE p.state = 'committed' AND p.committed_at IS NOT NULL
ORDER BY p.dataset_family COLLATE "C", p.range_start_ns, p.range_end_ns, p.dataset_partition_id
LIMIT $1
`, MaximumMetadataResults+1)
	if err != nil {
		return s.unavailable("query committed datasets", err)
	}
	ids := make([]string, 0, 128)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return s.unavailable("scan committed dataset identity", err)
		}
		if len(ids) == MaximumMetadataResults {
			rows.Close()
			return fmt.Errorf("%w: committed datasets", ErrQueryBound)
		}
		ids = append(ids, id)
	}
	iterationErr := rows.Err()
	rows.Close()
	if iterationErr != nil {
		return s.unavailable("iterate committed datasets", iterationErr)
	}
	for _, id := range ids {
		value, found, err := s.FindDataset(ctx, id)
		if err != nil {
			return err
		}
		if !found || value.State != DatasetCommitted {
			return queryConflict("committed dataset disappeared")
		}
		if err := visit(value); err != nil {
			return err
		}
	}
	return nil
}

func (s *QueryStore) CommitDatasetGeneration(ctx context.Context, value DatasetGenerationCommit) (err error) {
	if _, parseErr := uuid.Parse(value.DatasetID); parseErr != nil || !nonzeroHash(value.GenerationID) ||
		!nonzeroHash(value.ManifestHash) || !nonzeroHash(value.InputHash) || !nonzeroHash(value.CatalogSnapshotID) ||
		value.ExpectedEventCount == 0 || value.ExpectedRowCount < value.ExpectedEventCount ||
		value.ExpectedEventCount > math.MaxInt64 || value.ExpectedRowCount > math.MaxInt64 ||
		!validQueryText(value.Family) || !validQueryText(value.SchemaName) || value.SchemaVersion == 0 {
		return fmt.Errorf("%w: dataset generation commit", ErrInvalidQueryProjection)
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return s.unavailable("begin dataset generation commit", err)
	}
	defer rollbackQueryTx(ctx, tx, &err)
	var family, schemaName, state string
	var catalogHash, manifestHash []byte
	var schemaVersion int16
	if err := tx.QueryRow(ctx, `
SELECT p.dataset_family, p.catalog_snapshot_hash, m.schema_name, m.schema_version,
       m.manifest_hash, p.state::text
FROM dataset_partition AS p
JOIN dataset_manifest AS m ON m.dataset_partition_id = p.dataset_partition_id
WHERE p.dataset_partition_id = $1 FOR SHARE OF p, m
`, value.DatasetID).Scan(&family, &catalogHash, &schemaName, &schemaVersion, &manifestHash, &state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return queryNotFound("dataset publication")
		}
		return fmt.Errorf("catalog: read generation publication: %w", err)
	}
	if state != string(DatasetCommitted) || family != value.Family || schemaName != value.SchemaName ||
		schemaVersion != int16(value.SchemaVersion) || !bytes.Equal(catalogHash, value.CatalogSnapshotID[:]) ||
		!bytes.Equal(manifestHash, value.ManifestHash[:]) {
		return queryConflict("dataset generation publication identity")
	}
	var coverageCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM dataset_coverage WHERE dataset_partition_id = $1`, value.DatasetID).Scan(&coverageCount); err != nil {
		return fmt.Errorf("catalog: verify generation coverage: %w", err)
	}
	if coverageCount == 0 || coverageCount > MaximumDatasetCoverage {
		return queryConflict("dataset generation coverage")
	}
	_, err = tx.Exec(ctx, `
INSERT INTO dataset_generation (
    generation_id, dataset_partition_id, dataset_family, catalog_snapshot_hash,
    schema_name, schema_version, manifest_hash, input_hash,
    expected_event_count, expected_row_count, state, committed_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'committed', now())
ON CONFLICT (generation_id) DO NOTHING
`, value.GenerationID[:], value.DatasetID, value.Family, value.CatalogSnapshotID[:], value.SchemaName,
		value.SchemaVersion, value.ManifestHash[:], value.InputHash[:], value.ExpectedEventCount, value.ExpectedRowCount)
	if err != nil {
		return fmt.Errorf("catalog: insert dataset generation: %w", err)
	}
	var datasetID, existingFamily, existingSchema, existingState string
	var existingCatalog, existingManifest, existingInput []byte
	var existingSchemaVersion int16
	var existingEvents, existingRows int64
	if err := tx.QueryRow(ctx, `
SELECT dataset_partition_id::text, dataset_family, catalog_snapshot_hash,
       schema_name, schema_version, manifest_hash, input_hash,
       expected_event_count, expected_row_count, state::text
FROM dataset_generation WHERE generation_id = $1
`, value.GenerationID[:]).Scan(&datasetID, &existingFamily, &existingCatalog, &existingSchema,
		&existingSchemaVersion, &existingManifest, &existingInput, &existingEvents, &existingRows, &existingState); err != nil {
		return fmt.Errorf("catalog: verify dataset generation: %w", err)
	}
	if datasetID != value.DatasetID || existingFamily != value.Family || !bytes.Equal(existingCatalog, value.CatalogSnapshotID[:]) ||
		existingSchema != value.SchemaName || existingSchemaVersion != int16(value.SchemaVersion) ||
		!bytes.Equal(existingManifest, value.ManifestHash[:]) || !bytes.Equal(existingInput, value.InputHash[:]) ||
		existingEvents != int64(value.ExpectedEventCount) || existingRows != int64(value.ExpectedRowCount) || existingState != string(DatasetCommitted) {
		return queryConflict("dataset generation identity")
	}
	if err := tx.Commit(ctx); err != nil {
		return s.unavailable("commit dataset generation", err)
	}
	return nil
}

func (s *QueryStore) Dataset(ctx context.Context, id string) (DatasetIdentity, error) {
	decoded, err := decodeDatasetID(id)
	if err != nil {
		return DatasetIdentity{}, err
	}
	return s.readDatasetIdentity(ctx, decoded, "")
}

func (s *QueryStore) LatestDataset(ctx context.Context, family string) (DatasetIdentity, error) {
	if !validQueryText(family) {
		return DatasetIdentity{}, fmt.Errorf("%w: dataset family", ErrInvalidQueryProjection)
	}
	return s.readDatasetIdentity(ctx, [sha256.Size]byte{}, family)
}

func (s *QueryStore) readDatasetIdentity(ctx context.Context, id [sha256.Size]byte, family string) (DatasetIdentity, error) {
	var value DatasetIdentity
	var generation, catalogHash []byte
	var schemaVersion int16
	var err error
	if family == "" {
		err = s.database.QueryRow(ctx, `
SELECT generation_id, dataset_family, catalog_snapshot_hash, schema_name, schema_version
FROM dataset_generation
WHERE generation_id = $1 AND state = 'committed' AND committed_at IS NOT NULL
`, id[:]).Scan(&generation, &value.Family, &catalogHash, &value.SchemaName, &schemaVersion)
	} else {
		err = s.database.QueryRow(ctx, `
SELECT generation_id, dataset_family, catalog_snapshot_hash, schema_name, schema_version
FROM dataset_generation
WHERE dataset_family = $1 AND state = 'committed' AND committed_at IS NOT NULL
ORDER BY committed_at DESC, generation_id DESC
LIMIT 1
`, family).Scan(&generation, &value.Family, &catalogHash, &value.SchemaName, &schemaVersion)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return DatasetIdentity{}, queryNotFound("committed dataset generation")
	}
	if err != nil {
		return DatasetIdentity{}, s.unavailable("read dataset generation", err)
	}
	if len(generation) != sha256.Size || len(catalogHash) != sha256.Size || schemaVersion <= 0 {
		s.ready.Store(false)
		return DatasetIdentity{}, fmt.Errorf("%w: malformed dataset generation", ErrInvalidQueryProjection)
	}
	copy(value.ID[:], generation)
	copy(value.CatalogSnapshotID[:], catalogHash)
	value.SchemaVersion = uint16(schemaVersion)
	return value, nil
}

func (s *QueryStore) DatasetManifest(ctx context.Context, id string) (DatasetManifest, error) {
	decoded, err := decodeDatasetID(id)
	if err != nil {
		return DatasetManifest{}, err
	}
	identity, err := s.readDatasetIdentity(ctx, decoded, "")
	if err != nil {
		return DatasetManifest{}, err
	}
	var path string
	var hash []byte
	var state string
	err = s.database.QueryRow(ctx, `
SELECT m.manifest_object_key, m.manifest_hash, g.state::text
FROM dataset_generation AS g
JOIN dataset_partition AS p ON p.dataset_partition_id = g.dataset_partition_id
JOIN dataset_manifest AS m ON m.dataset_partition_id = g.dataset_partition_id
WHERE g.generation_id = $1 AND g.state = 'committed' AND g.committed_at IS NOT NULL
  AND p.state = 'committed' AND p.committed_at IS NOT NULL
`, decoded[:]).Scan(&path, &hash, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return DatasetManifest{}, queryNotFound("committed dataset manifest")
	}
	if err != nil {
		return DatasetManifest{}, s.unavailable("read dataset manifest", err)
	}
	if len(hash) != sha256.Size || state != string(DatasetCommitted) {
		s.ready.Store(false)
		return DatasetManifest{}, fmt.Errorf("%w: malformed dataset manifest", ErrInvalidQueryProjection)
	}
	result := DatasetManifest{Dataset: identity, ManifestPath: path, State: DatasetCommitted}
	copy(result.ManifestSHA256[:], hash)
	return result, nil
}

func (s *QueryStore) DatasetLineage(ctx context.Context, id string) ([]DatasetLineageEntry, error) {
	decoded, err := decodeDatasetID(id)
	if err != nil {
		return nil, err
	}
	rows, err := s.database.Query(ctx, `
SELECT encode(g.generation_id, 'hex'), l.raw_segment_id::text, l.input_ordinal
FROM dataset_generation AS g
JOIN dataset_partition AS p ON p.dataset_partition_id = g.dataset_partition_id
JOIN dataset_partition_segment AS l ON l.dataset_partition_id = g.dataset_partition_id
WHERE g.generation_id = $1 AND g.state = 'committed' AND g.committed_at IS NOT NULL
  AND p.state = 'committed' AND p.committed_at IS NOT NULL
ORDER BY l.input_ordinal, l.raw_segment_id
LIMIT $2
`, decoded[:], MaximumDatasetLineage+1)
	if err != nil {
		return nil, s.unavailable("query dataset lineage", err)
	}
	defer rows.Close()
	result := make([]DatasetLineageEntry, 0, 1024)
	for rows.Next() {
		var value DatasetLineageEntry
		if err := rows.Scan(&value.DatasetID, &value.RawSegmentID, &value.InputOrdinal); err != nil {
			return nil, s.unavailable("scan dataset lineage", err)
		}
		if len(result) == MaximumDatasetLineage {
			return nil, fmt.Errorf("%w: dataset lineage", ErrQueryBound)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate dataset lineage", err)
	}
	if len(result) == 0 {
		return nil, queryNotFound("committed dataset lineage")
	}
	return result, nil
}

func (s *QueryStore) datasetPartitionLineage(ctx context.Context, datasetID string) ([]DatasetLineageEntry, error) {
	rows, err := s.database.Query(ctx, `
SELECT dataset_partition_id::text, raw_segment_id::text, input_ordinal
FROM dataset_partition_segment
WHERE dataset_partition_id = $1
ORDER BY input_ordinal, raw_segment_id
LIMIT $2
`, datasetID, MaximumDatasetLineage+1)
	if err != nil {
		return nil, s.unavailable("query publication lineage", err)
	}
	defer rows.Close()
	result := make([]DatasetLineageEntry, 0, 1024)
	for rows.Next() {
		var value DatasetLineageEntry
		if err := rows.Scan(&value.DatasetID, &value.RawSegmentID, &value.InputOrdinal); err != nil {
			return nil, s.unavailable("scan publication lineage", err)
		}
		if len(result) == MaximumDatasetLineage {
			return nil, fmt.Errorf("%w: publication lineage", ErrQueryBound)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate publication lineage", err)
	}
	return result, nil
}

func (s *QueryStore) datasetPartitionCoverage(ctx context.Context, datasetID string) ([]DatasetCoverage, error) {
	rows, err := s.database.Query(ctx, `
SELECT coverage_id::text, source_id::text, channel_id, COALESCE(instrument_uid::text, ''),
       range_start_ns, range_end_ns, coverage_state
FROM dataset_coverage
WHERE dataset_partition_id = $1
ORDER BY coverage_id
LIMIT $2
`, datasetID, MaximumDatasetCoverage+1)
	if err != nil {
		return nil, s.unavailable("query publication coverage", err)
	}
	defer rows.Close()
	result := make([]DatasetCoverage, 0, 256)
	for rows.Next() {
		var value DatasetCoverage
		if err := rows.Scan(&value.ID, &value.Tuple.SourceID, &value.Tuple.ChannelID,
			&value.Tuple.InstrumentUID, &value.StartReceivedTimeNS, &value.EndReceivedTimeNS, &value.State); err != nil {
			return nil, s.unavailable("scan publication coverage", err)
		}
		if len(result) == MaximumDatasetCoverage {
			return nil, fmt.Errorf("%w: publication coverage", ErrQueryBound)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate publication coverage", err)
	}
	return result, nil
}

func decodeDatasetID(id string) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	if len(id) != hex.EncodedLen(len(result)) {
		return result, fmt.Errorf("%w: dataset generation identity", ErrInvalidQueryProjection)
	}
	decoded, err := hex.DecodeString(id)
	if err != nil {
		return result, fmt.Errorf("%w: dataset generation identity", ErrInvalidQueryProjection)
	}
	copy(result[:], decoded)
	return result, nil
}
