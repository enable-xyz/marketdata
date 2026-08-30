package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
)

func validateRawSegmentFilter(filter RawSegmentFilter) error {
	if _, err := decodeDatasetID(filter.DatasetID); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("source_ids", filter.SourceIDs, true, 64); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("channel_ids", filter.ChannelIDs, false, 64); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("instrument_uids", filter.InstrumentUIDs, false, 256); err != nil {
		return err
	}
	if filter.StartReceivedTimeNS < 0 || filter.StartReceivedTimeNS >= filter.EndReceivedTimeNS ||
		filter.Limit < 1 || filter.Limit > MaximumRawSegmentResults || filter.MaxManifestBytes < 1 {
		return fmt.Errorf("%w: raw segment time, result, or manifest byte bound", ErrInvalidQueryProjection)
	}
	return nil
}

// CommittedRawSegments returns only immutable manifest-backed raw publications
// in the selected committed dataset lineage. The half-open request interval is
// tested against the segments' inclusive end coordinates. Descriptor or
// cumulative manifest-byte overflow is an error rather than silent truncation.
func (s *QueryStore) CommittedRawSegments(ctx context.Context, filter RawSegmentFilter) ([]RawSegmentPublication, error) {
	if err := validateRawSegmentFilter(filter); err != nil {
		return nil, err
	}
	generationID, _ := decodeDatasetID(filter.DatasetID)
	if _, err := s.DatasetManifest(ctx, filter.DatasetID); err != nil {
		return nil, err
	}
	rows, err := s.database.Query(ctx, `
SELECT r.raw_segment_id::text, r.source_id::text, r.channel_id, r.epoch_id::text,
       r.receive_time_start_ns, r.receive_time_end_ns, r.ordinal_start, r.ordinal_end,
       r.object_key, r.content_hash, r.byte_length, m.manifest_version,
       m.manifest_hash, m.manifest_bytes, r.state::text
FROM dataset_generation AS g
JOIN dataset_partition AS p ON p.dataset_partition_id = g.dataset_partition_id
JOIN dataset_partition_segment AS l ON l.dataset_partition_id = g.dataset_partition_id
JOIN raw_segment AS r ON r.raw_segment_id = l.raw_segment_id
JOIN raw_segment_manifest AS m ON m.raw_segment_id = r.raw_segment_id
WHERE g.generation_id = $1
  AND g.state = 'committed' AND g.committed_at IS NOT NULL
  AND p.state = 'committed' AND p.committed_at IS NOT NULL
  AND r.state = 'committed' AND r.committed_at IS NOT NULL
  AND r.source_id::text = ANY($2::text[])
  AND (COALESCE(cardinality($3::text[]), 0) = 0 OR r.channel_id = ANY($3::text[]))
  AND r.receive_time_start_ns < $6
  AND r.receive_time_end_ns >= $5
  AND (
      COALESCE(cardinality($4::text[]), 0) = 0 OR EXISTS (
          SELECT 1 FROM dataset_coverage AS c
          WHERE c.dataset_partition_id = g.dataset_partition_id
            AND c.source_id = r.source_id AND c.channel_id = r.channel_id
            AND c.instrument_uid::text = ANY($4::text[])
            AND c.range_start_ns < $6 AND c.range_end_ns > $5
      )
  )
ORDER BY r.receive_time_start_ns, r.source_id, r.epoch_id, r.ordinal_start, r.raw_segment_id
LIMIT $7
`, generationID[:], filter.SourceIDs, filter.ChannelIDs, filter.InstrumentUIDs,
		filter.StartReceivedTimeNS, filter.EndReceivedTimeNS, filter.Limit+1)
	if err != nil {
		return nil, s.unavailable("query committed raw segments", err)
	}
	return s.scanCommittedRawSegments(rows, filter)
}

type rawSegmentRows interface {
	Close()
	Err() error
	Next() bool
	Scan(...any) error
}

func (s *QueryStore) scanCommittedRawSegments(rows rawSegmentRows, filter RawSegmentFilter) ([]RawSegmentPublication, error) {
	defer rows.Close()
	result := make([]RawSegmentPublication, 0, min(filter.Limit, 1024))
	var manifestBytes int64
	for rows.Next() {
		var value RawSegmentPublication
		var ordinalStart, ordinalEnd int64
		var contentHash, manifestHash []byte
		var manifestVersion int16
		if err := rows.Scan(&value.SegmentID, &value.SourceID, &value.ChannelID, &value.EpochID,
			&value.ReceivedStartNS, &value.ReceivedEndNS, &ordinalStart, &ordinalEnd,
			&value.ObjectKey, &contentHash, &value.ByteLength, &manifestVersion,
			&manifestHash, &value.ManifestBytes, &value.State); err != nil {
			return nil, s.unavailable("scan committed raw segment", err)
		}
		if int64(len(value.ManifestBytes)) > filter.MaxManifestBytes-manifestBytes {
			return nil, fmt.Errorf("%w: committed raw manifests exceed %d bytes", ErrQueryBound, filter.MaxManifestBytes)
		}
		manifestBytes += int64(len(value.ManifestBytes))
		if len(result) == filter.Limit {
			return nil, fmt.Errorf("%w: committed raw segments exceed %d", ErrQueryBound, filter.Limit)
		}
		if ordinalStart < 0 || ordinalEnd < ordinalStart || manifestVersion <= 0 ||
			len(contentHash) != sha256.Size || len(manifestHash) != sha256.Size ||
			value.State != RawSegmentCommitted || len(value.ManifestBytes) == 0 {
			s.ready.Store(false)
			return nil, fmt.Errorf("%w: malformed committed raw segment", ErrInvalidQueryProjection)
		}
		value.OrdinalStart, value.OrdinalEnd = uint64(ordinalStart), uint64(ordinalEnd)
		value.ManifestVersion = uint16(manifestVersion)
		copy(value.ContentSHA256[:], contentHash)
		copy(value.ManifestSHA256[:], manifestHash)
		if sha256.Sum256(value.ManifestBytes) != value.ManifestSHA256 {
			s.ready.Store(false)
			return nil, fmt.Errorf("%w: raw manifest hash mismatch", ErrInvalidQueryProjection)
		}
		value.ManifestBytes = bytes.Clone(value.ManifestBytes)
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate committed raw segments", err)
	}
	return result, nil
}

func validateReferenceFilter(filter ReferenceFilter) error {
	if _, err := decodeDatasetID(filter.DatasetID); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("source_ids", filter.SourceIDs, true, 64); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("channel_ids", filter.ChannelIDs, false, 64); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("instrument_uids", filter.InstrumentUIDs, false, 256); err != nil {
		return err
	}
	if filter.StartReceivedTimeNS < 0 || filter.StartReceivedTimeNS >= filter.EndReceivedTimeNS ||
		filter.Limit < 1 || filter.Limit > MaximumDatasetCoverage {
		return fmt.Errorf("%w: reference time or result bound", ErrInvalidQueryProjection)
	}
	return nil
}

// References returns the tuple evidence attached to one committed generation.
// Each result slice is independently ordered by globally unique ID, matching
// the warehouse query adapter's established validation contract.
func (s *QueryStore) References(ctx context.Context, filter ReferenceFilter) ([]CoverageReferenceProjection, []GapReferenceProjection, error) {
	if err := validateReferenceFilter(filter); err != nil {
		return nil, nil, err
	}
	generationID, _ := decodeDatasetID(filter.DatasetID)
	if _, err := s.DatasetManifest(ctx, filter.DatasetID); err != nil {
		return nil, nil, err
	}
	coverageRows, err := s.database.Query(ctx, `
SELECT c.coverage_id::text, c.source_id::text, c.channel_id,
       COALESCE(c.instrument_uid::text, ''), c.range_start_ns, c.range_end_ns, c.coverage_state
FROM dataset_generation AS g
JOIN dataset_coverage AS c ON c.dataset_partition_id = g.dataset_partition_id
WHERE g.generation_id = $1 AND g.state = 'committed' AND g.committed_at IS NOT NULL
  AND c.source_id::text = ANY($2::text[])
  AND (COALESCE(cardinality($3::text[]), 0) = 0 OR c.channel_id = ANY($3::text[]))
  AND (COALESCE(cardinality($4::text[]), 0) = 0 OR c.instrument_uid::text = ANY($4::text[]))
  AND c.range_start_ns < $6 AND c.range_end_ns > $5
ORDER BY c.coverage_id
LIMIT $7
`, generationID[:], filter.SourceIDs, filter.ChannelIDs, filter.InstrumentUIDs,
		filter.StartReceivedTimeNS, filter.EndReceivedTimeNS, filter.Limit+1)
	if err != nil {
		return nil, nil, s.unavailable("query coverage references", err)
	}
	coverage := make([]CoverageReferenceProjection, 0, 256)
	for coverageRows.Next() {
		var value CoverageReferenceProjection
		if err := coverageRows.Scan(&value.ID, &value.Tuple.SourceID, &value.Tuple.ChannelID,
			&value.Tuple.InstrumentUID, &value.StartReceivedTimeNS, &value.EndReceivedTimeNS, &value.State); err != nil {
			coverageRows.Close()
			return nil, nil, s.unavailable("scan coverage reference", err)
		}
		if len(coverage) == filter.Limit {
			coverageRows.Close()
			return nil, nil, fmt.Errorf("%w: coverage references exceed %d", ErrQueryBound, filter.Limit)
		}
		coverage = append(coverage, value)
	}
	coverageErr := coverageRows.Err()
	coverageRows.Close()
	if coverageErr != nil {
		return nil, nil, s.unavailable("iterate coverage references", coverageErr)
	}

	gapRows, err := s.database.Query(ctx, `
SELECT gap.gap_id::text, gap.source_id::text, gap.channel_id,
       COALESCE(gap.instrument_uid::text, ''), gap.range_start_ns, gap.range_end_ns,
       gap.detection_rule
FROM dataset_generation AS g
JOIN gap ON g.dataset_family = ANY(gap.affected_families)
WHERE g.generation_id = $1 AND g.state = 'committed' AND g.committed_at IS NOT NULL
  AND gap.source_id::text = ANY($2::text[])
  AND (COALESCE(cardinality($3::text[]), 0) = 0 OR gap.channel_id = ANY($3::text[]))
  AND (COALESCE(cardinality($4::text[]), 0) = 0 OR gap.instrument_uid::text = ANY($4::text[]))
  AND gap.range_start_ns < $6 AND gap.range_end_ns > $5
ORDER BY gap.gap_id
LIMIT $7
`, generationID[:], filter.SourceIDs, filter.ChannelIDs, filter.InstrumentUIDs,
		filter.StartReceivedTimeNS, filter.EndReceivedTimeNS, filter.Limit+1)
	if err != nil {
		return nil, nil, s.unavailable("query gap references", err)
	}
	defer gapRows.Close()
	gaps := make([]GapReferenceProjection, 0, 64)
	for gapRows.Next() {
		var value GapReferenceProjection
		if err := gapRows.Scan(&value.ID, &value.Tuple.SourceID, &value.Tuple.ChannelID,
			&value.Tuple.InstrumentUID, &value.StartReceivedTimeNS, &value.EndReceivedTimeNS, &value.Kind); err != nil {
			return nil, nil, s.unavailable("scan gap reference", err)
		}
		if len(gaps) == filter.Limit {
			return nil, nil, fmt.Errorf("%w: gap references exceed %d", ErrQueryBound, filter.Limit)
		}
		gaps = append(gaps, value)
	}
	if err := gapRows.Err(); err != nil {
		return nil, nil, s.unavailable("iterate gap references", err)
	}
	if len(coverage)+len(gaps) > MaximumDatasetCoverage {
		return nil, nil, fmt.Errorf("%w: combined references exceed %d", ErrQueryBound, MaximumDatasetCoverage)
	}
	return coverage, gaps, nil
}
