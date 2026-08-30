package catalog

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
)

func (s *QueryStore) Sources(ctx context.Context) ([]SourceProjection, error) {
	rows, err := s.database.Query(ctx, `
SELECT source_id::text, venue, product_family, api_family, environment, lifecycle_state::text
FROM source
ORDER BY venue COLLATE "C", product_family COLLATE "C", api_family COLLATE "C",
         environment COLLATE "C", source_id
LIMIT $1
`, MaximumMetadataResults+1)
	if err != nil {
		return nil, s.unavailable("query sources", err)
	}
	defer rows.Close()
	result := make([]SourceProjection, 0, 256)
	for rows.Next() {
		var value SourceProjection
		if err := rows.Scan(&value.SourceID, &value.Venue, &value.ProductFamily, &value.APIFamily,
			&value.Environment, &value.Lifecycle); err != nil {
			return nil, s.unavailable("scan source projection", err)
		}
		if len(result) == MaximumMetadataResults {
			return nil, fmt.Errorf("%w: sources exceed %d", ErrQueryBound, MaximumMetadataResults)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate sources", err)
	}
	return result, nil
}

func (s *QueryStore) Instruments(ctx context.Context) ([]InstrumentProjection, error) {
	rows, err := s.database.Query(ctx, `
SELECT i.instrument_uid::text, i.source_id::text, i.native_id, i.listing_epoch,
       v.lifecycle_state::text, COALESCE(v.base_asset, ''), COALESCE(v.quote_asset, ''),
       COALESCE(v.margin_asset, ''), COALESCE(v.settlement_asset, ''),
       v.instrument_kind, v.multiplier::text
FROM instrument AS i
JOIN instrument_version AS v ON v.instrument_uid = i.instrument_uid
WHERE v.valid_to IS NULL
ORDER BY i.source_id, i.native_id COLLATE "C", i.listing_epoch, i.instrument_uid
LIMIT $1
`, MaximumMetadataResults+1)
	if err != nil {
		return nil, s.unavailable("query instruments", err)
	}
	defer rows.Close()
	result := make([]InstrumentProjection, 0, 1024)
	for rows.Next() {
		var value InstrumentProjection
		if err := rows.Scan(&value.InstrumentUID, &value.SourceID, &value.NativeID, &value.ListingGeneration,
			&value.Lifecycle, &value.BaseAsset, &value.QuoteAsset, &value.MarginAsset,
			&value.SettlementAsset, &value.Kind, &value.Multiplier); err != nil {
			return nil, s.unavailable("scan instrument projection", err)
		}
		if len(result) == MaximumMetadataResults {
			return nil, fmt.Errorf("%w: instruments exceed %d", ErrQueryBound, MaximumMetadataResults)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate instruments", err)
	}
	return result, nil
}

func (s *QueryStore) Coverage(ctx context.Context) ([]CoverageProjection, error) {
	rows, err := s.database.Query(ctx, `
SELECT c.coverage_id::text, c.source_id::text, c.channel_id,
       COALESCE(c.instrument_uid::text, ''), c.range_start_ns, c.range_end_ns,
       c.coverage_state, encode(g.catalog_snapshot_hash, 'hex'), encode(g.generation_id, 'hex')
FROM dataset_coverage AS c
JOIN dataset_generation AS g ON g.dataset_partition_id = c.dataset_partition_id
JOIN dataset_partition AS p ON p.dataset_partition_id = c.dataset_partition_id
WHERE g.state = 'committed' AND g.committed_at IS NOT NULL
  AND p.state = 'committed' AND p.committed_at IS NOT NULL
ORDER BY c.source_id, c.channel_id COLLATE "C", COALESCE(c.instrument_uid::text, ''),
         c.range_start_ns, c.range_end_ns, c.coverage_id, g.generation_id
LIMIT $1
`, MaximumMetadataResults+1)
	if err != nil {
		return nil, s.unavailable("query coverage", err)
	}
	defer rows.Close()
	result := make([]CoverageProjection, 0, 1024)
	for rows.Next() {
		var value CoverageProjection
		if err := rows.Scan(&value.ID, &value.Tuple.SourceID, &value.Tuple.ChannelID,
			&value.Tuple.InstrumentUID, &value.StartReceivedTimeNS, &value.EndReceivedTimeNS,
			&value.State, &value.CatalogSnapshotID, &value.DatasetID); err != nil {
			return nil, s.unavailable("scan coverage projection", err)
		}
		if len(result) == MaximumMetadataResults {
			return nil, fmt.Errorf("%w: coverage exceeds %d", ErrQueryBound, MaximumMetadataResults)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate coverage", err)
	}
	return result, nil
}

type incidentTupleProjection struct {
	SourceID          string `json:"source_id"`
	Source            string `json:"source"`
	ChannelID         string `json:"channel_id"`
	Channel           string `json:"channel"`
	InstrumentUID     string `json:"instrument_uid"`
	Instrument        string `json:"instrument"`
	Kind              string `json:"kind"`
	GapRefID          string `json:"gap_ref_id"`
	GapID             string `json:"gap_id"`
	CatalogSnapshotID string `json:"catalog_snapshot_id"`
}

func (s *QueryStore) Incidents(ctx context.Context) ([]IncidentProjection, error) {
	rows, err := s.database.Query(ctx, `
SELECT incident_id::text, annotation, affected_tuples, range_start_ns, range_end_ns
FROM incident
ORDER BY COALESCE(range_start_ns, -1), COALESCE(range_end_ns, -1), incident_id
LIMIT $1
`, MaximumMetadataResults+1)
	if err != nil {
		return nil, s.unavailable("query incidents", err)
	}
	defer rows.Close()
	result := make([]IncidentProjection, 0, 128)
	for rows.Next() {
		var id, annotation string
		var encoded []byte
		var start, end *int64
		if err := rows.Scan(&id, &annotation, &encoded, &start, &end); err != nil {
			return nil, s.unavailable("scan incident projection", err)
		}
		if start == nil || end == nil || *start >= *end {
			s.ready.Store(false)
			return nil, fmt.Errorf("%w: incident %s has no representable half-open range", ErrInvalidQueryProjection, id)
		}
		var tuples []incidentTupleProjection
		if err := json.Unmarshal(encoded, &tuples); err != nil {
			s.ready.Store(false)
			return nil, fmt.Errorf("%w: decode incident %s tuples: %v", ErrInvalidQueryProjection, id, err)
		}
		if len(tuples) == 0 || len(result)+len(tuples) > MaximumMetadataResults {
			return nil, fmt.Errorf("%w: incident tuple count", ErrQueryBound)
		}
		for _, tuple := range tuples {
			value := IncidentProjection{
				ID: id, StartReceivedTimeNS: *start, EndReceivedTimeNS: *end,
				Kind: tuple.Kind, GapRefID: tuple.GapRefID, CatalogSnapshotID: tuple.CatalogSnapshotID,
				Tuple: TupleProjection{SourceID: tuple.SourceID, ChannelID: tuple.ChannelID, InstrumentUID: tuple.InstrumentUID},
			}
			if value.Tuple.SourceID == "" {
				value.Tuple.SourceID = tuple.Source
			}
			if value.Tuple.ChannelID == "" {
				value.Tuple.ChannelID = tuple.Channel
			}
			if value.Tuple.InstrumentUID == "" {
				value.Tuple.InstrumentUID = tuple.Instrument
			}
			if value.Kind == "" {
				value.Kind = annotation
			}
			if value.GapRefID == "" {
				value.GapRefID = tuple.GapID
			}
			result = append(result, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate incidents", err)
	}
	slices.SortFunc(result, func(left, right IncidentProjection) int {
		for _, pair := range [][2]string{{left.Tuple.SourceID, right.Tuple.SourceID}, {left.Tuple.ChannelID, right.Tuple.ChannelID},
			{left.Tuple.InstrumentUID, right.Tuple.InstrumentUID}} {
			if pair[0] < pair[1] {
				return -1
			}
			if pair[0] > pair[1] {
				return 1
			}
		}
		if left.StartReceivedTimeNS < right.StartReceivedTimeNS {
			return -1
		}
		if left.StartReceivedTimeNS > right.StartReceivedTimeNS {
			return 1
		}
		if left.ID < right.ID {
			return -1
		}
		if left.ID > right.ID {
			return 1
		}
		return 0
	})
	return result, nil
}

func (s *QueryStore) Datasets(ctx context.Context) ([]DatasetProjection, error) {
	rows, err := s.database.Query(ctx, `
SELECT encode(g.generation_id, 'hex'), g.dataset_family,
       encode(g.catalog_snapshot_hash, 'hex'), g.schema_name, g.schema_version, true
FROM dataset_generation AS g
JOIN dataset_partition AS p ON p.dataset_partition_id = g.dataset_partition_id
JOIN dataset_manifest AS m ON m.dataset_partition_id = g.dataset_partition_id
WHERE g.state = 'committed' AND g.committed_at IS NOT NULL
  AND p.state = 'committed' AND p.committed_at IS NOT NULL
ORDER BY g.dataset_family COLLATE "C", g.generation_id
LIMIT $1
`, MaximumMetadataResults+1)
	if err != nil {
		return nil, s.unavailable("query datasets", err)
	}
	defer rows.Close()
	result := make([]DatasetProjection, 0, 128)
	for rows.Next() {
		var value DatasetProjection
		var schemaVersion int16
		if err := rows.Scan(&value.DatasetID, &value.Family, &value.CatalogSnapshotID,
			&value.SchemaName, &schemaVersion, &value.Committed); err != nil {
			return nil, s.unavailable("scan dataset projection", err)
		}
		if schemaVersion <= 0 {
			s.ready.Store(false)
			return nil, fmt.Errorf("%w: dataset %s schema version", ErrInvalidQueryProjection, value.DatasetID)
		}
		value.SchemaVersion = uint16(schemaVersion)
		if _, err := hex.DecodeString(value.DatasetID); err != nil {
			s.ready.Store(false)
			return nil, fmt.Errorf("%w: dataset identity", ErrInvalidQueryProjection)
		}
		if len(result) == MaximumMetadataResults {
			return nil, fmt.Errorf("%w: datasets exceed %d", ErrQueryBound, MaximumMetadataResults)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate datasets", err)
	}
	return result, nil
}
