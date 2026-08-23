package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/jackc/pgx/v5"
)

type QualityStore struct {
	database CatalogDatabase
}

func NewQualityStore(database CatalogDatabase) (*QualityStore, error) {
	if database == nil {
		return nil, errors.New("catalog: quality database is required")
	}
	return &QualityStore{database: database}, nil
}

func (s *QualityStore) RecordOpportunity(ctx context.Context, opportunity quality.Opportunity) (err error) {
	if err := opportunity.Validate(); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("catalog: begin opportunity record: %w", err)
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	state := "scheduled"
	var terminalTime any
	var outcome any
	var evidence any
	if opportunity.Terminal {
		state = physicalOpportunityState(opportunity.TerminalOutcome)
		terminalTime = opportunity.TerminalTimeNS
		outcome = opportunity.TerminalOutcome.String()
		evidence = []byte(opportunity.TerminalEvidence)
	}
	command, err := tx.Exec(ctx, `
INSERT INTO opportunity (
    opportunity_id, ledger_partition, source_id, channel_id, instrument_uid,
    opportunity_kind, expectation_v1, expected_time_ns, window_start_ns, window_end_ns,
    request_identity, state, terminal_time_ns, terminal_outcome, terminal_outcome_v1,
    created_at, created_time_ns
) VALUES (
    $1, $2, $3, $4, NULLIF($5, '')::uuid,
    $6::text, $6::catalog_opportunity_expectation_v1, $7, $8, $9,
    $10::jsonb, $11, $12, $13::jsonb, $14,
    $15, $16
)
ON CONFLICT (opportunity_id) DO NOTHING
`, opportunity.OpportunityID, opportunity.LedgerPartition, opportunity.SourceID, opportunity.ChannelID,
		opportunity.InstrumentUID, opportunity.Expectation.String(), opportunity.ExpectedTimeNS,
		opportunity.WindowStartNS, opportunity.WindowEndNS, []byte(opportunity.Scope), state,
		terminalTime, evidence, outcome, time.Unix(0, opportunity.CreatedTimeNS).UTC(), opportunity.CreatedTimeNS)
	if err != nil {
		return fmt.Errorf("catalog: insert opportunity: %w", err)
	}
	if command.RowsAffected() == 0 {
		existing, err := readOpportunity(ctx, tx.QueryRow(ctx,
			opportunitySelect+" WHERE opportunity_id = $1 FOR UPDATE", opportunity.OpportunityID))
		if err != nil {
			return fmt.Errorf("catalog: read opportunity conflict: %w", err)
		}
		if opportunitiesEqual(existing, opportunity) {
			return tx.Commit(ctx)
		}
		if !opportunitySchedulingEqual(existing, opportunity) || existing.Terminal || !opportunity.Terminal {
			return fmt.Errorf("%w: %s", quality.ErrOpportunityConflict, opportunity.OpportunityID)
		}
		updated, err := tx.Exec(ctx, `
UPDATE opportunity
SET state = $2, terminal_time_ns = $3, terminal_outcome = $4::jsonb, terminal_outcome_v1 = $5
WHERE opportunity_id = $1 AND terminal_time_ns IS NULL
`, opportunity.OpportunityID, physicalOpportunityState(opportunity.TerminalOutcome), opportunity.TerminalTimeNS,
			[]byte(opportunity.TerminalEvidence), opportunity.TerminalOutcome.String())
		if err != nil {
			return fmt.Errorf("catalog: terminalize opportunity: %w", err)
		}
		if updated.RowsAffected() != 1 {
			return fmt.Errorf("%w: terminal transition raced", quality.ErrOpportunityConflict)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: commit opportunity record: %w", err)
	}
	return nil
}
func (s *QualityStore) CurrentOpportunities(ctx context.Context, partition string) (rows []quality.Opportunity, err error) {
	if partition == "" {
		return nil, fmt.Errorf("%w: ledger partition", quality.ErrInvalidOpportunity)
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	queryRows, err := tx.Query(ctx, opportunitySelect+`
WHERE o.ledger_partition = $1
  AND NOT EXISTS (
      SELECT 1
      FROM opportunity_spill_row sr
      JOIN opportunity_spill spill ON spill.opportunity_spill_id = sr.opportunity_spill_id
      WHERE sr.opportunity_id = o.opportunity_id AND spill.state = 'committed'
  )
ORDER BY o.opportunity_id
`, partition)
	if err != nil {
		return nil, err
	}
	defer queryRows.Close()
	for queryRows.Next() {
		row, err := readOpportunity(ctx, queryRows)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if err := queryRows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *QualityStore) BeginOpportunitySpill(ctx context.Context, request quality.SpillRequest) (generation quality.SpillGeneration, err error) {
	if err := request.Validate(); err != nil {
		return generation, err
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return generation, err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "opportunity-spill:"+request.Partition); err != nil {
		return generation, err
	}
	var existingPartition string
	var existingCutoff int64
	var existingMaximum int
	var existingCatalogSnapshot, existingMapperSet []byte
	err = tx.QueryRow(ctx, `
SELECT ledger_partition, requested_through_time_ns, requested_max_rows,
       catalog_snapshot_hash, mapper_set_hash
FROM opportunity_spill
WHERE opportunity_spill_id = $1
FOR UPDATE
`, request.GenerationID).Scan(&existingPartition, &existingCutoff, &existingMaximum,
		&existingCatalogSnapshot, &existingMapperSet)
	if err == nil {
		if existingPartition != request.Partition || existingCutoff != request.ThroughTimeNS ||
			existingMaximum != request.MaximumRows ||
			!bytes.Equal(existingCatalogSnapshot, request.CatalogSnapshotHash[:]) ||
			!bytes.Equal(existingMapperSet, request.MapperSetHash[:]) {
			return generation, fmt.Errorf("%w: generation request changed", quality.ErrInvalidSpill)
		}
		generation, err = loadOpportunitySpill(ctx, tx, request.GenerationID, false)
		if err != nil {
			return generation, err
		}
		if err := tx.Commit(ctx); err != nil {
			return generation, err
		}
		return generation, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return generation, err
	}

	candidateRows, err := tx.Query(ctx, opportunitySelect+`
WHERE o.ledger_partition = $1
  AND o.terminal_time_ns IS NOT NULL
  AND o.terminal_time_ns <= $2
  AND NOT EXISTS (
      SELECT 1
      FROM opportunity_spill_row sr
      JOIN opportunity_spill claim ON claim.opportunity_spill_id = sr.opportunity_spill_id
      WHERE sr.opportunity_id = o.opportunity_id
        AND sr.ledger_partition = o.ledger_partition
        AND claim.state IN ('pending', 'committed')
  )
ORDER BY o.terminal_time_ns, o.opportunity_id
LIMIT $3
`, request.Partition, request.ThroughTimeNS, request.MaximumRows)
	if err != nil {
		return generation, err
	}
	for candidateRows.Next() {
		row, readErr := readOpportunity(ctx, candidateRows)
		if readErr != nil {
			candidateRows.Close()
			return generation, readErr
		}
		if len(generation.Rows) > 0 {
			firstDate := time.Unix(0, generation.Rows[0].TerminalTimeNS).UTC().Format("2006-01-02")
			if time.Unix(0, row.TerminalTimeNS).UTC().Format("2006-01-02") != firstDate {
				break
			}
		}
		generation.Rows = append(generation.Rows, row)
	}
	candidateRows.Close()
	if err := candidateRows.Err(); err != nil {
		return generation, err
	}
	if len(generation.Rows) == 0 {
		return generation, quality.ErrNoSpillRows
	}
	generation, err = quality.NewSpillGeneration(request, generation.Rows)
	if err != nil {
		return generation, err
	}
	_, err = tx.Exec(ctx, `
INSERT INTO opportunity_spill (
    opportunity_spill_id, ledger_partition,
    boundary_from_time_ns, boundary_from_opportunity_id,
    boundary_through_time_ns, boundary_through_opportunity_id,
    dataset_partition_id, parquet_manifest_hash, state, committed_at,
    generation_fingerprint, row_count, parquet_manifest_key,
    requested_through_time_ns, requested_max_rows,
    catalog_snapshot_hash, mapper_set_hash, quarantine_reason
) VALUES (
    $1, $2, $3, $4, $5, $6,
    NULL, NULL, 'pending', NULL, $7, $8, NULL, $9, $10, $11, $12, NULL
)
`, generation.GenerationID, generation.Partition, generation.From.TerminalTimeNS, generation.From.OpportunityID,
		generation.Through.TerminalTimeNS, generation.Through.OpportunityID, generation.Fingerprint[:],
		len(generation.RowIdentities), request.ThroughTimeNS, request.MaximumRows,
		request.CatalogSnapshotHash[:], request.MapperSetHash[:])
	if err != nil {
		return generation, fmt.Errorf("catalog: insert spill generation: %w", err)
	}
	for ordinal, identity := range generation.RowIdentities {
		if _, err := tx.Exec(ctx, `
INSERT INTO opportunity_spill_row (
    opportunity_spill_id, ledger_partition, row_ordinal, opportunity_id, terminal_time_ns, logical_hash
) VALUES ($1, $2, $3, $4, $5, $6)
`, generation.GenerationID, generation.Partition, ordinal, identity.OpportunityID, identity.TerminalTimeNS, identity.LogicalHash[:]); err != nil {
			return generation, fmt.Errorf("catalog: insert spill row membership: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return generation, err
	}
	return generation, nil
}

func (s *QualityStore) OpportunitySpill(ctx context.Context, generationID string) (generation quality.SpillGeneration, err error) {
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return generation, err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	generation, err = loadOpportunitySpill(ctx, tx, generationID, false)
	if err != nil {
		return generation, err
	}
	if err := tx.Commit(ctx); err != nil {
		return generation, err
	}
	return generation, nil
}

func (s *QualityStore) CommitOpportunitySpill(ctx context.Context, generation quality.SpillGeneration, archive quality.ArchiveCommit) (err error) {
	if err := generation.Validate(); err != nil {
		return err
	}
	if err := archive.Validate(generation); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	current, err := loadOpportunitySpill(ctx, tx, generation.GenerationID, true)
	if err != nil {
		return err
	}
	if current.Fingerprint != generation.Fingerprint || !slices.Equal(current.RowIdentities, generation.RowIdentities) {
		return fmt.Errorf("%w: generation membership changed", quality.ErrInvalidSpill)
	}
	if current.State == quality.SpillCommitted {
		if current.Archive == nil || *current.Archive != archive {
			return fmt.Errorf("%w: committed archive conflict", quality.ErrInvalidSpill)
		}
		return tx.Commit(ctx)
	}
	if current.State != quality.SpillPending {
		return fmt.Errorf("%w: generation is %s", quality.ErrInvalidSpill, current.State)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO dataset_partition (
    dataset_partition_id, dataset_family, dataset_version, partition_key,
    range_start_ns, range_end_ns, input_segment_set_hash, catalog_snapshot_hash,
    mapper_set_hash, logical_hash, physical_hash, object_key, canonical, state,
    committed_at
) VALUES (
    $1, 'opportunity', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true, 'committed', now()
)
ON CONFLICT (dataset_partition_id) DO NOTHING
`, archive.DatasetPartitionID, archive.DatasetVersion, archive.PartitionKey, archive.RangeStartNS, archive.RangeEndNS,
		archive.InputSetHash[:], archive.CatalogSnapshotHash[:], archive.MapperSetHash[:], archive.LogicalHash[:], archive.PhysicalHash[:], archive.ParquetPath)
	if err != nil {
		return fmt.Errorf("catalog: commit opportunity dataset partition: %w", err)
	}
	var datasetVersion, partitionKey, objectKey, state string
	var rangeStartNS, rangeEndNS int64
	var inputSetHash, catalogSnapshotHash, mapperSetHash, logicalHash, physicalHash []byte
	var canonical bool
	if err := tx.QueryRow(ctx, `
SELECT dataset_version, partition_key, range_start_ns, range_end_ns,
       input_segment_set_hash, catalog_snapshot_hash, mapper_set_hash,
       logical_hash, physical_hash, object_key, canonical, state::text
FROM dataset_partition
WHERE dataset_partition_id = $1
`, archive.DatasetPartitionID).Scan(&datasetVersion, &partitionKey, &rangeStartNS, &rangeEndNS,
		&inputSetHash, &catalogSnapshotHash, &mapperSetHash, &logicalHash, &physicalHash,
		&objectKey, &canonical, &state); err != nil {
		return fmt.Errorf("catalog: verify opportunity dataset partition: %w", err)
	}
	if datasetVersion != archive.DatasetVersion || partitionKey != archive.PartitionKey ||
		rangeStartNS != archive.RangeStartNS || rangeEndNS != archive.RangeEndNS ||
		!bytes.Equal(inputSetHash, archive.InputSetHash[:]) ||
		!bytes.Equal(catalogSnapshotHash, archive.CatalogSnapshotHash[:]) ||
		!bytes.Equal(mapperSetHash, archive.MapperSetHash[:]) ||
		!bytes.Equal(logicalHash, archive.LogicalHash[:]) ||
		!bytes.Equal(physicalHash, archive.PhysicalHash[:]) ||
		objectKey != archive.ParquetPath || !canonical || state != "committed" {
		return fmt.Errorf("%w: dataset partition identity conflict", quality.ErrInvalidSpill)
	}
	command, err := tx.Exec(ctx, `
UPDATE opportunity_spill
SET dataset_partition_id = $2,
    parquet_manifest_hash = $3,
    parquet_manifest_key = $4,
    state = 'committed',
    committed_at = now()
WHERE opportunity_spill_id = $1 AND state = 'pending'
`, generation.GenerationID, archive.DatasetPartitionID, archive.ManifestHash[:], archive.ManifestPath)
	if err != nil {
		return fmt.Errorf("catalog: commit opportunity spill boundary: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: spill generation was not pending", quality.ErrInvalidSpill)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (s *QualityStore) DeleteCommittedOpportunityRows(ctx context.Context, generationID string) (err error) {
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	var state string
	if err := tx.QueryRow(ctx, `SELECT state::text FROM opportunity_spill WHERE opportunity_spill_id = $1 FOR UPDATE`, generationID).Scan(&state); err != nil {
		return err
	}
	if state != string(quality.SpillCommitted) {
		return fmt.Errorf("%w: cannot delete rows for %s spill", quality.ErrInvalidSpill, state)
	}
	_, err = tx.Exec(ctx, `
DELETE FROM opportunity o
USING opportunity_spill_row sr
WHERE sr.opportunity_spill_id = $1
  AND o.opportunity_id = sr.opportunity_id
  AND o.ledger_partition = sr.ledger_partition
  AND o.terminal_time_ns = sr.terminal_time_ns
`, generationID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *QualityStore) QuarantineOpportunitySpill(ctx context.Context, generationID, reason string) (err error) {
	if err := quality.ValidateSpillQuarantineReason(reason); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	var state string
	var existingReason *string
	if err := tx.QueryRow(ctx, `
SELECT state::text, quarantine_reason
FROM opportunity_spill
WHERE opportunity_spill_id = $1
FOR UPDATE
`, generationID).Scan(&state, &existingReason); err != nil {
		return err
	}
	if state == string(quality.SpillQuarantined) {
		if existingReason != nil && *existingReason == reason {
			return tx.Commit(ctx)
		}
		return fmt.Errorf("%w: quarantined generation evidence conflict", quality.ErrInvalidSpill)
	}
	if state != string(quality.SpillPending) {
		return fmt.Errorf("%w: only pending generation may be quarantined", quality.ErrInvalidSpill)
	}
	command, err := tx.Exec(ctx, `
UPDATE opportunity_spill
SET state = 'quarantined', quarantine_reason = $2
WHERE opportunity_spill_id = $1 AND state = 'pending'
`, generationID, reason)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: quarantine transition raced", quality.ErrInvalidSpill)
	}
	return tx.Commit(ctx)
}

func (s *QualityStore) RecordGap(ctx context.Context, gap quality.Gap) (err error) {
	if err := gap.Validate(); err != nil {
		return err
	}
	if gap.State != quality.GapOpen {
		return quality.ErrInvalidGapTransition
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	command, err := tx.Exec(ctx, `
INSERT INTO gap (
    gap_id, source_id, channel_id, instrument_uid, range_start_ns, range_end_ns,
    detection_rule, state, confidence, evidence, detected_at, resolved_at,
    first_good_coordinate, last_good_coordinate, affected_families,
    detected_time_ns, resolved_time_ns
) VALUES ($1, $2, $3, NULLIF($4, '')::uuid, $5, $6, $7, 'open', $8, $9::jsonb,
          $10, NULL, $11::jsonb, $12::jsonb, $13, $14, NULL)
ON CONFLICT (gap_id) DO NOTHING
`, gap.GapID, gap.SourceID, gap.ChannelID, gap.InstrumentUID, gap.RangeStartNS, gap.RangeEndNS,
		gap.DetectionBasis, gap.Confidence, []byte(gap.Evidence), time.Unix(0, gap.DetectedTimeNS).UTC(),
		[]byte(gap.FirstGoodCoordinate), []byte(gap.LastGoodCoordinate), gap.AffectedFamilies, gap.DetectedTimeNS)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		existing, err := readGapIdentity(ctx, tx, gap.GapID)
		if err != nil {
			return err
		}
		if !gapsHaveSameIdentity(existing, gap) {
			return fmt.Errorf("%w: immutable gap conflict", quality.ErrInvalidGap)
		}
	}
	return tx.Commit(ctx)
}

func (s *QualityStore) ResolveGap(ctx context.Context, gapID string, state quality.GapState, resolvedTimeNS int64) (err error) {
	if state == quality.GapBackfilledExplicitly {
		return quality.ErrReservedGapState
	}
	if gapID == "" || resolvedTimeNS < 0 ||
		(state != quality.GapRecoveredCurrentState && state != quality.GapPermanent) {
		return quality.ErrInvalidGapTransition
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	var currentState string
	var detectedTimeNS int64
	if err := tx.QueryRow(ctx, `
SELECT state::text, detected_time_ns
FROM gap WHERE gap_id = $1
FOR UPDATE
`, gapID).Scan(&currentState, &detectedTimeNS); err != nil {
		return err
	}
	if currentState != string(quality.GapOpen) || resolvedTimeNS < detectedTimeNS {
		return quality.ErrInvalidGapTransition
	}
	command, err := tx.Exec(ctx, `
UPDATE gap
SET state = $2, resolved_at = $3, resolved_time_ns = $4
WHERE gap_id = $1 AND state = 'open'
`, gapID, string(state), time.Unix(0, resolvedTimeNS).UTC(), resolvedTimeNS)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return quality.ErrInvalidGapTransition
	}
	return tx.Commit(ctx)
}

func (s *QualityStore) RecordIncident(ctx context.Context, incident quality.Incident) (err error) {
	if err := incident.Validate(); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	var start, end any
	if incident.HasRange() {
		start, end = incident.RangeStartNS(), incident.RangeEndNS()
	}
	command, err := tx.Exec(ctx, `
INSERT INTO incident (
    incident_id, annotation, report_source, affected_tuples,
    range_start_ns, range_end_ns, reported_at, created_at, reported_time_ns, created_time_ns
) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10)
ON CONFLICT (incident_id) DO NOTHING
`, incident.ID(), incident.Annotation(), incident.ReportSource(), []byte(incident.AffectedTuples()), start, end,
		time.Unix(0, incident.ReportedTimeNS()).UTC(), time.Unix(0, incident.CreatedTimeNS()).UTC(),
		incident.ReportedTimeNS(), incident.CreatedTimeNS())
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		var annotation, reportSource string
		var tuples []byte
		var existingStart, existingEnd *int64
		var reportedTimeNS, createdTimeNS int64
		if err := tx.QueryRow(ctx, `
SELECT annotation, report_source, affected_tuples, range_start_ns, range_end_ns,
       reported_time_ns, created_time_ns
FROM incident WHERE incident_id = $1
`, incident.ID()).Scan(&annotation, &reportSource, &tuples, &existingStart, &existingEnd, &reportedTimeNS, &createdTimeNS); err != nil {
			return err
		}
		rangeMatches := (!incident.HasRange() && existingStart == nil && existingEnd == nil) ||
			(incident.HasRange() && existingStart != nil && existingEnd != nil &&
				*existingStart == incident.RangeStartNS() && *existingEnd == incident.RangeEndNS())
		if annotation != incident.Annotation() || reportSource != incident.ReportSource() ||
			!jsonValuesEqual(tuples, incident.AffectedTuples()) || !rangeMatches ||
			reportedTimeNS != incident.ReportedTimeNS() || createdTimeNS != incident.CreatedTimeNS() {
			return quality.ErrInvalidIncident
		}
	}
	return tx.Commit(ctx)
}

func (s *QualityStore) RecordHealthTransition(ctx context.Context, transition quality.HealthTransition) (err error) {
	if err := transition.Validate(); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	command, err := tx.Exec(ctx, `
INSERT INTO source_health_transition (
    transition_id, source_id, channel_id, dimension, from_state, to_state, observed_time_ns, evidence
) VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, $7, $8::jsonb)
ON CONFLICT (transition_id) DO NOTHING
`, transition.TransitionID, transition.SourceID, transition.ChannelID, transition.Dimension,
		string(transition.FromState), string(transition.ToState), transition.ObservedTimeNS, []byte(transition.Evidence))
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		var existing quality.HealthTransition
		var fromState, toState string
		var evidence []byte
		if err := tx.QueryRow(ctx, `
SELECT transition_id::text, source_id::text, COALESCE(channel_id, ''), dimension,
       from_state::text, to_state::text, observed_time_ns, evidence
FROM source_health_transition WHERE transition_id = $1
`, transition.TransitionID).Scan(&existing.TransitionID, &existing.SourceID, &existing.ChannelID,
			&existing.Dimension, &fromState, &toState, &existing.ObservedTimeNS, &evidence); err != nil {
			return err
		}
		existing.FromState, existing.ToState = quality.HealthState(fromState), quality.HealthState(toState)
		existing.Evidence = evidence
		if !healthTransitionsEqual(existing, transition) {
			return fmt.Errorf("%w: immutable health transition conflict", quality.ErrInvalidHealth)
		}
	}
	return tx.Commit(ctx)
}

const opportunitySelect = `
SELECT o.opportunity_id::text, o.ledger_partition, o.source_id::text, o.channel_id,
       COALESCE(o.instrument_uid::text, ''), o.expectation_v1::text,
       o.expected_time_ns, o.window_start_ns, o.window_end_ns, o.request_identity,
       o.terminal_time_ns, o.terminal_outcome_v1::text, o.terminal_outcome, o.created_time_ns
FROM opportunity o`

func (s *QualityStore) RecordSchemaObservation(ctx context.Context, observation quality.SchemaObservation, sampleSegmentID string, sampleOrdinal uint64) (err error) {
	if err := observation.Validate(); err != nil {
		return err
	}
	if sampleSegmentID == "" {
		return quality.ErrInvalidSchemaObservation
	}
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	mapperDisposition := "pending"
	switch observation.MapperDisposition {
	case quality.NormalizationContinue, quality.NormalizationTransition:
		if observation.MapperReleaseID != "" {
			mapperDisposition = "accepted"
		}
	case quality.NormalizationQuarantine:
		mapperDisposition = "quarantined"
	case quality.NormalizationFailClosed:
		mapperDisposition = "rejected"
	}
	command, err := tx.Exec(ctx, `
INSERT INTO schema_observation (
    source_id, channel_id, fingerprint, first_seen_at, last_seen_at,
    observation_count, classification, sample_segment_id, sample_ordinal,
    mapper_disposition, mapper_release_id, first_raw_coordinate,
    last_raw_coordinate, redacted_sample, parser_can_preserve,
    optional_absence_resolved, normalization_disposition
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9,
    $10, NULLIF($11, '')::uuid, $12::jsonb, $13::jsonb, $14::jsonb, $15, $16, $17
)
ON CONFLICT (source_id, channel_id, fingerprint) DO UPDATE
SET last_seen_at = EXCLUDED.last_seen_at,
    observation_count = EXCLUDED.observation_count,
    last_raw_coordinate = EXCLUDED.last_raw_coordinate,
    mapper_disposition = EXCLUDED.mapper_disposition,
    mapper_release_id = EXCLUDED.mapper_release_id,
    normalization_disposition = EXCLUDED.normalization_disposition
WHERE schema_observation.classification = EXCLUDED.classification
  AND schema_observation.parser_can_preserve = EXCLUDED.parser_can_preserve
  AND schema_observation.optional_absence_resolved = EXCLUDED.optional_absence_resolved
  AND schema_observation.first_raw_coordinate = EXCLUDED.first_raw_coordinate
  AND schema_observation.redacted_sample = EXCLUDED.redacted_sample
  AND schema_observation.sample_segment_id = EXCLUDED.sample_segment_id
  AND schema_observation.sample_ordinal = EXCLUDED.sample_ordinal
  AND EXCLUDED.last_seen_at >= schema_observation.last_seen_at
  AND EXCLUDED.observation_count >= schema_observation.observation_count
`, observation.SourceID, observation.ChannelID, observation.Fingerprint[:],
		time.Unix(0, observation.FirstSeenTimeNS).UTC(), time.Unix(0, observation.LastSeenTimeNS).UTC(),
		observation.ObservationCount, string(observation.Classification), sampleSegmentID, sampleOrdinal,
		mapperDisposition, observation.MapperReleaseID, []byte(observation.FirstRawCoordinate),
		[]byte(observation.LastRawCoordinate), []byte(observation.RedactedSample),
		observation.ParserCanPreserve, observation.OptionalAbsenceResolved, string(observation.MapperDisposition))
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return quality.ErrInvalidSchemaObservation
	}
	return tx.Commit(ctx)
}

func (s *QualityStore) RecordCorrection(ctx context.Context, correction quality.Correction) (err error) {
	if err := correction.Validate(); err != nil {
		return err
	}
	input := correction.Input()
	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer rollbackCatalogTx(ctx, tx, &err)
	command, err := tx.Exec(ctx, `
INSERT INTO correction (
    correction_id, old_raw_segment_id, original_gap_id, new_dataset_partition_id,
    mapper_release_id, supersedes_correction_id, reason, lineage, state,
    created_at, committed_at, created_time_ns
) VALUES (
    $1, $2, NULLIF($3, '')::uuid, $4, $5, NULLIF($6, '')::uuid,
    $7, $8::jsonb, 'committed', $9, $9, $10
)
ON CONFLICT (correction_id) DO NOTHING
`, input.CorrectionID, input.OriginalRawSegmentID, input.OriginalGapID, input.ReplacementDatasetID,
		input.MapperReleaseID, input.SupersedesCorrectionID, input.Reason, []byte(input.Lineage),
		time.Unix(0, input.CreatedTimeNS).UTC(), input.CreatedTimeNS)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		existing, err := readCorrection(ctx, tx, input.CorrectionID)
		if err != nil {
			return fmt.Errorf("%w: read correction conflict: %v", quality.ErrInvalidCorrection, err)
		}
		if !correctionInputsEqual(existing, input) {
			return fmt.Errorf("%w: immutable correction conflict", quality.ErrInvalidCorrection)
		}
	}
	return tx.Commit(ctx)
}

type opportunityRowScanner interface{ Scan(...any) error }

func readOpportunity(_ context.Context, row opportunityRowScanner) (quality.Opportunity, error) {
	var result quality.Opportunity
	var expectation string
	var terminalTime *int64
	var outcome *string
	var scope, evidence []byte
	if err := row.Scan(&result.OpportunityID, &result.LedgerPartition, &result.SourceID, &result.ChannelID,
		&result.InstrumentUID, &expectation, &result.ExpectedTimeNS, &result.WindowStartNS,
		&result.WindowEndNS, &scope, &terminalTime, &outcome, &evidence, &result.CreatedTimeNS); err != nil {
		return result, err
	}
	parsedExpectation, err := quality.ParseOpportunityExpectation(expectation)
	if err != nil {
		return result, err
	}
	result.Expectation = parsedExpectation
	canonicalScope, err := quality.CanonicalOpportunityObject(scope)
	if err != nil {
		return result, err
	}
	result.Scope = canonicalScope
	if terminalTime != nil {
		if outcome == nil {
			return result, quality.ErrInvalidOpportunity
		}
		parsedOutcome, err := capture.ParseOpportunityOutcome(*outcome)
		if err != nil {
			return result, err
		}
		result.Terminal = true
		result.TerminalTimeNS = *terminalTime
		result.TerminalOutcome = parsedOutcome
		canonicalEvidence, err := quality.CanonicalOpportunityObject(evidence)
		if err != nil {
			return result, err
		}
		result.TerminalEvidence = canonicalEvidence
	}
	return result, result.Validate()
}

func opportunitiesEqual(left, right quality.Opportunity) bool {
	leftHash, leftErr := left.LogicalHash()
	rightHash, rightErr := right.LogicalHash()
	return leftErr == nil && rightErr == nil && leftHash == rightHash
}

func opportunitySchedulingEqual(left, right quality.Opportunity) bool {
	return left.OpportunityID == right.OpportunityID &&
		left.LedgerPartition == right.LedgerPartition &&
		left.SourceID == right.SourceID &&
		left.ChannelID == right.ChannelID &&
		left.InstrumentUID == right.InstrumentUID &&
		left.Expectation == right.Expectation &&
		left.ExpectedTimeNS == right.ExpectedTimeNS &&
		left.WindowStartNS == right.WindowStartNS &&
		left.WindowEndNS == right.WindowEndNS &&
		bytes.Equal(left.Scope, right.Scope) &&
		left.CreatedTimeNS == right.CreatedTimeNS
}

func readGapIdentity(ctx context.Context, tx pgx.Tx, gapID string) (quality.Gap, error) {
	var gap quality.Gap
	var evidence, first, last []byte
	if err := tx.QueryRow(ctx, `
SELECT gap_id::text, source_id::text, channel_id, COALESCE(instrument_uid::text, ''),
       range_start_ns, range_end_ns, detection_rule, confidence::float8, evidence,
       detected_time_ns, first_good_coordinate, last_good_coordinate, affected_families
FROM gap WHERE gap_id = $1
`, gapID).Scan(&gap.GapID, &gap.SourceID, &gap.ChannelID, &gap.InstrumentUID,
		&gap.RangeStartNS, &gap.RangeEndNS, &gap.DetectionBasis, &gap.Confidence, &evidence,
		&gap.DetectedTimeNS, &first, &last, &gap.AffectedFamilies); err != nil {
		return quality.Gap{}, err
	}
	gap.State = quality.GapOpen
	gap.Evidence, gap.FirstGoodCoordinate, gap.LastGoodCoordinate = evidence, first, last
	return gap, nil
}

func gapsHaveSameIdentity(left, right quality.Gap) bool {
	return left.GapID == right.GapID && left.SourceID == right.SourceID &&
		left.ChannelID == right.ChannelID && left.InstrumentUID == right.InstrumentUID &&
		left.RangeStartNS == right.RangeStartNS && left.RangeEndNS == right.RangeEndNS &&
		left.DetectionBasis == right.DetectionBasis && left.Confidence == right.Confidence &&
		left.DetectedTimeNS == right.DetectedTimeNS &&
		slices.Equal(left.AffectedFamilies, right.AffectedFamilies) &&
		jsonValuesEqual(left.FirstGoodCoordinate, right.FirstGoodCoordinate) &&
		jsonValuesEqual(left.LastGoodCoordinate, right.LastGoodCoordinate) &&
		jsonValuesEqual(left.Evidence, right.Evidence)
}

func healthTransitionsEqual(left, right quality.HealthTransition) bool {
	return left.TransitionID == right.TransitionID && left.SourceID == right.SourceID &&
		left.ChannelID == right.ChannelID && left.Dimension == right.Dimension &&
		left.FromState == right.FromState && left.ToState == right.ToState &&
		left.ObservedTimeNS == right.ObservedTimeNS && jsonValuesEqual(left.Evidence, right.Evidence)
}

func readCorrection(ctx context.Context, tx pgx.Tx, correctionID string) (quality.CorrectionInput, error) {
	var input quality.CorrectionInput
	var lineage []byte
	if err := tx.QueryRow(ctx, `
SELECT correction_id::text, old_raw_segment_id::text, COALESCE(original_gap_id::text, ''),
       new_dataset_partition_id::text, mapper_release_id::text,
       COALESCE(supersedes_correction_id::text, ''), reason, lineage, created_time_ns
FROM correction
WHERE correction_id = $1 AND state = 'committed' AND committed_at IS NOT NULL
`, correctionID).Scan(&input.CorrectionID, &input.OriginalRawSegmentID, &input.OriginalGapID,
		&input.ReplacementDatasetID, &input.MapperReleaseID, &input.SupersedesCorrectionID,
		&input.Reason, &lineage, &input.CreatedTimeNS); err != nil {
		return quality.CorrectionInput{}, err
	}
	input.Lineage = lineage
	return input, nil
}

func correctionInputsEqual(left, right quality.CorrectionInput) bool {
	return left.CorrectionID == right.CorrectionID &&
		left.OriginalRawSegmentID == right.OriginalRawSegmentID &&
		left.OriginalGapID == right.OriginalGapID &&
		left.ReplacementDatasetID == right.ReplacementDatasetID &&
		left.MapperReleaseID == right.MapperReleaseID &&
		left.SupersedesCorrectionID == right.SupersedesCorrectionID &&
		left.Reason == right.Reason && left.CreatedTimeNS == right.CreatedTimeNS &&
		jsonValuesEqual(left.Lineage, right.Lineage)
}

func jsonValuesEqual(left, right []byte) bool {
	var leftValue, rightValue any
	leftDecoder := json.NewDecoder(bytes.NewReader(left))
	leftDecoder.UseNumber()
	rightDecoder := json.NewDecoder(bytes.NewReader(right))
	rightDecoder.UseNumber()
	return leftDecoder.Decode(&leftValue) == nil && rightDecoder.Decode(&rightValue) == nil &&
		reflect.DeepEqual(leftValue, rightValue)
}

func physicalOpportunityState(outcome quality.OpportunityOutcome) string {
	switch outcome {
	case quality.OpportunityOutcomeObserved, quality.OpportunityOutcomeObservedUnchanged:
		return "observed"
	case quality.OpportunityOutcomeSourceStale, quality.OpportunityOutcomeSequenceGap:
		return "missed"
	case quality.OpportunityOutcomeIntentionallyExcluded:
		return "cancelled"
	default:
		return "failed"
	}
}

func loadOpportunitySpill(ctx context.Context, tx pgx.Tx, generationID string, lock bool) (quality.SpillGeneration, error) {
	query := `
SELECT ledger_partition, boundary_from_time_ns, boundary_from_opportunity_id::text,
       boundary_through_time_ns, boundary_through_opportunity_id::text,
       generation_fingerprint, state::text, dataset_partition_id::text,
       parquet_manifest_hash, parquet_manifest_key, catalog_snapshot_hash,
       mapper_set_hash, quarantine_reason
FROM opportunity_spill
WHERE opportunity_spill_id = $1`
	if lock {
		query += " FOR UPDATE"
	}
	var generation quality.SpillGeneration
	generation.GenerationID = generationID
	var fingerprint, catalogSnapshotHash, mapperSetHash []byte
	var state string
	var datasetID, manifestKey, quarantineReason *string
	var manifestHash []byte
	if err := tx.QueryRow(ctx, query, generationID).Scan(&generation.Partition, &generation.From.TerminalTimeNS,
		&generation.From.OpportunityID, &generation.Through.TerminalTimeNS, &generation.Through.OpportunityID,
		&fingerprint, &state, &datasetID, &manifestHash, &manifestKey,
		&catalogSnapshotHash, &mapperSetHash, &quarantineReason); err != nil {
		return generation, err
	}
	if len(fingerprint) != sha256.Size || len(catalogSnapshotHash) != sha256.Size || len(mapperSetHash) != sha256.Size {
		return generation, quality.ErrInvalidSpill
	}
	copy(generation.Fingerprint[:], fingerprint)
	copy(generation.CatalogSnapshotHash[:], catalogSnapshotHash)
	copy(generation.MapperSetHash[:], mapperSetHash)
	generation.State = quality.SpillState(state)
	if quarantineReason != nil {
		generation.QuarantineReason = *quarantineReason
	}
	identityRows, err := tx.Query(ctx, `
SELECT opportunity_id::text, terminal_time_ns, logical_hash
FROM opportunity_spill_row WHERE opportunity_spill_id = $1 ORDER BY row_ordinal
`, generationID)
	if err != nil {
		return generation, err
	}
	for identityRows.Next() {
		var identity quality.SpillRowIdentity
		var logicalHash []byte
		if err := identityRows.Scan(&identity.OpportunityID, &identity.TerminalTimeNS, &logicalHash); err != nil {
			identityRows.Close()
			return generation, err
		}
		if len(logicalHash) != sha256.Size {
			identityRows.Close()
			return generation, quality.ErrInvalidSpill
		}
		copy(identity.LogicalHash[:], logicalHash)
		generation.RowIdentities = append(generation.RowIdentities, identity)
	}
	identityRows.Close()
	if err := identityRows.Err(); err != nil {
		return generation, err
	}
	if generation.State == quality.SpillPending {
		rows, err := tx.Query(ctx, opportunitySelect+`
JOIN opportunity_spill_row sr ON sr.opportunity_id = o.opportunity_id
WHERE sr.opportunity_spill_id = $1
ORDER BY sr.row_ordinal
`, generationID)
		if err != nil {
			return generation, err
		}
		for rows.Next() {
			value, err := readOpportunity(ctx, rows)
			if err != nil {
				rows.Close()
				return generation, err
			}
			generation.Rows = append(generation.Rows, value)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return generation, err
		}
	}
	if generation.State == quality.SpillCommitted {
		if datasetID == nil || manifestKey == nil || len(manifestHash) != sha256.Size {
			return generation, quality.ErrInvalidSpill
		}
		archive := quality.ArchiveCommit{GenerationID: generationID, GenerationFingerprint: generation.Fingerprint,
			DatasetPartitionID: *datasetID, ManifestPath: *manifestKey, RangeStartNS: generation.From.TerminalTimeNS,
			RangeEndNS: generation.Through.TerminalTimeNS}
		copy(archive.ManifestHash[:], manifestHash)
		var input, catalogHash, mapperHash, logical, physical []byte
		if err := tx.QueryRow(ctx, `
SELECT dataset_version, partition_key, input_segment_set_hash, catalog_snapshot_hash,
       mapper_set_hash, logical_hash, physical_hash, object_key
FROM dataset_partition WHERE dataset_partition_id = $1 AND state = 'committed'
`, *datasetID).Scan(&archive.DatasetVersion, &archive.PartitionKey, &input, &catalogHash,
			&mapperHash, &logical, &physical, &archive.ParquetPath); err != nil {
			return generation, err
		}
		for destination, source := range map[*[sha256.Size]byte][]byte{
			&archive.InputSetHash: input, &archive.CatalogSnapshotHash: catalogHash, &archive.MapperSetHash: mapperHash,
			&archive.LogicalHash: logical, &archive.PhysicalHash: physical,
		} {
			if len(source) != sha256.Size {
				return generation, quality.ErrInvalidSpill
			}
			copy(destination[:], source)
		}
		generation.Archive = &archive
	}
	return generation, generation.Validate()
}

func rollbackCatalogTx(ctx context.Context, tx pgx.Tx, result *error) {
	if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
		*result = errors.Join(*result, rollbackErr)
	}
}

var _ quality.OpportunitySpillStore = (*QualityStore)(nil)
