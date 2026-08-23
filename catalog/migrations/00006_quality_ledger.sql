-- +goose Up
CREATE DOMAIN catalog_opportunity_expectation_v1 AS text
    CHECK (VALUE IN (
        'scheduled_rest_poll', 'acknowledgement_deadline', 'heartbeat_deadline',
        'periodic_publication', 'subscription_inventory', 'sequence_interval',
        'metadata_discovery', 'native_file_publication'
    ));
CREATE DOMAIN catalog_opportunity_outcome_v1 AS text
    CHECK (VALUE IN (
        'observed', 'observed_unchanged', 'venue_unavailable', 'source_stale',
        'sequence_gap', 'rate_limited', 'malformed', 'schema_rejected',
        'collector_failed', 'intentionally_excluded', 'unknown'
    ));
CREATE DOMAIN catalog_normalization_disposition_v1 AS text
    CHECK (VALUE IN ('continue', 'transition_table', 'quarantine', 'fail_closed'));
CREATE DOMAIN catalog_health_state_v1 AS text
    CHECK (VALUE IN ('healthy', 'degraded', 'unavailable', 'stale', 'recovering', 'quarantined'));

ALTER TABLE opportunity
    ADD COLUMN expectation_v1 catalog_opportunity_expectation_v1,
    ADD COLUMN terminal_outcome_v1 catalog_opportunity_outcome_v1,
    ADD COLUMN created_time_ns bigint;
UPDATE opportunity
SET expectation_v1 = CASE opportunity_kind
    WHEN 'metadata_sync' THEN 'metadata_discovery'::catalog_opportunity_expectation_v1
    WHEN 'scheduled_rest_poll' THEN 'scheduled_rest_poll'::catalog_opportunity_expectation_v1
    WHEN 'acknowledgement_deadline' THEN 'acknowledgement_deadline'::catalog_opportunity_expectation_v1
    WHEN 'heartbeat_deadline' THEN 'heartbeat_deadline'::catalog_opportunity_expectation_v1
    WHEN 'periodic_publication' THEN 'periodic_publication'::catalog_opportunity_expectation_v1
    WHEN 'subscription_inventory' THEN 'subscription_inventory'::catalog_opportunity_expectation_v1
    WHEN 'sequence_interval' THEN 'sequence_interval'::catalog_opportunity_expectation_v1
    WHEN 'metadata_discovery' THEN 'metadata_discovery'::catalog_opportunity_expectation_v1
    WHEN 'native_file_publication' THEN 'native_file_publication'::catalog_opportunity_expectation_v1
END,
terminal_outcome_v1 = CASE state::text
    WHEN 'observed' THEN 'observed'::catalog_opportunity_outcome_v1
    WHEN 'missed' THEN 'source_stale'::catalog_opportunity_outcome_v1
    WHEN 'failed' THEN 'collector_failed'::catalog_opportunity_outcome_v1
    WHEN 'cancelled' THEN 'intentionally_excluded'::catalog_opportunity_outcome_v1
END;
UPDATE opportunity
SET created_time_ns = floor(extract(epoch FROM created_at) * 1000000000)::bigint;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM opportunity
        WHERE expectation_v1 IS NULL
           OR (terminal_time_ns IS NOT NULL AND terminal_outcome_v1 IS NULL)
    ) THEN
        RAISE EXCEPTION 'legacy opportunity rows require explicit v1 expectation/outcome classification'
            USING ERRCODE = 'check_violation';
    END IF;
END;
$$;
-- +goose StatementEnd
ALTER TABLE opportunity
    ALTER COLUMN expectation_v1 SET NOT NULL,
    ALTER COLUMN created_time_ns SET NOT NULL,
    ADD CONSTRAINT opportunity_created_time_ns_nonnegative CHECK (created_time_ns >= 0),
    ADD CONSTRAINT opportunity_v1_terminal_fields CHECK (
        (terminal_time_ns IS NULL AND terminal_outcome_v1 IS NULL)
        OR (terminal_time_ns IS NOT NULL AND terminal_outcome_v1 IS NOT NULL)
    );
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION catalog_reject_terminal_opportunity_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.terminal_time_ns IS NOT NULL AND (
        OLD.state IS DISTINCT FROM NEW.state OR
        OLD.terminal_time_ns IS DISTINCT FROM NEW.terminal_time_ns OR
        OLD.terminal_outcome IS DISTINCT FROM NEW.terminal_outcome OR
        OLD.terminal_outcome_v1 IS DISTINCT FROM NEW.terminal_outcome_v1
    ) THEN
        RAISE EXCEPTION 'terminal opportunity % outcome is immutable', OLD.opportunity_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
DROP TRIGGER opportunity_identity_immutable ON opportunity;
CREATE TRIGGER opportunity_identity_immutable
BEFORE UPDATE OF opportunity_id, ledger_partition, source_id, channel_id, instrument_uid,
    opportunity_kind, expectation_v1, expected_time_ns, window_start_ns, window_end_ns,
    request_identity, created_time_ns ON opportunity
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'opportunity_id', 'ledger_partition', 'source_id', 'channel_id', 'instrument_uid',
    'opportunity_kind', 'expectation_v1', 'expected_time_ns', 'window_start_ns', 'window_end_ns',
    'request_identity', 'created_time_ns'
);
DROP TRIGGER opportunity_terminal_immutable ON opportunity;
CREATE TRIGGER opportunity_terminal_immutable
BEFORE UPDATE OF state, terminal_time_ns, terminal_outcome, terminal_outcome_v1 ON opportunity
FOR EACH ROW EXECUTE FUNCTION catalog_reject_terminal_opportunity_update();

ALTER TABLE opportunity_spill
    ALTER COLUMN dataset_partition_id DROP NOT NULL,
    ALTER COLUMN parquet_manifest_hash DROP NOT NULL,
    ADD COLUMN generation_fingerprint bytea CHECK (generation_fingerprint IS NULL OR octet_length(generation_fingerprint) = 32),
    ADD COLUMN row_count bigint CHECK (row_count IS NULL OR row_count > 0),
    ADD COLUMN parquet_manifest_key text CHECK (parquet_manifest_key IS NULL OR parquet_manifest_key <> ''),
    ADD COLUMN requested_through_time_ns bigint CHECK (requested_through_time_ns IS NULL OR requested_through_time_ns >= 0),
    ADD COLUMN requested_max_rows integer CHECK (requested_max_rows IS NULL OR requested_max_rows > 0),
    ADD COLUMN catalog_snapshot_hash bytea CHECK (catalog_snapshot_hash IS NULL OR octet_length(catalog_snapshot_hash) = 32),
    ADD COLUMN mapper_set_hash bytea CHECK (mapper_set_hash IS NULL OR octet_length(mapper_set_hash) = 32),
    ADD COLUMN quarantine_reason text CHECK (
        quarantine_reason IS NULL OR (quarantine_reason <> '' AND octet_length(quarantine_reason) <= 4096)
    ),
    ADD CONSTRAINT opportunity_spill_generation_complete CHECK (
        (generation_fingerprint IS NULL AND row_count IS NULL AND parquet_manifest_key IS NULL
         AND requested_through_time_ns IS NULL AND requested_max_rows IS NULL
         AND catalog_snapshot_hash IS NULL AND mapper_set_hash IS NULL AND quarantine_reason IS NULL)
        OR (generation_fingerprint IS NOT NULL AND row_count IS NOT NULL
            AND requested_through_time_ns IS NOT NULL AND requested_max_rows IS NOT NULL
            AND catalog_snapshot_hash IS NOT NULL AND mapper_set_hash IS NOT NULL
            AND ((state = 'quarantined') = (quarantine_reason IS NOT NULL))
            AND (state <> 'committed' OR (dataset_partition_id IS NOT NULL AND parquet_manifest_hash IS NOT NULL AND parquet_manifest_key IS NOT NULL)))
    ),
    ADD CONSTRAINT opportunity_spill_id_partition_unique UNIQUE (opportunity_spill_id, ledger_partition);
DROP TRIGGER opportunity_spill_identity_immutable ON opportunity_spill;
CREATE TRIGGER opportunity_spill_identity_immutable
BEFORE UPDATE OF opportunity_spill_id, ledger_partition, boundary_from_time_ns,
    boundary_from_opportunity_id, boundary_through_time_ns, boundary_through_opportunity_id,
    generation_fingerprint, row_count, requested_through_time_ns, requested_max_rows,
    catalog_snapshot_hash, mapper_set_hash ON opportunity_spill
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'opportunity_spill_id', 'ledger_partition', 'boundary_from_time_ns',
    'boundary_from_opportunity_id', 'boundary_through_time_ns',
    'boundary_through_opportunity_id', 'generation_fingerprint', 'row_count',
    'requested_through_time_ns', 'requested_max_rows', 'catalog_snapshot_hash', 'mapper_set_hash'
);
CREATE TABLE opportunity_spill_row (
    opportunity_spill_id uuid NOT NULL,
    ledger_partition text NOT NULL CHECK (ledger_partition <> ''),
    row_ordinal bigint NOT NULL CHECK (row_ordinal >= 0),
    opportunity_id uuid NOT NULL,
    terminal_time_ns bigint NOT NULL CHECK (terminal_time_ns >= 0),
    logical_hash bytea NOT NULL CHECK (octet_length(logical_hash) = 32),
    active_claim boolean NOT NULL DEFAULT true,
    PRIMARY KEY (opportunity_spill_id, row_ordinal),
    UNIQUE (opportunity_spill_id, opportunity_id),
    FOREIGN KEY (opportunity_spill_id, ledger_partition)
        REFERENCES opportunity_spill(opportunity_spill_id, ledger_partition)
);
CREATE UNIQUE INDEX opportunity_spill_row_active_claim_unique
ON opportunity_spill_row (ledger_partition, opportunity_id)
WHERE active_claim;
CREATE TRIGGER opportunity_spill_row_immutable
BEFORE UPDATE OF opportunity_spill_id, ledger_partition, row_ordinal, opportunity_id, terminal_time_ns, logical_hash
ON opportunity_spill_row
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'opportunity_spill_id', 'ledger_partition', 'row_ordinal', 'opportunity_id', 'terminal_time_ns', 'logical_hash'
);
-- +goose StatementBegin
CREATE FUNCTION catalog_validate_spill_row_claim()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    spill_state catalog_commit_state;
BEGIN
    IF OLD.active_claim = false OR NEW.active_claim = true THEN
        RAISE EXCEPTION 'spill row active claim cannot be reacquired or rewritten'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    SELECT state INTO spill_state
    FROM opportunity_spill
    WHERE opportunity_spill_id = OLD.opportunity_spill_id;
    IF spill_state <> 'quarantined' THEN
        RAISE EXCEPTION 'spill row claim can be released only by quarantine'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER opportunity_spill_row_claim_valid
BEFORE UPDATE OF active_claim ON opportunity_spill_row
FOR EACH ROW
WHEN (OLD.active_claim IS DISTINCT FROM NEW.active_claim)
EXECUTE FUNCTION catalog_validate_spill_row_claim();
-- +goose StatementBegin
CREATE FUNCTION catalog_validate_spill_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    actual_count bigint;
    actual_from record;
    actual_through record;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.state IN ('committed', 'quarantined') AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'terminal spill generation % is immutable', OLD.opportunity_spill_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF OLD.dataset_partition_id IS NOT NULL AND OLD.dataset_partition_id IS DISTINCT FROM NEW.dataset_partition_id THEN
            RAISE EXCEPTION 'spill generation % dataset identity is immutable', OLD.opportunity_spill_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF OLD.parquet_manifest_hash IS NOT NULL AND OLD.parquet_manifest_hash IS DISTINCT FROM NEW.parquet_manifest_hash THEN
            RAISE EXCEPTION 'spill generation % manifest hash is immutable', OLD.opportunity_spill_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF OLD.parquet_manifest_key IS NOT NULL AND OLD.parquet_manifest_key IS DISTINCT FROM NEW.parquet_manifest_key THEN
            RAISE EXCEPTION 'spill generation % manifest key is immutable', OLD.opportunity_spill_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF OLD.quarantine_reason IS NOT NULL AND OLD.quarantine_reason IS DISTINCT FROM NEW.quarantine_reason THEN
            RAISE EXCEPTION 'spill generation % quarantine evidence is immutable', OLD.opportunity_spill_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    END IF;
    IF NEW.state = 'committed' AND NEW.generation_fingerprint IS NOT NULL THEN
        SELECT count(*) INTO actual_count FROM opportunity_spill_row
        WHERE opportunity_spill_id = NEW.opportunity_spill_id;
        SELECT terminal_time_ns, opportunity_id INTO actual_from FROM opportunity_spill_row
        WHERE opportunity_spill_id = NEW.opportunity_spill_id ORDER BY row_ordinal LIMIT 1;
        SELECT terminal_time_ns, opportunity_id INTO actual_through FROM opportunity_spill_row
        WHERE opportunity_spill_id = NEW.opportunity_spill_id ORDER BY row_ordinal DESC LIMIT 1;
        IF actual_count <> NEW.row_count OR
           (actual_from.terminal_time_ns, actual_from.opportunity_id) IS DISTINCT FROM
               (NEW.boundary_from_time_ns, NEW.boundary_from_opportunity_id) OR
           (actual_through.terminal_time_ns, actual_through.opportunity_id) IS DISTINCT FROM
               (NEW.boundary_through_time_ns, NEW.boundary_through_opportunity_id) THEN
            RAISE EXCEPTION 'spill generation % membership/boundary mismatch', NEW.opportunity_spill_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE FUNCTION catalog_release_quarantined_spill_claims()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE opportunity_spill_row
    SET active_claim = false
    WHERE opportunity_spill_id = NEW.opportunity_spill_id AND active_claim;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER opportunity_spill_release_quarantined_claims
AFTER UPDATE OF state ON opportunity_spill
FOR EACH ROW
WHEN (OLD.state = 'pending' AND NEW.state = 'quarantined')
EXECUTE FUNCTION catalog_release_quarantined_spill_claims();
CREATE TRIGGER opportunity_spill_transition_valid
BEFORE INSERT OR UPDATE ON opportunity_spill
FOR EACH ROW EXECUTE FUNCTION catalog_validate_spill_transition();

ALTER DOMAIN catalog_gap_state DROP CONSTRAINT catalog_gap_state_check;
ALTER DOMAIN catalog_gap_state ADD CONSTRAINT catalog_gap_state_check
    CHECK (VALUE IN ('open', 'recovered', 'recovered_current_state', 'backfilled_explicitly', 'permanent'));
UPDATE gap SET state = 'recovered_current_state' WHERE state = 'recovered';
ALTER DOMAIN catalog_gap_state DROP CONSTRAINT catalog_gap_state_check;
ALTER DOMAIN catalog_gap_state ADD CONSTRAINT catalog_gap_state_check
    CHECK (VALUE IN ('open', 'recovered_current_state', 'backfilled_explicitly', 'permanent'));
ALTER TABLE gap
    ADD COLUMN first_good_coordinate jsonb CHECK (first_good_coordinate IS NULL OR jsonb_typeof(first_good_coordinate) = 'object'),
    ADD COLUMN last_good_coordinate jsonb CHECK (last_good_coordinate IS NULL OR jsonb_typeof(last_good_coordinate) = 'object'),
    ADD COLUMN affected_families text[] CHECK (affected_families IS NULL OR cardinality(affected_families) > 0),
    ADD COLUMN detected_time_ns bigint,
    ADD COLUMN resolved_time_ns bigint;
UPDATE gap
SET detected_time_ns = floor(extract(epoch FROM detected_at) * 1000000000)::bigint,
    resolved_time_ns = CASE WHEN resolved_at IS NULL THEN NULL
        ELSE floor(extract(epoch FROM resolved_at) * 1000000000)::bigint END;
ALTER TABLE gap
    ALTER COLUMN detected_time_ns SET NOT NULL,
    ADD CONSTRAINT gap_v1_evidence_complete CHECK (
        (first_good_coordinate IS NULL AND last_good_coordinate IS NULL AND affected_families IS NULL)
        OR (first_good_coordinate IS NOT NULL AND last_good_coordinate IS NOT NULL AND affected_families IS NOT NULL)
    ),
    ADD CONSTRAINT gap_v1_time_ns CHECK (
        detected_time_ns >= 0
        AND ((state = 'open') = (resolved_time_ns IS NULL))
        AND (resolved_time_ns IS NULL OR resolved_time_ns >= detected_time_ns)
    ),
    ADD CONSTRAINT gap_resolution_not_before_detection CHECK (
        resolved_at IS NULL OR resolved_at >= detected_at
    );
DROP TRIGGER gap_identity_immutable ON gap;
CREATE TRIGGER gap_identity_immutable
BEFORE UPDATE OF gap_id, source_id, channel_id, instrument_uid, range_start_ns, range_end_ns,
    detection_rule, confidence, evidence, detected_at, detected_time_ns,
    first_good_coordinate, last_good_coordinate, affected_families ON gap
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'gap_id', 'source_id', 'channel_id', 'instrument_uid', 'range_start_ns', 'range_end_ns',
    'detection_rule', 'confidence', 'evidence', 'detected_at', 'detected_time_ns',
    'first_good_coordinate', 'last_good_coordinate', 'affected_families'
);
-- +goose StatementBegin
CREATE FUNCTION catalog_validate_gap_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = 'backfilled_explicitly' THEN
        RAISE EXCEPTION 'backfilled_explicitly is reserved and cannot be emitted in v1'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF TG_OP = 'INSERT' AND NEW.state <> 'open' THEN
        RAISE EXCEPTION 'new gap % must be open', NEW.gap_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF TG_OP = 'UPDATE' AND (OLD.state <> 'open' OR NEW.state NOT IN ('recovered_current_state', 'permanent')) THEN
        RAISE EXCEPTION 'invalid gap state transition % -> %', OLD.state, NEW.state
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.resolved_at IS NOT NULL AND (
        NEW.resolved_at < NEW.detected_at OR NEW.resolved_time_ns < NEW.detected_time_ns
    ) THEN
        RAISE EXCEPTION 'gap % resolves before detection', NEW.gap_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER gap_transition_valid
BEFORE INSERT OR UPDATE ON gap
FOR EACH ROW EXECUTE FUNCTION catalog_validate_gap_transition();

ALTER TABLE incident
    ADD COLUMN reported_time_ns bigint,
    ADD COLUMN created_time_ns bigint;
UPDATE incident
SET reported_time_ns = floor(extract(epoch FROM reported_at) * 1000000000)::bigint,
    created_time_ns = floor(extract(epoch FROM created_at) * 1000000000)::bigint;
ALTER TABLE incident
    ALTER COLUMN reported_time_ns SET NOT NULL,
    ALTER COLUMN created_time_ns SET NOT NULL,
    ADD CONSTRAINT incident_time_ns_nonnegative CHECK (reported_time_ns >= 0 AND created_time_ns >= 0);
DROP TRIGGER incident_identity_immutable ON incident;
CREATE TRIGGER incident_immutable
BEFORE UPDATE ON incident
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'incident_id', 'annotation', 'report_source', 'affected_tuples',
    'range_start_ns', 'range_end_ns', 'reported_at', 'created_at',
    'reported_time_ns', 'created_time_ns'
);

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM schema_observation) THEN
        RAISE EXCEPTION 'legacy schema observations require explicit evidence-backed classification'
            USING ERRCODE = 'check_violation';
    END IF;
END;
$$;
-- +goose StatementEnd
ALTER DOMAIN catalog_schema_classification DROP CONSTRAINT catalog_schema_classification_check;
ALTER DOMAIN catalog_schema_classification ADD CONSTRAINT catalog_schema_classification_check
    CHECK (VALUE IN (
        'additive_unknown_field', 'known_optional_field_absent',
        'nonsemantic_type_or_shape_change', 'semantic_field_change', 'unknown_message_role'
    ));
ALTER TABLE schema_observation
    ADD COLUMN first_raw_coordinate jsonb NOT NULL CHECK (jsonb_typeof(first_raw_coordinate) = 'object'),
    ADD COLUMN last_raw_coordinate jsonb NOT NULL CHECK (jsonb_typeof(last_raw_coordinate) = 'object'),
    ADD COLUMN redacted_sample jsonb NOT NULL,
    ADD COLUMN parser_can_preserve boolean NOT NULL,
    ADD COLUMN optional_absence_resolved boolean NOT NULL,
    ADD COLUMN normalization_disposition catalog_normalization_disposition_v1 NOT NULL,
    ADD CONSTRAINT schema_observation_evidence_action CHECK (
        (classification = 'additive_unknown_field' AND normalization_disposition = 'continue')
        OR (classification = 'known_optional_field_absent' AND optional_absence_resolved AND normalization_disposition = 'transition_table')
        OR (classification = 'known_optional_field_absent' AND NOT optional_absence_resolved AND normalization_disposition = 'quarantine')
        OR (classification = 'nonsemantic_type_or_shape_change' AND parser_can_preserve AND normalization_disposition = 'continue')
        OR (classification = 'nonsemantic_type_or_shape_change' AND NOT parser_can_preserve AND normalization_disposition = 'quarantine')
        OR (classification = 'semantic_field_change' AND normalization_disposition = 'fail_closed')
        OR (classification = 'unknown_message_role' AND normalization_disposition = 'quarantine')
    );
DROP TRIGGER schema_observation_identity_immutable ON schema_observation;
CREATE TRIGGER schema_observation_identity_immutable
BEFORE UPDATE OF source_id, channel_id, fingerprint, first_seen_at, sample_segment_id,
    sample_ordinal, classification, first_raw_coordinate, redacted_sample,
    parser_can_preserve, optional_absence_resolved ON schema_observation
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'source_id', 'channel_id', 'fingerprint', 'first_seen_at', 'sample_segment_id',
    'sample_ordinal', 'classification', 'first_raw_coordinate', 'redacted_sample',
    'parser_can_preserve', 'optional_absence_resolved'
);

ALTER TABLE correction
    ADD COLUMN original_gap_id uuid REFERENCES gap(gap_id),
    ADD COLUMN created_time_ns bigint;
UPDATE correction
SET created_time_ns = floor(extract(epoch FROM created_at) * 1000000000)::bigint;
ALTER TABLE correction
    ALTER COLUMN created_time_ns SET NOT NULL,
    ADD CONSTRAINT correction_created_time_ns_nonnegative CHECK (created_time_ns >= 0);
DROP TRIGGER correction_identity_immutable ON correction;
CREATE TRIGGER correction_identity_immutable
BEFORE UPDATE OF correction_id, old_raw_segment_id, original_gap_id, new_dataset_partition_id,
    mapper_release_id, supersedes_correction_id, reason, lineage, created_at, created_time_ns ON correction
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'correction_id', 'old_raw_segment_id', 'original_gap_id', 'new_dataset_partition_id',
    'mapper_release_id', 'supersedes_correction_id', 'reason', 'lineage', 'created_at', 'created_time_ns'
);
-- +goose StatementBegin
CREATE FUNCTION catalog_reject_terminal_correction_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'committed' AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'committed correction % is immutable', OLD.correction_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER correction_terminal_immutable
BEFORE UPDATE ON correction
FOR EACH ROW EXECUTE FUNCTION catalog_reject_terminal_correction_update();


CREATE TABLE source_health_transition (
    transition_id uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text,
    dimension text NOT NULL CHECK (dimension <> ''),
    from_state catalog_health_state_v1 NOT NULL,
    to_state catalog_health_state_v1 NOT NULL,
    observed_time_ns bigint NOT NULL CHECK (observed_time_ns >= 0),
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (channel_id IS NULL OR channel_id <> ''),
    CHECK (from_state <> to_state)
);
CREATE TRIGGER source_health_transition_immutable
BEFORE UPDATE ON source_health_transition
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'transition_id', 'source_id', 'channel_id', 'dimension', 'from_state',
    'to_state', 'observed_time_ns', 'evidence', 'created_at'
);
