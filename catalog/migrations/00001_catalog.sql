-- +goose Up
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

CREATE DOMAIN catalog_source_state AS text
    CHECK (VALUE IN ('active', 'degraded', 'disabled', 'retired', 'quarantined'));
CREATE DOMAIN catalog_support_state AS text
    CHECK (VALUE IN ('supported', 'limited', 'unsupported', 'quarantined'));
CREATE DOMAIN catalog_binding_state AS text
    CHECK (VALUE IN ('candidate', 'dual_run', 'active', 'retired', 'rejected'));
CREATE DOMAIN catalog_commit_state AS text
    CHECK (VALUE IN ('pending', 'verified', 'committed', 'quarantined', 'superseded'));
CREATE DOMAIN catalog_opportunity_state AS text
    CHECK (VALUE IN ('scheduled', 'requested', 'observed', 'missed', 'failed', 'cancelled'));
CREATE DOMAIN catalog_gap_state AS text
    CHECK (VALUE IN ('open', 'recovered', 'permanent'));
CREATE DOMAIN catalog_schema_classification AS text
    CHECK (VALUE IN ('known', 'additive', 'changed', 'unknown'));
CREATE DOMAIN catalog_mapper_disposition AS text
    CHECK (VALUE IN ('pending', 'accepted', 'quarantined', 'rejected'));
CREATE DOMAIN catalog_correction_state AS text
    CHECK (VALUE IN ('pending', 'committed', 'rejected'));

-- +goose StatementBegin
CREATE FUNCTION catalog_reject_identity_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    column_name text;
BEGIN
    FOREACH column_name IN ARRAY TG_ARGV LOOP
        IF (to_jsonb(OLD) -> column_name) IS DISTINCT FROM (to_jsonb(NEW) -> column_name) THEN
            RAISE EXCEPTION '% identity column % is immutable', TG_TABLE_NAME, column_name
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION catalog_reject_terminal_opportunity_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.terminal_time_ns IS NOT NULL AND (
        OLD.state IS DISTINCT FROM NEW.state OR
        OLD.terminal_time_ns IS DISTINCT FROM NEW.terminal_time_ns OR
        OLD.terminal_outcome IS DISTINCT FROM NEW.terminal_outcome
    ) THEN
        RAISE EXCEPTION 'terminal opportunity % outcome is immutable', OLD.opportunity_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- A sign-bit-flipped big-endian int64 followed by UUID bytes preserves the
-- PostgreSQL row ordering of (terminal_time_ns, opportunity_id).
-- +goose StatementBegin
CREATE FUNCTION catalog_opportunity_boundary(terminal_time_ns bigint, opportunity_id uuid)
RETURNS bytea
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT int8send(terminal_time_ns # '-9223372036854775808'::bigint) || uuid_send(opportunity_id)
$$;
-- +goose StatementEnd

CREATE TYPE catalog_opportunity_boundary_range AS RANGE (
    subtype = bytea,
    subtype_opclass = bytea_ops
);

CREATE TABLE source (
    source_id uuid PRIMARY KEY,
    venue text NOT NULL CHECK (venue <> ''),
    product_family text NOT NULL CHECK (product_family <> ''),
    api_family text NOT NULL CHECK (api_family <> ''),
    environment text NOT NULL CHECK (environment <> ''),
    lifecycle_state catalog_source_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (venue, product_family, api_family, environment)
);

CREATE TRIGGER source_identity_immutable
BEFORE UPDATE OF source_id, venue, product_family, api_family, environment ON source
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'source_id', 'venue', 'product_family', 'api_family', 'environment'
);

CREATE TABLE source_version (
    source_id uuid NOT NULL REFERENCES source(source_id),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    official_api_version text NOT NULL CHECK (official_api_version <> ''),
    documentation_uri text NOT NULL CHECK (documentation_uri <> ''),
    endpoints jsonb NOT NULL CHECK (jsonb_typeof(endpoints) = 'object'),
    topology jsonb NOT NULL CHECK (jsonb_typeof(topology) = 'object'),
    entitlement jsonb NOT NULL CHECK (jsonb_typeof(entitlement) = 'object'),
    region text NOT NULL CHECK (region <> ''),
    rate_contract jsonb NOT NULL CHECK (jsonb_typeof(rate_contract) = 'object'),
    heartbeat_policy jsonb NOT NULL CHECK (jsonb_typeof(heartbeat_policy) = 'object'),
    acknowledgement_policy jsonb NOT NULL CHECK (jsonb_typeof(acknowledgement_policy) = 'object'),
    reconnect_policy jsonb NOT NULL CHECK (jsonb_typeof(reconnect_policy) = 'object'),
    PRIMARY KEY (source_id, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    EXCLUDE USING gist (
        source_id WITH =,
        tstzrange(valid_from, valid_to, '[)') WITH &&
    )
);

CREATE TRIGGER source_version_identity_immutable
BEFORE UPDATE OF source_id, valid_from ON source_version
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update('source_id', 'valid_from');

CREATE TABLE channel_contract (
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (channel_id <> ''),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    native_selector jsonb NOT NULL,
    channel_role text NOT NULL CHECK (channel_role <> ''),
    data_family text NOT NULL CHECK (data_family <> ''),
    cadence_source text NOT NULL CHECK (cadence_source <> ''),
    aggregation jsonb NOT NULL CHECK (jsonb_typeof(aggregation) = 'object'),
    depth jsonb NOT NULL CHECK (jsonb_typeof(depth) = 'object'),
    sequence_rules jsonb NOT NULL CHECK (jsonb_typeof(sequence_rules) = 'object'),
    checksum_rules jsonb NOT NULL CHECK (jsonb_typeof(checksum_rules) = 'object'),
    payload_schema jsonb NOT NULL CHECK (jsonb_typeof(payload_schema) = 'object'),
    support_state catalog_support_state NOT NULL,
    limitation text,
    PRIMARY KEY (source_id, channel_id, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    EXCLUDE USING gist (
        source_id WITH =,
        channel_id WITH =,
        tstzrange(valid_from, valid_to, '[)') WITH &&
    )
);

CREATE TRIGGER channel_contract_identity_immutable
BEFORE UPDATE OF source_id, channel_id, valid_from ON channel_contract
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update('source_id', 'channel_id', 'valid_from');

CREATE TABLE instrument (
    instrument_uid uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES source(source_id),
    native_id text NOT NULL CHECK (native_id <> ''),
    listing_epoch bigint NOT NULL CHECK (listing_epoch >= 0),
    first_observed_at timestamptz NOT NULL,
    UNIQUE (source_id, native_id, listing_epoch),
    UNIQUE (source_id, instrument_uid)
);

CREATE TRIGGER instrument_identity_immutable
BEFORE UPDATE OF instrument_uid, source_id, native_id, listing_epoch, first_observed_at ON instrument
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'instrument_uid', 'source_id', 'native_id', 'listing_epoch', 'first_observed_at'
);

CREATE TABLE instrument_version (
    instrument_uid uuid NOT NULL REFERENCES instrument(instrument_uid),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    aliases jsonb NOT NULL CHECK (jsonb_typeof(aliases) = 'array'),
    lifecycle_state catalog_source_state NOT NULL,
    base_asset text,
    quote_asset text,
    margin_asset text,
    settlement_asset text,
    instrument_kind text NOT NULL CHECK (instrument_kind <> ''),
    payoff jsonb NOT NULL CHECK (jsonb_typeof(payoff) = 'object'),
    multiplier numeric NOT NULL CHECK (multiplier > 0),
    strike numeric,
    expiry_at timestamptz,
    option_type text CHECK (option_type IN ('call', 'put')),
    tick_rules jsonb NOT NULL CHECK (jsonb_typeof(tick_rules) = 'object'),
    lot_rules jsonb NOT NULL CHECK (jsonb_typeof(lot_rules) = 'object'),
    raw_metadata jsonb NOT NULL CHECK (jsonb_typeof(raw_metadata) = 'object'),
    raw_metadata_hash bytea NOT NULL CHECK (octet_length(raw_metadata_hash) = 32),
    normalized_schema_version text NOT NULL CHECK (normalized_schema_version <> ''),
    PRIMARY KEY (instrument_uid, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK ((option_type IS NULL) = (strike IS NULL)),
    EXCLUDE USING gist (
        instrument_uid WITH =,
        tstzrange(valid_from, valid_to, '[)') WITH &&
    )
);

CREATE TRIGGER instrument_version_identity_immutable
BEFORE UPDATE OF instrument_uid, valid_from ON instrument_version
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update('instrument_uid', 'valid_from');

CREATE TABLE instrument_alias (
    source_id uuid NOT NULL REFERENCES source(source_id),
    alias text NOT NULL CHECK (alias <> ''),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    instrument_uid uuid NOT NULL,
    PRIMARY KEY (source_id, alias, valid_from),
    FOREIGN KEY (source_id, instrument_uid) REFERENCES instrument(source_id, instrument_uid),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    EXCLUDE USING gist (
        source_id WITH =,
        alias WITH =,
        tstzrange(valid_from, valid_to, '[)') WITH &&
    )
);

CREATE TRIGGER instrument_alias_identity_immutable
BEFORE UPDATE OF source_id, alias, valid_from ON instrument_alias
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update('source_id', 'alias', 'valid_from');

CREATE TABLE mapper_release (
    mapper_release_id uuid PRIMARY KEY,
    mapper_version text NOT NULL CHECK (mapper_version <> ''),
    build_identity text NOT NULL CHECK (build_identity <> ''),
    normalized_schema_version text NOT NULL CHECK (normalized_schema_version <> ''),
    fixture_bundle_hash bytea NOT NULL CHECK (octet_length(fixture_bundle_hash) = 32),
    source_code_identity text NOT NULL CHECK (source_code_identity <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (mapper_version, build_identity),
    UNIQUE (fixture_bundle_hash, source_code_identity)
);

CREATE TRIGGER mapper_release_identity_immutable
BEFORE UPDATE OF mapper_release_id, mapper_version, build_identity, normalized_schema_version,
    fixture_bundle_hash, source_code_identity ON mapper_release
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'mapper_release_id', 'mapper_version', 'build_identity', 'normalized_schema_version',
    'fixture_bundle_hash', 'source_code_identity'
);

CREATE TABLE mapper_binding (
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (channel_id <> ''),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    mapper_release_id uuid NOT NULL REFERENCES mapper_release(mapper_release_id),
    schema_fingerprints jsonb NOT NULL CHECK (jsonb_typeof(schema_fingerprints) = 'array'),
    dual_run_evidence jsonb NOT NULL CHECK (jsonb_typeof(dual_run_evidence) = 'object'),
    state catalog_binding_state NOT NULL,
    PRIMARY KEY (source_id, channel_id, effective_from),
    CHECK (effective_to IS NULL OR effective_to > effective_from),
    EXCLUDE USING gist (
        source_id WITH =,
        channel_id WITH =,
        tstzrange(effective_from, effective_to, '[)') WITH &&
    )
);

CREATE TRIGGER mapper_binding_identity_immutable
BEFORE UPDATE OF source_id, channel_id, effective_from ON mapper_binding
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update('source_id', 'channel_id', 'effective_from');

CREATE TABLE raw_segment (
    raw_segment_id uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (channel_id <> ''),
    epoch_id uuid NOT NULL,
    receive_time_start_ns bigint NOT NULL CHECK (receive_time_start_ns >= 0),
    receive_time_end_ns bigint NOT NULL CHECK (receive_time_end_ns >= receive_time_start_ns),
    ordinal_start bigint NOT NULL CHECK (ordinal_start >= 0),
    ordinal_end bigint NOT NULL CHECK (ordinal_end >= ordinal_start),
    object_key text NOT NULL CHECK (object_key <> ''),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    byte_length bigint NOT NULL CHECK (byte_length > 0),
    state catalog_commit_state NOT NULL,
    writer_lease_id uuid,
    corrected_from_segment_id uuid REFERENCES raw_segment(raw_segment_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    committed_at timestamptz,
    UNIQUE (object_key),
    UNIQUE (content_hash, byte_length),
    CHECK (corrected_from_segment_id IS NULL OR corrected_from_segment_id <> raw_segment_id),
    CHECK ((state IN ('committed', 'superseded')) = (committed_at IS NOT NULL))
);

CREATE UNIQUE INDEX raw_segment_committed_identity
ON raw_segment (source_id, epoch_id, ordinal_start, ordinal_end)
WHERE state = 'committed';

CREATE TRIGGER raw_segment_identity_immutable
BEFORE UPDATE OF raw_segment_id, source_id, channel_id, epoch_id, receive_time_start_ns,
    receive_time_end_ns, ordinal_start, ordinal_end, object_key, content_hash, byte_length,
    corrected_from_segment_id ON raw_segment
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'raw_segment_id', 'source_id', 'channel_id', 'epoch_id', 'receive_time_start_ns',
    'receive_time_end_ns', 'ordinal_start', 'ordinal_end', 'object_key', 'content_hash',
    'byte_length', 'corrected_from_segment_id'
);

CREATE TABLE dataset_partition (
    dataset_partition_id uuid PRIMARY KEY,
    dataset_family text NOT NULL CHECK (dataset_family <> ''),
    dataset_version text NOT NULL CHECK (dataset_version <> ''),
    partition_key text NOT NULL CHECK (partition_key <> ''),
    range_start_ns bigint NOT NULL CHECK (range_start_ns >= 0),
    range_end_ns bigint NOT NULL CHECK (range_end_ns >= range_start_ns),
    input_segment_set_hash bytea NOT NULL CHECK (octet_length(input_segment_set_hash) = 32),
    catalog_snapshot_hash bytea NOT NULL CHECK (octet_length(catalog_snapshot_hash) = 32),
    mapper_set_hash bytea NOT NULL CHECK (octet_length(mapper_set_hash) = 32),
    logical_hash bytea NOT NULL CHECK (octet_length(logical_hash) = 32),
    physical_hash bytea NOT NULL CHECK (octet_length(physical_hash) = 32),
    object_key text NOT NULL CHECK (object_key <> ''),
    canonical boolean NOT NULL DEFAULT false,
    state catalog_commit_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    committed_at timestamptz,
    UNIQUE (object_key),
    UNIQUE (physical_hash),
    UNIQUE (dataset_partition_id, state),
    CHECK ((state IN ('committed', 'superseded')) = (committed_at IS NOT NULL)),
    CHECK (NOT canonical OR state = 'committed')
);

CREATE UNIQUE INDEX dataset_partition_canonical_build
ON dataset_partition (
    dataset_family, dataset_version, partition_key, range_start_ns, range_end_ns,
    input_segment_set_hash, catalog_snapshot_hash, mapper_set_hash
)
WHERE canonical;

CREATE TRIGGER dataset_partition_identity_immutable
BEFORE UPDATE OF dataset_partition_id, dataset_family, dataset_version, partition_key,
    range_start_ns, range_end_ns, input_segment_set_hash, catalog_snapshot_hash,
    mapper_set_hash, logical_hash, physical_hash, object_key ON dataset_partition
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'dataset_partition_id', 'dataset_family', 'dataset_version', 'partition_key',
    'range_start_ns', 'range_end_ns', 'input_segment_set_hash', 'catalog_snapshot_hash',
    'mapper_set_hash', 'logical_hash', 'physical_hash', 'object_key'
);

CREATE TABLE dataset_partition_segment (
    dataset_partition_id uuid NOT NULL REFERENCES dataset_partition(dataset_partition_id),
    raw_segment_id uuid NOT NULL REFERENCES raw_segment(raw_segment_id),
    input_ordinal integer NOT NULL CHECK (input_ordinal >= 0),
    PRIMARY KEY (dataset_partition_id, raw_segment_id),
    UNIQUE (dataset_partition_id, input_ordinal)
);

CREATE TRIGGER dataset_partition_segment_identity_immutable
BEFORE UPDATE OF dataset_partition_id, raw_segment_id, input_ordinal ON dataset_partition_segment
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'dataset_partition_id', 'raw_segment_id', 'input_ordinal'
);

CREATE TABLE collection_lease (
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (channel_id <> ''),
    lease_id uuid NOT NULL UNIQUE,
    writer_id text NOT NULL CHECK (writer_id <> ''),
    acquired_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (source_id, channel_id),
    UNIQUE (source_id, channel_id, lease_id),
    CHECK (expires_at > acquired_at)
);

CREATE TRIGGER collection_lease_identity_immutable
BEFORE UPDATE OF source_id, channel_id, lease_id, writer_id, acquired_at ON collection_lease
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'source_id', 'channel_id', 'lease_id', 'writer_id', 'acquired_at'
);

ALTER TABLE raw_segment
ADD CONSTRAINT raw_segment_writer_lease_fk
FOREIGN KEY (source_id, channel_id, writer_lease_id)
REFERENCES collection_lease(source_id, channel_id, lease_id);

CREATE TABLE opportunity (
    opportunity_id uuid PRIMARY KEY,
    ledger_partition text NOT NULL CHECK (ledger_partition <> ''),
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (channel_id <> ''),
    instrument_uid uuid REFERENCES instrument(instrument_uid),
    opportunity_kind text NOT NULL CHECK (opportunity_kind <> ''),
    expected_time_ns bigint NOT NULL CHECK (expected_time_ns >= 0),
    window_start_ns bigint NOT NULL CHECK (window_start_ns >= 0),
    window_end_ns bigint NOT NULL CHECK (window_end_ns >= window_start_ns),
    request_identity jsonb NOT NULL CHECK (jsonb_typeof(request_identity) = 'object'),
    state catalog_opportunity_state NOT NULL,
    terminal_time_ns bigint CHECK (terminal_time_ns >= 0),
    terminal_outcome jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (expected_time_ns BETWEEN window_start_ns AND window_end_ns),
    CHECK (
        (state IN ('observed', 'missed', 'failed', 'cancelled')) =
        (terminal_time_ns IS NOT NULL AND terminal_outcome IS NOT NULL)
    ),
    CHECK (terminal_outcome IS NULL OR jsonb_typeof(terminal_outcome) = 'object')
);

CREATE INDEX opportunity_terminal_boundary
ON opportunity (ledger_partition, terminal_time_ns, opportunity_id)
WHERE terminal_time_ns IS NOT NULL;

CREATE TRIGGER opportunity_identity_immutable
BEFORE UPDATE OF opportunity_id, ledger_partition, source_id, channel_id, instrument_uid,
    opportunity_kind, expected_time_ns, window_start_ns, window_end_ns, request_identity ON opportunity
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'opportunity_id', 'ledger_partition', 'source_id', 'channel_id', 'instrument_uid',
    'opportunity_kind', 'expected_time_ns', 'window_start_ns', 'window_end_ns', 'request_identity'
);

CREATE TRIGGER opportunity_terminal_immutable
BEFORE UPDATE OF state, terminal_time_ns, terminal_outcome ON opportunity
FOR EACH ROW EXECUTE FUNCTION catalog_reject_terminal_opportunity_update();

CREATE TABLE opportunity_spill (
    opportunity_spill_id uuid PRIMARY KEY,
    ledger_partition text NOT NULL CHECK (ledger_partition <> ''),
    boundary_from_time_ns bigint NOT NULL CHECK (boundary_from_time_ns >= 0),
    boundary_from_opportunity_id uuid NOT NULL,
    boundary_through_time_ns bigint NOT NULL CHECK (boundary_through_time_ns >= 0),
    boundary_through_opportunity_id uuid NOT NULL,
    boundary_range catalog_opportunity_boundary_range GENERATED ALWAYS AS (
        catalog_opportunity_boundary_range(
            catalog_opportunity_boundary(boundary_from_time_ns, boundary_from_opportunity_id),
            catalog_opportunity_boundary(boundary_through_time_ns, boundary_through_opportunity_id),
            '[]'
        )
    ) STORED,
    dataset_partition_id uuid NOT NULL REFERENCES dataset_partition(dataset_partition_id),
    parquet_manifest_hash bytea NOT NULL CHECK (octet_length(parquet_manifest_hash) = 32),
    state catalog_commit_state NOT NULL,
    committed_at timestamptz,
    required_dataset_partition_id uuid GENERATED ALWAYS AS (
        CASE WHEN state = 'committed' THEN dataset_partition_id END
    ) STORED,
    required_dataset_state catalog_commit_state GENERATED ALWAYS AS (
        CASE WHEN state = 'committed' THEN 'committed'::catalog_commit_state END
    ) STORED,
    CHECK (
        (boundary_from_time_ns, boundary_from_opportunity_id) <=
        (boundary_through_time_ns, boundary_through_opportunity_id)
    ),
    CHECK (state IN ('pending', 'committed', 'quarantined')),
    CHECK ((state = 'committed') = (committed_at IS NOT NULL)),
    UNIQUE (dataset_partition_id),
    UNIQUE (parquet_manifest_hash),
    FOREIGN KEY (required_dataset_partition_id, required_dataset_state)
        REFERENCES dataset_partition(dataset_partition_id, state),
    EXCLUDE USING gist (
        ledger_partition WITH =,
        boundary_range WITH &&
    ) WHERE (state = 'committed')
);

CREATE TRIGGER opportunity_spill_identity_immutable
BEFORE UPDATE OF opportunity_spill_id, ledger_partition, boundary_from_time_ns,
    boundary_from_opportunity_id, boundary_through_time_ns, boundary_through_opportunity_id,
    dataset_partition_id, parquet_manifest_hash ON opportunity_spill
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'opportunity_spill_id', 'ledger_partition', 'boundary_from_time_ns',
    'boundary_from_opportunity_id', 'boundary_through_time_ns',
    'boundary_through_opportunity_id', 'dataset_partition_id', 'parquet_manifest_hash'
);

CREATE TABLE gap (
    gap_id uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (channel_id <> ''),
    instrument_uid uuid REFERENCES instrument(instrument_uid),
    range_start_ns bigint NOT NULL CHECK (range_start_ns >= 0),
    range_end_ns bigint NOT NULL CHECK (range_end_ns >= range_start_ns),
    detection_rule text NOT NULL CHECK (detection_rule <> ''),
    state catalog_gap_state NOT NULL,
    confidence numeric NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    detected_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    CHECK ((state = 'open') = (resolved_at IS NULL))
);

CREATE TRIGGER gap_identity_immutable
BEFORE UPDATE OF gap_id, source_id, channel_id, instrument_uid, range_start_ns, range_end_ns,
    detection_rule ON gap
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'gap_id', 'source_id', 'channel_id', 'instrument_uid', 'range_start_ns', 'range_end_ns',
    'detection_rule'
);

CREATE TABLE incident (
    incident_id uuid PRIMARY KEY,
    annotation text NOT NULL CHECK (annotation <> ''),
    report_source text NOT NULL CHECK (report_source <> ''),
    affected_tuples jsonb NOT NULL CHECK (jsonb_typeof(affected_tuples) = 'array'),
    range_start_ns bigint CHECK (range_start_ns >= 0),
    range_end_ns bigint CHECK (range_end_ns >= range_start_ns),
    reported_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((range_start_ns IS NULL) = (range_end_ns IS NULL))
);

CREATE TRIGGER incident_identity_immutable
BEFORE UPDATE OF incident_id, report_source, reported_at ON incident
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update('incident_id', 'report_source', 'reported_at');

CREATE TABLE schema_observation (
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (channel_id <> ''),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    observation_count bigint NOT NULL CHECK (observation_count > 0),
    classification catalog_schema_classification NOT NULL,
    sample_segment_id uuid NOT NULL REFERENCES raw_segment(raw_segment_id),
    sample_ordinal bigint NOT NULL CHECK (sample_ordinal >= 0),
    mapper_disposition catalog_mapper_disposition NOT NULL,
    mapper_release_id uuid REFERENCES mapper_release(mapper_release_id),
    PRIMARY KEY (source_id, channel_id, fingerprint),
    CHECK (last_seen_at >= first_seen_at),
    CHECK ((mapper_disposition = 'accepted') = (mapper_release_id IS NOT NULL))
);

CREATE TRIGGER schema_observation_identity_immutable
BEFORE UPDATE OF source_id, channel_id, fingerprint, first_seen_at, sample_segment_id,
    sample_ordinal ON schema_observation
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'source_id', 'channel_id', 'fingerprint', 'first_seen_at', 'sample_segment_id',
    'sample_ordinal'
);

CREATE TABLE correction (
    correction_id uuid PRIMARY KEY,
    old_raw_segment_id uuid NOT NULL REFERENCES raw_segment(raw_segment_id),
    new_dataset_partition_id uuid NOT NULL REFERENCES dataset_partition(dataset_partition_id),
    mapper_release_id uuid NOT NULL REFERENCES mapper_release(mapper_release_id),
    supersedes_correction_id uuid REFERENCES correction(correction_id),
    reason text NOT NULL CHECK (reason <> ''),
    lineage jsonb NOT NULL CHECK (jsonb_typeof(lineage) = 'object'),
    state catalog_correction_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    committed_at timestamptz,
    CHECK (supersedes_correction_id IS NULL OR supersedes_correction_id <> correction_id),
    CHECK ((state = 'committed') = (committed_at IS NOT NULL)),
    UNIQUE (old_raw_segment_id, new_dataset_partition_id, mapper_release_id)
);

CREATE TRIGGER correction_identity_immutable
BEFORE UPDATE OF correction_id, old_raw_segment_id, new_dataset_partition_id,
    mapper_release_id, supersedes_correction_id ON correction
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'correction_id', 'old_raw_segment_id', 'new_dataset_partition_id',
    'mapper_release_id', 'supersedes_correction_id'
);
