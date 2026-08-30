-- +goose Up
-- Dataset manifests are immutable publication evidence attached to the existing
-- dataset_partition lifecycle. Keeping this separate makes the migration safe
-- for historical partitions that predate manifest-backed serving.
CREATE TABLE dataset_manifest (
    dataset_partition_id uuid PRIMARY KEY REFERENCES dataset_partition(dataset_partition_id),
    source_id uuid NOT NULL REFERENCES source(source_id),
    schema_name text NOT NULL CHECK (btrim(schema_name) <> ''),
    schema_version smallint NOT NULL CHECK (schema_version > 0),
    manifest_version smallint NOT NULL CHECK (manifest_version > 0),
    manifest_object_key text NOT NULL UNIQUE CHECK (btrim(manifest_object_key) <> ''),
    manifest_hash bytea NOT NULL UNIQUE CHECK (octet_length(manifest_hash) = 32),
    manifest_bytes bytea NOT NULL CHECK (octet_length(manifest_bytes) BETWEEN 1 AND 1048576),
    parquet_byte_length bigint NOT NULL CHECK (parquet_byte_length > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER dataset_manifest_identity_immutable
BEFORE UPDATE ON dataset_manifest
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'dataset_partition_id', 'source_id', 'schema_name', 'schema_version',
    'manifest_version', 'manifest_object_key', 'manifest_hash', 'manifest_bytes',
    'parquet_byte_length', 'created_at'
);

-- A warehouse generation is the immutable dataset identity exposed to query
-- clients. It is distinct from the UUID publication identity because the load
-- generation also binds the warehouse server digest and partition layout.
CREATE TABLE dataset_generation (
    generation_id bytea PRIMARY KEY CHECK (octet_length(generation_id) = 32),
    dataset_partition_id uuid NOT NULL REFERENCES dataset_partition(dataset_partition_id),
    dataset_family text NOT NULL CHECK (btrim(dataset_family) <> ''),
    catalog_snapshot_hash bytea NOT NULL CHECK (octet_length(catalog_snapshot_hash) = 32),
    schema_name text NOT NULL CHECK (btrim(schema_name) <> ''),
    schema_version smallint NOT NULL CHECK (schema_version > 0),
    manifest_hash bytea NOT NULL CHECK (octet_length(manifest_hash) = 32),
    input_hash bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    expected_event_count bigint NOT NULL CHECK (expected_event_count > 0),
    expected_row_count bigint NOT NULL CHECK (
        expected_row_count > 0 AND expected_row_count >= expected_event_count
    ),
    state catalog_commit_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    committed_at timestamptz,
    CHECK ((state IN ('committed', 'superseded')) = (committed_at IS NOT NULL)),
    UNIQUE (generation_id, state)
);

CREATE TRIGGER dataset_generation_identity_immutable
BEFORE UPDATE OF generation_id, dataset_partition_id, dataset_family,
    catalog_snapshot_hash, schema_name, schema_version, manifest_hash, input_hash,
    expected_event_count, expected_row_count, created_at
ON dataset_generation
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'generation_id', 'dataset_partition_id', 'dataset_family',
    'catalog_snapshot_hash', 'schema_name', 'schema_version', 'manifest_hash',
    'input_hash', 'expected_event_count', 'expected_row_count', 'created_at'
);

CREATE TABLE dataset_coverage (
    coverage_id uuid PRIMARY KEY,
    dataset_partition_id uuid NOT NULL REFERENCES dataset_partition(dataset_partition_id),
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (btrim(channel_id) <> ''),
    instrument_uid uuid,
    range_start_ns bigint NOT NULL CHECK (range_start_ns >= 0),
    range_end_ns bigint NOT NULL CHECK (range_end_ns > range_start_ns),
    coverage_state text NOT NULL CHECK (btrim(coverage_state) <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dataset_partition_id, source_id, channel_id, instrument_uid,
        range_start_ns, range_end_ns, coverage_state),
    FOREIGN KEY (source_id, instrument_uid) REFERENCES instrument(source_id, instrument_uid)
);

CREATE TRIGGER dataset_coverage_identity_immutable
BEFORE UPDATE ON dataset_coverage
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'coverage_id', 'dataset_partition_id', 'source_id', 'channel_id', 'instrument_uid',
    'range_start_ns', 'range_end_ns', 'coverage_state', 'created_at'
);

-- Runtime checkpoints are mutable only by strictly advancing the stable
-- collector coordinate. Exact repeats are idempotent; regression and mutation
-- at an already-recorded coordinate fail at the database boundary.
CREATE TABLE runtime_checkpoint (
    checkpoint_key text PRIMARY KEY CHECK (btrim(checkpoint_key) <> ''),
    source_id uuid NOT NULL REFERENCES source(source_id),
    channel_id text NOT NULL CHECK (btrim(channel_id) <> ''),
    instrument_uid uuid REFERENCES instrument(instrument_uid),
    received_time_ns bigint NOT NULL CHECK (received_time_ns >= 0),
    stream_epoch_id uuid NOT NULL,
    arrival_ordinal bigint NOT NULL CHECK (arrival_ordinal >= 0),
    message_ordinal integer NOT NULL CHECK (message_ordinal >= 0),
    state_hash bytea NOT NULL CHECK (octet_length(state_hash) = 32),
    state_bytes bytea NOT NULL CHECK (octet_length(state_bytes) BETWEEN 1 AND 1048576),
    updated_at timestamptz NOT NULL
);

-- +goose StatementBegin
CREATE FUNCTION catalog_reject_checkpoint_regression()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (NEW.received_time_ns, NEW.stream_epoch_id, NEW.arrival_ordinal, NEW.message_ordinal)
       < (OLD.received_time_ns, OLD.stream_epoch_id, OLD.arrival_ordinal, OLD.message_ordinal) THEN
        RAISE EXCEPTION 'runtime checkpoint % cannot regress', OLD.checkpoint_key
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF (NEW.received_time_ns, NEW.stream_epoch_id, NEW.arrival_ordinal, NEW.message_ordinal)
       = (OLD.received_time_ns, OLD.stream_epoch_id, OLD.arrival_ordinal, OLD.message_ordinal)
       AND (NEW.state_hash IS DISTINCT FROM OLD.state_hash
            OR NEW.state_bytes IS DISTINCT FROM OLD.state_bytes
            OR NEW.updated_at IS DISTINCT FROM OLD.updated_at) THEN
        RAISE EXCEPTION 'runtime checkpoint % cannot change at the same coordinate', OLD.checkpoint_key
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER runtime_checkpoint_identity_immutable
BEFORE UPDATE OF checkpoint_key, source_id, channel_id, instrument_uid
ON runtime_checkpoint
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'checkpoint_key', 'source_id', 'channel_id', 'instrument_uid'
);

CREATE TRIGGER runtime_checkpoint_monotonic
BEFORE UPDATE OF received_time_ns, stream_epoch_id, arrival_ordinal, message_ordinal,
    state_hash, state_bytes, updated_at
ON runtime_checkpoint
FOR EACH ROW EXECUTE FUNCTION catalog_reject_checkpoint_regression();

CREATE INDEX raw_segment_committed_query
ON raw_segment (
    source_id, channel_id, receive_time_start_ns, receive_time_end_ns,
    epoch_id, ordinal_start, raw_segment_id
)
WHERE state = 'committed';

CREATE INDEX dataset_generation_committed_family
ON dataset_generation (dataset_family, committed_at DESC, generation_id)
WHERE state = 'committed';

CREATE INDEX dataset_coverage_partition_query
ON dataset_coverage (
    dataset_partition_id, source_id, channel_id, range_start_ns, range_end_ns,
    instrument_uid, coverage_id
);

CREATE INDEX gap_projection_query
ON gap (source_id, channel_id, range_start_ns, range_end_ns, instrument_uid, gap_id);

CREATE INDEX incident_projection_query
ON incident (range_start_ns, range_end_ns, incident_id)
WHERE range_start_ns IS NOT NULL;
