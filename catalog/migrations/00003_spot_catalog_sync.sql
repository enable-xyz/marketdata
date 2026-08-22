-- +goose Up
CREATE TABLE raw_record_evidence (
    raw_segment_id uuid NOT NULL REFERENCES raw_segment(raw_segment_id),
    arrival_ordinal bigint NOT NULL CHECK (arrival_ordinal > 0),
    message_ordinal integer NOT NULL CHECK (message_ordinal >= 0),
    envelope_version smallint NOT NULL CHECK (envelope_version > 0),
    request_id text NOT NULL CHECK (request_id <> ''),
    payload_sha256 bytea NOT NULL CHECK (octet_length(payload_sha256) = 32),
    payload_byte_length integer NOT NULL CHECK (payload_byte_length BETWEEN 1 AND 16777216),
    PRIMARY KEY (raw_segment_id, arrival_ordinal, message_ordinal)
);

CREATE TRIGGER raw_record_evidence_identity_immutable
BEFORE UPDATE OF raw_segment_id, arrival_ordinal, message_ordinal, envelope_version,
    request_id, payload_sha256, payload_byte_length ON raw_record_evidence
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'raw_segment_id', 'arrival_ordinal', 'message_ordinal', 'envelope_version',
    'request_id', 'payload_sha256', 'payload_byte_length'
);

CREATE TABLE catalog_snapshot (
    snapshot_sha256 bytea PRIMARY KEY CHECK (octet_length(snapshot_sha256) = 32),
    source_id uuid NOT NULL REFERENCES source(source_id),
    snapshot_version smallint NOT NULL CHECK (snapshot_version > 0),
    snapshot_bytes bytea NOT NULL CHECK (octet_length(snapshot_bytes) > 0),
    instrument_count integer NOT NULL CHECK (instrument_count >= 0),
    first_observed_at timestamptz NOT NULL,
    UNIQUE (source_id, snapshot_version, snapshot_sha256)
);

CREATE TRIGGER catalog_snapshot_identity_immutable
BEFORE UPDATE OF snapshot_sha256, source_id, snapshot_version, snapshot_bytes,
    instrument_count, first_observed_at ON catalog_snapshot
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'snapshot_sha256', 'source_id', 'snapshot_version', 'snapshot_bytes',
    'instrument_count', 'first_observed_at'
);

CREATE TABLE catalog_sync_run (
    sync_run_sha256 bytea PRIMARY KEY CHECK (octet_length(sync_run_sha256) = 32),
    source_id uuid NOT NULL REFERENCES source(source_id),
    observed_at timestamptz NOT NULL,
    page_count integer NOT NULL CHECK (page_count BETWEEN 1 AND 64),
    page_evidence jsonb NOT NULL CHECK (jsonb_typeof(page_evidence) = 'array'),
    snapshot_sha256 bytea NOT NULL REFERENCES catalog_snapshot(snapshot_sha256),
    UNIQUE (source_id, observed_at),
    CHECK (jsonb_array_length(page_evidence) = page_count)
);

CREATE TRIGGER catalog_sync_run_identity_immutable
BEFORE UPDATE OF sync_run_sha256, source_id, observed_at, page_count,
    page_evidence, snapshot_sha256 ON catalog_sync_run
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'sync_run_sha256', 'source_id', 'observed_at', 'page_count',
    'page_evidence', 'snapshot_sha256'
);
