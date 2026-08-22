-- +goose Up
CREATE TABLE raw_segment_manifest (
    raw_segment_id uuid PRIMARY KEY REFERENCES raw_segment(raw_segment_id),
    manifest_version smallint NOT NULL CHECK (manifest_version > 0),
    manifest_hash bytea NOT NULL UNIQUE CHECK (octet_length(manifest_hash) = 32),
    manifest_bytes bytea NOT NULL CHECK (octet_length(manifest_bytes) > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER raw_segment_manifest_identity_immutable
BEFORE UPDATE OF raw_segment_id, manifest_version, manifest_hash, manifest_bytes
ON raw_segment_manifest
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'raw_segment_id', 'manifest_version', 'manifest_hash', 'manifest_bytes'
);

CREATE TABLE raw_segment_quarantine (
    raw_segment_id uuid PRIMARY KEY REFERENCES raw_segment(raw_segment_id),
    reason text NOT NULL CHECK (reason <> ''),
    quarantined_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER raw_segment_quarantine_identity_immutable
BEFORE UPDATE OF raw_segment_id, reason
ON raw_segment_quarantine
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'raw_segment_id', 'reason'
);

CREATE TABLE object_orphan (
    object_key text PRIMARY KEY CHECK (object_key <> ''),
    byte_length bigint NOT NULL CHECK (byte_length >= 0),
    application_sha256 bytea CHECK (
        application_sha256 IS NULL OR octet_length(application_sha256) = 32
    ),
    state catalog_commit_state NOT NULL DEFAULT 'quarantined'
        CHECK (state = 'quarantined'),
    reason text NOT NULL CHECK (reason <> ''),
    discovered_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    CHECK (last_seen_at >= discovered_at)
);

CREATE TRIGGER object_orphan_identity_immutable
BEFORE UPDATE OF object_key, byte_length, application_sha256, state
ON object_orphan
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'object_key', 'byte_length', 'application_sha256', 'state'
);
