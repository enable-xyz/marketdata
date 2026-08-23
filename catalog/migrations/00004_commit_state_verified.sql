-- +goose Up
-- Historical schema v3 deployments predate the verified publication state.
-- Rebuild the domain constraint so rolling upgrades accept the state used by
-- RecordVerified without rewriting existing rows.
ALTER DOMAIN catalog_commit_state DROP CONSTRAINT catalog_commit_state_check;
ALTER DOMAIN catalog_commit_state ADD CONSTRAINT catalog_commit_state_check
    CHECK (VALUE IN ('pending', 'verified', 'committed', 'quarantined', 'superseded'));

