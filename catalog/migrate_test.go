package catalog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const postgresDSNEnvironment = "MARKETDATA_TEST_POSTGRES_DSN"

var schemaSequence atomic.Uint64

type postgresFixture struct {
	config *pgx.ConnConfig
}

func TestMigrationFreshAndNMinusOneConverge(t *testing.T) {
	fresh := newPostgresFixture(t)
	freshConn := fresh.connect(t)
	if err := Migrate(t.Context(), freshConn); err != nil {
		t.Fatalf("fresh Migrate() error = %v", err)
	}
	if err := CheckSchema(t.Context(), freshConn); err != nil {
		t.Fatalf("fresh CheckSchema() error = %v", err)
	}

	version, err := SchemaVersion(t.Context(), freshConn)
	if err != nil {
		t.Fatalf("fresh SchemaVersion() error = %v", err)
	}
	if version != MaximumSupportedSchemaVersion {
		t.Fatalf("fresh SchemaVersion() = %d, want %d", version, MaximumSupportedSchemaVersion)
	}

	wantTables := []string{
		"catalog_snapshot",
		"catalog_sync_run",
		"channel_contract",
		"collection_lease",
		"correction",
		"dataset_partition",
		"dataset_partition_segment",
		"gap",
		"goose_db_version",
		"incident",
		"instrument",
		"instrument_alias",
		"instrument_version",
		"mapper_binding",
		"mapper_release",
		"object_orphan",
		"opportunity",
		"opportunity_spill",
		"raw_record_evidence",
		"raw_segment",
		"raw_segment_manifest",
		"raw_segment_quarantine",
		"schema_observation",
		"source",
		"source_version",
	}
	if got := catalogTables(t, freshConn); !slices.Equal(got, wantTables) {
		t.Fatalf("fresh catalog tables = %q, want %q", got, wantTables)
	}

	nMinusOne := newPostgresFixture(t)
	nMinusOneConn := nMinusOne.connect(t)
	createVersionZero(t, nMinusOneConn)
	if err := Migrate(t.Context(), nMinusOneConn); err != nil {
		t.Fatalf("N-1 Migrate() error = %v", err)
	}

	freshFingerprint := schemaFingerprint(t, freshConn)
	nMinusOneFingerprint := schemaFingerprint(t, nMinusOneConn)
	if !slices.Equal(freshFingerprint, nMinusOneFingerprint) {
		t.Fatalf(
			"fresh and N-1 schemas did not converge\nfresh:\n%s\nN-1:\n%s",
			strings.Join(freshFingerprint, "\n"),
			strings.Join(nMinusOneFingerprint, "\n"),
		)
	}
}

func TestMigrationLeaderElectionAndRelease(t *testing.T) {
	fixture := newPostgresFixture(t)
	leader := fixture.connect(t)
	contender := fixture.connect(t)

	const versionIndependentLockSeed int64 = 0x454c4d44
	if migrationLockSeed != versionIndependentLockSeed {
		t.Fatalf("migration lock seed = %d, deployment-stable seed = %d", migrationLockSeed, versionIndependentLockSeed)
	}

	lockKey, err := migrationAdvisoryLockKey(t.Context(), leader)
	if err != nil {
		t.Fatalf("migrationAdvisoryLockKey() error = %v", err)
	}
	contenderLockKey, err := migrationAdvisoryLockKey(t.Context(), contender)
	if err != nil {
		t.Fatalf("contender migrationAdvisoryLockKey() error = %v", err)
	}
	if contenderLockKey != lockKey {
		t.Fatalf("same-schema lock keys differ: leader=%d contender=%d", lockKey, contenderLockKey)
	}
	var rollingV2Key int64
	if err := leader.QueryRow(t.Context(), `
SELECT hashtextextended(
    jsonb_build_array(current_database(), current_schema())::text,
    $1
)
`, versionIndependentLockSeed).Scan(&rollingV2Key); err != nil {
		t.Fatalf("derive rolling v2 lock key: %v", err)
	}
	if rollingV2Key != lockKey {
		t.Fatalf("rolling v2/v3 lock keys differ: v2=%d v3=%d", rollingV2Key, lockKey)
	}

	var locked bool
	if err := leader.QueryRow(
		t.Context(),
		"SELECT pg_try_advisory_lock($1)",
		lockKey,
	).Scan(&locked); err != nil {
		t.Fatalf("leader pg_try_advisory_lock() error = %v", err)
	}
	if !locked {
		t.Fatal("leader did not acquire migration advisory lock")
	}

	start := make(chan struct{})
	leaderResult := make(chan error, 1)
	contenderResult := make(chan error, 1)
	go func() {
		<-start
		leaderResult <- Migrate(t.Context(), leader)
	}()
	go func() {
		<-start
		contenderResult <- Migrate(t.Context(), contender)
	}()
	close(start)

	leaderErr := <-leaderResult
	contenderErr := <-contenderResult

	var unlocked bool
	if err := leader.QueryRow(
		t.Context(),
		"SELECT pg_advisory_unlock($1)",
		lockKey,
	).Scan(&unlocked); err != nil {
		t.Fatalf("leader pg_advisory_unlock() error = %v", err)
	}
	if !unlocked {
		t.Fatal("leader did not release migration advisory lock")
	}

	if leaderErr != nil {
		t.Fatalf("leader Migrate() error = %v", leaderErr)
	}
	var lockErr *MigrationLockError
	if !errors.As(contenderErr, &lockErr) {
		t.Fatalf("contender Migrate() error = %v, want *MigrationLockError", contenderErr)
	}
	if lockErr.Key != lockKey {
		t.Fatalf("MigrationLockError.Key = %d, want %d", lockErr.Key, lockKey)
	}

	if err := Migrate(t.Context(), contender); err != nil {
		t.Fatalf("post-release contender Migrate() error = %v", err)
	}
}

func TestMigrationReleasesLockAfterFailure(t *testing.T) {
	fixture := newPostgresFixture(t)
	first := fixture.connect(t)
	second := fixture.connect(t)
	execOK(t, first, "CREATE TABLE source (collision integer PRIMARY KEY)")

	firstErr := Migrate(t.Context(), first)
	if firstErr == nil {
		t.Fatal("first Migrate() unexpectedly succeeded with a conflicting source table")
	}
	var lockErr *MigrationLockError
	if errors.As(firstErr, &lockErr) {
		t.Fatalf("first Migrate() error = %v, want migration execution error", firstErr)
	}

	secondErr := Migrate(t.Context(), second)
	if secondErr == nil {
		t.Fatal("second Migrate() unexpectedly succeeded with a conflicting source table")
	}
	if errors.As(secondErr, &lockErr) {
		t.Fatalf("second Migrate() error = %v; first failure did not release advisory lock", secondErr)
	}
}

func TestMigrationRuntimeSchemaCheckFailsClosed(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := conn.Exec(
		t.Context(),
		"INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)",
		MaximumSupportedSchemaVersion+1,
	); err != nil {
		t.Fatalf("inserting future schema version: %v", err)
	}

	err := CheckSchema(t.Context(), conn)
	var versionErr *SchemaVersionError
	if !errors.As(err, &versionErr) {
		t.Fatalf("CheckSchema() error = %v, want *SchemaVersionError", err)
	}
	if versionErr.Found != MaximumSupportedSchemaVersion+1 {
		t.Fatalf(
			"SchemaVersionError.Found = %d, want %d",
			versionErr.Found,
			MaximumSupportedSchemaVersion+1,
		)
	}
}

func TestTemporalOverlapConstraints(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	const sourceID = "00000000-0000-0000-0000-000000000001"
	const instrumentID = "00000000-0000-0000-0000-000000000002"
	const mapperID = "00000000-0000-0000-0000-000000000003"

	execOK(t, conn, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES ($1, 'venue', 'product', 'api', 'test', 'active')
`, sourceID)

	execOK(t, conn, `
INSERT INTO source_version (
    source_id, valid_from, valid_to, official_api_version, documentation_uri,
    endpoints, topology, entitlement, region, rate_contract, heartbeat_policy,
    acknowledgement_policy, reconnect_policy
) VALUES (
    $1, '2025-01-01T00:00:00Z', '2025-02-01T00:00:00Z', 'v1',
    'https://example.invalid/docs', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
    'test-region', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb
)
`, sourceID)
	assertExclusionViolation(t, conn, `
INSERT INTO source_version (
    source_id, valid_from, valid_to, official_api_version, documentation_uri,
    endpoints, topology, entitlement, region, rate_contract, heartbeat_policy,
    acknowledgement_policy, reconnect_policy
) VALUES (
    $1, '2025-01-15T00:00:00Z', '2025-03-01T00:00:00Z', 'v2',
    'https://example.invalid/docs-v2', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
    'test-region', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb
)
`, sourceID)

	execOK(t, conn, `
INSERT INTO channel_contract (
    source_id, channel_id, valid_from, valid_to, native_selector, channel_role,
    data_family, cadence_source, aggregation, depth, sequence_rules,
    checksum_rules, payload_schema, support_state
) VALUES (
    $1, 'trades', '2025-01-01T00:00:00Z', '2025-02-01T00:00:00Z',
    '{}'::jsonb, 'primary', 'trade', 'exchange', '{}'::jsonb, '{}'::jsonb,
    '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'supported'
)
`, sourceID)
	assertExclusionViolation(t, conn, `
INSERT INTO channel_contract (
    source_id, channel_id, valid_from, valid_to, native_selector, channel_role,
    data_family, cadence_source, aggregation, depth, sequence_rules,
    checksum_rules, payload_schema, support_state
) VALUES (
    $1, 'trades', '2025-01-15T00:00:00Z', '2025-04-01T00:00:00Z',
    '{}'::jsonb, 'primary', 'trade', 'exchange', '{}'::jsonb, '{}'::jsonb,
    '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'supported'
)
`, sourceID)

	execOK(t, conn, `
INSERT INTO instrument (
    instrument_uid, source_id, native_id, listing_epoch, first_observed_at
) VALUES ($1, $2, 'BTC-USD', 1, '2025-01-01T00:00:00Z')
`, instrumentID, sourceID)
	execOK(t, conn, `
INSERT INTO instrument_version (
    instrument_uid, valid_from, valid_to, aliases, lifecycle_state, base_asset,
    quote_asset, instrument_kind, payoff, multiplier, tick_rules, lot_rules,
    raw_metadata, raw_metadata_hash, normalized_schema_version
) VALUES (
    $1, '2025-01-01T00:00:00Z', '2025-02-01T00:00:00Z', '[]'::jsonb,
    'active', 'BTC', 'USD', 'spot', '{}'::jsonb, 1, '{}'::jsonb, '{}'::jsonb,
    '{}'::jsonb, decode(repeat('01', 32), 'hex'), 'v1'
)
`, instrumentID)
	assertExclusionViolation(t, conn, `
INSERT INTO instrument_version (
    instrument_uid, valid_from, valid_to, aliases, lifecycle_state, base_asset,
    quote_asset, instrument_kind, payoff, multiplier, tick_rules, lot_rules,
    raw_metadata, raw_metadata_hash, normalized_schema_version
) VALUES (
    $1, '2025-01-15T00:00:00Z', '2025-04-01T00:00:00Z', '[]'::jsonb,
    'active', 'BTC', 'USD', 'spot', '{}'::jsonb, 1, '{}'::jsonb, '{}'::jsonb,
    '{}'::jsonb, decode(repeat('02', 32), 'hex'), 'v1'
)
`, instrumentID)

	execOK(t, conn, `
INSERT INTO instrument_alias (
    source_id, alias, valid_from, valid_to, instrument_uid
) VALUES ($1, 'BTCUSD', '2025-01-01T00:00:00Z', '2025-02-01T00:00:00Z', $2)
`, sourceID, instrumentID)
	assertExclusionViolation(t, conn, `
INSERT INTO instrument_alias (
    source_id, alias, valid_from, valid_to, instrument_uid
) VALUES ($1, 'BTCUSD', '2025-01-15T00:00:00Z', '2025-04-01T00:00:00Z', $2)
`, sourceID, instrumentID)

	execOK(t, conn, `
INSERT INTO mapper_release (
    mapper_release_id, mapper_version, build_identity, normalized_schema_version,
    fixture_bundle_hash, source_code_identity
) VALUES (
    $1, 'mapper-v1', 'build-1', 'v1', decode(repeat('03', 32), 'hex'), 'commit-1'
)
`, mapperID)
	execOK(t, conn, `
INSERT INTO mapper_binding (
    source_id, channel_id, effective_from, effective_to, mapper_release_id,
    schema_fingerprints, dual_run_evidence, state
) VALUES (
    $1, 'trades', '2025-01-01T00:00:00Z', '2025-02-01T00:00:00Z', $2,
    '[]'::jsonb, '{}'::jsonb, 'active'
)
`, sourceID, mapperID)
	assertExclusionViolation(t, conn, `
INSERT INTO mapper_binding (
    source_id, channel_id, effective_from, effective_to, mapper_release_id,
    schema_fingerprints, dual_run_evidence, state
) VALUES (
    $1, 'trades', '2025-01-15T00:00:00Z', '2025-04-01T00:00:00Z', $2,
    '[]'::jsonb, '{}'::jsonb, 'active'
)
`, sourceID, mapperID)
}

func TestTemporalOpportunitySpillOverlap(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	insertDatasetPartition(t, conn, "00000000-0000-0000-0000-000000000101", "one", "11")
	insertDatasetPartition(t, conn, "00000000-0000-0000-0000-000000000102", "two", "22")

	execOK(t, conn, `
INSERT INTO opportunity_spill (
    opportunity_spill_id, ledger_partition,
    boundary_from_time_ns, boundary_from_opportunity_id,
    boundary_through_time_ns, boundary_through_opportunity_id,
    dataset_partition_id, parquet_manifest_hash, state, committed_at
) VALUES (
    '00000000-0000-0000-0000-000000000201', 'quality-v1',
    100, '00000000-0000-0000-0000-000000000001',
    200, '00000000-0000-0000-0000-000000000009',
    '00000000-0000-0000-0000-000000000101', decode(repeat('31', 32), 'hex'),
    'committed', now()
)
`)
	assertExclusionViolation(t, conn, `
INSERT INTO opportunity_spill (
    opportunity_spill_id, ledger_partition,
    boundary_from_time_ns, boundary_from_opportunity_id,
    boundary_through_time_ns, boundary_through_opportunity_id,
    dataset_partition_id, parquet_manifest_hash, state, committed_at
) VALUES (
    '00000000-0000-0000-0000-000000000202', 'quality-v1',
    150, '00000000-0000-0000-0000-000000000001',
    250, '00000000-0000-0000-0000-000000000009',
    '00000000-0000-0000-0000-000000000102', decode(repeat('32', 32), 'hex'),
    'committed', now()
)
`)
}

func TestMigrationCommittedIdentityConstraints(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	const sourceID = "00000000-0000-0000-0000-000000000301"
	execOK(t, conn, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES ($1, 'venue', 'product', 'api', 'test', 'active')
`, sourceID)
	execOK(t, conn, `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id, receive_time_start_ns,
    receive_time_end_ns, ordinal_start, ordinal_end, object_key, content_hash,
    byte_length, state, committed_at
) VALUES (
    '00000000-0000-0000-0000-000000000302', $1, 'trades',
    '00000000-0000-0000-0000-000000000303', 100, 200, 1, 10,
    'raw/one', decode(repeat('41', 32), 'hex'), 10, 'committed', now()
)
`, sourceID)
	assertPostgresCode(t, conn, "23505", `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id, receive_time_start_ns,
    receive_time_end_ns, ordinal_start, ordinal_end, object_key, content_hash,
    byte_length, state, committed_at
) VALUES (
    '00000000-0000-0000-0000-000000000304', $1, 'trades',
    '00000000-0000-0000-0000-000000000303', 100, 200, 1, 10,
    'raw/two', decode(repeat('42', 32), 'hex'), 10, 'committed', now()
)
`, sourceID)

	insertDatasetPartition(t, conn, "00000000-0000-0000-0000-000000000305", "canonical-a", "51")
	assertPostgresCode(t, conn, "23505", `
INSERT INTO dataset_partition (
    dataset_partition_id, dataset_family, dataset_version, partition_key,
    range_start_ns, range_end_ns, input_segment_set_hash, catalog_snapshot_hash,
    mapper_set_hash, logical_hash, physical_hash, object_key, canonical, state,
    committed_at
) VALUES (
    '00000000-0000-0000-0000-000000000306', 'quality/v1', 'v1', 'p0', 0, 100,
    decode(repeat('51', 32), 'hex'), decode(repeat('52', 32), 'hex'),
    decode(repeat('53', 32), 'hex'), decode(repeat('61', 32), 'hex'),
    decode(repeat('62', 32), 'hex'), 'dataset/canonical-b', true, 'committed', now()
)
`)
}

func TestMigrationPublicationAndOwnershipConstraints(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	const sourceA = "00000000-0000-0000-0000-000000000401"
	const sourceB = "00000000-0000-0000-0000-000000000402"
	execOK(t, conn, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES
    ($1, 'venue-a', 'product', 'api', 'test', 'active'),
    ($2, 'venue-b', 'product', 'api', 'test', 'active')
`, sourceA, sourceB)

	execOK(t, conn, `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id, receive_time_start_ns,
    receive_time_end_ns, ordinal_start, ordinal_end, object_key, content_hash,
    byte_length, state
) VALUES (
    '00000000-0000-0000-0000-000000000403', $1, 'trades',
    '00000000-0000-0000-0000-000000000404', 100, 200, 1, 10,
    'raw/verified', decode(repeat('71', 32), 'hex'), 10, 'verified'
)
`, sourceA)
	execOK(t, conn, `
UPDATE raw_segment
SET state = 'committed', committed_at = '2025-01-01T00:01:00Z'
WHERE raw_segment_id = '00000000-0000-0000-0000-000000000403'
`)
	assertPostgresCode(t, conn, "23514", `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id, receive_time_start_ns,
    receive_time_end_ns, ordinal_start, ordinal_end, object_key, content_hash,
    byte_length, state
) VALUES (
    '00000000-0000-0000-0000-000000000405', $1, 'trades',
    '00000000-0000-0000-0000-000000000406', 100, 200, 1, 10,
    'raw/invalid-state', decode(repeat('72', 32), 'hex'), 10, 'uploaded'
)
`, sourceA)

	execOK(t, conn, `
INSERT INTO instrument (
    instrument_uid, source_id, native_id, listing_epoch, first_observed_at
) VALUES (
    '00000000-0000-0000-0000-000000000407', $1, 'BTC-USD', 1,
    '2025-01-01T00:00:00Z'
)
`, sourceB)
	assertPostgresCode(t, conn, "23503", `
INSERT INTO instrument_alias (
    source_id, alias, valid_from, instrument_uid
) VALUES (
    $1, 'BTCUSD', '2025-01-01T00:00:00Z',
    '00000000-0000-0000-0000-000000000407'
)
`, sourceA)

	execOK(t, conn, `
INSERT INTO collection_lease (
    source_id, channel_id, lease_id, writer_id, acquired_at, expires_at
) VALUES (
    $1, 'trades', '00000000-0000-0000-0000-000000000408', 'writer-a',
    '2025-01-01T00:00:00Z', '2025-01-01T01:00:00Z'
)
`, sourceA)
	assertPostgresCode(t, conn, "23503", `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id, receive_time_start_ns,
    receive_time_end_ns, ordinal_start, ordinal_end, object_key, content_hash,
    byte_length, state, writer_lease_id
) VALUES (
    '00000000-0000-0000-0000-000000000409', $1, 'book',
    '00000000-0000-0000-0000-000000000410', 100, 200, 1, 10,
    'raw/wrong-lease', decode(repeat('73', 32), 'hex'), 10, 'verified',
    '00000000-0000-0000-0000-000000000408'
)
`, sourceA)

	execOK(t, conn, `
INSERT INTO dataset_partition (
    dataset_partition_id, dataset_family, dataset_version, partition_key,
    range_start_ns, range_end_ns, input_segment_set_hash, catalog_snapshot_hash,
    mapper_set_hash, logical_hash, physical_hash, object_key, canonical, state
) VALUES (
    '00000000-0000-0000-0000-000000000411', 'quality/v1', 'v1', 'pending', 0, 100,
    decode(repeat('81', 32), 'hex'), decode(repeat('82', 32), 'hex'),
    decode(repeat('83', 32), 'hex'), decode(repeat('84', 32), 'hex'),
    decode(repeat('85', 32), 'hex'), 'dataset/pending', false, 'pending'
)
`)
	assertPostgresCode(t, conn, "23503", `
INSERT INTO opportunity_spill (
    opportunity_spill_id, ledger_partition,
    boundary_from_time_ns, boundary_from_opportunity_id,
    boundary_through_time_ns, boundary_through_opportunity_id,
    dataset_partition_id, parquet_manifest_hash, state, committed_at
) VALUES (
    '00000000-0000-0000-0000-000000000412', 'quality-v1',
    100, '00000000-0000-0000-0000-000000000001',
    200, '00000000-0000-0000-0000-000000000009',
    '00000000-0000-0000-0000-000000000411',
    decode(repeat('86', 32), 'hex'), 'committed', now()
)
`)
}

func newPostgresFixture(t *testing.T) *postgresFixture {
	t.Helper()

	dsn := os.Getenv(postgresDSNEnvironment)
	if dsn == "" {
		t.Skipf(
			"%s is not set; integration test requires an explicitly provisioned ephemeral PostgreSQL database",
			postgresDSNEnvironment,
		)
	}
	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connecting to explicit PostgreSQL test DSN: %v", err)
	}

	schema := fmt.Sprintf("catalog_test_%d_%d", os.Getpid(), schemaSequence.Add(1))
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+quotedSchema); err != nil {
		_ = admin.Close(context.Background())
		t.Fatalf("creating isolated PostgreSQL schema %s: %v", schema, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("dropping isolated PostgreSQL schema %s: %v", schema, err)
		}
		if err := admin.Close(cleanupCtx); err != nil {
			t.Errorf("closing PostgreSQL cleanup connection: %v", err)
		}
	})

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing explicit PostgreSQL test DSN: %v", err)
	}
	config.RuntimeParams["search_path"] = quotedSchema + ",public"
	return &postgresFixture{config: config}
}

func (f *postgresFixture) connect(t *testing.T) *pgx.Conn {
	t.Helper()

	conn, err := pgx.ConnectConfig(t.Context(), f.config.Copy())
	if err != nil {
		t.Fatalf("connecting to isolated PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("closing isolated PostgreSQL connection: %v", err)
		}
	})
	return conn
}

func createVersionZero(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	execOK(t, conn, `
CREATE TABLE goose_db_version (
    id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    version_id bigint NOT NULL,
    is_applied boolean NOT NULL,
    tstamp timestamp NOT NULL DEFAULT now()
)
`)
	execOK(t, conn, "INSERT INTO goose_db_version (version_id, is_applied) VALUES (0, true)")
}

func catalogTables(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()

	rows, err := conn.Query(t.Context(), `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
ORDER BY table_name
`)
	if err != nil {
		t.Fatalf("querying catalog tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scanning catalog table: %v", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading catalog table rows: %v", err)
	}
	return tables
}

func schemaFingerprint(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()

	rows, err := conn.Query(t.Context(), `
SELECT descriptor
FROM (
    SELECT format(
        'column|%s|%s|%s|%s|%s|%s|%s',
        table_name, ordinal_position, column_name, data_type, udt_name,
        is_nullable, COALESCE(column_default, '')
    ) AS descriptor
    FROM information_schema.columns
    WHERE table_schema = current_schema()

    UNION ALL

    SELECT format(
        'constraint|%s|%s|%s|%s',
        c.relname, con.conname, con.contype, pg_get_constraintdef(con.oid, true)
    )
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()

    UNION ALL

    SELECT format(
        'index|%s|%s|%s',
        table_class.relname, index_class.relname,
        replace(pg_get_indexdef(index_class.oid), current_schema() || '.', '<schema>.')
    )
    FROM pg_index idx
    JOIN pg_class table_class ON table_class.oid = idx.indrelid
    JOIN pg_class index_class ON index_class.oid = idx.indexrelid
    JOIN pg_namespace n ON n.oid = table_class.relnamespace
    WHERE n.nspname = current_schema()

    UNION ALL

    SELECT format(
        'trigger|%s|%s|%s',
        c.relname, tg.tgname,
        replace(pg_get_triggerdef(tg.oid, true), current_schema() || '.', '<schema>.')
    )
    FROM pg_trigger tg
    JOIN pg_class c ON c.oid = tg.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND NOT tg.tgisinternal

    UNION ALL

    SELECT format('type|%s|%s', typ.typname, typ.typtype)
    FROM pg_type typ
    JOIN pg_namespace n ON n.oid = typ.typnamespace
    WHERE n.nspname = current_schema() AND typ.typrelid = 0
) objects
ORDER BY descriptor
`)
	if err != nil {
		t.Fatalf("querying catalog schema fingerprint: %v", err)
	}
	defer rows.Close()

	var fingerprint []string
	for rows.Next() {
		var descriptor string
		if err := rows.Scan(&descriptor); err != nil {
			t.Fatalf("scanning catalog schema fingerprint: %v", err)
		}
		fingerprint = append(fingerprint, descriptor)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading catalog schema fingerprint rows: %v", err)
	}
	return fingerprint
}

func insertDatasetPartition(t *testing.T, conn *pgx.Conn, id, objectSuffix, hashPrefix string) {
	t.Helper()

	execOK(t, conn, `
INSERT INTO dataset_partition (
    dataset_partition_id, dataset_family, dataset_version, partition_key,
    range_start_ns, range_end_ns, input_segment_set_hash, catalog_snapshot_hash,
    mapper_set_hash, logical_hash, physical_hash, object_key, canonical, state,
    committed_at
) VALUES (
    $1, 'quality/v1', 'v1', 'p0', 0, 100,
    decode(repeat($2, 32), 'hex'), decode(repeat('52', 32), 'hex'),
    decode(repeat('53', 32), 'hex'), decode(repeat($2, 32), 'hex'),
    decode(repeat($3, 32), 'hex'), $4, true, 'committed', now()
)
`, id, hashPrefix, nextHexByte(hashPrefix), "dataset/"+objectSuffix)
}

func nextHexByte(value string) string {
	if len(value) != 2 {
		panic("test hash prefix must contain exactly one byte")
	}
	var byteValue uint8
	if _, err := fmt.Sscanf(value, "%02x", &byteValue); err != nil {
		panic(fmt.Sprintf("invalid test hash prefix %q: %v", value, err))
	}
	return fmt.Sprintf("%02x", byteValue+1)
}

func execOK(t *testing.T, conn *pgx.Conn, sql string, arguments ...any) {
	t.Helper()
	if _, err := conn.Exec(t.Context(), sql, arguments...); err != nil {
		t.Fatalf("executing PostgreSQL fixture statement: %v", err)
	}
}

func assertExclusionViolation(t *testing.T, conn *pgx.Conn, sql string, arguments ...any) {
	t.Helper()
	assertPostgresCode(t, conn, "23P01", sql, arguments...)
}

func assertPostgresCode(
	t *testing.T,
	conn *pgx.Conn,
	wantCode string,
	sql string,
	arguments ...any,
) {
	t.Helper()

	_, err := conn.Exec(t.Context(), sql, arguments...)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("PostgreSQL statement error = %v, want SQLSTATE %s", err, wantCode)
	}
	if pgErr.Code != wantCode {
		t.Fatalf("PostgreSQL statement SQLSTATE = %s (%v), want %s", pgErr.Code, pgErr, wantCode)
	}
}
