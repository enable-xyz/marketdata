package catalog

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// MinimumSupportedSchemaVersion and MaximumSupportedSchemaVersion are the
	// closed schema interval accepted by this build at runtime.
	MinimumSupportedSchemaVersion int64 = 6
	MaximumSupportedSchemaVersion int64 = 6

	// migrationLockSeed is deployment-wide and version-independent. Never
	// change it when adding a schema migration: rolling builds must contend.
	migrationLockSeed int64 = 0x454c4d44
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// SchemaInterval is an inclusive interval of catalog schema versions.
type SchemaInterval struct {
	Minimum int64
	Maximum int64
}

// SupportedSchemaInterval returns the exact schema interval accepted by this
// package. Returning a value prevents callers from mutating package state.
func SupportedSchemaInterval() SchemaInterval {
	return SchemaInterval{
		Minimum: MinimumSupportedSchemaVersion,
		Maximum: MaximumSupportedSchemaVersion,
	}
}

// MigrationLockError reports that another session is the migration leader.
type MigrationLockError struct {
	Key int64
}

func (e *MigrationLockError) Error() string {
	return fmt.Sprintf("catalog migration advisory lock %d is held by another session", e.Key)
}

// SchemaVersionError reports a database schema outside the interval supported
// by this build.
type SchemaVersionError struct {
	Found   int64
	Minimum int64
	Maximum int64
}

func (e *SchemaVersionError) Error() string {
	return fmt.Sprintf(
		"catalog schema version %d is incompatible with supported interval [%d, %d]",
		e.Found,
		e.Minimum,
		e.Maximum,
	)
}

type migration struct {
	version int64
	name    string
	sql     string
}

type historyEntry struct {
	version   int64
	isApplied bool
}

// Migrate applies all embedded, forward-only migrations on conn. Migration
// leadership is session-scoped, so callers must not use conn concurrently.
func Migrate(ctx context.Context, conn *pgx.Conn) (err error) {
	if conn == nil {
		return errors.New("migrating catalog: nil PostgreSQL connection")
	}

	migrations, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("loading catalog migrations: %w", err)
	}

	lockKey, err := migrationAdvisoryLockKey(ctx, conn)
	if err != nil {
		return err
	}

	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&locked); err != nil {
		return fmt.Errorf("acquiring catalog migration advisory lock: %w", err)
	}
	if !locked {
		return &MigrationLockError{Key: lockKey}
	}

	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		var unlocked bool
		unlockErr := conn.QueryRow(
			unlockCtx,
			"SELECT pg_advisory_unlock($1)",
			lockKey,
		).Scan(&unlocked)
		if unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("releasing catalog migration advisory lock: %w", unlockErr))
			return
		}
		if !unlocked {
			err = errors.Join(err, errors.New("releasing catalog migration advisory lock: lock was not held"))
		}
	}()

	if err := initializeVersionTable(ctx, conn); err != nil {
		return err
	}

	history, err := readMigrationHistory(ctx, conn)
	if err != nil {
		return err
	}
	current, err := validateHistory(history, migrations)
	if err != nil {
		return err
	}
	if current > MaximumSupportedSchemaVersion {
		return incompatibleSchema(current)
	}

	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			return err
		}
		current = m.version
	}

	if current < MinimumSupportedSchemaVersion || current > MaximumSupportedSchemaVersion {
		return incompatibleSchema(current)
	}
	return nil
}

func migrationAdvisoryLockKey(ctx context.Context, conn *pgx.Conn) (int64, error) {
	var key int64
	if err := conn.QueryRow(ctx, `
SELECT hashtextextended(
    jsonb_build_array(current_database(), current_schema())::text,
    $1
)
`, migrationLockSeed).Scan(&key); err != nil {
		return 0, fmt.Errorf("deriving catalog migration advisory lock key: %w", err)
	}
	return key, nil
}

// SchemaVersion returns the current Goose-compatible catalog schema version.
// A missing or malformed version table is an error; it is never treated as a
// compatible empty schema.
func SchemaVersion(ctx context.Context, conn *pgx.Conn) (int64, error) {
	if conn == nil {
		return 0, errors.New("reading catalog schema version: nil PostgreSQL connection")
	}

	migrations, err := loadMigrations()
	if err != nil {
		return 0, fmt.Errorf("loading catalog migrations: %w", err)
	}
	history, err := readMigrationHistory(ctx, conn)
	if err != nil {
		return 0, err
	}
	return validateHistory(history, migrations)
}

// CheckSchema fails closed unless the runtime schema is inside the exact
// supported interval.
func CheckSchema(ctx context.Context, conn *pgx.Conn) error {
	version, err := SchemaVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("checking catalog schema compatibility: %w", err)
	}
	if version < MinimumSupportedSchemaVersion || version > MaximumSupportedSchemaVersion {
		return incompatibleSchema(version)
	}
	return nil
}

func incompatibleSchema(version int64) error {
	return &SchemaVersionError{
		Found:   version,
		Minimum: MinimumSupportedSchemaVersion,
		Maximum: MaximumSupportedSchemaVersion,
	}
}

func loadMigrations() ([]migration, error) {
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, fmt.Errorf("enumerating embedded migrations: %w", err)
	}
	if len(names) == 0 {
		return nil, errors.New("no embedded migrations")
	}
	slices.Sort(names)

	migrations := make([]migration, 0, len(names))
	var previous int64
	for _, name := range names {
		base := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		prefix, _, ok := strings.Cut(base, "_")
		if !ok || prefix == "" {
			return nil, fmt.Errorf("migration %q has no numeric version prefix", name)
		}
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migration %q has invalid version %q", name, prefix)
		}
		if version != previous+1 {
			return nil, fmt.Errorf(
				"migration %q has version %d; expected contiguous version %d",
				name,
				version,
				previous+1,
			)
		}

		contents, err := migrationFiles.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("reading embedded migration %q: %w", name, err)
		}
		sql, err := forwardSQL(string(contents))
		if err != nil {
			return nil, fmt.Errorf("parsing embedded migration %q: %w", name, err)
		}
		migrations = append(migrations, migration{version: version, name: name, sql: sql})
		previous = version
	}

	if previous != MaximumSupportedSchemaVersion {
		return nil, fmt.Errorf(
			"latest embedded migration is %d, but maximum supported schema is %d",
			previous,
			MaximumSupportedSchemaVersion,
		)
	}
	return migrations, nil
}

func forwardSQL(contents string) (string, error) {
	const up = "-- +goose Up"
	const down = "-- +goose Down"

	upAt := strings.Index(contents, up)
	if upAt < 0 {
		return "", errors.New("missing -- +goose Up annotation")
	}
	if strings.Contains(contents[:upAt], down) {
		return "", errors.New("-- +goose Down precedes -- +goose Up")
	}

	sql := contents[upAt+len(up):]
	if downAt := strings.Index(sql, down); downAt >= 0 {
		return "", errors.New("forward-only migration contains -- +goose Down")
	}
	if strings.TrimSpace(sql) == "" {
		return "", errors.New("empty forward migration")
	}
	return sql, nil
}

func initializeVersionTable(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting catalog version-table transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS goose_db_version (
    id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    version_id bigint NOT NULL,
    is_applied boolean NOT NULL,
    tstamp timestamp NOT NULL DEFAULT now()
)`); err != nil {
		return fmt.Errorf("creating catalog version table: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO goose_db_version (version_id, is_applied)
SELECT 0, true
WHERE NOT EXISTS (SELECT 1 FROM goose_db_version)
`); err != nil {
		return fmt.Errorf("initializing catalog version table: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing catalog version-table transaction: %w", err)
	}
	return nil
}

func readMigrationHistory(ctx context.Context, conn *pgx.Conn) ([]historyEntry, error) {
	rows, err := conn.Query(ctx, `
SELECT DISTINCT ON (version_id) version_id, is_applied
FROM goose_db_version
ORDER BY version_id, id DESC
`)
	if err != nil {
		return nil, fmt.Errorf("reading catalog migration history: %w", err)
	}
	defer rows.Close()

	var history []historyEntry
	for rows.Next() {
		var entry historyEntry
		if err := rows.Scan(&entry.version, &entry.isApplied); err != nil {
			return nil, fmt.Errorf("scanning catalog migration history: %w", err)
		}
		history = append(history, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading catalog migration history rows: %w", err)
	}
	if len(history) == 0 {
		return nil, errors.New("catalog migration history is empty")
	}
	return history, nil
}

func validateHistory(history []historyEntry, migrations []migration) (int64, error) {
	latest := make(map[int64]bool, len(history))
	var highestRecorded int64
	for _, entry := range history {
		if entry.version < 0 {
			return 0, fmt.Errorf("catalog migration history contains negative version %d", entry.version)
		}
		latest[entry.version] = entry.isApplied
		highestRecorded = max(highestRecorded, entry.version)
	}
	if applied, ok := latest[0]; !ok || !applied {
		return 0, errors.New("catalog migration history is missing applied version 0")
	}
	if highestRecorded > MaximumSupportedSchemaVersion {
		return 0, incompatibleSchema(highestRecorded)
	}

	var current int64
	for _, m := range migrations {
		applied, recorded := latest[m.version]
		if !recorded || !applied {
			break
		}
		current = m.version
	}
	for version, applied := range latest {
		if version > current && applied {
			return 0, fmt.Errorf(
				"catalog migration history is non-contiguous: version %d is applied after current version %d",
				version,
				current,
			)
		}
	}
	return current, nil
}

func applyMigration(ctx context.Context, conn *pgx.Conn, m migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting catalog migration %d transaction: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, m.sql, pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("applying catalog migration %d (%s): %w", m.version, m.name, err)
	}
	if _, err := tx.Exec(
		ctx,
		"INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)",
		m.version,
	); err != nil {
		return fmt.Errorf("recording catalog migration %d: %w", m.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing catalog migration %d: %w", m.version, err)
	}
	return nil
}
