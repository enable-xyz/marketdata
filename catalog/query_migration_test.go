package catalog

import (
	"io/fs"
	"testing"
)

func TestQueryProjectionMigrationIsSingleForwardVersion(t *testing.T) {
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	const want = "migrations/00007_query_projections.sql"
	if len(names) != int(RequiredQuerySchemaVersion) || names[len(names)-1] != want {
		t.Fatalf("embedded migrations = %q, want exactly %d ending in %q", names, RequiredQuerySchemaVersion, want)
	}
}
