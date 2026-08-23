package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// LoadLatestSnapshot returns the newest immutable snapshot committed for one
// source. Runtime composition uses it to pin live verification before replay;
// it does not synthesize or mutate catalog state.
func LoadLatestSnapshot(ctx context.Context, database PublicationDatabase, sourceID string) (Snapshot, error) {
	if database == nil || sourceID == "" {
		return Snapshot{}, fmt.Errorf("%w: snapshot source and database are required", ErrInvalidCatalog)
	}
	var snapshot Snapshot
	var digest []byte
	err := database.QueryRow(ctx, `
SELECT snapshot_version, snapshot_sha256, snapshot_bytes, instrument_count
FROM catalog_snapshot
WHERE source_id = $1
ORDER BY first_observed_at DESC, snapshot_sha256 DESC
LIMIT 1
`, sourceID).Scan(&snapshot.Version, &digest, &snapshot.Bytes, &snapshot.InstrumentCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return Snapshot{}, fmt.Errorf("%w: no committed catalog snapshot for source", ErrInvalidCatalog)
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: load latest snapshot: %w", err)
	}
	if snapshot.Version != SnapshotVersion || len(digest) != sha256.Size || len(snapshot.Bytes) == 0 ||
		len(snapshot.Bytes) > MaxCatalogJSONBytes || snapshot.InstrumentCount < 1 || snapshot.InstrumentCount > MaxCatalogInstruments {
		return Snapshot{}, fmt.Errorf("%w: latest snapshot identity or bounds", ErrInvalidCatalog)
	}
	copy(snapshot.SHA256[:], digest)
	if sha256.Sum256(snapshot.Bytes) != snapshot.SHA256 {
		return Snapshot{}, fmt.Errorf("%w: latest snapshot byte hash mismatch", ErrInvalidCatalog)
	}
	return snapshot, nil
}
