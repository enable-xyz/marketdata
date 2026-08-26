package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/enable-xyz/marketdata/quality"
)

func TestOpportunitySpillArchive(t *testing.T) {
	generation := testOpportunityGeneration(t)
	t.Run("deterministic round trip and generation lookup", func(t *testing.T) {
		firstRoot, secondRoot := t.TempDir(), t.TempDir()
		first, err := BuildOpportunityArchive(t.Context(), firstRoot, generation, testWriterOptions())
		if err != nil {
			t.Fatalf("first BuildOpportunityArchive() error = %v", err)
		}
		second, err := BuildOpportunityArchive(t.Context(), secondRoot, generation, testWriterOptions())
		if err != nil {
			t.Fatalf("second BuildOpportunityArchive() error = %v", err)
		}
		if first.ManifestHash != second.ManifestHash || first.Manifest.LogicalSHA256 != second.Manifest.LogicalSHA256 ||
			first.Manifest.PhysicalSHA256 != second.Manifest.PhysicalSHA256 || first.Manifest.DatasetPartitionID != second.Manifest.DatasetPartitionID {
			t.Fatalf("opportunity archive identity changed:\nfirst=%+v\nsecond=%+v", first.Manifest, second.Manifest)
		}
		firstBytes, err := os.ReadFile(first.ParquetPath)
		if err != nil {
			t.Fatal(err)
		}
		secondBytes, err := os.ReadFile(second.ParquetPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(firstBytes, secondBytes) {
			t.Fatal("opportunity archive physical bytes are not deterministic")
		}
		store, err := NewOpportunityArchiveStore(firstRoot, testWriterOptions())
		if err != nil {
			t.Fatal(err)
		}
		commit, found, err := store.LookupOpportunityArchive(t.Context(), generation)
		if err != nil || !found {
			t.Fatalf("LookupOpportunityArchive() found=%v error=%v", found, err)
		}
		if err := commit.Validate(generation); err != nil {
			t.Fatalf("archive commit validation error = %v", err)
		}
		verified, err := VerifyOpportunityArchive(t.Context(), firstRoot, first.ManifestPath, generation)
		if err != nil || len(verified.Rows) != len(generation.Rows) {
			t.Fatalf("VerifyOpportunityArchive() rows=%d error=%v", len(verified.Rows), err)
		}
		for index := range verified.Rows {
			wantHash, _ := generation.Rows[index].LogicalHash()
			gotHash, _ := verified.Rows[index].LogicalHash()
			if wantHash != gotHash {
				t.Fatalf("round-trip row %d logical identity changed", index)
			}
		}
	})

	t.Run("crash after parquet before manifest leaves PostgreSQL retryable", func(t *testing.T) {
		root := t.TempDir()
		injected := errors.New("synthetic directory-sync crash")
		options := testWriterOptions()
		calls := 0
		options.DirectorySync = func(string) error {
			calls++
			if calls == 1 {
				return injected
			}
			return nil
		}
		if _, err := BuildOpportunityArchive(t.Context(), root, generation, options); !errors.Is(err, injected) {
			t.Fatalf("injected build error = %v, want %v", err, injected)
		}
		store, err := NewOpportunityArchiveStore(root, testWriterOptions())
		if err != nil {
			t.Fatal(err)
		}
		if _, found, err := store.LookupOpportunityArchive(t.Context(), generation); err != nil || found {
			t.Fatalf("pre-manifest archive visibility found=%v error=%v", found, err)
		}
		commit, err := store.WriteOpportunityArchive(t.Context(), generation)
		if err != nil {
			t.Fatalf("retry WriteOpportunityArchive() error = %v", err)
		}
		if err := commit.Validate(generation); err != nil {
			t.Fatalf("retry commit validation error = %v", err)
		}
		secondCommit, found, err := store.LookupOpportunityArchive(t.Context(), generation)
		if err != nil || !found || secondCommit != commit {
			t.Fatalf("retry lookup found=%v error=%v commit=%+v want=%+v", found, err, secondCommit, commit)
		}
	})

	t.Run("rejects resolved symlink escapes", func(t *testing.T) {
		t.Run("manifest", func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			result, err := BuildOpportunityArchive(t.Context(), root, generation, testWriterOptions())
			if err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(result.ManifestPath)
			if err != nil {
				t.Fatal(err)
			}
			outsideManifest := filepath.Join(outside, "manifest.json")
			if err := os.WriteFile(outsideManifest, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(result.ManifestPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outsideManifest, result.ManifestPath); err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyOpportunityArchive(t.Context(), root, result.ManifestPath, generation); !errors.Is(err, ErrManifestMismatch) {
				t.Fatalf("manifest symlink escape error = %v, want ErrManifestMismatch", err)
			}
		})
		t.Run("parquet", func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			result, err := BuildOpportunityArchive(t.Context(), root, generation, testWriterOptions())
			if err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(result.ParquetPath)
			if err != nil {
				t.Fatal(err)
			}
			outsideParquet := filepath.Join(outside, "archive.parquet")
			if err := os.WriteFile(outsideParquet, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(result.ParquetPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outsideParquet, result.ParquetPath); err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyOpportunityArchive(t.Context(), root, result.ManifestPath, generation); !errors.Is(err, ErrManifestMismatch) {
				t.Fatalf("Parquet symlink escape error = %v, want ErrManifestMismatch", err)
			}
		})
	})

	t.Run("rejects self-consistent content substitution against generation identity", func(t *testing.T) {
		root := t.TempDir()
		tamperedRows := append([]quality.Opportunity(nil), generation.Rows...)
		tamperedRows[0].TerminalOutcome = quality.OpportunityOutcomeUnknown
		tamperedRows[0].TerminalEvidence = []byte(`{"coordinate":"synthetic-substitution"}`)
		request := quality.SpillRequest{
			GenerationID: generation.GenerationID, Partition: generation.Partition,
			ThroughTimeNS: generation.Through.TerminalTimeNS, MaximumRows: len(tamperedRows),
			CatalogSnapshotHash: generation.CatalogSnapshotHash, MapperSetHash: generation.MapperSetHash,
		}
		tamperedGeneration, err := quality.NewSpillGeneration(request, tamperedRows)
		if err != nil {
			t.Fatal(err)
		}
		result, err := BuildOpportunityArchive(t.Context(), root, tamperedGeneration, testWriterOptions())
		if err != nil {
			t.Fatal(err)
		}
		manifest := result.Manifest
		manifest.GenerationFingerprint = hex.EncodeToString(generation.Fingerprint[:])
		logicalHash, err := decodeHash(manifest.LogicalSHA256)
		if err != nil {
			t.Fatal(err)
		}
		physicalHash, err := decodeHash(manifest.PhysicalSHA256)
		if err != nil {
			t.Fatal(err)
		}
		manifest.DatasetPartitionID = opportunityDatasetPartitionID(generation.Fingerprint, logicalHash, physicalHash)
		manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		manifestBytes = append(manifestBytes, '\n')
		if err := os.WriteFile(result.ManifestPath, manifestBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyOpportunityArchive(t.Context(), root, result.ManifestPath, generation); !errors.Is(err, ErrManifestMismatch) {
			t.Fatalf("content substitution error = %v, want ErrManifestMismatch", err)
		}
	})

	t.Run("rejects oversized manifest through bounded read", func(t *testing.T) {
		root := t.TempDir()
		result, err := BuildOpportunityArchive(t.Context(), root, generation, testWriterOptions())
		if err != nil {
			t.Fatal(err)
		}
		oversized := bytes.Repeat([]byte{'x'}, MaxManifestBytes+1)
		if err := os.WriteFile(result.ManifestPath, oversized, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyOpportunityArchive(t.Context(), root, result.ManifestPath, generation); !errors.Is(err, ErrManifestMismatch) {
			t.Fatalf("oversized manifest error = %v, want ErrManifestMismatch", err)
		}
	})
}

func testOpportunityGeneration(t *testing.T) quality.SpillGeneration {
	t.Helper()
	rows := []quality.Opportunity{
		testArchiveOpportunity("00000000-0000-0000-0000-000000000101", 1, quality.OpportunityOutcomeObserved),
		testArchiveOpportunity("00000000-0000-0000-0000-000000000102", 2, quality.OpportunityOutcomeSourceStale),
	}
	request := quality.SpillRequest{
		GenerationID: "00000000-0000-0000-0000-000000000201", Partition: "quality-v1",
		ThroughTimeNS: 2_000, MaximumRows: 10,
		CatalogSnapshotHash: sha256.Sum256([]byte("synthetic-catalog-snapshot")),
		MapperSetHash:       sha256.Sum256([]byte("synthetic-mapper-set")),
	}
	generation, err := quality.NewSpillGeneration(request, rows)
	if err != nil {
		t.Fatal(err)
	}
	return generation
}

func testArchiveOpportunity(id string, offset int64, outcome quality.OpportunityOutcome) quality.Opportunity {
	return quality.Opportunity{OpportunityID: id, LedgerPartition: "quality-v1",
		SourceID: "00000000-0000-0000-0000-000000000001", ChannelID: "synthetic.poll",
		Expectation: quality.OpportunityScheduledRESTPoll, ExpectedTimeNS: 1_000 + offset,
		WindowStartNS: 1_000, WindowEndNS: 2_000, Scope: []byte(`{"request":"synthetic"}`),
		Terminal: true, TerminalTimeNS: 1_500 + offset, TerminalOutcome: outcome,
		TerminalEvidence: []byte(`{"coordinate":"synthetic"}`), CreatedTimeNS: 900 + offset}
}
