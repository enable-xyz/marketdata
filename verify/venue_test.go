package verify

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/objectstore"
)

func TestSpotVerticalSliceFirstRunAndRestart(t *testing.T) {
	cfg := fixtureVerifyConfig(t)
	first, err := RunVenue(t.Context(), "binance-spot", cfg, BuildInfo{Version: "test", Commit: "fixed", Date: "fixed"}, Dependencies{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunVenue(t.Context(), "binance-spot", cfg, BuildInfo{Version: "test", Commit: "fixed", Date: "fixed"}, Dependencies{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("restart changed exact evidence bytes")
	}
	packet, err := unmarshalEvidence(first)
	if err != nil {
		t.Fatal(err)
	}
	if packet.Status != "passed" || packet.GapLifecycleStatus != GapLifecycleDeferred || packet.Mode != config.VerifyModeFixture {
		t.Fatalf("unexpected evidence outcome: %+v", packet)
	}
	if packet.Counts.RawRecords == 0 || packet.Counts.CommittedSegments != 2 || packet.Counts.ReplayDiscontinuities == 0 ||
		len(packet.Discontinuities) == 0 || packet.Counts.Acknowledgements == 0 || packet.Counts.Heartbeats == 0 || packet.Counts.Disconnects == 0 {
		t.Fatalf("raw/control/discontinuity evidence is incomplete: %+v", packet.Counts)
	}
	if packet.Counts.NormalizedRows != 4 || packet.Counts.Trades != 1 || packet.Counts.BookUpdates != 1 ||
		packet.Counts.Quotes != 1 || packet.Counts.Tickers != 1 || packet.Counts.BookSnapshots != 1 ||
		packet.Counts.ParquetPartitions != 4 || len(packet.Datasets) != 4 {
		t.Fatalf("derived evidence is incomplete: %+v", packet.Counts)
	}
	for _, item := range packet.Datasets {
		if _, err := datasetManifestForTest(cfg.Verify.ArtifactRoot, item.ManifestFile); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSpotVerticalSliceRejectsCommittedObjectCorruption(t *testing.T) {
	cfg := fixtureVerifyConfig(t)
	encoded, err := RunVenue(t.Context(), "binance-spot", cfg, BuildInfo{Version: "test", Commit: "fixed", Date: "fixed"}, Dependencies{})
	if err != nil {
		t.Fatal(err)
	}
	packet, err := unmarshalEvidence(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(packet.Segments) == 0 {
		t.Fatal("evidence contains no committed segment")
	}
	path := filepath.Join(cfg.Verify.ArtifactRoot, "objects", filepath.FromSlash(packet.Segments[0].ObjectKey))
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := RunVenue(t.Context(), "binance-spot", cfg, BuildInfo{Version: "test", Commit: "fixed", Date: "fixed"}, Dependencies{}); err == nil {
		t.Fatal("corrupted committed object was accepted")
	}
}

func TestSpotVerticalSliceCrashResumePublicationBoundaries(t *testing.T) {
	for _, boundary := range []string{"object", "verified", "commit"} {
		t.Run(boundary, func(t *testing.T) {
			cfg := fixtureVerifyConfig(t)
			fixture, err := loadFixture(t.Context(), cfg.Verify.FixtureRoot, cfg.Verify.FixtureManifest)
			if err != nil {
				t.Fatal(err)
			}
			objects, err := OpenFileObjectClient(cfg.Verify.ArtifactRoot)
			if err != nil {
				t.Fatal(err)
			}
			publicationCatalog, err := OpenFileCatalog(cfg.Verify.ArtifactRoot)
			if err != nil {
				t.Fatal(err)
			}
			clock, err := capture.NewManualClock(fixture.manifest.StartWallTimeNS, fixture.manifest.ClockEpochID)
			if err != nil {
				t.Fatal(err)
			}
			ws, rest := newFixtureTransports(fixture)
			faultObjects := &failOnceObjects{Client: objects, fail: boundary == "object"}
			faultCatalog := &failOnceCatalog{PublicationCatalog: publicationCatalog, failVerified: boundary == "verified", failCommit: boundary == "commit"}
			runtime := captureRuntime{
				objects: faultObjects, catalog: faultCatalog, ws: ws, rest: rest, clock: clock,
				advance: func() error { return clock.Advance(fixture.manifest.StepNanoseconds) },
				now:     func() time.Time { return time.Unix(0, clock.Read().WallTimeNS).UTC() },
				wsEpoch: fixture.connectionID, restEpochs: [][16]byte{fixture.pollID},
			}
			if _, err := ensureRawCapture(t.Context(), cfg, runtime); err == nil {
				t.Fatalf("%s boundary fault did not interrupt publication", boundary)
			}
			if _, err := ensureRawCapture(t.Context(), cfg, runtime); err != nil {
				t.Fatalf("resume after %s boundary: %v", boundary, err)
			}
			publications, err := committedPublications(t.Context(), publicationCatalog)
			if err != nil {
				t.Fatal(err)
			}
			publications = selectRunPublications(publications, fixture.connectionID, [][16]byte{fixture.pollID})
			if len(publications) != 2 {
				t.Fatalf("resume committed %d segments, want 2", len(publications))
			}
		})
	}
}

func fixtureVerifyConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(filepath.Join("..", "testdata", "config", "binance-spot-verify.yaml"), nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cfg.Verify.SpoolRoot = filepath.Join(root, "spool")
	cfg.Verify.ArtifactRoot = filepath.Join(root, "artifacts")
	if err := os.Mkdir(cfg.Verify.SpoolRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cfg.Verify.ArtifactRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func datasetManifestForTest(artifactRoot, relative string) ([]byte, error) {
	path := filepath.Join(artifactRoot, "datasets", filepath.FromSlash(relative))
	return os.ReadFile(path)
}

type failOnceObjects struct {
	objectstore.Client
	fail bool
}

func (f *failOnceObjects) PutIfAbsent(ctx context.Context, object objectstore.PutObject) error {
	if f.fail {
		f.fail = false
		return errors.New("injected object publication interruption")
	}
	return f.Client.PutIfAbsent(ctx, object)
}

type failOnceCatalog struct {
	PublicationCatalog
	failVerified bool
	failCommit   bool
}

func (f *failOnceCatalog) RecordVerified(ctx context.Context, publication catalog.RawSegmentPublication) error {
	if f.failVerified {
		f.failVerified = false
		return errors.New("injected verified-state interruption")
	}
	return f.PublicationCatalog.RecordVerified(ctx, publication)
}

func (f *failOnceCatalog) CommitRawSegment(ctx context.Context, segmentID string) error {
	if f.failCommit {
		f.failCommit = false
		return errors.New("injected commit-state interruption")
	}
	return f.PublicationCatalog.CommitRawSegment(ctx, segmentID)
}
