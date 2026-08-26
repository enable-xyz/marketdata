package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/warehouse"
)

func TestDisasterRecovery(t *testing.T) {
	spool, config := recoverySpool(t)
	complete := writeRecoveryReady(t, spool, recoveryEnvelope(config, 1))
	uploaded := writeRecoveryReady(t, spool, recoveryEnvelope(config, 2))
	committed := writeRecoveryReady(t, spool, recoveryEnvelope(config, 3))
	corrupted := writeRecoveryReady(t, spool, recoveryEnvelope(config, 4))
	partialWriter, err := spool.NewWriter(recoveryWriterOptions(func(point segment.FaultPoint) error {
		if point == segment.FaultAfterFramePrefix {
			return errors.New("synthetic partial spool crash")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partialWriter.Write(recoveryEnvelope(config, 5)); err != nil {
		t.Fatal(err)
	}
	if _, err := partialWriter.Shutdown(); err == nil {
		t.Fatal("partial writer fault did not fire")
	}

	absentSpool, absentConfig := recoverySpool(t)
	absentReady := writeRecoveryReady(t, absentSpool, recoveryEnvelope(absentConfig, 6))
	committedRecord := recoveryPublication(t, committed, "00000000-0000-0000-0000-000000000103")
	corruptedRecord := recoveryPublication(t, corrupted, "00000000-0000-0000-0000-000000000104")
	absentRecord := recoveryPublication(t, absentReady, "00000000-0000-0000-0000-000000000106")
	snapshot, err := catalog.NewRecoverySnapshot(time.Unix(1_710_000_000, 0), []catalog.RawSegmentPublication{
		committedRecord, corruptedRecord, absentRecord,
	})
	if err != nil {
		t.Fatal(err)
	}

	client := newRecoveryObjectClient()
	client.storeReady(t, uploaded)
	client.storeReady(t, committed)
	client.store(corrupted.Manifest.ObjectKey, []byte("synthetic corrupt immutable object"), sha256.Sum256([]byte("synthetic corrupt immutable object")))
	orphanKey := "raw/v1/source=00000000-0000-0000-0000-000000000001/orphan-synthetic.emseg.zst"
	client.store(orphanKey, []byte("synthetic orphan"), sha256.Sum256([]byte("synthetic orphan")))
	targetCatalog := newRecoveryCatalog()
	publisher, err := objectstore.NewPublisher(client, targetCatalog, objectstore.PublisherConfig{})
	if err != nil {
		t.Fatal(err)
	}
	warehouseReader := newRecoveryManifestReader(3)
	warehouseStore := &recoveryWarehouseStore{}
	loader, err := warehouse.NewLoader(warehouseStore, warehouseReader, "sha256:synthetic-clickhouse",
		warehouse.Config{BatchRows: 2, Compression: warehouse.CompressionLZ4, Layout: warehouse.PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_720_000_000, 0)
	clock := func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	drill, err := NewDrill(spool, publisher, loader, clock)
	if err != nil {
		t.Fatal(err)
	}
	input := DrillInput{
		SpoolRecovery: segment.RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "synthetic-recovery-v1"},
		PublicationIdentities: []PublicationIdentity{
			{ObjectKey: complete.Manifest.ObjectKey, SegmentID: "00000000-0000-0000-0000-000000000101"},
			{ObjectKey: uploaded.Manifest.ObjectKey, SegmentID: "00000000-0000-0000-0000-000000000102"},
		},
		CatalogBackup: snapshot,
		RawPrefix:     "raw/v1/",
		ParquetManifests: []warehouse.CommittedManifest{{Root: "synthetic", ManifestPath: "synthetic.manifest.json",
			ManifestSHA256: warehouseReader.generation.ManifestHash, State: warehouse.ManifestCommitted}},
		StorageMeasurements: []StorageMeasurement{{SourceID: config.SourceID, Family: "trade", ObservedBytes: 1 << 20, ObservedWindow: 24 * time.Hour}},
		BackupDuration:      2 * time.Second,
		Provider: ProviderCapabilities{ProviderID: "synthetic-s3", Versioning: CapabilitySupported,
			Immutability: CapabilitySupported, Replication: CapabilityUnknown, Evidence: "synthetic provider contract fixture"},
		CallerRecoveryRequirements: []string{"caller must freeze recovery objectives after measured evidence"},
	}

	report, err := drill.Run(t.Context(), input)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	states := make(map[CrashState]bool)
	for _, evidence := range report.Evidence {
		states[evidence.State] = true
		if evidence.Resolution == "" || evidence.ArtifactID == "" {
			t.Fatalf("incomplete recovery evidence: %#v", evidence)
		}
	}
	for _, state := range []CrashState{CrashComplete, CrashPartial, CrashUploadedUncommitted, CrashCommitted, CrashCorrupted, CrashAbsent} {
		if !states[state] {
			t.Fatalf("recovery omitted crash state %q: %#v", state, report.Evidence)
		}
	}

	targetCatalog.mu.Lock()
	restored := make([]catalog.RawSegmentPublication, 0, len(targetCatalog.records))
	for _, record := range targetCatalog.records {
		restored = append(restored, record)
	}
	orphanCount := len(targetCatalog.orphans)
	targetCatalog.mu.Unlock()
	if len(restored) != 3 {
		t.Fatalf("restored catalog rows = %d, want complete/uploaded/committed only", len(restored))
	}
	for _, record := range restored {
		if record.State != catalog.RawSegmentCommitted {
			t.Fatalf("restored catalog exposed non-committed row: %#v", record)
		}
		if _, err := objectstore.VerifyRecoveryObject(t.Context(), client, record.ObjectKey, record.ByteLength, record.ContentSHA256); err != nil {
			t.Fatalf("restored catalog points at unverified object %q: %v", record.ObjectKey, err)
		}
	}
	if orphanCount != 2 || !slices.Contains(report.Orphans.Orphans, orphanKey) || !slices.Contains(report.Orphans.Orphans, corrupted.Manifest.ObjectKey) {
		t.Fatalf("orphan reconciliation did not retain exact corrupt/unidentified objects: count=%d report=%#v", orphanCount, report.Orphans)
	}
	if report.Warehouse.Source.EventSetSHA256 != report.Warehouse.Target.EventSetSHA256 ||
		report.Warehouse.Source.LogicalSHA256 != report.Warehouse.Target.LogicalSHA256 ||
		report.Warehouse.Source.AggregateSHA256 != report.Warehouse.Target.AggregateSHA256 {
		t.Fatalf("warehouse rebuild evidence mismatch: %#v", report.Warehouse)
	}
	if report.X8.ProjectionDays != 30 || report.X8.ProjectedBytesTotal != 30*(1<<20) ||
		report.X8.RPO.Status != "unresolved" || report.X8.RTO.Status != "unresolved" ||
		report.X8.RawRetention.Status != "default_indefinite" || report.X8.DeletionPolicy.Status != "unresolved" ||
		report.X8.RawDeletionAuthorized {
		t.Fatalf("X8 fabricated a recovery/deletion decision: %#v", report.X8)
	}

	retried, err := drill.Run(t.Context(), input)
	if err != nil {
		t.Fatalf("idempotent Run() error = %v", err)
	}
	targetCatalog.mu.Lock()
	retriedRecords, retriedOrphans := len(targetCatalog.records), len(targetCatalog.orphans)
	targetCatalog.mu.Unlock()
	if retriedRecords != len(restored) || retriedOrphans != orphanCount ||
		retried.Warehouse.Target.LogicalSHA256 != report.Warehouse.Target.LogicalSHA256 ||
		retried.X8.ProjectedBytesTotal != report.X8.ProjectedBytesTotal {
		t.Fatalf("idempotent retry changed recovery state: records=%d/%d orphans=%d/%d", retriedRecords, len(restored), retriedOrphans, orphanCount)
	}
}

func TestDisasterRecoveryReportsPostReplicaRestoreState(t *testing.T) {
	sourceSpool, config := recoverySpool(t)
	replicaReady := writeRecoveryReady(t, sourceSpool, recoveryEnvelope(config, 7))
	record := recoveryPublication(t, replicaReady, "00000000-0000-0000-0000-000000000107")
	snapshot, err := catalog.NewRecoverySnapshot(time.Unix(1_710_000_000, 0), []catalog.RawSegmentPublication{record})
	if err != nil {
		t.Fatal(err)
	}
	replicaBody, err := os.ReadFile(replicaReady.SegmentPath)
	if err != nil {
		t.Fatal(err)
	}

	client := newRecoveryObjectClient()
	targetCatalog := newRecoveryCatalog()
	publisher, err := objectstore.NewPublisher(client, targetCatalog, objectstore.PublisherConfig{})
	if err != nil {
		t.Fatal(err)
	}
	warehouseReader := newRecoveryManifestReader(1)
	warehouseStore := &recoveryWarehouseStore{}
	loader, err := warehouse.NewLoader(warehouseStore, warehouseReader, "sha256:synthetic-clickhouse",
		warehouse.Config{BatchRows: 2, Compression: warehouse.CompressionLZ4, Layout: warehouse.PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	drillSpool, _ := recoverySpool(t)
	now := time.Unix(1_720_000_000, 0)
	drill, err := NewDrill(drillSpool, publisher, loader, func() time.Time {
		now = now.Add(time.Second)
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	replica := &recoveryReplicaRestorer{
		primary:   client,
		objectKey: record.ObjectKey,
		object:    recoveryStoredObject{body: replicaBody, hash: record.ContentSHA256},
	}
	report, err := drill.Run(t.Context(), DrillInput{
		SpoolRecovery: segment.RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "synthetic-recovery-v1"},
		CatalogBackup: snapshot,
		Replica:       replica,
		RawPrefix:     "raw/v1/",
		ParquetManifests: []warehouse.CommittedManifest{{Root: "synthetic", ManifestPath: "synthetic.manifest.json",
			ManifestSHA256: warehouseReader.generation.ManifestHash, State: warehouse.ManifestCommitted}},
		StorageMeasurements: []StorageMeasurement{{SourceID: config.SourceID, Family: "trade", ObservedBytes: 1 << 20, ObservedWindow: 24 * time.Hour}},
		BackupDuration:      2 * time.Second,
		Provider: ProviderCapabilities{ProviderID: "synthetic-s3", Versioning: CapabilitySupported,
			Immutability: CapabilitySupported, Replication: CapabilitySupported, Evidence: "synthetic replica restore fixture"},
		CallerRecoveryRequirements: []string{"caller must freeze recovery objectives after measured evidence"},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if replica.calls != 1 {
		t.Fatalf("replica restore calls = %d, want 1", replica.calls)
	}
	if len(report.CatalogRestore.Evidence) != 1 {
		t.Fatalf("catalog restore evidence count = %d, want 1", len(report.CatalogRestore.Evidence))
	}
	restoreEvidence := report.CatalogRestore.Evidence[0]
	if restoreEvidence.ObjectKey != record.ObjectKey ||
		restoreEvidence.ObservedState != objectstore.RecoveryObjectAbsent ||
		restoreEvidence.State != objectstore.RecoveryObjectVerified ||
		restoreEvidence.Resolution != "catalog_restored_committed" ||
		!restoreEvidence.RestoredFromReplica || restoreEvidence.PermanentGap || restoreEvidence.Reason != "" {
		t.Fatalf("catalog replica restore evidence = %#v", restoreEvidence)
	}
	flattenedIndex := slices.IndexFunc(report.Evidence, func(evidence RecoveryEvidence) bool {
		return evidence.Kind == ArtifactCatalog && evidence.ArtifactID == record.ObjectKey
	})
	if flattenedIndex < 0 {
		t.Fatalf("flattened catalog evidence missing from %#v", report.Evidence)
	}
	flattened := report.Evidence[flattenedIndex]
	if flattened.State != CrashCommitted || flattened.Resolution != "catalog_restored_committed" ||
		flattened.PermanentGap || flattened.Reason != "" {
		t.Fatalf("flattened replica restore evidence = %#v", flattened)
	}
	if _, err := objectstore.VerifyRecoveryObject(t.Context(), client, record.ObjectKey, record.ByteLength, record.ContentSHA256); err != nil {
		t.Fatalf("post-restore object verification failed: %v", err)
	}
	restored, found, err := targetCatalog.FindRawSegment(t.Context(), record.ObjectKey)
	if err != nil || !found || restored.State != catalog.RawSegmentCommitted {
		t.Fatalf("restored catalog publication = %#v, found=%v, error=%v", restored, found, err)
	}
}

func TestReconcileEvidencePreservesTerminalCatalogState(t *testing.T) {
	tests := []struct {
		name         string
		evidence     objectstore.ReconcileEvidence
		wantState    CrashState
		permanentGap bool
	}{
		{
			name: "quarantined",
			evidence: objectstore.ReconcileEvidence{
				ObjectKey:         "raw/v1/quarantined",
				PriorCatalogState: catalog.RawSegmentQuarantined,
				Outcome:           objectstore.ReconcileOutcomeQuarantined,
				Resolution:        "quarantined_catalog_state_retained",
				ByteLength:        17,
				Reason:            "exact recovery quarantine evidence",
			},
			wantState:    CrashQuarantined,
			permanentGap: true,
		},
		{
			name: "superseded",
			evidence: objectstore.ReconcileEvidence{
				ObjectKey:         "raw/v1/superseded",
				PriorCatalogState: catalog.RawSegmentSuperseded,
				Outcome:           objectstore.ReconcileOutcomeSuperseded,
				Resolution:        "superseded_catalog_state_retained",
				ByteLength:        19,
			},
			wantState: CrashSuperseded,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first, err := recoveryEvidenceForReconcile(test.evidence)
			if err != nil {
				t.Fatalf("recoveryEvidenceForReconcile() error = %v", err)
			}
			retried, err := recoveryEvidenceForReconcile(test.evidence)
			if err != nil {
				t.Fatalf("idempotent recoveryEvidenceForReconcile() error = %v", err)
			}
			if first != retried || first.State != test.wantState || first.PermanentGap != test.permanentGap ||
				first.Resolution != test.evidence.Resolution || first.Reason != test.evidence.Reason {
				t.Fatalf("typed terminal evidence changed on retry: first=%#v retried=%#v", first, retried)
			}
		})
	}

	mismatched := tests[0].evidence
	mismatched.Outcome = objectstore.ReconcileOutcomeCommitted
	if _, err := recoveryEvidenceForReconcile(mismatched); err == nil {
		t.Fatal("quarantined PriorCatalogState accepted committed outcome")
	}
}

func recoverySpool(t *testing.T) (*segment.Spool, segment.SpoolConfig) {
	t.Helper()
	root := t.TempDir()
	var epoch [16]byte
	copy(epoch[:], []byte("synthetic-epoch!"))
	config := segment.SpoolConfig{Root: root, SourceID: "00000000-0000-0000-0000-000000000001",
		ChannelID: "synthetic-trades", EpochKind: segment.EpochConnection, EpochID: epoch}
	spool, err := segment.OpenSpool(config)
	if err != nil {
		t.Fatal(err)
	}
	return spool, config
}

func recoveryWriterOptions(fault segment.FaultInjector) segment.WriterOptions {
	return segment.WriterOptions{FrameBytes: 1 << 20, SegmentBytes: segment.DefaultSegmentBytes,
		MaxAge: segment.DefaultSegmentAge, WriterVersion: "synthetic-writer-v1", Fault: fault}
}

func writeRecoveryReady(t *testing.T, spool *segment.Spool, envelope segment.Envelope) segment.ReadySegment {
	t.Helper()
	writer, err := spool.NewWriter(recoveryWriterOptions(nil))
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := writer.Write(envelope); err != nil || ready != nil {
		t.Fatalf("Write() = %#v, %v", ready, err)
	}
	ready, err := writer.Shutdown()
	if err != nil || ready == nil {
		t.Fatalf("Shutdown() = %#v, %v", ready, err)
	}
	return *ready
}

func recoveryEnvelope(config segment.SpoolConfig, ordinal uint64) segment.Envelope {
	return segment.Envelope{Kind: segment.RecordKindWebSocket, SourceID: config.SourceID,
		ChannelOrEndpoint: config.ChannelID, ConnectionEpoch: segment.OptionalEpoch{Value: config.EpochID, Valid: true},
		ArrivalOrdinal: ordinal, ReceivedWallTimeNS: 1_700_000_000_000_000_000 + int64(ordinal),
		ClockEpochID: "synthetic-clock", PayloadEncoding: segment.PayloadEncodingBinary,
		RawPayload: bytes.Repeat([]byte{byte(ordinal)}, 128), TerminalOutcome: segment.OutcomeObserved,
		RecorderVersion: "synthetic-recorder-v1"}
}

func recoveryPublication(t *testing.T, ready segment.ReadySegment, segmentID string) catalog.RawSegmentPublication {
	t.Helper()
	manifestBytes, err := os.ReadFile(ready.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	epochHex := ready.Manifest.EpochID
	epochID := epochHex[0:8] + "-" + epochHex[8:12] + "-" + epochHex[12:16] + "-" + epochHex[16:20] + "-" + epochHex[20:32]
	return catalog.RawSegmentPublication{SegmentID: segmentID, SourceID: ready.Manifest.SourceID,
		ChannelID: ready.Manifest.ChannelID, EpochID: epochID,
		ReceivedStartNS: ready.Manifest.Segment.FirstReceivedAtNS, ReceivedEndNS: ready.Manifest.Segment.LastReceivedAtNS,
		OrdinalStart: ready.Manifest.Segment.FirstOrdinal, OrdinalEnd: ready.Manifest.Segment.LastOrdinal,
		ObjectKey: ready.Manifest.ObjectKey, ContentSHA256: ready.Manifest.Segment.CompressedSHA256,
		ByteLength: int64(ready.Manifest.Segment.CompressedBytes), ManifestVersion: ready.Manifest.ManifestVersion,
		ManifestSHA256: ready.ManifestSHA256, ManifestBytes: manifestBytes, State: catalog.RawSegmentCommitted}
}

type recoveryStoredObject struct {
	body []byte
	hash [sha256.Size]byte
}

type recoveryReplicaRestorer struct {
	primary   *recoveryObjectClient
	objectKey string
	object    recoveryStoredObject
	calls     int
}

func (r *recoveryReplicaRestorer) RestoreVerifiedObject(_ context.Context, key string, size int64, expected [sha256.Size]byte) error {
	if key != r.objectKey {
		return objectstore.ErrNotFound
	}
	if int64(len(r.object.body)) != size {
		return objectstore.ErrSizeMismatch
	}
	if r.object.hash != expected || sha256.Sum256(r.object.body) != expected {
		return objectstore.ErrHashMismatch
	}
	r.primary.store(key, r.object.body, r.object.hash)
	r.calls++
	return nil
}

type recoveryObjectClient struct {
	objects map[string]recoveryStoredObject
}

func newRecoveryObjectClient() *recoveryObjectClient {
	return &recoveryObjectClient{objects: make(map[string]recoveryStoredObject)}
}

func (c *recoveryObjectClient) store(key string, body []byte, hash [sha256.Size]byte) {
	c.objects[key] = recoveryStoredObject{body: bytes.Clone(body), hash: hash}
}

func (c *recoveryObjectClient) storeReady(t *testing.T, ready segment.ReadySegment) {
	t.Helper()
	body, err := os.ReadFile(ready.SegmentPath)
	if err != nil {
		t.Fatal(err)
	}
	c.store(ready.Manifest.ObjectKey, body, ready.Manifest.Segment.CompressedSHA256)
}

func (c *recoveryObjectClient) Head(_ context.Context, key string) (objectstore.ObjectInfo, error) {
	object, ok := c.objects[key]
	if !ok {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectInfo{Key: key, Size: int64(len(object.body)),
		Metadata: map[string]string{objectstore.ApplicationSHA256Metadata: hex.EncodeToString(object.hash[:])}}, nil
}

func (c *recoveryObjectClient) PutIfAbsent(_ context.Context, object objectstore.PutObject) error {
	if _, exists := c.objects[object.Key]; exists {
		return objectstore.ErrPreconditionFailed
	}
	body, err := io.ReadAll(io.LimitReader(object.Body, object.Size+1))
	if err != nil {
		return err
	}
	if int64(len(body)) != object.Size || sha256.Sum256(body) != object.SHA256 {
		return objectstore.ErrHashMismatch
	}
	c.store(object.Key, body, object.SHA256)
	return nil
}

func (c *recoveryObjectClient) Get(_ context.Context, key string) (io.ReadCloser, error) {
	object, ok := c.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(object.body)), nil
}

func (c *recoveryObjectClient) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	object, ok := c.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if offset < 0 || length < 0 || offset+length > int64(len(object.body)) {
		return nil, objectstore.ErrInvalidResponse
	}
	return io.NopCloser(bytes.NewReader(object.body[offset : offset+length])), nil
}

func (c *recoveryObjectClient) List(_ context.Context, prefix, continuation string) (objectstore.ListPage, error) {
	keys := make([]string, 0, len(c.objects))
	for key := range c.objects {
		if strings.HasPrefix(key, prefix) && key > continuation {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	objects := make([]objectstore.ListedObject, len(keys))
	for i, key := range keys {
		objects[i] = objectstore.ListedObject{Key: key, Size: int64(len(c.objects[key].body))}
	}
	return objectstore.ListPage{Objects: objects}, nil
}

func (*recoveryObjectClient) StartMultipart(context.Context, string, [sha256.Size]byte) (string, error) {
	return "", objectstore.ErrMultipartUnsupported
}
func (*recoveryObjectClient) UploadPart(context.Context, string, string, int32, io.Reader, int64) (objectstore.UploadedPart, error) {
	return objectstore.UploadedPart{}, objectstore.ErrMultipartUnsupported
}
func (*recoveryObjectClient) CompleteMultipart(context.Context, string, string, []objectstore.UploadedPart) error {
	return objectstore.ErrMultipartUnsupported
}
func (*recoveryObjectClient) AbortMultipart(context.Context, string, string) error { return nil }
func (*recoveryObjectClient) ReconcileMultipart(context.Context, string) error     { return nil }

type recoveryCatalog struct {
	mu      sync.Mutex
	records map[string]catalog.RawSegmentPublication
	orphans map[string]catalog.ObjectOrphan
}

func newRecoveryCatalog() *recoveryCatalog {
	return &recoveryCatalog{records: make(map[string]catalog.RawSegmentPublication), orphans: make(map[string]catalog.ObjectOrphan)}
}

func (c *recoveryCatalog) FindRawSegment(_ context.Context, key string) (catalog.RawSegmentPublication, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[key]
	return record, ok, nil
}

func (c *recoveryCatalog) RecordVerified(_ context.Context, record catalog.RawSegmentPublication) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.records[record.ObjectKey]; ok {
		if existing.SegmentID != record.SegmentID || existing.ContentSHA256 != record.ContentSHA256 || existing.ByteLength != record.ByteLength ||
			!bytes.Equal(existing.ManifestBytes, record.ManifestBytes) {
			return catalog.ErrPublicationConflict
		}
		if existing.State == catalog.RawSegmentCommitted || existing.State == catalog.RawSegmentVerified {
			return nil
		}
		return catalog.ErrPublicationState
	}
	record.State = catalog.RawSegmentVerified
	record.ManifestBytes = bytes.Clone(record.ManifestBytes)
	c.records[record.ObjectKey] = record
	return nil
}

func (c *recoveryCatalog) CommitRawSegment(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[key]
	if !ok || (record.State != catalog.RawSegmentVerified && record.State != catalog.RawSegmentCommitted) {
		return catalog.ErrPublicationState
	}
	record.State = catalog.RawSegmentCommitted
	c.records[key] = record
	return nil
}

func (c *recoveryCatalog) QuarantineRawSegment(_ context.Context, key, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[key]
	if !ok || (record.State != catalog.RawSegmentPending && record.State != catalog.RawSegmentVerified && record.State != catalog.RawSegmentQuarantined) {
		return catalog.ErrPublicationState
	}
	record.State = catalog.RawSegmentQuarantined
	c.records[key] = record
	return nil
}

func (c *recoveryCatalog) InvalidateCommittedRawSegmentForRecovery(
	_ context.Context,
	expected catalog.RawSegmentPublication,
	_ string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[expected.ObjectKey]
	if !ok {
		return nil
	}
	if record.SegmentID != expected.SegmentID || record.SourceID != expected.SourceID ||
		record.ChannelID != expected.ChannelID || record.EpochID != expected.EpochID ||
		record.ReceivedStartNS != expected.ReceivedStartNS || record.ReceivedEndNS != expected.ReceivedEndNS ||
		record.OrdinalStart != expected.OrdinalStart || record.OrdinalEnd != expected.OrdinalEnd ||
		record.ContentSHA256 != expected.ContentSHA256 || record.ByteLength != expected.ByteLength ||
		record.ManifestVersion != expected.ManifestVersion || record.ManifestSHA256 != expected.ManifestSHA256 ||
		!bytes.Equal(record.ManifestBytes, expected.ManifestBytes) {
		return catalog.ErrPublicationConflict
	}
	if record.State != catalog.RawSegmentCommitted && record.State != catalog.RawSegmentQuarantined {
		return catalog.ErrPublicationState
	}
	record.State = catalog.RawSegmentQuarantined
	c.records[expected.ObjectKey] = record
	return nil
}

func (c *recoveryCatalog) RecordObjectOrphan(_ context.Context, orphan catalog.ObjectOrphan) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.orphans[orphan.ObjectKey]; ok {
		if existing.ByteLength != orphan.ByteLength || existing.HasApplicationSHA256 != orphan.HasApplicationSHA256 ||
			existing.ApplicationSHA256 != orphan.ApplicationSHA256 {
			return catalog.ErrPublicationConflict
		}
		return nil
	}
	c.orphans[orphan.ObjectKey] = orphan
	return nil
}

type recoveryManifestReader struct {
	generation warehouse.Generation
	rows       []warehouse.Row
}

func newRecoveryManifestReader(count int) recoveryManifestReader {
	ids := make([]warehouse.EventID, count)
	rows := make([]warehouse.Row, count)
	generationID := recoveryWarehouseHash("generation")
	manifestHash := recoveryWarehouseHash("manifest")
	catalogHash := recoveryWarehouseHash("catalog")
	inputHash := recoveryWarehouseHash("input")
	for i := range count {
		ids[i] = recoveryWarehouseHash(fmt.Sprintf("event-%d", i))
		rows[i] = warehouse.Row{GenerationID: generationID, ManifestHash: manifestHash,
			RowID: recoveryWarehouseHash(fmt.Sprintf("row-%d", i)), EventID: ids[i], LogicalHash: recoveryWarehouseHash(fmt.Sprintf("logical-%d", i)),
			Family: "trade", SourceID: "synthetic", ChannelID: "trades", InstrumentUID: "instrument", EpochKind: "connection",
			ConnectionEpoch: [16]byte{1}, ReceivedTimeNS: 1_700_000_000_000_000_000 + int64(i), ArrivalOrdinal: uint64(i + 1),
			RawSegmentSHA256: recoveryWarehouseHash("raw"), RawPayloadSHA256: recoveryWarehouseHash(fmt.Sprintf("payload-%d", i)),
			CatalogSnapshotID: catalogHash, SchemaName: "enable.trade.v1", SchemaVersion: 1,
			DatasetPolicyID: recoveryWarehouseHash("policy"), ReplayConfigID: recoveryWarehouseHash("replay"), InputManifestSetID: inputHash}
	}
	slices.SortFunc(ids, compareWarehouseHash)
	return recoveryManifestReader{generation: warehouse.Generation{ID: generationID, ServerDigest: "sha256:synthetic-clickhouse",
		ManifestHash: manifestHash, InputHash: inputHash, DatasetIdentity: recoveryWarehouseHash("dataset"), CatalogIdentity: catalogHash,
		SchemaIdentity: recoveryWarehouseHash("schema"), Family: "trade", SourceID: "synthetic", UTCDate: "2024-01-01",
		PartitionValue: 202401, Layout: warehouse.PartitionMonth, ExpectedEventIDs: ids,
		ExpectedEventSetHash: recoveryEventSetHash(ids), ExpectedEventCount: uint64(count), ExpectedRowCount: uint64(count), State: warehouse.GenerationPending}, rows: rows}
}

func (r recoveryManifestReader) Plan(context.Context, warehouse.CommittedManifest, string, warehouse.PartitionLayout) (warehouse.Generation, error) {
	generation := r.generation
	generation.ExpectedEventIDs = slices.Clone(r.generation.ExpectedEventIDs)
	return generation, nil
}
func (r recoveryManifestReader) Scan(_ context.Context, _ warehouse.CommittedManifest, generationID warehouse.GenerationID, consume func(warehouse.Row) error) error {
	for _, source := range r.rows {
		row := source
		row.GenerationID = generationID
		if err := consume(row); err != nil {
			return err
		}
	}
	return nil
}

type recoveryWarehouseStore struct {
	generation warehouse.Generation
	found      bool
	expected   []warehouse.EventID
	rows       []warehouse.Row
}

func (*recoveryWarehouseStore) StoreIdentity() warehouse.StoreIdentity {
	return warehouse.StoreIdentity{
		ServerDigest: "sha256:synthetic-clickhouse",
		Database:     "recovery",
		TablePrefix:  "marketdata",
		Layout:       warehouse.PartitionMonth,
	}
}

func (*recoveryWarehouseStore) EnsureSchema(context.Context) error { return nil }
func (s *recoveryWarehouseStore) BeginGeneration(_ context.Context, generation warehouse.Generation) error {
	s.generation, s.found, s.expected = generation, true, slices.Clone(generation.ExpectedEventIDs)
	return nil
}
func (s *recoveryWarehouseStore) Generation(context.Context, warehouse.GenerationID) (warehouse.Generation, bool, error) {
	return s.generation, s.found, nil
}
func (s *recoveryWarehouseStore) ExpectedEventIDs(context.Context, warehouse.GenerationID) ([]warehouse.EventID, error) {
	return slices.Clone(s.expected), nil
}
func (s *recoveryWarehouseStore) ActualEventIDs(context.Context, warehouse.GenerationID) ([]warehouse.EventID, error) {
	ids := make([]warehouse.EventID, len(s.rows))
	for i := range s.rows {
		ids[i] = s.rows[i].EventID
	}
	slices.SortFunc(ids, compareWarehouseHash)
	return slices.Compact(ids), nil
}
func (s *recoveryWarehouseStore) GenerationRowCount(context.Context, warehouse.GenerationID) (uint64, error) {
	return uint64(len(s.rows)), nil
}
func (s *recoveryWarehouseStore) InsertRows(_ context.Context, rows []warehouse.Row) error {
	s.rows = append(s.rows, slices.Clone(rows)...)
	return nil
}
func (s *recoveryWarehouseStore) SetGenerationState(_ context.Context, _ warehouse.GenerationID, state warehouse.GenerationState, message string, updated time.Time) error {
	s.generation.State, s.generation.LastError, s.generation.UpdatedAt = state, message, updated
	return nil
}
func (s *recoveryWarehouseStore) DeleteGeneration(context.Context, warehouse.GenerationID) error {
	return s.Truncate(context.Background())
}
func (s *recoveryWarehouseStore) DropPartition(context.Context, warehouse.Partition) error {
	return s.Truncate(context.Background())
}
func (s *recoveryWarehouseStore) Truncate(context.Context) error {
	s.generation, s.found, s.expected, s.rows = warehouse.Generation{}, false, nil, nil
	return nil
}
func (s *recoveryWarehouseStore) StreamRebuildRows(_ context.Context, visit func(warehouse.Row) error) error {
	for _, row := range s.rows {
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func (s *recoveryWarehouseStore) PersistedGenerationIDs(context.Context) ([]warehouse.GenerationID, error) {
	if !s.found {
		return nil, nil
	}
	return []warehouse.GenerationID{s.generation.ID}, nil
}

func recoveryWarehouseHash(value string) warehouse.Hash   { return sha256.Sum256([]byte(value)) }
func compareWarehouseHash(left, right warehouse.Hash) int { return bytes.Compare(left[:], right[:]) }
func recoveryEventSetHash(ids []warehouse.EventID) warehouse.Hash {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("warehouse-event-id-set-v1\x00"))
	for _, id := range ids {
		_, _ = hasher.Write(id[:])
	}
	var result warehouse.Hash
	copy(result[:], hasher.Sum(nil))
	return result
}
