package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	querySourceID     = "00000000-0000-0000-0000-000000007001"
	queryInstrumentID = "00000000-0000-0000-0000-000000007002"
	queryDatasetID    = "00000000-0000-0000-0000-000000007003"
	queryCoverageID   = "00000000-0000-0000-0000-000000007004"
	queryGapID        = "00000000-0000-0000-0000-000000007005"
)

func TestQueryStorePostgreSQLProjectionLifecycle(t *testing.T) {
	fixture := newPostgresFixture(t)
	migrationConn := fixture.connect(t)
	if err := Migrate(t.Context(), migrationConn); err != nil {
		t.Fatal(err)
	}
	bootstrapStore, err := NewQueryStore(t.Context(), migrationConn, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	declarativeSource := Source{SourceID: querySourceID, Venue: "synthetic", ProductFamily: "spot",
		APIFamily: "v1", Environment: "test", Lifecycle: "active"}
	if err := bootstrapStore.BootstrapSource(t.Context(), declarativeSource); err != nil {
		t.Fatal(err)
	}
	if err := bootstrapStore.BootstrapSource(t.Context(), declarativeSource); err != nil {
		t.Fatalf("exact source bootstrap retry: %v", err)
	}
	conflictingSource := declarativeSource
	conflictingSource.Venue = "different"
	if err := bootstrapStore.BootstrapSource(t.Context(), conflictingSource); !errors.Is(err, ErrQueryConflict) {
		t.Fatalf("conflicting source bootstrap error = %v, want ErrQueryConflict", err)
	}
	insertQueryInstrument(t, migrationConn)
	rawPublications := insertQueryRawSegments(t, migrationConn)
	segments := []string{rawPublications[0].SegmentID, rawPublications[1].SegmentID}

	poolConfig, err := pgxpool.ParseConfig(fixture.config.ConnString())
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = fixture.config.RuntimeParams["search_path"]
	poolConfig.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(t.Context(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store, err := NewQueryStore(t.Context(), pool, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !store.Ready() {
		t.Fatal("new query store is not ready")
	}
	requestEnvelope, responseEnvelope := queryRESTEvidenceEnvelopes(t)
	if err := store.RecordCommittedRESTEvidence(t.Context(), rawPublications[0], requestEnvelope, responseEnvelope); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommittedRESTEvidence(t.Context(), rawPublications[0], requestEnvelope, responseEnvelope); err != nil {
		t.Fatalf("exact REST evidence retry: %v", err)
	}
	conflictingResponse := responseEnvelope
	conflictingResponse.SetRawPayload([]byte(`{"different":true}`))
	if err := store.RecordCommittedRESTEvidence(t.Context(), rawPublications[0], requestEnvelope, conflictingResponse); !errors.Is(err, ErrQueryConflict) {
		t.Fatalf("conflicting REST evidence error = %v, want ErrQueryConflict", err)
	}
	beforeRequest := responseEnvelope
	beforeRequest.ArrivalOrdinal = requestEnvelope.ArrivalOrdinal
	if err := store.RecordCommittedRESTEvidence(t.Context(), rawPublications[0], requestEnvelope, beforeRequest); !errors.Is(err, ErrInvalidQueryProjection) {
		t.Fatalf("response-before-request error = %v, want ErrInvalidQueryProjection", err)
	}

	manifestBytes := []byte(`{"manifest_version":1,"dataset":"synthetic"}`)
	publication := DatasetPublication{
		DatasetID: queryDatasetID, DatasetFamily: "trade", DatasetVersion: "parquet-dataset-v1",
		SourceID: querySourceID, SchemaName: "enable.trade.parquet.v1", SchemaVersion: 1, ManifestVersion: 1,
		PartitionKey: "trade/v1/date=1970-01-01/hour=00", RangeStartNS: 100, RangeEndNS: 300,
		InputSegmentSetHash: sha256.Sum256([]byte("input segments")), CatalogSnapshotHash: sha256.Sum256([]byte("catalog")),
		MapperSetHash: sha256.Sum256([]byte("mappers")), LogicalHash: sha256.Sum256([]byte("logical")),
		PhysicalHash: sha256.Sum256([]byte("physical")), ParquetObjectKey: "datasets/trade/part.parquet",
		ManifestObjectKey: "datasets/trade/manifest.json", ParquetBytes: 4096,
		ManifestHash: sha256.Sum256(manifestBytes), ManifestBytes: manifestBytes, State: DatasetVerified,
		InputSegmentIDs: segments,
		Coverage: []DatasetCoverage{{ID: queryCoverageID,
			Tuple:               TupleProjection{SourceID: querySourceID, ChannelID: "trades", InstrumentUID: queryInstrumentID},
			StartReceivedTimeNS: 100, EndReceivedTimeNS: 300, State: "complete"}},
	}
	if err := store.RecordVerifiedDataset(t.Context(), publication); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordVerifiedDataset(t.Context(), publication); err != nil {
		t.Fatalf("exact verified retry: %v", err)
	}
	conflict := publication
	conflict.ParquetBytes++
	if err := store.RecordVerifiedDataset(t.Context(), conflict); !errors.Is(err, ErrQueryConflict) {
		t.Fatalf("changed publication error = %v, want ErrQueryConflict", err)
	}
	if err := store.CommitDataset(t.Context(), publication.DatasetID); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitDataset(t.Context(), publication.DatasetID); err != nil {
		t.Fatalf("exact commit retry: %v", err)
	}
	var streamed []DatasetPublication
	if err := store.StreamCommittedDatasets(t.Context(), func(value DatasetPublication) error {
		streamed = append(streamed, value)
		return nil
	}); err != nil {
		t.Fatalf("StreamCommittedDatasets() error = %v", err)
	}
	if len(streamed) != 1 || streamed[0].DatasetID != queryDatasetID || len(streamed[0].InputSegmentIDs) != len(segments) {
		t.Fatalf("StreamCommittedDatasets() = %+v", streamed)
	}

	generationID := sha256.Sum256([]byte("warehouse generation"))
	generation := DatasetGenerationCommit{
		DatasetID: publication.DatasetID, GenerationID: generationID, ManifestHash: publication.ManifestHash,
		InputHash: publication.InputSegmentSetHash, ExpectedEventCount: 2, ExpectedRowCount: 2,
		Family: publication.DatasetFamily, CatalogSnapshotID: publication.CatalogSnapshotHash,
		SchemaName: publication.SchemaName, SchemaVersion: publication.SchemaVersion,
	}
	if err := store.CommitDatasetGeneration(t.Context(), generation); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitDatasetGeneration(t.Context(), generation); err != nil {
		t.Fatalf("exact generation retry: %v", err)
	}
	changedGeneration := generation
	changedGeneration.ExpectedRowCount++
	if err := store.CommitDatasetGeneration(t.Context(), changedGeneration); !errors.Is(err, ErrQueryConflict) {
		t.Fatalf("changed generation error = %v, want ErrQueryConflict", err)
	}
	generationText := hex.EncodeToString(generationID[:])

	manifest, err := store.DatasetManifest(t.Context(), generationText)
	if err != nil || manifest.Dataset.ID != generationID || manifest.ManifestSHA256 != publication.ManifestHash || manifest.State != DatasetCommitted {
		t.Fatalf("DatasetManifest() = %+v, %v", manifest, err)
	}
	latest, err := store.LatestDataset(t.Context(), "trade")
	if err != nil || latest.ID != generationID {
		t.Fatalf("LatestDataset() = %+v, %v", latest, err)
	}
	metadata, err := store.Datasets(t.Context())
	if err != nil || len(metadata) != 1 || metadata[0].DatasetID != generationText || !metadata[0].Committed {
		t.Fatalf("Datasets() = %+v, %v", metadata, err)
	}

	filter := RawSegmentFilter{DatasetID: generationText, SourceIDs: []string{querySourceID},
		ChannelIDs: []string{"trades"}, InstrumentUIDs: []string{queryInstrumentID},
		StartReceivedTimeNS: 100, EndReceivedTimeNS: 300, Limit: 1, MaxManifestBytes: 1 << 20}
	if values, err := store.CommittedRawSegments(t.Context(), filter); !errors.Is(err, ErrQueryBound) || values != nil {
		t.Fatalf("bounded raw query values=%v error=%v, want ErrQueryBound and no partial result", values, err)
	}
	filter.Limit = 2
	raw, err := store.CommittedRawSegments(t.Context(), filter)
	if err != nil || len(raw) != 2 || raw[0].ReceivedStartNS != 100 || raw[1].ReceivedStartNS != 200 {
		t.Fatalf("CommittedRawSegments() = %+v, %v", raw, err)
	}
	filter.ChannelIDs, filter.InstrumentUIDs = nil, nil
	raw, err = store.CommittedRawSegments(t.Context(), filter)
	if err != nil || len(raw) != 2 {
		t.Fatalf("CommittedRawSegments() with omitted optional filters = %+v, %v", raw, err)
	}

	insertQueryGap(t, migrationConn)
	references := ReferenceFilter{DatasetID: generationText, SourceIDs: []string{querySourceID},
		ChannelIDs: []string{"trades"}, InstrumentUIDs: []string{queryInstrumentID},
		StartReceivedTimeNS: 100, EndReceivedTimeNS: 300, Limit: 8}
	coverage, gaps, err := store.References(t.Context(), references)
	if err != nil || len(coverage) != 1 || len(gaps) != 1 || coverage[0].ID != queryCoverageID || gaps[0].ID != queryGapID {
		t.Fatalf("References() coverage=%+v gaps=%+v error=%v", coverage, gaps, err)
	}
	references.ChannelIDs, references.InstrumentUIDs = nil, nil
	coverage, gaps, err = store.References(t.Context(), references)
	if err != nil || len(coverage) != 1 || len(gaps) != 1 {
		t.Fatalf("References() with omitted optional filters coverage=%+v gaps=%+v error=%v", coverage, gaps, err)
	}

	checkpointState := []byte(`{"arrival":2}`)
	checkpoint := RuntimeCheckpoint{Key: "normalizer/trades", SourceID: querySourceID, ChannelID: "trades",
		InstrumentUID: queryInstrumentID, ReceivedTimeNS: 200,
		StreamEpochID: "00000000-0000-0000-0000-000000007102", ArrivalOrdinal: 2,
		StateSHA256: sha256.Sum256(checkpointState), StateBytes: checkpointState, UpdatedAt: time.Unix(1, 0).UTC()}
	if err := store.PutCheckpoint(t.Context(), checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCheckpoint(t.Context(), checkpoint); err != nil {
		t.Fatalf("checkpoint retry: %v", err)
	}
	advanced := checkpoint
	advanced.ArrivalOrdinal = 3
	advanced.StateBytes = []byte(`{"arrival":3}`)
	advanced.StateSHA256 = sha256.Sum256(advanced.StateBytes)
	advanced.UpdatedAt = advanced.UpdatedAt.Add(time.Second)
	if err := store.PutCheckpoint(t.Context(), advanced); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCheckpoint(t.Context(), checkpoint); !errors.Is(err, ErrQueryConflict) {
		t.Fatalf("checkpoint regression error = %v, want ErrQueryConflict", err)
	}
	gotCheckpoint, err := store.Checkpoint(t.Context(), checkpoint.Key)
	if err != nil || gotCheckpoint.ArrivalOrdinal != advanced.ArrivalOrdinal {
		t.Fatalf("Checkpoint() = %+v, %v", gotCheckpoint, err)
	}

	var wait sync.WaitGroup
	errorsFound := make(chan error, 16)
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			sources, sourceErr := store.Sources(t.Context())
			if sourceErr != nil || len(sources) != 1 || sources[0].SourceID != querySourceID {
				errorsFound <- fmt.Errorf("Sources() = %+v, %v", sources, sourceErr)
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for concurrentErr := range errorsFound {
		t.Error(concurrentErr)
	}
}

func insertQueryInstrument(t *testing.T, conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) {
	t.Helper()
	rawHash := sha256.Sum256([]byte("instrument metadata"))
	if _, err := conn.Exec(t.Context(), `
INSERT INTO instrument (instrument_uid, source_id, native_id, listing_epoch, first_observed_at)
VALUES ($1, $2, 'SYNTHETICUSD', 1, TIMESTAMPTZ '2024-01-01 00:00:00+00');
INSERT INTO instrument_version (
    instrument_uid, valid_from, aliases, lifecycle_state, base_asset, quote_asset,
    instrument_kind, payoff, multiplier, tick_rules, lot_rules, raw_metadata,
    raw_metadata_hash, normalized_schema_version
) VALUES (
    $1, TIMESTAMPTZ '2024-01-01 00:00:00+00', '[]', 'active', 'SYNTHETIC', 'USD',
    'spot', '{}', 1, '{}', '{}', '{}', $3, 'v1'
)
`, queryInstrumentID, querySourceID, rawHash[:]); err != nil {
		t.Fatal(err)
	}
}

func insertQueryRawSegments(t *testing.T, conn PublicationDatabase) []RawSegmentPublication {
	t.Helper()
	store, err := NewPublicationStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"00000000-0000-0000-0000-000000007101", "00000000-0000-0000-0000-000000007102"}
	publications := make([]RawSegmentPublication, 0, len(ids))
	for index, id := range ids {
		manifest := []byte(fmt.Sprintf(`{"manifest_version":1,"segment":%d}`, index+1))
		content := sha256.Sum256([]byte(fmt.Sprintf("segment-%d", index+1)))
		publication := RawSegmentPublication{
			SegmentID: id, SourceID: querySourceID, ChannelID: "trades",
			EpochID:         "00000000-0000-0000-0000-000000007201",
			ReceivedStartNS: int64(100 + index*100), ReceivedEndNS: int64(199 + index*100),
			OrdinalStart: uint64(index*2 + 1), OrdinalEnd: uint64(index*2 + 2),
			ObjectKey: fmt.Sprintf("raw/synthetic/%d.emseg.zst", index+1), ContentSHA256: content,
			ByteLength: int64(index + 100), ManifestVersion: 1,
			ManifestSHA256: sha256.Sum256(manifest), ManifestBytes: manifest, State: RawSegmentVerified,
		}
		if err := store.RecordVerified(t.Context(), publication); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitRawSegment(t.Context(), publication.ObjectKey); err != nil {
			t.Fatal(err)
		}
		publication.State = RawSegmentCommitted
		publications = append(publications, publication)
	}
	return publications
}

func queryRESTEvidenceEnvelopes(t *testing.T) (capture.EnvelopeV1, capture.EnvelopeV1) {
	t.Helper()
	pollUUID, err := uuid.Parse("00000000-0000-0000-0000-000000007201")
	if err != nil {
		t.Fatal(err)
	}
	poll := [16]byte(pollUUID)
	requestRecord, err := capture.NewControlRecord(capture.ControlRequestStarted, capture.EnvelopeV1{
		SourceID:                   querySourceID,
		ChannelOrEndpoint:          "trades",
		PollCycleID:                capture.OptionalEpoch{Value: poll, Valid: true},
		ArrivalOrdinal:             1,
		MessageOrdinal:             0,
		ScheduledAtNS:              capture.OptionalInt64{Value: 101, Valid: true},
		RequestStartedAtNS:         capture.OptionalInt64{Value: 105, Valid: true},
		ReceivedWallTimeNS:         110,
		ClockEpochID:               "query-test-clock",
		MonotonicNSSinceClockEpoch: 1,
		SubscriptionOrRequestID:    capture.OptionalString{Value: "exchange-info-1", Valid: true},
		PayloadEncoding:            capture.PayloadEncodingNone,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "query-test-recorder",
		Extensions:                 []byte(`{"method":"GET"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	response := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindREST,
		SourceID:                   querySourceID,
		ChannelOrEndpoint:          "trades",
		PollCycleID:                capture.OptionalEpoch{Value: poll, Valid: true},
		ArrivalOrdinal:             2,
		MessageOrdinal:             0,
		ScheduledAtNS:              capture.OptionalInt64{Value: 101, Valid: true},
		RequestStartedAtNS:         capture.OptionalInt64{Value: 105, Valid: true},
		RequestCompletedAtNS:       capture.OptionalInt64{Value: 115, Valid: true},
		ReceivedWallTimeNS:         120,
		ClockEpochID:               "query-test-clock",
		MonotonicNSSinceClockEpoch: 2,
		SubscriptionOrRequestID:    capture.OptionalString{Value: "exchange-info-1", Valid: true},
		HTTPStatusOrWSState:        capture.OptionalString{Value: "200", Valid: true},
		PayloadEncoding:            capture.PayloadEncodingJSON,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "query-test-recorder",
	}
	response.SetRawPayload([]byte(`{"symbols":[]}`))
	if err := response.Validate(); err != nil {
		t.Fatal(err)
	}
	return requestRecord.Envelope, response
}

func insertQueryGap(t *testing.T, conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) {
	t.Helper()
	if _, err := conn.Exec(t.Context(), `
INSERT INTO gap (
    gap_id, source_id, channel_id, instrument_uid, range_start_ns, range_end_ns,
    detection_rule, state, confidence, evidence, detected_at,
    first_good_coordinate, last_good_coordinate, affected_families, detected_time_ns
) VALUES (
    $1, $2, 'trades', $3, 120, 180, 'sequence_interval', 'open', 1,
    '{"basis":"synthetic"}', TIMESTAMPTZ '1970-01-01 00:00:00.0000002+00',
    '{"arrival":1}', '{"arrival":2}', ARRAY['trade'], 200
)
`, queryGapID, querySourceID, queryInstrumentID); err != nil {
		t.Fatal(err)
	}
}
