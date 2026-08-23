package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/orderbook"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/spf13/fileflow"
)

const evidenceDirectoryName = "evidence"

type Dependencies struct {
	Objects         objectstore.Client
	Catalog         PublicationCatalog
	WebSocket       binance.SpotWSConnector
	REST            binance.SpotRESTClient
	Clock           capture.Clock
	Now             func() time.Time
	CatalogSnapshot catalog.Snapshot
}

type replayDerivation struct {
	result          replay.Result
	rawRecords      []normalize.RawRecord
	dataRecords     []normalize.RawRecord
	snapshotRecords map[string]normalize.RawRecord
	counts          EvidenceCounts
	rawHash         string
	discontinuities []DiscontinuityEvidence
}

type fixedSnapshotFetcher struct {
	snapshot orderbook.SnapshotObservation
}

func (f fixedSnapshotFetcher) Fetch(ctx context.Context) (orderbook.SnapshotObservation, error) {
	if err := ctx.Err(); err != nil {
		return orderbook.SnapshotObservation{}, err
	}
	return f.snapshot, nil
}

func ValidateVenueInputs(cfg config.Config) error {
	if len(cfg.Sources) != 1 || cfg.Sources[0].ID != binance.SpotSourceID {
		return errors.New("verify: configured source identity differs from the pinned Binance Spot contract")
	}
	if _, err := binance.NewSpotSubscriptionPlan(cfg.Sources[0].Symbols); err != nil {
		return err
	}
	for index, symbol := range cfg.Sources[0].Symbols {
		if _, err := binance.NewSpotDepthRequest(fmt.Sprintf("elmd-014-preflight-%d", index+1), symbol, cfg.Verify.DepthLimit, false); err != nil {
			return err
		}
	}
	return nil
}

func ValidateRuntimeRoots(cfg config.Config) error {
	if err := requireRealDirectory(cfg.Verify.SpoolRoot); err != nil {
		return err
	}
	return requireRealDirectory(cfg.Verify.ArtifactRoot)
}

// RunVenue executes the complete raw-first evidence workflow. It returns the
// exact durable evidence bytes; callers must write those bytes without adding
// prose so repeated executions are byte-identical.
func RunVenue(ctx context.Context, venue string, cfg config.Config, build BuildInfo, dependencies Dependencies) ([]byte, error) {
	if ctx == nil || venue != "binance-spot" {
		return nil, errors.New("verify: unsupported venue or nil context")
	}
	if err := cfg.ValidateVerifyVenue(ctx, venue, func(context.Context, string) error { return nil }); err != nil {
		return nil, err
	}
	if err := ValidateVenueInputs(cfg); err != nil {
		return nil, err
	}
	if err := ValidateRuntimeRoots(cfg); err != nil {
		return nil, err
	}

	configurationHash, err := verificationConfigHash(venue, cfg)
	if err != nil {
		return nil, err
	}
	var fixture loadedFixture
	runtime := captureRuntime{}
	fixtureManifestHash := ""
	fixtureInputHash := ""
	catalogSnapshot := dependencies.CatalogSnapshot
	if cfg.Verify.Mode == config.VerifyModeFixture {
		fixture, err = loadFixture(ctx, cfg.Verify.FixtureRoot, cfg.Verify.FixtureManifest)
		if err != nil {
			return nil, err
		}
		if len(cfg.Sources[0].Symbols) != 1 || cfg.Sources[0].Symbols[0] != fixture.manifest.Symbol {
			return nil, errors.New("verify: configured fixture symbol does not match the immutable fixture")
		}
		objects, err := OpenFileObjectClient(cfg.Verify.ArtifactRoot)
		if err != nil {
			return nil, err
		}
		publicationCatalog, err := OpenFileCatalog(cfg.Verify.ArtifactRoot)
		if err != nil {
			return nil, err
		}
		clock, err := capture.NewManualClock(fixture.manifest.StartWallTimeNS, fixture.manifest.ClockEpochID)
		if err != nil {
			return nil, err
		}
		ws, rest := newFixtureTransports(fixture)
		runtime = captureRuntime{
			objects: objects, catalog: publicationCatalog, ws: ws, rest: rest, clock: clock,
			advance: func() error { return clock.Advance(fixture.manifest.StepNanoseconds) },
			now:     func() time.Time { return time.Unix(0, clock.Read().WallTimeNS).UTC() },
			wsEpoch: fixture.connectionID, restEpochs: [][16]byte{fixture.pollID},
		}
		catalogSnapshot = fixture.catalog
		fixtureManifestHash = hex.EncodeToString(fixture.manifestSHA256[:])
		fixtureInputHash = hex.EncodeToString(fixture.inputSHA256[:])
	} else {
		if dependencies.Objects == nil || dependencies.Catalog == nil || dependencies.WebSocket == nil || dependencies.REST == nil ||
			dependencies.Clock == nil || dependencies.Now == nil || catalogSnapshot.SHA256 == ([sha256.Size]byte{}) {
			return nil, errors.New("verify: live dependencies are incomplete")
		}
		wsID, err := parseUUID(stableUUID("verify-live-ws", configurationHash))
		if err != nil {
			return nil, err
		}
		restIDs := make([][16]byte, len(cfg.Sources[0].Symbols))
		for index, symbol := range cfg.Sources[0].Symbols {
			restIDs[index], err = parseUUID(stableUUID("verify-live-rest", configurationHash+"\x00"+symbol))
			if err != nil {
				return nil, err
			}
		}
		runtime = captureRuntime{
			objects: dependencies.Objects, catalog: dependencies.Catalog, ws: dependencies.WebSocket, rest: dependencies.REST,
			clock: dependencies.Clock, now: dependencies.Now, wsEpoch: wsID, restEpochs: restIDs,
		}
	}
	catalogView, err := normalize.NewCatalogView(catalogSnapshot)
	if err != nil {
		return nil, err
	}
	seenCatalogInstruments := make(map[string]struct{}, len(cfg.Sources[0].Symbols))
	for _, symbol := range cfg.Sources[0].Symbols {
		instrument, found := catalogView.Lookup(binance.SpotSourceID, symbol)
		if !found {
			return nil, fmt.Errorf("verify: configured symbol %q is absent from the pinned catalog snapshot", symbol)
		}
		if _, duplicate := seenCatalogInstruments[instrument.InstrumentUID]; duplicate {
			return nil, errors.New("verify: configured symbols resolve to a duplicate pinned instrument")
		}
		seenCatalogInstruments[instrument.InstrumentUID] = struct{}{}
	}

	captureContext, cancelCapture := context.WithTimeout(ctx, cfg.Verify.MaxDuration)
	_, captureErr := ensureRawCapture(captureContext, cfg, runtime)
	cancelCapture()
	if captureErr != nil {
		return nil, fmt.Errorf("verify: raw capture/publication: %w", captureErr)
	}
	publications, err := committedPublications(ctx, runtime.catalog)
	if err != nil {
		return nil, err
	}
	publications = selectRunPublications(publications, runtime.wsEpoch, runtime.restEpochs)
	if len(publications) < 1+len(cfg.Sources[0].Symbols) {
		return nil, errors.New("verify: committed raw segments do not cover WebSocket plus every symbol snapshot")
	}
	if err := verifyCommittedObjects(ctx, runtime.objects, publications, cfg.Verify.MaxBytes); err != nil {
		return nil, err
	}
	derivation, segmentEvidence, inputManifestHash, err := deriveReplay(ctx, runtime.objects, publications, cfg.Sources[0].Symbols)
	if err != nil {
		return nil, err
	}
	batch, bookRows, err := normalizeSpot(catalogSnapshot, derivation.dataRecords, cfg.Sources[0].Symbols)
	if err != nil {
		return nil, err
	}
	rows := batch.Rows
	normalizedHash := normalizedRowsHash(rows)
	bookPayloadBytes := make(map[normalize.Hash]uint64, len(derivation.dataRecords))
	for _, record := range derivation.dataRecords {
		bookPayloadBytes[record.Coordinate.RawPayloadSHA256] = uint64(len(record.Envelope.RawPayload))
	}
	bookHash, bookSnapshots, err := reconstructSpotBooks(ctx, catalogSnapshot, bookRows, derivation.snapshotRecords, bookPayloadBytes, cfg.Sources[0].Symbols)
	if err != nil {
		return nil, err
	}

	evidencePath, err := prepareEvidencePath(cfg.Verify.ArtifactRoot)
	if err != nil {
		return nil, err
	}
	var existing *Evidence
	if encoded, readErr := os.ReadFile(evidencePath); readErr == nil {
		packet, decodeErr := unmarshalEvidence(encoded)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if packet.SchemaVersion != EvidenceSchemaVersion || packet.Status != "passed" || packet.Venue != venue ||
			packet.Mode != cfg.Verify.Mode || packet.GapLifecycleStatus != GapLifecycleDeferred ||
			packet.Hashes.ConfigurationSHA256 != configurationHash || packet.Hashes.InputManifestSetSHA256 != inputManifestHash {
			return nil, errors.New("verify: existing evidence identity differs from this committed run")
		}
		existing = &packet
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, readErr
	}

	datasets, datasetLogicalHash, datasetPhysicalHash, parquetRows, err := buildAndVerifyDatasets(ctx, cfg, rows, inputManifestHash, existing)
	if err != nil {
		return nil, err
	}
	counts := derivation.counts
	counts.Symbols = len(cfg.Sources[0].Symbols)
	counts.CommittedSegments = len(publications)
	counts.NormalizedRows = len(rows)
	counts.BookSnapshots = bookSnapshots
	counts.ParquetPartitions = len(datasets)
	counts.ParquetRows = parquetRows
	for _, row := range rows {
		switch row.Kind {
		case normalize.EventTrade:
			counts.Trades++
		case normalize.EventBookUpdate:
			counts.BookUpdates++
		case normalize.EventQuote:
			counts.Quotes++
		case normalize.EventTicker:
			counts.Tickers++
		}
	}
	expectedSymbols := len(cfg.Sources[0].Symbols)
	if counts.NormalizedRows != 4*expectedSymbols || counts.Trades != expectedSymbols || counts.BookUpdates != expectedSymbols ||
		counts.Quotes != expectedSymbols || counts.Tickers != expectedSymbols || counts.BookSnapshots != expectedSymbols ||
		counts.Acknowledgements == 0 || counts.Heartbeats == 0 || counts.Disconnects == 0 ||
		counts.RESTRecords < uint64(expectedSymbols) || counts.ReplayDiscontinuities == 0 {
		return nil, errors.New("verify: vertical-slice evidence inventory is incomplete")
	}
	if build.Version == "" {
		build.Version = "dev"
	}
	if build.Commit == "" {
		build.Commit = "none"
	}
	if build.Date == "" {
		build.Date = "unknown"
	}
	buildVersion := build.Version + "@" + build.Commit
	packet := Evidence{
		SchemaVersion: EvidenceSchemaVersion, Status: "passed", Venue: venue, Mode: cfg.Verify.Mode,
		GapLifecycleStatus: GapLifecycleDeferred, VerifierBuild: build,
		Components: []ComponentEvidence{
			component("capture", VerifierVersion, configurationHash, fixtureInputHash, derivation.rawHash, derivation.rawHash),
			component("catalog", fmt.Sprintf("snapshot-v%d", catalogSnapshot.Version), configurationHash, fixtureInputHash, hex.EncodeToString(catalogSnapshot.SHA256[:]), hex.EncodeToString(catalogSnapshot.SHA256[:])),
			component("object-publication", "immutable-v1", configurationHash, inputManifestHash, inputManifestHash, inputManifestHash),
			component("native-replay", "native-v1", configurationHash, inputManifestHash, hex.EncodeToString(derivation.result.LogicalHash[:]), inputManifestHash),
			component("normalization", binance.SpotMapperVersion, configurationHash, derivation.rawHash, normalizedHash, normalizedHash),
			component("order-book", binance.SpotBookPolicyVersion, configurationHash, normalizedHash, bookHash, bookHash),
			component("parquet", dataset.DatasetVersion, configurationHash, normalizedHash, datasetLogicalHash, datasetPhysicalHash),
			component("verifier", buildVersion, configurationHash, inputManifestHash, datasetLogicalHash, datasetPhysicalHash),
		},
		Counts: counts,
		Hashes: EvidenceHashes{
			ConfigurationSHA256: configurationHash, FixtureManifestSHA256: fixtureManifestHash, FixtureInputSHA256: fixtureInputHash,
			CatalogSnapshotSHA256: hex.EncodeToString(catalogSnapshot.SHA256[:]), InputManifestSetSHA256: inputManifestHash,
			RawRecordSHA256: derivation.rawHash, NativeReplaySHA256: hex.EncodeToString(derivation.result.LogicalHash[:]),
			NormalizedLogicalSHA256: normalizedHash, BookLogicalSHA256: bookHash,
			DatasetLogicalSHA256: datasetLogicalHash, DatasetPhysicalSHA256: datasetPhysicalHash,
		},
		Segments: segmentEvidence, Discontinuities: derivation.discontinuities, Datasets: datasets,
		OpportunityOutcomes: []OutcomeCount{{Outcome: "observed", Count: counts.Opportunities}},
	}
	encoded, err := marshalEvidence(packet)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		prior, err := os.ReadFile(evidencePath)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(prior, encoded) {
			return nil, errors.New("verify: repeated execution changed committed evidence")
		}
		return prior, nil
	}
	if err := writeImmutableFile(evidencePath, encoded); err != nil {
		return nil, err
	}
	return encoded, nil
}

func selectRunPublications(publications []catalog.RawSegmentPublication, websocketEpoch [16]byte, restEpochs [][16]byte) []catalog.RawSegmentPublication {
	websocketID := canonicalUUID(websocketEpoch)
	restIDs := make(map[string]struct{}, len(restEpochs))
	for _, epoch := range restEpochs {
		restIDs[canonicalUUID(epoch)] = struct{}{}
	}
	selected := make([]catalog.RawSegmentPublication, 0, len(publications))
	for _, publication := range publications {
		if publication.SourceID != binance.SpotSourceID {
			continue
		}
		_, isRESTEpoch := restIDs[publication.EpochID]
		if (publication.ChannelID == binance.SpotRawChannel && publication.EpochID == websocketID) ||
			(publication.ChannelID == binance.SpotDepthChannel && isRESTEpoch) {
			selected = append(selected, publication)
		}
	}
	return selected
}

func verifyCommittedObjects(ctx context.Context, objects objectstore.Client, publications []catalog.RawSegmentPublication, maximum int64) error {
	policy := objectstore.DefaultVerifyPolicy()
	policy.FullReadLimit = maximum + 8<<20
	for _, publication := range publications {
		if publication.State != catalog.RawSegmentCommitted || publication.SourceID != binance.SpotSourceID ||
			(publication.ChannelID != binance.SpotRawChannel && publication.ChannelID != binance.SpotDepthChannel) {
			return errors.New("verify: catalog exposed an unexpected committed publication")
		}
		if publication.ByteLength > policy.FullReadLimit {
			return errors.New("verify: committed segment exceeds the configured raw bound plus framing allowance")
		}
		if _, err := objectstore.VerifyObject(ctx, objects, publication.ObjectKey, publication.ByteLength, publication.ContentSHA256, nil, policy); err != nil {
			return fmt.Errorf("verify: full object verification: %w", err)
		}
	}
	return nil
}

func deriveReplay(ctx context.Context, objects objectstore.Client, publications []catalog.RawSegmentPublication, symbols []string) (replayDerivation, []SegmentEvidence, string, error) {
	descriptors := make([]replay.InputDescriptor, len(publications))
	segments := make([]SegmentEvidence, len(publications))
	manifestParts := make([][]byte, 0, len(publications))
	for index, publication := range publications {
		descriptor, err := replay.NewInputDescriptor(publication)
		if err != nil {
			return replayDerivation{}, nil, "", err
		}
		descriptors[index] = descriptor
		segments[index] = SegmentEvidence{
			SegmentID: publication.SegmentID, ChannelID: publication.ChannelID, EpochID: publication.EpochID,
			ObjectKey: publication.ObjectKey, ContentSHA256: hex.EncodeToString(publication.ContentSHA256[:]),
			ManifestSHA256: hex.EncodeToString(publication.ManifestSHA256[:]), FirstOrdinal: publication.OrdinalStart,
			LastOrdinal: publication.OrdinalEnd, RecordCount: publication.OrdinalEnd - publication.OrdinalStart + 1,
		}
		manifestParts = append(manifestParts, publication.ManifestSHA256[:])
	}
	plan, err := binance.NewSpotSubscriptionPlan(symbols)
	if err != nil {
		return replayDerivation{}, nil, "", err
	}
	observer := binance.NewSpotRawObserver(plan)
	result := replayDerivation{snapshotRecords: make(map[string]normalize.RawRecord, len(symbols))}
	rawHasher := sha256.New()
	replayResult, err := replay.ReplaySource(ctx, objects, descriptors, replay.DefaultConfig(), func(event replay.Event) error {
		if event.Kind == replay.EventDiscontinuity {
			result.counts.ReplayDiscontinuities++
			result.discontinuities = append(result.discontinuities, DiscontinuityEvidence{
				Kind: replayDiscontinuityKind(event.Discontinuity.Kind), Reason: replayIntegrityReason(event.Discontinuity.Reason),
				SegmentID: event.Discontinuity.SegmentID, PreviousStreamEpochID: event.Discontinuity.PreviousStreamEpochID,
				FirstOrdinal: event.Discontinuity.FirstOrdinal, LastOrdinal: event.Discontinuity.LastOrdinal,
				FrameOrdinal: event.Discontinuity.FrameOrdinal, CompressedOffset: event.Discontinuity.CompressedOffset,
			})
			return nil
		}
		if event.Kind != replay.EventRecord {
			return errors.New("verify: unknown replay event")
		}
		envelope, err := capture.EnvelopeV1FromSegment(event.Record)
		if err != nil {
			return err
		}
		publication, found := publicationForEnvelope(publications, envelope)
		if !found {
			return errors.New("verify: replay record is not covered by a committed publication")
		}
		record, err := normalize.BindRawRecord(envelope, normalize.Hash(publication.ContentSHA256), envelope.ArrivalOrdinal, nil)
		if err != nil {
			return err
		}
		result.rawRecords = append(result.rawRecords, record)
		result.counts.RawRecords++
		result.counts.ReplayRecords++
		_, _ = rawHasher.Write(publication.ContentSHA256[:])
		_, _ = rawHasher.Write(envelope.RawPayloadSHA256[:])
		var ordinal [12]byte
		for i := range 8 {
			ordinal[7-i] = byte(envelope.ArrivalOrdinal >> (i * 8))
		}
		for i := range 4 {
			ordinal[11-i] = byte(envelope.MessageOrdinal >> (i * 8))
		}
		_, _ = rawHasher.Write(ordinal[:])
		switch envelope.RecordKind {
		case capture.RecordKindWebSocket:
			result.counts.WebSocketRecords++
			observation, err := observer.Observe(ctx, envelope)
			if err != nil {
				return err
			}
			switch observation.Role {
			case capture.MessageData:
				if observation.Schema != capture.SchemaAccepted {
					return errors.New("verify: fixture data schema was not accepted")
				}
				result.dataRecords = append(result.dataRecords, record)
			case capture.MessageAcknowledgement:
				result.counts.Acknowledgements++
				result.counts.Opportunities++
			}
		case capture.RecordKindREST:
			result.counts.RESTRecords++
			result.counts.Opportunities++
			if envelope.HTTPStatusOrWSState.Valid && envelope.HTTPStatusOrWSState.Value == "200" && envelope.NativeSymbol.Valid {
				if _, exists := result.snapshotRecords[envelope.NativeSymbol.Value]; exists {
					return errors.New("verify: duplicate captured depth snapshot for one symbol")
				}
				result.snapshotRecords[envelope.NativeSymbol.Value] = record
			}
		case capture.RecordKindControl:
			result.counts.ControlRecords++
			if envelope.ControlKind.Valid {
				switch envelope.ControlKind.Value {
				case capture.ControlAcknowledgement:
					result.counts.Acknowledgements++
					result.counts.Opportunities++
				case capture.ControlHeartbeat:
					result.counts.Heartbeats++
					result.counts.Opportunities++
				case capture.ControlDisconnect, capture.ControlShutdown:
					result.counts.Disconnects++
				}
			}
		}
		return nil
	})
	if err != nil {
		return replayDerivation{}, nil, "", err
	}
	result.result = replayResult
	result.rawHash = hex.EncodeToString(rawHasher.Sum(nil))
	return result, segments, hashesHex(manifestParts...), nil
}

func replayDiscontinuityKind(kind replay.DiscontinuityKind) string {
	switch kind {
	case replay.DiscontinuityEpochBoundary:
		return "epoch_boundary"
	case replay.DiscontinuityDisconnect:
		return "disconnect"
	case replay.DiscontinuityQuarantinedFrame:
		return "quarantined_frame"
	case replay.DiscontinuityMissingSegment:
		return "missing_segment"
	case replay.DiscontinuityOrdinalGap:
		return "ordinal_gap"
	case replay.DiscontinuityOrdinalOverlap:
		return "ordinal_overlap"
	default:
		return "unknown"
	}
}

func replayIntegrityReason(reason replay.IntegrityReason) string {
	switch reason {
	case replay.IntegrityReasonNone:
		return "none"
	case replay.IntegrityReasonObjectLength:
		return "object_length"
	case replay.IntegrityReasonObjectSHA256:
		return "object_sha256"
	case replay.IntegrityReasonFrame:
		return "frame"
	case replay.IntegrityReasonRecord:
		return "record"
	case replay.IntegrityReasonRecordCount:
		return "record_count"
	case replay.IntegrityReasonIdentity:
		return "identity"
	case replay.IntegrityReasonOrdinalOrder:
		return "ordinal_order"
	default:
		return "unknown"
	}
}

func publicationForEnvelope(publications []catalog.RawSegmentPublication, envelope capture.EnvelopeV1) (catalog.RawSegmentPublication, bool) {
	epoch, err := envelope.StreamEpoch()
	if err != nil {
		return catalog.RawSegmentPublication{}, false
	}
	epochID := canonicalUUID(epoch.ID)
	for _, publication := range publications {
		if publication.SourceID == envelope.SourceID && publication.ChannelID == envelope.ChannelOrEndpoint && publication.EpochID == epochID &&
			envelope.ArrivalOrdinal >= publication.OrdinalStart && envelope.ArrivalOrdinal <= publication.OrdinalEnd {
			return publication, true
		}
	}
	return catalog.RawSegmentPublication{}, false
}

func canonicalUUID(value [16]byte) string {
	raw := hex.EncodeToString(value[:])
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:]
}

func normalizeSpot(snapshot catalog.Snapshot, records []normalize.RawRecord, symbols []string) (normalize.Batch, []normalize.Row, error) {
	bound, err := binance.NewSpotMapperBinding(normalize.Hash(snapshot.SHA256), binance.SpotMapperVersion, 0, normalize.OptionalInt64{}, normalize.ResolutionMillisecond, nil)
	if err != nil {
		return normalize.Batch{}, nil, err
	}
	orchestrator, err := normalize.NewOrchestrator(snapshot, []normalize.BoundMapper{bound})
	if err != nil {
		return normalize.Batch{}, nil, err
	}
	batch, err := orchestrator.Normalize(records)
	if err != nil {
		return normalize.Batch{}, nil, err
	}
	if len(batch.Quarantines) != 0 {
		return normalize.Batch{}, nil, errors.New("verify: Spot normalization quarantined an observed public-data row")
	}
	view, err := normalize.NewCatalogView(snapshot)
	if err != nil {
		return normalize.Batch{}, nil, err
	}
	kinds := []normalize.EventKind{normalize.EventTrade, normalize.EventBookUpdate, normalize.EventQuote, normalize.EventTicker}
	selected := make([]normalize.Row, 0, len(symbols)*len(kinds))
	seenInstruments := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		instrument, found := view.Lookup(binance.SpotSourceID, symbol)
		if !found {
			return normalize.Batch{}, nil, fmt.Errorf("verify: configured symbol %q is absent from the pinned catalog", symbol)
		}
		if _, duplicate := seenInstruments[instrument.InstrumentUID]; duplicate {
			return normalize.Batch{}, nil, errors.New("verify: configured symbols resolve to a duplicate instrument")
		}
		seenInstruments[instrument.InstrumentUID] = struct{}{}
		for _, kind := range kinds {
			found = false
			for _, row := range batch.Rows {
				if row.Kind == kind && row.Common().InstrumentUID == instrument.InstrumentUID {
					selected = append(selected, row)
					found = true
					break
				}
			}
			if !found {
				return normalize.Batch{}, nil, fmt.Errorf("verify: symbol %q did not produce normalized %s evidence", symbol, kind)
			}
		}
	}
	bookRows := make([]normalize.Row, 0)
	for _, row := range batch.Rows {
		if row.Kind == normalize.EventBookUpdate {
			bookRows = append(bookRows, row)
		}
	}
	return normalize.Batch{Rows: selected}, bookRows, nil
}

func reconstructSpotBooks(
	ctx context.Context,
	snapshot catalog.Snapshot,
	rows []normalize.Row,
	rawSnapshots map[string]normalize.RawRecord,
	rawPayloadBytes map[normalize.Hash]uint64,
	symbols []string,
) (string, int, error) {
	view, err := normalize.NewCatalogView(snapshot)
	if err != nil {
		return "", 0, err
	}
	hashes := make([][]byte, 0, len(symbols))
	for _, symbol := range symbols {
		rawSnapshot, found := rawSnapshots[symbol]
		if !found {
			return "", 0, fmt.Errorf("verify: captured depth snapshot for %q is absent", symbol)
		}
		instrument, found := view.Lookup(binance.SpotSourceID, symbol)
		if !found {
			return "", 0, errors.New("verify: order-book instrument is absent from the pinned catalog")
		}
		updates := make([]normalize.BookUpdateV1, 0)
		for index := range rows {
			if rows[index].Kind == normalize.EventBookUpdate && rows[index].Common().InstrumentUID == instrument.InstrumentUID {
				updates = append(updates, *rows[index].BookUpdate)
			}
		}
		if len(updates) == 0 {
			return "", 0, errors.New("verify: normalized book update is absent")
		}
		seed, err := binance.ParseSpotBookSnapshot(rawSnapshot, instrument)
		if err != nil {
			return "", 0, err
		}
		boundary := seed.LastSequence + 1
		var bridge *normalize.BookUpdateV1
		for index := range updates {
			if updates[index].FirstSequence <= boundary && boundary <= updates[index].LastSequence {
				bridge = &updates[index]
				break
			}
		}
		if bridge == nil {
			return "", 0, fmt.Errorf(
				"verify: captured depth updates do not span the REST snapshot boundary (snapshot_last=%d first_update=%d last_update=%d update_count=%d)",
				seed.LastSequence, updates[0].FirstSequence, updates[len(updates)-1].LastSequence, len(updates),
			)
		}
		book, err := binance.NewSpotBook(binance.SpotBookConfig{
			Instrument: instrument, CatalogSnapshotID: normalize.Hash(snapshot.SHA256), MapperVersion: bridge.Metadata.MapperVersion,
			MapperBindingID: bridge.Metadata.MapperBindingID,
		}, fixedSnapshotFetcher{snapshot: seed})
		if err != nil {
			return "", 0, err
		}
		payloadBytes, found := rawPayloadBytes[bridge.Metadata.RawPayloadSHA256]
		if !found {
			return "", 0, errors.New("verify: book update payload length is absent from replay evidence")
		}
		if _, err := book.Accept(ctx, *bridge, payloadBytes); err != nil {
			return "", 0, err
		}
		seeded, err := book.Seed(ctx)
		if err != nil {
			return "", 0, err
		}
		if seeded.Output == nil {
			return "", 0, fmt.Errorf(
				"verify: order-book reconstruction emitted no snapshot (snapshot_last=%d first_update=%d last_update=%d update_count=%d state=%s)",
				seed.LastSequence, updates[0].FirstSequence, updates[len(updates)-1].LastSequence, len(updates), seeded.State,
			)
		}
		if err := seeded.Output.Validate(); err != nil {
			return "", 0, err
		}
		hashes = append(hashes, append([]byte(symbol+"\x00"), seeded.Output.ProjectionHash[:]...))
	}
	return hashesHex(hashes...), len(hashes), nil
}

func buildAndVerifyDatasets(ctx context.Context, cfg config.Config, rows []normalize.Row, inputManifestHash string, existing *Evidence) ([]DatasetEvidence, string, string, uint64, error) {
	root := filepath.Join(cfg.Verify.ArtifactRoot, "datasets")
	if err := ensureContainedDirectory(cfg.Verify.ArtifactRoot, root); err != nil {
		return nil, "", "", 0, err
	}
	if existing != nil {
		datasets := slices.Clone(existing.Datasets)
		if len(datasets) != 4 {
			return nil, "", "", 0, errors.New("verify: existing evidence does not name all four dataset families")
		}
		var parquetRows uint64
		for _, item := range datasets {
			verification, err := dataset.VerifyManifest(ctx, root, filepath.FromSlash(item.ManifestFile))
			if err != nil {
				return nil, "", "", 0, err
			}
			manifestBytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(item.ManifestFile)))
			if err != nil || hashHex(manifestBytes) != item.ManifestSHA256 {
				return nil, "", "", 0, errors.New("verify: existing dataset manifest hash differs from evidence")
			}
			if verification.Manifest.LogicalSHA256 != item.LogicalSHA256 || verification.Manifest.PhysicalSHA256 != item.PhysicalSHA256 ||
				verification.Manifest.InputRows != item.InputRows || verification.Manifest.ParquetRows != item.ParquetRows {
				return nil, "", "", 0, errors.New("verify: existing dataset evidence differs from full verification")
			}
			parquetRows += item.ParquetRows
		}
		return datasets, datasetAggregateHash(datasets, true), datasetAggregateHash(datasets, false), parquetRows, nil
	}
	inputID, err := decodeNormalizeHash(inputManifestHash)
	if err != nil {
		return nil, "", "", 0, err
	}
	datasetPolicy := normalize.Hash(sha256.Sum256([]byte(fmt.Sprintf("dataset-policy-v1\x00%d\x00%s", cfg.Dataset.RowGroupBytes, cfg.Dataset.Compression))))
	replayConfigBytes, _ := json.Marshal(replay.DefaultConfig())
	replayConfigID := normalize.Hash(sha256.Sum256(replayConfigBytes))
	options := dataset.DefaultWriterOptions(datasetPolicy, replayConfigID, inputID)
	options.RowGroupTargetBytes = cfg.Dataset.RowGroupBytes
	families := []normalize.EventKind{normalize.EventTrade, normalize.EventBookUpdate, normalize.EventQuote, normalize.EventTicker}
	result := make([]DatasetEvidence, 0, len(families))
	var parquetRows uint64
	for _, family := range families {
		var selected []normalize.Row
		for _, row := range rows {
			if row.Kind == family {
				selected = append(selected, row)
			}
		}
		if len(selected) != len(cfg.Sources[0].Symbols) {
			return nil, "", "", 0, fmt.Errorf(
				"verify: %s dataset family contains %d rows for %d configured symbols",
				family, len(selected), len(cfg.Sources[0].Symbols),
			)
		}
		built, err := dataset.BuildNormalizedPartition(ctx, root, &dataset.SliceNormalizedSource{Rows: selected}, options)
		if err != nil {
			return nil, "", "", 0, fmt.Errorf("verify: build %s dataset: %w", family, err)
		}
		verified, err := dataset.VerifyManifest(ctx, root, built.ManifestPath)
		if err != nil {
			return nil, "", "", 0, fmt.Errorf("verify: verify %s dataset: %w", family, err)
		}
		relative, err := filepath.Rel(root, built.ManifestPath)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil, "", "", 0, errors.New("verify: dataset manifest escaped artifact root")
		}
		item := DatasetEvidence{
			Family: string(verified.Manifest.Family), ManifestFile: filepath.ToSlash(relative), ManifestSHA256: hex.EncodeToString(built.ManifestHash[:]),
			LogicalSHA256: verified.Manifest.LogicalSHA256, PhysicalSHA256: verified.Manifest.PhysicalSHA256,
			InputRows: verified.Manifest.InputRows, ParquetRows: verified.Manifest.ParquetRows,
		}
		result = append(result, item)
		parquetRows += item.ParquetRows
	}
	slices.SortFunc(result, func(left, right DatasetEvidence) int { return strings.Compare(left.Family, right.Family) })
	return result, datasetAggregateHash(result, true), datasetAggregateHash(result, false), parquetRows, nil
}

func datasetAggregateHash(datasets []DatasetEvidence, logical bool) string {
	parts := make([][]byte, 0, len(datasets))
	for _, item := range datasets {
		value := item.PhysicalSHA256
		if logical {
			value = item.LogicalSHA256
		}
		parts = append(parts, []byte(item.Family+"\x00"+value))
	}
	return hashesHex(parts...)
}

func normalizedRowsHash(rows []normalize.Row) string {
	parts := make([][]byte, len(rows))
	for index := range rows {
		parts[index] = rows[index].LogicalHash[:]
	}
	return hashesHex(parts...)
}

func decodeNormalizeHash(value string) (normalize.Hash, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return normalize.Hash{}, errors.New("verify: invalid immutable hash identity")
	}
	var result normalize.Hash
	copy(result[:], decoded)
	return result, nil
}

func component(name, version, configHash, inputHash, logicalHash, physicalHash string) ComponentEvidence {
	if inputHash == "" {
		inputHash = configHash
	}
	return ComponentEvidence{Name: name, Version: version, ConfigSHA256: configHash, InputSHA256: inputHash, LogicalSHA256: logicalHash, PhysicalSHA256: physicalHash}
}

func verificationConfigHash(venue string, cfg config.Config) (string, error) {
	projection := struct {
		Venue       string                   `json:"venue"`
		Mode        string                   `json:"mode"`
		Sources     []config.SourceConfig    `json:"sources"`
		ObjectStore config.ObjectStoreConfig `json:"object_store"`
		CatalogRef  string                   `json:"catalog_ref"`
		Maximums    struct {
			Messages int           `json:"messages"`
			Bytes    int64         `json:"bytes"`
			Duration time.Duration `json:"duration"`
			Depth    int           `json:"depth"`
		} `json:"maximums"`
		Dataset struct {
			RowGroupBytes int64  `json:"row_group_bytes"`
			Compression   string `json:"compression"`
		} `json:"dataset"`
	}{Venue: venue, Mode: cfg.Verify.Mode, Sources: slices.Clone(cfg.Sources), ObjectStore: cfg.ObjectStore, CatalogRef: cfg.Catalog.DSNRef}
	projection.Maximums.Messages = cfg.Verify.MaxMessages
	projection.Maximums.Bytes = cfg.Verify.MaxBytes
	projection.Maximums.Duration = cfg.Verify.MaxDuration
	projection.Maximums.Depth = cfg.Verify.DepthLimit
	projection.Dataset.RowGroupBytes = cfg.Dataset.RowGroupBytes
	projection.Dataset.Compression = cfg.Dataset.Compression
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", err
	}
	return hashHex(encoded), nil
}

func prepareEvidencePath(root string) (string, error) {
	directory := filepath.Join(root, evidenceDirectoryName)
	if err := ensureContainedDirectory(root, directory); err != nil {
		return "", err
	}
	return filepath.Join(directory, EvidenceFileName), nil
}

func writeImmutableFile(path string, contents []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".evidence-*.tmp")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	flow := fileflow.Flow{
		NoCreateDirs: true,
		FindAvailableName: func(string) (string, error) {
			return "", errors.New("verify: immutable evidence identity conflict")
		},
	}
	final, err := flow.Move(name, path)
	if err != nil {
		return err
	}
	if final != path {
		return errors.New("verify: immutable evidence moved to an unexpected path")
	}
	return syncDirectory(filepath.Dir(path))
}
