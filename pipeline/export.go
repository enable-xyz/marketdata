package pipeline

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/replay"
)

type Exporter struct {
	raw        RawCatalog
	catalog    ExportCatalog
	objects    objectstore.Client
	normalizer Normalizer
	config     ExporterConfig
}

func NewExporter(raw RawCatalog, publicationCatalog ExportCatalog, objects objectstore.Client, normalizer Normalizer, config ExporterConfig) (*Exporter, error) {
	if raw == nil || publicationCatalog == nil || objects == nil || normalizer == nil {
		return nil, fmt.Errorf("%w: raw catalog, dataset catalog, object store, and normalizer are required", ErrInvalidPipeline)
	}
	normalized, err := validateExporterConfig(config)
	if err != nil {
		return nil, err
	}
	return &Exporter{raw: raw, catalog: publicationCatalog, objects: objects, normalizer: normalizer, config: normalized}, nil
}

func (e *Exporter) RunOnce(ctx context.Context, request ExportRequest) (receipt ExportReceipt, err error) {
	if e == nil || ctx == nil {
		return receipt, fmt.Errorf("%w: exporter and context are required", ErrInvalidPipeline)
	}
	if err := validateExportRequest(request, e.config.MaxSegments); err != nil {
		return receipt, err
	}
	receipt.SourceID = request.SourceID
	publications, err := selectCommittedPublications(ctx, e.raw, request)
	if err != nil {
		return receipt, err
	}
	publications = orderPublications(publications)
	descriptors := make([]replay.InputDescriptor, len(publications))
	for i, publication := range publications {
		descriptor, descriptorErr := replay.NewInputDescriptor(publication)
		if descriptorErr != nil {
			return receipt, fmt.Errorf("pipeline: bind committed raw segment %q: %w", publication.SegmentID, descriptorErr)
		}
		descriptors[i] = descriptor
		receipt.Inputs = append(receipt.Inputs, rawInputReceipt(publication))
	}
	inputSetID := inputManifestSetID(publications)
	receipt.InputManifestSetID = inputSetID

	partitions := make(map[partitionKey]*partitionRows)
	tracker, err := newSegmentTracker(publications, descriptors)
	if err != nil {
		return receipt, err
	}
	batch := make([]normalize.RawRecord, 0, e.config.NormalizeBatch)
	var outputRows uint64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		inputs := batch
		normalized, normalizeErr := e.normalizer.Normalize(inputs)
		if normalizeErr != nil {
			return fmt.Errorf("pipeline: normalize committed replay batch: %w", normalizeErr)
		}
		if err := validateNormalizationAccounting(inputs, normalized); err != nil {
			return err
		}
		batch = batch[:0]
		additional := uint64(len(normalized.Rows) + len(normalized.Quarantines))
		if additional > e.config.MaxOutputRows-outputRows {
			return fmt.Errorf("%w: normalized and quarantine rows exceed %d", ErrPipelineBound, e.config.MaxOutputRows)
		}
		outputRows += additional
		receipt.Replay.NormalizedRows += uint64(len(normalized.Rows))
		receipt.Replay.QuarantinedRows += uint64(len(normalized.Quarantines))
		for _, row := range normalized.Rows {
			metadata := row.Common()
			if metadata.SourceID != request.SourceID || metadata.CatalogSnapshotID != e.config.CatalogSnapshotID {
				return fmt.Errorf("%w: normalized row changed source or catalog snapshot identity", ErrPublicationConflict)
			}
			if err := addNormalizedPartition(partitions, row, e.config.MaxPartitions); err != nil {
				return err
			}
		}
		for _, row := range normalized.Quarantines {
			if row.SourceID != request.SourceID || row.CatalogSnapshotID != e.config.CatalogSnapshotID {
				return fmt.Errorf("%w: quarantine row changed source or catalog snapshot identity", ErrPublicationConflict)
			}
			if err := addQuarantinePartition(partitions, row, e.config.MaxPartitions); err != nil {
				return err
			}
		}
		return nil
	}

	replayResult, replayErr := replay.ReplaySource(ctx, e.objects, descriptors, e.config.Replay, func(event replay.Event) error {
		switch event.Kind {
		case replay.EventDiscontinuity:
			receipt.Replay.Discontinuities = append(receipt.Replay.Discontinuities, event.Discontinuity)
			switch event.Discontinuity.Kind {
			case replay.DiscontinuityEpochBoundary, replay.DiscontinuityDisconnect, replay.DiscontinuityOrdinalGap:
				return nil
			case replay.DiscontinuityQuarantinedFrame, replay.DiscontinuityMissingSegment, replay.DiscontinuityOrdinalOverlap:
				return fmt.Errorf("%w: kind=%d segment=%q ordinals=%d..%d", ErrReplayDiscontinuity, event.Discontinuity.Kind,
					event.Discontinuity.SegmentID, event.Discontinuity.FirstOrdinal, event.Discontinuity.LastOrdinal)
			default:
				return fmt.Errorf("%w: unknown kind %d", ErrReplayDiscontinuity, event.Discontinuity.Kind)
			}
		case replay.EventRecord:
			if receipt.Replay.RecordCount == e.config.MaxRecords {
				return fmt.Errorf("%w: replay records exceed %d", ErrPipelineBound, e.config.MaxRecords)
			}
			envelope, conversionErr := capture.EnvelopeV1FromOwnedSegment(event.Record)
			if conversionErr != nil {
				return fmt.Errorf("pipeline: raise replayed envelope: %w", conversionErr)
			}
			segmentHash, rawOrdinal, trackingErr := tracker.bind(event)
			if trackingErr != nil {
				return trackingErr
			}
			receipt.Replay.RecordCount++
			if envelope.ReceivedWallTimeNS < request.StartReceivedTimeNS ||
				envelope.ReceivedWallTimeNS >= request.EndReceivedTimeNS {
				return nil
			}
			record, bindingErr := normalize.BindRawRecord(envelope, normalize.Hash(segmentHash), rawOrdinal, nil)
			if bindingErr != nil {
				return fmt.Errorf("pipeline: bind raw normalization coordinate: %w", bindingErr)
			}
			batch = append(batch, record)
			if len(batch) == e.config.NormalizeBatch {
				return flush()
			}
			return nil
		default:
			return fmt.Errorf("%w: unknown replay event kind %d", ErrReplayDiscontinuity, event.Kind)
		}
	})
	if replayErr != nil {
		return receipt, replayErr
	}
	if err := flush(); err != nil {
		return receipt, err
	}
	if err := tracker.complete(); err != nil {
		return receipt, err
	}
	receipt.Replay.Order = replayResult.Order
	receipt.Replay.LogicalHashVersion = replayResult.LogicalHashVersion
	receipt.Replay.LogicalHash = replayResult.LogicalHash
	receipt.Replay.EventCount = replayResult.EventCount
	if len(partitions) == 0 {
		return receipt, ErrEmptyExport
	}

	writer := e.config.Writer
	writer.InputManifestSetID = normalize.Hash(inputSetID)
	orderedPartitions := slices.SortedFunc(maps.Values(partitions), comparePartitionRows)
	built := make([]builtPartition, 0, len(orderedPartitions))
	for _, partition := range orderedPartitions {
		if uint64(partition.rowCount()) > writer.MaxInputRows {
			return receipt, fmt.Errorf("%w: partition %s has %d rows", ErrPipelineBound, partition.key.String(), partition.rowCount())
		}
		var result dataset.BuildResult
		if partition.key.family == dataset.FamilySchemaQuarantine {
			result, err = dataset.BuildQuarantinePartition(ctx, request.BuildRoot, &dataset.SliceQuarantineSource{Rows: slices.Clone(partition.quarantines)}, writer)
		} else {
			result, err = dataset.BuildNormalizedPartition(ctx, request.BuildRoot, &dataset.SliceNormalizedSource{Rows: slices.Clone(partition.normalized)}, writer)
		}
		if err != nil {
			return receipt, fmt.Errorf("pipeline: build deterministic partition %s: %w", partition.key.String(), err)
		}
		built = append(built, builtPartition{result: result, coverage: partition.coverage})
	}

	segmentIDs := make([]string, len(publications))
	for i, publication := range publications {
		segmentIDs[i] = publication.SegmentID
	}
	for _, partition := range built {
		datasetReceipt, publicationErr := e.publishDataset(ctx, request, partition, segmentIDs, inputSetID)
		receipt.Datasets = append(receipt.Datasets, datasetReceipt)
		if publicationErr != nil {
			return receipt, publicationErr
		}
	}
	receipt.Complete = true
	return receipt, nil
}

func validateExportRequest(request ExportRequest, maxSegments int) error {
	if request.SourceID == "" || !utf8.ValidString(request.SourceID) || strings.TrimSpace(request.SourceID) != request.SourceID || strings.IndexByte(request.SourceID, 0) >= 0 ||
		request.BuildRoot == "" || len(request.SegmentIDs) == 0 || len(request.SegmentIDs) > maxSegments ||
		request.StartReceivedTimeNS < 0 || request.StartReceivedTimeNS >= request.EndReceivedTimeNS {
		return fmt.Errorf("%w: source, explicit segment set, half-open receive-time range, build root, and segment bound are required", ErrInvalidPipeline)
	}
	info, err := os.Lstat(request.BuildRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: build root must be an existing real directory", ErrInvalidPipeline)
	}
	if err := validateObjectPrefix(request.ObjectPrefix); err != nil {
		return err
	}
	ids := slices.Clone(request.SegmentIDs)
	slices.Sort(ids)
	for i, id := range ids {
		if id == "" || strings.TrimSpace(id) != id || strings.IndexByte(id, 0) >= 0 || (i > 0 && id == ids[i-1]) {
			return fmt.Errorf("%w: segment IDs must be nonempty and unique", ErrInvalidPipeline)
		}
	}
	return nil
}

func validateObjectPrefix(value string) error {
	if value == "" || len(value) > 1024 || !utf8.ValidString(value) || strings.TrimSpace(value) != value || strings.HasPrefix(value, "/") ||
		strings.HasSuffix(value, "/") || strings.Contains(value, "\\") || strings.IndexByte(value, 0) >= 0 || path.Clean(value) != value || !filepath.IsLocal(filepath.FromSlash(value)) {
		return fmt.Errorf("%w: object prefix must be an explicit clean relative key", ErrInvalidPipeline)
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("%w: object prefix segment", ErrInvalidPipeline)
		}
	}
	return nil
}

var errSelectionComplete = errors.New("pipeline: selection complete")

func selectCommittedPublications(ctx context.Context, source RawCatalog, request ExportRequest) ([]catalog.RawSegmentPublication, error) {
	requested := make(map[string]struct{}, len(request.SegmentIDs))
	for _, id := range request.SegmentIDs {
		requested[id] = struct{}{}
	}
	selected := make(map[string]catalog.RawSegmentPublication, len(requested))
	err := source.StreamCommittedRawSegments(ctx, func(publication catalog.RawSegmentPublication) error {
		if _, wanted := requested[publication.SegmentID]; !wanted {
			return nil
		}
		if publication.State != catalog.RawSegmentCommitted || publication.SourceID != request.SourceID {
			return fmt.Errorf("%w: segment %q is not a committed publication for source %q", ErrRawSelection, publication.SegmentID, request.SourceID)
		}
		if _, duplicate := selected[publication.SegmentID]; duplicate {
			return fmt.Errorf("%w: duplicate catalog segment %q", ErrRawSelection, publication.SegmentID)
		}
		selected[publication.SegmentID] = publication
		if len(selected) == len(requested) {
			return errSelectionComplete
		}
		return nil
	})
	if err != nil && !errors.Is(err, errSelectionComplete) {
		return nil, fmt.Errorf("pipeline: select committed raw manifests: %w", err)
	}
	if len(selected) != len(requested) {
		missing := make([]string, 0, len(requested)-len(selected))
		for id := range requested {
			if _, found := selected[id]; !found {
				missing = append(missing, id)
			}
		}
		slices.Sort(missing)
		return nil, fmt.Errorf("%w: committed segments not found: %s", ErrRawSelection, strings.Join(missing, ","))
	}
	result := make([]catalog.RawSegmentPublication, 0, len(selected))
	for _, publication := range selected {
		result = append(result, publication)
	}
	return result, nil
}

func orderPublications(publications []catalog.RawSegmentPublication) []catalog.RawSegmentPublication {
	result := slices.Clone(publications)
	epochStart := make(map[string]int64, len(result))
	for _, publication := range result {
		start, found := epochStart[publication.EpochID]
		if !found || publication.ReceivedStartNS < start {
			epochStart[publication.EpochID] = publication.ReceivedStartNS
		}
	}
	slices.SortFunc(result, func(left, right catalog.RawSegmentPublication) int {
		if order := cmp.Compare(epochStart[left.EpochID], epochStart[right.EpochID]); order != 0 {
			return order
		}
		if order := cmp.Compare(left.EpochID, right.EpochID); order != 0 {
			return order
		}
		if order := cmp.Compare(left.OrdinalStart, right.OrdinalStart); order != 0 {
			return order
		}
		if order := cmp.Compare(left.OrdinalEnd, right.OrdinalEnd); order != 0 {
			return order
		}
		return cmp.Compare(left.SegmentID, right.SegmentID)
	})
	return result
}

func rawInputReceipt(publication catalog.RawSegmentPublication) RawInputReceipt {
	return RawInputReceipt{SegmentID: publication.SegmentID, SourceID: publication.SourceID, ChannelID: publication.ChannelID,
		EpochID: publication.EpochID, ObjectKey: publication.ObjectKey, OrdinalStart: publication.OrdinalStart, OrdinalEnd: publication.OrdinalEnd,
		ContentSHA256: publication.ContentSHA256, ManifestSHA256: publication.ManifestSHA256}
}

func inputManifestSetID(publications []catalog.RawSegmentPublication) [sha256.Size]byte {
	hasher := sha256.New()
	writeDigestString(hasher, "pipeline-input-manifest-set-v1")
	for _, publication := range publications {
		writeDigestString(hasher, publication.SegmentID)
		writeDigestString(hasher, publication.ObjectKey)
		_, _ = hasher.Write(publication.ContentSHA256[:])
		_, _ = hasher.Write(publication.ManifestSHA256[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func writeDigestString(writer io.Writer, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = io.WriteString(writer, value)
}

type segmentTrackerKey struct {
	source  string
	channel string
	epoch   string
}

type trackedSegment struct {
	publication catalog.RawSegmentPublication
	expected    uint64
	seen        uint64
}

type segmentTracker struct {
	byKey map[segmentTrackerKey][]*trackedSegment
	all   []*trackedSegment
}

func newSegmentTracker(publications []catalog.RawSegmentPublication, descriptors []replay.InputDescriptor) (*segmentTracker, error) {
	tracker := &segmentTracker{byKey: make(map[segmentTrackerKey][]*trackedSegment), all: make([]*trackedSegment, 0, len(publications))}
	for i, publication := range publications {
		tracked := &trackedSegment{publication: publication, expected: descriptors[i].RecordCount()}
		key := segmentTrackerKey{source: publication.SourceID, channel: publication.ChannelID, epoch: publication.EpochID}
		tracker.byKey[key] = append(tracker.byKey[key], tracked)
		tracker.all = append(tracker.all, tracked)
	}
	for key, segments := range tracker.byKey {
		slices.SortFunc(segments, func(left, right *trackedSegment) int {
			if order := cmp.Compare(left.publication.OrdinalStart, right.publication.OrdinalStart); order != 0 {
				return order
			}
			return cmp.Compare(left.publication.OrdinalEnd, right.publication.OrdinalEnd)
		})
		for i := 1; i < len(segments); i++ {
			if segments[i].publication.OrdinalStart <= segments[i-1].publication.OrdinalEnd {
				return nil, fmt.Errorf("%w: overlapping raw segment ranges for %v", ErrRawSelection, key)
			}
		}
	}
	return tracker, nil
}

func (t *segmentTracker) bind(event replay.Event) ([sha256.Size]byte, uint64, error) {
	key := segmentTrackerKey{source: event.Record.SourceID, channel: event.Record.ChannelOrEndpoint, epoch: event.Coordinate.StreamEpochID}
	segments := t.byKey[key]
	index, found := slices.BinarySearchFunc(segments, event.Record.ArrivalOrdinal, func(candidate *trackedSegment, arrival uint64) int {
		if candidate.publication.OrdinalEnd < arrival {
			return -1
		}
		if candidate.publication.OrdinalStart > arrival {
			return 1
		}
		return 0
	})
	var match *trackedSegment
	if found {
		match = segments[index]
	}
	if match == nil {
		return [sha256.Size]byte{}, 0, fmt.Errorf("%w: replay coordinate has no committed segment lineage", ErrRawSelection)
	}
	ordinal := match.seen
	match.seen++
	if match.seen > match.expected {
		return [sha256.Size]byte{}, 0, fmt.Errorf("%w: replay emitted too many records for segment %q", ErrRawSelection, match.publication.SegmentID)
	}
	return match.publication.ContentSHA256, ordinal, nil
}

func (t *segmentTracker) complete() error {
	for _, segment := range t.all {
		if segment.seen != segment.expected {
			return fmt.Errorf("%w: segment %q emitted %d records, expected %d", ErrRawSelection, segment.publication.SegmentID, segment.seen, segment.expected)
		}
	}
	return nil
}

type normalizationCoordinate struct {
	source        string
	channel       string
	epoch         [16]byte
	arrival       uint64
	message       uint32
	segmentHash   normalize.Hash
	recordOrdinal uint64
	payloadHash   normalize.Hash
}

func validateNormalizationAccounting(inputs []normalize.RawRecord, output normalize.Batch) error {
	const (
		accountedAccepted uint8 = iota + 1
		accountedQuarantined
	)
	accounted := make(map[normalizationCoordinate]uint8, len(inputs))
	for _, input := range inputs {
		coordinate := normalizationCoordinateFromRaw(input)
		if _, duplicate := accounted[coordinate]; duplicate {
			return fmt.Errorf("%w: duplicate raw normalization coordinate", ErrRawSelection)
		}
		accounted[coordinate] = 0
	}
	for _, row := range output.Rows {
		coordinate := normalizationCoordinateFromMetadata(row.Common())
		state, found := accounted[coordinate]
		if !found || state == accountedQuarantined {
			return fmt.Errorf("%w: normalized row is not exclusively bound to an input record", ErrRawSelection)
		}
		accounted[coordinate] = accountedAccepted
	}
	for _, quarantine := range output.Quarantines {
		coordinate := normalizationCoordinate{
			source: quarantine.SourceID, channel: quarantine.ChannelID, epoch: quarantine.Coordinate.EpochID,
			arrival: quarantine.Coordinate.ArrivalOrdinal, message: quarantine.Coordinate.MessageOrdinal,
			segmentHash: quarantine.Coordinate.RawSegmentSHA256, recordOrdinal: quarantine.Coordinate.RawRecordOrdinal,
			payloadHash: quarantine.Coordinate.RawPayloadSHA256,
		}
		state, found := accounted[coordinate]
		if !found || state != 0 {
			return fmt.Errorf("%w: quarantine is not exclusively bound to one input record", ErrRawSelection)
		}
		accounted[coordinate] = accountedQuarantined
	}
	for _, state := range accounted {
		if state == 0 {
			return fmt.Errorf("%w: normalizer silently omitted a committed raw record", ErrRawSelection)
		}
	}
	return nil
}

func normalizationCoordinateFromRaw(record normalize.RawRecord) normalizationCoordinate {
	return normalizationCoordinate{
		source: record.Coordinate.SourceID, channel: record.Coordinate.ChannelID, epoch: record.Coordinate.EpochID,
		arrival: record.Coordinate.ArrivalOrdinal, message: record.Coordinate.MessageOrdinal,
		segmentHash: record.Coordinate.RawSegmentSHA256, recordOrdinal: record.Coordinate.RawRecordOrdinal,
		payloadHash: record.Coordinate.RawPayloadSHA256,
	}
}

func normalizationCoordinateFromMetadata(metadata normalize.Metadata) normalizationCoordinate {
	return normalizationCoordinate{
		source: metadata.SourceID, channel: metadata.ChannelID, epoch: metadata.EpochID,
		arrival: metadata.ArrivalOrdinal, message: metadata.MessageOrdinal, segmentHash: metadata.RawSegmentSHA256,
		recordOrdinal: metadata.RawRecordOrdinal, payloadHash: metadata.RawPayloadSHA256,
	}
}

type partitionKey struct {
	family dataset.Family
	source string
	date   string
	hour   string
}

func (k partitionKey) String() string {
	return string(k.family) + "/" + k.source + "/" + k.date + "/" + k.hour
}

type coverageKey struct {
	source     string
	channel    string
	instrument string
}

type coverageRange struct {
	start int64
	end   int64
	state string
}

type partitionRows struct {
	key         partitionKey
	normalized  []normalize.Row
	quarantines []normalize.SchemaQuarantineV1
	coverage    map[coverageKey]coverageRange
}

func (p *partitionRows) rowCount() int { return len(p.normalized) + len(p.quarantines) }

func addNormalizedPartition(partitions map[partitionKey]*partitionRows, row normalize.Row, maximum int) error {
	metadata := row.Common()
	family, err := datasetFamily(row.Kind)
	if err != nil {
		return err
	}
	date, hour := utcPartition(metadata.ReceivedTimeNS)
	key := partitionKey{family: family, source: metadata.SourceID, date: date, hour: hour}
	partition, err := ensurePartition(partitions, key, maximum)
	if err != nil {
		return err
	}
	partition.normalized = append(partition.normalized, row)
	partition.includeCoverage(coverageKey{source: metadata.SourceID, channel: metadata.ChannelID, instrument: metadata.InstrumentUID}, metadata.ReceivedTimeNS, "observed_normalized")
	return nil
}

func addQuarantinePartition(partitions map[partitionKey]*partitionRows, row normalize.SchemaQuarantineV1, maximum int) error {
	date, hour := utcPartition(row.ReceivedTimeNS)
	key := partitionKey{family: dataset.FamilySchemaQuarantine, source: row.SourceID, date: date, hour: hour}
	partition, err := ensurePartition(partitions, key, maximum)
	if err != nil {
		return err
	}
	partition.quarantines = append(partition.quarantines, row)
	partition.includeCoverage(coverageKey{source: row.SourceID, channel: row.ChannelID}, row.ReceivedTimeNS, "observed_quarantine")
	return nil
}

func ensurePartition(partitions map[partitionKey]*partitionRows, key partitionKey, maximum int) (*partitionRows, error) {
	partition := partitions[key]
	if partition != nil {
		return partition, nil
	}
	if len(partitions) == maximum {
		return nil, fmt.Errorf("%w: partition count exceeds %d", ErrPipelineBound, maximum)
	}
	partition = &partitionRows{key: key, coverage: make(map[coverageKey]coverageRange)}
	partitions[key] = partition
	return partition, nil
}

func (p *partitionRows) includeCoverage(key coverageKey, receivedNS int64, state string) {
	value, found := p.coverage[key]
	if !found {
		p.coverage[key] = coverageRange{start: receivedNS, end: receivedNS, state: state}
		return
	}
	value.start = min(value.start, receivedNS)
	value.end = max(value.end, receivedNS)
	p.coverage[key] = value
}

func comparePartitionRows(left, right *partitionRows) int {
	if order := cmp.Compare(left.key.family, right.key.family); order != 0 {
		return order
	}
	if order := cmp.Compare(left.key.source, right.key.source); order != 0 {
		return order
	}
	if order := cmp.Compare(left.key.date, right.key.date); order != 0 {
		return order
	}
	return cmp.Compare(left.key.hour, right.key.hour)
}

func datasetFamily(kind normalize.EventKind) (dataset.Family, error) {
	switch kind {
	case normalize.EventTrade:
		return dataset.FamilyTrade, nil
	case normalize.EventBookUpdate:
		return dataset.FamilyBookUpdate, nil
	case normalize.EventQuote:
		return dataset.FamilyQuote, nil
	case normalize.EventTicker:
		return dataset.FamilyTicker, nil
	default:
		return "", fmt.Errorf("%w: normalized event family %q has no v1 dataset builder", ErrInvalidPipeline, kind)
	}
}

func utcPartition(receivedNS int64) (string, string) {
	value := timeUnix(receivedNS)
	return value.Format("2006-01-02"), value.Format("15")
}

// timeUnix is a variable-free wrapper kept separate so partition identity has
// one visibly UTC conversion point.
func timeUnix(receivedNS int64) time.Time { return time.Unix(0, receivedNS).UTC() }

type builtPartition struct {
	result   dataset.BuildResult
	coverage map[coverageKey]coverageRange
}

func (e *Exporter) publishDataset(ctx context.Context, request ExportRequest, built builtPartition, segmentIDs []string, inputSetID [sha256.Size]byte) (DatasetReceipt, error) {
	manifestBytes, err := os.ReadFile(built.result.ManifestPath)
	if err != nil {
		return DatasetReceipt{}, fmt.Errorf("pipeline: read built manifest: %w", err)
	}
	if sha256.Sum256(manifestBytes) != built.result.ManifestHash {
		return DatasetReceipt{}, fmt.Errorf("%w: built manifest hash changed", ErrPublicationConflict)
	}
	manifest := built.result.Manifest
	if manifest.SourceID != request.SourceID ||
		manifest.DatasetPolicyID != hex.EncodeToString(e.config.DatasetPolicyID[:]) ||
		manifest.ReplayConfigID != hex.EncodeToString(e.config.ReplayConfigID[:]) ||
		manifest.InputManifestSetID != hex.EncodeToString(inputSetID[:]) {
		return DatasetReceipt{}, fmt.Errorf("%w: built manifest changed source, policy, replay, or input identity", ErrPublicationConflict)
	}
	datasetID, err := datasetIDFromBuildID(manifest.BuildID)
	if err != nil {
		return DatasetReceipt{}, err
	}
	parquetRelative, err := filepath.Rel(request.BuildRoot, built.result.ParquetPath)
	if err != nil || !filepath.IsLocal(parquetRelative) {
		return DatasetReceipt{}, fmt.Errorf("%w: built parquet escaped root", ErrPublicationConflict)
	}
	manifestRelative, err := filepath.Rel(request.BuildRoot, built.result.ManifestPath)
	if err != nil || !filepath.IsLocal(manifestRelative) {
		return DatasetReceipt{}, fmt.Errorf("%w: built manifest escaped root", ErrPublicationConflict)
	}
	parquetKey := path.Join(request.ObjectPrefix, filepath.ToSlash(parquetRelative))
	manifestKey := path.Join(request.ObjectPrefix, filepath.ToSlash(manifestRelative))
	logicalHash, err := decodeDatasetHash(manifest.LogicalSHA256)
	if err != nil {
		return DatasetReceipt{}, err
	}
	physicalHash, err := decodeDatasetHash(manifest.PhysicalSHA256)
	if err != nil {
		return DatasetReceipt{}, err
	}
	partitionKey := strings.TrimPrefix(filepath.ToSlash(filepath.Dir(manifest.ParquetFile)), "dataset-v1/")
	rangeEnd := manifest.LastReceivedTimeNS + 1
	if rangeEnd <= manifest.LastReceivedTimeNS {
		return DatasetReceipt{}, fmt.Errorf("%w: dataset receive-time range cannot be made half-open", ErrPublicationConflict)
	}
	coverage, err := datasetCoverage(datasetID, built.coverage)
	if err != nil {
		return DatasetReceipt{}, err
	}
	publication := catalog.DatasetPublication{
		DatasetID: datasetID, DatasetFamily: string(manifest.Family), DatasetVersion: manifest.DatasetVersion, SourceID: manifest.SourceID,
		SchemaName: manifest.SchemaName, SchemaVersion: manifest.SchemaVersion, ManifestVersion: manifest.ManifestVersion,
		PartitionKey: partitionKey, RangeStartNS: manifest.FirstReceivedTimeNS, RangeEndNS: rangeEnd,
		InputSegmentSetHash: inputSetID, CatalogSnapshotHash: [sha256.Size]byte(e.config.CatalogSnapshotID), MapperSetHash: [sha256.Size]byte(e.config.MapperSetID),
		LogicalHash: logicalHash, PhysicalHash: physicalHash, ParquetObjectKey: parquetKey, ManifestObjectKey: manifestKey,
		ParquetBytes: manifest.FileBytes, ManifestHash: built.result.ManifestHash, ManifestBytes: bytes.Clone(manifestBytes), State: catalog.DatasetVerified,
		InputSegmentIDs: slices.Clone(segmentIDs), Coverage: coverage,
	}
	receipt := DatasetReceipt{DatasetID: datasetID, Family: string(manifest.Family), SchemaName: manifest.SchemaName, SchemaVersion: manifest.SchemaVersion,
		PartitionKey: partitionKey, ManifestHash: built.result.ManifestHash, PhysicalHash: physicalHash,
		ParquetObject:  ObjectReceipt{Key: parquetKey, SHA256: physicalHash, Bytes: manifest.FileBytes},
		ManifestObject: ObjectReceipt{Key: manifestKey, SHA256: built.result.ManifestHash, Bytes: int64(len(manifestBytes))}}

	existing, found, err := e.catalog.FindDataset(ctx, datasetID)
	if err != nil {
		return receipt, fmt.Errorf("pipeline: find dataset publication: %w", err)
	}
	if found && !sameDatasetPublication(existing, publication) {
		return receipt, fmt.Errorf("%w: dataset %q is already bound to different immutable bytes or lineage", ErrPublicationConflict, datasetID)
	}
	if found && existing.State == catalog.DatasetCommitted {
		parquetRecovered, verifyErr := e.verifyLocalObject(ctx, built.result.ParquetPath, parquetKey, manifest.FileBytes, physicalHash)
		receipt.ParquetObject.Recovered = parquetRecovered
		if verifyErr != nil {
			return receipt, verifyErr
		}
		manifestRecovered, verifyErr := e.verifyLocalObject(ctx, built.result.ManifestPath, manifestKey, int64(len(manifestBytes)), built.result.ManifestHash)
		receipt.ManifestObject.Recovered = manifestRecovered
		if verifyErr != nil {
			return receipt, verifyErr
		}
		receipt.CatalogState = catalog.DatasetCommitted
		receipt.ReusedCommitted = true
		return receipt, nil
	}

	parquetRecovered, err := e.putAndVerify(ctx, built.result.ParquetPath, parquetKey, manifest.FileBytes, physicalHash)
	receipt.ParquetObject.Recovered = parquetRecovered
	if err != nil {
		return receipt, err
	}
	manifestRecovered, err := e.putAndVerify(ctx, built.result.ManifestPath, manifestKey, int64(len(manifestBytes)), built.result.ManifestHash)
	receipt.ManifestObject.Recovered = manifestRecovered
	if err != nil {
		return receipt, err
	}
	if err := e.catalog.RecordVerifiedDataset(ctx, publication); err != nil {
		return receipt, fmt.Errorf("pipeline: record verified dataset %q: %w", datasetID, err)
	}
	if err := e.catalog.CommitDataset(ctx, datasetID); err != nil {
		return receipt, fmt.Errorf("pipeline: commit verified dataset %q: %w", datasetID, err)
	}
	receipt.CatalogState = catalog.DatasetCommitted
	return receipt, nil
}

func (e *Exporter) putAndVerify(ctx context.Context, localPath, key string, size int64, hash [sha256.Size]byte) (bool, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return false, fmt.Errorf("pipeline: open immutable publication source: %w", err)
	}
	defer file.Close()
	object := objectstore.PutObject{Key: key, Body: file, Size: size, SHA256: hash}
	createErr := e.objects.PutIfAbsent(ctx, object)
	if errors.Is(createErr, objectstore.ErrConditionalCreateUnsupported) {
		if e.config.Reconciler == nil {
			return false, errors.Join(objectstore.ErrProviderDisqualified, createErr)
		}
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			return false, fmt.Errorf("pipeline: rewind for immutable object reconciler: %w", seekErr)
		}
		createErr = e.config.Reconciler.CreateImmutable(ctx, object)
	}
	if _, verifyErr := objectstore.VerifyObject(ctx, e.objects, key, size, hash, file, e.config.Verify); verifyErr != nil {
		if createErr != nil {
			return false, fmt.Errorf("pipeline: create and verify immutable object %q: %w", key, errors.Join(createErr, verifyErr))
		}
		return false, fmt.Errorf("pipeline: verify immutable object %q: %w", key, verifyErr)
	}
	return createErr != nil, nil
}

func (e *Exporter) verifyLocalObject(ctx context.Context, localPath, key string, size int64, hash [sha256.Size]byte) (bool, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return false, err
	}
	defer file.Close()
	if _, err := objectstore.VerifyObject(ctx, e.objects, key, size, hash, file, e.config.Verify); err != nil {
		return false, fmt.Errorf("pipeline: reconcile committed object %q: %w", key, err)
	}
	return true, nil
}

func datasetIDFromBuildID(buildID string) (string, error) {
	decoded, err := hex.DecodeString(buildID)
	if err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf("%w: dataset build ID is not SHA-256", ErrPublicationConflict)
	}
	var id [16]byte
	copy(id[:], decoded[:16])
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(id[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func decodeDatasetHash(value string) ([sha256.Size]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, fmt.Errorf("%w: dataset manifest SHA-256", ErrPublicationConflict)
	}
	var result [sha256.Size]byte
	copy(result[:], decoded)
	return result, nil
}

func datasetCoverage(datasetID string, ranges map[coverageKey]coverageRange) ([]catalog.DatasetCoverage, error) {
	keys := slices.SortedFunc(maps.Keys(ranges), func(left, right coverageKey) int {
		if order := cmp.Compare(left.source, right.source); order != 0 {
			return order
		}
		if order := cmp.Compare(left.channel, right.channel); order != 0 {
			return order
		}
		return cmp.Compare(left.instrument, right.instrument)
	})
	result := make([]catalog.DatasetCoverage, 0, len(keys))
	for _, key := range keys {
		value := ranges[key]
		end := value.end + 1
		if end <= value.end {
			return nil, fmt.Errorf("%w: coverage receive-time range cannot be made half-open", ErrPublicationConflict)
		}
		identity := deterministicUUID("pipeline-dataset-coverage-v1", datasetID, key.source, key.channel, key.instrument,
			fmt.Sprint(value.start), fmt.Sprint(end), value.state)
		result = append(result, catalog.DatasetCoverage{ID: identity,
			Tuple:               catalog.TupleProjection{SourceID: key.source, ChannelID: key.channel, InstrumentUID: key.instrument},
			StartReceivedTimeNS: value.start, EndReceivedTimeNS: end, State: value.state})
	}
	slices.SortFunc(result, func(left, right catalog.DatasetCoverage) int {
		return cmp.Compare(left.ID, right.ID)
	})
	return result, nil
}

func deterministicUUID(domain string, values ...string) string {
	hasher := sha256.New()
	writeDigestString(hasher, domain)
	for _, value := range values {
		writeDigestString(hasher, value)
	}
	digest := hasher.Sum(nil)
	var id [16]byte
	copy(id[:], digest[:16])
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(id[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func sameDatasetPublication(left, right catalog.DatasetPublication) bool {
	return left.DatasetID == right.DatasetID && left.DatasetFamily == right.DatasetFamily && left.DatasetVersion == right.DatasetVersion &&
		left.SourceID == right.SourceID && left.SchemaName == right.SchemaName && left.SchemaVersion == right.SchemaVersion &&
		left.ManifestVersion == right.ManifestVersion && left.PartitionKey == right.PartitionKey && left.RangeStartNS == right.RangeStartNS &&
		left.RangeEndNS == right.RangeEndNS && left.InputSegmentSetHash == right.InputSegmentSetHash &&
		left.CatalogSnapshotHash == right.CatalogSnapshotHash && left.MapperSetHash == right.MapperSetHash && left.LogicalHash == right.LogicalHash &&
		left.PhysicalHash == right.PhysicalHash && left.ParquetObjectKey == right.ParquetObjectKey && left.ManifestObjectKey == right.ManifestObjectKey &&
		left.ParquetBytes == right.ParquetBytes && left.ManifestHash == right.ManifestHash && bytes.Equal(left.ManifestBytes, right.ManifestBytes) &&
		slices.Equal(left.InputSegmentIDs, right.InputSegmentIDs) && slices.Equal(left.Coverage, right.Coverage)
}
