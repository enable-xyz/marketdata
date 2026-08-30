package collector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/spf13/pathologize"
)

const (
	journalDirectoryName      = "collector-journal-v1"
	journalRecordHeaderBytes  = 4 + sha256.Size
	maximumRetainedEpochs     = 256
	maximumJournalRecordBytes = segment.MaxRecordBytes
	publishedCleanupPrefix    = ".published-cleanup-"
	maxCleanupMarkerBytes     = 4 << 10
)

var (
	journalMagic     = [8]byte{'E', 'L', 'C', 'J', 'N', 'L', '1', '\n'}
	ErrConfiguration = errors.New("collector: invalid configuration")
	ErrRawSink       = errors.New("collector: durable raw sink failure")
)

// SegmentPublisher is the exact publication operation consumed by the raw
// sink. *objectstore.Publisher implements it.
type SegmentPublisher interface {
	Publish(context.Context, objectstore.PublishRequest) (objectstore.PublishResult, error)
}

// RawSinkConfig controls local durability and immutable segment rotation. Root
// must be an existing explicit absolute directory.
type RawSinkConfig struct {
	Root             string
	FrameBytes       int
	SegmentBytes     uint64
	SegmentMaxAge    time.Duration
	MaxBytes         int64
	WriterVersion    string
	CleanupPublished bool
}

// PublishedSegment is the committed immutable coordinate for one ready
// segment. It is sufficient to bind catalog raw evidence to an exact object.
type PublishedSegment struct {
	SegmentID     string
	ObjectKey     string
	ContentSHA256 [sha256.Size]byte
	ByteLength    int64
	Epoch         capture.StreamEpoch
	ChannelID     string
	OrdinalStart  uint64
	OrdinalEnd    uint64
}

// SinkStats is a bounded snapshot: all fields are counters or current gauges.
type SinkStats struct {
	RawEnvelopes          uint64
	RawPayloadBytes       uint64
	SegmentsReady         uint64
	SegmentsPublished     uint64
	RecoveredReady        uint64
	DuplicatePublications uint64
	RecoveryItems         uint64
	QuarantinedFiles      uint64
	JournalReplays        uint64
	JournalTruncations    uint64
	EpochsClosed          uint64
	Disconnects           uint64
	BlindIntervals        uint64
	ActiveEpochs          uint64
	RetainedEpochs        uint64
}

type sinkCounters struct {
	rawEnvelopes          atomic.Uint64
	rawPayloadBytes       atomic.Uint64
	segmentsReady         atomic.Uint64
	segmentsPublished     atomic.Uint64
	recoveredReady        atomic.Uint64
	duplicatePublications atomic.Uint64
	recoveryItems         atomic.Uint64
	quarantinedFiles      atomic.Uint64
	journalReplays        atomic.Uint64
	journalTruncations    atomic.Uint64
	epochsClosed          atomic.Uint64
	disconnects           atomic.Uint64
	blindIntervals        atomic.Uint64
	activeEpochs          atomic.Uint64
	retainedEpochs        atomic.Uint64
}

type epochIdentity struct {
	kind capture.EpochKind
	id   [16]byte
}

type epochKey struct {
	epochIdentity
	channel string
}

type epochState struct {
	key     epochKey
	spool   *segment.Spool
	writer  *segment.Writer
	journal *epochJournal

	mu      sync.Mutex
	last    uint64
	pending []segment.ReadySegment
}

// DurableRawSink journals every semantic envelope and fsyncs it before
// returning from WriteRaw. The segment writer can therefore batch frames
// without weakening capture.RawSink's durable-before-parse contract.
type DurableRawSink struct {
	config    RawSinkConfig
	publisher SegmentPublisher
	clock     capture.Clock
	gaps      GapRecorder

	mu                    sync.Mutex
	states                map[epochKey]*epochState
	activeEpochs          map[epochIdentity]*epochState
	published             map[epochIdentity][]PublishedSegment
	publishOrder          []epochIdentity
	pendingGaps           map[gapKey]string
	recoveredObservations map[gapKey]int64
	baseBytes             uint64
	reservedBytes         uint64
	counters              sinkCounters
}

// NewDurableRawSink recovers every Binance Spot tuple, republishes all ready
// segments, and replays fsynced journal records before any live writer can be
// created.
func NewDurableRawSink(ctx context.Context, config RawSinkConfig, publisher SegmentPublisher, clock capture.Clock) (*DurableRawSink, error) {
	if publisher == nil || clock == nil {
		return nil, fmt.Errorf("%w: publisher and clock are required", ErrConfiguration)
	}
	if err := validateRawSinkConfig(config); err != nil {
		return nil, err
	}
	sink := &DurableRawSink{
		config:                config,
		publisher:             publisher,
		clock:                 clock,
		states:                make(map[epochKey]*epochState),
		activeEpochs:          make(map[epochIdentity]*epochState),
		published:             make(map[epochIdentity][]PublishedSegment),
		pendingGaps:           make(map[gapKey]string),
		recoveredObservations: make(map[gapKey]int64),
	}
	startupBytes, err := directoryBytes(config.Root)
	if err != nil {
		return nil, fmt.Errorf("%w: measure startup spool: %v", ErrConfiguration, err)
	}
	if startupBytes > uint64(config.MaxBytes) {
		return nil, fmt.Errorf("%w: startup spool exceeds configured byte bound", ErrConfiguration)
	}
	if err := sink.recover(ctx); err != nil {
		return nil, err
	}
	baseBytes, err := directoryBytes(config.Root)
	if err != nil {
		return nil, fmt.Errorf("%w: measure recovered spool: %v", ErrConfiguration, err)
	}
	if baseBytes > uint64(config.MaxBytes) {
		return nil, fmt.Errorf("%w: recovered spool exceeds configured byte bound", ErrConfiguration)
	}
	sink.baseBytes = baseBytes
	return sink, nil
}

func validateRawSinkConfig(config RawSinkConfig) error {
	if config.Root == "" || !filepath.IsAbs(config.Root) || config.WriterVersion == "" ||
		len(config.WriterVersion) > segment.MaxRecorderVersionBytes || config.SegmentMaxAge <= 0 ||
		config.MaxBytes <= 0 {
		return fmt.Errorf("%w: absolute root, bounded writer version, positive segment age, and spool byte bound are required", ErrConfiguration)
	}
	if config.FrameBytes < 1<<20 || config.FrameBytes > segment.MaxSupportedFrameBytes ||
		config.FrameBytes&(config.FrameBytes-1) != 0 ||
		config.SegmentBytes < uint64(config.FrameBytes) || config.SegmentBytes > uint64(^uint(0)>>1) {
		return fmt.Errorf("%w: explicit frame and segment byte bounds are invalid", ErrConfiguration)
	}
	reservation, ok := epochReservation(config)
	if !ok || reservation > uint64(config.MaxBytes) {
		return fmt.Errorf("%w: spool byte bound cannot reserve one active epoch", ErrConfiguration)
	}
	info, err := os.Lstat(config.Root)
	if err != nil {
		return fmt.Errorf("%w: stat spool root: %v", ErrConfiguration, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: spool root must be a real directory", ErrConfiguration)
	}
	return nil
}

func (s *DurableRawSink) Stats() SinkStats {
	return SinkStats{
		RawEnvelopes:          s.counters.rawEnvelopes.Load(),
		RawPayloadBytes:       s.counters.rawPayloadBytes.Load(),
		SegmentsReady:         s.counters.segmentsReady.Load(),
		SegmentsPublished:     s.counters.segmentsPublished.Load(),
		RecoveredReady:        s.counters.recoveredReady.Load(),
		DuplicatePublications: s.counters.duplicatePublications.Load(),
		RecoveryItems:         s.counters.recoveryItems.Load(),
		QuarantinedFiles:      s.counters.quarantinedFiles.Load(),
		JournalReplays:        s.counters.journalReplays.Load(),
		JournalTruncations:    s.counters.journalTruncations.Load(),
		EpochsClosed:          s.counters.epochsClosed.Load(),
		Disconnects:           s.counters.disconnects.Load(),
		BlindIntervals:        s.counters.blindIntervals.Load(),
		ActiveEpochs:          s.counters.activeEpochs.Load(),
		RetainedEpochs:        s.counters.retainedEpochs.Load(),
	}
}

func (s *DurableRawSink) WriteRaw(ctx context.Context, envelope capture.EnvelopeV1) error {
	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("%w: validate envelope: %v", ErrRawSink, err)
	}
	if envelope.SourceID != binance.SpotSourceID {
		return fmt.Errorf("%w: source is not Binance Spot", ErrRawSink)
	}
	epoch, err := envelope.StreamEpoch()
	if err != nil {
		return fmt.Errorf("%w: stream epoch: %v", ErrRawSink, err)
	}
	if err := validateSpotTuple(envelope.ChannelOrEndpoint, epoch.Kind); err != nil {
		return err
	}
	record, err := envelope.ToSegment()
	if err != nil {
		return fmt.Errorf("%w: frame envelope: %v", ErrRawSink, err)
	}
	encoded, err := capture.MarshalEnvelopeV1(envelope)
	if err != nil {
		return fmt.Errorf("%w: journal envelope: %v", ErrRawSink, err)
	}
	state, err := s.stateFor(epochKey{epochIdentity: epochIdentity{kind: epoch.Kind, id: epoch.ID}, channel: envelope.ChannelOrEndpoint})
	if err != nil {
		return err
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.last != 0 && envelope.ArrivalOrdinal <= state.last {
		return fmt.Errorf("%w: non-increasing arrival ordinal", ErrRawSink)
	}
	if err := state.journal.append(encoded); err != nil {
		return fmt.Errorf("%w: fsync raw journal: %v", ErrRawSink, err)
	}
	s.counters.rawEnvelopes.Add(1)
	s.counters.rawPayloadBytes.Add(uint64(len(envelope.RawPayload)))
	ready, err := state.writer.Write(record)
	if err != nil {
		return fmt.Errorf("%w: write segment: %v", ErrRawSink, err)
	}
	state.last = envelope.ArrivalOrdinal
	if ready == nil {
		if err := s.resolveGapAfterWrite(ctx, envelope); err != nil {
			return err
		}
		return nil
	}
	state.pending = append(state.pending, *ready)
	s.counters.segmentsReady.Add(1)
	if err := s.publishPending(ctx, state, false); err != nil {
		return err
	}
	if err := state.journal.replace([][]byte{encoded}); err != nil {
		return fmt.Errorf("%w: compact raw journal: %v", ErrRawSink, err)
	}
	return s.resolveGapAfterWrite(ctx, envelope)
}

func (s *DurableRawSink) Commit(ctx context.Context, commit capture.EpochCommit) error {
	if commit.SourceID != binance.SpotSourceID {
		return fmt.Errorf("%w: commit source is not Binance Spot", ErrRawSink)
	}
	state, err := s.stateForCommit(commit.Epoch)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.last != commit.LastArrivalOrdinal {
		return fmt.Errorf("%w: commit ordinal %d does not match durable ordinal %d", ErrRawSink, commit.LastArrivalOrdinal, state.last)
	}
	return s.publishPending(ctx, state, false)
}

func (s *DurableRawSink) CloseEpoch(ctx context.Context, close capture.EpochClose) error {
	if close.Commit.SourceID != binance.SpotSourceID {
		return fmt.Errorf("%w: close source is not Binance Spot", ErrRawSink)
	}
	if err := close.Terminal.Validate(); err != nil {
		return fmt.Errorf("%w: terminal record: %v", ErrRawSink, err)
	}
	terminal := close.Terminal.Envelope
	epoch, err := terminal.StreamEpoch()
	if err != nil || epoch != close.Commit.Epoch {
		return fmt.Errorf("%w: close epoch differs from terminal", ErrRawSink)
	}
	state, err := s.stateForCommit(epoch)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if close.Commit.LastArrivalOrdinal != terminal.ArrivalOrdinal || terminal.ArrivalOrdinal != state.last+1 {
		return fmt.Errorf("%w: terminal ordinal does not immediately follow durable data", ErrRawSink)
	}
	if err := s.publishPending(ctx, state, false); err != nil {
		return err
	}
	record, err := terminal.ToSegment()
	if err != nil {
		return fmt.Errorf("%w: frame terminal: %v", ErrRawSink, err)
	}
	encoded, err := capture.MarshalEnvelopeV1(terminal)
	if err != nil {
		return fmt.Errorf("%w: journal terminal: %v", ErrRawSink, err)
	}
	if err := state.journal.append(encoded); err != nil {
		return fmt.Errorf("%w: fsync terminal journal: %v", ErrRawSink, err)
	}
	s.counters.rawEnvelopes.Add(1)
	ready, err := state.writer.Write(record)
	if err != nil {
		return fmt.Errorf("%w: write terminal segment: %v", ErrRawSink, err)
	}
	state.last = terminal.ArrivalOrdinal
	if ready != nil {
		state.pending = append(state.pending, *ready)
		s.counters.segmentsReady.Add(1)
		if err := s.publishPending(ctx, state, false); err != nil {
			return err
		}
		if err := state.journal.replace([][]byte{encoded}); err != nil {
			return fmt.Errorf("%w: compact terminal journal: %v", ErrRawSink, err)
		}
	}
	ready, err = state.writer.EndEpoch()
	if err != nil {
		return fmt.Errorf("%w: seal terminal segment: %v", ErrRawSink, err)
	}
	if ready != nil {
		state.pending = append(state.pending, *ready)
		s.counters.segmentsReady.Add(1)
	}
	if err := s.publishPending(ctx, state, false); err != nil {
		return err
	}
	if err := s.recordGap(ctx, state, close); err != nil {
		return err
	}
	if err := state.journal.removeAll(); err != nil {
		return fmt.Errorf("%w: remove committed journal: %v", ErrRawSink, err)
	}
	if s.config.CleanupPublished {
		if err := removeEmptyJournalDirectory(state.journal.dir); err != nil {
			return fmt.Errorf("%w: prune committed journal: %v", ErrRawSink, err)
		}
		if err := state.spool.RemoveIfEmpty(); err != nil {
			return fmt.Errorf("%w: prune committed local spool: %v", ErrRawSink, err)
		}
	}
	if terminal.ControlKind.Value == capture.ControlDisconnect {
		s.counters.disconnects.Add(1)
	}
	if close.BlindInterval != nil {
		s.counters.blindIntervals.Add(1)
	}
	s.counters.epochsClosed.Add(1)
	s.removeState(state)
	return nil
}

// Publication returns the committed segment containing ordinal. Callers should
// call ForgetEpoch after consuming any catalog coordinate.
func (s *DurableRawSink) Publication(epoch capture.StreamEpoch, ordinal uint64) (PublishedSegment, bool) {
	identity := epochIdentity{kind: epoch.Kind, id: epoch.ID}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, publication := range s.published[identity] {
		if ordinal >= publication.OrdinalStart && ordinal <= publication.OrdinalEnd {
			return publication, true
		}
	}
	return PublishedSegment{}, false
}

// ForgetEpoch releases retained publication coordinates for a completed epoch.
func (s *DurableRawSink) ForgetEpoch(epoch capture.StreamEpoch) {
	identity := epochIdentity{kind: epoch.Kind, id: epoch.ID}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.published[identity]; !ok {
		return
	}
	delete(s.published, identity)
	for i, retained := range s.publishOrder {
		if retained == identity {
			s.publishOrder = slices.Delete(s.publishOrder, i, i+1)
			break
		}
	}
	s.counters.retainedEpochs.Store(uint64(len(s.published)))
}

func (s *DurableRawSink) stateFor(key epochKey) (*epochState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.states[key]; state != nil {
		return state, nil
	}
	if active := s.activeEpochs[key.epochIdentity]; active != nil {
		return nil, fmt.Errorf("%w: one epoch identity is active on multiple channels", ErrRawSink)
	}
	reservation, ok := epochReservation(s.config)
	if !ok || s.baseBytes > uint64(s.config.MaxBytes)-s.reservedBytes ||
		reservation > uint64(s.config.MaxBytes)-s.baseBytes-s.reservedBytes {
		return nil, fmt.Errorf("%w: spool byte reservation exhausted", ErrRawSink)
	}
	spool, err := segment.OpenSpool(s.spoolConfig(key))
	if err != nil {
		return nil, fmt.Errorf("%w: open spool: %v", ErrRawSink, err)
	}
	writer, err := spool.NewWriter(s.writerOptions())
	if err != nil {
		return nil, fmt.Errorf("%w: open segment writer: %v", ErrRawSink, err)
	}
	journal, err := openEpochJournal(s.config.Root, key)
	if err != nil {
		return nil, fmt.Errorf("%w: open raw journal: %v", ErrRawSink, err)
	}
	state := &epochState{key: key, spool: spool, writer: writer, journal: journal}
	s.states[key] = state
	s.activeEpochs[key.epochIdentity] = state
	s.reservedBytes += reservation
	s.counters.activeEpochs.Store(uint64(len(s.activeEpochs)))
	return state, nil
}

func (s *DurableRawSink) stateForCommit(epoch capture.StreamEpoch) (*epochState, error) {
	if err := epoch.Validate(); err != nil {
		return nil, fmt.Errorf("%w: invalid commit epoch: %v", ErrRawSink, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.activeEpochs[epochIdentity{kind: epoch.Kind, id: epoch.ID}]
	if state == nil {
		return nil, fmt.Errorf("%w: commit epoch has no active writer", ErrRawSink)
	}
	return state, nil
}

func (s *DurableRawSink) removeState(state *epochState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, state.key)
	delete(s.activeEpochs, state.key.epochIdentity)
	reservation, ok := epochReservation(s.config)
	if ok && s.reservedBytes >= reservation {
		s.reservedBytes -= reservation
	}
	s.counters.activeEpochs.Store(uint64(len(s.activeEpochs)))
}

func (s *DurableRawSink) publishPending(ctx context.Context, state *epochState, recovered bool) error {
	for len(state.pending) > 0 {
		ready := state.pending[0]
		publication, duplicate, err := s.publishReady(ctx, ready, recovered)
		if err != nil {
			return err
		}
		if s.config.CleanupPublished {
			if err := removePublishedReady(s.config.Root, ready); err != nil {
				return fmt.Errorf("%w: remove committed local segment: %v", ErrRawSink, err)
			}
		}
		state.pending = slices.Delete(state.pending, 0, 1)
		s.retainPublication(publication)
		if duplicate {
			s.counters.duplicatePublications.Add(1)
		}
	}
	return nil
}

func (s *DurableRawSink) publishReady(ctx context.Context, ready segment.ReadySegment, recovered bool) (PublishedSegment, bool, error) {
	segmentID := deterministicSegmentID(ready)
	result, err := s.publisher.Publish(ctx, objectstore.PublishRequest{SegmentID: segmentID, Ready: ready})
	if err != nil {
		return PublishedSegment{}, false, fmt.Errorf("%w: publish ready segment: %v", ErrRawSink, err)
	}
	epochBytes, err := hex.DecodeString(ready.Manifest.EpochID)
	if err != nil || len(epochBytes) != 16 {
		return PublishedSegment{}, false, fmt.Errorf("%w: ready epoch identity is invalid", ErrRawSink)
	}
	var epochID [16]byte
	copy(epochID[:], epochBytes)
	kind := capture.EpochPollCycle
	if ready.Manifest.EpochKind == segment.EpochConnection {
		kind = capture.EpochConnection
	}
	publication := PublishedSegment{
		SegmentID:     segmentID,
		ObjectKey:     ready.Manifest.ObjectKey,
		ContentSHA256: ready.Manifest.Segment.CompressedSHA256,
		ByteLength:    int64(ready.Manifest.Segment.CompressedBytes),
		Epoch:         capture.StreamEpoch{Kind: kind, ID: epochID},
		ChannelID:     ready.Manifest.ChannelID,
		OrdinalStart:  ready.Manifest.Segment.FirstOrdinal,
		OrdinalEnd:    ready.Manifest.Segment.LastOrdinal,
	}
	s.counters.segmentsPublished.Add(1)
	if recovered {
		s.counters.recoveredReady.Add(1)
	}
	return publication, result.Recovered, nil
}

func (s *DurableRawSink) retainPublication(publication PublishedSegment) {
	identity := epochIdentity{kind: publication.Epoch.Kind, id: publication.Epoch.ID}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.published[identity]; !exists {
		if len(s.publishOrder) == maximumRetainedEpochs {
			evicted := s.publishOrder[0]
			delete(s.published, evicted)
			s.publishOrder = slices.Delete(s.publishOrder, 0, 1)
		}
		s.publishOrder = append(s.publishOrder, identity)
	}
	for _, retained := range s.published[identity] {
		if retained.SegmentID == publication.SegmentID {
			return
		}
	}
	s.published[identity] = append(s.published[identity], publication)
	s.counters.retainedEpochs.Store(uint64(len(s.published)))
}

func deterministicSegmentID(ready segment.ReadySegment) string {
	hasher := sha256.New()
	hasher.Write([]byte("collector.raw-segment.v1\x00"))
	hasher.Write(ready.ManifestSHA256[:])
	hasher.Write(ready.Manifest.Segment.CompressedSHA256[:])
	sum := hasher.Sum(nil)
	var id [16]byte
	copy(id[:], sum)
	id[6] = id[6]&0x0f | 0x50
	id[8] = id[8]&0x3f | 0x80
	encoded := hex.EncodeToString(id[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func (s *DurableRawSink) spoolConfig(key epochKey) segment.SpoolConfig {
	kind := segment.EpochPoll
	if key.kind == capture.EpochConnection {
		kind = segment.EpochConnection
	}
	return segment.SpoolConfig{Root: s.config.Root, SourceID: binance.SpotSourceID, ChannelID: key.channel, EpochKind: kind, EpochID: key.id}
}

func (s *DurableRawSink) writerOptions() segment.WriterOptions {
	return segment.WriterOptions{
		FrameBytes:    s.config.FrameBytes,
		SegmentBytes:  s.config.SegmentBytes,
		MaxAge:        s.config.SegmentMaxAge,
		WriterVersion: s.config.WriterVersion,
		Now: func() time.Time {
			return time.Unix(0, s.clock.Read().WallTimeNS).UTC()
		},
	}
}

func validateSpotTuple(channel string, kind capture.EpochKind) error {
	expected, ok := spotChannelEpochKind(channel)
	if !ok || expected != kind {
		return fmt.Errorf("%w: unsupported Binance Spot channel/epoch tuple", ErrRawSink)
	}
	return nil
}

func spotChannelEpochKind(channel string) (capture.EpochKind, bool) {
	switch channel {
	case binance.SpotRawChannel:
		return capture.EpochConnection, true
	case binance.SpotDepthChannel, binance.SpotExchangeInfoChannel:
		return capture.EpochPollCycle, true
	default:
		return 0, false
	}
}

type recoveredTuple struct {
	key      epochKey
	spool    *segment.Spool
	ready    []segment.ReadySegment
	journals []string
}

func (s *DurableRawSink) recover(ctx context.Context) error {
	tuples, err := s.discoverTuples()
	if err != nil {
		return err
	}
	for i := range tuples {
		if s.config.CleanupPublished {
			if err := finishPublishedCleanup(readyDirectory(s.config.Root, tuples[i].key)); err != nil {
				return fmt.Errorf("%w: finish interrupted publication cleanup: %v", ErrRawSink, err)
			}
		}
		report, err := tuples[i].spool.Recover(segment.RecoveryOptions{FrameBytes: s.config.FrameBytes, WriterVersion: s.config.WriterVersion})
		if err != nil {
			return fmt.Errorf("%w: recover spool: %v", ErrRawSink, err)
		}
		s.counters.recoveryItems.Add(uint64(len(report.Items)))
		for _, item := range report.Items {
			s.counters.quarantinedFiles.Add(uint64(len(item.Quarantined)))
		}
		ready, err := tuples[i].spool.Ready()
		if err != nil {
			return fmt.Errorf("%w: list recovered ready segments: %v", ErrRawSink, err)
		}
		tuples[i].ready = ready
		s.counters.segmentsReady.Add(uint64(len(ready)))
		for _, item := range ready {
			publication, duplicate, err := s.publishReady(ctx, item, true)
			if err != nil {
				return err
			}
			s.retainPublication(publication)
			if duplicate {
				s.counters.duplicatePublications.Add(1)
			}
			if s.config.CleanupPublished {
				if err := removePublishedReady(s.config.Root, item); err != nil {
					return fmt.Errorf("%w: remove recovered committed segment: %v", ErrRawSink, err)
				}
			}
		}
	}
	// This second phase is deliberate: every pre-existing ready segment across
	// every tuple is published before recovery opens any new segment writer.
	for i := range tuples {
		if err := s.replayTupleJournal(ctx, tuples[i]); err != nil {
			return err
		}
		if s.config.CleanupPublished {
			if err := tuples[i].spool.RemoveIfEmpty(); err != nil {
				return fmt.Errorf("%w: prune recovered committed spool: %v", ErrRawSink, err)
			}
		}
	}
	return nil
}

func (s *DurableRawSink) discoverTuples() ([]recoveredTuple, error) {
	sourceDir := pathologize.Join(s.config.Root, "elmd-segment-v1", "source="+binance.SpotSourceID)
	entries, err := os.ReadDir(sourceDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: scan Spot source spool: %v", ErrRawSink, err)
	}

	var tuples []recoveredTuple
	for _, channelEntry := range entries {
		if !channelEntry.IsDir() || !strings.HasPrefix(channelEntry.Name(), "channel=") {
			return nil, fmt.Errorf("%w: unexpected Spot spool source entry %q", ErrRawSink, channelEntry.Name())
		}
		channel := strings.TrimPrefix(channelEntry.Name(), "channel=")
		expectedKind, ok := spotChannelEpochKind(channel)
		if !ok {
			return nil, fmt.Errorf("%w: unsupported Spot spool channel %q", ErrRawSink, channel)
		}
		channelDir := pathologize.Join(sourceDir, channelEntry.Name())
		epochEntries, err := os.ReadDir(channelDir)
		if err != nil {
			return nil, fmt.Errorf("%w: scan channel spool: %v", ErrRawSink, err)
		}
		for _, epochEntry := range epochEntries {
			if !epochEntry.IsDir() {
				return nil, fmt.Errorf("%w: unexpected channel entry %q", ErrRawSink, epochEntry.Name())
			}
			epoch, err := parseEpochDirectory(epochEntry.Name())
			if err != nil || epoch.Kind != expectedKind {
				return nil, fmt.Errorf("%w: invalid epoch directory %q", ErrRawSink, epochEntry.Name())
			}
			key := epochKey{epochIdentity: epochIdentity{kind: epoch.Kind, id: epoch.ID}, channel: channel}
			spool, err := segment.OpenSpool(s.spoolConfig(key))
			if err != nil {
				return nil, fmt.Errorf("%w: open recovered spool: %v", ErrRawSink, err)
			}
			journalDir := journalDirectory(s.config.Root, key)
			journals, err := listJournalFiles(journalDir)
			if err != nil {
				return nil, fmt.Errorf("%w: scan raw journals: %v", ErrRawSink, err)
			}
			tuples = append(tuples, recoveredTuple{key: key, spool: spool, journals: journals})
		}
	}
	slices.SortFunc(tuples, func(a, b recoveredTuple) int {
		if compared := strings.Compare(a.key.channel, b.key.channel); compared != 0 {
			return compared
		}
		return bytes.Compare(a.key.id[:], b.key.id[:])
	})
	return tuples, nil
}

func parseEpochDirectory(name string) (capture.StreamEpoch, error) {
	var kind capture.EpochKind
	var encoded string
	switch {
	case strings.HasPrefix(name, "epoch=connection-"):
		kind = capture.EpochConnection
		encoded = strings.TrimPrefix(name, "epoch=connection-")
	case strings.HasPrefix(name, "epoch=poll-"):
		kind = capture.EpochPollCycle
		encoded = strings.TrimPrefix(name, "epoch=poll-")
	default:
		return capture.StreamEpoch{}, ErrConfiguration
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != encoded {
		return capture.StreamEpoch{}, ErrConfiguration
	}
	var id [16]byte
	copy(id[:], decoded)
	epoch := capture.StreamEpoch{Kind: kind, ID: id}
	return epoch, epoch.Validate()
}

func (s *DurableRawSink) replayTupleJournal(ctx context.Context, tuple recoveredTuple) error {
	if len(tuple.journals) == 0 {
		if s.config.CleanupPublished {
			return removeEmptyJournalDirectory(journalDirectory(s.config.Root, tuple.key))
		}
		return nil
	}
	byOrdinal := make(map[uint64][]byte)
	truncated := false
	for _, path := range tuple.journals {
		records, partial, err := readJournal(path)
		if err != nil {
			return fmt.Errorf("%w: read journal %s: %v", ErrRawSink, filepath.Base(path), err)
		}
		truncated = truncated || partial
		for _, encoded := range records {
			envelope, err := capture.UnmarshalEnvelopeV1(encoded)
			if err != nil {
				return fmt.Errorf("%w: decode journal envelope: %v", ErrRawSink, err)
			}
			epoch, _ := envelope.StreamEpoch()
			if envelope.SourceID != binance.SpotSourceID || envelope.ChannelOrEndpoint != tuple.key.channel || epoch.Kind != tuple.key.kind || epoch.ID != tuple.key.id {
				return fmt.Errorf("%w: journal record crosses tuple boundary", ErrRawSink)
			}
			if prior, exists := byOrdinal[envelope.ArrivalOrdinal]; exists && !bytes.Equal(prior, encoded) {
				return fmt.Errorf("%w: conflicting journal record at ordinal %d", ErrRawSink, envelope.ArrivalOrdinal)
			}
			byOrdinal[envelope.ArrivalOrdinal] = encoded
		}
	}
	for _, encoded := range byOrdinal {
		envelope, _ := capture.UnmarshalEnvelopeV1(encoded)
		s.retainRecoveredObservation(envelope)
	}
	if truncated {
		s.counters.journalTruncations.Add(1)
	}
	ordinals := make([]uint64, 0, len(byOrdinal))
	for ordinal := range byOrdinal {
		if !readyCovers(tuple.ready, ordinal) {
			ordinals = append(ordinals, ordinal)
		}
	}
	slices.Sort(ordinals)
	if len(ordinals) == 0 {
		if err := removeJournalFiles(filepath.Dir(tuple.journals[0]), tuple.journals); err != nil {
			return err
		}
		if s.config.CleanupPublished {
			return removeEmptyJournalDirectory(filepath.Dir(tuple.journals[0]))
		}
		return nil
	}
	writer, err := tuple.spool.NewWriter(s.writerOptions())
	if err != nil {
		return fmt.Errorf("%w: create journal recovery writer: %v", ErrRawSink, err)
	}
	var previous uint64
	for _, ordinal := range ordinals {
		if previous != 0 && ordinal != previous+1 {
			return fmt.Errorf("%w: journal recovery has an ordinal gap", ErrRawSink)
		}
		envelope, _ := capture.UnmarshalEnvelopeV1(byOrdinal[ordinal])
		record, _ := envelope.ToSegment()
		ready, err := writer.Write(record)
		if err != nil {
			return fmt.Errorf("%w: replay journal record: %v", ErrRawSink, err)
		}
		s.counters.journalReplays.Add(1)
		if ready != nil {
			s.counters.segmentsReady.Add(1)
			publication, duplicate, err := s.publishReady(ctx, *ready, false)
			if err != nil {
				return err
			}
			s.retainPublication(publication)
			if duplicate {
				s.counters.duplicatePublications.Add(1)
			}
			if s.config.CleanupPublished {
				if err := removePublishedReady(s.config.Root, *ready); err != nil {
					return fmt.Errorf("%w: remove replayed committed segment: %v", ErrRawSink, err)
				}
			}
		}
		previous = ordinal
	}
	ready, err := writer.Shutdown()
	if err != nil {
		return fmt.Errorf("%w: seal replayed journal: %v", ErrRawSink, err)
	}
	if ready != nil {
		s.counters.segmentsReady.Add(1)
		publication, duplicate, err := s.publishReady(ctx, *ready, false)
		if err != nil {
			return err
		}
		s.retainPublication(publication)
		if duplicate {
			s.counters.duplicatePublications.Add(1)
		}
		if s.config.CleanupPublished {
			if err := removePublishedReady(s.config.Root, *ready); err != nil {
				return fmt.Errorf("%w: remove replayed committed segment: %v", ErrRawSink, err)
			}
		}
	}
	if err := removeJournalFiles(filepath.Dir(tuple.journals[0]), tuple.journals); err != nil {
		return err
	}
	if s.config.CleanupPublished {
		return removeEmptyJournalDirectory(filepath.Dir(tuple.journals[0]))
	}
	return nil
}

func readyCovers(ready []segment.ReadySegment, ordinal uint64) bool {
	for _, item := range ready {
		if ordinal >= item.Manifest.Segment.FirstOrdinal && ordinal <= item.Manifest.Segment.LastOrdinal {
			return true
		}
	}
	return false
}

type epochJournal struct {
	dir        string
	generation uint64
	path       string
	file       *os.File
}

func openEpochJournal(root string, key epochKey) (*epochJournal, error) {
	dir := journalDirectory(root, key)
	if err := ensurePrivateDirectory(root, dir); err != nil {
		return nil, err
	}
	files, err := listJournalFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(files) != 0 {
		return nil, errors.New("existing journals must be recovered before opening a live writer")
	}
	journal := &epochJournal{dir: dir}
	if err := journal.replace(nil); err != nil {
		return nil, err
	}
	return journal, nil
}

func journalDirectory(root string, key epochKey) string {
	kind := "poll"
	if key.kind == capture.EpochConnection {
		kind = "connection"
	}
	epoch := "epoch=" + kind + "-" + hex.EncodeToString(key.id[:])
	tuple := pathologize.Join(root, "elmd-segment-v1", "source="+binance.SpotSourceID, "channel="+key.channel, epoch)
	return pathologize.Join(tuple, journalDirectoryName)
}

func (j *epochJournal) append(encoded []byte) error {
	if j.file == nil || len(encoded) == 0 || len(encoded) > maximumJournalRecordBytes {
		return errors.New("journal is not writable or record exceeds bounds")
	}
	var header [journalRecordHeaderBytes]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(encoded)))
	hash := sha256.Sum256(encoded)
	copy(header[4:], hash[:])
	if err := writeFull(j.file, header[:]); err != nil {
		return err
	}
	if err := writeFull(j.file, encoded); err != nil {
		return err
	}
	return j.file.Sync()
}

func (j *epochJournal) replace(records [][]byte) error {
	next := j.generation + 1
	name := fmt.Sprintf("journal-%020d.bin", next)
	temp := filepath.Join(j.dir, "."+name+".tmp")
	final := filepath.Join(j.dir, name)
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	failed := true
	defer func() {
		file.Close()
		if failed {
			_ = os.Remove(temp)
		}
	}()
	if err := writeFull(file, journalMagic[:]); err != nil {
		return err
	}
	for _, encoded := range records {
		var header [journalRecordHeaderBytes]byte
		binary.BigEndian.PutUint32(header[:4], uint32(len(encoded)))
		hash := sha256.Sum256(encoded)
		copy(header[4:], hash[:])
		if err := writeFull(file, header[:]); err != nil {
			return err
		}
		if err := writeFull(file, encoded); err != nil {
			return err
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		return err
	}
	if err := syncDirectory(j.dir); err != nil {
		return err
	}
	oldPath := j.path
	oldFile := j.file
	j.generation, j.path = next, final
	j.file, err = os.OpenFile(final, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	failed = false
	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			return err
		}
	}
	if oldPath != "" {
		if err := os.Remove(oldPath); err != nil {
			return err
		}
		return syncDirectory(j.dir)
	}
	return nil
}

func (j *epochJournal) removeAll() error {
	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return err
		}
		j.file = nil
	}
	files, err := listJournalFiles(j.dir)
	if err != nil {
		return err
	}
	return removeJournalFiles(j.dir, files)
}

func listJournalFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "journal-") || !strings.HasSuffix(entry.Name(), ".bin") {
			return nil, fmt.Errorf("unexpected journal entry %q", entry.Name())
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	slices.Sort(files)
	return files, nil
}

func readJournal(path string) ([][]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	var magic [len(journalMagic)]byte
	if _, err := io.ReadFull(file, magic[:]); err != nil || magic != journalMagic {
		return nil, false, errors.New("invalid journal header")
	}
	var records [][]byte
	for {
		var header [journalRecordHeaderBytes]byte
		_, err := io.ReadFull(file, header[:])
		if errors.Is(err, io.EOF) {
			return records, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return records, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		length := binary.BigEndian.Uint32(header[:4])
		if length == 0 || length > maximumJournalRecordBytes {
			return nil, false, errors.New("journal record length exceeds bounds")
		}
		encoded := make([]byte, length)
		if _, err := io.ReadFull(file, encoded); errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return records, true, nil
		} else if err != nil {
			return nil, false, err
		}
		hash := sha256.Sum256(encoded)
		if !bytes.Equal(hash[:], header[4:]) {
			return nil, false, errors.New("journal record digest mismatch")
		}
		records = append(records, encoded)
	}
}

func removeJournalFiles(dir string, files []string) error {
	for _, path := range files {
		if filepath.Dir(path) != dir {
			return errors.New("journal removal escaped directory")
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(dir)
}

func ensurePrivateDirectory(root, target string) error {
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("journal directory escaped spool root")
	}
	current := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		if err := os.Mkdir(current, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("journal namespace component is not a real directory")
		}
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

var _ capture.RawSink = (*DurableRawSink)(nil)

func epochReservation(config RawSinkConfig) (uint64, bool) {
	segmentBytes := config.SegmentBytes
	frameBytes := uint64(config.FrameBytes)
	maximum := ^uint64(0)
	if segmentBytes > maximum/2 || frameBytes > maximum/2 ||
		segmentBytes*2 > maximum-frameBytes*2 {
		return 0, false
	}
	return segmentBytes*2 + frameBytes*2, true
}

func directoryBytes(root string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is not allowed in spool: %s", path)
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("non-regular entry is not allowed in spool: %s", path)
		}
		size := uint64(info.Size())
		if total > ^uint64(0)-size {
			return errors.New("spool byte count overflow")
		}
		total += size
		return nil
	})
	return total, err
}

type publishedCleanupMarker struct {
	Manifest string `json:"manifest"`
	Segment  string `json:"segment"`
}

func removePublishedReady(root string, ready segment.ReadySegment) error {
	directory := filepath.Dir(ready.ManifestPath)
	relative, err := filepath.Rel(root, directory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) ||
		filepath.Base(directory) != "ready" || directory != filepath.Dir(ready.SegmentPath) ||
		filepath.Base(ready.SegmentPath) != ready.Manifest.SegmentFile {
		return errors.New("committed spool paths do not name one rooted ready pair")
	}
	marker := publishedCleanupMarker{
		Manifest: filepath.Base(ready.ManifestPath),
		Segment:  filepath.Base(ready.SegmentPath),
	}
	if err := validatePublishedCleanupMarker(marker); err != nil {
		return err
	}
	for _, path := range []string{ready.ManifestPath, ready.SegmentPath} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("committed spool object is not a regular file")
		}
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	markerPath := filepath.Join(directory, fmt.Sprintf("%s%x.json", publishedCleanupPrefix, hash))
	tempPath := markerPath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	failed := true
	defer func() {
		file.Close()
		if failed {
			_ = os.Remove(tempPath)
		}
	}()
	if err := writeFull(file, data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, markerPath); err != nil {
		return err
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	failed = false
	if err := removePublishedCleanupTargets(directory, marker); err != nil {
		return err
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return syncDirectory(directory)
}

func finishPublishedCleanup(directory string) error {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, publishedCleanupPrefix) && strings.HasSuffix(name, ".json.tmp") {
			if entry.IsDir() {
				return errors.New("publication cleanup temporary marker is not a regular file")
			}
			if err := os.Remove(filepath.Join(directory, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	entries, err = os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, publishedCleanupPrefix) || !strings.HasSuffix(name, ".json") {
			continue
		}
		markerPath := filepath.Join(directory, name)
		marker, err := readPublishedCleanupMarker(markerPath)
		if err != nil {
			return err
		}
		if err := removePublishedCleanupTargets(directory, marker); err != nil {
			return err
		}
		if err := syncDirectory(directory); err != nil {
			return err
		}
		if err := os.Remove(markerPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

func readPublishedCleanupMarker(path string) (publishedCleanupMarker, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return publishedCleanupMarker{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxCleanupMarkerBytes {
		return publishedCleanupMarker{}, errors.New("publication cleanup marker is invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return publishedCleanupMarker{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxCleanupMarkerBytes+1))
	if err != nil || int64(len(data)) != info.Size() {
		return publishedCleanupMarker{}, errors.New("read publication cleanup marker")
	}
	var marker publishedCleanupMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return publishedCleanupMarker{}, err
	}
	canonical, _ := json.Marshal(marker)
	if !bytes.Equal(data, canonical) {
		return publishedCleanupMarker{}, errors.New("publication cleanup marker is not canonical")
	}
	if err := validatePublishedCleanupMarker(marker); err != nil {
		return publishedCleanupMarker{}, err
	}
	return marker, nil
}

func validatePublishedCleanupMarker(marker publishedCleanupMarker) error {
	if marker.Manifest == "" || marker.Segment == "" ||
		filepath.Base(marker.Manifest) != marker.Manifest || filepath.Base(marker.Segment) != marker.Segment ||
		!strings.HasPrefix(marker.Manifest, "manifest=") || !strings.HasSuffix(marker.Manifest, ".ready.json") ||
		!strings.HasPrefix(marker.Segment, "segment=") || !strings.HasSuffix(marker.Segment, ".emseg.zst") {
		return errors.New("publication cleanup marker paths are invalid")
	}
	return nil
}

func removePublishedCleanupTargets(directory string, marker publishedCleanupMarker) error {
	for _, name := range []string{marker.Manifest, marker.Segment} {
		path := filepath.Join(directory, name)
		info, err := os.Lstat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("publication cleanup target is not a regular file")
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

func readyDirectory(root string, key epochKey) string {
	return pathologize.Join(filepath.Dir(journalDirectory(root, key)), "ready")
}

func removeEmptyJournalDirectory(directory string) error {
	if err := os.Remove(directory); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove empty journal directory: %w", err)
	}
	return syncDirectory(filepath.Dir(directory))
}
