package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/fileflow"
	"github.com/spf13/pathologize"
)

const (
	SpoolManifestVersion uint16 = 1
	maxManifestBytes            = 4 << 20
	maxStoredFrameBytes         = MaxSupportedFrameBytes + (1 << 20)
)

var (
	ErrSpoolRoot  = errors.New("segment: invalid spool root")
	ErrWriterDone = errors.New("segment: spool writer is not active")
	ErrNotReady   = errors.New("segment: spool pair is not committed-ready")
)

type EpochKind string

const (
	EpochConnection EpochKind = "connection"
	EpochPoll       EpochKind = "poll"
)

type FaultPoint string

const (
	FaultTemporaryCreated              FaultPoint = "temporary_created"
	FaultBeforeFrameWrite              FaultPoint = "before_frame_write"
	FaultAfterFramePrefix              FaultPoint = "after_frame_prefix"
	FaultAfterFrameWrite               FaultPoint = "after_frame_write"
	FaultBeforeFrameClose              FaultPoint = "before_frame_close"
	FaultAfterFrameClose               FaultPoint = "after_frame_close"
	FaultBeforeSegmentSync             FaultPoint = "before_segment_sync"
	FaultAfterSegmentSync              FaultPoint = "after_segment_sync"
	FaultBeforeSegmentClose            FaultPoint = "before_segment_close"
	FaultAfterSegmentClose             FaultPoint = "after_segment_close"
	FaultBeforeClosedRename            FaultPoint = "before_closed_rename"
	FaultAfterClosedRename             FaultPoint = "after_closed_rename"
	FaultBeforeSegmentExpose           FaultPoint = "before_segment_expose"
	FaultAfterSegmentExpose            FaultPoint = "after_segment_expose"
	FaultBeforeManifestWrite           FaultPoint = "before_manifest_write"
	FaultAfterManifestWrite            FaultPoint = "after_manifest_write"
	FaultBeforeManifestSync            FaultPoint = "before_manifest_sync"
	FaultAfterManifestSync             FaultPoint = "after_manifest_sync"
	FaultBeforeManifestClose           FaultPoint = "before_manifest_close"
	FaultAfterManifestClose            FaultPoint = "after_manifest_close"
	FaultBeforeManifestExpose          FaultPoint = "before_manifest_expose"
	FaultAfterManifestExpose           FaultPoint = "after_manifest_expose"
	FaultBeforeClosedDirectorySync     FaultPoint = "before_closed_directory_sync"
	FaultAfterClosedDirectorySync      FaultPoint = "after_closed_directory_sync"
	FaultBeforeSegmentDirectorySync    FaultPoint = "before_segment_directory_sync"
	FaultAfterSegmentDirectorySync     FaultPoint = "after_segment_directory_sync"
	FaultBeforeManifestDirectorySync   FaultPoint = "before_manifest_directory_sync"
	FaultAfterManifestDirectorySync    FaultPoint = "after_manifest_directory_sync"
	FaultBeforeReadyVerify             FaultPoint = "before_ready_verify"
	FaultBeforeQuarantine              FaultPoint = "before_quarantine"
	FaultAfterQuarantine               FaultPoint = "after_quarantine"
	FaultBeforeQuarantineSourceSync    FaultPoint = "before_quarantine_source_sync"
	FaultAfterQuarantineSourceSync     FaultPoint = "after_quarantine_source_sync"
	FaultBeforeQuarantineDirectorySync FaultPoint = "before_quarantine_directory_sync"
	FaultAfterQuarantineDirectorySync  FaultPoint = "after_quarantine_directory_sync"
)

type FaultInjector func(FaultPoint) error

type SpoolConfig struct {
	Root      string
	SourceID  string
	ChannelID string
	EpochKind EpochKind
	EpochID   [16]byte
}

type WriterOptions struct {
	FrameBytes    int
	SegmentBytes  uint64
	MaxAge        time.Duration
	WriterVersion string
	Now           func() time.Time
	Fault         FaultInjector
}

type RecoveryOptions struct {
	FrameBytes    int
	WriterVersion string
	Fault         FaultInjector
}

type ReadyManifest struct {
	ManifestVersion    uint16         `json:"manifest_version"`
	SourceID           string         `json:"source_id"`
	ChannelID          string         `json:"channel_id"`
	EpochKind          EpochKind      `json:"epoch_kind"`
	EpochID            string         `json:"epoch_id"`
	WriterVersion      string         `json:"writer_version"`
	RotationReason     RotationReason `json:"rotation_reason"`
	SegmentFile        string         `json:"segment_file"`
	ObjectKey          string         `json:"object_key"`
	SchemaFingerprints [][32]byte     `json:"schema_fingerprints"`
	Segment            Manifest       `json:"segment"`
}

type ReadySegment struct {
	SegmentPath    string
	ManifestPath   string
	ManifestBytes  uint64
	ManifestSHA256 [32]byte
	Manifest       ReadyManifest
}

type RecoveryState string

const (
	RecoveryOpen         RecoveryState = "open"
	RecoveryTruncated    RecoveryState = "truncated"
	RecoveryClosed       RecoveryState = "closed"
	RecoveryManifestOnly RecoveryState = "manifest_only"
	RecoverySegmentOnly  RecoveryState = "segment_only"
	RecoveryCorrupt      RecoveryState = "corrupt"
	RecoveryConflicting  RecoveryState = "conflicting"
	RecoveryReady        RecoveryState = "ready"
)

type RecoveryItem struct {
	State          RecoveryState
	Paths          []string
	CompleteFrames uint32
	CompleteBytes  uint64
	Quarantined    []string
	Ready          *ReadySegment
	Err            error
}

type RecoveryReport struct {
	Items []RecoveryItem
}

type Spool struct {
	config        SpoolConfig
	tupleDir      string
	openDir       string
	readyDir      string
	quarantineDir string
	flow          fileflow.Flow
}

func OpenSpool(config SpoolConfig) (*Spool, error) {
	if err := validateSpoolConfig(config); err != nil {
		return nil, err
	}
	rootInfo, err := os.Lstat(config.Root)
	if err != nil {
		return nil, fmt.Errorf("%w: stat explicit root: %v", ErrSpoolRoot, err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: explicit root must be a real directory", ErrSpoolRoot)
	}

	namespace := pathologize.Join(config.Root, "elmd-segment-v1")
	epoch := string(config.EpochKind) + "-" + hex.EncodeToString(config.EpochID[:])
	tuple := pathologize.Join(namespace, "source="+config.SourceID, "channel="+config.ChannelID, "epoch="+epoch)
	spool := &Spool{
		config:        config,
		tupleDir:      tuple,
		openDir:       pathologize.Join(tuple, "open"),
		readyDir:      pathologize.Join(tuple, "ready"),
		quarantineDir: pathologize.Join(tuple, "quarantine"),
		flow: fileflow.Flow{
			FindAvailableName: fileflow.FindAvailableNameInc,
			DirMode:           0o700,
			NoCreateDirs:      true,
		},
	}
	for _, dir := range []string{namespace, tuple, spool.openDir, spool.readyDir, spool.quarantineDir} {
		if err := ensurePrivateDirectory(config.Root, dir); err != nil {
			return nil, err
		}
	}
	return spool, nil
}

func validateSpoolConfig(config SpoolConfig) error {
	if config.Root == "" || !filepath.IsAbs(config.Root) {
		return fmt.Errorf("%w: root must be explicit and absolute", ErrSpoolRoot)
	}
	if err := validateCatalogPathID("source", config.SourceID, MaxSourceIDBytes); err != nil {
		return err
	}
	if err := validateCatalogPathID("channel", config.ChannelID, MaxContractIDBytes); err != nil {
		return err
	}
	if config.EpochKind != EpochConnection && config.EpochKind != EpochPoll {
		return fmt.Errorf("%w: invalid epoch kind %q", ErrBounds, config.EpochKind)
	}
	return nil
}

func validateCatalogPathID(name, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || !pathologize.IsClean(value) || value == "." || value == ".." {
		return fmt.Errorf("%w: invalid %s path part", ErrBounds, name)
	}
	for i, r := range value {
		valid := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.'
		if !valid || i == 0 && (r == '-' || r == '_' || r == '.') {
			return fmt.Errorf("%w: invalid %s path part", ErrBounds, name)
		}
	}
	return nil
}

func ensurePrivateDirectory(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("%w: namespace escaped root", ErrSpoolRoot)
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		err := os.Mkdir(current, 0o700)
		if err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("segment: create spool namespace %s: %w", current, err)
		}
		info, statErr := os.Lstat(current)
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: namespace component %s is not a real directory", ErrSpoolRoot, current)
		}
	}
	return nil
}

func (s *Spool) NewWriter(options WriterOptions) (*Writer, error) {
	normalized, err := normalizeEncodeOptions(EncodeOptions{FrameBytes: options.FrameBytes, Concurrency: 1})
	if err != nil {
		return nil, err
	}
	if options.SegmentBytes == 0 {
		options.SegmentBytes = DefaultSegmentBytes
	}
	if options.MaxAge == 0 {
		options.MaxAge = DefaultSegmentAge
	}
	if options.WriterVersion == "" || len(options.WriterVersion) > MaxRecorderVersionBytes {
		return nil, fmt.Errorf("%w: invalid writer version", ErrBounds)
	}
	if options.SegmentBytes < uint64(normalized.FrameBytes) || options.SegmentBytes > uint64(^uint(0)>>1) || options.MaxAge <= 0 {
		return nil, fmt.Errorf("%w: invalid segment rotation bounds", ErrBounds)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	options.FrameBytes = normalized.FrameBytes
	return &Writer{
		spool:    s,
		options:  options,
		rotation: RotationPolicy{MaxUncompressedBytes: int(options.SegmentBytes), MaxAge: options.MaxAge},
		state:    writerActive,
	}, nil
}

type writerState uint8

const (
	writerActive writerState = iota
	writerFailed
	writerClosed
)

type Writer struct {
	spool    *Spool
	options  WriterOptions
	state    writerState
	rotation RotationPolicy

	file               *os.File
	tempPath           string
	startedAt          time.Time
	frame              []byte
	frameCount         uint32
	frameFirstOrdinal  uint64
	frameLastOrdinal   uint64
	frameFirstReceived int64
	frameLastReceived  int64

	manifest         Manifest
	segmentHash      hash.Hash
	uncompressedHash hash.Hash
	schema           map[[32]byte]struct{}
	haveEpochOrdinal bool
	lastEpochOrdinal uint64
}

func (w *Writer) BufferedBytes() int {
	if w.frameCount == 0 {
		return 0
	}
	return len(w.frame)
}

func (w *Writer) BufferedLimit() int { return w.options.FrameBytes }

func (w *Writer) Write(record Envelope) (*ReadySegment, error) {
	if w.state != writerActive {
		return nil, ErrWriterDone
	}
	if err := w.validateRecord(record); err != nil {
		return nil, err
	}
	recordSize, err := encodedRecordSize(record)
	if err != nil {
		return nil, err
	}
	if recordSize+FrameHeaderSize > w.options.FrameBytes {
		return nil, fmt.Errorf("%w: encoded record needs %d bytes, frame bound is %d", ErrBounds, recordSize+FrameHeaderSize, w.options.FrameBytes)
	}

	var rotated *ReadySegment
	projected := w.manifest.UncompressedBytes
	if w.frameCount == 0 {
		projected += uint64(FrameHeaderSize + recordSize)
	} else {
		projected += uint64(len(w.frame) + recordSize)
	}
	if w.manifest.RecordCount > 0 {
		policyBytes := int(projected)
		if projected > uint64(^uint(0)>>1) {
			policyBytes = int(^uint(0) >> 1)
		}
		reason := w.rotation.Reason(policyBytes, w.startedAt, w.options.Now(), false, false)
		if reason != RotationNone {
			rotated, err = w.seal(reason)
			if err != nil {
				return nil, err
			}
		}
	}
	if w.file == nil {
		if err := w.startSegment(); err != nil {
			return nil, err
		}
	}
	if w.frameCount > 0 && len(w.frame)+recordSize > w.options.FrameBytes {
		if err := w.flushFrame(); err != nil {
			return nil, err
		}
	}
	if w.frameCount == 0 {
		w.frameFirstOrdinal = record.ArrivalOrdinal
		w.frameFirstReceived = record.ReceivedWallTimeNS
	}
	w.frame = appendEncodedRecord(w.frame, record, nil)
	w.frameCount++
	w.frameLastOrdinal = record.ArrivalOrdinal
	w.frameLastReceived = record.ReceivedWallTimeNS
	if w.manifest.RecordCount == 0 {
		w.manifest.FirstOrdinal = record.ArrivalOrdinal
		w.manifest.FirstReceivedAtNS = record.ReceivedWallTimeNS
	}
	w.manifest.RecordCount++
	w.manifest.LastOrdinal = record.ArrivalOrdinal
	w.manifest.LastReceivedAtNS = record.ReceivedWallTimeNS
	if record.SchemaFingerprint.Valid {
		w.schema[record.SchemaFingerprint.Value] = struct{}{}
	}
	w.haveEpochOrdinal = true
	w.lastEpochOrdinal = record.ArrivalOrdinal
	return rotated, nil
}

func (w *Writer) EndEpoch() (*ReadySegment, error) { return w.closeWithReason(RotationEpochEnd) }
func (w *Writer) Shutdown() (*ReadySegment, error) { return w.closeWithReason(RotationShutdown) }

func (w *Writer) closeWithReason(reason RotationReason) (*ReadySegment, error) {
	if w.state != writerActive {
		return nil, ErrWriterDone
	}
	ready, err := w.seal(reason)
	if err != nil {
		return nil, err
	}
	w.state = writerClosed
	return ready, nil
}

func (w *Writer) validateRecord(record Envelope) error {
	if record.SourceID != w.spool.config.SourceID || record.ChannelOrEndpoint != w.spool.config.ChannelID {
		return fmt.Errorf("%w: record crosses source/channel writer boundary", ErrBounds)
	}
	if w.spool.config.EpochKind == EpochConnection {
		if !record.ConnectionEpoch.Valid || record.ConnectionEpoch.Value != w.spool.config.EpochID || record.PollCycleID.Valid {
			return fmt.Errorf("%w: record crosses connection epoch writer boundary", ErrBounds)
		}
	} else if !record.PollCycleID.Valid || record.PollCycleID.Value != w.spool.config.EpochID || record.ConnectionEpoch.Valid {
		return fmt.Errorf("%w: record crosses poll epoch writer boundary", ErrBounds)
	}
	if w.haveEpochOrdinal && record.ArrivalOrdinal <= w.lastEpochOrdinal {
		return fmt.Errorf("%w: ordinal %d is not greater than epoch-local ordinal %d", ErrBounds, record.ArrivalOrdinal, w.lastEpochOrdinal)
	}
	return nil
}

func (w *Writer) startSegment() error {
	file, err := os.CreateTemp(w.spool.openDir, ".open-*.emseg.zst")
	if err != nil {
		return w.fail(fmt.Errorf("segment: create unique spool file: %w", err))
	}
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return w.fail(fmt.Errorf("segment: set spool file mode: %w", err))
	}
	w.file = file
	w.tempPath = file.Name()
	w.startedAt = w.options.Now()
	w.frame = make([]byte, FrameHeaderSize, w.options.FrameBytes)
	w.segmentHash = sha256.New()
	w.uncompressedHash = sha256.New()
	w.schema = make(map[[32]byte]struct{})
	w.manifest = Manifest{FormatVersion: FormatVersion, FrameBytes: uint64(w.options.FrameBytes)}
	if err := w.trip(FaultTemporaryCreated); err != nil {
		return w.fail(err)
	}
	return nil
}

type frameDiskWriter struct {
	file    *os.File
	frame   hash.Hash
	segment hash.Hash
	bytes   uint64
}

func (w *frameDiskWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	if n > 0 {
		_, _ = w.frame.Write(p[:n])
		_, _ = w.segment.Write(p[:n])
		w.bytes += uint64(n)
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *Writer) flushFrame() error {
	if w.frameCount == 0 {
		return nil
	}
	ordinal := uint32(len(w.manifest.Frames))
	recordBytes := len(w.frame) - FrameHeaderSize
	copy(w.frame[:8], frameMagic[:])
	binary.LittleEndian.PutUint16(w.frame[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(w.frame[10:12], FrameHeaderSize)
	binary.LittleEndian.PutUint32(w.frame[12:16], ordinal)
	binary.LittleEndian.PutUint32(w.frame[16:20], w.frameCount)
	binary.LittleEndian.PutUint32(w.frame[20:24], uint32(recordBytes))
	binary.LittleEndian.PutUint64(w.frame[24:32], w.frameFirstOrdinal)
	binary.LittleEndian.PutUint64(w.frame[32:40], w.frameLastOrdinal)
	binary.LittleEndian.PutUint64(w.frame[40:48], uint64(w.frameFirstReceived))
	binary.LittleEndian.PutUint64(w.frame[48:56], uint64(w.frameLastReceived))
	recordsHash := sha256.Sum256(w.frame[FrameHeaderSize:])
	copy(w.frame[56:88], recordsHash[:])
	plainHash := sha256.Sum256(w.frame)
	_, _ = w.uncompressedHash.Write(w.frame)

	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(nextPowerOfTwo(max(len(w.frame), zstd.MinWindowSize))),
		zstd.WithSingleSegment(true),
	)
	if err != nil {
		return w.fail(fmt.Errorf("segment: create frame encoder: %w", err))
	}
	compressed := encoder.EncodeAll(w.frame, nil)
	if len(compressed) > maxStoredFrameBytes {
		_ = encoder.Close()
		return w.fail(fmt.Errorf("%w: compressed frame exceeds stored bound", ErrBounds))
	}
	if err := w.trip(FaultBeforeFrameWrite); err != nil {
		_ = encoder.Close()
		return w.fail(err)
	}
	disk := &frameDiskWriter{file: w.file, frame: sha256.New(), segment: w.segmentHash}
	prefix := max(1, len(compressed)/2)
	if err := writeFull(disk, compressed[:prefix]); err != nil {
		_ = encoder.Close()
		return w.fail(fmt.Errorf("segment: write frame prefix: %w", err))
	}
	if err := w.trip(FaultAfterFramePrefix); err != nil {
		_ = encoder.Close()
		return w.fail(err)
	}
	if err := writeFull(disk, compressed[prefix:]); err != nil {
		_ = encoder.Close()
		return w.fail(fmt.Errorf("segment: write frame suffix: %w", err))
	}
	if err := w.trip(FaultAfterFrameWrite); err != nil {
		_ = encoder.Close()
		return w.fail(err)
	}
	if err := w.trip(FaultBeforeFrameClose); err != nil {
		_ = encoder.Close()
		return w.fail(err)
	}
	if err := encoder.Close(); err != nil {
		return w.fail(fmt.Errorf("segment: close frame encoder: %w", err))
	}
	if err := w.trip(FaultAfterFrameClose); err != nil {
		return w.fail(err)
	}

	var compressedHash [32]byte
	copy(compressedHash[:], disk.frame.Sum(nil))
	w.manifest.Frames = append(w.manifest.Frames, FrameIndex{
		Ordinal:            ordinal,
		CompressedOffset:   w.manifest.CompressedBytes,
		CompressedBytes:    disk.bytes,
		UncompressedBytes:  uint64(len(w.frame)),
		RecordCount:        w.frameCount,
		FirstOrdinal:       w.frameFirstOrdinal,
		LastOrdinal:        w.frameLastOrdinal,
		FirstReceivedAtNS:  w.frameFirstReceived,
		LastReceivedAtNS:   w.frameLastReceived,
		CompressedSHA256:   compressedHash,
		UncompressedSHA256: plainHash,
	})
	w.manifest.CompressedBytes += disk.bytes
	w.manifest.UncompressedBytes += uint64(len(w.frame))
	w.frame = w.frame[:FrameHeaderSize]
	w.frameCount = 0
	return nil
}

func (w *Writer) seal(reason RotationReason) (*ReadySegment, error) {
	if w.manifest.RecordCount == 0 {
		return nil, nil
	}
	if err := w.flushFrame(); err != nil {
		return nil, err
	}
	if err := w.trip(FaultBeforeSegmentSync); err != nil {
		return nil, w.fail(err)
	}
	if err := w.file.Sync(); err != nil {
		return nil, w.fail(fmt.Errorf("segment: fsync segment: %w", err))
	}
	if err := w.trip(FaultAfterSegmentSync); err != nil {
		return nil, w.fail(err)
	}
	if err := w.trip(FaultBeforeSegmentClose); err != nil {
		return nil, w.fail(err)
	}
	if err := w.file.Close(); err != nil {
		w.file = nil
		return nil, w.fail(fmt.Errorf("segment: close segment: %w", err))
	}
	w.file = nil
	if err := w.trip(FaultAfterSegmentClose); err != nil {
		return nil, w.fail(err)
	}
	storedHash, storedBytes, err := hashFile(w.tempPath)
	if err != nil {
		return nil, w.fail(err)
	}
	var streamedHash [32]byte
	copy(streamedHash[:], w.segmentHash.Sum(nil))
	if storedHash != streamedHash || storedBytes != w.manifest.CompressedBytes {
		return nil, w.fail(fmt.Errorf("%w: exact stored segment bytes changed before seal", ErrCorrupt))
	}
	w.manifest.CompressedSHA256 = storedHash
	copy(w.manifest.UncompressedSHA256[:], w.uncompressedHash.Sum(nil))

	closedPath := filepath.Join(w.spool.openDir, strings.Replace(filepath.Base(w.tempPath), ".open-", ".closed-", 1))
	if err := w.trip(FaultBeforeClosedRename); err != nil {
		return nil, w.fail(err)
	}
	closedPath, err = w.spool.flow.Rename(w.tempPath, closedPath)
	if err != nil {
		return nil, w.fail(fmt.Errorf("segment: mark closed: %w", err))
	}
	w.tempPath = closedPath
	if err := w.syncRenameDirs(FaultBeforeClosedDirectorySync, FaultAfterClosedDirectorySync, w.spool.openDir); err != nil {
		return nil, w.fail(err)
	}
	if err := w.trip(FaultAfterClosedRename); err != nil {
		return nil, w.fail(err)
	}

	segmentName := fmt.Sprintf("segment=%020d-%020d-%x.emseg.zst", w.manifest.FirstOrdinal, w.manifest.LastOrdinal, storedHash)
	if err := w.trip(FaultBeforeSegmentExpose); err != nil {
		return nil, w.fail(err)
	}
	segmentPath, err := w.spool.flow.Rename(closedPath, filepath.Join(w.spool.readyDir, segmentName))
	if err != nil {
		return nil, w.fail(fmt.Errorf("segment: expose sealed segment: %w", err))
	}
	w.tempPath = ""
	if err := w.syncRenameDirs(FaultBeforeSegmentDirectorySync, FaultAfterSegmentDirectorySync, w.spool.openDir, w.spool.readyDir); err != nil {
		return nil, w.fail(err)
	}
	if err := w.trip(FaultAfterSegmentExpose); err != nil {
		return nil, w.fail(err)
	}

	manifest := w.readyManifest(filepath.Base(segmentPath), reason)
	manifestBytes, err := marshalReadyManifest(manifest)
	if err != nil {
		return nil, w.fail(err)
	}
	manifestHash := sha256.Sum256(manifestBytes)
	manifestPath, err := w.writeManifest(manifestBytes, storedHash, manifestHash)
	if err != nil {
		return nil, err
	}
	if err := w.trip(FaultBeforeReadyVerify); err != nil {
		return nil, w.fail(err)
	}
	ready, err := w.spool.verifyManifestPath(manifestPath)
	if err != nil {
		return nil, w.fail(fmt.Errorf("segment: verify newly ready pair: %w", err))
	}
	w.resetSegment()
	return &ready, nil
}

func (w *Writer) readyManifest(segmentFile string, reason RotationReason) ReadyManifest {
	fingerprints := make([][32]byte, 0, len(w.schema))
	for fingerprint := range w.schema {
		fingerprints = append(fingerprints, fingerprint)
	}
	slices.SortFunc(fingerprints, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
	return ReadyManifest{
		ManifestVersion:    SpoolManifestVersion,
		SourceID:           w.spool.config.SourceID,
		ChannelID:          w.spool.config.ChannelID,
		EpochKind:          w.spool.config.EpochKind,
		EpochID:            hex.EncodeToString(w.spool.config.EpochID[:]),
		WriterVersion:      w.options.WriterVersion,
		RotationReason:     reason,
		SegmentFile:        segmentFile,
		ObjectKey:          objectKey(w.spool.config, w.manifest),
		SchemaFingerprints: fingerprints,
		Segment:            w.manifest,
	}
}

func (w *Writer) writeManifest(data []byte, segmentHash, manifestHash [32]byte) (string, error) {
	if err := w.trip(FaultBeforeManifestWrite); err != nil {
		return "", w.fail(err)
	}
	file, err := os.CreateTemp(w.spool.openDir, ".manifest-*.tmp")
	if err != nil {
		return "", w.fail(fmt.Errorf("segment: create manifest sidecar: %w", err))
	}
	temp := file.Name()
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return "", w.fail(fmt.Errorf("segment: set manifest mode: %w", err))
	}
	if err := writeFull(file, data); err != nil {
		file.Close()
		return "", w.fail(fmt.Errorf("segment: write manifest: %w", err))
	}
	if err := w.trip(FaultAfterManifestWrite); err != nil {
		file.Close()
		return "", w.fail(err)
	}
	if err := w.trip(FaultBeforeManifestSync); err != nil {
		file.Close()
		return "", w.fail(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", w.fail(fmt.Errorf("segment: fsync manifest: %w", err))
	}
	if err := w.trip(FaultAfterManifestSync); err != nil {
		file.Close()
		return "", w.fail(err)
	}
	if err := w.trip(FaultBeforeManifestClose); err != nil {
		file.Close()
		return "", w.fail(err)
	}
	if err := file.Close(); err != nil {
		return "", w.fail(fmt.Errorf("segment: close manifest: %w", err))
	}
	if err := w.trip(FaultAfterManifestClose); err != nil {
		return "", w.fail(err)
	}
	name := fmt.Sprintf("manifest=%x-%x.ready.json", segmentHash, manifestHash)
	if err := w.trip(FaultBeforeManifestExpose); err != nil {
		return "", w.fail(err)
	}
	final, err := w.spool.flow.Rename(temp, filepath.Join(w.spool.readyDir, name))
	if err != nil {
		return "", w.fail(fmt.Errorf("segment: expose ready manifest: %w", err))
	}
	if err := w.syncRenameDirs(FaultBeforeManifestDirectorySync, FaultAfterManifestDirectorySync, w.spool.openDir, w.spool.readyDir); err != nil {
		return "", w.fail(err)
	}
	if err := w.trip(FaultAfterManifestExpose); err != nil {
		return "", w.fail(err)
	}
	return final, nil
}

func (w *Writer) syncRenameDirs(before, after FaultPoint, dirs ...string) error {
	if err := w.trip(before); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(dirs))
	for _, dir := range dirs {
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}
		if err := syncDirectory(dir); err != nil {
			return err
		}
	}
	return w.trip(after)
}

func (w *Writer) trip(point FaultPoint) error {
	if w.options.Fault == nil {
		return nil
	}
	if err := w.options.Fault(point); err != nil {
		return fmt.Errorf("segment: injected fault at %s: %w", point, err)
	}
	return nil
}

func (w *Writer) fail(err error) error {
	w.state = writerFailed
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	return err
}

func (w *Writer) resetSegment() {
	w.file = nil
	w.tempPath = ""
	w.frame = nil
	w.frameCount = 0
	w.manifest = Manifest{}
	w.segmentHash = nil
	w.uncompressedHash = nil
	w.schema = nil
}

func marshalReadyManifest(manifest ReadyManifest) ([]byte, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("segment: marshal ready manifest: %w", err)
	}
	return append(data, '\n'), nil
}

func objectKey(config SpoolConfig, manifest Manifest) string {
	first := time.Unix(0, manifest.FirstReceivedAtNS).UTC()
	return fmt.Sprintf("raw/v1/source=%s/date=%s/hour=%02d/epoch=%x/segment=%d-%d-%x.emseg.zst",
		config.SourceID, first.Format("2006-01-02"), first.Hour(), config.EpochID,
		manifest.FirstOrdinal, manifest.LastOrdinal, manifest.CompressedSHA256)
}

func (s *Spool) Ready() ([]ReadySegment, error) {
	entries, err := os.ReadDir(s.readyDir)
	if err != nil {
		return nil, fmt.Errorf("segment: list ready namespace: %w", err)
	}
	var ready []ReadySegment
	for _, entry := range entries {
		if entry.IsDir() || !isReadyManifestName(entry.Name()) {
			continue
		}
		verified, err := s.verifyManifestPath(filepath.Join(s.readyDir, entry.Name()))
		if err != nil {
			return nil, err
		}
		ready = append(ready, verified)
	}
	slices.SortFunc(ready, func(a, b ReadySegment) int { return strings.Compare(a.ManifestPath, b.ManifestPath) })
	return ready, nil
}

func (s *Spool) VerifyReady(manifestFile string) (ReadySegment, error) {
	if filepath.Base(manifestFile) != manifestFile || !isReadyManifestName(manifestFile) {
		return ReadySegment{}, fmt.Errorf("%w: invalid manifest file name", ErrNotReady)
	}
	return s.verifyManifestPath(filepath.Join(s.readyDir, manifestFile))
}

// ReadReady verifies the complete ready pair before yielding records in exact
// frame and record order. It then rechecks each frame while streaming so a
// concurrent local mutation cannot pass through unchecked bytes.
func (s *Spool) ReadReady(manifestFile string, yield func(Envelope) error) (ReadySegment, error) {
	ready, err := s.VerifyReady(manifestFile)
	if err != nil {
		return ReadySegment{}, err
	}
	if yield == nil {
		return ReadySegment{}, fmt.Errorf("%w: nil record consumer", ErrBounds)
	}
	info, err := regularFileInfo(ready.SegmentPath)
	if err != nil {
		return ReadySegment{}, fmt.Errorf("%w: reopen verified segment: %v", ErrNotReady, err)
	}
	file, err := os.Open(ready.SegmentPath)
	if err != nil {
		return ReadySegment{}, fmt.Errorf("%w: reopen verified segment: %v", ErrNotReady, err)
	}
	defer file.Close()
	if current, err := file.Stat(); err != nil || !os.SameFile(info, current) {
		return ReadySegment{}, fmt.Errorf("%w: verified segment changed while reopening", ErrCorrupt)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecodedFrameBytes), zstd.WithDecoderMaxWindow(maxEncodedFrameWindowBytes))
	if err != nil {
		return ReadySegment{}, fmt.Errorf("segment: create streaming decoder: %w", err)
	}
	defer decoder.Close()
	segmentHash := sha256.New()
	for i, expected := range ready.Manifest.Segment.Frames {
		compressed := make([]byte, expected.CompressedBytes)
		if _, err := io.ReadFull(file, compressed); err != nil {
			return ReadySegment{}, &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: %v", ErrTruncated, err)}
		}
		if sha256.Sum256(compressed) != expected.CompressedSHA256 {
			return ReadySegment{}, &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: frame changed after verification", ErrCorrupt)}
		}
		_, _ = segmentHash.Write(compressed)
		plain, err := decoder.DecodeAll(compressed, nil)
		if err != nil {
			return ReadySegment{}, &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: zstd decode: %v", ErrCorrupt, err)}
		}
		records, _, err := decodeFrame(plain, uint32(i), expected.CompressedOffset, expected.CompressedBytes)
		if err != nil {
			return ReadySegment{}, err
		}
		for _, record := range records {
			if err := yield(record); err != nil {
				return ReadySegment{}, fmt.Errorf("segment: consume ready record: %w", err)
			}
		}
	}
	if sha256Sum(segmentHash) != ready.Manifest.Segment.CompressedSHA256 {
		return ReadySegment{}, fmt.Errorf("%w: segment changed after verification", ErrCorrupt)
	}
	var extra [1]byte
	if n, err := file.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
		return ReadySegment{}, fmt.Errorf("%w: trailing segment bytes after verification", ErrCorrupt)
	}
	return ready, nil
}

func (s *Spool) verifyManifestPath(manifestPath string) (ReadySegment, error) {
	data, err := readBoundedFile(manifestPath, maxManifestBytes)
	if err != nil {
		return ReadySegment{}, fmt.Errorf("%w: read manifest: %v", ErrNotReady, err)
	}
	expectedSegmentHash, expectedManifestHash, err := hashesFromManifestName(filepath.Base(manifestPath))
	if err != nil {
		return ReadySegment{}, err
	}
	actualManifestHash := sha256.Sum256(data)
	if actualManifestHash != expectedManifestHash {
		return ReadySegment{}, fmt.Errorf("%w: exact manifest SHA-256 mismatch", ErrCorrupt)
	}
	var manifest ReadyManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return ReadySegment{}, fmt.Errorf("%w: decode manifest: %v", ErrCorrupt, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return ReadySegment{}, err
	}
	canonical, err := marshalReadyManifest(manifest)
	if err != nil || !bytes.Equal(canonical, data) {
		return ReadySegment{}, fmt.Errorf("%w: manifest is not exact canonical bytes", ErrCorrupt)
	}
	if manifest.Segment.CompressedSHA256 != expectedSegmentHash {
		return ReadySegment{}, fmt.Errorf("%w: manifest name segment SHA-256 mismatch", ErrCorrupt)
	}
	if err := s.validateReadyManifest(manifest); err != nil {
		return ReadySegment{}, err
	}
	segmentPath := filepath.Join(s.readyDir, manifest.SegmentFile)
	if err := s.verifySegmentFile(segmentPath, manifest); err != nil {
		return ReadySegment{}, err
	}
	return ReadySegment{
		SegmentPath:    segmentPath,
		ManifestPath:   manifestPath,
		ManifestBytes:  uint64(len(data)),
		ManifestSHA256: actualManifestHash,
		Manifest:       manifest,
	}, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: manifest has trailing JSON", ErrCorrupt)
	}
	return nil
}

func (s *Spool) validateReadyManifest(manifest ReadyManifest) error {
	if manifest.ManifestVersion != SpoolManifestVersion || manifest.Segment.FormatVersion != FormatVersion {
		return fmt.Errorf("%w: unsupported spool or segment manifest version", ErrVersion)
	}
	if manifest.SourceID != s.config.SourceID || manifest.ChannelID != s.config.ChannelID || manifest.EpochKind != s.config.EpochKind || manifest.EpochID != hex.EncodeToString(s.config.EpochID[:]) {
		return fmt.Errorf("%w: manifest crosses declared spool namespace", ErrCorrupt)
	}
	if manifest.WriterVersion == "" || len(manifest.WriterVersion) > MaxRecorderVersionBytes {
		return fmt.Errorf("%w: invalid manifest writer version", ErrCorrupt)
	}
	switch manifest.RotationReason {
	case RotationSize, RotationAge, RotationEpochEnd, RotationShutdown, RotationRecovery:
	default:
		return fmt.Errorf("%w: invalid rotation reason", ErrCorrupt)
	}
	if filepath.Base(manifest.SegmentFile) != manifest.SegmentFile || !pathologize.IsClean(manifest.SegmentFile) {
		return fmt.Errorf("%w: invalid contained segment file", ErrCorrupt)
	}
	if manifest.Segment.RecordCount == 0 || len(manifest.Segment.Frames) == 0 {
		return fmt.Errorf("%w: empty ready segment", ErrCorrupt)
	}
	options, err := normalizeEncodeOptions(EncodeOptions{FrameBytes: int(manifest.Segment.FrameBytes), Concurrency: 1})
	if err != nil || options.FrameBytes != int(manifest.Segment.FrameBytes) {
		return fmt.Errorf("%w: invalid manifest frame bound", ErrCorrupt)
	}
	if manifest.ObjectKey != objectKey(s.config, manifest.Segment) {
		return fmt.Errorf("%w: object key disagrees with exact segment identity", ErrCorrupt)
	}
	if !slices.IsSortedFunc(manifest.SchemaFingerprints, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) }) {
		return fmt.Errorf("%w: schema fingerprints are not sorted", ErrCorrupt)
	}
	for i := 1; i < len(manifest.SchemaFingerprints); i++ {
		if manifest.SchemaFingerprints[i] == manifest.SchemaFingerprints[i-1] {
			return fmt.Errorf("%w: duplicate schema fingerprint", ErrCorrupt)
		}
	}
	var compressedOffset, compressedBytes, uncompressedBytes, records uint64
	for i, frame := range manifest.Segment.Frames {
		if frame.Ordinal != uint32(i) || frame.CompressedOffset != compressedOffset || frame.CompressedBytes == 0 || frame.CompressedBytes > maxStoredFrameBytes || frame.UncompressedBytes < FrameHeaderSize || frame.UncompressedBytes > manifest.Segment.FrameBytes || frame.RecordCount == 0 {
			return fmt.Errorf("%w: invalid frame index %d", ErrCorrupt, i)
		}
		if i > 0 && frame.FirstOrdinal <= manifest.Segment.Frames[i-1].LastOrdinal {
			return fmt.Errorf("%w: non-increasing frame ordinal bounds", ErrCorrupt)
		}
		compressedOffset += frame.CompressedBytes
		compressedBytes += frame.CompressedBytes
		uncompressedBytes += frame.UncompressedBytes
		records += uint64(frame.RecordCount)
	}
	m := manifest.Segment
	if compressedBytes != m.CompressedBytes || compressedOffset != m.CompressedBytes || uncompressedBytes != m.UncompressedBytes || records != m.RecordCount || m.FirstOrdinal != m.Frames[0].FirstOrdinal || m.LastOrdinal != m.Frames[len(m.Frames)-1].LastOrdinal || m.FirstReceivedAtNS != m.Frames[0].FirstReceivedAtNS || m.LastReceivedAtNS != m.Frames[len(m.Frames)-1].LastReceivedAtNS {
		return fmt.Errorf("%w: manifest summary disagrees with frame index", ErrCorrupt)
	}
	return nil
}

func (s *Spool) verifySegmentFile(path string, manifest ReadyManifest) error {
	info, err := regularFileInfo(path)
	if err != nil || uint64(info.Size()) != manifest.Segment.CompressedBytes {
		return fmt.Errorf("%w: segment file size/type mismatch", ErrCorrupt)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: open segment: %v", ErrNotReady, err)
	}
	defer file.Close()
	if current, err := file.Stat(); err != nil || !os.SameFile(info, current) {
		return fmt.Errorf("%w: segment file changed while opening", ErrCorrupt)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecodedFrameBytes), zstd.WithDecoderMaxWindow(maxEncodedFrameWindowBytes))
	if err != nil {
		return fmt.Errorf("segment: create verifier decoder: %w", err)
	}
	defer decoder.Close()
	segmentCompressed := sha256.New()
	segmentPlain := sha256.New()
	var previous Envelope
	havePrevious := false
	seenSchema := make(map[[32]byte]struct{})
	var previousFrameBytes uint64
	for i, expected := range manifest.Segment.Frames {
		compressed := make([]byte, expected.CompressedBytes)
		if _, err := io.ReadFull(file, compressed); err != nil {
			return &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: %v", ErrTruncated, err)}
		}
		_, _ = segmentCompressed.Write(compressed)
		if sha256.Sum256(compressed) != expected.CompressedSHA256 {
			return &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: compressed frame hash mismatch", ErrCorrupt)}
		}
		length, err := zstdFrameLength(compressed)
		if err != nil || length != len(compressed) {
			return &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: frame boundary mismatch", ErrCorrupt)}
		}
		plain, err := decoder.DecodeAll(compressed, nil)
		if err != nil {
			return &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: zstd decode: %v", ErrCorrupt, err)}
		}
		if uint64(len(plain)) != expected.UncompressedBytes || sha256.Sum256(plain) != expected.UncompressedSHA256 {
			return &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: fmt.Errorf("%w: uncompressed frame mismatch", ErrCorrupt)}
		}
		_, _ = segmentPlain.Write(plain)
		records, got, err := decodeFrame(plain, uint32(i), expected.CompressedOffset, expected.CompressedBytes)
		if err != nil {
			return &DecodeError{Frame: uint32(i), Offset: expected.CompressedOffset, Err: err}
		}
		if i > 0 {
			firstRecordBytes, sizeErr := encodedRecordSize(records[0])
			if sizeErr != nil || previousFrameBytes+uint64(firstRecordBytes) <= manifest.Segment.FrameBytes {
				return &DecodeError{Frame: uint32(i - 1), Offset: manifest.Segment.Frames[i-1].CompressedOffset, Err: fmt.Errorf("%w: frame ended before its declared bound", ErrCorrupt)}
			}
		}
		previousFrameBytes = expected.UncompressedBytes
		got.CompressedSHA256 = expected.CompressedSHA256
		if err := compareFrameIndex(got, expected); err != nil {
			return err
		}
		for j := range records {
			var prior *Envelope
			if havePrevious {
				prior = &previous
			}
			if err := s.validateRecoveredRecord(records[j], prior); err != nil {
				return err
			}
			if records[j].SchemaFingerprint.Valid {
				seenSchema[records[j].SchemaFingerprint.Value] = struct{}{}
			}
			previous.ArrivalOrdinal = records[j].ArrivalOrdinal
			havePrevious = true
		}
	}
	if got := sha256Sum(segmentCompressed); got != manifest.Segment.CompressedSHA256 || sha256Sum(segmentPlain) != manifest.Segment.UncompressedSHA256 {
		return fmt.Errorf("%w: exact segment hash mismatch", ErrCorrupt)
	}
	if len(seenSchema) != len(manifest.SchemaFingerprints) {
		return fmt.Errorf("%w: schema fingerprint set mismatch", ErrCorrupt)
	}
	for _, fingerprint := range manifest.SchemaFingerprints {
		if _, ok := seenSchema[fingerprint]; !ok {
			return fmt.Errorf("%w: schema fingerprint set mismatch", ErrCorrupt)
		}
	}
	return nil
}

func (s *Spool) validateRecoveredRecord(record Envelope, previous *Envelope) error {
	if record.SourceID != s.config.SourceID || record.ChannelOrEndpoint != s.config.ChannelID {
		return fmt.Errorf("%w: record crosses declared spool namespace", ErrCorrupt)
	}
	if s.config.EpochKind == EpochConnection {
		if !record.ConnectionEpoch.Valid || record.ConnectionEpoch.Value != s.config.EpochID || record.PollCycleID.Valid {
			return fmt.Errorf("%w: record crosses declared connection epoch", ErrCorrupt)
		}
	} else if !record.PollCycleID.Valid || record.PollCycleID.Value != s.config.EpochID || record.ConnectionEpoch.Valid {
		return fmt.Errorf("%w: record crosses declared poll epoch", ErrCorrupt)
	}
	if previous != nil && record.ArrivalOrdinal <= previous.ArrivalOrdinal {
		return fmt.Errorf("%w: recovered ordinal order is not strict", ErrCorrupt)
	}
	return nil
}

func (s *Spool) Recover(options RecoveryOptions) (RecoveryReport, error) {
	if options.FrameBytes == 0 {
		options.FrameBytes = DefaultFrameBytes
	}
	if _, err := normalizeEncodeOptions(EncodeOptions{FrameBytes: options.FrameBytes, Concurrency: 1}); err != nil {
		return RecoveryReport{}, err
	}
	if len(options.WriterVersion) > MaxRecorderVersionBytes {
		return RecoveryReport{}, fmt.Errorf("%w: invalid recovery writer version", ErrBounds)
	}

	var report RecoveryReport
	appendQuarantined := func(item RecoveryItem) error {
		moved, err := s.quarantineMany(item.State, item.Paths, options.Fault)
		item.Quarantined = moved
		report.Items = append(report.Items, item)
		if err != nil {
			return fmt.Errorf("segment: %s recovery quarantine is unresolved: %w", item.State, err)
		}
		return nil
	}

	type invalidReadyManifest struct {
		path string
		err  error
	}
	protectedReady := make(map[string]bool)
	var protectedFiles []fs.FileInfo
	isProtectedReadyFile := func(name, path string) bool {
		if protectedReady[name] {
			return true
		}
		info, err := regularFileInfo(path)
		if err != nil {
			return false
		}
		for _, protectedInfo := range protectedFiles {
			if os.SameFile(info, protectedInfo) {
				return true
			}
		}
		return false
	}
	var invalidManifests []invalidReadyManifest

	readyEntries, err := os.ReadDir(s.readyDir)
	if err != nil {
		return report, fmt.Errorf("segment: scan ready namespace: %w", err)
	}
	// Establish every cryptographically valid pair before looking at any
	// unchecked manifest content. A corrupt manifest can therefore never make
	// a segment belonging to a valid pair look orphaned or quarantineable.
	for _, entry := range readyEntries {
		if entry.IsDir() || !isReadyManifestName(entry.Name()) {
			continue
		}
		manifestPath := filepath.Join(s.readyDir, entry.Name())
		ready, verifyErr := s.verifyManifestPath(manifestPath)
		if verifyErr != nil {
			invalidManifests = append(invalidManifests, invalidReadyManifest{path: manifestPath, err: verifyErr})
			continue
		}
		for _, protectedPath := range []string{ready.SegmentPath, ready.ManifestPath} {
			protectedReady[filepath.Base(protectedPath)] = true
			info, infoErr := regularFileInfo(protectedPath)
			if infoErr != nil {
				return report, fmt.Errorf("segment: retain verified ready reference: %w", infoErr)
			}
			protectedFiles = append(protectedFiles, info)
		}
		copy := ready
		report.Items = append(report.Items, RecoveryItem{
			State:          RecoveryReady,
			Paths:          []string{ready.SegmentPath, ready.ManifestPath},
			CompleteFrames: uint32(len(ready.Manifest.Segment.Frames)),
			CompleteBytes:  ready.Manifest.Segment.CompressedBytes,
			Ready:          &copy,
		})
	}

	for _, invalid := range invalidManifests {
		item := RecoveryItem{State: RecoveryCorrupt, Paths: []string{invalid.path}, Err: invalid.err}
		manifest, parseErr := parseManifestUnchecked(invalid.path)
		if parseErr == nil && validLocalFileName(manifest.SegmentFile) {
			segmentName := manifest.SegmentFile
			segmentPath := filepath.Join(s.readyDir, segmentName)
			_, statErr := os.Lstat(segmentPath)
			switch {
			case errors.Is(statErr, fs.ErrNotExist):
				item.State = RecoveryManifestOnly
			case statErr == nil && !isProtectedReadyFile(segmentName, segmentPath):
				item.Paths = append(item.Paths, segmentPath)
				diagnostic := s.diagnoseSegment(segmentPath)
				item.CompleteFrames = uint32(len(diagnostic.frames))
				item.CompleteBytes = diagnostic.completeBytes
			case statErr != nil:
				item.Err = errors.Join(item.Err, fmt.Errorf("segment: inspect unchecked manifest segment reference: %w", statErr))
			}
		}
		if err := appendQuarantined(item); err != nil {
			return report, err
		}
	}

	readyEntries, err = os.ReadDir(s.readyDir)
	if err != nil {
		return report, fmt.Errorf("segment: rescan ready namespace: %w", err)
	}
	for _, entry := range readyEntries {
		path := filepath.Join(s.readyDir, entry.Name())
		if entry.IsDir() {
			item := RecoveryItem{State: RecoveryCorrupt, Paths: []string{path}, Err: fmt.Errorf("%w: unexpected directory in ready namespace", ErrCorrupt)}
			if err := appendQuarantined(item); err != nil {
				return report, err
			}
			continue
		}
		if isReadyManifestName(entry.Name()) {
			continue
		}
		if !strings.Contains(entry.Name(), ".emseg") {
			item := RecoveryItem{State: RecoveryCorrupt, Paths: []string{path}, Err: fmt.Errorf("%w: unknown ready artifact", ErrCorrupt)}
			if err := appendQuarantined(item); err != nil {
				return report, err
			}
			continue
		}
		if protectedReady[entry.Name()] {
			continue
		}
		diagnostic := s.diagnoseSegment(path)
		state := RecoverySegmentOnly
		if diagnostic.err != nil && errors.Is(diagnostic.err, ErrTruncated) {
			state = RecoveryTruncated
		} else if diagnostic.err != nil {
			state = RecoveryCorrupt
		}
		item := RecoveryItem{
			State:          state,
			Paths:          []string{path},
			CompleteFrames: uint32(len(diagnostic.frames)),
			CompleteBytes:  diagnostic.completeBytes,
			Err:            diagnostic.err,
		}
		if err := appendQuarantined(item); err != nil {
			return report, err
		}
	}

	openEntries, err := os.ReadDir(s.openDir)
	if err != nil {
		return report, fmt.Errorf("segment: scan open namespace: %w", err)
	}
	for _, entry := range openEntries {
		path := filepath.Join(s.openDir, entry.Name())
		if entry.IsDir() {
			item := RecoveryItem{State: RecoveryCorrupt, Paths: []string{path}, Err: fmt.Errorf("%w: unexpected directory in open namespace", ErrCorrupt)}
			if err := appendQuarantined(item); err != nil {
				return report, err
			}
			continue
		}
		if strings.HasPrefix(entry.Name(), ".closed-") && strings.Contains(entry.Name(), ".emseg") {
			diagnostic := s.diagnoseSegment(path)
			state := RecoveryClosed
			if diagnostic.err != nil && errors.Is(diagnostic.err, ErrTruncated) {
				state = RecoveryTruncated
			} else if diagnostic.err != nil {
				state = RecoveryCorrupt
			}
			item := RecoveryItem{
				State:          state,
				Paths:          []string{path},
				CompleteFrames: uint32(len(diagnostic.frames)),
				CompleteBytes:  diagnostic.completeBytes,
				Err:            diagnostic.err,
			}
			if diagnostic.err == nil && len(diagnostic.frames) > 0 && options.WriterVersion != "" {
				ready, recoverErr := s.exposeRecoveredClosed(path, diagnostic, options)
				if recoverErr == nil {
					item.Ready = &ready
					item.Paths = []string{ready.SegmentPath, ready.ManifestPath}
					report.Items = append(report.Items, item)
				} else {
					item.Err = recoverErr
					if err := appendQuarantined(item); err != nil {
						return report, err
					}
				}
			} else if err := appendQuarantined(item); err != nil {
				return report, err
			}
			continue
		}
		if strings.HasPrefix(entry.Name(), ".open-") && strings.Contains(entry.Name(), ".emseg") {
			diagnostic := s.diagnoseSegment(path)
			state := RecoveryOpen
			if diagnostic.err != nil && errors.Is(diagnostic.err, ErrTruncated) {
				state = RecoveryTruncated
			} else if diagnostic.err != nil {
				state = RecoveryCorrupt
			}
			item := RecoveryItem{
				State:          state,
				Paths:          []string{path},
				CompleteFrames: uint32(len(diagnostic.frames)),
				CompleteBytes:  diagnostic.completeBytes,
				Err:            diagnostic.err,
			}
			if err := appendQuarantined(item); err != nil {
				return report, err
			}
			continue
		}
		if strings.HasPrefix(entry.Name(), ".manifest-") {
			item := RecoveryItem{State: RecoveryManifestOnly, Paths: []string{path}}
			if err := appendQuarantined(item); err != nil {
				return report, err
			}
			continue
		}
		item := RecoveryItem{State: RecoveryCorrupt, Paths: []string{path}, Err: fmt.Errorf("%w: unknown open artifact", ErrCorrupt)}
		if err := appendQuarantined(item); err != nil {
			return report, err
		}
	}

	for i := range report.Items {
		if report.Items[i].Ready == nil {
			continue
		}
		for j := range i {
			if report.Items[j].Ready == nil {
				continue
			}
			a := report.Items[i].Ready.Manifest.Segment
			b := report.Items[j].Ready.Manifest.Segment
			if a.FirstOrdinal > b.LastOrdinal || b.FirstOrdinal > a.LastOrdinal {
				continue
			}
			conflictErr := fmt.Errorf("%w: ready epoch-local ordinal ranges overlap", ErrCorrupt)
			report.Items[i].State = RecoveryConflicting
			report.Items[i].Err = conflictErr
			report.Items[j].State = RecoveryConflicting
			report.Items[j].Err = conflictErr
		}
	}
	for i := range report.Items {
		if report.Items[i].State != RecoveryConflicting {
			continue
		}
		moved, err := s.quarantineMany(RecoveryConflicting, report.Items[i].Paths, options.Fault)
		report.Items[i].Quarantined = moved
		report.Items[i].Ready = nil
		if err != nil {
			return report, fmt.Errorf("segment: conflicting recovery quarantine is unresolved: %w", err)
		}
	}
	slices.SortFunc(report.Items, func(a, b RecoveryItem) int {
		if len(a.Paths) == 0 || len(b.Paths) == 0 {
			return len(a.Paths) - len(b.Paths)
		}
		return strings.Compare(a.Paths[0], b.Paths[0])
	})
	return report, nil
}

type segmentDiagnostic struct {
	manifest         Manifest
	frames           []FrameIndex
	completeBytes    uint64
	schema           map[[32]byte]struct{}
	firstRecordBytes []uint64
	err              error
}

func (s *Spool) diagnoseSegment(path string) segmentDiagnostic {
	info, infoErr := regularFileInfo(path)
	if infoErr != nil {
		return segmentDiagnostic{err: infoErr}
	}
	file, err := os.Open(path)
	if err != nil {
		return segmentDiagnostic{err: err}
	}
	defer file.Close()
	current, err := file.Stat()
	if err != nil || !os.SameFile(info, current) {
		return segmentDiagnostic{err: fmt.Errorf("%w: diagnostic file changed while opening", ErrCorrupt)}
	}
	result := segmentDiagnostic{schema: make(map[[32]byte]struct{})}
	compressedHasher := sha256.New()
	plainHasher := sha256.New()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecodedFrameBytes), zstd.WithDecoderMaxWindow(maxEncodedFrameWindowBytes))
	if err != nil {
		result.err = err
		return result
	}
	defer decoder.Close()
	var previous Envelope
	havePrevious := false
	for result.completeBytes < uint64(info.Size()) {
		compressed, readErr := readFrameAt(file, int64(result.completeBytes), info.Size())
		if readErr != nil {
			result.err = &DecodeError{Frame: uint32(len(result.frames)), Offset: result.completeBytes, Err: readErr}
			break
		}
		plain, decodeErr := decoder.DecodeAll(compressed, nil)
		if decodeErr != nil {
			result.err = &DecodeError{Frame: uint32(len(result.frames)), Offset: result.completeBytes, Err: fmt.Errorf("%w: zstd decode: %v", ErrCorrupt, decodeErr)}
			break
		}
		records, frame, decodeErr := decodeFrame(plain, uint32(len(result.frames)), result.completeBytes, uint64(len(compressed)))
		if decodeErr != nil {
			result.err = decodeErr
			break
		}
		firstRecordBytes, sizeErr := encodedRecordSize(records[0])
		if sizeErr != nil {
			result.err = fmt.Errorf("%w: recovered first record size: %v", ErrCorrupt, sizeErr)
			break
		}
		result.firstRecordBytes = append(result.firstRecordBytes, uint64(firstRecordBytes))
		frame.CompressedSHA256 = sha256.Sum256(compressed)
		for i := range records {
			var prior *Envelope
			if havePrevious {
				prior = &previous
			}
			if recoverErr := s.validateRecoveredRecord(records[i], prior); recoverErr != nil {
				result.err = recoverErr
				return result
			}
			if records[i].SchemaFingerprint.Valid {
				result.schema[records[i].SchemaFingerprint.Value] = struct{}{}
			}
			previous.ArrivalOrdinal = records[i].ArrivalOrdinal
			havePrevious = true
		}
		_, _ = compressedHasher.Write(compressed)
		_, _ = plainHasher.Write(plain)
		result.completeBytes += uint64(len(compressed))
		result.frames = append(result.frames, frame)
	}
	if len(result.frames) > 0 {
		first := result.frames[0]
		last := result.frames[len(result.frames)-1]
		result.manifest = Manifest{
			FormatVersion:      FormatVersion,
			RecordCount:        sumFrameRecords(result.frames),
			UncompressedBytes:  sumFramePlainBytes(result.frames),
			CompressedBytes:    result.completeBytes,
			FirstOrdinal:       first.FirstOrdinal,
			LastOrdinal:        last.LastOrdinal,
			FirstReceivedAtNS:  first.FirstReceivedAtNS,
			LastReceivedAtNS:   last.LastReceivedAtNS,
			CompressedSHA256:   sha256Sum(compressedHasher),
			UncompressedSHA256: sha256Sum(plainHasher),
			Frames:             result.frames,
		}
	}
	return result
}

func (s *Spool) exposeRecoveredClosed(path string, diagnostic segmentDiagnostic, options RecoveryOptions) (ReadySegment, error) {
	manifest := diagnostic.manifest
	manifest.FrameBytes = uint64(options.FrameBytes)
	if err := validateRecoveredFrameTarget(manifest, diagnostic.firstRecordBytes); err != nil {
		return ReadySegment{}, err
	}
	segmentName := fmt.Sprintf("segment=%020d-%020d-%x.emseg.zst", manifest.FirstOrdinal, manifest.LastOrdinal, manifest.CompressedSHA256)
	segmentPath, err := s.flow.Rename(path, filepath.Join(s.readyDir, segmentName))
	if err != nil {
		return ReadySegment{}, err
	}
	if err := syncDirectory(s.openDir); err != nil {
		return ReadySegment{}, err
	}
	if err := syncDirectory(s.readyDir); err != nil {
		return ReadySegment{}, err
	}
	fingerprints := make([][32]byte, 0, len(diagnostic.schema))
	for fingerprint := range diagnostic.schema {
		fingerprints = append(fingerprints, fingerprint)
	}
	slices.SortFunc(fingerprints, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
	readyManifest := ReadyManifest{
		ManifestVersion:    SpoolManifestVersion,
		SourceID:           s.config.SourceID,
		ChannelID:          s.config.ChannelID,
		EpochKind:          s.config.EpochKind,
		EpochID:            hex.EncodeToString(s.config.EpochID[:]),
		WriterVersion:      options.WriterVersion,
		RotationReason:     RotationRecovery,
		SegmentFile:        filepath.Base(segmentPath),
		ObjectKey:          objectKey(s.config, manifest),
		SchemaFingerprints: fingerprints,
		Segment:            manifest,
	}
	data, err := marshalReadyManifest(readyManifest)
	if err != nil {
		return ReadySegment{}, err
	}
	manifestHash := sha256.Sum256(data)
	temp, err := os.CreateTemp(s.openDir, ".manifest-*.tmp")
	if err != nil {
		return ReadySegment{}, err
	}
	if err := writeFull(temp, data); err != nil {
		temp.Close()
		return ReadySegment{}, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return ReadySegment{}, err
	}
	if err := temp.Close(); err != nil {
		return ReadySegment{}, err
	}
	name := fmt.Sprintf("manifest=%x-%x.ready.json", manifest.CompressedSHA256, manifestHash)
	manifestPath, err := s.flow.Rename(temp.Name(), filepath.Join(s.readyDir, name))
	if err != nil {
		return ReadySegment{}, err
	}
	if err := syncDirectory(s.openDir); err != nil {
		return ReadySegment{}, err
	}
	if err := syncDirectory(s.readyDir); err != nil {
		return ReadySegment{}, err
	}
	return s.verifyManifestPath(manifestPath)
}

func validateRecoveredFrameTarget(manifest Manifest, firstRecordBytes []uint64) error {
	if len(firstRecordBytes) != len(manifest.Frames) {
		return fmt.Errorf("%w: recovered frame sizing evidence is incomplete", ErrCorrupt)
	}
	for i, frame := range manifest.Frames {
		if frame.UncompressedBytes > manifest.FrameBytes {
			return fmt.Errorf("%w: recovered frame %d exceeds declared bound", ErrCorrupt, i)
		}
		if i > 0 && manifest.Frames[i-1].UncompressedBytes+firstRecordBytes[i] <= manifest.FrameBytes {
			return fmt.Errorf("%w: recovered frame %d ended before its declared bound", ErrCorrupt, i-1)
		}
	}
	return nil
}

func (s *Spool) quarantineMany(state RecoveryState, paths []string, fault FaultInjector) ([]string, error) {
	moved := make([]string, 0, len(paths))
	for _, path := range paths {
		parent := filepath.Dir(path)
		if parent != s.openDir && parent != s.readyDir || !validLocalFileName(filepath.Base(path)) {
			return moved, fmt.Errorf("%w: quarantine source is outside the declared spool namespace: %s", ErrBounds, path)
		}
		if _, err := regularFileInfo(path); err != nil {
			return moved, fmt.Errorf("segment: validate quarantine source %s: %w", path, err)
		}
		if err := injectRecoveryFault(fault, FaultBeforeQuarantine); err != nil {
			return moved, err
		}
		name := pathologize.Clean(string(state) + "-" + filepath.Base(path))
		final, err := s.flow.Rename(path, filepath.Join(s.quarantineDir, name))
		if err != nil {
			return moved, fmt.Errorf("segment: quarantine %s: %w", path, err)
		}
		moved = append(moved, final)
		if err := injectRecoveryFault(fault, FaultBeforeQuarantineSourceSync); err != nil {
			return moved, err
		}
		if err := syncDirectory(parent); err != nil {
			return moved, err
		}
		if err := injectRecoveryFault(fault, FaultAfterQuarantineSourceSync); err != nil {
			return moved, err
		}
		if err := injectRecoveryFault(fault, FaultBeforeQuarantineDirectorySync); err != nil {
			return moved, err
		}
		if err := syncDirectory(s.quarantineDir); err != nil {
			return moved, err
		}
		if err := injectRecoveryFault(fault, FaultAfterQuarantineDirectorySync); err != nil {
			return moved, err
		}
		if err := injectRecoveryFault(fault, FaultAfterQuarantine); err != nil {
			return moved, err
		}
	}
	return moved, nil
}

func injectRecoveryFault(fault FaultInjector, point FaultPoint) error {
	if fault == nil {
		return nil
	}
	if err := fault(point); err != nil {
		return fmt.Errorf("segment: injected recovery fault at %s: %w", point, err)
	}
	return nil
}

func readFrameAt(file *os.File, offset, size int64) ([]byte, error) {
	remaining := size - offset
	if remaining <= 0 {
		return nil, io.EOF
	}
	capacity := int64(64 << 10)
	if capacity > remaining {
		capacity = remaining
	}
	buf := make([]byte, capacity)
	for {
		n, err := file.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		buf = buf[:n]
		length, frameErr := zstdFrameLength(buf)
		if frameErr == nil {
			if length > maxStoredFrameBytes {
				return nil, fmt.Errorf("%w: stored frame exceeds bound", ErrBounds)
			}
			return append([]byte(nil), buf[:length]...), nil
		}
		if !errors.Is(frameErr, ErrTruncated) {
			return nil, frameErr
		}
		if int64(n) == remaining {
			return nil, frameErr
		}
		next := int64(cap(buf)) * 2
		if next > remaining {
			next = remaining
		}
		if next > maxStoredFrameBytes {
			next = maxStoredFrameBytes
		}
		if next <= int64(n) {
			return nil, fmt.Errorf("%w: stored frame exceeds bound", ErrBounds)
		}
		buf = make([]byte, next)
	}
}

func parseManifestUnchecked(path string) (ReadyManifest, error) {
	data, err := readBoundedFile(path, maxManifestBytes)
	if err != nil {
		return ReadyManifest{}, err
	}
	var manifest ReadyManifest
	err = json.Unmarshal(data, &manifest)
	return manifest, err
}

func isReadyManifestName(name string) bool {
	return strings.HasPrefix(name, "manifest=") && strings.HasSuffix(name, ".json")
}

func hashesFromManifestName(name string) ([32]byte, [32]byte, error) {
	var segmentHash, manifestHash [32]byte
	if !isReadyManifestName(name) {
		return segmentHash, manifestHash, fmt.Errorf("%w: invalid ready manifest name", ErrNotReady)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, "manifest="), ".json")
	parts := strings.Split(body, "-")
	if len(parts) < 2 || len(parts[0]) != 64 || len(parts[1]) < 64 {
		return segmentHash, manifestHash, fmt.Errorf("%w: invalid ready manifest identity", ErrNotReady)
	}
	segmentBytes, segmentErr := hex.DecodeString(parts[0])
	manifestBytes, manifestErr := hex.DecodeString(parts[1][:64])
	if segmentErr != nil || manifestErr != nil || len(segmentBytes) != sha256.Size || len(manifestBytes) != sha256.Size {
		return segmentHash, manifestHash, fmt.Errorf("%w: invalid ready manifest hashes", ErrNotReady)
	}
	copy(segmentHash[:], segmentBytes)
	copy(manifestHash[:], manifestBytes)
	return segmentHash, manifestHash, nil
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	info, err := regularFileInfo(path)
	if err != nil {
		return nil, err
	}
	if info.Size() < 1 || info.Size() > limit {
		return nil, fmt.Errorf("file size outside bound")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	current, err := file.Stat()
	if err != nil || !os.SameFile(info, current) {
		return nil, fmt.Errorf("file changed while opening")
	}
	data := make([]byte, info.Size())
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	return data, nil
}
func regularFileInfo(path string) (fs.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular non-symlink file")
	}
	return info, nil
}

func validLocalFileName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && pathologize.IsClean(name)
}

func hashFile(path string) ([32]byte, uint64, error) {
	var result [32]byte
	file, err := os.Open(path)
	if err != nil {
		return result, 0, fmt.Errorf("segment: open exact stored file: %w", err)
	}
	defer file.Close()
	hasher := sha256.New()
	bytes, err := io.CopyBuffer(hasher, file, make([]byte, 64<<10))
	if err != nil {
		return result, 0, fmt.Errorf("segment: hash exact stored file: %w", err)
	}
	copy(result[:], hasher.Sum(nil))
	return result, uint64(bytes), nil
}

func sha256Sum(hasher hash.Hash) [32]byte {
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func sumFrameRecords(frames []FrameIndex) uint64 {
	var total uint64
	for _, frame := range frames {
		total += uint64(frame.RecordCount)
	}
	return total
}

func sumFramePlainBytes(frames []FrameIndex) uint64 {
	var total uint64
	for _, frame := range frames {
		total += frame.UncompressedBytes
	}
	return total
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("segment: open parent directory for fsync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("segment: fsync parent directory: %w", err)
	}
	return nil
}
