package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
)

const (
	manifestFormat      = "enable-market-release-gate-input/v1"
	observationFormat   = "enable-market-release-gate-observation/v1"
	faultReceiptFormat  = "enable-market-release-gate-fault-receipt/v1"
	determinismFormat   = "enable-market-release-gate-determinism-receipt/v1"
	canaryReceiptFormat = "enable-market-release-gate-canary-receipt/v1"
	x5ReceiptFormat     = "enable-market-release-gate-x5-receipt/v1"
	maxManifestBytes    = int64(4 << 20)
	maxEvidenceBytes    = int64(64 << 20)
	maxInputFileBytes   = int64(1 << 30)
	maxTotalInputBytes  = int64(8 << 30)
	maxCorpusObjects    = 4096
	maxRSSSamples       = 1 << 20
)

type signedHardware struct {
	Value     quality.HardwareIdentity `json:"value"`
	Signature string                   `json:"signature"`
}

type signedWorkload struct {
	Value     quality.WorkloadIdentity `json:"value"`
	Signature string                   `json:"signature"`
}

type signedCorpus struct {
	Value     quality.FixedCorpusIdentity `json:"value"`
	Signature string                      `json:"signature"`
}

type fileRef struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type replayLimits struct {
	Concurrency          int    `json:"concurrency"`
	Prefetch             int    `json:"prefetch"`
	MaxSources           int    `json:"max_sources"`
	MaxSegments          int    `json:"max_segments"`
	MaxSegmentBytes      int64  `json:"max_segment_bytes"`
	MaxRecordsPerSegment uint64 `json:"max_records_per_segment"`
	MaxFramesPerSegment  int    `json:"max_frames_per_segment"`
}

func (l replayLimits) config() replay.Config {
	return replay.Config{
		Concurrency: l.Concurrency, Prefetch: l.Prefetch, MaxSources: l.MaxSources,
		MaxSegments: l.MaxSegments, MaxSegmentBytes: l.MaxSegmentBytes,
		MaxRecordsPerSegment: l.MaxRecordsPerSegment, MaxFramesPerSegment: l.MaxFramesPerSegment,
	}
}

type memoryLimits struct {
	QueueBoundBytes     uint64 `json:"queue_bound_bytes"`
	FrameBoundBytes     uint64 `json:"frame_bound_bytes"`
	RowGroupBoundBytes  uint64 `json:"row_group_bound_bytes"`
	TelemetryBoundBytes uint64 `json:"telemetry_bound_bytes"`
}

type durationPolicy struct {
	ReplayNS     int64 `json:"replay_ns"`
	NormalizedNS int64 `json:"normalized_ns"`
	TelemetryNS  int64 `json:"telemetry_ns"`
}

type rssPolicy struct {
	MaximumSamples        int    `json:"maximum_samples"`
	PlateauWindowSamples  int    `json:"plateau_window_samples"`
	PlateauToleranceBytes uint64 `json:"plateau_tolerance_bytes"`
	SampleIntervalNS      int64  `json:"sample_interval_ns"`
}

type publicationSpec struct {
	SegmentID       string `json:"segment_id"`
	SourceID        string `json:"source_id"`
	ChannelID       string `json:"channel_id"`
	EpochID         string `json:"epoch_id"`
	ReceivedStartNS int64  `json:"received_start_ns"`
	ReceivedEndNS   int64  `json:"received_end_ns"`
	OrdinalStart    uint64 `json:"ordinal_start"`
	OrdinalEnd      uint64 `json:"ordinal_end"`
	ObjectKey       string `json:"object_key"`
	ManifestVersion uint16 `json:"manifest_version"`
}

type normalizerSpec struct {
	Kind                 string  `json:"kind"`
	CatalogSnapshot      fileRef `json:"catalog_snapshot"`
	MapperVersion        string  `json:"mapper_version"`
	EffectiveFromNS      int64   `json:"effective_from_ns"`
	EffectiveUntilNS     *int64  `json:"effective_until_ns,omitempty"`
	SourceTimeResolution string  `json:"source_time_resolution"`
}

type datasetSpec struct {
	Root                string `json:"root"`
	RowGroupTargetBytes int64  `json:"row_group_target_bytes"`
	PageBufferBytes     int    `json:"page_buffer_bytes"`
	Dictionary          bool   `json:"dictionary"`
	BloomFilter         bool   `json:"bloom_filter"`
	MaxInputRows        uint64 `json:"max_input_rows"`
	MaxParquetRows      uint64 `json:"max_parquet_rows"`
	DatasetPolicyID     string `json:"dataset_policy_id"`
	ReplayConfigID      string `json:"replay_config_id"`
	InputManifestSetID  string `json:"input_manifest_set_id"`
}

type corpusObject struct {
	ID                     string               `json:"id"`
	VenueFamily            string               `json:"venue_family"`
	PayloadClass           quality.PayloadClass `json:"payload_class"`
	HighCardinalitySymbols bool                 `json:"high_cardinality_symbols"`
	LongBooks              bool                 `json:"long_books"`
	SparseTickerUpdates    bool                 `json:"sparse_ticker_updates"`
	Reconnect              bool                 `json:"reconnect"`
	LongHistory            bool                 `json:"long_history"`
	Segment                fileRef              `json:"segment"`
	ReadyManifest          fileRef              `json:"ready_manifest"`
	Publication            publicationSpec      `json:"publication"`
	Normalizer             *normalizerSpec      `json:"normalizer,omitempty"`
	Dataset                *datasetSpec         `json:"dataset,omitempty"`
}

type inputManifest struct {
	Format        string         `json:"format"`
	MaxFileBytes  int64          `json:"max_file_bytes"`
	MaxTotalBytes int64          `json:"max_total_bytes"`
	Hardware      signedHardware `json:"hardware"`
	Workload      signedWorkload `json:"workload"`
	FixedCorpus   signedCorpus   `json:"fixed_corpus"`
	Replay        replayLimits   `json:"replay"`
	Memory        memoryLimits   `json:"memory"`
	Durations     durationPolicy `json:"durations"`
	RSS           rssPolicy      `json:"rss"`
	Objects       []corpusObject `json:"objects"`
}

type loadedObject struct {
	spec          corpusObject
	segment       []byte
	readyBytes    []byte
	ready         segment.ReadyManifest
	descriptor    replay.InputDescriptor
	contentSHA256 [sha256.Size]byte
	snapshot      []byte
}

type loadedManifest struct {
	value   inputManifest
	bytes   []byte
	sha256  string
	base    string
	objects []loadedObject
}

type boundedLoader struct {
	base       string
	fileLimit  int64
	totalLimit int64
	frameLimit uint64
	total      int64
	cache      map[string]loadedFile
}

type loadedFile struct {
	ref  fileRef
	data []byte
}

func loadInputManifest(path, trustedSignerPublicKey string) (loadedManifest, error) {
	trustedSigner, err := decodeTrustedSignerPublicKey(trustedSignerPublicKey)
	if err != nil {
		return loadedManifest{}, err
	}
	data, err := readTopLevelRegular(path, maxManifestBytes)
	if err != nil {
		return loadedManifest{}, fmt.Errorf("read input manifest: %w", err)
	}
	var manifest inputManifest
	if err := decodeStrict(data, &manifest); err != nil {
		return loadedManifest{}, fmt.Errorf("decode input manifest: %w", err)
	}
	if err := validateManifestDeclarations(&manifest, trustedSigner); err != nil {
		return loadedManifest{}, err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return loadedManifest{}, fmt.Errorf("resolve input manifest: %w", err)
	}
	loader := boundedLoader{
		base: filepath.Dir(absolute), fileLimit: manifest.MaxFileBytes,
		totalLimit: manifest.MaxTotalBytes, frameLimit: manifest.Memory.FrameBoundBytes,
		cache: make(map[string]loadedFile),
	}
	objects, err := loader.loadObjects(manifest.Objects)
	if err != nil {
		return loadedManifest{}, err
	}
	digest := sha256.Sum256(data)
	return loadedManifest{value: manifest, bytes: data, sha256: hex.EncodeToString(digest[:]), base: loader.base, objects: objects}, nil
}

func validateManifestDeclarations(manifest *inputManifest, trustedSigner ed25519.PublicKey) error {
	if len(trustedSigner) != ed25519.PublicKeySize {
		return errors.New("trusted signer public key is required")
	}
	if manifest.Format != manifestFormat {
		return fmt.Errorf("unsupported input manifest format %q", manifest.Format)
	}
	if manifest.MaxFileBytes <= 0 || manifest.MaxFileBytes > maxInputFileBytes || manifest.MaxTotalBytes <= 0 || manifest.MaxTotalBytes > maxTotalInputBytes || manifest.MaxFileBytes > manifest.MaxTotalBytes {
		return errors.New("input manifest file or total byte bound is invalid")
	}
	if err := verifyHardwareDeclaration(manifest.Hardware, trustedSigner); err != nil {
		return fmt.Errorf("hardware declaration: %w", err)
	}
	if err := verifyWorkloadDeclaration(manifest.Workload, trustedSigner); err != nil {
		return fmt.Errorf("workload declaration: %w", err)
	}
	if err := verifyCorpusDeclaration(manifest.FixedCorpus, trustedSigner); err != nil {
		return fmt.Errorf("fixed corpus declaration: %w", err)
	}
	if len(manifest.Objects) == 0 || len(manifest.Objects) > maxCorpusObjects {
		return errors.New("fixed corpus object count is zero or unbounded")
	}
	if manifest.Durations.ReplayNS <= 0 || manifest.Durations.ReplayNS > quality.MaxReleaseGateDurationNS || manifest.Durations.NormalizedNS <= 0 || manifest.Durations.NormalizedNS > quality.MaxReleaseGateDurationNS || manifest.Durations.TelemetryNS <= 0 || manifest.Durations.TelemetryNS > quality.MaxReleaseGateDurationNS {
		return errors.New("replay, normalized, or telemetry duration is invalid")
	}
	if manifest.RSS.MaximumSamples < 2 || manifest.RSS.MaximumSamples > maxRSSSamples || manifest.RSS.PlateauWindowSamples < 1 || manifest.RSS.PlateauWindowSamples > manifest.RSS.MaximumSamples/2 || manifest.RSS.SampleIntervalNS <= 0 || manifest.RSS.SampleIntervalNS > quality.MaxReleaseGateDurationNS {
		return errors.New("RSS sampling policy is invalid")
	}
	if manifest.Replay.Concurrency < 1 || manifest.Replay.Concurrency > replay.MaximumWorkers ||
		manifest.Replay.Prefetch < 1 || manifest.Replay.Prefetch > replay.MaximumPrefetch ||
		manifest.Replay.MaxSources < 1 || manifest.Replay.MaxSegments < 1 || manifest.Replay.MaxSegmentBytes < 1 ||
		manifest.Replay.MaxRecordsPerSegment < 1 || manifest.Replay.MaxFramesPerSegment < 1 {
		return errors.New("replay worker, queue, or input bounds are invalid")
	}
	queueBytes, err := checkedMultiply(uint64(manifest.Replay.Prefetch), uint64(manifest.Replay.MaxSegmentBytes))
	if err != nil || queueBytes > manifest.Memory.QueueBoundBytes {
		return errors.New("replay prefetch memory exceeds the declared queue bound")
	}
	if manifest.Memory.QueueBoundBytes == 0 || manifest.Memory.FrameBoundBytes == 0 || manifest.Memory.RowGroupBoundBytes == 0 || manifest.Memory.TelemetryBoundBytes == 0 {
		return errors.New("queue, frame, row-group, and telemetry memory bounds are required")
	}
	if _, err := checkedSum(manifest.Memory.QueueBoundBytes, manifest.Memory.FrameBoundBytes, manifest.Memory.RowGroupBoundBytes); err != nil {
		return fmt.Errorf("memory bounds: %w", err)
	}
	return validateCorpusCoverage(manifest)
}

func verifyHardwareDeclaration(declaration signedHardware, trustedSigner ed25519.PublicKey) error {
	value := declaration.Value
	claimed := value.ManifestSHA256
	value.ManifestSHA256 = ""
	return verifyDeclaration(value, claimed, trustedSigner, declaration.Signature)
}

func verifyWorkloadDeclaration(declaration signedWorkload, trustedSigner ed25519.PublicKey) error {
	value := declaration.Value
	claimed := value.ManifestSHA256
	value.ManifestSHA256 = ""
	return verifyDeclaration(value, claimed, trustedSigner, declaration.Signature)
}

func verifyCorpusDeclaration(declaration signedCorpus, trustedSigner ed25519.PublicKey) error {
	value := declaration.Value
	claimed := value.ManifestSHA256
	value.ManifestSHA256 = ""
	return verifyDeclaration(value, claimed, trustedSigner, declaration.Signature)
}

func verifyDeclaration(value any, claimed string, trustedSigner ed25519.PublicKey, signatureText string) error {
	if len(trustedSigner) != ed25519.PublicKeySize {
		return errors.New("trusted signer public key is required")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal signed body: %w", err)
	}
	digest := sha256.Sum256(body)
	if claimed != hex.EncodeToString(digest[:]) {
		return errors.New("body SHA-256 mismatch")
	}
	signature, err := decodeExactHex(signatureText, ed25519.SignatureSize)
	if err != nil {
		return fmt.Errorf("signature: %w", err)
	}
	if !ed25519.Verify(trustedSigner, body, signature) {
		return errors.New("Ed25519 signature mismatch")
	}
	return nil
}

func validateCorpusCoverage(manifest *inputManifest) error {
	identity := manifest.FixedCorpus.Value
	families := make(map[string]map[quality.PayloadClass]struct{})
	ids := make(map[string]struct{}, len(manifest.Objects))
	var high, books, sparse, reconnects, histories bool
	for i := range manifest.Objects {
		object := &manifest.Objects[i]
		if object.ID == "" || len(object.ID) > quality.MaxReleaseGateTextBytes || object.VenueFamily == "" {
			return fmt.Errorf("corpus object %d has invalid identity", i)
		}
		ids[object.ID] = struct{}{}
		if object.PayloadClass != quality.PayloadTiny && object.PayloadClass != quality.PayloadMedian && object.PayloadClass != quality.PayloadMax {
			return fmt.Errorf("corpus object %q has unknown payload class", object.ID)
		}
		if families[object.VenueFamily] == nil {
			families[object.VenueFamily] = make(map[quality.PayloadClass]struct{})
		}
		families[object.VenueFamily][object.PayloadClass] = struct{}{}
		high = high || object.HighCardinalitySymbols
		if object.Dataset != nil && (object.Dataset.RowGroupTargetBytes <= 0 || uint64(object.Dataset.RowGroupTargetBytes) > manifest.Memory.RowGroupBoundBytes) {
			return fmt.Errorf("corpus object %q dataset row-group size exceeds the declared bound", object.ID)
		}
		books = books || object.LongBooks
		sparse = sparse || object.SparseTickerUpdates
		reconnects = reconnects || object.Reconnect
		histories = histories || object.LongHistory
	}
	declaredFamilies := slices.Clone(identity.VenueFamilies)
	slices.Sort(declaredFamilies)
	actualFamilies := make([]string, 0, len(families))
	for family, classes := range families {
		actualFamilies = append(actualFamilies, family)
		for _, class := range []quality.PayloadClass{quality.PayloadTiny, quality.PayloadMedian, quality.PayloadMax} {
			if _, ok := classes[class]; !ok {
				return fmt.Errorf("venue family %q lacks payload class %q", family, class)
			}
		}
	}
	slices.Sort(actualFamilies)
	if !slices.Equal(actualFamilies, declaredFamilies) {
		return errors.New("corpus object venue families differ from the signed corpus identity")
	}
	if !high || !books || !sparse || !reconnects || !histories {
		return errors.New("corpus objects do not evidence every mandatory workload shape")
	}
	if high != identity.HighCardinalitySymbols || books != identity.LongBooks || sparse != identity.SparseTickerUpdates || reconnects != identity.Reconnects || histories != identity.LongHistories {
		return errors.New("corpus workload shapes differ from the signed corpus identity")
	}
	return nil
}

func (l *boundedLoader) loadObjects(specs []corpusObject) ([]loadedObject, error) {
	ordered := slices.Clone(specs)
	slices.SortFunc(ordered, func(a, b corpusObject) int { return strings.Compare(a.ID, b.ID) })
	objects := make([]loadedObject, 0, len(ordered))
	for _, spec := range ordered {
		segmentFile, err := l.read(spec.Segment)
		if err != nil {
			return nil, fmt.Errorf("corpus object %q segment: %w", spec.ID, err)
		}
		manifestFile, err := l.read(spec.ReadyManifest)
		if err != nil {
			return nil, fmt.Errorf("corpus object %q ready manifest: %w", spec.ID, err)
		}
		var ready segment.ReadyManifest
		if err := decodeStrict(manifestFile, &ready); err != nil {
			return nil, fmt.Errorf("corpus object %q ready manifest: %w", spec.ID, err)
		}
		if ready.Segment.FrameBytes == 0 || ready.Segment.FrameBytes > uint64(l.fileLimit) || ready.Segment.FrameBytes > l.frameLimit {
			return nil, fmt.Errorf("corpus object %q frame size exceeds the declared bound", spec.ID)
		}
		contentDigest := sha256.Sum256(segmentFile)
		manifestDigest := sha256.Sum256(manifestFile)
		publication := catalog.RawSegmentPublication{
			SegmentID: spec.Publication.SegmentID, SourceID: spec.Publication.SourceID,
			ChannelID: spec.Publication.ChannelID, EpochID: spec.Publication.EpochID,
			ReceivedStartNS: spec.Publication.ReceivedStartNS, ReceivedEndNS: spec.Publication.ReceivedEndNS,
			OrdinalStart: spec.Publication.OrdinalStart, OrdinalEnd: spec.Publication.OrdinalEnd,
			ObjectKey: spec.Publication.ObjectKey, ContentSHA256: contentDigest,
			ByteLength: int64(len(segmentFile)), ManifestVersion: spec.Publication.ManifestVersion,
			ManifestSHA256: manifestDigest, ManifestBytes: manifestFile, State: catalog.RawSegmentCommitted,
		}
		descriptor, err := replay.NewInputDescriptor(publication)
		if err != nil {
			return nil, fmt.Errorf("corpus object %q replay descriptor: %w", spec.ID, err)
		}
		loaded := loadedObject{spec: spec, segment: segmentFile, readyBytes: manifestFile, ready: ready, descriptor: descriptor, contentSHA256: contentDigest}
		if spec.Normalizer != nil {
			snapshot, err := l.read(spec.Normalizer.CatalogSnapshot)
			if err != nil {
				return nil, fmt.Errorf("corpus object %q catalog snapshot: %w", spec.ID, err)
			}
			loaded.snapshot = snapshot
		}
		objects = append(objects, loaded)
	}
	return objects, nil
}

func (l *boundedLoader) read(ref fileRef) ([]byte, error) {
	if err := validateFileRef(ref, l.fileLimit); err != nil {
		return nil, err
	}
	if cached, ok := l.cache[ref.Path]; ok {
		if cached.ref != ref {
			return nil, errors.New("same path is declared with conflicting size or digest")
		}
		return cached.data, nil
	}
	path, err := containedRegularPath(l.base, ref.Path)
	if err != nil {
		return nil, err
	}
	if ref.Bytes > l.totalLimit-l.total {
		return nil, errors.New("declared corpus exceeds total byte bound")
	}
	data, err := readRegularExact(path, ref.Bytes)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, errors.New("SHA-256 mismatch")
	}
	l.total += ref.Bytes
	l.cache[ref.Path] = loadedFile{ref: ref, data: data}
	return data, nil
}

func validateFileRef(ref fileRef, maximum int64) error {
	if ref.Path == "" || ref.Bytes <= 0 || ref.Bytes > maximum {
		return errors.New("path or exact byte bound is invalid")
	}
	if !validSHA256(ref.SHA256) {
		return errors.New("SHA-256 must be lowercase hexadecimal")
	}
	return nil
}

func containedRegularPath(base, relative string) (string, error) {
	if filepath.IsAbs(relative) || relative == "." || filepath.Clean(relative) != relative || strings.Contains(relative, `\`) {
		return "", errors.New("input path must be a clean relative path")
	}
	for _, part := range strings.Split(filepath.ToSlash(relative), "/") {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("input path traversal is forbidden")
		}
	}
	current := base
	parts := strings.Split(filepath.FromSlash(relative), string(filepath.Separator))
	for _, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return "", err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return "", errors.New("input path contains a symbolic link")
		}
	}
	return current, nil
}

func readTopLevelRegular(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("path is not a regular non-symlink file")
	}
	if info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("file size is zero or exceeds bound")
	}
	return readRegularExact(path, info.Size())
}

func readRegularExact(path string, expected int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	listed, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || listed.Mode()&fs.ModeSymlink != 0 || !os.SameFile(opened, listed) || opened.Size() != expected {
		return nil, errors.New("file identity or exact size changed")
	}
	data, err := io.ReadAll(io.LimitReader(file, expected+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != expected {
		return nil, errors.New("file length differs from declaration")
	}
	return data, nil
}

func decodeStrict(data []byte, target any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing token %v", token)
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("unexpected JSON delimiter")
	}
}

func decodeTrustedSignerPublicKey(text string) (ed25519.PublicKey, error) {
	decoded, err := decodeExactHex(text, ed25519.PublicKeySize)
	if err != nil {
		return nil, fmt.Errorf("trusted signer public key: %w", err)
	}
	return ed25519.PublicKey(decoded), nil
}

func decodeExactHex(text string, size int) ([]byte, error) {
	if len(text) != size*2 || strings.ToLower(text) != text {
		return nil, errors.New("value is not canonical lowercase hexadecimal")
	}
	decoded, err := hex.DecodeString(text)
	if err != nil || len(decoded) != size {
		return nil, errors.New("value is not canonical lowercase hexadecimal")
	}
	return decoded, nil
}

func validSHA256(value string) bool {
	_, err := decodeExactHex(value, sha256.Size)
	return err == nil
}

func decodeNormalizeHash(value string) (normalize.Hash, error) {
	decoded, err := decodeExactHex(value, sha256.Size)
	if err != nil {
		return normalize.Hash{}, err
	}
	var result normalize.Hash
	copy(result[:], decoded)
	return result, nil
}

func checkedSum(values ...uint64) (uint64, error) {
	var result uint64
	for _, value := range values {
		if value > ^uint64(0)-result {
			return 0, errors.New("numeric sum overflow")
		}
		result += value
	}
	return result, nil
}

func checkedMultiply(left, right uint64) (uint64, error) {
	high, low := bits.Mul64(left, right)
	if high != 0 {
		return 0, errors.New("numeric product overflow")
	}
	return low, nil
}
