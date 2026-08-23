package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	MapperEvidenceVersionV1          uint16 = 1
	DownstreamBookResultVersionV1    uint16 = 1
	MapperSelectionReceiveWallTimeNS        = "received_wall_time_ns"

	MaximumMapperEvidenceRecords     = normalize.MaxNormalizationBatch
	MaximumMapperEvidenceRows        = MaximumMapperEvidenceRecords * 4
	MaximumMapperCorpusBytes         = int64(320 << 20)
	MaximumDownstreamBookResultBytes = 4 << 10
)

var (
	ErrMapperEvidence = errors.New("replay: invalid mapper evidence")
	ErrDownstreamBook = errors.New("replay: downstream book replay failed")
)

// DownstreamBookResult is the bounded, deterministic result of replaying one
// normalized run through the caller's book implementation. Result identifies
// the terminal state (for example, applied or sequence_gap); LogicalHash binds
// the complete terminal book state owned by that implementation.
type DownstreamBookResult struct {
	Version     uint16
	Result      string
	LogicalHash normalize.Hash
}

// DownstreamBookReplay is defined at replay's consuming boundary. Implementors
// must evaluate only the supplied ordered rows and return a deterministic
// result; replay invokes no clock, network, storage, or other ambient input.
type DownstreamBookReplay interface {
	ReplayBook([]normalize.Row) (DownstreamBookResult, error)
}

type DownstreamBookReplayFunc func([]normalize.Row) (DownstreamBookResult, error)

func (f DownstreamBookReplayFunc) ReplayBook(rows []normalize.Row) (DownstreamBookResult, error) {
	return f(rows)
}

// MapperDualRunRequest binds both mapper runs and both downstream-book
// implementations to the same exact, ordered raw corpus.
type MapperDualRunRequest struct {
	Corpus        []normalize.RawRecord
	Baseline      *normalize.Orchestrator
	Candidate     *normalize.Orchestrator
	BaselineBook  DownstreamBookReplay
	CandidateBook DownstreamBookReplay
}

type MapperRecordIdentity struct {
	CorpusIndex        uint32
	ReceivedWallTimeNS int64
	Coordinate         normalize.RawCoordinate
}

// AcceptedFieldIdentity retains the complete release-bound identity of one
// accepted row, including EventID and mapper provenance. Dual-run equivalence
// uses the adjacent MapperAcceptedProjection instead.
type AcceptedFieldIdentity struct {
	Record          MapperRecordIdentity
	RowOrdinal      uint32
	EventKind       normalize.EventKind
	SchemaName      string
	SchemaVersion   uint16
	EventID         normalize.Hash
	MapperVersion   string
	MapperBindingID normalize.Hash
}

type MapperRejection struct {
	Record                  MapperRecordIdentity
	QuarantineID            normalize.Hash
	Code                    normalize.QuarantineCode
	Field                   string
	SourceState             normalize.SourceState
	FingerprintClass        normalize.FingerprintClass
	SourceSchemaFingerprint normalize.Hash
	MapperVersion           string
	MapperBindingID         normalize.Hash
}

type MapperRejectionCount struct {
	Code  normalize.QuarantineCode
	Count uint64
}

// MapperRunEvidence retains the exact ordered identities and hashes, plus the
// compact aggregate needed by immutable catalog publication evidence.
type MapperRunEvidence struct {
	AcceptedFields           []AcceptedFieldIdentity
	SemanticAcceptedFields   []MapperAcceptedProjection
	OrderedLogicalRowHashes  []normalize.Hash
	LogicalSHA256            normalize.Hash
	OrderedSemanticRowHashes []normalize.Hash
	SemanticSHA256           normalize.Hash
	Rejections               []MapperRejection
	RejectionProjections     []MapperRejectionProjection
	RejectionSHA256          normalize.Hash
	RejectionCounts          []MapperRejectionCount
	DownstreamBook           DownstreamBookResult
}

type MapperMismatchCode string

const (
	MismatchAcceptedFieldIdentity  MapperMismatchCode = "accepted_field_identity"
	MismatchRejectionProjection    MapperMismatchCode = "rejection_projection"
	MismatchRejectionCounts        MapperMismatchCode = "rejection_counts"
	MismatchOrderedSemanticRowHash MapperMismatchCode = "ordered_semantic_row_hash"
	MismatchDownstreamBookResult   MapperMismatchCode = "downstream_book_result"
	MismatchDownstreamBookHash     MapperMismatchCode = "downstream_book_hash"
)

type MapperMismatch struct {
	Code  MapperMismatchCode
	Index uint32
}

// MapperDualRunEvidenceV1 is immutable once returned: ReportSHA256 binds its
// deterministic binary form. Any mismatch makes the evidence non-publishable.
type MapperDualRunEvidenceV1 struct {
	Version            uint16
	SelectionTimeBasis string
	ReceivedStartNS    int64
	ReceivedEndNS      int64
	CorpusCount        uint32
	CorpusSHA256       normalize.Hash
	Baseline           MapperRunEvidence
	Candidate          MapperRunEvidence
	Mismatches         []MapperMismatch
	ReportSHA256       normalize.Hash
}

var _ encoding.BinaryMarshaler = MapperDualRunEvidenceV1{}

func CompareMapperRuns(request MapperDualRunRequest) (MapperDualRunEvidenceV1, error) {
	corpus, err := inspectMapperCorpus(request.Corpus)
	if err != nil {
		return MapperDualRunEvidenceV1{}, err
	}
	if request.Baseline == nil || request.Candidate == nil || request.BaselineBook == nil || request.CandidateBook == nil {
		return MapperDualRunEvidenceV1{}, fmt.Errorf("%w: both orchestrators and book replays are required", ErrMapperEvidence)
	}
	baseline, err := runMapperEvidence(request.Corpus, request.Baseline, request.BaselineBook)
	if err != nil {
		return MapperDualRunEvidenceV1{}, fmt.Errorf("%w: baseline: %v", ErrMapperEvidence, err)
	}
	candidate, err := runMapperEvidence(request.Corpus, request.Candidate, request.CandidateBook)
	if err != nil {
		return MapperDualRunEvidenceV1{}, fmt.Errorf("%w: candidate: %v", ErrMapperEvidence, err)
	}
	report := MapperDualRunEvidenceV1{
		Version: MapperEvidenceVersionV1, SelectionTimeBasis: MapperSelectionReceiveWallTimeNS,
		ReceivedStartNS: corpus.receivedStartNS, ReceivedEndNS: corpus.receivedEndNS,
		CorpusCount: uint32(len(request.Corpus)), CorpusSHA256: corpus.sha256,
		Baseline: baseline, Candidate: candidate,
		Mismatches: compareMapperEvidence(baseline, candidate),
	}
	body, err := report.canonicalBytes()
	if err != nil {
		return MapperDualRunEvidenceV1{}, err
	}
	report.ReportSHA256 = normalize.Hash(sha256.Sum256(body))
	return report, nil
}

func (r MapperDualRunEvidenceV1) Publishable() bool {
	return len(r.Mismatches) == 0 && r.Validate() == nil
}

func (r MapperDualRunEvidenceV1) Validate() error {
	if r.Version != MapperEvidenceVersionV1 || r.SelectionTimeBasis != MapperSelectionReceiveWallTimeNS ||
		r.CorpusCount == 0 || r.CorpusCount > MaximumMapperEvidenceRecords || r.CorpusSHA256 == (normalize.Hash{}) ||
		r.ReceivedStartNS < 0 || r.ReceivedEndNS < r.ReceivedStartNS {
		return fmt.Errorf("%w: dual-run identity or corpus bounds", ErrMapperEvidence)
	}
	if err := validateMapperRun(r.Baseline, r.CorpusCount); err != nil {
		return err
	}
	if err := validateMapperRun(r.Candidate, r.CorpusCount); err != nil {
		return err
	}
	for i, mismatch := range r.Mismatches {
		if !validMapperMismatchCode(mismatch.Code) || (i > 0 && mapperMismatchRank(r.Mismatches[i-1].Code) >= mapperMismatchRank(mismatch.Code)) {
			return fmt.Errorf("%w: unordered or unknown mismatch code", ErrMapperEvidence)
		}
	}
	if expected := compareMapperEvidence(r.Baseline, r.Candidate); !slices.Equal(r.Mismatches, expected) {
		return fmt.Errorf("%w: mismatch list disagrees with dual-run summaries", ErrMapperEvidence)
	}
	body, err := r.canonicalBytes()
	if err != nil {
		return err
	}
	if r.ReportSHA256 != normalize.Hash(sha256.Sum256(body)) {
		return fmt.Errorf("%w: dual-run report hash", ErrMapperEvidence)
	}
	return nil
}

func (r MapperDualRunEvidenceV1) MarshalBinary() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r.canonicalBytes()
}

// CatalogEvidence converts the exact replay report into catalog's immutable
// publication contract. A matching report is accepted; any replay mismatch is
// durably rejected and therefore cannot activate a mapper binding.
func (r MapperDualRunEvidenceV1) CatalogEvidence(sourceID, channelID, baselineReleaseID, candidateReleaseID string) (catalog.DualRunEvidence, error) {
	if err := r.Validate(); err != nil {
		return catalog.DualRunEvidence{}, err
	}
	evidence := catalog.DualRunEvidence{
		Version: catalog.MapperEvidenceVersion, SourceID: sourceID, ChannelID: channelID,
		SelectionTimeBasis: catalog.MapperSelectionReceivedWall,
		ReceivedStartNS:    r.ReceivedStartNS, ReceivedEndNS: r.ReceivedEndNS,
		CorpusCount: uint64(r.CorpusCount), CorpusSHA256: [32]byte(r.CorpusSHA256),
		OldMapperReleaseID: baselineReleaseID, NewMapperReleaseID: candidateReleaseID,
		Old: catalogMapperRunEvidence(r.Baseline), New: catalogMapperRunEvidence(r.Candidate),
		Mismatch: len(r.Mismatches) != 0,
	}
	evidence.MismatchCodes = make([]string, len(r.Mismatches))
	for i, mismatch := range r.Mismatches {
		evidence.MismatchCodes[i] = string(mismatch.Code)
	}
	slices.Sort(evidence.MismatchCodes)
	if evidence.Mismatch {
		evidence.Decision = catalog.MapperEvidenceRejected
	} else {
		evidence.Decision = catalog.MapperEvidenceAccepted
	}
	if err := evidence.Validate(); err != nil {
		return catalog.DualRunEvidence{}, fmt.Errorf("%w: catalog evidence: %v", ErrMapperEvidence, err)
	}
	return evidence, nil
}

func catalogMapperRunEvidence(run MapperRunEvidence) catalog.MapperRunEvidence {
	fields := make([]string, len(run.SemanticAcceptedFields))
	for i, accepted := range run.SemanticAcceptedFields {
		fields[i] = accepted.String()
	}
	slices.Sort(fields)
	rejections := make(map[string]uint64, len(run.RejectionCounts))
	for _, count := range run.RejectionCounts {
		rejections[string(count.Code)] = count.Count
	}
	rejectionDetails := make([]string, len(run.RejectionProjections))
	for i, projection := range run.RejectionProjections {
		rejectionDetails[i] = projection.String()
	}
	return catalog.MapperRunEvidence{
		AcceptedFields: fields, RejectionCounts: rejections,
		LogicalSHA256: [32]byte(run.LogicalSHA256), SemanticSHA256: [32]byte(run.SemanticSHA256),
		Rejections: rejectionDetails, RejectionSHA256: [32]byte(run.RejectionSHA256),
		DownstreamBookResult: run.DownstreamBook.Result, DownstreamBookSHA256: [32]byte(run.DownstreamBook.LogicalHash),
	}
}

type MapperCutoverSpec struct {
	EffectiveFromNS     int64
	BeforeMapperVersion string
	AfterMapperVersion  string
}

type MapperCutoverRequest struct {
	Corpus       []normalize.RawRecord
	Orchestrator *normalize.Orchestrator
	Book         DownstreamBookReplay
	Cutover      MapperCutoverSpec
}

type MapperCutoverCheck struct {
	Record                MapperRecordIdentity
	ExpectedMapperVersion string
	ObservedMapperVersion string
	AcceptedRows          uint32
	Rejected              bool
}

type MapperCutoverMismatchCode string

const (
	MismatchCutoverCoverage      MapperCutoverMismatchCode = "cutover_boundary_coverage"
	MismatchCutoverMapperVersion MapperCutoverMismatchCode = "cutover_mapper_version"
	MismatchCutoverOutcome       MapperCutoverMismatchCode = "cutover_record_outcome"
)

type MapperCutoverMismatch struct {
	Code        MapperCutoverMismatchCode
	HasRecord   bool
	CorpusIndex uint32
}

// MapperCutoverEvidenceV1 proves selection on a half-open receive-wall-time
// boundary. Exchange time is deliberately absent from every selection check.
type MapperCutoverEvidenceV1 struct {
	Version             uint16
	SelectionTimeBasis  string
	EffectiveFromNS     int64
	BeforeMapperVersion string
	AfterMapperVersion  string
	ReceivedStartNS     int64
	ReceivedEndNS       int64
	CorpusCount         uint32
	CorpusSHA256        normalize.Hash
	BeforeCount         uint32
	AtBoundaryCount     uint32
	AfterCount          uint32
	Run                 MapperRunEvidence
	Checks              []MapperCutoverCheck
	Mismatches          []MapperCutoverMismatch
	ReportSHA256        normalize.Hash
}

var _ encoding.BinaryMarshaler = MapperCutoverEvidenceV1{}

func ReplayMapperCutover(request MapperCutoverRequest) (MapperCutoverEvidenceV1, error) {
	corpus, err := inspectMapperCorpus(request.Corpus)
	if err != nil {
		return MapperCutoverEvidenceV1{}, err
	}
	if request.Orchestrator == nil || request.Book == nil || request.Cutover.EffectiveFromNS < 0 ||
		request.Cutover.BeforeMapperVersion == "" || request.Cutover.AfterMapperVersion == "" ||
		request.Cutover.BeforeMapperVersion == request.Cutover.AfterMapperVersion {
		return MapperCutoverEvidenceV1{}, fmt.Errorf("%w: cutover orchestrator, book, boundary, and distinct mapper versions are required", ErrMapperEvidence)
	}
	run, err := runMapperEvidence(request.Corpus, request.Orchestrator, request.Book)
	if err != nil {
		return MapperCutoverEvidenceV1{}, err
	}
	report := MapperCutoverEvidenceV1{
		Version: MapperEvidenceVersionV1, SelectionTimeBasis: MapperSelectionReceiveWallTimeNS,
		EffectiveFromNS:     request.Cutover.EffectiveFromNS,
		BeforeMapperVersion: request.Cutover.BeforeMapperVersion, AfterMapperVersion: request.Cutover.AfterMapperVersion,
		ReceivedStartNS: corpus.receivedStartNS, ReceivedEndNS: corpus.receivedEndNS,
		CorpusCount: uint32(len(request.Corpus)), CorpusSHA256: corpus.sha256, Run: run,
	}
	report.Checks, report.BeforeCount, report.AtBoundaryCount, report.AfterCount, report.Mismatches =
		checkMapperCutover(request.Corpus, run, request.Cutover)
	body, err := report.canonicalBytes()
	if err != nil {
		return MapperCutoverEvidenceV1{}, err
	}
	report.ReportSHA256 = normalize.Hash(sha256.Sum256(body))
	return report, nil
}

func (r MapperCutoverEvidenceV1) Publishable() bool {
	return len(r.Mismatches) == 0 && r.Validate() == nil
}

func (r MapperCutoverEvidenceV1) Validate() error {
	if r.Version != MapperEvidenceVersionV1 || r.SelectionTimeBasis != MapperSelectionReceiveWallTimeNS ||
		r.EffectiveFromNS < 0 || r.BeforeMapperVersion == "" || r.AfterMapperVersion == "" || r.BeforeMapperVersion == r.AfterMapperVersion ||
		r.CorpusCount == 0 || r.CorpusCount > MaximumMapperEvidenceRecords || r.CorpusSHA256 == (normalize.Hash{}) ||
		r.ReceivedStartNS < 0 || r.ReceivedEndNS < r.ReceivedStartNS ||
		r.BeforeCount+r.AtBoundaryCount+r.AfterCount != r.CorpusCount || len(r.Checks) != int(r.CorpusCount) {
		return fmt.Errorf("%w: cutover identity or corpus bounds", ErrMapperEvidence)
	}
	if err := validateMapperRun(r.Run, r.CorpusCount); err != nil {
		return err
	}
	var before, at, after uint32
	for i, check := range r.Checks {
		if check.Record.CorpusIndex != uint32(i) || check.AcceptedRows > 4 {
			return fmt.Errorf("%w: cutover checks are not in corpus order or bounds", ErrMapperEvidence)
		}
		expected := r.AfterMapperVersion
		switch {
		case check.Record.ReceivedWallTimeNS < r.EffectiveFromNS:
			expected = r.BeforeMapperVersion
			before++
		case check.Record.ReceivedWallTimeNS == r.EffectiveFromNS:
			at++
		default:
			after++
		}
		if check.ExpectedMapperVersion != expected {
			return fmt.Errorf("%w: cutover check expectation does not follow half-open receive time", ErrMapperEvidence)
		}
	}
	if before != r.BeforeCount || at != r.AtBoundaryCount || after != r.AfterCount {
		return fmt.Errorf("%w: cutover coverage counts disagree with checks", ErrMapperEvidence)
	}
	expectedMismatches := cutoverMismatchesFromChecks(r.Checks, before, at)
	if !slices.Equal(r.Mismatches, expectedMismatches) {
		return fmt.Errorf("%w: cutover mismatch list disagrees with checks", ErrMapperEvidence)
	}
	body, err := r.canonicalBytes()
	if err != nil {
		return err
	}
	if r.ReportSHA256 != normalize.Hash(sha256.Sum256(body)) {
		return fmt.Errorf("%w: cutover report hash", ErrMapperEvidence)
	}
	return nil
}

func (r MapperCutoverEvidenceV1) MarshalBinary() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r.canonicalBytes()
}

type mapperCorpusInspection struct {
	sha256          normalize.Hash
	receivedStartNS int64
	receivedEndNS   int64
}

func inspectMapperCorpus(records []normalize.RawRecord) (mapperCorpusInspection, error) {
	if len(records) == 0 || len(records) > MaximumMapperEvidenceRecords {
		return mapperCorpusInspection{}, fmt.Errorf("%w: corpus record count", ErrMapperEvidence)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("enable-marketdata/mapper-boundary-corpus/v1\x00"))
	var number [8]byte
	binary.BigEndian.PutUint32(number[:4], uint32(len(records)))
	_, _ = h.Write(number[:4])
	start, end := records[0].Envelope.ReceivedWallTimeNS, records[0].Envelope.ReceivedWallTimeNS
	var totalBytes int64
	for i, record := range records {
		if err := record.Validate(); err != nil {
			return mapperCorpusInspection{}, fmt.Errorf("%w: corpus record %d: %v", ErrMapperEvidence, i, err)
		}
		encoded, err := capture.MarshalEnvelopeV1(record.Envelope)
		if err != nil {
			return mapperCorpusInspection{}, fmt.Errorf("%w: encode corpus record %d: %v", ErrMapperEvidence, i, err)
		}
		totalBytes += int64(len(encoded))
		if totalBytes > MaximumMapperCorpusBytes {
			return mapperCorpusInspection{}, fmt.Errorf("%w: corpus bytes exceed %d", ErrMapperEvidence, MaximumMapperCorpusBytes)
		}
		writeHashBytes(h, encoded, &number)
		writeRawCoordinateHash(h, record.Coordinate, &number)
		binary.BigEndian.PutUint32(number[:4], uint32(len(record.QualityFlags)))
		_, _ = h.Write(number[:4])
		for _, flag := range record.QualityFlags {
			writeHashBytes(h, []byte(flag), &number)
		}
		start = min(start, record.Envelope.ReceivedWallTimeNS)
		end = max(end, record.Envelope.ReceivedWallTimeNS)
	}
	var digest normalize.Hash
	copy(digest[:], h.Sum(nil))
	return mapperCorpusInspection{sha256: digest, receivedStartNS: start, receivedEndNS: end}, nil
}

func runMapperEvidence(records []normalize.RawRecord, orchestrator *normalize.Orchestrator, book DownstreamBookReplay) (MapperRunEvidence, error) {
	batch, err := orchestrator.Normalize(records)
	if err != nil {
		return MapperRunEvidence{}, err
	}
	if len(batch.Rows) > MaximumMapperEvidenceRows || len(batch.Quarantines) > MaximumMapperEvidenceRecords {
		return MapperRunEvidence{}, fmt.Errorf("%w: normalized output bounds", ErrMapperEvidence)
	}
	indexByCoordinate := make(map[normalize.RawCoordinate]uint32, len(records))
	for i, record := range records {
		if _, exists := indexByCoordinate[record.Coordinate]; exists {
			return MapperRunEvidence{}, fmt.Errorf("%w: duplicate raw coordinate", ErrMapperEvidence)
		}
		indexByCoordinate[record.Coordinate] = uint32(i)
	}
	run := MapperRunEvidence{
		AcceptedFields:           make([]AcceptedFieldIdentity, 0, len(batch.Rows)),
		SemanticAcceptedFields:   make([]MapperAcceptedProjection, 0, len(batch.Rows)),
		OrderedLogicalRowHashes:  make([]normalize.Hash, 0, len(batch.Rows)),
		OrderedSemanticRowHashes: make([]normalize.Hash, 0, len(batch.Rows)),
		Rejections:               make([]MapperRejection, 0, len(batch.Quarantines)),
		RejectionProjections:     make([]MapperRejectionProjection, 0, len(batch.Quarantines)),
	}
	rowOrdinal := make(map[uint32]uint32)
	for outputIndex, row := range batch.Rows {
		if err := row.Validate(); err != nil {
			return MapperRunEvidence{}, fmt.Errorf("%w: invalid normalized row %d: %v", ErrMapperEvidence, outputIndex, err)
		}
		metadata := row.Common()
		corpusIndex, found := indexByCoordinate[metadataCoordinate(metadata)]
		if !found {
			return MapperRunEvidence{}, fmt.Errorf("%w: normalized row has no corpus coordinate", ErrMapperEvidence)
		}
		ordinal := rowOrdinal[corpusIndex]
		rowOrdinal[corpusIndex] = ordinal + 1
		run.AcceptedFields = append(run.AcceptedFields, AcceptedFieldIdentity{
			Record:     MapperRecordIdentity{CorpusIndex: corpusIndex, ReceivedWallTimeNS: metadata.ReceivedTimeNS, Coordinate: metadataCoordinate(metadata)},
			RowOrdinal: ordinal, EventKind: row.Kind, SchemaName: metadata.SchemaName, SchemaVersion: metadata.SchemaVersion,
			EventID: metadata.EventID, MapperVersion: metadata.MapperVersion, MapperBindingID: metadata.MapperBindingID,
		})
		run.SemanticAcceptedFields = append(run.SemanticAcceptedFields, MapperAcceptedProjection{
			Record:     MapperRecordIdentity{CorpusIndex: corpusIndex, ReceivedWallTimeNS: metadata.ReceivedTimeNS, Coordinate: metadataCoordinate(metadata)},
			RowOrdinal: ordinal, EventKind: row.Kind, SchemaName: metadata.SchemaName, SchemaVersion: metadata.SchemaVersion,
			SemanticEventID: semanticEventID(metadata, row.Kind),
		})
		semanticHash, err := semanticRowHash(row)
		if err != nil {
			return MapperRunEvidence{}, fmt.Errorf("%w: semantic normalized row %d: %v", ErrMapperEvidence, outputIndex, err)
		}
		run.OrderedSemanticRowHashes = append(run.OrderedSemanticRowHashes, semanticHash)
		run.OrderedLogicalRowHashes = append(run.OrderedLogicalRowHashes, row.LogicalHash)
	}
	counts := make(map[normalize.QuarantineCode]uint64)
	for _, rejection := range batch.Quarantines {
		corpusIndex, found := indexByCoordinate[rejection.Coordinate]
		if !found {
			return MapperRunEvidence{}, fmt.Errorf("%w: quarantine has no corpus coordinate", ErrMapperEvidence)
		}
		run.Rejections = append(run.Rejections, MapperRejection{
			Record:       MapperRecordIdentity{CorpusIndex: corpusIndex, ReceivedWallTimeNS: rejection.ReceivedTimeNS, Coordinate: rejection.Coordinate},
			QuarantineID: rejection.QuarantineID, Code: rejection.Code, Field: rejection.Field, SourceState: rejection.SourceState,
			FingerprintClass: rejection.FingerprintClass, SourceSchemaFingerprint: rejection.SourceSchemaFingerprint,
			MapperVersion: rejection.MapperVersion, MapperBindingID: rejection.MapperBindingID,
		})
		run.RejectionProjections = append(run.RejectionProjections, MapperRejectionProjection{
			Record: MapperRecordIdentity{CorpusIndex: corpusIndex, ReceivedWallTimeNS: rejection.ReceivedTimeNS, Coordinate: rejection.Coordinate},
			Code:   rejection.Code, Field: rejection.Field, SourceState: rejection.SourceState,
			FingerprintClass: rejection.FingerprintClass, SourceSchemaFingerprint: rejection.SourceSchemaFingerprint,
		})
		counts[rejection.Code]++
	}
	codes := slices.Sorted(maps.Keys(counts))
	for _, code := range codes {
		run.RejectionCounts = append(run.RejectionCounts, MapperRejectionCount{Code: code, Count: counts[code]})
	}
	run.LogicalSHA256 = orderedLogicalHash(run.OrderedLogicalRowHashes)
	run.SemanticSHA256 = orderedSemanticHash(run.OrderedSemanticRowHashes)
	run.RejectionSHA256 = orderedRejectionHash(run.RejectionProjections)
	bookResult, err := book.ReplayBook(slices.Clone(batch.Rows))
	if err != nil {
		return MapperRunEvidence{}, fmt.Errorf("%w: %v", ErrDownstreamBook, err)
	}
	if err := validateBookResult(bookResult); err != nil {
		return MapperRunEvidence{}, err
	}
	run.DownstreamBook = bookResult
	return run, nil
}

func compareMapperEvidence(baseline, candidate MapperRunEvidence) []MapperMismatch {
	mismatches := make([]MapperMismatch, 0, 6)
	if index, equal := firstDifferent(baseline.SemanticAcceptedFields, candidate.SemanticAcceptedFields); !equal {
		mismatches = append(mismatches, MapperMismatch{Code: MismatchAcceptedFieldIdentity, Index: index})
	}
	if index, equal := firstDifferent(baseline.RejectionProjections, candidate.RejectionProjections); !equal {
		mismatches = append(mismatches, MapperMismatch{Code: MismatchRejectionProjection, Index: index})
	}
	if index, equal := firstDifferent(baseline.RejectionCounts, candidate.RejectionCounts); !equal {
		mismatches = append(mismatches, MapperMismatch{Code: MismatchRejectionCounts, Index: index})
	}
	if index, equal := firstDifferent(baseline.OrderedSemanticRowHashes, candidate.OrderedSemanticRowHashes); !equal {
		mismatches = append(mismatches, MapperMismatch{Code: MismatchOrderedSemanticRowHash, Index: index})
	}
	if baseline.DownstreamBook.Version != candidate.DownstreamBook.Version || baseline.DownstreamBook.Result != candidate.DownstreamBook.Result {
		mismatches = append(mismatches, MapperMismatch{Code: MismatchDownstreamBookResult})
	}
	if baseline.DownstreamBook.LogicalHash != candidate.DownstreamBook.LogicalHash {
		mismatches = append(mismatches, MapperMismatch{Code: MismatchDownstreamBookHash})
	}
	return mismatches
}

func firstDifferent[T comparable](left, right []T) (uint32, bool) {
	common := min(len(left), len(right))
	for i := range common {
		if left[i] != right[i] {
			return uint32(i), false
		}
	}
	if len(left) != len(right) {
		return uint32(common), false
	}
	return 0, true
}

type mapperCutoverOutcome struct {
	mapperVersion string
	acceptedRows  uint32
	rejected      bool
	conflict      bool
}

func checkMapperCutover(records []normalize.RawRecord, run MapperRunEvidence, spec MapperCutoverSpec) ([]MapperCutoverCheck, uint32, uint32, uint32, []MapperCutoverMismatch) {
	outcomes := make([]mapperCutoverOutcome, len(records))
	setVersion := func(index uint32, version string) {
		outcome := &outcomes[index]
		if outcome.mapperVersion != "" && outcome.mapperVersion != version {
			outcome.conflict = true
		}
		if outcome.mapperVersion == "" {
			outcome.mapperVersion = version
		}
	}
	for _, accepted := range run.AcceptedFields {
		outcomes[accepted.Record.CorpusIndex].acceptedRows++
		setVersion(accepted.Record.CorpusIndex, accepted.MapperVersion)
	}
	for _, rejected := range run.Rejections {
		outcomes[rejected.Record.CorpusIndex].rejected = true
		setVersion(rejected.Record.CorpusIndex, rejected.MapperVersion)
	}
	checks := make([]MapperCutoverCheck, len(records))
	var before, at, after uint32
	for i, record := range records {
		received := record.Envelope.ReceivedWallTimeNS
		expected := spec.AfterMapperVersion
		switch {
		case received < spec.EffectiveFromNS:
			expected = spec.BeforeMapperVersion
			before++
		case received == spec.EffectiveFromNS:
			at++
		default:
			after++
		}
		outcome := outcomes[i]
		observed := outcome.mapperVersion
		if outcome.conflict {
			observed = ""
		}
		checks[i] = MapperCutoverCheck{
			Record:                MapperRecordIdentity{CorpusIndex: uint32(i), ReceivedWallTimeNS: received, Coordinate: record.Coordinate},
			ExpectedMapperVersion: expected, ObservedMapperVersion: observed,
			AcceptedRows: outcome.acceptedRows, Rejected: outcome.rejected,
		}
	}
	return checks, before, at, after, cutoverMismatchesFromChecks(checks, before, at)
}

func cutoverMismatchesFromChecks(checks []MapperCutoverCheck, before, at uint32) []MapperCutoverMismatch {
	mismatches := make([]MapperCutoverMismatch, 0)
	for i, check := range checks {
		if (check.AcceptedRows > 0) == check.Rejected {
			mismatches = append(mismatches, MapperCutoverMismatch{Code: MismatchCutoverOutcome, HasRecord: true, CorpusIndex: uint32(i)})
		}
		if check.ObservedMapperVersion != check.ExpectedMapperVersion {
			mismatches = append(mismatches, MapperCutoverMismatch{Code: MismatchCutoverMapperVersion, HasRecord: true, CorpusIndex: uint32(i)})
		}
	}
	if before == 0 || at == 0 {
		mismatches = append(mismatches, MapperCutoverMismatch{Code: MismatchCutoverCoverage})
	}
	slices.SortFunc(mismatches, compareCutoverMismatch)
	return mismatches
}

func compareCutoverMismatch(left, right MapperCutoverMismatch) int {
	if left.Code < right.Code {
		return -1
	}
	if left.Code > right.Code {
		return 1
	}
	if !left.HasRecord && right.HasRecord {
		return -1
	}
	if left.HasRecord && !right.HasRecord {
		return 1
	}
	if left.CorpusIndex < right.CorpusIndex {
		return -1
	}
	if left.CorpusIndex > right.CorpusIndex {
		return 1
	}
	return 0
}

func validateMapperRun(run MapperRunEvidence, corpusCount uint32) error {
	if len(run.AcceptedFields) != len(run.SemanticAcceptedFields) ||
		len(run.AcceptedFields) != len(run.OrderedLogicalRowHashes) ||
		len(run.AcceptedFields) != len(run.OrderedSemanticRowHashes) ||
		len(run.AcceptedFields) > MaximumMapperEvidenceRows ||
		len(run.Rejections) != len(run.RejectionProjections) ||
		len(run.Rejections) > MaximumMapperEvidenceRecords {
		return fmt.Errorf("%w: mapper output counts", ErrMapperEvidence)
	}
	for i, accepted := range run.AcceptedFields {
		projection := run.SemanticAcceptedFields[i]
		if accepted.Record.CorpusIndex >= corpusCount || accepted.EventKind == "" || accepted.SchemaName == "" || accepted.SchemaVersion == 0 ||
			accepted.EventID == (normalize.Hash{}) || accepted.MapperVersion == "" || accepted.MapperBindingID == (normalize.Hash{}) ||
			run.OrderedLogicalRowHashes[i] == (normalize.Hash{}) || run.OrderedSemanticRowHashes[i] == (normalize.Hash{}) ||
			projection.Record != accepted.Record || projection.RowOrdinal != accepted.RowOrdinal ||
			projection.EventKind != accepted.EventKind || projection.SchemaName != accepted.SchemaName ||
			projection.SchemaVersion != accepted.SchemaVersion || projection.SemanticEventID == (normalize.Hash{}) {
			return fmt.Errorf("%w: accepted field identity", ErrMapperEvidence)
		}
	}
	if run.LogicalSHA256 != orderedLogicalHash(run.OrderedLogicalRowHashes) {
		return fmt.Errorf("%w: ordered logical hash aggregate", ErrMapperEvidence)
	}
	if run.SemanticSHA256 != orderedSemanticHash(run.OrderedSemanticRowHashes) {
		return fmt.Errorf("%w: ordered semantic hash aggregate", ErrMapperEvidence)
	}
	counts := make(map[normalize.QuarantineCode]uint64)
	for i, rejection := range run.Rejections {
		projection := run.RejectionProjections[i]
		if rejection.Record.CorpusIndex >= corpusCount || rejection.QuarantineID == (normalize.Hash{}) || rejection.Code == "" ||
			projection.Record != rejection.Record || projection.Code != rejection.Code || projection.Field != rejection.Field ||
			projection.SourceState != rejection.SourceState || projection.FingerprintClass != rejection.FingerprintClass ||
			projection.SourceSchemaFingerprint != rejection.SourceSchemaFingerprint {
			return fmt.Errorf("%w: rejection identity", ErrMapperEvidence)
		}
		counts[rejection.Code]++
	}
	if run.RejectionSHA256 != orderedRejectionHash(run.RejectionProjections) {
		return fmt.Errorf("%w: ordered rejection hash aggregate", ErrMapperEvidence)
	}
	codes := slices.Sorted(maps.Keys(counts))
	if len(codes) != len(run.RejectionCounts) {
		return fmt.Errorf("%w: rejection code counts", ErrMapperEvidence)
	}
	for i, code := range codes {
		if run.RejectionCounts[i] != (MapperRejectionCount{Code: code, Count: counts[code]}) {
			return fmt.Errorf("%w: rejection code counts", ErrMapperEvidence)
		}
	}
	return validateBookResult(run.DownstreamBook)
}

func validateBookResult(result DownstreamBookResult) error {
	if result.Version != DownstreamBookResultVersionV1 || result.Result == "" || len(result.Result) > MaximumDownstreamBookResultBytes ||
		strings.IndexByte(result.Result, 0) >= 0 || result.LogicalHash == (normalize.Hash{}) {
		return fmt.Errorf("%w: invalid bounded result", ErrDownstreamBook)
	}
	return nil
}

func orderedLogicalHash(hashes []normalize.Hash) normalize.Hash {
	h := sha256.New()
	_, _ = h.Write([]byte("enable-marketdata/mapper-ordered-logical-rows/v1\x00"))
	var number [4]byte
	binary.BigEndian.PutUint32(number[:], uint32(len(hashes)))
	_, _ = h.Write(number[:])
	for _, value := range hashes {
		_, _ = h.Write(value[:])
	}
	var result normalize.Hash
	copy(result[:], h.Sum(nil))
	return result
}

func orderedSemanticHash(hashes []normalize.Hash) normalize.Hash {
	h := sha256.New()
	_, _ = h.Write([]byte("enable-marketdata/mapper-ordered-semantic-rows/v1\x00"))
	var number [4]byte
	binary.BigEndian.PutUint32(number[:], uint32(len(hashes)))
	_, _ = h.Write(number[:])
	for _, value := range hashes {
		_, _ = h.Write(value[:])
	}
	var result normalize.Hash
	copy(result[:], h.Sum(nil))
	return result
}

func metadataCoordinate(metadata normalize.Metadata) normalize.RawCoordinate {
	return normalize.RawCoordinate{
		SourceID: metadata.SourceID, ChannelID: metadata.ChannelID, EpochKind: metadata.EpochKind, EpochID: metadata.EpochID,
		ArrivalOrdinal: metadata.ArrivalOrdinal, MessageOrdinal: metadata.MessageOrdinal,
		RawSegmentSHA256: metadata.RawSegmentSHA256, RawRecordOrdinal: metadata.RawRecordOrdinal,
		RawPayloadSHA256: metadata.RawPayloadSHA256,
	}
}

func validMapperMismatchCode(code MapperMismatchCode) bool {
	return mapperMismatchRank(code) >= 0
}

func mapperMismatchRank(code MapperMismatchCode) int {
	switch code {
	case MismatchAcceptedFieldIdentity:
		return 0
	case MismatchRejectionProjection:
		return 1
	case MismatchRejectionCounts:
		return 2
	case MismatchOrderedSemanticRowHash:
		return 3
	case MismatchDownstreamBookResult:
		return 4
	case MismatchDownstreamBookHash:
		return 5
	default:
		return -1
	}
}

type mapperEvidenceEncoder struct {
	bytes []byte
	err   error
}

func (e *mapperEvidenceEncoder) u8(value uint8) { e.bytes = append(e.bytes, value) }
func (e *mapperEvidenceEncoder) bool(value bool) {
	if value {
		e.u8(1)
		return
	}
	e.u8(0)
}
func (e *mapperEvidenceEncoder) u16(value uint16) {
	e.bytes = binary.BigEndian.AppendUint16(e.bytes, value)
}
func (e *mapperEvidenceEncoder) u32(value uint32) {
	e.bytes = binary.BigEndian.AppendUint32(e.bytes, value)
}
func (e *mapperEvidenceEncoder) u64(value uint64) {
	e.bytes = binary.BigEndian.AppendUint64(e.bytes, value)
}
func (e *mapperEvidenceEncoder) i64(value int64)           { e.u64(uint64(value)) }
func (e *mapperEvidenceEncoder) hash(value normalize.Hash) { e.bytes = append(e.bytes, value[:]...) }
func (e *mapperEvidenceEncoder) epoch(value [16]byte)      { e.bytes = append(e.bytes, value[:]...) }
func (e *mapperEvidenceEncoder) string(value string) {
	if e.err != nil {
		return
	}
	if uint64(len(value)) > uint64(^uint32(0)) {
		e.err = fmt.Errorf("%w: canonical string bound", ErrMapperEvidence)
		return
	}
	e.u32(uint32(len(value)))
	e.bytes = append(e.bytes, value...)
}

func (e *mapperEvidenceEncoder) record(value MapperRecordIdentity) {
	e.u32(value.CorpusIndex)
	e.i64(value.ReceivedWallTimeNS)
	e.coordinate(value.Coordinate)
}

func (e *mapperEvidenceEncoder) coordinate(value normalize.RawCoordinate) {
	e.string(value.SourceID)
	e.string(value.ChannelID)
	e.string(string(value.EpochKind))
	e.epoch(value.EpochID)
	e.u64(value.ArrivalOrdinal)
	e.u32(value.MessageOrdinal)
	e.hash(value.RawSegmentSHA256)
	e.u64(value.RawRecordOrdinal)
	e.hash(value.RawPayloadSHA256)
}

func (e *mapperEvidenceEncoder) book(value DownstreamBookResult) {
	e.u16(value.Version)
	e.string(value.Result)
	e.hash(value.LogicalHash)
}

func (e *mapperEvidenceEncoder) run(value MapperRunEvidence) {
	e.u32(uint32(len(value.AcceptedFields)))
	for _, accepted := range value.AcceptedFields {
		e.record(accepted.Record)
		e.u32(accepted.RowOrdinal)
		e.string(string(accepted.EventKind))
		e.string(accepted.SchemaName)
		e.u16(accepted.SchemaVersion)
		e.hash(accepted.EventID)
		e.string(accepted.MapperVersion)
		e.hash(accepted.MapperBindingID)
	}
	e.u32(uint32(len(value.SemanticAcceptedFields)))
	for _, accepted := range value.SemanticAcceptedFields {
		e.record(accepted.Record)
		e.u32(accepted.RowOrdinal)
		e.string(string(accepted.EventKind))
		e.string(accepted.SchemaName)
		e.u16(accepted.SchemaVersion)
		e.hash(accepted.SemanticEventID)
	}
	e.u32(uint32(len(value.OrderedLogicalRowHashes)))
	for _, hash := range value.OrderedLogicalRowHashes {
		e.hash(hash)
	}
	e.hash(value.LogicalSHA256)
	e.u32(uint32(len(value.OrderedSemanticRowHashes)))
	for _, hash := range value.OrderedSemanticRowHashes {
		e.hash(hash)
	}
	e.hash(value.SemanticSHA256)
	e.u32(uint32(len(value.Rejections)))
	for _, rejection := range value.Rejections {
		e.record(rejection.Record)
		e.hash(rejection.QuarantineID)
		e.string(string(rejection.Code))
		e.string(rejection.Field)
		e.string(string(rejection.SourceState))
		e.string(string(rejection.FingerprintClass))
		e.hash(rejection.SourceSchemaFingerprint)
		e.string(rejection.MapperVersion)
		e.hash(rejection.MapperBindingID)
	}
	e.u32(uint32(len(value.RejectionProjections)))
	for _, rejection := range value.RejectionProjections {
		e.record(rejection.Record)
		e.string(string(rejection.Code))
		e.string(rejection.Field)
		e.string(string(rejection.SourceState))
		e.string(string(rejection.FingerprintClass))
		e.hash(rejection.SourceSchemaFingerprint)
	}
	e.hash(value.RejectionSHA256)
	e.u32(uint32(len(value.RejectionCounts)))
	for _, count := range value.RejectionCounts {
		e.string(string(count.Code))
		e.u64(count.Count)
	}
	e.book(value.DownstreamBook)
}

func (r MapperDualRunEvidenceV1) canonicalBytes() ([]byte, error) {
	var e mapperEvidenceEncoder
	e.string("enable-marketdata/mapper-dual-run-evidence/v1")
	e.u16(r.Version)
	e.string(r.SelectionTimeBasis)
	e.i64(r.ReceivedStartNS)
	e.i64(r.ReceivedEndNS)
	e.u32(r.CorpusCount)
	e.hash(r.CorpusSHA256)
	e.run(r.Baseline)
	e.run(r.Candidate)
	e.u32(uint32(len(r.Mismatches)))
	for _, mismatch := range r.Mismatches {
		e.string(string(mismatch.Code))
		e.u32(mismatch.Index)
	}
	return bytes.Clone(e.bytes), e.err
}

func (r MapperCutoverEvidenceV1) canonicalBytes() ([]byte, error) {
	var e mapperEvidenceEncoder
	e.string("enable-marketdata/mapper-cutover-evidence/v1")
	e.u16(r.Version)
	e.string(r.SelectionTimeBasis)
	e.i64(r.EffectiveFromNS)
	e.string(r.BeforeMapperVersion)
	e.string(r.AfterMapperVersion)
	e.i64(r.ReceivedStartNS)
	e.i64(r.ReceivedEndNS)
	e.u32(r.CorpusCount)
	e.hash(r.CorpusSHA256)
	e.u32(r.BeforeCount)
	e.u32(r.AtBoundaryCount)
	e.u32(r.AfterCount)
	e.run(r.Run)
	e.u32(uint32(len(r.Checks)))
	for _, check := range r.Checks {
		e.record(check.Record)
		e.string(check.ExpectedMapperVersion)
		e.string(check.ObservedMapperVersion)
		e.u32(check.AcceptedRows)
		e.bool(check.Rejected)
	}
	e.u32(uint32(len(r.Mismatches)))
	for _, mismatch := range r.Mismatches {
		e.string(string(mismatch.Code))
		e.bool(mismatch.HasRecord)
		e.u32(mismatch.CorpusIndex)
	}
	return bytes.Clone(e.bytes), e.err
}

func writeHashBytes(h interface{ Write([]byte) (int, error) }, value []byte, number *[8]byte) {
	binary.BigEndian.PutUint32(number[:4], uint32(len(value)))
	_, _ = h.Write(number[:4])
	_, _ = h.Write(value)
}

func writeRawCoordinateHash(h interface{ Write([]byte) (int, error) }, value normalize.RawCoordinate, number *[8]byte) {
	writeHashBytes(h, []byte(value.SourceID), number)
	writeHashBytes(h, []byte(value.ChannelID), number)
	writeHashBytes(h, []byte(value.EpochKind), number)
	_, _ = h.Write(value.EpochID[:])
	binary.BigEndian.PutUint64(number[:], value.ArrivalOrdinal)
	_, _ = h.Write(number[:])
	binary.BigEndian.PutUint32(number[:4], value.MessageOrdinal)
	_, _ = h.Write(number[:4])
	_, _ = h.Write(value.RawSegmentSHA256[:])
	binary.BigEndian.PutUint64(number[:], value.RawRecordOrdinal)
	_, _ = h.Write(number[:])
	_, _ = h.Write(value.RawPayloadSHA256[:])
}
