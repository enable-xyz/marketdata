package quality

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	ReleaseGateFormat = "enable-market-release-gate/v1"

	MinimumSustainedDurationNS = int64(60 * 60 * 1_000_000_000)
	MinimumBurstDurationNS     = int64(30 * 1_000_000_000)
	MinimumCanaryDurationNS    = int64(26 * 60 * 60 * 1_000_000_000)

	MaxReleaseGateContracts       = 256
	MaxReleaseGateQueryThresholds = 1024
	MaxReleaseGateDurationNS      = int64(31 * 24 * 60 * 60 * 1_000_000_000)
	MaxReleaseGateRate            = uint64(1 << 56)
	MaxReleaseGateCount           = uint64(1 << 60)
	MaxReleaseGateBytes           = uint64(1 << 60)
	MaxReleaseGateWorkers         = uint32(1 << 20)
	MaxReleaseGateTextBytes       = 512
)

var (
	ErrInvalidReleaseGate = errors.New("quality: invalid release gate report")
	ErrReleaseGateFailed  = errors.New("quality: release gate failed")
)

type CanaryRequirement string

const (
	CanaryRequired    CanaryRequirement = "required"
	CanaryNotRequired CanaryRequirement = "not_required"
)

type CanaryStatus string

const (
	CanaryPassedStatus      CanaryStatus = "passed"
	CanaryNotRequiredStatus CanaryStatus = "not_required"
)

type PayloadClass string

const (
	PayloadTiny   PayloadClass = "tiny"
	PayloadMedian PayloadClass = "median"
	PayloadMax    PayloadClass = "max"
)

type FaultCategoryID string

const (
	FaultSubscriptionSuccess           FaultCategoryID = "subscription.success"
	FaultSubscriptionFailure           FaultCategoryID = "subscription.failure"
	FaultSubscriptionPartialACK        FaultCategoryID = "subscription.partial_acknowledgement"
	FaultSubscriptionTimeout           FaultCategoryID = "subscription.timeout"
	FaultHeartbeatOnlySocket           FaultCategoryID = "heartbeat.only_socket"
	FaultHeartbeatMissedResponse       FaultCategoryID = "heartbeat.missed_pong_or_test_response"
	FaultDisconnectPlanned             FaultCategoryID = "disconnect.planned"
	FaultDisconnectAbrupt              FaultCategoryID = "disconnect.abrupt"
	FaultDNSFailure                    FaultCategoryID = "transport.dns_failure"
	FaultTLSFailure                    FaultCategoryID = "transport.tls_failure"
	FaultConnectFailure                FaultCategoryID = "transport.connect_failure"
	FaultHTTP429                       FaultCategoryID = "transport.http_429"
	FaultHTTP403                       FaultCategoryID = "transport.http_403"
	FaultHTTP418                       FaultCategoryID = "transport.http_418"
	FaultHTTP5xx                       FaultCategoryID = "transport.http_5xx"
	FaultConnectionBudgetExhaustion    FaultCategoryID = "budget.connection_exhaustion"
	FaultRateBudgetExhaustion          FaultCategoryID = "budget.rate_exhaustion"
	FaultPayloadDuplicate              FaultCategoryID = "payload.duplicate"
	FaultPayloadStale                  FaultCategoryID = "payload.stale"
	FaultPayloadEqualTime              FaultCategoryID = "payload.equal_time"
	FaultPayloadRegressingTime         FaultCategoryID = "payload.regressing_time"
	FaultPayloadMalformed              FaultCategoryID = "payload.malformed"
	FaultPayloadUnknownRole            FaultCategoryID = "payload.unknown_role"
	FaultPayloadAdditiveField          FaultCategoryID = "payload.additive_field"
	FaultPayloadTypeChanged            FaultCategoryID = "payload.type_changed"
	FaultPayloadOversized              FaultCategoryID = "payload.oversized"
	FaultAdversarialRandomEpochID      FaultCategoryID = "epoch.adversarial_random_id"
	FaultWallClockRegression           FaultCategoryID = "clock.wall_regression"
	FaultBookMissingUpdate             FaultCategoryID = "book.missing_update"
	FaultBookReorderedUpdate           FaultCategoryID = "book.reordered_update"
	FaultBookDuplicateUpdate           FaultCategoryID = "book.duplicate_update"
	FaultBookSnapshotRace              FaultCategoryID = "book.snapshot_race"
	FaultBookBufferOverflow            FaultCategoryID = "book.buffer_overflow"
	FaultBookSequenceException         FaultCategoryID = "book.sequence_exception"
	FaultBookChecksumException         FaultCategoryID = "book.checksum_exception"
	FaultBookTrueGap                   FaultCategoryID = "book.true_gap"
	FaultKillFrameClose                FaultCategoryID = "process_kill.frame_close"
	FaultKillSegmentClose              FaultCategoryID = "process_kill.segment_close"
	FaultKillUpload                    FaultCategoryID = "process_kill.upload"
	FaultKillVerify                    FaultCategoryID = "process_kill.verify"
	FaultKillManifest                  FaultCategoryID = "process_kill.manifest"
	FaultKillCatalogCommit             FaultCategoryID = "process_kill.catalog_commit"
	FaultKillParquetFooter             FaultCategoryID = "process_kill.parquet_footer"
	FaultKillClickHouseAcknowledgement FaultCategoryID = "process_kill.clickhouse_acknowledgement"
	FaultFramePartial                  FaultCategoryID = "corruption.frame_partial"
	FaultFrameTruncated                FaultCategoryID = "corruption.frame_truncated"
	FaultFrameCorrupted                FaultCategoryID = "corruption.frame_corrupted"
	FaultObjectPartial                 FaultCategoryID = "corruption.object_partial"
	FaultObjectTruncated               FaultCategoryID = "corruption.object_truncated"
	FaultObjectCorrupted               FaultCategoryID = "corruption.object_corrupted"
	FaultParquetPagePartial            FaultCategoryID = "corruption.parquet_page_partial"
	FaultParquetPageTruncated          FaultCategoryID = "corruption.parquet_page_truncated"
	FaultParquetPageCorrupted          FaultCategoryID = "corruption.parquet_page_corrupted"
	FaultWriterQueueFull               FaultCategoryID = "pressure.writer_queue_full"
	FaultSpoolFull                     FaultCategoryID = "pressure.spool_full"
	FaultObjectStoreSlow               FaultCategoryID = "slow.object_store"
	FaultPostgreSQLSlow                FaultCategoryID = "slow.postgresql"
	FaultClickHouseSlow                FaultCategoryID = "slow.clickhouse"
	FaultReplayClientSlow              FaultCategoryID = "slow.replay_client"
	FaultCatalogPagination             FaultCategoryID = "catalog.pagination"
	FaultCatalogSymbolReuse            FaultCategoryID = "catalog.symbol_reuse"
	FaultCatalogListing                FaultCategoryID = "catalog.listing"
	FaultCatalogAuction                FaultCategoryID = "catalog.auction"
	FaultCatalogExpiry                 FaultCategoryID = "catalog.expiry"
	FaultCatalogDelisting              FaultCategoryID = "catalog.delisting"
	FaultCatalogRecycle                FaultCategoryID = "catalog.recycle"
	FaultCatalogChangedMultiplier      FaultCategoryID = "catalog.changed_multiplier"
	FaultCatalogChangedTick            FaultCategoryID = "catalog.changed_tick"
	FaultCatalogChangedLot             FaultCategoryID = "catalog.changed_lot"
	FaultCatalogUnresolvedUnit         FaultCategoryID = "catalog.unresolved_unit"
	FaultMapperDualRunMismatch         FaultCategoryID = "mapper.dual_run_mismatch"
	FaultMapperEffectiveBoundaryReplay FaultCategoryID = "mapper.effective_boundary_replay"
	FaultBackupRestore                 FaultCategoryID = "recovery.backup_restore"
	FaultOrphanReconciliation          FaultCategoryID = "recovery.orphan_reconciliation"
	FaultFullClickHouseRebuild         FaultCategoryID = "recovery.full_clickhouse_rebuild"
)

var requiredFaultCategoryIDs = []FaultCategoryID{
	FaultSubscriptionSuccess,
	FaultSubscriptionFailure,
	FaultSubscriptionPartialACK,
	FaultSubscriptionTimeout,
	FaultHeartbeatOnlySocket,
	FaultHeartbeatMissedResponse,
	FaultDisconnectPlanned,
	FaultDisconnectAbrupt,
	FaultDNSFailure,
	FaultTLSFailure,
	FaultConnectFailure,
	FaultHTTP429,
	FaultHTTP403,
	FaultHTTP418,
	FaultHTTP5xx,
	FaultConnectionBudgetExhaustion,
	FaultRateBudgetExhaustion,
	FaultPayloadDuplicate,
	FaultPayloadStale,
	FaultPayloadEqualTime,
	FaultPayloadRegressingTime,
	FaultPayloadMalformed,
	FaultPayloadUnknownRole,
	FaultPayloadAdditiveField,
	FaultPayloadTypeChanged,
	FaultPayloadOversized,
	FaultAdversarialRandomEpochID,
	FaultWallClockRegression,
	FaultBookMissingUpdate,
	FaultBookReorderedUpdate,
	FaultBookDuplicateUpdate,
	FaultBookSnapshotRace,
	FaultBookBufferOverflow,
	FaultBookSequenceException,
	FaultBookChecksumException,
	FaultBookTrueGap,
	FaultKillFrameClose,
	FaultKillSegmentClose,
	FaultKillUpload,
	FaultKillVerify,
	FaultKillManifest,
	FaultKillCatalogCommit,
	FaultKillParquetFooter,
	FaultKillClickHouseAcknowledgement,
	FaultFramePartial,
	FaultFrameTruncated,
	FaultFrameCorrupted,
	FaultObjectPartial,
	FaultObjectTruncated,
	FaultObjectCorrupted,
	FaultParquetPagePartial,
	FaultParquetPageTruncated,
	FaultParquetPageCorrupted,
	FaultWriterQueueFull,
	FaultSpoolFull,
	FaultObjectStoreSlow,
	FaultPostgreSQLSlow,
	FaultClickHouseSlow,
	FaultReplayClientSlow,
	FaultCatalogPagination,
	FaultCatalogSymbolReuse,
	FaultCatalogListing,
	FaultCatalogAuction,
	FaultCatalogExpiry,
	FaultCatalogDelisting,
	FaultCatalogRecycle,
	FaultCatalogChangedMultiplier,
	FaultCatalogChangedTick,
	FaultCatalogChangedLot,
	FaultCatalogUnresolvedUnit,
	FaultMapperDualRunMismatch,
	FaultMapperEffectiveBoundaryReplay,
	FaultBackupRestore,
	FaultOrphanReconciliation,
	FaultFullClickHouseRebuild,
}

func RequiredFaultCategoryIDs() []FaultCategoryID {
	return slices.Clone(requiredFaultCategoryIDs)
}

type DeterminismGateID string

const (
	DeterminismNativeReplay      DeterminismGateID = "native_replay_ordered_hash"
	DeterminismConcurrencyOrder  DeterminismGateID = "concurrency_and_cross_source_order"
	DeterminismNormalizedReplay  DeterminismGateID = "normalized_replay_logical_hash"
	DeterminismParquetBuild      DeterminismGateID = "parquet_logical_and_physical_hash"
	DeterminismCrossArchitecture DeterminismGateID = "amd64_arm64_logical_hash"
	DeterminismClickHouseRebuild DeterminismGateID = "clickhouse_rebuild_event_and_query_set"
)

var requiredDeterminismGateIDs = []DeterminismGateID{
	DeterminismNativeReplay,
	DeterminismConcurrencyOrder,
	DeterminismNormalizedReplay,
	DeterminismParquetBuild,
	DeterminismCrossArchitecture,
	DeterminismClickHouseRebuild,
}

func RequiredDeterminismGateIDs() []DeterminismGateID {
	return slices.Clone(requiredDeterminismGateIDs)
}

type HardwareIdentity struct {
	ID                   string `json:"id"`
	OS                   string `json:"os"`
	Architecture         string `json:"architecture"`
	CPUModel             string `json:"cpu_model"`
	LogicalCPUs          uint32 `json:"logical_cpus"`
	MemoryBytes          uint64 `json:"memory_bytes"`
	ProductionEquivalent bool   `json:"production_equivalent"`
	ManifestSHA256       string `json:"manifest_sha256"`
}

type ContractIdentity struct {
	ContractID        string            `json:"contract_id"`
	SourceID          string            `json:"source_id"`
	APIVersion        string            `json:"api_version"`
	Entitlement       string            `json:"entitlement"`
	ChannelOrEndpoint string            `json:"channel_or_endpoint"`
	DataFamily        string            `json:"data_family"`
	NativeGranularity string            `json:"native_granularity"`
	VenueFamily       string            `json:"venue_family"`
	CoverageStartNS   int64             `json:"coverage_start_ns"`
	CoverageEndNS     int64             `json:"coverage_end_ns"`
	AdapterVersion    string            `json:"adapter_version"`
	ContractSHA256    string            `json:"contract_sha256"`
	AdapterSHA256     string            `json:"adapter_sha256"`
	CanaryRequirement CanaryRequirement `json:"canary_requirement"`
}

type WorkloadIdentity struct {
	ID                           string             `json:"id"`
	ManifestSHA256               string             `json:"manifest_sha256"`
	MaxObservedMessagesPerSecond uint64             `json:"max_observed_messages_per_second"`
	MaxObservedBytesPerSecond    uint64             `json:"max_observed_bytes_per_second"`
	AcquisitionRecordsPerSecond  uint64             `json:"acquisition_records_per_second"`
	AcquisitionBytesPerSecond    uint64             `json:"acquisition_bytes_per_second"`
	Contracts                    []ContractIdentity `json:"contracts"`
}

type FixedCorpusIdentity struct {
	ID                     string         `json:"id"`
	ManifestSHA256         string         `json:"manifest_sha256"`
	VenueFamilies          []string       `json:"venue_families"`
	PayloadClasses         []PayloadClass `json:"payload_classes"`
	HighCardinalitySymbols bool           `json:"high_cardinality_symbols"`
	LongBooks              bool           `json:"long_books"`
	SparseTickerUpdates    bool           `json:"sparse_ticker_updates"`
	Reconnects             bool           `json:"reconnects"`
	LongHistories          bool           `json:"long_histories"`
}

type RawAccounting struct {
	ExpectedRecords        uint64 `json:"expected_records"`
	CommittedRecords       uint64 `json:"committed_records"`
	ExplainedLossRecords   uint64 `json:"explained_loss_records"`
	UnexplainedLossRecords uint64 `json:"unexplained_loss_records"`
}

type SustainedLoadObservation struct {
	DurationNS        int64         `json:"duration_ns"`
	MessagesPerSecond uint64        `json:"messages_per_second"`
	BytesPerSecond    uint64        `json:"bytes_per_second"`
	Raw               RawAccounting `json:"raw"`
	EvidenceSHA256    string        `json:"evidence_sha256"`
}

type BurstObservation struct {
	DurationNS                 int64         `json:"duration_ns"`
	MessagesPerSecond          uint64        `json:"messages_per_second"`
	BytesPerSecond             uint64        `json:"bytes_per_second"`
	DocumentedBackpressurePath bool          `json:"documented_backpressure_path"`
	BackpressureBeforeBounds   bool          `json:"backpressure_before_bounds"`
	MemoryLimitExceeded        bool          `json:"memory_limit_exceeded"`
	SpoolLimitExceeded         bool          `json:"spool_limit_exceeded"`
	QueueLimitExceeded         bool          `json:"queue_limit_exceeded"`
	Raw                        RawAccounting `json:"raw"`
	EvidenceSHA256             string        `json:"evidence_sha256"`
}

type MemoryObservation struct {
	QueueBoundBytes    uint64 `json:"queue_bound_bytes"`
	FrameBoundBytes    uint64 `json:"frame_bound_bytes"`
	RowGroupBoundBytes uint64 `json:"row_group_bound_bytes"`
	PeakRSSBytes       uint64 `json:"peak_rss_bytes"`
	Plateaued          bool   `json:"plateaued"`
	UnboundedGrowth    bool   `json:"unbounded_growth"`
	EvidenceSHA256     string `json:"evidence_sha256"`
}

type ReplayPerformanceObservation struct {
	NativeRecordsPerSecond     uint64 `json:"native_records_per_second"`
	NativeBytesPerSecond       uint64 `json:"native_bytes_per_second"`
	NativeWorkers              uint32 `json:"native_workers"`
	NormalizedRecordsPerSecond uint64 `json:"normalized_records_per_second"`
	NormalizedBytesPerSecond   uint64 `json:"normalized_bytes_per_second"`
	NormalizedWorkers          uint32 `json:"normalized_workers"`
	ParallelismMeasured        bool   `json:"parallelism_measured"`
	ParallelismOrderPreserved  bool   `json:"parallelism_order_preserved"`
	EvidenceSHA256             string `json:"evidence_sha256"`
}

type TelemetryBlackholeObservation struct {
	Raw              RawAccounting `json:"raw"`
	MemoryBoundBytes uint64        `json:"memory_bound_bytes"`
	PeakMemoryBytes  uint64        `json:"peak_memory_bytes"`
	CaptureStalled   bool          `json:"capture_stalled"`
	EvidenceSHA256   string        `json:"evidence_sha256"`
}

type QueryThreshold struct {
	ID                    string `json:"id"`
	MaxLatencyNS          int64  `json:"max_latency_ns"`
	ObservedLatencyNS     int64  `json:"observed_latency_ns"`
	MaxResponseBytes      uint64 `json:"max_response_bytes"`
	ObservedResponseBytes uint64 `json:"observed_response_bytes"`
}

type X5QueryBudgetObservation struct {
	DatasetSHA256  string           `json:"dataset_sha256"`
	EvidenceSHA256 string           `json:"evidence_sha256"`
	RequiredIDs    []string         `json:"required_ids"`
	Thresholds     []QueryThreshold `json:"thresholds"`
}

type CorruptionObservation struct {
	InjectedCases   uint64 `json:"injected_cases"`
	DetectedCases   uint64 `json:"detected_cases"`
	UndetectedCases uint64 `json:"undetected_cases"`
	EvidenceSHA256  string `json:"evidence_sha256"`
}

type PerformanceObservations struct {
	Sustained  SustainedLoadObservation      `json:"sustained"`
	Burst      BurstObservation              `json:"burst"`
	Memory     MemoryObservation             `json:"memory"`
	Replay     ReplayPerformanceObservation  `json:"replay"`
	Telemetry  TelemetryBlackholeObservation `json:"telemetry_blackhole"`
	Queries    X5QueryBudgetObservation      `json:"x5_query_budgets"`
	Corruption CorruptionObservation         `json:"corruption"`
}

type FaultCategoryObservation struct {
	CategoryID     FaultCategoryID `json:"category_id"`
	Passed         bool            `json:"passed"`
	FailureCount   uint64          `json:"failure_count"`
	EvidenceSHA256 string          `json:"evidence_sha256"`
}

type ContractFaultCoverage struct {
	ContractID string                     `json:"contract_id"`
	Categories []FaultCategoryObservation `json:"categories"`
}

type DeterminismObservation struct {
	GateID         DeterminismGateID `json:"gate_id"`
	Passed         bool              `json:"passed"`
	MismatchCount  uint64            `json:"mismatch_count"`
	EvidenceSHA256 string            `json:"evidence_sha256"`
}

type ContractDeterminismCoverage struct {
	ContractID string                   `json:"contract_id"`
	Gates      []DeterminismObservation `json:"gates"`
}

type CanaryObservation struct {
	ContractID       string        `json:"contract_id"`
	Status           CanaryStatus  `json:"status"`
	StartNS          int64         `json:"start_ns"`
	EndNS            int64         `json:"end_ns"`
	ObservedNS       int64         `json:"observed_ns"`
	ExplainedGapNS   int64         `json:"explained_gap_ns"`
	UnexplainedGapNS int64         `json:"unexplained_gap_ns"`
	Raw              RawAccounting `json:"raw"`
	EvidenceSHA256   string        `json:"evidence_sha256"`
}

type ReleaseGateBundle struct {
	Hardware            HardwareIdentity              `json:"hardware"`
	Workload            WorkloadIdentity              `json:"workload"`
	Corpus              FixedCorpusIdentity           `json:"fixed_corpus"`
	Performance         PerformanceObservations       `json:"performance"`
	FaultCoverage       []ContractFaultCoverage       `json:"fault_coverage"`
	DeterminismCoverage []ContractDeterminismCoverage `json:"determinism_coverage"`
	Canaries            []CanaryObservation           `json:"canaries"`
}

type ReleaseGateCheck struct {
	ID     string `json:"id"`
	Scope  string `json:"scope"`
	Passed bool   `json:"passed"`
	Reason string `json:"reason"`
}

type ContractGateResult struct {
	ContractID        string            `json:"contract_id"`
	Tuple             ContractIdentity  `json:"tuple"`
	Passed            bool              `json:"passed"`
	CanaryRequirement CanaryRequirement `json:"canary_requirement"`
	CanaryStatus      CanaryStatus      `json:"canary_status"`
}

type ReleaseGateReport struct {
	Format          string               `json:"format"`
	Bundle          ReleaseGateBundle    `json:"bundle"`
	Passed          bool                 `json:"passed"`
	Checks          []ReleaseGateCheck   `json:"checks"`
	ContractResults []ContractGateResult `json:"contract_results"`
	BodySHA256      string               `json:"body_sha256"`
}

func EvaluateReleaseGate(input ReleaseGateBundle) (ReleaseGateReport, error) {
	bundle := canonicalReleaseGateBundle(input)
	checks := evaluateReleaseGateChecks(bundle)
	slices.SortFunc(checks, compareReleaseGateCheck)
	passed := true
	for _, check := range checks {
		if !check.Passed {
			passed = false
			break
		}
	}
	report := ReleaseGateReport{
		Format:          ReleaseGateFormat,
		Bundle:          bundle,
		Passed:          passed,
		Checks:          checks,
		ContractResults: contractGateResults(bundle, checks),
	}
	body, err := marshalReleaseGateBody(report)
	if err != nil {
		return ReleaseGateReport{}, fmt.Errorf("%w: marshal canonical report: %v", ErrInvalidReleaseGate, err)
	}
	digest := sha256.Sum256(body)
	report.BodySHA256 = hex.EncodeToString(digest[:])
	return report, nil
}

func (r ReleaseGateReport) CanonicalBytes() ([]byte, error) {
	expected, err := EvaluateReleaseGate(r.Bundle)
	if err != nil {
		return nil, err
	}
	expected.BodySHA256 = ""
	candidate := r
	candidate.BodySHA256 = ""
	actual, err := json.Marshal(candidate)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal report: %v", ErrInvalidReleaseGate, err)
	}
	canonical, err := json.Marshal(expected)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal canonical report: %v", ErrInvalidReleaseGate, err)
	}
	if !bytes.Equal(actual, canonical) {
		return nil, fmt.Errorf("%w: report fields, ordering, decision, or checks are not canonical", ErrInvalidReleaseGate)
	}
	return canonical, nil
}

func (r ReleaseGateReport) ContentSHA256() (string, error) {
	if !validSHA256(r.BodySHA256) {
		return "", fmt.Errorf("%w: body_sha256 is not a lowercase SHA-256", ErrInvalidReleaseGate)
	}
	body, err := r.CanonicalBytes()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	if r.BodySHA256 != hex.EncodeToString(digest[:]) {
		return "", fmt.Errorf("%w: body_sha256 mismatch", ErrInvalidReleaseGate)
	}
	return r.BodySHA256, nil
}

func (r ReleaseGateReport) Validate() error {
	if r.Format != ReleaseGateFormat {
		return fmt.Errorf("%w: unknown format %q", ErrInvalidReleaseGate, r.Format)
	}
	if _, err := r.ContentSHA256(); err != nil {
		return err
	}
	if !r.Passed {
		for _, check := range r.Checks {
			if !check.Passed {
				return fmt.Errorf("%w: %s[%s]: %s", ErrReleaseGateFailed, check.ID, check.Scope, check.Reason)
			}
		}
		return fmt.Errorf("%w: failed without a reason", ErrReleaseGateFailed)
	}
	return nil
}

func ParseReleaseGateReport(data []byte) (ReleaseGateReport, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var report ReleaseGateReport
	if err := decoder.Decode(&report); err != nil {
		return ReleaseGateReport{}, fmt.Errorf("%w: decode report: %v", ErrInvalidReleaseGate, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ReleaseGateReport{}, err
	}
	if err := report.Validate(); err != nil {
		return ReleaseGateReport{}, err
	}
	return report, nil
}

func (r ReleaseGateReport) ContractResult(contractID string) (ContractGateResult, bool) {
	index := slices.IndexFunc(r.ContractResults, func(result ContractGateResult) bool {
		return result.ContractID == contractID
	})
	if index < 0 {
		return ContractGateResult{}, false
	}
	if slices.IndexFunc(r.ContractResults[index+1:], func(result ContractGateResult) bool {
		return result.ContractID == contractID
	}) >= 0 {
		return ContractGateResult{}, false
	}
	return r.ContractResults[index], true
}

func (r ReleaseGateReport) ValidateContract(identity ContractIdentity) error {
	if err := r.Validate(); err != nil {
		return err
	}
	result, ok := r.ContractResult(identity.ContractID)
	if !ok {
		return fmt.Errorf("%w: contract %q is missing or ambiguous", ErrInvalidReleaseGate, identity.ContractID)
	}
	if compareContractIdentity(result.Tuple, identity) != 0 {
		return fmt.Errorf("%w: contract %q tuple identity does not match", ErrInvalidReleaseGate, identity.ContractID)
	}
	if !result.Passed {
		return fmt.Errorf("%w: contract %q did not pass", ErrReleaseGateFailed, identity.ContractID)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: multiple JSON values", ErrInvalidReleaseGate)
		}
		return fmt.Errorf("%w: trailing JSON: %v", ErrInvalidReleaseGate, err)
	}
	return nil
}

func marshalReleaseGateBody(report ReleaseGateReport) ([]byte, error) {
	report.BodySHA256 = ""
	return json.Marshal(report)
}

func canonicalReleaseGateBundle(input ReleaseGateBundle) ReleaseGateBundle {
	bundle := input
	bundle.Workload.Contracts = slices.Clone(input.Workload.Contracts)
	slices.SortFunc(bundle.Workload.Contracts, compareContractIdentity)
	bundle.Corpus.VenueFamilies = slices.Clone(input.Corpus.VenueFamilies)
	slices.Sort(bundle.Corpus.VenueFamilies)
	bundle.Corpus.PayloadClasses = slices.Clone(input.Corpus.PayloadClasses)
	slices.Sort(bundle.Corpus.PayloadClasses)
	bundle.Performance.Queries.RequiredIDs = slices.Clone(input.Performance.Queries.RequiredIDs)
	slices.Sort(bundle.Performance.Queries.RequiredIDs)
	bundle.Performance.Queries.Thresholds = slices.Clone(input.Performance.Queries.Thresholds)
	slices.SortFunc(bundle.Performance.Queries.Thresholds, compareQueryThreshold)

	bundle.FaultCoverage = make([]ContractFaultCoverage, len(input.FaultCoverage))
	for i, coverage := range input.FaultCoverage {
		bundle.FaultCoverage[i] = coverage
		bundle.FaultCoverage[i].Categories = slices.Clone(coverage.Categories)
		slices.SortFunc(bundle.FaultCoverage[i].Categories, compareFaultObservation)
	}
	slices.SortFunc(bundle.FaultCoverage, compareFaultCoverage)

	bundle.DeterminismCoverage = make([]ContractDeterminismCoverage, len(input.DeterminismCoverage))
	for i, coverage := range input.DeterminismCoverage {
		bundle.DeterminismCoverage[i] = coverage
		bundle.DeterminismCoverage[i].Gates = slices.Clone(coverage.Gates)
		slices.SortFunc(bundle.DeterminismCoverage[i].Gates, compareDeterminismObservation)
	}
	slices.SortFunc(bundle.DeterminismCoverage, compareDeterminismCoverage)

	bundle.Canaries = slices.Clone(input.Canaries)
	slices.SortFunc(bundle.Canaries, compareCanaryObservation)
	return bundle
}

func evaluateReleaseGateChecks(bundle ReleaseGateBundle) []ReleaseGateCheck {
	checks := make([]ReleaseGateCheck, 0, 64+len(bundle.Workload.Contracts)*(len(requiredFaultCategoryIDs)+len(requiredDeterminismGateIDs)+8))
	checks = append(checks,
		checkFromError("identity.hardware", "", validateHardwareIdentity(bundle.Hardware), "hardware identity is pinned, bounded, and production-equivalent"),
		checkFromError("identity.workload", "", validateWorkloadIdentity(bundle.Workload), "workload and every declared contract tuple are pinned and unambiguous"),
		checkFromError("identity.fixed_corpus", "", validateFixedCorpusIdentity(bundle.Corpus, bundle.Workload), "fixed corpus covers every venue family and required workload shape"),
	)
	checks = append(checks, performanceChecks(bundle)...)
	checks = append(checks, faultCoverageChecks(bundle.Workload.Contracts, bundle.FaultCoverage)...)
	checks = append(checks, determinismCoverageChecks(bundle.Workload.Contracts, bundle.DeterminismCoverage)...)
	checks = append(checks, canaryChecks(bundle.Workload.Contracts, bundle.Canaries)...)
	return checks
}

func performanceChecks(bundle ReleaseGateBundle) []ReleaseGateCheck {
	workload := bundle.Workload
	performance := bundle.Performance
	checks := make([]ReleaseGateCheck, 0, 24+len(performance.Queries.RequiredIDs))

	sustained := performance.Sustained
	checks = append(checks,
		gateCheck("sustained.duration", "", sustained.DurationNS >= MinimumSustainedDurationNS && sustained.DurationNS <= MaxReleaseGateDurationNS,
			"sustained observation ran for at least 60 minutes", "sustained observation must run for at least 60 minutes within the duration bound"),
		gateCheck("sustained.message_rate", "", validRate(sustained.MessagesPerSecond) && atLeastMultiple(sustained.MessagesPerSecond, workload.MaxObservedMessagesPerSecond, 2),
			"sustained message rate reached 2x the declared observed maximum", "sustained message rate is below 2x the declared observed maximum or unbounded"),
		gateCheck("sustained.byte_rate", "", validRate(sustained.BytesPerSecond) && atLeastMultiple(sustained.BytesPerSecond, workload.MaxObservedBytesPerSecond, 2),
			"sustained byte rate reached 2x the declared observed maximum", "sustained byte rate is below 2x the declared observed maximum or unbounded"),
		checkFromError("sustained.raw_accounting", "", validateRawAccounting(sustained.Raw, true, false), "sustained raw accounting is exact with zero unexplained loss"),
		gateCheck("sustained.evidence", "", validSHA256(sustained.EvidenceSHA256), "sustained evidence digest is pinned", "sustained evidence digest is missing or invalid"),
	)

	burst := performance.Burst
	fullBurst := burst.DurationNS >= MinimumBurstDurationNS && burst.DurationNS <= MaxReleaseGateDurationNS &&
		validRate(burst.MessagesPerSecond) && validRate(burst.BytesPerSecond) &&
		atLeastMultiple(burst.MessagesPerSecond, workload.MaxObservedMessagesPerSecond, 5) &&
		atLeastMultiple(burst.BytesPerSecond, workload.MaxObservedBytesPerSecond, 5)
	backpressure := burst.DurationNS > 0 && burst.DurationNS <= MaxReleaseGateDurationNS &&
		validRate(burst.MessagesPerSecond) && validRate(burst.BytesPerSecond) &&
		burst.DocumentedBackpressurePath && burst.BackpressureBeforeBounds
	checks = append(checks,
		gateCheck("burst.rate_or_backpressure", "", fullBurst || backpressure,
			"burst reached 5x for 30 seconds or entered the documented backpressure path before bounds", "burst neither reached 5x for 30 seconds nor entered documented backpressure before bounds"),
		gateCheck("burst.resource_bounds", "", !burst.MemoryLimitExceeded && !burst.SpoolLimitExceeded && !burst.QueueLimitExceeded,
			"burst stayed within memory, spool, and queue bounds", "burst exceeded a memory, spool, or queue bound"),
		checkFromError("burst.raw_accounting", "", validateRawAccounting(burst.Raw, true, false), "burst raw accounting is exact with zero unexplained loss"),
		gateCheck("burst.evidence", "", validSHA256(burst.EvidenceSHA256), "burst evidence digest is pinned", "burst evidence digest is missing or invalid"),
	)

	memory := performance.Memory
	memoryBound, boundOK := boundedSum(memory.QueueBoundBytes, memory.FrameBoundBytes, memory.RowGroupBoundBytes)
	allowedRSS := uint64(0)
	if boundOK {
		allowedRSS = memoryBound + memoryBound/10
	}
	checks = append(checks,
		gateCheck("memory.configured_bounds", "", boundOK && memory.QueueBoundBytes > 0 && memory.FrameBoundBytes > 0 && memory.RowGroupBoundBytes > 0,
			"queue, frame, and row-group memory bounds are finite", "queue, frame, or row-group memory bound is zero or exceeds the numeric bound"),
		gateCheck("memory.rss_plateau", "", boundOK && memory.PeakRSSBytes > 0 && memory.PeakRSSBytes <= MaxReleaseGateBytes && memory.PeakRSSBytes <= allowedRSS && memory.Plateaued && !memory.UnboundedGrowth,
			"RSS plateaued within configured bounds plus 10 percent", "RSS is unbounded, did not plateau, or exceeded configured bounds plus 10 percent"),
		gateCheck("memory.evidence", "", validSHA256(memory.EvidenceSHA256), "memory evidence digest is pinned", "memory evidence digest is missing or invalid"),
	)

	replay := performance.Replay
	nativeRates := validRate(replay.NativeRecordsPerSecond) && validRate(replay.NativeBytesPerSecond) &&
		replay.NativeRecordsPerSecond >= workload.AcquisitionRecordsPerSecond &&
		replay.NativeBytesPerSecond >= workload.AcquisitionBytesPerSecond
	normalizedDouble := validRate(replay.NormalizedRecordsPerSecond) && validRate(replay.NormalizedBytesPerSecond) &&
		atLeastMultiple(replay.NormalizedRecordsPerSecond, workload.AcquisitionRecordsPerSecond, 2) &&
		atLeastMultiple(replay.NormalizedBytesPerSecond, workload.AcquisitionBytesPerSecond, 2)
	normalizedParallel := validRate(replay.NormalizedRecordsPerSecond) && validRate(replay.NormalizedBytesPerSecond) &&
		replay.NormalizedWorkers > 1 && replay.NormalizedWorkers <= MaxReleaseGateWorkers &&
		replay.ParallelismMeasured && replay.ParallelismOrderPreserved
	checks = append(checks,
		gateCheck("replay.native_rate", "", nativeRates && replay.NativeWorkers == 1 && bundle.Hardware.ProductionEquivalent,
			"native replay reached acquisition record and byte rates on one production-equivalent worker", "native replay is below acquisition rate, unbounded, or did not use one production-equivalent worker"),
		gateCheck("replay.normalized_rate_or_parallelism", "", (normalizedDouble || normalizedParallel) && replay.NormalizedWorkers > 0 && replay.NormalizedWorkers <= MaxReleaseGateWorkers,
			"normalized replay reached 2x acquisition or used measured order-preserving parallelism", "normalized replay is below 2x acquisition without measured order-preserving parallelism"),
		gateCheck("replay.evidence", "", validSHA256(replay.EvidenceSHA256), "replay performance evidence digest is pinned", "replay performance evidence digest is missing or invalid"),
	)

	telemetry := performance.Telemetry
	checks = append(checks,
		checkFromError("telemetry.raw_accounting", "", validateRawAccounting(telemetry.Raw, true, true), "telemetry blackhole caused no raw loss"),
		gateCheck("telemetry.memory", "", telemetry.MemoryBoundBytes > 0 && telemetry.MemoryBoundBytes <= MaxReleaseGateBytes && telemetry.PeakMemoryBytes <= telemetry.MemoryBoundBytes,
			"telemetry memory stayed within its configured bound", "telemetry memory bound is invalid or was exceeded"),
		gateCheck("telemetry.capture_progress", "", !telemetry.CaptureStalled, "telemetry blackhole did not stall capture", "telemetry blackhole stalled capture"),
		gateCheck("telemetry.evidence", "", validSHA256(telemetry.EvidenceSHA256), "telemetry blackhole evidence digest is pinned", "telemetry blackhole evidence digest is missing or invalid"),
	)

	queries := performance.Queries
	checks = append(checks,
		gateCheck("query.x5_evidence", "", validSHA256(queries.DatasetSHA256) && validSHA256(queries.EvidenceSHA256),
			"X5 dataset and query evidence digests are pinned", "X5 dataset or query evidence digest is missing or invalid"),
		gateCheck("query.threshold_set", "", validQueryThresholdSet(queries),
			"every declared X5 query threshold is present exactly once", "X5 query thresholds are missing, duplicated, extra, invalid, or ambiguous"),
	)
	for _, id := range uniqueValidStrings(queries.RequiredIDs) {
		threshold, count := findQueryThreshold(queries.Thresholds, id)
		valid := count == 1 && validQueryThreshold(threshold) && threshold.ObservedLatencyNS <= threshold.MaxLatencyNS && threshold.ObservedResponseBytes <= threshold.MaxResponseBytes
		checks = append(checks, gateCheck("query.threshold."+id, "", valid,
			"observed X5 latency and response size meet the frozen threshold", "observed X5 latency or response size exceeds the frozen threshold, or the threshold is invalid"))
	}

	corruption := performance.Corruption
	corruptionExact := corruption.InjectedCases > 0 && corruption.InjectedCases <= MaxReleaseGateCount &&
		corruption.DetectedCases <= MaxReleaseGateCount && corruption.UndetectedCases <= MaxReleaseGateCount &&
		corruption.DetectedCases+corruption.UndetectedCases == corruption.InjectedCases && corruption.UndetectedCases == 0
	checks = append(checks,
		gateCheck("integrity.corruption_zero_tolerance", "", corruptionExact,
			"every injected corruption was detected", "corruption accounting is invalid or at least one corruption was undetected"),
		gateCheck("integrity.corruption_evidence", "", validSHA256(corruption.EvidenceSHA256),
			"corruption evidence digest is pinned", "corruption evidence digest is missing or invalid"),
	)
	return checks
}

func faultCoverageChecks(contracts []ContractIdentity, coverages []ContractFaultCoverage) []ReleaseGateCheck {
	expectedContracts := uniqueContracts(contracts)
	checks := []ReleaseGateCheck{gateCheck("fault.contract_set", "", exactContractIDSet(expectedContracts, faultCoverageIDs(coverages)),
		"fault coverage contains every declared contract exactly once", "fault coverage has a missing, duplicate, or undeclared contract")}
	for _, contract := range expectedContracts {
		coverage, count := findFaultCoverage(coverages, contract.ContractID)
		setValid := count == 1 && exactFaultCategorySet(coverage.Categories)
		checks = append(checks, gateCheck("fault.category_set", contract.ContractID, setValid,
			"the complete mandatory fault category set is present", "mandatory fault categories are missing, duplicated, or unknown"))
		for _, categoryID := range requiredFaultCategoryIDs {
			observation, observationCount := findFaultObservation(coverage.Categories, categoryID)
			valid := count == 1 && observationCount == 1 && observation.Passed && observation.FailureCount == 0 && validSHA256(observation.EvidenceSHA256)
			checks = append(checks, gateCheck("fault."+string(categoryID), contract.ContractID, valid,
				"fault category passed with zero failures and pinned evidence", "fault category is missing, duplicated, failed, unknown, or lacks pinned evidence"))
		}
	}
	return checks
}

func determinismCoverageChecks(contracts []ContractIdentity, coverages []ContractDeterminismCoverage) []ReleaseGateCheck {
	expectedContracts := uniqueContracts(contracts)
	checks := []ReleaseGateCheck{gateCheck("determinism.contract_set", "", exactContractIDSet(expectedContracts, determinismCoverageIDs(coverages)),
		"determinism coverage contains every declared contract exactly once", "determinism coverage has a missing, duplicate, or undeclared contract")}
	for _, contract := range expectedContracts {
		coverage, count := findDeterminismCoverage(coverages, contract.ContractID)
		setValid := count == 1 && exactDeterminismGateSet(coverage.Gates)
		checks = append(checks, gateCheck("determinism.gate_set", contract.ContractID, setValid,
			"the complete determinism gate set is present", "determinism gates are missing, duplicated, or unknown"))
		for _, gateID := range requiredDeterminismGateIDs {
			observation, observationCount := findDeterminismObservation(coverage.Gates, gateID)
			valid := count == 1 && observationCount == 1 && observation.Passed && observation.MismatchCount == 0 && validSHA256(observation.EvidenceSHA256)
			checks = append(checks, gateCheck("determinism."+string(gateID), contract.ContractID, valid,
				"determinism gate passed with zero mismatches and pinned evidence", "determinism gate is missing, duplicated, failed, mismatched, unknown, or lacks pinned evidence"))
		}
	}
	return checks
}

func canaryChecks(contracts []ContractIdentity, canaries []CanaryObservation) []ReleaseGateCheck {
	expectedContracts := uniqueContracts(contracts)
	checks := []ReleaseGateCheck{gateCheck("canary.contract_set", "", exactContractIDSet(expectedContracts, canaryIDs(canaries)),
		"canary observations contain every declared contract exactly once", "canary observations have a missing, duplicate, or undeclared contract")}
	for _, contract := range expectedContracts {
		canary, count := findCanary(canaries, contract.ContractID)
		checks = append(checks, gateCheck("canary.evidence", contract.ContractID, count == 1 && validSHA256(canary.EvidenceSHA256),
			"canary requirement evidence digest is pinned", "canary observation is missing, duplicated, or lacks pinned evidence"))
		switch contract.CanaryRequirement {
		case CanaryRequired:
			elapsed, intervalValid := canaryElapsed(canary)
			accountingValid := intervalValid && canary.ObservedNS >= 0 && canary.ExplainedGapNS >= 0 && canary.UnexplainedGapNS >= 0 &&
				canary.ObservedNS <= MaxReleaseGateDurationNS && canary.ExplainedGapNS <= MaxReleaseGateDurationNS && canary.UnexplainedGapNS <= MaxReleaseGateDurationNS &&
				canary.ObservedNS+canary.ExplainedGapNS+canary.UnexplainedGapNS == elapsed
			checks = append(checks,
				gateCheck("canary.status", contract.ContractID, count == 1 && canary.Status == CanaryPassedStatus,
					"required canary completed", "required canary status is missing, unknown, or not passed"),
				gateCheck("canary.duration", contract.ContractID, intervalValid && elapsed >= MinimumCanaryDurationNS,
					"required canary ran for at least 26 hours", "required canary interval is invalid or shorter than 26 hours"),
				gateCheck("canary.gap_accounting", contract.ContractID, accountingValid && canary.UnexplainedGapNS == 0,
					"canary interval accounting is exact with no unexplained gap", "canary interval accounting is inexact, unbounded, or contains an unexplained gap"),
				checkFromError("canary.raw_accounting", contract.ContractID, validateRawAccounting(canary.Raw, true, false), "canary raw accounting is exact with zero unexplained loss"),
			)
		case CanaryNotRequired:
			empty := canary.StartNS == 0 && canary.EndNS == 0 && canary.ObservedNS == 0 && canary.ExplainedGapNS == 0 && canary.UnexplainedGapNS == 0 && canary.Raw == (RawAccounting{})
			checks = append(checks, gateCheck("canary.status", contract.ContractID, count == 1 && canary.Status == CanaryNotRequiredStatus && empty,
				"contract explicitly records that a canary is not required", "non-required canary status is unknown, ambiguous, or carries an interval"))
		default:
			checks = append(checks, gateCheck("canary.status", contract.ContractID, false, "", "contract has an unknown canary requirement"))
		}
	}
	return checks
}

func contractGateResults(bundle ReleaseGateBundle, checks []ReleaseGateCheck) []ContractGateResult {
	contracts := uniqueContracts(bundle.Workload.Contracts)
	globalPassed := true
	for _, check := range checks {
		if check.Scope == "" && !check.Passed {
			globalPassed = false
			break
		}
	}
	results := make([]ContractGateResult, 0, len(contracts))
	for _, contract := range contracts {
		passed := globalPassed
		for _, check := range checks {
			if check.Scope == contract.ContractID && !check.Passed {
				passed = false
				break
			}
		}
		canary, count := findCanary(bundle.Canaries, contract.ContractID)
		status := CanaryStatus("")
		if count == 1 {
			status = canary.Status
		}
		results = append(results, ContractGateResult{
			ContractID: contract.ContractID, Tuple: contract, Passed: passed,
			CanaryRequirement: contract.CanaryRequirement, CanaryStatus: status,
		})
	}
	slices.SortFunc(results, func(left, right ContractGateResult) int {
		if result := cmp.Compare(left.ContractID, right.ContractID); result != 0 {
			return result
		}
		return compareContractIdentity(left.Tuple, right.Tuple)
	})
	return results
}

func validateHardwareIdentity(hardware HardwareIdentity) error {
	for _, value := range []struct{ name, value string }{
		{"id", hardware.ID}, {"os", hardware.OS}, {"architecture", hardware.Architecture}, {"cpu_model", hardware.CPUModel},
	} {
		if !validText(value.value) {
			return fmt.Errorf("%s is blank, malformed, or unbounded", value.name)
		}
	}
	if hardware.LogicalCPUs == 0 || hardware.LogicalCPUs > MaxReleaseGateWorkers {
		return errors.New("logical CPU count is zero or unbounded")
	}
	if hardware.MemoryBytes == 0 || hardware.MemoryBytes > MaxReleaseGateBytes {
		return errors.New("hardware memory is zero or unbounded")
	}
	if !hardware.ProductionEquivalent {
		return errors.New("hardware is not declared production-equivalent")
	}
	if !validSHA256(hardware.ManifestSHA256) {
		return errors.New("hardware manifest SHA-256 is missing or invalid")
	}
	return nil
}

func validateWorkloadIdentity(workload WorkloadIdentity) error {
	if !validText(workload.ID) || !validSHA256(workload.ManifestSHA256) {
		return errors.New("workload ID or manifest SHA-256 is missing or invalid")
	}
	for _, value := range []struct {
		name  string
		value uint64
	}{
		{"max observed messages per second", workload.MaxObservedMessagesPerSecond},
		{"max observed bytes per second", workload.MaxObservedBytesPerSecond},
		{"acquisition records per second", workload.AcquisitionRecordsPerSecond},
		{"acquisition bytes per second", workload.AcquisitionBytesPerSecond},
	} {
		if !validRate(value.value) {
			return fmt.Errorf("%s is zero or unbounded", value.name)
		}
	}
	if len(workload.Contracts) == 0 || len(workload.Contracts) > MaxReleaseGateContracts {
		return errors.New("declared contract count is zero or unbounded")
	}
	contractIDs := make(map[string]struct{}, len(workload.Contracts))
	tupleIDs := make(map[contractTupleKey]struct{}, len(workload.Contracts))
	for _, contract := range workload.Contracts {
		if err := validateContractIdentity(contract); err != nil {
			return fmt.Errorf("contract %q: %w", contract.ContractID, err)
		}
		if _, duplicate := contractIDs[contract.ContractID]; duplicate {
			return fmt.Errorf("duplicate contract ID %q", contract.ContractID)
		}
		contractIDs[contract.ContractID] = struct{}{}
		key := tupleKey(contract)
		if _, duplicate := tupleIDs[key]; duplicate {
			return fmt.Errorf("duplicate contract tuple %q", contract.ContractID)
		}
		tupleIDs[key] = struct{}{}
	}
	return nil
}

type contractTupleKey struct {
	sourceID, apiVersion, entitlement, channelOrEndpoint, dataFamily, nativeGranularity string
	coverageStartNS, coverageEndNS                                                      int64
	adapterVersion                                                                      string
}

func tupleKey(contract ContractIdentity) contractTupleKey {
	return contractTupleKey{
		sourceID: contract.SourceID, apiVersion: contract.APIVersion, entitlement: contract.Entitlement,
		channelOrEndpoint: contract.ChannelOrEndpoint, dataFamily: contract.DataFamily, nativeGranularity: contract.NativeGranularity,
		coverageStartNS: contract.CoverageStartNS, coverageEndNS: contract.CoverageEndNS, adapterVersion: contract.AdapterVersion,
	}
}

func validateContractIdentity(contract ContractIdentity) error {
	for _, field := range []struct{ name, value string }{
		{"contract_id", contract.ContractID}, {"source_id", contract.SourceID}, {"api_version", contract.APIVersion},
		{"entitlement", contract.Entitlement}, {"channel_or_endpoint", contract.ChannelOrEndpoint}, {"data_family", contract.DataFamily},
		{"native_granularity", contract.NativeGranularity}, {"venue_family", contract.VenueFamily}, {"adapter_version", contract.AdapterVersion},
	} {
		if !validText(field.value) {
			return fmt.Errorf("%s is blank, malformed, or unbounded", field.name)
		}
	}
	if contract.CoverageStartNS < 0 || contract.CoverageEndNS <= contract.CoverageStartNS {
		return errors.New("coverage interval is invalid or ambiguous")
	}
	if !validSHA256(contract.ContractSHA256) || !validSHA256(contract.AdapterSHA256) {
		return errors.New("contract or adapter SHA-256 is missing or invalid")
	}
	if contract.CanaryRequirement != CanaryRequired && contract.CanaryRequirement != CanaryNotRequired {
		return errors.New("canary requirement is unknown")
	}
	return nil
}

func validateFixedCorpusIdentity(corpus FixedCorpusIdentity, workload WorkloadIdentity) error {
	if !validText(corpus.ID) || !validSHA256(corpus.ManifestSHA256) {
		return errors.New("fixed corpus ID or manifest SHA-256 is missing or invalid")
	}
	expectedFamilies := make([]string, 0, len(workload.Contracts))
	for _, contract := range workload.Contracts {
		expectedFamilies = append(expectedFamilies, contract.VenueFamily)
	}
	expectedFamilies = uniqueValidStrings(expectedFamilies)
	if !exactUniqueStringSet(expectedFamilies, corpus.VenueFamilies) {
		return errors.New("fixed corpus venue families do not exactly cover the declared workload")
	}
	if len(corpus.PayloadClasses) != 3 || corpus.PayloadClasses[0] != PayloadMax || corpus.PayloadClasses[1] != PayloadMedian || corpus.PayloadClasses[2] != PayloadTiny {
		return errors.New("fixed corpus must contain tiny, median, and max payload classes exactly once")
	}
	if !corpus.HighCardinalitySymbols || !corpus.LongBooks || !corpus.SparseTickerUpdates || !corpus.Reconnects || !corpus.LongHistories {
		return errors.New("fixed corpus is missing a mandatory workload shape")
	}
	return nil
}

func validateRawAccounting(accounting RawAccounting, requireExpected, requireZeroLoss bool) error {
	if accounting.ExpectedRecords > MaxReleaseGateCount || accounting.CommittedRecords > MaxReleaseGateCount ||
		accounting.ExplainedLossRecords > MaxReleaseGateCount || accounting.UnexplainedLossRecords > MaxReleaseGateCount {
		return errors.New("raw accounting exceeds numeric bounds")
	}
	if requireExpected && accounting.ExpectedRecords == 0 {
		return errors.New("raw accounting has no expected records")
	}
	accounted := accounting.CommittedRecords + accounting.ExplainedLossRecords + accounting.UnexplainedLossRecords
	if accounted != accounting.ExpectedRecords {
		return errors.New("raw accounting is not exact")
	}
	if accounting.UnexplainedLossRecords != 0 {
		return errors.New("raw accounting contains unexplained loss")
	}
	if requireZeroLoss && accounting.ExplainedLossRecords != 0 {
		return errors.New("raw accounting contains loss")
	}
	return nil
}

func validQueryThresholdSet(queries X5QueryBudgetObservation) bool {
	if len(queries.RequiredIDs) == 0 || len(queries.RequiredIDs) > MaxReleaseGateQueryThresholds ||
		len(queries.Thresholds) != len(queries.RequiredIDs) {
		return false
	}
	for i, id := range queries.RequiredIDs {
		if !validText(id) || (i > 0 && id == queries.RequiredIDs[i-1]) {
			return false
		}
	}
	for i, threshold := range queries.Thresholds {
		if !validQueryThreshold(threshold) || (i > 0 && threshold.ID == queries.Thresholds[i-1].ID) || threshold.ID != queries.RequiredIDs[i] {
			return false
		}
	}
	return true
}

func validQueryThreshold(threshold QueryThreshold) bool {
	return validText(threshold.ID) && threshold.MaxLatencyNS > 0 && threshold.MaxLatencyNS <= MaxReleaseGateDurationNS &&
		threshold.ObservedLatencyNS >= 0 && threshold.ObservedLatencyNS <= MaxReleaseGateDurationNS &&
		threshold.MaxResponseBytes > 0 && threshold.MaxResponseBytes <= MaxReleaseGateBytes && threshold.ObservedResponseBytes <= MaxReleaseGateBytes
}

func exactFaultCategorySet(observations []FaultCategoryObservation) bool {
	if len(observations) != len(requiredFaultCategoryIDs) {
		return false
	}
	for i, required := range requiredFaultCategoryIDs {
		if observations[i].CategoryID != required {
			return false
		}
	}
	return true
}

func exactDeterminismGateSet(observations []DeterminismObservation) bool {
	if len(observations) != len(requiredDeterminismGateIDs) {
		return false
	}
	for i, required := range requiredDeterminismGateIDs {
		if observations[i].GateID != required {
			return false
		}
	}
	return true
}

func exactContractIDSet(expected []ContractIdentity, actual []string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i, contract := range expected {
		if contract.ContractID != actual[i] || (i > 0 && actual[i] == actual[i-1]) {
			return false
		}
	}
	return true
}

func exactUniqueStringSet(expected, actual []string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i := range expected {
		if !validText(actual[i]) || expected[i] != actual[i] || (i > 0 && actual[i] == actual[i-1]) {
			return false
		}
	}
	return true
}

func uniqueContracts(contracts []ContractIdentity) []ContractIdentity {
	result := make([]ContractIdentity, 0, len(contracts))
	for _, contract := range contracts {
		if !validText(contract.ContractID) {
			continue
		}
		if len(result) == 0 || result[len(result)-1].ContractID != contract.ContractID {
			result = append(result, contract)
		}
	}
	return result
}

func uniqueValidStrings(values []string) []string {
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	result := make([]string, 0, len(ordered))
	for _, value := range ordered {
		if !validText(value) || (len(result) > 0 && result[len(result)-1] == value) {
			continue
		}
		result = append(result, value)
	}
	return result
}

func faultCoverageIDs(coverages []ContractFaultCoverage) []string {
	ids := make([]string, len(coverages))
	for i, coverage := range coverages {
		ids[i] = coverage.ContractID
	}
	return ids
}

func determinismCoverageIDs(coverages []ContractDeterminismCoverage) []string {
	ids := make([]string, len(coverages))
	for i, coverage := range coverages {
		ids[i] = coverage.ContractID
	}
	return ids
}

func canaryIDs(canaries []CanaryObservation) []string {
	ids := make([]string, len(canaries))
	for i, canary := range canaries {
		ids[i] = canary.ContractID
	}
	return ids
}

func findFaultCoverage(coverages []ContractFaultCoverage, contractID string) (ContractFaultCoverage, int) {
	var found ContractFaultCoverage
	count := 0
	for _, coverage := range coverages {
		if coverage.ContractID == contractID {
			found = coverage
			count++
		}
	}
	return found, count
}

func findFaultObservation(observations []FaultCategoryObservation, categoryID FaultCategoryID) (FaultCategoryObservation, int) {
	var found FaultCategoryObservation
	count := 0
	for _, observation := range observations {
		if observation.CategoryID == categoryID {
			found = observation
			count++
		}
	}
	return found, count
}

func findDeterminismCoverage(coverages []ContractDeterminismCoverage, contractID string) (ContractDeterminismCoverage, int) {
	var found ContractDeterminismCoverage
	count := 0
	for _, coverage := range coverages {
		if coverage.ContractID == contractID {
			found = coverage
			count++
		}
	}
	return found, count
}

func findDeterminismObservation(observations []DeterminismObservation, gateID DeterminismGateID) (DeterminismObservation, int) {
	var found DeterminismObservation
	count := 0
	for _, observation := range observations {
		if observation.GateID == gateID {
			found = observation
			count++
		}
	}
	return found, count
}

func findCanary(canaries []CanaryObservation, contractID string) (CanaryObservation, int) {
	var found CanaryObservation
	count := 0
	for _, canary := range canaries {
		if canary.ContractID == contractID {
			found = canary
			count++
		}
	}
	return found, count
}

func findQueryThreshold(thresholds []QueryThreshold, id string) (QueryThreshold, int) {
	var found QueryThreshold
	count := 0
	for _, threshold := range thresholds {
		if threshold.ID == id {
			found = threshold
			count++
		}
	}
	return found, count
}

func canaryElapsed(canary CanaryObservation) (int64, bool) {
	if canary.StartNS < 0 || canary.EndNS <= canary.StartNS {
		return 0, false
	}
	elapsed := canary.EndNS - canary.StartNS
	return elapsed, elapsed > 0 && elapsed <= MaxReleaseGateDurationNS
}

func validRate(value uint64) bool {
	return value > 0 && value <= MaxReleaseGateRate
}

func atLeastMultiple(observed, baseline, multiple uint64) bool {
	return validRate(baseline) && multiple > 0 && baseline <= MaxReleaseGateRate/multiple && observed >= baseline*multiple
}

func boundedSum(values ...uint64) (uint64, bool) {
	var sum uint64
	for _, value := range values {
		if value == 0 || value > MaxReleaseGateBytes || sum > MaxReleaseGateBytes-value {
			return 0, false
		}
		sum += value
	}
	return sum, sum <= MaxReleaseGateBytes
}

func validText(value string) bool {
	if value == "" || len(value) > MaxReleaseGateTextBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	switch strings.ToLower(value) {
	case "unknown", "unset", "unspecified", "ambiguous", "n/a", "none", "null", "-":
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return false
	}
	return slices.ContainsFunc(decoded, func(value byte) bool { return value != 0 })
}

func gateCheck(id, scope string, passed bool, passReason, failReason string) ReleaseGateCheck {
	reason := failReason
	if passed {
		reason = passReason
	}
	if reason == "" {
		reason = "gate result is invalid"
	}
	return ReleaseGateCheck{ID: id, Scope: scope, Passed: passed, Reason: reason}
}

func checkFromError(id, scope string, err error, passReason string) ReleaseGateCheck {
	if err == nil {
		return gateCheck(id, scope, true, passReason, "")
	}
	return gateCheck(id, scope, false, "", err.Error())
}

func compareReleaseGateCheck(left, right ReleaseGateCheck) int {
	if result := cmp.Compare(left.ID, right.ID); result != 0 {
		return result
	}
	if result := cmp.Compare(left.Scope, right.Scope); result != 0 {
		return result
	}
	if result := compareBool(left.Passed, right.Passed); result != 0 {
		return result
	}
	return cmp.Compare(left.Reason, right.Reason)
}

func compareContractIdentity(left, right ContractIdentity) int {
	for _, values := range [][2]string{
		{left.ContractID, right.ContractID}, {left.SourceID, right.SourceID}, {left.APIVersion, right.APIVersion},
		{left.Entitlement, right.Entitlement}, {left.ChannelOrEndpoint, right.ChannelOrEndpoint}, {left.DataFamily, right.DataFamily},
		{left.NativeGranularity, right.NativeGranularity}, {left.VenueFamily, right.VenueFamily}, {left.AdapterVersion, right.AdapterVersion},
		{left.ContractSHA256, right.ContractSHA256}, {left.AdapterSHA256, right.AdapterSHA256},
		{string(left.CanaryRequirement), string(right.CanaryRequirement)},
	} {
		if result := cmp.Compare(values[0], values[1]); result != 0 {
			return result
		}
	}
	if result := cmp.Compare(left.CoverageStartNS, right.CoverageStartNS); result != 0 {
		return result
	}
	return cmp.Compare(left.CoverageEndNS, right.CoverageEndNS)
}

func compareQueryThreshold(left, right QueryThreshold) int {
	if result := cmp.Compare(left.ID, right.ID); result != 0 {
		return result
	}
	if result := cmp.Compare(left.MaxLatencyNS, right.MaxLatencyNS); result != 0 {
		return result
	}
	if result := cmp.Compare(left.ObservedLatencyNS, right.ObservedLatencyNS); result != 0 {
		return result
	}
	if result := cmp.Compare(left.MaxResponseBytes, right.MaxResponseBytes); result != 0 {
		return result
	}
	return cmp.Compare(left.ObservedResponseBytes, right.ObservedResponseBytes)
}

func compareFaultObservation(left, right FaultCategoryObservation) int {
	if result := cmp.Compare(faultCategoryRank(left.CategoryID), faultCategoryRank(right.CategoryID)); result != 0 {
		return result
	}
	if result := cmp.Compare(string(left.CategoryID), string(right.CategoryID)); result != 0 {
		return result
	}
	if result := compareBool(left.Passed, right.Passed); result != 0 {
		return result
	}
	if result := cmp.Compare(left.FailureCount, right.FailureCount); result != 0 {
		return result
	}
	return cmp.Compare(left.EvidenceSHA256, right.EvidenceSHA256)
}

func compareFaultCoverage(left, right ContractFaultCoverage) int {
	if result := cmp.Compare(left.ContractID, right.ContractID); result != 0 {
		return result
	}
	return compareFaultObservations(left.Categories, right.Categories)
}

func compareFaultObservations(left, right []FaultCategoryObservation) int {
	for i := range min(len(left), len(right)) {
		if result := compareFaultObservation(left[i], right[i]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(left), len(right))
}

func compareDeterminismObservation(left, right DeterminismObservation) int {
	if result := cmp.Compare(determinismGateRank(left.GateID), determinismGateRank(right.GateID)); result != 0 {
		return result
	}
	if result := cmp.Compare(string(left.GateID), string(right.GateID)); result != 0 {
		return result
	}
	if result := compareBool(left.Passed, right.Passed); result != 0 {
		return result
	}
	if result := cmp.Compare(left.MismatchCount, right.MismatchCount); result != 0 {
		return result
	}
	return cmp.Compare(left.EvidenceSHA256, right.EvidenceSHA256)
}

func compareDeterminismCoverage(left, right ContractDeterminismCoverage) int {
	if result := cmp.Compare(left.ContractID, right.ContractID); result != 0 {
		return result
	}
	for i := range min(len(left.Gates), len(right.Gates)) {
		if result := compareDeterminismObservation(left.Gates[i], right.Gates[i]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(left.Gates), len(right.Gates))
}

func faultCategoryRank(id FaultCategoryID) int {
	for index, required := range requiredFaultCategoryIDs {
		if id == required {
			return index
		}
	}
	return len(requiredFaultCategoryIDs)
}

func determinismGateRank(id DeterminismGateID) int {
	for index, required := range requiredDeterminismGateIDs {
		if id == required {
			return index
		}
	}
	return len(requiredDeterminismGateIDs)
}

func compareCanaryObservation(left, right CanaryObservation) int {
	if result := cmp.Compare(left.ContractID, right.ContractID); result != 0 {
		return result
	}
	if result := cmp.Compare(string(left.Status), string(right.Status)); result != 0 {
		return result
	}
	for _, values := range [][2]int64{
		{left.StartNS, right.StartNS}, {left.EndNS, right.EndNS}, {left.ObservedNS, right.ObservedNS},
		{left.ExplainedGapNS, right.ExplainedGapNS}, {left.UnexplainedGapNS, right.UnexplainedGapNS},
	} {
		if result := cmp.Compare(values[0], values[1]); result != 0 {
			return result
		}
	}
	for _, values := range [][2]uint64{
		{left.Raw.ExpectedRecords, right.Raw.ExpectedRecords}, {left.Raw.CommittedRecords, right.Raw.CommittedRecords},
		{left.Raw.ExplainedLossRecords, right.Raw.ExplainedLossRecords}, {left.Raw.UnexplainedLossRecords, right.Raw.UnexplainedLossRecords},
	} {
		if result := cmp.Compare(values[0], values[1]); result != 0 {
			return result
		}
	}
	return cmp.Compare(left.EvidenceSHA256, right.EvidenceSHA256)
}

func compareBool(left, right bool) int {
	if left == right {
		return 0
	}
	if !left {
		return -1
	}
	return 1
}
