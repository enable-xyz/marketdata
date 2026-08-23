package dataset

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/elastic/go-sysinfo"
	"github.com/elastic/go-sysinfo/types"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/spf13/fileflow"
	"github.com/spf13/pathologize"
)

const (
	FullX4Consent            = "execute ELMD-013 X4 1,000,000-row experiment"
	X4RowCount               = 1_000_000
	MaxX4EngineIdentityBytes = 512
)

type X4Configuration struct {
	RowGroupTargetBytes int64 `json:"row_group_target_bytes"`
	Dictionary          bool  `json:"dictionary"`
	BloomFilter         bool  `json:"bloom_filter"`
}

type X4InteropObservation struct {
	DuckDBPassed         bool   `json:"duckdb_passed"`
	PolarsPassed         bool   `json:"polars_passed"`
	ClickHousePassed     bool   `json:"clickhouse_passed"`
	DuckDBIdentity       string `json:"duckdb_identity"`
	PolarsIdentity       string `json:"polars_identity"`
	ClickHouseIdentity   string `json:"clickhouse_identity"`
	MeasurementsObserved bool   `json:"measurements_observed"`
	RangeReadBytes       uint64 `json:"range_read_bytes"`
	QueryRowsRead        uint64 `json:"query_rows_read"`
	BloomPrunedRows      uint64 `json:"bloom_pruned_rows"`
}

type X4Probe interface {
	Verify(context.Context, string, [32]byte, uint64) (X4InteropObservation, error)
}

type X4RunConfig struct {
	Consent          string
	OutputRoot       string
	ReportWriter     io.Writer
	InteropProbe     X4Probe
	MaxPeakRSSBytes  uint64
	DecisionCriteria X4DecisionCriteria
}

type X4CandidateReport struct {
	Configuration        X4Configuration      `json:"configuration"`
	ObservedMeasurements bool                 `json:"observed_measurements"`
	Rows                 uint64               `json:"rows"`
	RowGroups            uint64               `json:"row_groups"`
	Pages                uint64               `json:"pages"`
	Values               uint64               `json:"values"`
	FileBytes            int64                `json:"file_bytes"`
	PhysicalSHA256       string               `json:"physical_sha256"`
	LogicalSHA256        string               `json:"logical_sha256"`
	BaselineRSSBytes     uint64               `json:"baseline_rss_bytes"`
	PeakRSSBytes         uint64               `json:"peak_rss_bytes"`
	PeakRSSDeltaBytes    uint64               `json:"peak_rss_delta_bytes"`
	AllocatedBytes       uint64               `json:"allocated_bytes"`
	MaximumWriterBytes   int64                `json:"maximum_writer_accounted_bytes"`
	Interop              X4InteropObservation `json:"interop"`
	ParquetFile          string               `json:"parquet_file"`
}

type X4Report struct {
	ExperimentID         string              `json:"experiment_id"`
	ObservedMeasurements bool                `json:"observed_measurements"`
	Rows                 uint64              `json:"rows"`
	GOOS                 string              `json:"goos"`
	GOARCH               string              `json:"goarch"`
	Candidates           []X4CandidateReport `json:"candidates"`
	Decision             X4Decision          `json:"decision"`
}

type X4DecisionCriteria struct {
	MaxPeakRSSBytes        uint64
	MaxRangeReadBytes      uint64
	MaxQueryRowsRead       uint64
	MinimumBloomPrunedRows uint64
}

type X4MeasuredDecisionVector struct {
	Configuration    X4Configuration
	PeakRSSBytes     uint64
	RangeReadBytes   uint64
	QueryRowsRead    uint64
	BloomPrunedRows  uint64
	DuckDBPassed     bool
	PolarsPassed     bool
	ClickHousePassed bool
}

type X4Decision struct {
	Status              string          `json:"status"`
	Configuration       X4Configuration `json:"configuration"`
	Compression         string          `json:"compression"`
	ParquetGoVersion    string          `json:"parquet_go_version"`
	FormatCompatibility string          `json:"format_compatibility"`
	Reason              string          `json:"reason"`
}

func FrozenX4Decision() X4Decision {
	return X4Decision{Status: "default_retained_pending_qualifying_measured_vectors",
		Configuration: X4Configuration{RowGroupTargetBytes: DefaultRowGroupBytes}, Compression: "zstd",
		ParquetGoVersion: ParquetWriterVersion, FormatCompatibility: ParquetFormatCompatibility,
		Reason: "No committed measured vector proves a smaller qualifying configuration or bloom-filter pruning benefit."}
}

func DecideX4(criteria X4DecisionCriteria, vectors []X4MeasuredDecisionVector) (X4Decision, error) {
	if criteria.MaxPeakRSSBytes == 0 || criteria.MaxRangeReadBytes == 0 || criteria.MaxQueryRowsRead == 0 {
		return X4Decision{}, fmt.Errorf("%w: X4 decision budgets are required", ErrInvalidInput)
	}
	var qualified []X4MeasuredDecisionVector
	for _, vector := range vectors {
		if !validX4Configuration(vector.Configuration) {
			return X4Decision{}, fmt.Errorf("%w: X4 configuration", ErrInvalidInput)
		}
		if !vector.DuckDBPassed || !vector.PolarsPassed || !vector.ClickHousePassed || vector.PeakRSSBytes > criteria.MaxPeakRSSBytes ||
			vector.RangeReadBytes > criteria.MaxRangeReadBytes || vector.QueryRowsRead > criteria.MaxQueryRowsRead {
			continue
		}
		if vector.Configuration.BloomFilter && vector.BloomPrunedRows < criteria.MinimumBloomPrunedRows {
			continue
		}
		qualified = append(qualified, vector)
	}
	if len(qualified) == 0 {
		return FrozenX4Decision(), nil
	}
	best := qualified[0]
	for _, candidate := range qualified[1:] {
		if candidate.Configuration.RowGroupTargetBytes < best.Configuration.RowGroupTargetBytes ||
			(candidate.Configuration.RowGroupTargetBytes == best.Configuration.RowGroupTargetBytes && !candidate.Configuration.BloomFilter && best.Configuration.BloomFilter) ||
			(candidate.Configuration == best.Configuration && candidate.PeakRSSBytes < best.PeakRSSBytes) {
			best = candidate
		}
	}
	return X4Decision{Status: "measured_vector_selected", Configuration: best.Configuration, Compression: "zstd",
		ParquetGoVersion: ParquetWriterVersion, FormatCompatibility: ParquetFormatCompatibility,
		Reason: "Selected the smallest measured configuration meeting caller-supplied RSS, range-read, query, and three-engine interoperability budgets."}, nil
}

func RunFullX4(ctx context.Context, config X4RunConfig) (X4Report, error) {
	if config.Consent != FullX4Consent || config.OutputRoot == "" || config.ReportWriter == nil || config.InteropProbe == nil ||
		config.MaxPeakRSSBytes == 0 || config.DecisionCriteria.MaxPeakRSSBytes == 0 ||
		config.DecisionCriteria.MaxRangeReadBytes == 0 || config.DecisionCriteria.MaxQueryRowsRead == 0 {
		return X4Report{}, fmt.Errorf("%w: full X4 requires exact consent, output, report writer, interop probe, RSS bound, and decision budgets", ErrInvalidInput)
	}
	process, err := sysinfo.Self()
	if err != nil {
		return X4Report{}, fmt.Errorf("dataset: inspect process for X4: %w", err)
	}
	report := X4Report{ExperimentID: "ELMD-013-X4-full-v1", ObservedMeasurements: true, Rows: X4RowCount,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Candidates: make([]X4CandidateReport, 0, 12)}
	vectors := make([]X4MeasuredDecisionVector, 0, 12)
	for _, candidate := range X4Configurations() {
		run, err := runX4Candidate(ctx, process, config, candidate)
		if err != nil {
			return X4Report{}, err
		}
		report.Candidates = append(report.Candidates, run)
		vectors = append(vectors, X4MeasuredDecisionVector{Configuration: run.Configuration, PeakRSSBytes: run.PeakRSSBytes,
			RangeReadBytes: run.Interop.RangeReadBytes, QueryRowsRead: run.Interop.QueryRowsRead,
			BloomPrunedRows: run.Interop.BloomPrunedRows, DuckDBPassed: run.Interop.DuckDBPassed,
			PolarsPassed: run.Interop.PolarsPassed, ClickHousePassed: run.Interop.ClickHousePassed})
	}
	report.Decision, err = DecideX4(config.DecisionCriteria, vectors)
	if err != nil {
		return X4Report{}, err
	}
	encoder := json.NewEncoder(config.ReportWriter)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return X4Report{}, fmt.Errorf("dataset: write X4 report: %w", err)
	}
	return report, nil
}

func X4Configurations() []X4Configuration {
	result := make([]X4Configuration, 0, 12)
	for _, bytes := range []int64{64 << 20, 256 << 20, 1024 << 20} {
		for _, dictionary := range []bool{false, true} {
			for _, bloom := range []bool{false, true} {
				result = append(result, X4Configuration{RowGroupTargetBytes: bytes, Dictionary: dictionary, BloomFilter: bloom})
			}
		}
	}
	return result
}

func validX4Configuration(value X4Configuration) bool {
	return value.RowGroupTargetBytes == 64<<20 || value.RowGroupTargetBytes == 256<<20 || value.RowGroupTargetBytes == 1024<<20
}

func validateX4InteropObservation(value X4InteropObservation) error {
	if !value.DuckDBPassed || !value.PolarsPassed || !value.ClickHousePassed ||
		!value.MeasurementsObserved || value.RangeReadBytes == 0 || value.QueryRowsRead == 0 {
		return fmt.Errorf("dataset: X4 cross-reader or measured probe mismatch")
	}
	identities := [...]struct {
		name, value string
	}{
		{"DuckDB", value.DuckDBIdentity},
		{"Polars", value.PolarsIdentity},
		{"ClickHouse", value.ClickHouseIdentity},
	}
	for _, identity := range identities {
		if identity.value == "" || len(identity.value) > MaxX4EngineIdentityBytes ||
			!utf8.ValidString(identity.value) || strings.IndexByte(identity.value, 0) >= 0 {
			return fmt.Errorf("dataset: X4 %s engine identity is empty, oversized, or invalid UTF-8", identity.name)
		}
	}
	return nil
}

type x4ExperimentRow struct {
	EventID          [32]byte `parquet:"event_id"`
	SourceID         string   `parquet:"source_id"`
	Family           string   `parquet:"family,enum"`
	InstrumentUID    string   `parquet:"instrument_uid"`
	EpochID          [16]byte `parquet:"epoch_id"`
	ArrivalOrdinal   uint64   `parquet:"arrival_ordinal,uint(64)"`
	MessageOrdinal   uint32   `parquet:"message_ordinal,uint(32)"`
	ReceivedTimeNS   int64    `parquet:"received_time_ns,timestamp(nanosecond:utc)"`
	Price            [16]byte `parquet:"price,decimal(18:38)"`
	Amount           [16]byte `parquet:"amount,decimal(18:38)"`
	RawPayloadSHA256 [32]byte `parquet:"raw_payload_sha256"`
	QualityFlags     []string `parquet:"quality_flags,list"`
}

func runX4Candidate(ctx context.Context, process types.Process, config X4RunConfig, candidate X4Configuration) (X4CandidateReport, error) {
	baseline, err := process.Memory()
	if err != nil {
		return X4CandidateReport{}, err
	}
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	run := X4CandidateReport{Configuration: candidate, ObservedMeasurements: true, Rows: X4RowCount,
		BaselineRSSBytes: baseline.Resident, PeakRSSBytes: baseline.Resident}
	root := pathologize.Join(config.OutputRoot, "x4-v1")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return run, err
	}
	staging, err := os.CreateTemp(root, ".x4-staging.*")
	if err != nil {
		return run, err
	}
	stagingPath := staging.Name()
	defer os.Remove(stagingPath)
	schema := parquet.SchemaOf(new(x4ExperimentRow))
	options := []parquet.WriterOption{schema, parquet.CreatedBy("enable-market-dataset-x4", "1", ParquetWriterVersion),
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}), parquet.PageBufferSize(DefaultPageBufferBytes),
		parquet.DataPageVersion(2), parquet.DataPageStatistics(true), parquet.ColumnPageBuffers(parquet.NewFileBufferPool(root, ".x4-pages.*"))}
	if candidate.Dictionary {
		options = append(options, parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary), parquet.DictionaryMaxBytes(16<<20))
	}
	if candidate.BloomFilter {
		options = append(options, parquet.BloomFilters(parquet.SplitBlockFilter(10, "event_id")))
	}
	writer := parquet.NewGenericWriter[x4ExperimentRow](staging, options...)
	logical := sha256.New()
	_, _ = logical.Write([]byte("dataset-x4-production-shaped-v1\x00"))
	batch := make([]x4ExperimentRow, 1024)
	lastFlush := int64(0)
	for base := uint64(0); base < X4RowCount; base += uint64(len(batch)) {
		if err := ctx.Err(); err != nil {
			_ = writer.Close()
			_ = staging.Close()
			return run, err
		}
		count := min(uint64(len(batch)), X4RowCount-base)
		for i := range int(count) {
			batch[i] = deterministicX4Row(base + uint64(i))
			_, _ = logical.Write(batch[i].EventID[:])
		}
		if _, err := writer.Write(batch[:count]); err != nil {
			_ = writer.Close()
			_ = staging.Close()
			return run, err
		}
		run.MaximumWriterBytes = max(run.MaximumWriterBytes, writer.Size()-lastFlush)
		if writer.Size()-lastFlush >= candidate.RowGroupTargetBytes {
			if err := writer.Flush(); err != nil {
				_ = writer.Close()
				_ = staging.Close()
				return run, err
			}
			lastFlush = writer.Size()
		}
		memory, err := process.Memory()
		if err != nil {
			_ = writer.Close()
			_ = staging.Close()
			return run, err
		}
		run.PeakRSSBytes = max(run.PeakRSSBytes, memory.Resident)
		if run.PeakRSSBytes > config.MaxPeakRSSBytes {
			_ = writer.Close()
			_ = staging.Close()
			return run, fmt.Errorf("dataset: X4 RSS bound exceeded")
		}
	}
	logicalHash := sumHash(logical)
	writer.SetKeyValueMetadata("enable.x4_logical_sha256", hex.EncodeToString(logicalHash[:]))
	if err := writer.Close(); err != nil {
		_ = staging.Close()
		return run, err
	}
	if err := staging.Sync(); err != nil {
		_ = staging.Close()
		return run, err
	}
	if err := staging.Close(); err != nil {
		return run, err
	}
	physical, bytes, err := hashFileContext(ctx, stagingPath)
	if err != nil {
		return run, err
	}
	name := "candidate-rg" + strconv.FormatInt(candidate.RowGroupTargetBytes>>20, 10) + "-dict" + strconv.FormatBool(candidate.Dictionary) + "-bloom" + strconv.FormatBool(candidate.BloomFilter) + "-" + hex.EncodeToString(physical[:]) + ".parquet"
	final := filepath.Join(root, name)
	moved, err := fileflow.Move(stagingPath, final)
	if err != nil {
		return run, err
	}
	if moved != final {
		return run, fmt.Errorf("dataset: X4 content identity collision")
	}
	inspection, err := inspectParquet(ctx, final)
	if err != nil {
		return run, err
	}
	interop, err := config.InteropProbe.Verify(ctx, final, logicalHash, X4RowCount)
	if err != nil {
		return run, err
	}
	if err := validateX4InteropObservation(interop); err != nil {
		return run, err
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	run.RowGroups, run.Pages, run.Values, run.FileBytes = inspection.rowGroups, inspection.pages, inspection.values, bytes
	run.PhysicalSHA256, run.LogicalSHA256, run.Interop = hex.EncodeToString(physical[:]), hex.EncodeToString(logicalHash[:]), interop
	run.PeakRSSDeltaBytes = run.PeakRSSBytes - min(run.PeakRSSBytes, run.BaselineRSSBytes)
	run.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	relative, _ := filepath.Rel(config.OutputRoot, final)
	run.ParquetFile = filepath.ToSlash(relative)
	return run, nil
}

func deterministicX4Row(ordinal uint64) x4ExperimentRow {
	var ordinalBytes [8]byte
	binary.BigEndian.PutUint64(ordinalBytes[:], ordinal)
	eventID := sha256.Sum256(append([]byte("x4-event-v1\x00"), ordinalBytes[:]...))
	rawHash := sha256.Sum256(append([]byte("x4-raw-v1\x00"), ordinalBytes[:]...))
	var epoch [16]byte
	copy(epoch[:], eventID[:16])
	var price, amount [16]byte
	binary.BigEndian.PutUint64(price[8:], 10_000_000_000_000_000_000+ordinal%1_000_000)
	binary.BigEndian.PutUint64(amount[8:], 1_000_000_000_000_000_000+ordinal%10_000)
	flags := []string(nil)
	if ordinal%97 == 0 {
		flags = []string{"clock_uncertain"}
	}
	return x4ExperimentRow{EventID: eventID, SourceID: "binance-spot-api-v3", Family: []string{"trade", "quote", "ticker", "book_update"}[ordinal%4],
		InstrumentUID: "instrument-" + strconv.FormatUint(ordinal%1024, 10), EpochID: epoch, ArrivalOrdinal: ordinal + 1,
		MessageOrdinal: uint32(ordinal % 4), ReceivedTimeNS: 1_700_000_000_000_000_000 + int64(ordinal)*100_000,
		Price: price, Amount: amount, RawPayloadSHA256: rawHash, QualityFlags: flags}
}
