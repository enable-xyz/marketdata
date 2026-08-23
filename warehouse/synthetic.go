package warehouse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	X5Rows             uint64 = 10_000_000
	X5RowsPerManifest  uint64 = 100_000
	X5DisconnectPoints        = 100
)

type SyntheticDatasetConfig struct {
	TotalRows       uint64
	RowsPerManifest uint64
	StartTime       time.Time
}

func DefaultX5SyntheticDatasetConfig() SyntheticDatasetConfig {
	return SyntheticDatasetConfig{TotalRows: X5Rows, RowsPerManifest: X5RowsPerManifest,
		StartTime: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)}
}

func (c SyntheticDatasetConfig) validate() error {
	if c.TotalRows == 0 || c.TotalRows > X5Rows || c.RowsPerManifest == 0 ||
		c.RowsPerManifest > dataset.MaxPartitionInputRows || c.TotalRows%c.RowsPerManifest != 0 ||
		c.StartTime.IsZero() || c.StartTime.Location() != time.UTC {
		return fmt.Errorf("%w: bounded synthetic dataset configuration", ErrInvalidWarehouseInput)
	}
	return nil
}

// GenerateSyntheticDataset creates deterministic trade Parquet partitions and
// returns explicit synthetic committed-manifest assertions. X5 callers use the
// exact DefaultX5SyntheticDatasetConfig; smaller bounded configurations exist
// only for changed-contract tests.
func GenerateSyntheticDataset(ctx context.Context, root string, config SyntheticDatasetConfig) ([]CommittedManifest, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: synthetic dataset root", ErrInvalidWarehouseInput)
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	datasetPolicy := syntheticHash("x5-dataset-policy-v1")
	replayConfig := syntheticHash("x5-replay-config-v1")
	inputSet := syntheticHash("x5-input-manifest-set-v1")
	options := dataset.DefaultWriterOptions(datasetPolicy, replayConfig, inputSet)
	options.MaxInputRows = config.RowsPerManifest
	options.MaxParquetRows = config.RowsPerManifest
	manifestCount := config.TotalRows / config.RowsPerManifest
	manifests := make([]CommittedManifest, 0, manifestCount)
	for manifestIndex := range manifestCount {
		source := &syntheticTradeSource{start: config.StartTime.Add(time.Duration(manifestIndex) * time.Hour),
			manifestIndex: manifestIndex, rows: config.RowsPerManifest}
		result, err := dataset.BuildNormalizedPartition(ctx, root, source, options)
		if err != nil {
			return nil, fmt.Errorf("warehouse: generate X5 Parquet partition %d: %w", manifestIndex, err)
		}
		manifests = append(manifests, CommittedManifest{Root: root, ManifestPath: result.ManifestPath,
			ManifestSHA256: Hash(result.ManifestHash), State: ManifestCommitted})
	}
	return manifests, nil
}

type syntheticTradeSource struct {
	start         time.Time
	manifestIndex uint64
	rows          uint64
	next          uint64
}

func (s *syntheticTradeSource) Next(context.Context) (normalize.Row, error) {
	if s.next == s.rows {
		return normalize.Row{}, io.EOF
	}
	index := s.next
	s.next++
	arrival := index + 1
	var epoch [16]byte
	binary.BigEndian.PutUint64(epoch[:8], s.manifestIndex+1)
	copy(epoch[8:], []byte("x5epoch!"))
	received := s.start.Add(time.Duration(index) * time.Microsecond).UnixNano()
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: "x5-synthetic-source", ChannelOrEndpoint: "x5-trades-v1",
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true}, ArrivalOrdinal: arrival,
		MessageOrdinal: 0, ExchangeTimeNS: capture.OptionalInt64{Value: received - int64(time.Millisecond), Valid: true},
		ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: received,
		ClockEpochID: "x5-synthetic-clock", MonotonicNSSinceClockEpoch: arrival * uint64(time.Microsecond),
		PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved,
		RecorderVersion: "x5-synthetic-recorder-v1",
	}
	envelope.SetRawPayload([]byte(`{"synthetic":"x5"}`))
	record, err := normalize.BindRawRecord(envelope, syntheticHash(fmt.Sprintf("x5-segment-%06d", s.manifestIndex)), arrival, nil)
	if err != nil {
		return normalize.Row{}, err
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{
		Record: record, SchemaName: normalize.TradeSchemaName, SchemaVersion: normalize.TradeSchemaVersion,
		InstrumentUID:          fmt.Sprintf("x5-instrument-%04d", index%1_000),
		ExchangeTimeNS:         normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true},
		ExchangeTimeResolution: normalize.ResolutionMillisecond,
		SourceEventTimeNS:      normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true},
		SourceTimeResolution:   normalize.ResolutionMillisecond, SourceSchemaFingerprint: syntheticHash("x5-source-schema-v1"),
		MapperVersion: "x5-mapper-v1", MapperBindingID: syntheticHash("x5-mapper-binding-v1"),
		CatalogSnapshotID: syntheticHash("x5-catalog-snapshot-v1"),
	})
	if err != nil {
		return normalize.Row{}, err
	}
	price := normalize.Numeric{Decimal: normalize.Decimal{Coefficient: fmt.Sprintf("%d%016d", 100+index%100_000, 0), Scale: 18},
		Unit: normalize.SpotPriceUnit("x5-base", "x5-quote")}
	amount := normalize.Numeric{Decimal: normalize.Decimal{Coefficient: fmt.Sprintf("%d%016d", 1+index%100, 0), Scale: 18},
		Unit: normalize.BaseAssetUnit("x5-base")}
	return normalize.NewTradeRow(normalize.TradeV1{Metadata: metadata, NativeTradeID: s.manifestIndex*s.rows + arrival,
		AggressorSide: normalize.SideBuy, Price: price, Amount: amount, AggregationKind: normalize.AggregationSingleMatch,
		NativeDuplicateStatus: normalize.DuplicateUnassessed})
}

func syntheticHash(value string) normalize.Hash {
	return sha256.Sum256([]byte(value))
}
