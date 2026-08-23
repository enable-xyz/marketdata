package dataset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/fileflow"
)

const MaxInteropOutputBytes = 1 << 20

type InteropVector struct {
	EventOrdinal  uint64
	EventID       string
	NativeTradeID uint64
	Price         string
	PriceType     string
	Amount        string
	AmountType    string
	SourceID      string
	InstrumentUID string
}

func InteropExpectedVectors() ([]InteropVector, error) {
	rows, err := interopNormalizedRows()
	if err != nil {
		return nil, err
	}
	result := make([]InteropVector, len(rows))
	for i, row := range rows {
		event := row.Trade
		result[i] = InteropVector{EventOrdinal: uint64(i), EventID: hex.EncodeToString(event.Metadata.EventID[:]),
			NativeTradeID: event.NativeTradeID, Price: decimalText(event.Price.Decimal), PriceType: "DECIMAL(38,18)",
			Amount: decimalText(event.Amount.Decimal), AmountType: "DECIMAL(38,18)", SourceID: event.Metadata.SourceID,
			InstrumentUID: event.Metadata.InstrumentUID}
	}
	return result, nil
}

func WriteInteropGolden(path string) ([32]byte, error) {
	var zero [32]byte
	if path == "" {
		return zero, fmt.Errorf("%w: golden path", ErrInvalidInput)
	}
	rows, err := interopNormalizedRows()
	if err != nil {
		return zero, err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return zero, err
	}
	temp, err := os.CreateTemp(directory, ".interop-golden.*")
	if err != nil {
		return zero, err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	policy := normalize.Hash(sha256.Sum256([]byte("interop-dataset-policy-v1")))
	config := normalize.Hash(sha256.Sum256([]byte("interop-replay-config-v1")))
	inputs := normalize.Hash(sha256.Sum256([]byte("interop-input-manifest-set-v1")))
	options := DefaultWriterOptions(policy, config, inputs)
	writer, schemaName, schemaVersion, schema, err := newNormalizedWriter(temp, FamilyTrade, options, directory)
	if err != nil {
		_ = temp.Close()
		return zero, err
	}
	schemaHash := schemaDigest(schemaName, schemaVersion, schema)
	setStaticMetadata(writer, schemaName, schemaVersion, schemaHash, options)
	logical := sha256.New()
	_, _ = logical.Write([]byte("dataset-normalized-partition-logical-v1\x00"))
	for i, row := range rows {
		if _, err := writeNormalizedRow(writer, row, uint64(i), options); err != nil {
			_ = writer.Close()
			_ = temp.Close()
			return zero, err
		}
		_, _ = logical.Write(row.LogicalHash[:])
	}
	logicalHash := sumHash(logical)
	writer.SetKeyValueMetadata("enable.logical_sha256", hex.EncodeToString(logicalHash[:]))
	writer.SetKeyValueMetadata("enable.input_rows", strconv.Itoa(len(rows)))
	writer.SetKeyValueMetadata("enable.parquet_rows", strconv.Itoa(len(rows)))
	if err := writer.Close(); err != nil {
		_ = temp.Close()
		return zero, err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return zero, err
	}
	if err := temp.Close(); err != nil {
		return zero, err
	}
	digest, _, err := hashFile(tempPath)
	if err != nil {
		return zero, err
	}
	final, err := fileflow.Move(tempPath, path)
	if err != nil {
		return zero, err
	}
	if final != path {
		return zero, fmt.Errorf("dataset: golden path contains different bytes; generated file is %s", final)
	}
	return digest, nil
}

func RunDuckDBVector(ctx context.Context, engine, parquetPath string) error {
	if engine == "" || parquetPath == "" {
		return fmt.Errorf("%w: explicit DuckDB engine and parquet path are required", ErrInvalidInput)
	}
	query := "SELECT event_ordinal, lower(hex(event_id)), native_trade_id, CAST(price AS VARCHAR), typeof(price), CAST(amount AS VARCHAR), typeof(amount), source_id, instrument_uid FROM read_parquet('" +
		strings.ReplaceAll(parquetPath, "'", "''") + "') ORDER BY event_ordinal"
	stdout, stderr, err := runBoundedCommand(ctx, engine, "-csv", "-noheader", "-c", query)
	if err != nil {
		return fmt.Errorf("dataset: DuckDB vector failed: %w: %s", err, stderr)
	}
	actual, err := parseInteropCSV(stdout)
	if err != nil {
		return fmt.Errorf("dataset: parse DuckDB vector: %w", err)
	}
	return compareInterop(actual)
}

func RunPolarsVector(ctx context.Context, python, parquetPath string) error {
	if python == "" || parquetPath == "" {
		return fmt.Errorf("%w: explicit Polars Python engine and parquet path are required", ErrInvalidInput)
	}
	script := `import csv,sys,polars as pl
p=sys.argv[1]
df=pl.read_parquet(p).sort("event_ordinal")
required={"event_ordinal","event_id","native_trade_id","price","amount","source_id","instrument_uid"}
if not required.issubset(set(df.columns)): raise SystemExit("missing expected columns")
pt=str(df.schema["price"]); at=str(df.schema["amount"])
if "Decimal" not in pt or "38" not in pt or "18" not in pt: raise SystemExit("price decimal type mismatch: "+pt)
if "Decimal" not in at or "38" not in at or "18" not in at: raise SystemExit("amount decimal type mismatch: "+at)
w=csv.writer(sys.stdout,lineterminator="\n")
for r in df.select(["event_ordinal","event_id","native_trade_id","price","amount","source_id","instrument_uid"]).iter_rows():
 w.writerow([r[0],bytes(r[1]).hex(),r[2],str(r[3]),"DECIMAL(38,18)",str(r[4]),"DECIMAL(38,18)",r[5],r[6]])`
	stdout, stderr, err := runBoundedCommand(ctx, python, "-c", script, parquetPath)
	if err != nil {
		return fmt.Errorf("dataset: Polars vector failed: %w: %s", err, stderr)
	}
	actual, err := parseInteropCSV(stdout)
	if err != nil {
		return fmt.Errorf("dataset: parse Polars vector: %w", err)
	}
	return compareInterop(actual)
}

func parseInteropCSV(data string) ([]InteropVector, error) {
	reader := csv.NewReader(strings.NewReader(data))
	reader.FieldsPerRecord = 9
	var result []InteropVector
	for {
		fields, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		ordinal, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, err
		}
		tradeID, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return nil, err
		}
		result = append(result, InteropVector{EventOrdinal: ordinal, EventID: strings.ToLower(fields[1]), NativeTradeID: tradeID,
			Price: fields[3], PriceType: strings.ToUpper(fields[4]), Amount: fields[5], AmountType: strings.ToUpper(fields[6]),
			SourceID: fields[7], InstrumentUID: fields[8]})
		if len(result) > 100 {
			return nil, fmt.Errorf("interop row bound")
		}
	}
	return result, nil
}

func compareInterop(actual []InteropVector) error {
	expected, err := InteropExpectedVectors()
	if err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("dataset: interoperability row count got %d want %d", len(actual), len(expected))
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return fmt.Errorf("dataset: interoperability row %d mismatch: got %+v want %+v", i, actual[i], expected[i])
		}
	}
	return nil
}

type boundedBuffer struct {
	buffer   bytes.Buffer
	maximum  int
	exceeded bool
}

func (b *boundedBuffer) Write(value []byte) (int, error) {
	if b.buffer.Len()+len(value) > b.maximum {
		b.exceeded = true
		return 0, fmt.Errorf("output exceeds %d bytes", b.maximum)
	}
	return b.buffer.Write(value)
}

func runBoundedCommand(ctx context.Context, name string, args ...string) (string, string, error) {
	command := exec.CommandContext(ctx, name, args...)
	stdout := &boundedBuffer{maximum: MaxInteropOutputBytes}
	stderr := &boundedBuffer{maximum: MaxInteropOutputBytes}
	command.Stdout, command.Stderr = stdout, stderr
	err := command.Run()
	if stdout.exceeded || stderr.exceeded {
		return "", stderr.buffer.String(), fmt.Errorf("engine output bound")
	}
	return stdout.buffer.String(), stderr.buffer.String(), err
}

func interopNormalizedRows() ([]normalize.Row, error) {
	rows := make([]normalize.Row, 2)
	for i := range rows {
		arrival := uint64(i + 1)
		payload := []byte(`{"e":"trade","s":"BTCUSDT"}`)
		var epoch [16]byte
		copy(epoch[:], []byte("interop-epoch-v1"))
		envelope := capture.EnvelopeV1{EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
			SourceID: "binance-spot-api-v3", ChannelOrEndpoint: "spot-ws-trade-v1", ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true},
			ArrivalOrdinal: arrival, MessageOrdinal: 0, ExchangeTimeNS: capture.OptionalInt64{Value: 1_700_000_000_000_000_000 + int64(i), Valid: true},
			ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: 1_700_000_000_100_000_000 + int64(i),
			ClockEpochID: "interop-clock-v1", MonotonicNSSinceClockEpoch: uint64(i), PayloadEncoding: capture.PayloadEncodingJSON,
			TerminalOutcome: capture.TerminalObserved, RecorderVersion: "interop-recorder-v1"}
		envelope.SetRawPayload(payload)
		rawSegment := normalize.Hash(sha256.Sum256([]byte("interop-segment-" + strconv.Itoa(i))))
		record, err := normalize.BindRawRecord(envelope, rawSegment, arrival, nil)
		if err != nil {
			return nil, err
		}
		fingerprint := normalize.Hash(sha256.Sum256([]byte("interop-schema-v1")))
		binding := normalize.Hash(sha256.Sum256([]byte("interop-binding-v1")))
		catalog := normalize.Hash(sha256.Sum256([]byte("interop-catalog-v1")))
		metadata, err := normalize.NewMetadata(normalize.MetadataInput{Record: record, SchemaName: normalize.TradeSchemaName,
			SchemaVersion: normalize.TradeSchemaVersion, InstrumentUID: "instrument-btcusdt-v1",
			ExchangeTimeNS: normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true}, ExchangeTimeResolution: normalize.ResolutionMillisecond,
			SourceEventTimeNS: normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond,
			SourceSchemaFingerprint: fingerprint, MapperVersion: "binance-spot-mapper-v1", MapperBindingID: binding, CatalogSnapshotID: catalog})
		if err != nil {
			return nil, err
		}
		priceText := []string{"123.45", "124.00"}[i]
		amountText := []string{"0.125", "2.5"}[i]
		price, err := normalize.ParseDecimal(priceText, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
		if err != nil {
			return nil, err
		}
		amount, err := normalize.ParseDecimal(amountText, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
		if err != nil {
			return nil, err
		}
		row, err := normalize.NewTradeRow(normalize.TradeV1{Metadata: metadata, NativeTradeID: uint64(1001 + i), AggressorSide: []normalize.Side{normalize.SideBuy, normalize.SideSell}[i],
			BuyerIsMaker: i == 1, Price: normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit("asset-btc", "asset-usdt")},
			Amount: normalize.Numeric{Decimal: amount, Unit: normalize.BaseAssetUnit("asset-btc")}, AggregationKind: normalize.AggregationSingleMatch,
			NativeDuplicateStatus: normalize.DuplicateUnassessed})
		if err != nil {
			return nil, err
		}
		rows[i] = row
	}
	return rows, nil
}

func decimalText(value normalize.Decimal) string {
	coefficient := value.Coefficient
	negative := strings.HasPrefix(coefficient, "-")
	if negative {
		coefficient = strings.TrimPrefix(coefficient, "-")
	}
	for len(coefficient) <= int(value.Scale) {
		coefficient = "0" + coefficient
	}
	position := len(coefficient) - int(value.Scale)
	result := coefficient[:position]
	if value.Scale > 0 {
		result += "." + coefficient[position:]
	}
	if negative {
		result = "-" + result
	}
	return result
}

func goldenSchema() *parquet.Schema { return parquet.SchemaOf(new(tradeParquetRow)) }
