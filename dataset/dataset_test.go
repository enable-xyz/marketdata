package dataset

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

func TestParquetRoundTrip(t *testing.T) {
	rows := testNormalizedRows(t)
	for _, row := range rows {
		row := row
		t.Run(string(row.Kind), func(t *testing.T) {
			options := testWriterOptions()
			root := t.TempDir()
			result, err := BuildNormalizedPartition(t.Context(), root, &SliceNormalizedSource{Rows: []normalize.Row{row}}, options)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			verified, err := VerifyManifest(t.Context(), root, result.ManifestPath)
			if err != nil {
				t.Fatalf("verify: %v", err)
			}
			if verified.Manifest.InputRows != 1 || verified.Manifest.LogicalSHA256 != result.Manifest.LogicalSHA256 || verified.Manifest.PhysicalSHA256 != result.Manifest.PhysicalSHA256 {
				t.Fatalf("round trip identity mismatch: %+v", verified.Manifest)
			}
		})
	}

	t.Run("schema_quarantine", func(t *testing.T) {
		metadata := rows[0].Common()
		quarantine := normalize.SchemaQuarantineV1{Version: normalize.SchemaQuarantineVersion,
			QuarantineID: testHash("quarantine"), Code: normalize.QuarantineSchemaUnknown, Field: "unexpected",
			SourceState: normalize.SourceValue, FingerprintClass: normalize.FingerprintUnknown,
			SourceSchemaFingerprint: metadata.SourceSchemaFingerprint, SourceID: metadata.SourceID, ChannelID: metadata.ChannelID,
			ReceivedTimeNS: metadata.ReceivedTimeNS, Coordinate: testCoordinate(metadata), MapperVersion: metadata.MapperVersion,
			MapperBindingID: metadata.MapperBindingID, SourceTimeResolution: metadata.SourceTimeResolution, CatalogSnapshotID: metadata.CatalogSnapshotID}
		root := t.TempDir()
		result, err := BuildQuarantinePartition(t.Context(), root, &SliceQuarantineSource{Rows: []normalize.SchemaQuarantineV1{quarantine}}, testWriterOptions())
		if err != nil {
			t.Fatalf("build quarantine: %v", err)
		}
		if _, err := VerifyManifest(t.Context(), root, result.ManifestPath); err != nil {
			t.Fatalf("verify quarantine: %v", err)
		}
	})

	t.Run("quality", func(t *testing.T) {
		metadata := rows[0].Common()
		quality := QualityRowV1{Version: QualitySchemaVersion, QualityID: testHash("quality"), Kind: "schema_observation", Code: "additive_field",
			SourceState: normalize.SourceValue, SourceSchemaFingerprint: metadata.SourceSchemaFingerprint, SchemaName: metadata.SchemaName,
			SchemaVersion: metadata.SchemaVersion, SourceID: metadata.SourceID, ChannelID: metadata.ChannelID,
			InstrumentUID: metadata.InstrumentUID, InstrumentUIDState: normalize.SourceValue,
			ReceivedTimeNS: metadata.ReceivedTimeNS, Coordinate: testCoordinate(metadata), MapperVersion: metadata.MapperVersion,
			MapperBindingID: metadata.MapperBindingID, CatalogSnapshotID: metadata.CatalogSnapshotID, PolicyID: testHash("quality-policy"),
			QualityFlags: []string{"schema_additive_field"}}
		root := t.TempDir()
		result, err := BuildQualityPartition(t.Context(), root, &SliceQualitySource{Rows: []QualityRowV1{quality}}, testWriterOptions())
		if err != nil {
			t.Fatalf("build quality: %v", err)
		}
		if _, err := VerifyManifest(t.Context(), root, result.ManifestPath); err != nil {
			t.Fatalf("verify quality: %v", err)
		}
	})
}

func TestPartitionDeterminism(t *testing.T) {
	rows := testNormalizedRows(t)
	for _, row := range rows {
		row := row
		t.Run(string(row.Kind), func(t *testing.T) {
			firstRoot, secondRoot := t.TempDir(), t.TempDir()
			first, err := BuildNormalizedPartition(t.Context(), firstRoot, &SliceNormalizedSource{Rows: []normalize.Row{row}}, testWriterOptions())
			if err != nil {
				t.Fatalf("first build: %v", err)
			}
			second, err := BuildNormalizedPartition(t.Context(), secondRoot, &SliceNormalizedSource{Rows: []normalize.Row{row}}, testWriterOptions())
			if err != nil {
				t.Fatalf("second build: %v", err)
			}
			if first.Manifest.LogicalSHA256 != second.Manifest.LogicalSHA256 || first.Manifest.PhysicalSHA256 != second.Manifest.PhysicalSHA256 || first.Manifest.BuildID != second.Manifest.BuildID {
				t.Fatalf("determinism mismatch:\nfirst=%+v\nsecond=%+v", first.Manifest, second.Manifest)
			}
			firstBytes, err := os.ReadFile(first.ParquetPath)
			if err != nil {
				t.Fatal(err)
			}
			secondBytes, err := os.ReadFile(second.ParquetPath)
			if err != nil {
				t.Fatal(err)
			}
			if string(firstBytes) != string(secondBytes) {
				t.Fatal("physical parquet bytes differ")
			}
		})
	}
}

func TestInstrumentEpochSourceOrdering(t *testing.T) {
	var zEpoch, aEpoch [16]byte
	copy(zEpoch[:], []byte("zzzz-early-epoch"))
	copy(aEpoch[:], []byte("aaaa-later-epoch"))
	const baseTime = int64(1_700_000_000_100_000_000)
	input := []normalize.Row{
		testTradeAt(t, "instrument-z", zEpoch, 4, 0, baseTime+40),
		testTradeAt(t, "instrument-a", aEpoch, 1, 0, baseTime+30),
		testTradeAt(t, "instrument-a", zEpoch, 10, 2, baseTime+20),
		testTradeAt(t, "instrument-a", zEpoch, 10, 1, baseTime+10),
	}
	rows, coordinates, err := collectNormalizedRows(t.Context(), &SliceNormalizedSource{Rows: input}, testWriterOptions())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := make([]string, len(rows))
	for index, row := range rows {
		meta := row.Common()
		got[index] = meta.InstrumentUID + "/" + string(meta.EpochID[:4]) + "/" +
			fmt.Sprint(meta.ArrivalOrdinal) + "/" + fmt.Sprint(meta.MessageOrdinal)
	}
	want := []string{
		"instrument-a/aaaa/1/0",
		"instrument-a/zzzz/10/1",
		"instrument-a/zzzz/10/2",
		"instrument-z/zzzz/4/0",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	if len(coordinates.epochs) != 2 || !strings.HasPrefix(coordinates.epochs[0].ID, "61616161") ||
		!strings.HasPrefix(coordinates.epochs[1].ID, "7a7a7a7a") {
		t.Fatalf("epoch order = %+v", coordinates.epochs)
	}
	lexicalOpposition := []normalize.Row{
		testTradeAt(t, "instrument-a", aEpoch, 2, 0, baseTime+60),
		testTradeAt(t, "instrument-a", zEpoch, 1, 0, baseTime+50),
	}
	lexicalRows, lexicalCoordinates, err := collectNormalizedRows(
		t.Context(), &SliceNormalizedSource{Rows: lexicalOpposition}, testWriterOptions())
	if err != nil {
		t.Fatalf("collect lexical opposition: %v", err)
	}
	if lexicalRows[0].Common().EpochID != zEpoch || lexicalRows[1].Common().EpochID != aEpoch ||
		len(lexicalCoordinates.epochs) != 2 ||
		!strings.HasPrefix(lexicalCoordinates.epochs[0].ID, "7a7a7a7a") ||
		!strings.HasPrefix(lexicalCoordinates.epochs[1].ID, "61616161") {
		t.Fatalf("epoch first-receive order did not override lexical ID order: rows=%x,%x epochs=%+v",
			lexicalRows[0].Common().EpochID, lexicalRows[1].Common().EpochID, lexicalCoordinates.epochs)
	}
	root := t.TempDir()
	result, err := BuildNormalizedPartition(t.Context(), root, &SliceNormalizedSource{Rows: input}, testWriterOptions())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if strings.Contains(result.Manifest.ParquetFile, "epoch=") || !strings.Contains(filepath.Base(result.ParquetPath), "part-epochs-") {
		t.Fatalf("epoch leaked into directory layout: %s", result.Manifest.ParquetFile)
	}
}

func TestAuxiliarySameArrivalOrdering(t *testing.T) {
	var epoch [16]byte
	copy(epoch[:], []byte("aux-order-epoch!"))
	const received = int64(1_700_000_000_100_000_000)
	second := testQualityAt(t, "instrument-a", epoch, 7, 2, received+2)
	first := testQualityAt(t, "instrument-a", epoch, 7, 1, received+1)
	root := t.TempDir()
	result, err := BuildQualityPartition(t.Context(), root, &SliceQualitySource{Rows: []QualityRowV1{second, first}}, testWriterOptions())
	if err != nil {
		t.Fatalf("build reordered quality: %v", err)
	}
	if _, err := VerifyManifest(t.Context(), root, result.ManifestPath); err != nil {
		t.Fatalf("verify reordered quality: %v", err)
	}
	duplicate := testQualityAt(t, "instrument-a", epoch, 7, 1, received+3)
	_, err = BuildQualityPartition(t.Context(), t.TempDir(), &SliceQualitySource{Rows: []QualityRowV1{first, duplicate}}, testWriterOptions())
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate coordinate error = %v, want ErrInvalidInput", err)
	}
}

func TestPublicationDirectorySyncFailures(t *testing.T) {
	row := testNormalizedRows(t)[0]
	options := testWriterOptions()
	syncCalls := 0
	options.DirectorySync = func(string) error {
		syncCalls++
		return nil
	}
	if _, err := BuildNormalizedPartition(t.Context(), t.TempDir(), &SliceNormalizedSource{Rows: []normalize.Row{row}}, options); err != nil {
		t.Fatalf("measure sync calls: %v", err)
	}
	if syncCalls < 2 || syncCalls%2 != 0 {
		t.Fatalf("directory sync calls = %d, want two equal publication rounds", syncCalls)
	}
	callsPerRound := syncCalls / 2
	for _, failAt := range []int{1, callsPerRound + 1} {
		failAt := failAt
		t.Run(fmt.Sprintf("call_%d", failAt), func(t *testing.T) {
			injected := errors.New("injected directory sync failure")
			options := testWriterOptions()
			calls := 0
			options.DirectorySync = func(string) error {
				calls++
				if calls == failAt {
					return injected
				}
				return nil
			}
			root := t.TempDir()
			_, err := BuildNormalizedPartition(t.Context(), root, &SliceNormalizedSource{Rows: []normalize.Row{row}}, options)
			if !errors.Is(err, injected) {
				t.Fatalf("build error = %v, want injected failure", err)
			}
			manifestCount := 0
			if walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if !entry.IsDir() && strings.HasPrefix(entry.Name(), "manifest-") && strings.HasSuffix(entry.Name(), ".json") {
					manifestCount++
				}
				return nil
			}); walkErr != nil {
				t.Fatal(walkErr)
			}
			wantManifests := 0
			if failAt > callsPerRound {
				wantManifests = 1
			}
			if manifestCount != wantManifests {
				t.Fatalf("published manifests = %d, want %d", manifestCount, wantManifests)
			}
		})
	}
}

func TestManifestRejectsTrailingJSON(t *testing.T) {
	root := t.TempDir()
	result, err := BuildNormalizedPartition(t.Context(), root, &SliceNormalizedSource{Rows: testNormalizedRows(t)[:1]}, testWriterOptions())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	file, err := os.OpenFile(result.ManifestPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("{}\n"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyManifest(t.Context(), root, result.ManifestPath); !errors.Is(err, ErrManifestMismatch) {
		t.Fatalf("verify error = %v, want ErrManifestMismatch", err)
	}
}

func TestOptionalParquetStatePairs(t *testing.T) {
	value := int64(7)
	for _, test := range []struct {
		name  string
		value *int64
		state string
		valid bool
	}{
		{"missing", nil, string(normalize.SourceMissing), true},
		{"value", &value, string(normalize.SourceValue), true},
		{"null_state", nil, "", false},
		{"nil_value_state", nil, string(normalize.SourceValue), false},
		{"present_missing_state", &value, string(normalize.SourceMissing), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parquetOptionalInt64(test.value, test.state)
			if (err == nil) != test.valid {
				t.Fatalf("error = %v, valid = %v", err, test.valid)
			}
		})
	}
	unsigned := uint64(7)
	if _, err := parquetOptionalUint64(&unsigned, string(normalize.SourceMissing)); !errors.Is(err, ErrCorruptDataset) {
		t.Fatalf("uint state mismatch error = %v", err)
	}
}

func TestAuxiliaryEnumDomains(t *testing.T) {
	var epoch [16]byte
	copy(epoch[:], []byte("enum-test-epoch!"))
	quality := testQualityAt(t, "instrument-a", epoch, 1, 0, 1_700_000_000_100_000_000)
	quality.SourceState = normalize.SourceState("invalid")
	if _, err := BuildQualityPartition(t.Context(), t.TempDir(), &SliceQualitySource{Rows: []QualityRowV1{quality}}, testWriterOptions()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("quality source state error = %v, want ErrInvalidInput", err)
	}

	base := testQuarantineAt(t, epoch)
	tests := []struct {
		name   string
		mutate func(*normalize.SchemaQuarantineV1)
	}{
		{"source_state", func(row *normalize.SchemaQuarantineV1) { row.SourceState = normalize.SourceState("invalid") }},
		{"code", func(row *normalize.SchemaQuarantineV1) { row.Code = normalize.QuarantineCode("invalid") }},
		{"fingerprint_class", func(row *normalize.SchemaQuarantineV1) { row.FingerprintClass = normalize.FingerprintClass("invalid") }},
		{"source_time_resolution", func(row *normalize.SchemaQuarantineV1) {
			row.SourceTimeResolution = normalize.TimeResolution("invalid")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := base
			test.mutate(&row)
			_, err := BuildQuarantinePartition(t.Context(), t.TempDir(), &SliceQuarantineSource{Rows: []normalize.SchemaQuarantineV1{row}}, testWriterOptions())
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("build error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestX4InteropEngineIdentities(t *testing.T) {
	valid := X4InteropObservation{
		DuckDBPassed: true, PolarsPassed: true, ClickHousePassed: true, MeasurementsObserved: true,
		DuckDBIdentity: "duckdb/v1", PolarsIdentity: "polars/v1", ClickHouseIdentity: "clickhouse/v1",
		RangeReadBytes: 1, QueryRowsRead: 1,
	}
	if err := validateX4InteropObservation(valid); err != nil {
		t.Fatalf("valid observation: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*X4InteropObservation)
	}{
		{"empty_duckdb", func(value *X4InteropObservation) { value.DuckDBIdentity = "" }},
		{"empty_polars", func(value *X4InteropObservation) { value.PolarsIdentity = "" }},
		{"empty_clickhouse", func(value *X4InteropObservation) { value.ClickHouseIdentity = "" }},
		{"oversized", func(value *X4InteropObservation) {
			value.ClickHouseIdentity = strings.Repeat("x", MaxX4EngineIdentityBytes+1)
		}},
		{"invalid_utf8", func(value *X4InteropObservation) { value.PolarsIdentity = string([]byte{0xff}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := valid
			test.mutate(&observation)
			if err := validateX4InteropObservation(observation); err == nil {
				t.Fatal("invalid engine identity accepted")
			}
		})
	}
}

func TestBuildRequiresExistingOutputRoot(t *testing.T) {
	row := testNormalizedRows(t)[0]
	missing := filepath.Join(t.TempDir(), "caller-must-create")
	_, err := BuildNormalizedPartition(t.Context(), missing, &SliceNormalizedSource{Rows: []normalize.Row{row}}, testWriterOptions())
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing root error = %v, want ErrInvalidInput", err)
	}
	if _, statErr := os.Stat(missing); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("builder created missing output root: %v", statErr)
	}
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = BuildNormalizedPartition(t.Context(), file, &SliceNormalizedSource{Rows: []normalize.Row{row}}, testWriterOptions())
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("file root error = %v, want ErrInvalidInput", err)
	}
}

func TestDecideX4UsesMeasuredBudgets(t *testing.T) {
	criteria := X4DecisionCriteria{MaxPeakRSSBytes: 100, MaxRangeReadBytes: 100, MaxQueryRowsRead: 100, MinimumBloomPrunedRows: 10}
	vectors := []X4MeasuredDecisionVector{
		{Configuration: X4Configuration{RowGroupTargetBytes: 64 << 20, BloomFilter: true}, PeakRSSBytes: 80,
			RangeReadBytes: 90, QueryRowsRead: 90, BloomPrunedRows: 9, DuckDBPassed: true, PolarsPassed: true, ClickHousePassed: true},
		{Configuration: X4Configuration{RowGroupTargetBytes: 256 << 20}, PeakRSSBytes: 80,
			RangeReadBytes: 90, QueryRowsRead: 90, DuckDBPassed: true, PolarsPassed: true, ClickHousePassed: true},
	}
	decision, err := DecideX4(criteria, vectors)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Status != "measured_vector_selected" || decision.Configuration.RowGroupTargetBytes != 256<<20 {
		t.Fatalf("decision = %+v", decision)
	}
	vectors[1].QueryRowsRead = 101
	decision, err = DecideX4(criteria, vectors)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Status != FrozenX4Decision().Status {
		t.Fatalf("decision = %+v, want frozen default", decision)
	}
}

func TestCorruption(t *testing.T) {
	for _, test := range []struct {
		name     string
		truncate bool
	}{{"page_bit_flip", false}, {"truncated_footer", true}} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			row := testNormalizedRows(t)[0]
			result, err := BuildNormalizedPartition(t.Context(), root, &SliceNormalizedSource{Rows: []normalize.Row{row}}, testWriterOptions())
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			info, err := os.Stat(result.ParquetPath)
			if err != nil {
				t.Fatal(err)
			}
			if test.truncate {
				if err := os.Truncate(result.ParquetPath, info.Size()-8); err != nil {
					t.Fatal(err)
				}
			} else {
				file, err := os.OpenFile(result.ParquetPath, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				offset := info.Size() / 2
				var value [1]byte
				if _, err := file.ReadAt(value[:], offset); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				value[0] ^= 0x80
				if _, err := file.WriteAt(value[:], offset); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			_, err = VerifyManifest(t.Context(), root, result.ManifestPath)
			if err == nil || (!errors.Is(err, ErrCorruptDataset) && !errors.Is(err, ErrManifestMismatch)) {
				t.Fatalf("got %v, want corruption rejection", err)
			}
		})
	}
}

func TestDuckDBInteropVector(t *testing.T) {
	engine := os.Getenv("DUCKDB_BIN")
	if engine == "" {
		t.Skip("set DUCKDB_BIN to execute the explicit DuckDB interoperability vector")
	}
	if err := RunDuckDBVector(t.Context(), engine, filepath.Join("..", "testdata", "dataset", "interop-v1.parquet")); err != nil {
		t.Fatal(err)
	}
}

func TestPolarsInteropVector(t *testing.T) {
	engine := os.Getenv("POLARS_PYTHON")
	if engine == "" {
		t.Skip("set POLARS_PYTHON to execute the explicit Polars interoperability vector")
	}
	if err := RunPolarsVector(t.Context(), engine, filepath.Join("..", "testdata", "dataset", "interop-v1.parquet")); err != nil {
		t.Fatal(err)
	}
}

func testWriterOptions() WriterOptions {
	options := DefaultWriterOptions(testHash("dataset-policy"), testHash("replay-config"), testHash("input-manifest-set"))
	options.RowGroupTargetBytes = 64 << 20
	return options
}

func testHash(value string) normalize.Hash { return normalize.Hash(sha256.Sum256([]byte(value))) }

func testCoordinate(metadata normalize.Metadata) normalize.RawCoordinate {
	return normalize.RawCoordinate{SourceID: metadata.SourceID, ChannelID: metadata.ChannelID, EpochKind: metadata.EpochKind,
		EpochID: metadata.EpochID, ArrivalOrdinal: metadata.ArrivalOrdinal, MessageOrdinal: metadata.MessageOrdinal,
		RawSegmentSHA256: metadata.RawSegmentSHA256, RawRecordOrdinal: metadata.RawRecordOrdinal, RawPayloadSHA256: metadata.RawPayloadSHA256}
}

func testNormalizedRows(t testing.TB) []normalize.Row {
	t.Helper()
	price := testNumeric(t, "123.45", normalize.CanonicalPriceScale, normalize.SpotPriceUnit("asset-btc", "asset-usdt"))
	amount := testNumeric(t, "2.5", normalize.CanonicalAmountScale, normalize.BaseAssetUnit("asset-btc"))
	zero := testNumeric(t, "0", normalize.CanonicalAmountScale, normalize.BaseAssetUnit("asset-btc"))
	quoteAmount := testNumeric(t, "308.625", normalize.CanonicalAmountScale, normalize.QuoteAssetUnit("asset-usdt"))
	percent := testNumeric(t, "1.25", normalize.CanonicalPercentScale, normalize.PercentUnit())
	trade, err := normalize.NewTradeRow(normalize.TradeV1{Metadata: testMetadata(t, normalize.TradeSchemaName, normalize.TradeSchemaVersion, 1),
		NativeTradeID: 101, AggressorSide: normalize.SideBuy, Price: price, Amount: amount,
		AggregationKind: normalize.AggregationSingleMatch, NativeDuplicateStatus: normalize.DuplicateUnassessed})
	if err != nil {
		t.Fatal(err)
	}
	book, err := normalize.NewBookUpdateRow(normalize.BookUpdateV1{Metadata: testMetadata(t, normalize.BookUpdateSchemaName, normalize.BookUpdateSchemaVersion, 2),
		UpdateKind: normalize.UpdateDelta, DepthContract: "diff_depth", AggregationContract: "100ms", FirstSequence: 10, LastSequence: 11,
		Checksum: normalize.SourceMissing, Bids: []normalize.BookLevel{{Side: normalize.SideBuy, LevelOrdinal: 0, Action: normalize.LevelUpsert, Price: price, Amount: amount}},
		Asks:            []normalize.BookLevel{{Side: normalize.SideSell, LevelOrdinal: 0, Action: normalize.LevelDelete, Price: price, Amount: zero}},
		AmountSemantics: "absolute_base_asset_quantity", ReconstructionEligibility: "eligible_with_rest_snapshot_bridge"})
	if err != nil {
		t.Fatal(err)
	}
	quote, err := normalize.NewQuoteRow(normalize.QuoteV1{Metadata: testMetadata(t, normalize.QuoteSchemaName, normalize.QuoteSchemaVersion, 3),
		NativeSourceRole: "bookTicker_native_bbo", UpdateID: 17, BidPrice: price, BidAmount: amount, AskPrice: price, AskAmount: amount,
		RPIInclusionState: normalize.RPINotApplicable})
	if err != nil {
		t.Fatal(err)
	}
	tickerEvent := normalize.TickerV1{Metadata: testMetadata(t, normalize.TickerSchemaName, normalize.TickerSchemaVersion, 4),
		NativeSourceRole: "24hrTicker_statistics_not_bbo", WindowKind: normalize.WindowRolling24Hours,
		WindowOpenSemantics: "native_statistics_open_time", WindowCloseSemantics: "native_statistics_close_time",
		WindowOpenTimeNS: 1_700_000_000_000_000_000, WindowCloseTimeNS: 1_700_086_400_000_000_000,
		WindowTimeResolution: normalize.ResolutionMillisecond, NominalWindowDurationNS: 86_400_000_000_000,
		PriceChange: price, PriceChangePercent: percent, WeightedAveragePrice: price, FirstTradeBeforeWindowPrice: price,
		LastPrice: price, LastAmount: amount, NativeBestBidPrice: price, NativeBestBidAmount: amount,
		NativeBestAskPrice: price, NativeBestAskAmount: amount, OpenPrice: price, HighPrice: price, LowPrice: price,
		BaseVolume: amount, QuoteVolume: quoteAmount, FirstTradeID: 1, LastTradeID: 2, TradeCount: 2}
	ticker, err := normalize.NewTickerRow(tickerEvent)
	if err != nil {
		t.Fatal(err)
	}
	return []normalize.Row{trade, book, quote, ticker}
}

func testNumeric(t testing.TB, text string, scale uint8, unit normalize.Unit) normalize.Numeric {
	t.Helper()
	decimal, err := normalize.ParseDecimal(text, scale, normalize.DefaultDecimalBounds())
	if err != nil {
		t.Fatal(err)
	}
	return normalize.Numeric{Decimal: decimal, Unit: unit}
}

func testMetadata(t testing.TB, schema string, version uint16, arrival uint64) normalize.Metadata {
	t.Helper()
	var epoch [16]byte
	copy(epoch[:], []byte("dataset-test-v1!"))
	envelope := capture.EnvelopeV1{EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: "binance-spot-api-v3", ChannelOrEndpoint: "spot-test-v1", ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true},
		ArrivalOrdinal: arrival, ExchangeTimeNS: capture.OptionalInt64{Value: 1_700_000_000_000_000_000 + int64(arrival), Valid: true},
		ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: 1_700_000_000_100_000_000 + int64(arrival),
		ClockEpochID: "dataset-test-clock", MonotonicNSSinceClockEpoch: arrival, PayloadEncoding: capture.PayloadEncodingJSON,
		TerminalOutcome: capture.TerminalObserved, RecorderVersion: "dataset-test-recorder"}
	envelope.SetRawPayload([]byte(`{"synthetic":true}`))
	record, err := normalize.BindRawRecord(envelope, testHash("segment"), arrival, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{Record: record, SchemaName: schema, SchemaVersion: version,
		InstrumentUID: "instrument-btcusdt-v1", ExchangeTimeNS: normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true},
		ExchangeTimeResolution: normalize.ResolutionMillisecond, SourceEventTimeNS: normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true},
		SourceTimeResolution: normalize.ResolutionMillisecond, SourceSchemaFingerprint: testHash("schema"), MapperVersion: "mapper-v1",
		MapperBindingID: testHash("binding"), CatalogSnapshotID: testHash("catalog")})
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

func testTradeAt(t testing.TB, instrument string, epoch [16]byte, arrival uint64, message uint32, received int64) normalize.Row {
	t.Helper()
	metadata := testMetadataAt(t, normalize.TradeSchemaName, normalize.TradeSchemaVersion, instrument, epoch, arrival, message, received)
	price := testNumeric(t, "123.45", normalize.CanonicalPriceScale, normalize.SpotPriceUnit("asset-btc", "asset-usdt"))
	amount := testNumeric(t, "2.5", normalize.CanonicalAmountScale, normalize.BaseAssetUnit("asset-btc"))
	row, err := normalize.NewTradeRow(normalize.TradeV1{
		Metadata: metadata, NativeTradeID: arrival*100 + uint64(message), AggressorSide: normalize.SideBuy,
		Price: price, Amount: amount, AggregationKind: normalize.AggregationSingleMatch,
		NativeDuplicateStatus: normalize.DuplicateUnassessed,
	})
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func testQualityAt(t testing.TB, instrument string, epoch [16]byte, arrival uint64, message uint32, received int64) QualityRowV1 {
	t.Helper()
	metadata := testMetadataAt(t, normalize.TradeSchemaName, normalize.TradeSchemaVersion, instrument, epoch, arrival, message, received)
	return QualityRowV1{
		Version: QualitySchemaVersion, QualityID: testHash(fmt.Sprintf("quality-%d-%d-%d", arrival, message, received)),
		Kind: "schema_observation", Code: "additive_field", SourceState: normalize.SourceValue,
		SourceSchemaFingerprint: metadata.SourceSchemaFingerprint, SchemaName: metadata.SchemaName, SchemaVersion: metadata.SchemaVersion,
		SourceID: metadata.SourceID, ChannelID: metadata.ChannelID, InstrumentUID: metadata.InstrumentUID,
		InstrumentUIDState: normalize.SourceValue, ReceivedTimeNS: metadata.ReceivedTimeNS, Coordinate: testCoordinate(metadata),
		MapperVersion: metadata.MapperVersion, MapperBindingID: metadata.MapperBindingID, CatalogSnapshotID: metadata.CatalogSnapshotID,
		PolicyID: testHash("quality-policy"), QualityFlags: []string{"schema_additive_field"},
	}
}

func testMetadataAt(t testing.TB, schema string, version uint16, instrument string, epoch [16]byte,
	arrival uint64, message uint32, received int64) normalize.Metadata {
	t.Helper()
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: "binance-spot-api-v3", ChannelOrEndpoint: "spot-test-v1",
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true}, ArrivalOrdinal: arrival, MessageOrdinal: message,
		ExchangeTimeNS:         capture.OptionalInt64{Value: received - 100_000_000, Valid: true},
		ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: received,
		ClockEpochID: "dataset-test-clock", MonotonicNSSinceClockEpoch: arrival*100 + uint64(message),
		PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved,
		RecorderVersion: "dataset-test-recorder",
	}
	envelope.SetRawPayload([]byte(fmt.Sprintf(`{"arrival":%d,"message":%d}`, arrival, message)))
	record, err := normalize.BindRawRecord(envelope, testHash("segment"), arrival*100+uint64(message), nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{
		Record: record, SchemaName: schema, SchemaVersion: version, InstrumentUID: instrument,
		ExchangeTimeNS:         normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true},
		ExchangeTimeResolution: normalize.ResolutionMillisecond,
		SourceEventTimeNS:      normalize.OptionalInt64{Value: envelope.ExchangeTimeNS.Value, Valid: true},
		SourceTimeResolution:   normalize.ResolutionMillisecond, SourceSchemaFingerprint: testHash("schema"),
		MapperVersion: "mapper-v1", MapperBindingID: testHash("binding"), CatalogSnapshotID: testHash("catalog"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

func testQuarantineAt(t testing.TB, epoch [16]byte) normalize.SchemaQuarantineV1 {
	t.Helper()
	metadata := testMetadataAt(t, normalize.TradeSchemaName, normalize.TradeSchemaVersion,
		"instrument-a", epoch, 1, 0, 1_700_000_000_100_000_000)
	return normalize.SchemaQuarantineV1{
		Version: normalize.SchemaQuarantineVersion, QuarantineID: testHash("enum-quarantine"),
		Code: normalize.QuarantineSchemaUnknown, Field: "unexpected", SourceState: normalize.SourceValue,
		FingerprintClass: normalize.FingerprintUnknown, SourceSchemaFingerprint: metadata.SourceSchemaFingerprint,
		SourceID: metadata.SourceID, ChannelID: metadata.ChannelID, ReceivedTimeNS: metadata.ReceivedTimeNS,
		Coordinate: testCoordinate(metadata), MapperVersion: metadata.MapperVersion, MapperBindingID: metadata.MapperBindingID,
		SourceTimeResolution: metadata.SourceTimeResolution, CatalogSnapshotID: metadata.CatalogSnapshotID,
	}
}
