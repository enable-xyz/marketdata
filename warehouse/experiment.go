package warehouse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"
)

const (
	X5FixtureVersion  uint16 = 1
	MaxX5FixtureBytes        = 1 << 20
)

type X5Variant struct {
	BatchRows   int             `json:"batch_rows"`
	Compression Compression     `json:"compression"`
	Layout      PartitionLayout `json:"partition_layout"`
}

func X5Variants() []X5Variant {
	variants := make([]X5Variant, 0, 12)
	for _, batchRows := range []int{1_000, 10_000, 100_000} {
		for _, compression := range []Compression{CompressionLZ4, CompressionZstd} {
			for _, layout := range []PartitionLayout{PartitionMonth, PartitionDate} {
				variants = append(variants, X5Variant{BatchRows: batchRows, Compression: compression, Layout: layout})
			}
		}
	}
	return variants
}

type QueryName string

const (
	QueryCoverage QueryName = "coverage"
	QueryReplay   QueryName = "replay"
	QueryResearch QueryName = "research"
)

type QueryObservation struct {
	Name          QueryName `json:"name"`
	DurationNS    int64     `json:"duration_ns"`
	ResponseRows  uint64    `json:"response_rows"`
	ResponseBytes uint64    `json:"response_bytes"`
	ResultSHA256  string    `json:"result_sha256"`
}

type X5CaseObservation struct {
	Variant          X5Variant          `json:"variant"`
	IngestDurationNS int64              `json:"ingest_duration_ns"`
	EventCount       uint64             `json:"event_count"`
	RowCount         uint64             `json:"row_count"`
	EventSetSHA256   string             `json:"event_set_sha256"`
	Queries          []QueryObservation `json:"queries"`
}

type DisconnectObservation struct {
	Point             int       `json:"point"`
	ManifestOrdinal   int       `json:"manifest_ordinal,omitempty"`
	BatchKind         BatchKind `json:"batch_kind"`
	BatchOrdinal      uint64    `json:"batch_ordinal"`
	EventCount        uint64    `json:"event_count"`
	RowCount          uint64    `json:"row_count"`
	EventSetSHA256    string    `json:"event_set_sha256"`
	Rebuilt           bool      `json:"rebuilt"`
	ReconciledUnknown bool      `json:"reconciled_unknown"`
	Reconnected       bool      `json:"reconnected"`
}

type X5MeasuredResult struct {
	FixtureVersion    uint16                  `json:"fixture_version"`
	Measured          bool                    `json:"measured"`
	ServerDigest      string                  `json:"server_digest"`
	SyntheticRows     uint64                  `json:"synthetic_rows"`
	ManifestCount     uint64                  `json:"manifest_count"`
	DisconnectVariant X5Variant               `json:"disconnect_variant,omitempty"`
	Cases             []X5CaseObservation     `json:"cases"`
	DisconnectMatrix  []DisconnectObservation `json:"disconnect_matrix"`
}

type QueryBudget struct {
	Name             QueryName `json:"name"`
	MaxDurationNS    int64     `json:"max_duration_ns"`
	MaxResponseRows  uint64    `json:"max_response_rows"`
	MaxResponseBytes uint64    `json:"max_response_bytes"`
	ExpectedSHA256   string    `json:"expected_sha256"`
}

type X5CaseBudget struct {
	Variant             X5Variant     `json:"variant"`
	MaxIngestDurationNS int64         `json:"max_ingest_duration_ns"`
	ExpectedEventCount  uint64        `json:"expected_event_count"`
	ExpectedRowCount    uint64        `json:"expected_row_count"`
	ExpectedEventSet    string        `json:"expected_event_set_sha256"`
	Queries             []QueryBudget `json:"queries"`
}

type X5Fixture struct {
	FixtureVersion    uint16                  `json:"fixture_version"`
	Measured          bool                    `json:"measured"`
	ServerDigest      string                  `json:"server_digest"`
	SyntheticRows     uint64                  `json:"synthetic_rows"`
	ManifestCount     uint64                  `json:"manifest_count"`
	DisconnectVariant X5Variant               `json:"disconnect_variant,omitempty"`
	Cases             []X5CaseBudget          `json:"cases"`
	DisconnectMatrix  []DisconnectObservation `json:"disconnect_matrix"`
}

type X5RunConfig struct {
	Native            NativeConfig
	Manifests         []CommittedManifest
	DisconnectVariant X5Variant
}

func RunX5(ctx context.Context, config X5RunConfig) (X5MeasuredResult, error) {
	if err := validateX5Corpus(ctx, config.Native.ServerDigest, config.Manifests); err != nil {
		return X5MeasuredResult{}, err
	}
	if !slices.Contains(X5Variants(), config.DisconnectVariant) {
		return X5MeasuredResult{}, fmt.Errorf("%w: explicit X5 disconnect variant", ErrInvalidWarehouseInput)
	}
	basePrefix := config.Native.TablePrefix
	if basePrefix == "" {
		basePrefix = "x5"
	}
	result := X5MeasuredResult{FixtureVersion: X5FixtureVersion, Measured: true,
		ServerDigest: config.Native.ServerDigest, SyntheticRows: X5Rows, ManifestCount: uint64(len(config.Manifests)),
		DisconnectVariant: config.DisconnectVariant}
	for _, variant := range X5Variants() {
		nativeConfig := config.Native
		nativeConfig.BatchRows = variant.BatchRows
		nativeConfig.Compression = variant.Compression
		nativeConfig.Layout = variant.Layout
		nativeConfig.TablePrefix = variantPrefix(basePrefix, variant)
		nativeConfig.AcknowledgementFault = nil
		store, err := OpenNative(ctx, nativeConfig)
		if err != nil {
			return X5MeasuredResult{}, err
		}
		loader, err := NewLoader(store, ParquetManifestReader{}, nativeConfig.ServerDigest,
			Config{BatchRows: variant.BatchRows, Compression: variant.Compression, Layout: variant.Layout})
		if err != nil {
			_ = store.Close()
			return X5MeasuredResult{}, err
		}
		started := time.Now()
		_, runErr := loader.RebuildAll(ctx, config.Manifests)
		ingestDuration := time.Since(started)
		if runErr != nil {
			_ = store.Close()
			return X5MeasuredResult{}, runErr
		}
		caseObservation, runErr := observeX5Case(ctx, store, variant, ingestDuration)
		if runErr == nil {
			runErr = store.Truncate(ctx)
		}
		closeErr := store.Close()
		if runErr != nil || closeErr != nil {
			return X5MeasuredResult{}, errors.Join(runErr, closeErr)
		}
		result.Cases = append(result.Cases, caseObservation)
	}
	disconnects, err := runDisconnectMatrix(ctx, config.Native, basePrefix, config.Manifests, config.DisconnectVariant)
	if err != nil {
		return X5MeasuredResult{}, err
	}
	result.DisconnectMatrix = disconnects
	if err := validateMeasuredResult(result); err != nil {
		return X5MeasuredResult{}, err
	}
	return result, nil
}

func FreezeX5(result X5MeasuredResult) (X5Fixture, error) {
	if err := validateMeasuredResult(result); err != nil {
		return X5Fixture{}, errors.Join(ErrMeasuredResultRequired, err)
	}
	fixture := X5Fixture{FixtureVersion: result.FixtureVersion, Measured: true, ServerDigest: result.ServerDigest,
		SyntheticRows: result.SyntheticRows, ManifestCount: result.ManifestCount, DisconnectVariant: result.DisconnectVariant,
		DisconnectMatrix: slices.Clone(result.DisconnectMatrix)}
	for _, observation := range result.Cases {
		budget := X5CaseBudget{Variant: observation.Variant,
			MaxIngestDurationNS: marginInt64(observation.IngestDurationNS), ExpectedEventCount: observation.EventCount,
			ExpectedRowCount: observation.RowCount, ExpectedEventSet: observation.EventSetSHA256}
		for _, query := range observation.Queries {
			budget.Queries = append(budget.Queries, QueryBudget{Name: query.Name, MaxDurationNS: marginInt64(query.DurationNS),
				MaxResponseRows: marginUint64(query.ResponseRows), MaxResponseBytes: marginUint64(query.ResponseBytes),
				ExpectedSHA256: query.ResultSHA256})
		}
		fixture.Cases = append(fixture.Cases, budget)
	}
	if err := validateFixture(fixture); err != nil {
		return X5Fixture{}, err
	}
	return fixture, nil
}

func WriteX5Fixture(writer io.Writer, result X5MeasuredResult) error {
	if writer == nil {
		return fmt.Errorf("%w: fixture writer", ErrInvalidWarehouseInput)
	}
	fixture, err := FreezeX5(result)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(fixture)
}

func ReadX5Fixture(reader io.Reader) (X5Fixture, error) {
	if reader == nil {
		return X5Fixture{}, fmt.Errorf("%w: fixture reader", ErrInvalidWarehouseInput)
	}
	decoder := json.NewDecoder(io.LimitReader(reader, MaxX5FixtureBytes+1))
	decoder.DisallowUnknownFields()
	var fixture X5Fixture
	if err := decoder.Decode(&fixture); err != nil {
		return X5Fixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return X5Fixture{}, fmt.Errorf("%w: trailing X5 fixture content", ErrInvalidWarehouseInput)
	}
	if err := validateFixture(fixture); err != nil {
		return X5Fixture{}, err
	}
	return fixture, nil
}

// VerifyX5Budgets always applies every frozen numeric case/query threshold and
// exact case/query hash. A legacy fixture has no declared fault variant, so its
// old fault observations are retained as provenance but not relabeled or
// compared to the fresh explicitly selected matrix validated by RunX5.
func VerifyX5Budgets(ctx context.Context, config X5RunConfig, fixture X5Fixture) error {
	if err := validateFixture(fixture); err != nil {
		return err
	}
	legacyFaultPlan := fixture.DisconnectVariant == (X5Variant{})
	if fixture.ServerDigest != config.Native.ServerDigest ||
		(!legacyFaultPlan && fixture.DisconnectVariant != config.DisconnectVariant) {
		return ErrGenerationConflict
	}
	measured, err := RunX5(ctx, config)
	if err != nil {
		return err
	}
	if len(measured.Cases) != len(fixture.Cases) {
		return ErrReconciliationFailed
	}
	for i, observation := range measured.Cases {
		budget := fixture.Cases[i]
		if observation.Variant != budget.Variant || observation.IngestDurationNS > budget.MaxIngestDurationNS ||
			observation.EventCount != budget.ExpectedEventCount || observation.RowCount != budget.ExpectedRowCount ||
			observation.EventSetSHA256 != budget.ExpectedEventSet || len(observation.Queries) != len(budget.Queries) {
			return fmt.Errorf("%w: X5 case %d budget", ErrReconciliationFailed, i)
		}
		for queryIndex, query := range observation.Queries {
			limit := budget.Queries[queryIndex]
			if query.Name != limit.Name || query.DurationNS > limit.MaxDurationNS || query.ResponseRows > limit.MaxResponseRows ||
				query.ResponseBytes > limit.MaxResponseBytes || query.ResultSHA256 != limit.ExpectedSHA256 {
				return fmt.Errorf("%w: X5 case %d query %q budget", ErrReconciliationFailed, i, query.Name)
			}
		}
	}
	if legacyFaultPlan {
		return nil
	}
	for i, observation := range measured.DisconnectMatrix {
		expected := fixture.DisconnectMatrix[i]
		if observation.Point != expected.Point || observation.ManifestOrdinal != expected.ManifestOrdinal ||
			observation.BatchKind != expected.BatchKind || observation.BatchOrdinal != expected.BatchOrdinal ||
			observation.EventCount != expected.EventCount || observation.RowCount != expected.RowCount ||
			observation.Rebuilt != expected.Rebuilt || observation.ReconciledUnknown != expected.ReconciledUnknown ||
			!observation.Reconnected {
			return fmt.Errorf("%w: X5 disconnect point %d", ErrReconciliationFailed, i)
		}
	}
	return nil
}

func observeX5Case(ctx context.Context, store *NativeStore, variant X5Variant, ingestDuration time.Duration) (X5CaseObservation, error) {
	eventIDs, rowCount, err := store.allEventIDsAndRows(ctx)
	if err != nil {
		return X5CaseObservation{}, err
	}
	queries, err := store.representativeQueries(ctx)
	if err != nil {
		return X5CaseObservation{}, err
	}
	return X5CaseObservation{Variant: variant, IngestDurationNS: ingestDuration.Nanoseconds(), EventCount: uint64(len(eventIDs)),
		RowCount: rowCount, EventSetSHA256: eventSetHash(eventIDs).String(), Queries: queries}, nil
}

func (s *NativeStore) allEventIDsAndRows(ctx context.Context) ([]EventID, uint64, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	rows, err := s.conn.Query(ctx, "SELECT DISTINCT event_id FROM "+s.schema.EventsTable()+" ORDER BY event_id")
	if err != nil {
		return nil, 0, err
	}
	var eventIDs []EventID
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			_ = rows.Close()
			return nil, 0, err
		}
		var id EventID
		if err := assignFixedHash(&id, encoded); err != nil {
			_ = rows.Close()
			return nil, 0, err
		}
		eventIDs = append(eventIDs, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, 0, err
	}
	var rowCount uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM "+s.schema.EventsTable()).Scan(&rowCount); err != nil {
		return nil, 0, err
	}
	return eventIDs, rowCount, nil
}

func (s *NativeStore) representativeQueries(ctx context.Context) ([]QueryObservation, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	queries := make([]QueryObservation, 0, 3)
	coverage, err := s.observeCoverage(ctx)
	if err != nil {
		return nil, err
	}
	queries = append(queries, coverage)
	replay, err := s.observeReplay(ctx)
	if err != nil {
		return nil, err
	}
	queries = append(queries, replay)
	research, err := s.observeResearch(ctx)
	if err != nil {
		return nil, err
	}
	queries = append(queries, research)
	return queries, nil
}

func (s *NativeStore) observeCoverage(ctx context.Context) (QueryObservation, error) {
	started := time.Now()
	var count uint64
	var minimum, maximum int64
	query := "SELECT count(), min(received_time_ns), max(received_time_ns) FROM " + s.schema.EventsTable()
	if err := s.conn.QueryRow(ctx, query).Scan(&count, &minimum, &maximum); err != nil {
		return QueryObservation{}, err
	}
	hasher := sha256.New()
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], count)
	_, _ = hasher.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], uint64(minimum))
	_, _ = hasher.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], uint64(maximum))
	_, _ = hasher.Write(encoded[:])
	return queryObservation(QueryCoverage, started, 1, 24, hasher.Sum(nil)), nil
}

func (s *NativeStore) observeReplay(ctx context.Context) (QueryObservation, error) {
	started := time.Now()
	query := "SELECT event_id, source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal FROM " +
		s.schema.EventsTable() + " ORDER BY (source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal) LIMIT 10000"
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return QueryObservation{}, err
	}
	defer rows.Close()
	hasher := sha256.New()
	var responseRows, responseBytes uint64
	for rows.Next() {
		var eventID, epoch []byte
		var sourceID, instrumentUID string
		var received int64
		var arrival uint64
		var message uint32
		if err := rows.Scan(&eventID, &sourceID, &instrumentUID, &received, &epoch, &arrival, &message); err != nil {
			return QueryObservation{}, err
		}
		responseRows++
		responseBytes += uint64(len(eventID) + len(sourceID) + len(instrumentUID) + len(epoch) + 20)
		writeLengthBytes(hasher, eventID)
		writeLengthBytes(hasher, []byte(sourceID))
		writeLengthBytes(hasher, []byte(instrumentUID))
		writeLengthBytes(hasher, epoch)
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(received))
		_, _ = hasher.Write(encoded[:])
		binary.BigEndian.PutUint64(encoded[:], arrival)
		_, _ = hasher.Write(encoded[:])
		binary.BigEndian.PutUint32(encoded[:4], message)
		_, _ = hasher.Write(encoded[:4])
	}
	if err := rows.Err(); err != nil {
		return QueryObservation{}, err
	}
	return queryObservation(QueryReplay, started, responseRows, responseBytes, hasher.Sum(nil)), nil
}

func (s *NativeStore) observeResearch(ctx context.Context) (QueryObservation, error) {
	started := time.Now()
	query := "SELECT instrument_uid, count(), toString(sum(price)) FROM " + s.schema.EventsTable() +
		" WHERE price IS NOT NULL GROUP BY instrument_uid ORDER BY instrument_uid LIMIT 1000"
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return QueryObservation{}, err
	}
	defer rows.Close()
	hasher := sha256.New()
	var responseRows, responseBytes uint64
	for rows.Next() {
		var instrumentUID, sum string
		var count uint64
		if err := rows.Scan(&instrumentUID, &count, &sum); err != nil {
			return QueryObservation{}, err
		}
		responseRows++
		responseBytes += uint64(len(instrumentUID) + len(sum) + 8)
		writeLengthBytes(hasher, []byte(instrumentUID))
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], count)
		_, _ = hasher.Write(encoded[:])
		writeLengthBytes(hasher, []byte(sum))
	}
	if err := rows.Err(); err != nil {
		return QueryObservation{}, err
	}
	return queryObservation(QueryResearch, started, responseRows, responseBytes, hasher.Sum(nil)), nil
}

func queryObservation(name QueryName, started time.Time, rows, bytes uint64, digest []byte) QueryObservation {
	return QueryObservation{Name: name, DurationNS: time.Since(started).Nanoseconds(), ResponseRows: rows,
		ResponseBytes: bytes, ResultSHA256: hex.EncodeToString(digest)}
}

func writeLengthBytes(writer io.Writer, value []byte) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(value)))
	_, _ = writer.Write(encoded[:])
	_, _ = writer.Write(value)
}

type DisconnectSelection struct {
	Point           int
	ManifestOrdinal int
	BatchKind       BatchKind
	BatchOrdinal    uint64
}

// X5DisconnectSchedule binds every point to a distinct corpus manifest. At the
// selected 100,000-row batch size each 100,000-row manifest has exactly one
// expected-ID batch and one event batch, so every selected batch ordinal is 0.
func X5DisconnectSchedule(manifestCount uint64) []DisconnectSelection {
	if manifestCount < X5DisconnectPoints {
		return nil
	}
	schedule := make([]DisconnectSelection, X5DisconnectPoints)
	for point := range X5DisconnectPoints {
		kind := BatchEvents
		if point == 0 {
			kind = BatchGeneration
		} else if point%2 == 1 {
			kind = BatchExpectedIDs
		}
		schedule[point] = DisconnectSelection{
			Point: point, ManifestOrdinal: point, BatchKind: kind, BatchOrdinal: 0,
		}
	}
	return schedule
}

type disconnectFault struct {
	mu        sync.Mutex
	selection DisconnectSelection
	ordinals  map[BatchKind]uint64
	armed     bool
}

func (f *disconnectFault) arm(selection DisconnectSelection) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.selection = selection
	f.ordinals = make(map[BatchKind]uint64)
	f.armed = true
}

func (f *disconnectFault) AfterSynchronousBatch(_ context.Context, observation BatchObservation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ordinal := f.ordinals[observation.Kind]
	f.ordinals[observation.Kind] = ordinal + 1
	if f.armed && observation.Kind == f.selection.BatchKind && ordinal == f.selection.BatchOrdinal {
		f.armed = false
		return errors.New("deterministic X5 acknowledgement disconnect")
	}
	return nil
}

func disconnectNativeConfig(base NativeConfig, prefix string, variant X5Variant, fault AcknowledgementFault) NativeConfig {
	base.BatchRows = variant.BatchRows
	base.Compression = variant.Compression
	base.Layout = variant.Layout
	base.TablePrefix = variantPrefix(prefix, variant) + "_fault"
	base.AcknowledgementFault = fault
	return base
}

func runDisconnectMatrix(ctx context.Context, base NativeConfig, prefix string, manifests []CommittedManifest,
	variant X5Variant) ([]DisconnectObservation, error) {
	schedule := X5DisconnectSchedule(uint64(len(manifests)))
	if len(schedule) != X5DisconnectPoints {
		return nil, fmt.Errorf("%w: disconnect corpus must contain 100 distinct manifests", ErrInvalidWarehouseInput)
	}
	fault := &disconnectFault{}
	base = disconnectNativeConfig(base, prefix, variant, fault)
	store, err := OpenNative(ctx, base)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	loader, err := NewLoader(store, ParquetManifestReader{}, base.ServerDigest,
		Config{BatchRows: variant.BatchRows, Compression: variant.Compression, Layout: variant.Layout})
	if err != nil {
		return nil, err
	}
	observations := make([]DisconnectObservation, 0, X5DisconnectPoints)
	for _, selection := range schedule {
		manifest := manifests[selection.ManifestOrdinal]
		plan, err := (ParquetManifestReader{}).Plan(ctx, manifest, base.ServerDigest, variant.Layout)
		if err != nil {
			return nil, err
		}
		if err := store.Truncate(ctx); err != nil {
			return nil, err
		}
		beforeReconnect := store.ReconnectCount()
		fault.arm(selection)
		receipt, err := loader.Load(ctx, manifest)
		if err != nil {
			return nil, fmt.Errorf("warehouse: X5 disconnect point %d manifest %d (%s batch %d): %w",
				selection.Point, selection.ManifestOrdinal, selection.BatchKind, selection.BatchOrdinal, err)
		}
		reconnected := store.ReconnectCount() == beforeReconnect+1
		if !reconnected {
			return nil, fmt.Errorf("%w: disconnect point %d did not replace the native connection",
				ErrReconciliationFailed, selection.Point)
		}
		exact, err := loader.reconcileExact(ctx, plan)
		if err != nil || !exact {
			return nil, errors.Join(ErrReconciliationFailed, err)
		}
		ids, err := store.ActualEventIDs(ctx, plan.ID)
		if err != nil {
			return nil, err
		}
		rowCount, err := store.GenerationRowCount(ctx, plan.ID)
		if err != nil {
			return nil, err
		}
		observations = append(observations, DisconnectObservation{
			Point: selection.Point, ManifestOrdinal: selection.ManifestOrdinal,
			BatchKind: selection.BatchKind, BatchOrdinal: selection.BatchOrdinal, EventCount: uint64(len(ids)),
			RowCount: rowCount, EventSetSHA256: eventSetHash(ids).String(), Rebuilt: receipt.Rebuilt,
			ReconciledUnknown: receipt.ReconciledUnknown, Reconnected: reconnected,
		})
	}
	if err := store.Truncate(ctx); err != nil {
		return nil, err
	}
	return observations, nil
}

func validateX5Corpus(ctx context.Context, serverDigest string, manifests []CommittedManifest) error {
	if serverDigest == "" || len(manifests) == 0 {
		return fmt.Errorf("%w: X5 server digest or corpus", ErrInvalidWarehouseInput)
	}
	reader := ParquetManifestReader{}
	var rows uint64
	for _, manifest := range manifests {
		plan, err := reader.Plan(ctx, manifest, serverDigest, PartitionMonth)
		if err != nil {
			return err
		}
		rows += plan.ExpectedRowCount
	}
	if rows != X5Rows {
		return fmt.Errorf("%w: X5 requires exactly %d synthetic rows, got %d", ErrInvalidWarehouseInput, X5Rows, rows)
	}
	return nil
}

func validateMeasuredResult(result X5MeasuredResult) error {
	if !result.Measured || result.FixtureVersion != X5FixtureVersion || result.ServerDigest == "" ||
		result.SyntheticRows != X5Rows || result.ManifestCount < X5DisconnectPoints ||
		!slices.Contains(X5Variants(), result.DisconnectVariant) || len(result.Cases) != len(X5Variants()) ||
		len(result.DisconnectMatrix) != X5DisconnectPoints {
		return ErrMeasuredResultRequired
	}
	variants := X5Variants()
	for i, observation := range result.Cases {
		if observation.Variant != variants[i] || observation.IngestDurationNS <= 0 || observation.EventCount == 0 ||
			observation.RowCount != X5Rows || !validDigest(observation.EventSetSHA256) || len(observation.Queries) != 3 {
			return ErrMeasuredResultRequired
		}
		for queryIndex, query := range observation.Queries {
			if query.Name != []QueryName{QueryCoverage, QueryReplay, QueryResearch}[queryIndex] || query.DurationNS <= 0 ||
				query.ResponseRows == 0 || query.ResponseBytes == 0 || !validDigest(query.ResultSHA256) {
				return ErrMeasuredResultRequired
			}
		}
	}
	schedule := X5DisconnectSchedule(result.ManifestCount)
	for point, observation := range result.DisconnectMatrix {
		selection := schedule[point]
		if observation.Point != point || observation.ManifestOrdinal != selection.ManifestOrdinal ||
			observation.BatchKind != selection.BatchKind || observation.BatchOrdinal != selection.BatchOrdinal ||
			!observation.Reconnected || observation.EventCount == 0 || observation.RowCount == 0 ||
			!validDigest(observation.EventSetSHA256) || (!observation.Rebuilt && !observation.ReconciledUnknown) {
			return ErrMeasuredResultRequired
		}
	}
	return nil
}

func validateFixture(fixture X5Fixture) error {
	legacyFaultPlan := fixture.DisconnectVariant == (X5Variant{})
	if !fixture.Measured || fixture.FixtureVersion != X5FixtureVersion || fixture.ServerDigest == "" ||
		fixture.SyntheticRows != X5Rows || fixture.ManifestCount < X5DisconnectPoints ||
		(!legacyFaultPlan && !slices.Contains(X5Variants(), fixture.DisconnectVariant)) ||
		len(fixture.Cases) != len(X5Variants()) || len(fixture.DisconnectMatrix) != X5DisconnectPoints {
		return ErrMeasuredResultRequired
	}
	variants := X5Variants()
	for i, budget := range fixture.Cases {
		if budget.Variant != variants[i] || budget.MaxIngestDurationNS <= 0 || budget.ExpectedEventCount == 0 ||
			budget.ExpectedRowCount != X5Rows || !validDigest(budget.ExpectedEventSet) || len(budget.Queries) != 3 {
			return ErrMeasuredResultRequired
		}
		for queryIndex, query := range budget.Queries {
			if query.Name != []QueryName{QueryCoverage, QueryReplay, QueryResearch}[queryIndex] || query.MaxDurationNS <= 0 ||
				query.MaxResponseRows == 0 || query.MaxResponseBytes == 0 || !validDigest(query.ExpectedSHA256) {
				return ErrMeasuredResultRequired
			}
		}
	}
	schedule := X5DisconnectSchedule(fixture.ManifestCount)
	for point, observation := range fixture.DisconnectMatrix {
		selection := schedule[point]
		if observation.Point != point || !observation.Reconnected || observation.EventCount == 0 ||
			observation.RowCount == 0 || !validDigest(observation.EventSetSHA256) ||
			(!observation.Rebuilt && !observation.ReconciledUnknown) {
			return ErrMeasuredResultRequired
		}
		if !legacyFaultPlan && (observation.ManifestOrdinal != selection.ManifestOrdinal ||
			observation.BatchKind != selection.BatchKind || observation.BatchOrdinal != selection.BatchOrdinal) {
			return ErrMeasuredResultRequired
		}
	}
	return nil
}

func variantPrefix(base string, variant X5Variant) string {
	return fmt.Sprintf("%s_%s_%s_%d", base, variant.Layout, variant.Compression, variant.BatchRows)
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func marginInt64(value int64) int64 {
	if value > (1<<63-1)/5*4 {
		return 1<<63 - 1
	}
	return max(value+1, (value*5+3)/4)
}

func marginUint64(value uint64) uint64 {
	if value > ^uint64(0)/5*4 {
		return ^uint64(0)
	}
	return max(value+1, (value*5+3)/4)
}
