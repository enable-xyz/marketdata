package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	mapperEvidenceSourceID = "10000000-0000-4000-8000-000000000019"
	mapperEvidenceChannel  = "synthetic.quote"
)

type mapperEvidenceMapper struct {
	version        string
	bidCoefficient string
	rejectCode     normalize.QuarantineCode
	rejectField    string
	rejectState    normalize.SourceState
}

func (m mapperEvidenceMapper) Version() string { return m.version }

func (m mapperEvidenceMapper) Map(input normalize.MappingInput) ([]normalize.Row, error) {
	if m.rejectCode != "" {
		field := m.rejectField
		if field == "" {
			field = "bid_price"
		}
		state := m.rejectState
		if state == "" {
			state = normalize.SourceValue
		}
		return nil, normalize.RejectProjection(m.rejectCode, field, state)
	}
	exchangeTime := normalize.OptionalInt64{Value: input.Record.Envelope.ExchangeTimeNS.Value, Valid: input.Record.Envelope.ExchangeTimeNS.Valid}
	exchangeResolution := normalize.ResolutionAbsent
	if exchangeTime.Valid {
		exchangeResolution = normalize.ResolutionMillisecond
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{
		Record: input.Record, SchemaName: normalize.QuoteSchemaName, SchemaVersion: normalize.QuoteSchemaVersion,
		InstrumentUID: "synthetic-instrument", ExchangeTimeNS: exchangeTime, ExchangeTimeResolution: exchangeResolution,
		SourceEventTimeNS: normalize.OptionalInt64{}, SourceTimeResolution: input.Binding.SourceTimeResolution,
		SourceSchemaFingerprint: input.Fingerprint.Fingerprint, MapperVersion: input.Binding.MapperVersion,
		MapperBindingID: input.Binding.BindingID, CatalogSnapshotID: input.Binding.CatalogSnapshotID,
	})
	if err != nil {
		return nil, err
	}
	priceUnit := normalize.SpotPriceUnit("BASE", "QUOTE")
	amountUnit := normalize.BaseAssetUnit("BASE")
	row, err := normalize.NewQuoteRow(normalize.QuoteV1{
		Metadata: metadata, NativeSourceRole: "bookTicker_native_bbo", UpdateID: input.Record.Coordinate.ArrivalOrdinal,
		BidPrice:          normalize.Numeric{Decimal: normalize.Decimal{Coefficient: m.bidCoefficient, Scale: 2}, Unit: priceUnit},
		BidAmount:         normalize.Numeric{Decimal: normalize.Decimal{Coefficient: "200", Scale: 2}, Unit: amountUnit},
		AskPrice:          normalize.Numeric{Decimal: normalize.Decimal{Coefficient: "110", Scale: 2}, Unit: priceUnit},
		AskAmount:         normalize.Numeric{Decimal: normalize.Decimal{Coefficient: "300", Scale: 2}, Unit: amountUnit},
		RPIInclusionState: normalize.RPINotApplicable,
	})
	if err != nil {
		return nil, err
	}
	return []normalize.Row{row}, nil
}

func TestMapperCutoverDualRunByteIdentical(t *testing.T) {
	fixture := newMapperEvidenceFixture(t)
	book := fixedBookReplay("applied", "same-book")
	baselineMapper := mapperEvidenceMapper{version: "mapper-old", bidCoefficient: "100"}
	candidateMapper := mapperEvidenceMapper{version: "mapper-new", bidCoefficient: "100"}
	request := MapperDualRunRequest{
		Corpus: fixture.records, Baseline: fixture.orchestrator(t, baselineMapper, 0, normalize.OptionalInt64{}),
		Candidate:    fixture.orchestrator(t, candidateMapper, 0, normalize.OptionalInt64{}),
		BaselineBook: book, CandidateBook: book,
	}
	first, err := CompareMapperRuns(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompareMapperRuns(request)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := first.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := second.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !first.Publishable() || !second.Publishable() || first.ReportSHA256 != second.ReportSHA256 || !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("same corpus report changed: publishable=%v/%v hash=%x/%x", first.Publishable(), second.Publishable(), first.ReportSHA256, second.ReportSHA256)
	}
	if first.CorpusCount != uint32(len(fixture.records)) || len(first.Baseline.AcceptedFields) != len(fixture.records) ||
		first.Baseline.LogicalSHA256 == (normalize.Hash{}) || len(first.Baseline.Rejections) != 0 {
		t.Fatalf("dual-run summary = %#v", first)
	}
	if first.Baseline.LogicalSHA256 == first.Candidate.LogicalSHA256 ||
		first.Baseline.SemanticSHA256 != first.Candidate.SemanticSHA256 ||
		first.Baseline.AcceptedFields[0].EventID == first.Candidate.AcceptedFields[0].EventID {
		t.Fatalf("release provenance was not retained separately from semantic equivalence: %#v", first)
	}
	catalogEvidence, err := first.CatalogEvidence(
		mapperEvidenceSourceID,
		mapperEvidenceChannel,
		"20000000-0000-4000-8000-000000000019",
		"30000000-0000-4000-8000-000000000019",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !catalogEvidence.Publishable() {
		t.Fatalf("matching replay did not produce publishable catalog evidence: %#v", catalogEvidence)
	}
}

func TestMapperCutoverDualRunMismatchRegressions(t *testing.T) {
	fixture := newMapperEvidenceFixture(t)
	baselineMapper := mapperEvidenceMapper{version: "mapper-v1", bidCoefficient: "100"}
	baselineBook := fixedBookReplay("applied", "book-a")
	tests := []struct {
		name           string
		baseline       mapperEvidenceMapper
		candidate      mapperEvidenceMapper
		baselineClass  normalize.FingerprintClass
		candidateClass normalize.FingerprintClass
		candidateBook  DownstreamBookReplay
		wantCode       MapperMismatchCode
	}{
		{name: "changed field and semantic hash", baseline: baselineMapper, candidate: mapperEvidenceMapper{version: "mapper-v2", bidCoefficient: "101"}, candidateBook: baselineBook, wantCode: MismatchOrderedSemanticRowHash},
		{name: "changed rejection count", baseline: baselineMapper, candidate: mapperEvidenceMapper{version: "mapper-v2", bidCoefficient: "100", rejectCode: normalize.QuarantineInvalidField}, candidateBook: baselineBook, wantCode: MismatchRejectionCounts},
		{name: "same rejection code and count changed field state", baseline: mapperEvidenceMapper{version: "mapper-v1", rejectCode: normalize.QuarantineInvalidField, rejectField: "bid_price", rejectState: normalize.SourceValue}, candidate: mapperEvidenceMapper{version: "mapper-v2", rejectCode: normalize.QuarantineInvalidField, rejectField: "ask_price", rejectState: normalize.SourceMissing}, candidateBook: baselineBook, wantCode: MismatchRejectionProjection},
		{name: "same rejection code and count changed fingerprint class", baseline: mapperEvidenceMapper{version: "mapper-v1", rejectCode: normalize.QuarantineInvalidField}, candidate: mapperEvidenceMapper{version: "mapper-v2", rejectCode: normalize.QuarantineInvalidField}, baselineClass: normalize.FingerprintExact, candidateClass: normalize.FingerprintAdditiveHarmless, candidateBook: baselineBook, wantCode: MismatchRejectionProjection},
		{name: "changed book result", baseline: baselineMapper, candidate: baselineMapper, candidateBook: fixedBookReplay("sequence_gap", "book-a"), wantCode: MismatchDownstreamBookResult},
		{name: "changed book hash", baseline: baselineMapper, candidate: baselineMapper, candidateBook: fixedBookReplay("applied", "book-b"), wantCode: MismatchDownstreamBookHash},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baselineClass := test.baselineClass
			if baselineClass == "" {
				baselineClass = normalize.FingerprintExact
			}
			candidateClass := test.candidateClass
			if candidateClass == "" {
				candidateClass = normalize.FingerprintExact
			}
			report, err := CompareMapperRuns(MapperDualRunRequest{
				Corpus:       fixture.records,
				Baseline:     fixture.orchestratorRule(t, test.baseline, 0, normalize.OptionalInt64{}, baselineClass),
				Candidate:    fixture.orchestratorRule(t, test.candidate, 0, normalize.OptionalInt64{}, candidateClass),
				BaselineBook: baselineBook, CandidateBook: test.candidateBook,
			})
			if err != nil {
				t.Fatal(err)
			}
			if report.Publishable() {
				t.Fatal("mismatched dual run was publishable")
			}
			if !slices.ContainsFunc(report.Mismatches, func(mismatch MapperMismatch) bool { return mismatch.Code == test.wantCode }) {
				t.Fatalf("mismatch codes = %#v, want %q", report.Mismatches, test.wantCode)
			}
			if _, err := report.MarshalBinary(); err != nil {
				t.Fatalf("non-publishable evidence must remain durable and valid: %v", err)
			}
			catalogEvidence, err := report.CatalogEvidence(
				mapperEvidenceSourceID,
				mapperEvidenceChannel,
				"20000000-0000-4000-8000-000000000019",
				"30000000-0000-4000-8000-000000000019",
			)
			if err != nil {
				t.Fatal(err)
			}
			if catalogEvidence.Publishable() {
				t.Fatal("mismatched replay produced publishable catalog evidence")
			}
		})
	}
}

func TestMapperCutoverRejectionProjectionDetectsFieldStateAndFingerprint(t *testing.T) {
	baseProjection := MapperRejectionProjection{
		Record: MapperRecordIdentity{CorpusIndex: 1, ReceivedWallTimeNS: 99, Coordinate: normalize.RawCoordinate{SourceID: "source", ChannelID: "channel"}},
		Code:   normalize.QuarantineInvalidField, Field: "price", SourceState: normalize.SourceValue,
		FingerprintClass: normalize.FingerprintExact, SourceSchemaFingerprint: normalize.Hash{1},
	}
	base := MapperRunEvidence{
		RejectionProjections: []MapperRejectionProjection{baseProjection},
		RejectionCounts:      []MapperRejectionCount{{Code: normalize.QuarantineInvalidField, Count: 1}},
	}
	base.RejectionSHA256 = orderedRejectionHash(base.RejectionProjections)
	tests := []struct {
		name   string
		mutate func(*MapperRejectionProjection)
	}{
		{name: "field", mutate: func(projection *MapperRejectionProjection) { projection.Field = "amount" }},
		{name: "state", mutate: func(projection *MapperRejectionProjection) { projection.SourceState = normalize.SourceMissing }},
		{name: "fingerprint", mutate: func(projection *MapperRejectionProjection) { projection.SourceSchemaFingerprint[0] ^= 0xff }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			candidate.RejectionProjections = slices.Clone(base.RejectionProjections)
			test.mutate(&candidate.RejectionProjections[0])
			candidate.RejectionSHA256 = orderedRejectionHash(candidate.RejectionProjections)
			mismatches := compareMapperEvidence(base, candidate)
			if !slices.ContainsFunc(mismatches, func(mismatch MapperMismatch) bool { return mismatch.Code == MismatchRejectionProjection }) {
				t.Fatalf("rejection %s mismatch was not detected: %#v", test.name, mismatches)
			}
			if base.RejectionSHA256 == candidate.RejectionSHA256 {
				t.Fatalf("rejection %s digest did not change", test.name)
			}
			if !slices.Equal(base.RejectionCounts, candidate.RejectionCounts) {
				t.Fatal("regression changed rejection code/count")
			}
		})
	}
}

func TestMapperCutoverReceiveWallTimeHalfOpenAndExchangeTimeIndependent(t *testing.T) {
	fixture := newMapperEvidenceFixture(t)
	cutover := int64(100)
	beforeRecord := fixture.record(t, 1, cutover-1, capture.OptionalInt64{Value: 9_000, Valid: true})
	atRecord := fixture.record(t, 2, cutover, capture.OptionalInt64{})
	afterRecord := fixture.record(t, 3, cutover+1, capture.OptionalInt64{Value: 1_000, Valid: true})
	corpus := []normalize.RawRecord{beforeRecord, atRecord, afterRecord}
	oldMapper := mapperEvidenceMapper{version: "mapper-old", bidCoefficient: "100"}
	newMapper := mapperEvidenceMapper{version: "mapper-new", bidCoefficient: "100"}
	orchestrator := fixture.cutoverOrchestrator(t, oldMapper, newMapper, cutover)
	request := MapperCutoverRequest{
		Corpus: corpus, Orchestrator: orchestrator, Book: fixedBookReplay("applied", "cutover-book"),
		Cutover: MapperCutoverSpec{EffectiveFromNS: cutover, BeforeMapperVersion: oldMapper.version, AfterMapperVersion: newMapper.version},
	}
	first, err := ReplayMapperCutover(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ReplayMapperCutover(request)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := first.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := second.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !first.Publishable() || !bytes.Equal(firstBytes, secondBytes) || first.ReportSHA256 != second.ReportSHA256 {
		t.Fatalf("cutover report was not exact and stable: %#v", first)
	}
	if first.BeforeCount != 1 || first.AtBoundaryCount != 1 || first.AfterCount != 1 || len(first.Checks) != 3 ||
		first.Checks[0].ObservedMapperVersion != oldMapper.version || first.Checks[1].ObservedMapperVersion != newMapper.version ||
		first.Checks[2].ObservedMapperVersion != newMapper.version {
		t.Fatalf("half-open receive-time selection = %#v", first.Checks)
	}
	if first.Checks[1].Record.ReceivedWallTimeNS != cutover || first.Checks[0].Record.ReceivedWallTimeNS >= cutover {
		t.Fatalf("cutover coordinates = %#v", first.Checks)
	}
}

type mapperEvidenceFixture struct {
	snapshot    catalog.Snapshot
	fingerprint normalize.Hash
	payload     []byte
	records     []normalize.RawRecord
}

func newMapperEvidenceFixture(t *testing.T) mapperEvidenceFixture {
	t.Helper()
	rawMetadata := json.RawMessage(`{"symbol":"SYNTH"}`)
	snapshot, err := catalog.BuildFreshSnapshot(
		catalog.Source{SourceID: mapperEvidenceSourceID, Venue: "synthetic", ProductFamily: "spot", APIFamily: "fixture-v1", Environment: "test", Lifecycle: "active"},
		catalog.SourceVersion{
			OfficialAPIVersion: "v1", DocumentationURI: "https://example.invalid/synthetic", Endpoints: json.RawMessage(`{}`), Topology: json.RawMessage(`{}`),
			Entitlement: json.RawMessage(`{}`), Region: "synthetic", RateContract: json.RawMessage(`{}`), HeartbeatPolicy: json.RawMessage(`{}`),
			AcknowledgementPolicy: json.RawMessage(`{}`), ReconnectPolicy: json.RawMessage(`{}`),
		},
		[]catalog.ChannelContract{{
			ChannelID: mapperEvidenceChannel, NativeSelector: json.RawMessage(`{}`), Role: "quote", DataFamily: "quote", CadenceSource: "event",
			Aggregation: json.RawMessage(`{}`), Depth: json.RawMessage(`{}`), SequenceRules: json.RawMessage(`{}`), ChecksumRules: json.RawMessage(`{}`),
			PayloadSchema: json.RawMessage(`{}`), SupportState: "supported",
		}},
		[]catalog.InstrumentCandidate{{
			NativeID: "SYNTH", Aliases: []string{"SYNTH"}, Lifecycle: "active", BaseAsset: "BASE", QuoteAsset: "QUOTE", SettlementAsset: "QUOTE",
			Kind: "spot", Payoff: json.RawMessage(`{"kind":"spot"}`), Multiplier: "1", TickRules: json.RawMessage(`{}`), LotRules: json.RawMessage(`{}`),
			RawMetadata: rawMetadata, RawMetadataSHA256: sha256.Sum256(rawMetadata), NormalizedSchemaVersion: "v1",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"bid":"1.00","ask":"1.10"}`)
	observation, err := normalize.StructuralFingerprint(payload)
	if err != nil {
		t.Fatal(err)
	}
	fixture := mapperEvidenceFixture{snapshot: snapshot, fingerprint: observation.Fingerprint, payload: payload}
	fixture.records = []normalize.RawRecord{
		fixture.record(t, 1, 10, capture.OptionalInt64{}),
		fixture.record(t, 2, 11, capture.OptionalInt64{Value: 8_000, Valid: true}),
	}
	return fixture
}

func (f mapperEvidenceFixture) record(t *testing.T, arrival uint64, received int64, exchange capture.OptionalInt64) normalize.RawRecord {
	t.Helper()
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: mapperEvidenceSourceID, ChannelOrEndpoint: mapperEvidenceChannel,
		ConnectionEpoch: capture.OptionalEpoch{Value: [16]byte{0x19}, Valid: true}, ArrivalOrdinal: arrival,
		ReceivedWallTimeNS: received, ClockEpochID: "mapper-evidence-clock", MonotonicNSSinceClockEpoch: arrival,
		ExchangeTimeNS: exchange, ExchangeTimeResolution: capture.ExchangeTimeAbsent,
		PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved, RecorderVersion: "mapper-evidence-recorder-v1",
	}
	if exchange.Valid {
		envelope.ExchangeTimeResolution = capture.ExchangeTimeMillisecond
	}
	envelope.SetRawPayload(f.payload)
	record, err := normalize.BindRawRecord(envelope, normalize.Hash(sha256.Sum256([]byte("mapper-evidence-segment"))), arrival-1, nil)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (f mapperEvidenceFixture) orchestrator(t *testing.T, mapper mapperEvidenceMapper, from int64, until normalize.OptionalInt64) *normalize.Orchestrator {
	t.Helper()
	return f.orchestratorRule(t, mapper, from, until, normalize.FingerprintExact)
}

func (f mapperEvidenceFixture) orchestratorRule(t *testing.T, mapper mapperEvidenceMapper, from int64, until normalize.OptionalInt64, class normalize.FingerprintClass) *normalize.Orchestrator {
	t.Helper()
	binding := normalize.MapperBinding{
		Version: normalize.MapperBindingVersion, SourceID: mapperEvidenceSourceID, ChannelID: mapperEvidenceChannel,
		EffectiveFromNS: from, EffectiveUntilNS: until, MapperVersion: mapper.version,
		SourceTimeResolution: normalize.ResolutionMillisecond, CatalogSnapshotID: normalize.Hash(f.snapshot.SHA256),
		FingerprintRules: []normalize.FingerprintRule{{Fingerprint: f.fingerprint, Class: class}},
	}
	orchestrator, err := normalize.NewOrchestrator(f.snapshot, []normalize.BoundMapper{{Binding: binding, Mapper: mapper}})
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator
}

func (f mapperEvidenceFixture) cutoverOrchestrator(t *testing.T, before, after mapperEvidenceMapper, cutover int64) *normalize.Orchestrator {
	t.Helper()
	makeBinding := func(mapper mapperEvidenceMapper, from int64, until normalize.OptionalInt64) normalize.BoundMapper {
		return normalize.BoundMapper{Binding: normalize.MapperBinding{
			Version: normalize.MapperBindingVersion, SourceID: mapperEvidenceSourceID, ChannelID: mapperEvidenceChannel,
			EffectiveFromNS: from, EffectiveUntilNS: until, MapperVersion: mapper.version,
			SourceTimeResolution: normalize.ResolutionMillisecond, CatalogSnapshotID: normalize.Hash(f.snapshot.SHA256),
			FingerprintRules: []normalize.FingerprintRule{{Fingerprint: f.fingerprint, Class: normalize.FingerprintExact}},
		}, Mapper: mapper}
	}
	orchestrator, err := normalize.NewOrchestrator(f.snapshot, []normalize.BoundMapper{
		makeBinding(after, cutover, normalize.OptionalInt64{}),
		makeBinding(before, 0, normalize.OptionalInt64{Value: cutover, Valid: true}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator
}

func fixedBookReplay(result, seed string) DownstreamBookReplay {
	hash := normalize.Hash(sha256.Sum256([]byte(seed)))
	return DownstreamBookReplayFunc(func([]normalize.Row) (DownstreamBookResult, error) {
		return DownstreamBookResult{Version: DownstreamBookResultVersionV1, Result: result, LogicalHash: hash}, nil
	})
}
