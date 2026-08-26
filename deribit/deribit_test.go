package deribit

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

func TestDeribitRawAuthorizationAndDeclaredFallback(t *testing.T) {
	if _, err := SourceContract(CadencePolicy{Requested: CadenceRaw, Fallback: Cadence100MS}); !errors.Is(err, ErrAuthorizationRequired) {
		t.Fatalf("unauthorized raw contract error = %v", err)
	}
	policy := CadencePolicy{Requested: CadenceRaw, Fallback: Cadence100MS, Authorized: true}
	contract, err := SourceContract(policy)
	if err != nil {
		t.Fatalf("SourceContract(raw): %v", err)
	}
	if err := contract.Validate(); err != nil {
		t.Fatalf("raw contract validation: %v", err)
	}
	for _, capability := range contract.Capabilities {
		if capability.Entitlement != "authorized_raw_1ms_aggregation" {
			t.Fatalf("raw entitlement = %q", capability.Entitlement)
		}
	}
	fallback, err := policy.FallbackPolicy()
	if err != nil || fallback.Requested != Cadence100MS || fallback.Authorized || fallback.Fallback != "" {
		t.Fatalf("fallback = %#v, %v", fallback, err)
	}
	fallbackContract, err := SourceContract(fallback)
	if err != nil || fallbackContract.ContractID == contract.ContractID {
		t.Fatalf("fallback contract = %#v, %v", fallbackContract, err)
	}
	session, err := NewSession(policy, []ChannelRequest{{Role: RoleBook, Instrument: "BTC-PERPETUAL"}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := session.SubscribeRequest(2); !errors.Is(err, ErrAuthorizationRequired) {
		t.Fatalf("pre-auth subscribe error = %v", err)
	}
	credentials, err := NewClientCredentials("synthetic-client", []byte("synthetic-secret"))
	if err != nil {
		t.Fatalf("NewClientCredentials: %v", err)
	}
	authPayload, err := session.AuthorizationRequest(1, credentials)
	if err != nil {
		t.Fatalf("AuthorizationRequest: %v", err)
	}
	var auth struct {
		Method string `json:"method"`
		Params struct {
			GrantType string `json:"grant_type"`
		} `json:"params"`
	}
	if err := json.Unmarshal(authPayload, &auth); err != nil || auth.Method != "public/auth" || auth.Params.GrantType != "client_credentials" {
		t.Fatalf("auth request = %s, %v", authPayload, err)
	}
	if err := session.AcceptAuthorizationResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":{"access_token":"synthetic-token"}}`), 1); err != nil {
		t.Fatalf("AcceptAuthorizationResponse: %v", err)
	}
	subscribe, err := session.SubscribeRequest(2)
	if err != nil {
		t.Fatalf("SubscribeRequest: %v", err)
	}
	var request struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(subscribe, &request); err != nil || request.Method != "public/subscribe" {
		t.Fatalf("raw public-channel subscribe = %s, %v", subscribe, err)
	}
	if err := session.AcceptAuthorizationResponse(deribitFixture(t, "credit-exhausted.json"), 1); !errors.Is(err, ErrReconnectRequired) {
		t.Fatalf("authorization 10028 error = %v", err)
	}
	if _, err := session.SubscribeRequest(3); !errors.Is(err, ErrAuthorizationRequired) {
		t.Fatalf("post-reconnect raw subscribe error = %v", err)
	}
}

func TestDeribitSubscribeDiffHeartbeatAnd10028Reconnect(t *testing.T) {
	session, err := NewSession(CadencePolicy{Requested: Cadence100MS}, []ChannelRequest{
		{Role: RoleTrade, Instrument: "BTC-PERPETUAL"},
		{Role: RoleBook, Instrument: "BTC-PERPETUAL"},
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := session.SubscribeRequest(7); err != nil {
		t.Fatalf("SubscribeRequest: %v", err)
	}
	if _, err := session.SubscribeRequest(8); !errors.Is(err, ErrSubscriptionPending) {
		t.Fatalf("second subscribe before ACK error = %v", err)
	}
	decision, err := session.Inspect(deribitFixture(t, "subscribe-partial.json"), 0)
	if !errors.Is(err, ErrSubscribeMismatch) || decision.Reconciliation == nil || decision.Reconciliation.Exact ||
		!slices.Equal(decision.Reconciliation.Missing, []string{"trades.BTC-PERPETUAL.100ms"}) || len(decision.Reconciliation.Unexpected) != 0 {
		t.Fatalf("subscribe reconciliation = %#v, %v", decision.Reconciliation, err)
	}
	if _, err := session.SubscribeRequest(8); err != nil {
		t.Fatalf("corrective subscribe after partial ACK: %v", err)
	}
	unrelatedError := []byte(`{"jsonrpc":"2.0","id":99,"error":{"code":10001,"message":"synthetic unrelated error"}}`)
	if _, err := session.Inspect(unrelatedError, 0); !errors.Is(err, ErrInvalidRPC) {
		t.Fatalf("unrelated RPC error = %v", err)
	}
	if _, err := session.SubscribeRequest(9); !errors.Is(err, ErrSubscriptionPending) {
		t.Fatalf("unrelated response cleared pending subscribe: %v", err)
	}
	matchingError := []byte(`{"jsonrpc":"2.0","id":8,"error":{"code":10001,"message":"synthetic subscribe error"}}`)
	if _, err := session.Inspect(matchingError, 0); !errors.Is(err, ErrInvalidRPC) {
		t.Fatalf("matching subscribe RPC error = %v", err)
	}
	if _, err := session.SubscribeRequest(9); err != nil {
		t.Fatalf("corrective subscribe after error ACK: %v", err)
	}
	empty, err := ReconcileSubscriptions(session.Channels(), nil)
	if err != nil || empty.Exact || !slices.Equal(empty.Missing, session.Channels()) || len(empty.Returned) != 0 {
		t.Fatalf("empty subscription reconciliation = %#v, %v", empty, err)
	}
	exactPayload := []byte(`{"jsonrpc":"2.0","id":9,"result":["book.BTC-PERPETUAL.100ms","trades.BTC-PERPETUAL.100ms"]}`)
	decision, err = session.Inspect(exactPayload, 0)
	if err != nil || decision.Reconciliation == nil || !decision.Reconciliation.Exact {
		t.Fatalf("exact subscription reconciliation = %#v, %v", decision.Reconciliation, err)
	}
	if _, err := session.SubscribeRequest(10); err != nil {
		t.Fatalf("subscribe after exact ACK: %v", err)
	}
	decision, err = session.Inspect(deribitFixture(t, "heartbeat-test-request.json"), 11)
	if err != nil || decision.Action != SessionRespondTest || !decision.ReuseConnection {
		t.Fatalf("heartbeat decision = %#v, %v", decision, err)
	}
	var response struct {
		ID     uint64 `json:"id"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(decision.Response, &response); err != nil || response.ID != 11 || response.Method != "public/test" {
		t.Fatalf("heartbeat response = %s, %v", decision.Response, err)
	}
	decision, err = session.Inspect(deribitFixture(t, "credit-exhausted.json"), 0)
	if !errors.Is(err, ErrReconnectRequired) || decision.Action != SessionReconnectAfterCredit || decision.ReuseConnection {
		t.Fatalf("10028 decision = %#v, %v", decision, err)
	}
}

func TestDeribitMappingsAndTradeOnlyLiquidation(t *testing.T) {
	inverse, inverseTerms := deribitTerms(t, "instrument-inverse.json", "deribit:BTC-PERPETUAL:1")
	_, linearTerms := deribitTerms(t, "instrument-linear.json", "deribit:BTC_USDC-PERPETUAL:1")
	option, optionTerms := deribitTerms(t, "instrument-option.json", "deribit:BTC-30DEC27-50000-C:1")
	trades, err := ParseTrades(deribitFixture(t, "trade-liquidation.json"))
	if err != nil || len(trades) != 1 {
		t.Fatalf("ParseTrades = %#v, %v", trades, err)
	}
	trade, err := trades[0].Normalized(inverseTerms)
	if err != nil {
		t.Fatalf("Normalized trade: %v", err)
	}
	if trade.UnitInference.Unit.Kind != normalize.NativeUnitUSD || trade.SourceAggregation != "100ms" ||
		trade.Liquidation == nil || trade.Liquidation.Completeness != normalize.LiquidationTradeFlagOnly ||
		trade.Liquidation.PublicChannel ||
		trade.Liquidation.NativeSourceRole != "deribit_public_trade_liquidation_flag" {
		t.Fatalf("inverse trade = %#v", trade)
	}
	quote, err := ParseQuote(deribitFixture(t, "quote.json"))
	if err != nil {
		t.Fatalf("ParseQuote: %v", err)
	}
	mappedQuote, err := quote.Normalized(inverseTerms)
	if err != nil || mappedQuote.BidAmount.Value.Unit.Kind != normalize.NativeUnitUSD {
		t.Fatalf("quote = %#v, %v", mappedQuote, err)
	}
	linearTicker, err := ParseTicker(deribitFixture(t, "ticker-linear.json"))
	if err != nil {
		t.Fatalf("ParseTicker(linear): %v", err)
	}
	mappedLinear, err := linearTicker.Normalized(linearTerms)
	if err != nil || mappedLinear.UnitInference.Unit.Kind != normalize.NativeUnitBaseAsset || mappedLinear.OpenInterest.Value.Unit.AssetID != "BTC" {
		t.Fatalf("linear ticker = %#v, %v", mappedLinear, err)
	}
	derivativeMetadata := deribitMetadata(
		t, normalize.DerivativeTickerSchemaName, normalize.DerivativeTickerSchemaVersion,
		linearTerms.InstrumentUID, mappedLinear.SourceTimeNS, mappedLinear.Channel,
		deribitFixture(t, "ticker-linear.json"),
	)
	derivativeEvent, err := mappedLinear.DerivativeTickerV1(derivativeMetadata)
	if err != nil || len(derivativeEvent.OpenInterest) != 1 ||
		derivativeEvent.OpenInterest[0].Native.Unit.Kind != normalize.NativeUnitBaseAsset {
		t.Fatalf("DerivativeTickerV1 = %#v, %v", derivativeEvent, err)
	}
	optionTicker, err := ParseTicker(deribitFixture(t, "ticker-option.json"))
	if err != nil {
		t.Fatalf("ParseTicker(option): %v", err)
	}
	mappedOption, err := optionTicker.NormalizedOption(option, optionTerms)
	if err != nil || mappedOption.CallPut != normalize.OptionCall || mappedOption.Ticker.MarkIV.State != normalize.SourceValue ||
		mappedOption.Ticker.UnitInference.Unit.Kind != normalize.NativeUnitBaseAsset ||
		mappedOption.Ticker.BestBidPrice.Value.Unit != normalize.BaseAssetUnit("BTC") ||
		mappedOption.Strike.Unit != normalize.SpotPriceUnit("BTC", "USD") ||
		mappedOption.Ticker.IndexPrice.Value.Unit != normalize.SpotPriceUnit("BTC", "USD") ||
		mappedOption.Ticker.Delta.Value.Unit.Kind != normalize.NativeUnitVenueUnspecified {
		t.Fatalf("option summary = %#v, %v", mappedOption, err)
	}
	optionMetadata := deribitMetadata(
		t, normalize.OptionSummarySchemaName, normalize.OptionSummarySchemaVersion,
		optionTerms.InstrumentUID, mappedOption.Ticker.SourceTimeNS, mappedOption.Ticker.Channel,
		deribitFixture(t, "ticker-option.json"),
	)
	optionEvent, err := mappedOption.OptionSummaryV1(optionMetadata, DocumentationAccessedAtNS)
	if err != nil || optionEvent.BidPrice.Value.Unit != normalize.BaseAssetUnit("BTC") ||
		optionEvent.Strike.Value.Unit != normalize.SpotPriceUnit("BTC", "USD") ||
		optionEvent.Delta.Value.Unit.Kind != normalize.NativeUnitVenueUnspecified {
		t.Fatalf("OptionSummaryV1 = %#v, %v", optionEvent, err)
	}
	funding, err := ParseFunding(deribitFixture(t, "funding.json"))
	if err != nil {
		t.Fatalf("ParseFunding: %v", err)
	}
	if _, err := funding.Normalized(inverseTerms.InstrumentUID); err != nil {
		t.Fatalf("Normalized funding: %v", err)
	}
	index, err := ParseIndex(deribitFixture(t, "index.json"))
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if price, _, err := index.Normalized("BTC", "USD"); err != nil || price.State != normalize.SourceValue {
		t.Fatalf("Normalized index = %#v, %v", price, err)
	}
	lifecycle, err := ParseLifecycle(deribitFixture(t, "lifecycle-state.json"))
	if err != nil {
		t.Fatalf("ParseLifecycle: %v", err)
	}
	mappedLifecycle, err := lifecycle.Normalized(inverseTerms.InstrumentUID)
	if err != nil || mappedLifecycle.State != normalize.InstrumentStateDelivered || inverse.InstrumentName != lifecycle.InstrumentName {
		t.Fatalf("lifecycle = %#v, %v", mappedLifecycle, err)
	}
}

func TestDeribitInferenceProvisionalSourceState(t *testing.T) {
	_, terms := deribitTerms(t, "instrument-inverse.json", "deribit:BTC-PERPETUAL:1")
	proven, err := normalize.InferDeribitAmountUnit(terms, DocumentationAccessedAtNS+1)
	if err != nil || proven.State != normalize.DeribitInferenceFixtureProven ||
		proven.Authority != capture.RuleAdapterPolicyInference {
		t.Fatalf("proven inference = %#v, %v", proven, err)
	}
	unboundTerms := terms
	unboundTerms.DerivedFrom = ""
	unbound, err := normalize.InferDeribitAmountUnit(unboundTerms, DocumentationAccessedAtNS+1)
	if err != nil || unbound.State != normalize.DeribitInferenceProvisional ||
		unbound.DerivedFrom != "" || unbound.Reason == "" {
		t.Fatalf("unbound provenance inference = %#v, %v", unbound, err)
	}
	terms.CatalogGeneration = 0
	terms.MetadataRawSHA256 = normalize.Hash{}
	provisional, err := normalize.InferDeribitAmountUnit(terms, DocumentationAccessedAtNS+1)
	if err != nil || provisional.State != normalize.DeribitInferenceProvisional ||
		provisional.Unit.Kind != normalize.NativeUnitVenueUnspecified || provisional.Reason == "" ||
		provisional.CatalogGeneration != 0 || provisional.MetadataRawSHA256 != (normalize.Hash{}) {
		t.Fatalf("provisional inference = %#v, %v", provisional, err)
	}
}

func TestDeribitOfflineVerifierAndExactRootContainment(t *testing.T) {
	summary, err := VerifyFixtures("testdata")
	if err != nil {
		t.Fatalf("VerifyFixtures(root): %v", err)
	}
	fromManifest, err := VerifyFixtures(filepath.Join("testdata", "manifest.json"))
	if err != nil {
		t.Fatalf("VerifyFixtures(manifest): %v", err)
	}
	if summary.ManifestSHA256 != fromManifest.ManifestSHA256 || summary.FixtureCount != 16 ||
		len(summary.ClassificationCounts) != 1 ||
		summary.ClassificationCounts[0] != (FixtureClassificationCount{
			Classification: normalize.DeribitFixtureClassificationSynthetic, Count: 16,
		}) || len(summary.FixtureProvenance) != 16 ||
		summary.HeartbeatResponseMethod != "public/test" ||
		summary.CreditExhaustionAction != string(SessionReconnectAfterCredit) ||
		summary.ProvisionalUnitState != string(normalize.DeribitInferenceProvisional) ||
		summary.LiquidationCompleteness != string(normalize.LiquidationTradeFlagOnly) {
		t.Fatalf("summary = %#v", summary)
	}
	var tamperedManifest FixtureManifest
	if err := json.Unmarshal(deribitFixture(t, "manifest.json"), &tamperedManifest); err != nil {
		t.Fatalf("decode fixture manifest: %v", err)
	}
	tamperedRoot := t.TempDir()
	for _, entry := range tamperedManifest.Fixtures {
		if err := os.WriteFile(filepath.Join(tamperedRoot, entry.Path), deribitFixture(t, entry.Path), 0o600); err != nil {
			t.Fatalf("copy fixture %s: %v", entry.Role, err)
		}
	}
	tamperedManifest.Fixtures[0].OfficialURL = "https://example.invalid/not-deribit"
	tamperedRaw, err := json.Marshal(tamperedManifest)
	if err != nil {
		t.Fatalf("encode tampered manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tamperedRoot, "manifest.json"), tamperedRaw, 0o600); err != nil {
		t.Fatalf("write tampered manifest: %v", err)
	}
	if _, err := VerifyFixtures(tamperedRoot); !errors.Is(err, ErrFixtureVerification) {
		t.Fatalf("tampered provenance error = %v", err)
	}
	outside := t.TempDir()
	outsideFixture := filepath.Join(outside, "outside.json")
	payload := []byte(`{"synthetic":true}`)
	if err := os.WriteFile(outsideFixture, payload, 0o600); err != nil {
		t.Fatalf("write outside fixture: %v", err)
	}
	root := t.TempDir()
	if err := os.Symlink(outsideFixture, filepath.Join(root, "escape.json")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	digest := sha256.Sum256(payload)
	manifest := FixtureManifest{
		Version: FixtureManifestVersion, Venue: "deribit", AccessDate: "2026-08-22", Provenance: "synthetic",
		Fixtures: []FixtureEntry{{
			Role: "book_snapshot", Path: "escape.json", SHA256: stringHex(digest[:]),
			Classification: normalize.DeribitFixtureClassificationSynthetic,
			OfficialURL:    BookDocumentationURI, Section: "subscription.params.data.snapshot-change-continuity",
			DerivedFrom: BookDocumentationURI + "#subscription.params.data.snapshot-change-continuity",
		}},
	}
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifestRaw, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureVerification) {
		t.Fatalf("symlink escape error = %v", err)
	}
}

func deribitFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func deribitTerms(t *testing.T, name, uid string) (Instrument, normalize.DeribitInstrumentTerms) {
	t.Helper()
	raw := deribitFixture(t, name)
	instrument, err := ParseInstrument(raw)
	if err != nil {
		t.Fatalf("ParseInstrument: %v", err)
	}
	digest := normalize.Hash(sha256.Sum256(raw))
	terms, err := instrument.Terms(InstrumentEvidence{
		InstrumentUID: uid, CatalogGeneration: 1, MetadataRawSHA256: digest,
		ValidFromNS:           DocumentationAccessedAtNS,
		FixtureClassification: normalize.DeribitFixtureClassificationSynthetic,
		OfficialURL:           normalize.DeribitInstrumentProvenanceURL,
		Section:               normalize.DeribitInstrumentProvenanceSection,
		DerivedFrom:           normalize.DeribitInstrumentDerivedFrom,
	})
	if err != nil {
		t.Fatalf("Instrument.Terms: %v", err)
	}
	return instrument, terms
}

func deribitMetadata(
	t *testing.T,
	schema string,
	version uint16,
	instrumentUID string,
	sourceTimeNS int64,
	channel string,
	payload []byte,
) normalize.Metadata {
	t.Helper()
	receivedTimeNS := sourceTimeNS + 1_000_000_000
	epoch := [16]byte{1}
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: SourceID, ChannelOrEndpoint: channel,
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true}, ArrivalOrdinal: 1,
		ExchangeTimeNS:         capture.OptionalInt64{Value: sourceTimeNS, Valid: true},
		ExchangeTimeResolution: capture.ExchangeTimeMillisecond,
		ReceivedWallTimeNS:     receivedTimeNS, ClockEpochID: "deribit-synthetic-clock",
		MonotonicNSSinceClockEpoch: 1, PayloadEncoding: capture.PayloadEncodingJSON,
		TerminalOutcome: capture.TerminalObserved, RecorderVersion: "deribit-fixture-verifier-v1",
	}
	envelope.SetRawPayload(payload)
	record, err := normalize.BindRawRecord(
		envelope, normalize.Hash(sha256.Sum256([]byte("deribit-synthetic-segment"))), 1, nil,
	)
	if err != nil {
		t.Fatalf("BindRawRecord: %v", err)
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{
		Record: record, SchemaName: schema, SchemaVersion: version, InstrumentUID: instrumentUID,
		ExchangeTimeNS:          normalize.OptionalInt64{Value: sourceTimeNS, Valid: true},
		ExchangeTimeResolution:  normalize.ResolutionMillisecond,
		SourceEventTimeNS:       normalize.OptionalInt64{Value: sourceTimeNS, Valid: true},
		SourceTimeResolution:    normalize.ResolutionMillisecond,
		SourceSchemaFingerprint: normalize.Hash(sha256.Sum256([]byte("deribit-synthetic-schema"))),
		MapperVersion:           "deribit-json-rpc-v2-mapper-v1",
		MapperBindingID:         normalize.Hash(sha256.Sum256([]byte("deribit-synthetic-binding"))),
		CatalogSnapshotID:       normalize.Hash(sha256.Sum256([]byte("deribit-synthetic-catalog"))),
	})
	if err != nil {
		t.Fatalf("NewMetadata: %v", err)
	}
	return metadata
}

func stringHex(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, b := range value {
		encoded[index*2] = digits[b>>4]
		encoded[index*2+1] = digits[b&0x0f]
	}
	return string(encoded)
}
