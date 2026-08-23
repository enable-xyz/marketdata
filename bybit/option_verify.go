package bybit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

const OptionEvidenceVersion uint16 = 1

type OptionEvidenceSummary struct {
	Version              uint16          `json:"version"`
	Venue                string          `json:"venue"`
	AccessDate           string          `json:"access_date"`
	ManifestSHA256       string          `json:"manifest_sha256"`
	FixtureCount         uint32          `json:"fixture_count"`
	OfficialDerivedCount uint32          `json:"official_derived_count"`
	SyntheticCount       uint32          `json:"synthetic_count"`
	Checks               []EvidenceCheck `json:"checks"`
	EvidenceSHA256       string          `json:"evidence_sha256"`
}

type optionManifestRoleContract struct {
	ID             string
	Classification string
	File           string
	SourceURL      string
	SourceSection  string
	DerivedFrom    string
}

var optionManifestRoleContracts = map[string]optionManifestRoleContract{
	"option_trade": {
		ID: "option-trade", Classification: "official_documentation_projection", File: "official/trade.json",
		SourceURL: OptionTradeDocumentationURI, SourceSection: "Option response parameters and response example",
	},
	"option_book_snapshot": {
		ID: "option-book-25", Classification: "official_documentation_projection", File: "official/book-25-snapshot.json",
		SourceURL: OptionOrderbookDocumentationURI, SourceSection: "Option depth contract and response parameters",
	},
	"option_ticker_snapshot": {
		ID: "option-ticker", Classification: "official_documentation_projection", File: "official/ticker-snapshot.json",
		SourceURL: OptionTickerDocumentationURI, SourceSection: "Option response parameters and snapshot contract",
	},
	"option_metadata": {
		ID: "option-metadata", Classification: "official_documentation_projection", File: "official/instrument-option.json",
		SourceURL: OptionInstrumentDocumentationURI, SourceSection: "Option response parameters",
	},
	"base_coin_topics": {
		ID: "base-coin-topics", Classification: "synthetic_topic_identity_contract", File: "synthetic/base-coin-topics.json",
		SourceURL: OptionTradeDocumentationURI, DerivedFrom: "documented Option publicTrade and tickers subscription identity rules",
	},
	"minimum_depth": {
		ID: "minimum-depth", Classification: "synthetic_depth_contract", File: "synthetic/minimum-depth.json",
		SourceURL: OptionOrderbookDocumentationURI, DerivedFrom: "documented Option depths 25 and 100 and absence of depth 1",
	},
	"snapshot_only_mutation": {
		ID: "ticker-delta", Classification: "synthetic_message_type_mutation", File: "synthetic/ticker-delta.json",
		SourceURL: OptionTickerDocumentationURI, DerivedFrom: "official/ticker-snapshot.json with snapshot changed to delta",
	},
	"greek_type_drift": {
		ID: "greek-type-drift", Classification: "synthetic_type_drift_mutation", File: "synthetic/greek-type-drift.json",
		SourceURL: OptionTickerDocumentationURI, DerivedFrom: "official/ticker-snapshot.json with delta changed from JSON string to number",
	},
	"unsupported_roles": {
		ID: "unsupported-roles", Classification: "synthetic_capability_claims", File: "synthetic/unsupported-roles.json",
		SourceURL: OptionOrderbookDocumentationURI, DerivedFrom: "documented Option channel inventory and allLiquidation derivatives-only scope",
	},
}

type verifiedOptionFixture struct {
	entry   FixtureEntry
	payload []byte
}

// VerifyOptionFixtures deterministically consumes an explicit manifest or root.
// It performs no discovery or network access and reads only regular digest-bound
// files that remain canonically contained by the supplied root.
func VerifyOptionFixtures(manifestPathOrRoot string) (OptionEvidenceSummary, error) {
	manifestPath, root, err := resolveManifest(manifestPathOrRoot)
	if err != nil {
		return OptionEvidenceSummary{}, err
	}
	canonicalRoot, err := canonicalFixturePath(root)
	if err != nil {
		return OptionEvidenceSummary{}, err
	}
	if err := ensureFixtureContained(canonicalRoot, manifestPath); err != nil {
		return OptionEvidenceSummary{}, err
	}
	manifestBytes, err := readRegularBounded(manifestPath, maximumFixtureBytes)
	if err != nil {
		return OptionEvidenceSummary{}, err
	}
	var manifest FixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil || manifest.Version != FixtureManifestVersion || manifest.Venue != "bybit-v5-option" || manifest.AccessDate != DocumentationAccessDate || manifest.FixtureClaim == "" || len(manifest.Fixtures) != len(optionManifestRoleContracts) {
		return OptionEvidenceSummary{}, fmt.Errorf("%w: invalid option manifest", ErrFixtureBoundary)
	}
	fixtures := make(map[string]verifiedOptionFixture, len(manifest.Fixtures))
	ids := make(map[string]struct{}, len(manifest.Fixtures))
	official, synthetic := uint32(0), uint32(0)
	for _, entry := range manifest.Fixtures {
		contract, ok := optionManifestRoleContracts[entry.Role]
		expectedProvenance := contract.SourceSection
		if strings.HasPrefix(contract.Classification, "synthetic_") {
			expectedProvenance = contract.DerivedFrom
		}
		if !ok || contract.ID == "" || contract.Classification == "" || contract.File == "" || contract.SourceURL == "" || expectedProvenance == "" ||
			entry.ID != contract.ID || entry.Classification != contract.Classification || entry.File != contract.File ||
			entry.SourceURL != contract.SourceURL || entry.SourceSection != contract.SourceSection || entry.DerivedFrom != contract.DerivedFrom ||
			entry.ByteLength == 0 || len(entry.SHA256) != sha256.Size*2 {
			return OptionEvidenceSummary{}, fmt.Errorf("%w: option fixture role identity mismatch for %s", ErrFixtureBoundary, entry.Role)
		}
		if _, exists := ids[entry.ID]; exists {
			return OptionEvidenceSummary{}, fmt.Errorf("%w: duplicate option fixture ID %s", ErrFixtureBoundary, entry.ID)
		}
		ids[entry.ID] = struct{}{}
		if _, exists := fixtures[entry.Role]; exists {
			return OptionEvidenceSummary{}, fmt.Errorf("%w: duplicate option fixture role %s", ErrFixtureBoundary, entry.Role)
		}
		if entry.Classification == "official_documentation_projection" {
			if entry.SourceSection == "" || entry.DerivedFrom != "" {
				return OptionEvidenceSummary{}, fmt.Errorf("%w: incomplete official-derived provenance for %s", ErrFixtureBoundary, entry.ID)
			}
			official++
		} else if strings.HasPrefix(entry.Classification, "synthetic_") {
			if entry.DerivedFrom == "" || entry.SourceSection != "" {
				return OptionEvidenceSummary{}, fmt.Errorf("%w: incomplete synthetic provenance for %s", ErrFixtureBoundary, entry.ID)
			}
			synthetic++
		} else {
			return OptionEvidenceSummary{}, fmt.Errorf("%w: invalid option fixture classification", ErrFixtureBoundary)
		}
		clean := filepath.Clean(entry.File)
		if clean != entry.File || filepath.IsAbs(clean) || clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return OptionEvidenceSummary{}, fmt.Errorf("%w: option fixture path traversal", ErrFixtureBoundary)
		}
		path := filepath.Join(root, clean)
		if err := ensureFixtureContained(canonicalRoot, path); err != nil {
			return OptionEvidenceSummary{}, err
		}
		payload, err := readRegularBounded(path, maximumFixtureBytes)
		if err != nil {
			return OptionEvidenceSummary{}, err
		}
		digest := sha256.Sum256(payload)
		if len(payload) != int(entry.ByteLength) || hex.EncodeToString(digest[:]) != entry.SHA256 {
			return OptionEvidenceSummary{}, fmt.Errorf("%w: immutable option fixture mismatch for %s", ErrFixtureBoundary, entry.ID)
		}
		fixtures[entry.Role] = verifiedOptionFixture{entry: entry, payload: payload}
	}
	checks, err := verifyOptionFixtureContracts(fixtures)
	if err != nil {
		return OptionEvidenceSummary{}, err
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	summary := OptionEvidenceSummary{
		Version: OptionEvidenceVersion, Venue: manifest.Venue, AccessDate: manifest.AccessDate,
		ManifestSHA256: hex.EncodeToString(manifestDigest[:]), FixtureCount: uint32(len(manifest.Fixtures)),
		OfficialDerivedCount: official, SyntheticCount: synthetic, Checks: checks,
	}
	material, err := json.Marshal(summary)
	if err != nil {
		return OptionEvidenceSummary{}, err
	}
	evidenceDigest := sha256.Sum256(material)
	summary.EvidenceSHA256 = hex.EncodeToString(evidenceDigest[:])
	return summary, nil
}

func verifyOptionFixtureContracts(fixtures map[string]verifiedOptionFixture) ([]EvidenceCheck, error) {
	need := func(role string) (verifiedOptionFixture, error) {
		fixture, ok := fixtures[role]
		if !ok {
			return verifiedOptionFixture{}, fmt.Errorf("%w: missing option fixture role %s", ErrFixtureBoundary, role)
		}
		return fixture, nil
	}
	checks := make([]EvidenceCheck, 0, len(fixtures))
	addCheck := func(name string, values ...verifiedOptionFixture) {
		ids := make([]string, len(values))
		for i, value := range values {
			ids[i] = value.entry.ID
		}
		slices.Sort(ids)
		checks = append(checks, EvidenceCheck{Name: name, FixtureIDs: ids})
	}

	tradeFixture, err := need("option_trade")
	if err != nil {
		return nil, err
	}
	trades, err := ParseOptionTrades(tradeFixture.payload)
	if err != nil || len(trades) != 1 || trades[0].BaseCoin != "BTC" || trades[0].TradeID != "20f43950-d8dd-5b31-9112-a178eb6023af" ||
		trades[0].MarkIV.State != normalize.SourceValue || trades[0].MarkIV.Text != "0.7567" ||
		trades[0].TradeIV.State != normalize.SourceValue || trades[0].TradeIV.Text != "0.8000" {
		return nil, fmt.Errorf("%w: option trade semantic check", ErrFixtureBoundary)
	}
	addCheck("option_trade_strict_context", tradeFixture)

	bookFixture, err := need("option_book_snapshot")
	if err != nil {
		return nil, err
	}
	bookMessage, err := ParseOptionOrderbook(bookFixture.payload)
	book, constructorErr := NewOptionBook("BTC-30DEC22-18000-P", OptionMinimumBookDepth)
	if err != nil || constructorErr != nil || bookMessage.Depth != OptionMinimumBookDepth || bookMessage.UpdateID != 18521288 ||
		len(bookMessage.Bids) != 1 || bookMessage.Bids[0] != (PriceLevel{Price: "0.0005", Amount: "12.5"}) ||
		book.Apply(bookMessage) != nil || !book.Snapshot().Seeded {
		return nil, fmt.Errorf("%w: option bounded book semantic check", ErrFixtureBoundary)
	}
	addCheck("option_bounded_book_minimum_depth", bookFixture)

	tickerFixture, err := need("option_ticker_snapshot")
	if err != nil {
		return nil, err
	}
	ticker, err := ParseOptionTickerSnapshot(tickerFixture.payload)
	if err != nil || ticker.BaseCoin != "BTC" || ticker.Fields.Delta.State != normalize.SourceValue || ticker.Fields.Delta.Text != "-0.9876" ||
		ticker.Fields.Gamma.State != normalize.SourceValue || ticker.Fields.Gamma.Text != "0.0001" ||
		ticker.Fields.Vega.State != normalize.SourceValue || ticker.Fields.Vega.Text != "0.0123" ||
		ticker.Fields.Theta.State != normalize.SourceValue || ticker.Fields.Theta.Text != "-0.0045" ||
		ticker.Fields.Rho.State != normalize.SourceMissing {
		return nil, fmt.Errorf("%w: option ticker/Greek semantic check", ErrFixtureBoundary)
	}

	metadataFixture, err := need("option_metadata")
	if err != nil {
		return nil, err
	}
	page, err := ParseOptionInstrumentInfo(metadataFixture.payload)
	if err != nil || len(page.Instruments) != 1 || page.ObservedTimeMS != 1672304487000 ||
		page.Instruments[0].Kind() != normalize.OptionPut || page.Instruments[0].StrikePrice != "18000" ||
		page.Instruments[0].DeliveryTimeMS != 1672387200000 {
		return nil, fmt.Errorf("%w: option metadata semantic check", ErrFixtureBoundary)
	}
	metadata, err := optionFixtureMetadata(tickerFixture.payload)
	if err != nil {
		return nil, err
	}
	units := optionFixtureUnits(metadata.InstrumentUID)
	summary, err := ticker.Normalized(metadata, page.Instruments[0], page.ObservedTimeMS*int64(1e6), OptionIdentities{InstrumentUID: metadata.InstrumentUID, UnderlyingID: "BTC", IndexID: "BTC-USD"}, units)
	if err != nil || summary.Rho.State != normalize.SourceMissing || summary.Rho.Value != (normalize.NativeValue{}) || summary.OpenInterest.State != normalize.SourceValue || !summary.Delta.Provenance.AgeNS.Valid || !summary.Instrument.Provenance.AgeNS.Valid {
		return nil, fmt.Errorf("%w: OptionSummaryV1 semantic check", ErrFixtureBoundary)
	}
	addCheck("option_summary_snapshot_greeks_metadata_and_age", tickerFixture, metadataFixture)

	topicFixture, err := need("base_coin_topics")
	if err != nil {
		return nil, err
	}
	var topics struct {
		BaseCoin    string `json:"base_coin"`
		Instrument  string `json:"instrument"`
		TradeTopic  string `json:"trade_topic"`
		TickerTopic string `json:"ticker_topic"`
	}
	if json.Unmarshal(topicFixture.payload, &topics) != nil {
		return nil, fmt.Errorf("%w: base-coin topic fixture", ErrFixtureBoundary)
	}
	tradeTopic, tradeErr := (OptionTopicRequest{Role: RoleTrade, BaseCoin: topics.BaseCoin}).Topic()
	tickerTopic, tickerErr := (OptionTopicRequest{Role: RoleOptionTicker, BaseCoin: topics.BaseCoin}).Topic()
	if tradeErr != nil || tickerErr != nil || topics.BaseCoin != "BTC" || topics.Instrument != "BTC-30DEC22-18000-P" ||
		tradeTopic != topics.TradeTopic || tickerTopic != topics.TickerTopic {
		return nil, fmt.Errorf("%w: base-coin topic identity", ErrFixtureBoundary)
	}
	addCheck("option_base_coin_subscription_identity", topicFixture)

	depthFixture, err := need("minimum_depth")
	if err != nil {
		return nil, err
	}
	var depths struct {
		Instrument        string `json:"instrument"`
		MinimumDepth      int    `json:"minimum_depth"`
		SupportedDepths   []int  `json:"supported_depths"`
		UnsupportedDepths []int  `json:"unsupported_depths"`
	}
	if json.Unmarshal(depthFixture.payload, &depths) != nil || depths.Instrument != "BTC-30DEC22-18000-P" ||
		depths.MinimumDepth != OptionMinimumBookDepth || !slices.Equal(depths.SupportedDepths, []int{25, 100}) ||
		!slices.Equal(depths.UnsupportedDepths, []int{1, 50, 200, 1000}) {
		return nil, fmt.Errorf("%w: minimum option depth fixture", ErrFixtureBoundary)
	}
	for _, depth := range depths.SupportedDepths {
		if _, err := (OptionTopicRequest{Role: RoleBoundedOrderbook, Symbol: depths.Instrument, Depth: depth}).Topic(); err != nil {
			return nil, fmt.Errorf("%w: supported option depth %d", ErrFixtureBoundary, depth)
		}
	}
	for _, depth := range depths.UnsupportedDepths {
		if _, err := (OptionTopicRequest{Role: RoleBoundedOrderbook, Symbol: depths.Instrument, Depth: depth}).Topic(); err == nil {
			return nil, fmt.Errorf("%w: unsupported option depth %d", ErrFixtureBoundary, depth)
		}
	}
	addCheck("option_documented_minimum_depth", depthFixture)

	deltaFixture, err := need("snapshot_only_mutation")
	if err != nil {
		return nil, err
	}
	if _, err := ParseOptionTickerSnapshot(deltaFixture.payload); !errors.Is(err, ErrInvalidPayload) {
		return nil, fmt.Errorf("%w: option ticker accepted delta", ErrFixtureBoundary)
	}
	addCheck("option_ticker_snapshot_only", deltaFixture)

	driftFixture, err := need("greek_type_drift")
	if err != nil {
		return nil, err
	}
	if _, err := ParseOptionTickerSnapshot(driftFixture.payload); !errors.Is(err, ErrInvalidPayload) {
		return nil, fmt.Errorf("%w: option ticker accepted Greek type drift", ErrFixtureBoundary)
	}
	addCheck("option_greek_type_drift_rejected", driftFixture)

	unsupportedFixture, err := need("unsupported_roles")
	if err != nil {
		return nil, err
	}
	var unsupported struct {
		Claims []struct {
			Role    SourceRole `json:"role"`
			Support string     `json:"support"`
		} `json:"claims"`
	}
	if json.Unmarshal(unsupportedFixture.payload, &unsupported) != nil || len(unsupported.Claims) != 4 {
		return nil, fmt.Errorf("%w: unsupported option role fixture", ErrFixtureBoundary)
	}
	expectedRoles := []SourceRole{RoleAllLiquidation, RoleBBO, RoleFullOrderbook, RoleRPIOrderbook}
	observedRoles := make([]SourceRole, 0, len(unsupported.Claims))
	for _, claim := range unsupported.Claims {
		support, ok := OptionSupports(claim.Role)
		if claim.Support != "unsupported" || !ok || support.Support != capture.SupportUnsupported || support.Limitation == "" {
			return nil, fmt.Errorf("%w: unsupported option role %s", ErrFixtureBoundary, claim.Role)
		}
		observedRoles = append(observedRoles, claim.Role)
		request := OptionTopicRequest{Role: claim.Role, Symbol: "BTC-30DEC22-18000-P", Depth: 1}
		if _, err := request.Topic(); !errors.Is(err, ErrUnsupportedRole) {
			return nil, fmt.Errorf("%w: unsupported option topic %s", ErrFixtureBoundary, claim.Role)
		}
	}
	slices.Sort(observedRoles)
	slices.Sort(expectedRoles)
	if !slices.Equal(observedRoles, expectedRoles) {
		return nil, fmt.Errorf("%w: incomplete unsupported option roles", ErrFixtureBoundary)
	}
	addCheck("option_explicit_unsupported_l1_full_rpi_liquidation", unsupportedFixture)

	slices.SortFunc(checks, func(a, b EvidenceCheck) int { return strings.Compare(a.Name, b.Name) })
	return checks, nil
}

func optionFixtureMetadata(payload []byte) (normalize.Metadata, error) {
	const (
		sourceTimeNS   int64 = 1672304486868 * 1_000_000
		receivedTimeNS int64 = 1672304488000 * 1_000_000
	)
	epoch := [16]byte{1}
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: OptionSourceID, ChannelOrEndpoint: "tickers.BTC",
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true}, ArrivalOrdinal: 1,
		ExchangeTimeNS: capture.OptionalInt64{Value: sourceTimeNS, Valid: true}, ExchangeTimeResolution: capture.ExchangeTimeMillisecond,
		ReceivedWallTimeNS: receivedTimeNS, ClockEpochID: "bybit-option-fixture-verification", MonotonicNSSinceClockEpoch: 1,
		PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved, RecorderVersion: "bybit-option-fixture-verifier-v1",
	}
	envelope.SetRawPayload(payload)
	record, err := normalize.BindRawRecord(envelope, normalize.Hash(sha256.Sum256([]byte("bybit-v5-option-fixture-segment-v1"))), 1, nil)
	if err != nil {
		return normalize.Metadata{}, err
	}
	return normalize.NewMetadata(normalize.MetadataInput{
		Record: record, SchemaName: normalize.OptionSummarySchemaName, SchemaVersion: normalize.OptionSummarySchemaVersion,
		InstrumentUID:  "bybit-v5-option-btc-30dec22-18000-p",
		ExchangeTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, ExchangeTimeResolution: normalize.ResolutionMillisecond,
		SourceEventTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond,
		SourceSchemaFingerprint: normalize.Hash(sha256.Sum256([]byte("bybit-v5-option-ticker-schema-v1"))),
		MapperVersion:           "bybit-v5-option-ticker-fixture-verifier-v1",
		MapperBindingID:         normalize.Hash(sha256.Sum256([]byte("bybit-v5-option-ticker-fixture-binding-v1"))),
		CatalogSnapshotID:       normalize.Hash(sha256.Sum256([]byte("bybit-v5-option-ticker-fixture-catalog-v1"))),
	})
}

func optionFixtureUnits(instrumentUID string) OptionUnitContract {
	venueUnit := func(label string) normalize.NativeUnit {
		return normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: label}
	}
	return OptionUnitContract{
		PremiumPrice: normalize.SpotPriceUnit("BTC", "USD"), ReferencePrice: normalize.SpotPriceUnit("BTC", "USD"),
		OpenInterest: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrumentUID},
		Volume:       normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrumentUID},
		Delta:        venueUnit("bybit_option_delta"), Gamma: venueUnit("bybit_option_gamma"), Vega: venueUnit("bybit_option_vega"),
		Theta: venueUnit("bybit_option_theta"), Rho: venueUnit("bybit_option_rho"),
	}
}
