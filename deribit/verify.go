package deribit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/orderbook"
)

const (
	FixtureManifestVersion uint16 = 2
	MaxFixtureCount               = 32
	MaxManifestBytes              = 64 << 10
	MaxFixtureBytes               = 1 << 20
	MaxFixtureCorpusBytes         = 16 << 20
)

var ErrFixtureVerification = errors.New("deribit: fixture verification failed")

type FixtureManifest struct {
	Version    uint16         `json:"version"`
	Venue      string         `json:"venue"`
	AccessDate string         `json:"access_date"`
	Provenance string         `json:"provenance"`
	Fixtures   []FixtureEntry `json:"fixtures"`
}

type FixtureEntry struct {
	Role           string `json:"role"`
	Path           string `json:"path"`
	SHA256         string `json:"sha256"`
	Classification string `json:"classification"`
	OfficialURL    string `json:"official_url"`
	Section        string `json:"section"`
	DerivedFrom    string `json:"derived_from"`
}

type FixtureClassificationCount struct {
	Classification string `json:"classification"`
	Count          uint32 `json:"count"`
}

type FixtureProvenanceEvidence struct {
	Role           string `json:"role"`
	Classification string `json:"classification"`
	OfficialURL    string `json:"official_url"`
	Section        string `json:"section"`
	DerivedFrom    string `json:"derived_from"`
}

type EvidenceSummary struct {
	Version                 uint16                       `json:"version"`
	Venue                   string                       `json:"venue"`
	AccessDate              string                       `json:"access_date"`
	ManifestSHA256          string                       `json:"manifest_sha256"`
	FixtureCount            uint32                       `json:"fixture_count"`
	Roles                   []string                     `json:"roles"`
	ClassificationCounts    []FixtureClassificationCount `json:"classification_counts"`
	FixtureProvenance       []FixtureProvenanceEvidence  `json:"fixture_provenance"`
	SubscribeMissing        []string                     `json:"subscribe_missing"`
	HeartbeatResponseMethod string                       `json:"heartbeat_response_method"`
	CreditExhaustionAction  string                       `json:"credit_exhaustion_action"`
	ContinuityRecovery      string                       `json:"continuity_recovery"`
	ContinuityAuthority     string                       `json:"continuity_authority"`
	UnitInferenceVersion    string                       `json:"unit_inference_version"`
	InverseUnit             string                       `json:"inverse_unit"`
	LinearUnit              string                       `json:"linear_unit"`
	OptionUnit              string                       `json:"option_unit"`
	ProvisionalUnitState    string                       `json:"provisional_unit_state"`
	LiquidationCompleteness string                       `json:"liquidation_completeness"`
	LiquidationSourceRole   string                       `json:"liquidation_source_role"`
}

type verifiedFixture struct {
	entry  FixtureEntry
	raw    []byte
	digest normalize.Hash
}

func VerifyFixtures(manifestOrRoot string) (EvidenceSummary, error) {
	root, manifestPath, err := fixtureRootAndManifest(manifestOrRoot)
	if err != nil {
		return EvidenceSummary{}, err
	}
	manifestRaw, err := readContainedRegular(root, manifestPath, MaxManifestBytes)
	if err != nil {
		return EvidenceSummary{}, err
	}
	var manifest FixtureManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestRaw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil || manifest.Version != FixtureManifestVersion || manifest.Venue != "deribit" ||
		manifest.AccessDate != "2026-08-22" || manifest.Provenance != "synthetic" || len(manifest.Fixtures) == 0 || len(manifest.Fixtures) > MaxFixtureCount {
		return EvidenceSummary{}, ErrFixtureVerification
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return EvidenceSummary{}, err
	}
	fixtures := make(map[string]verifiedFixture, len(manifest.Fixtures))
	var corpusBytes int64
	for _, entry := range manifest.Fixtures {
		if entry.Role == "" || entry.Path == "" || len(entry.Role) > 128 || strings.IndexByte(entry.Role, 0) >= 0 ||
			filepath.IsAbs(entry.Path) || filepath.Clean(entry.Path) != entry.Path || entry.Path == "." || strings.HasPrefix(entry.Path, ".."+string(filepath.Separator)) {
			return EvidenceSummary{}, ErrFixtureVerification
		}
		officialURL, section, ok := expectedFixtureProvenance(entry.Role)
		if !ok || entry.Classification != normalize.DeribitFixtureClassificationSynthetic ||
			entry.OfficialURL != officialURL || entry.Section != section ||
			entry.DerivedFrom != officialURL+"#"+section {
			return EvidenceSummary{}, ErrFixtureVerification
		}
		if _, duplicate := fixtures[entry.Role]; duplicate {
			return EvidenceSummary{}, ErrFixtureVerification
		}
		expected, err := decodeFixtureHash(entry.SHA256)
		if err != nil {
			return EvidenceSummary{}, err
		}
		raw, err := readContainedRegular(root, filepath.Join(root, entry.Path), MaxFixtureBytes)
		if err != nil {
			return EvidenceSummary{}, err
		}
		corpusBytes += int64(len(raw))
		if corpusBytes > MaxFixtureCorpusBytes {
			return EvidenceSummary{}, ErrFixtureVerification
		}
		digest := normalize.Hash(sha256.Sum256(raw))
		if digest != expected {
			return EvidenceSummary{}, ErrFixtureVerification
		}
		fixtures[entry.Role] = verifiedFixture{entry: entry, raw: raw, digest: digest}
	}
	return verifyFixtureSemantics(manifest, manifestRaw, fixtures)
}

func verifyFixtureSemantics(manifest FixtureManifest, manifestRaw []byte, fixtures map[string]verifiedFixture) (EvidenceSummary, error) {
	requiredRoles := []string{
		"book_gap", "book_recovery", "book_snapshot", "credit_exhausted", "funding", "heartbeat_test_request", "index",
		"instrument_inverse", "instrument_linear", "instrument_option", "lifecycle_state", "quote", "subscribe_partial",
		"ticker_linear", "ticker_option", "trade_liquidation",
	}
	roles := make([]string, 0, len(fixtures))
	for role := range fixtures {
		roles = append(roles, role)
	}
	slices.Sort(roles)
	if !slices.Equal(roles, requiredRoles) {
		return EvidenceSummary{}, ErrFixtureVerification
	}
	policy := CadencePolicy{Requested: Cadence100MS}
	session, err := NewSession(policy, []ChannelRequest{
		{Role: RoleBook, Instrument: "BTC-PERPETUAL"},
		{Role: RoleTrade, Instrument: "BTC-PERPETUAL"},
	})
	if err != nil {
		return EvidenceSummary{}, err
	}
	if _, err := session.SubscribeRequest(7); err != nil {
		return EvidenceSummary{}, err
	}
	subscribeDecision, err := session.Inspect(fixtures["subscribe_partial"].raw, 0)
	if !errors.Is(err, ErrSubscribeMismatch) || subscribeDecision.Reconciliation == nil || subscribeDecision.Reconciliation.Exact ||
		len(subscribeDecision.Reconciliation.Missing) != 1 {
		return EvidenceSummary{}, ErrFixtureVerification
	}
	heartbeatDecision, err := session.Inspect(fixtures["heartbeat_test_request"].raw, 9)
	if err != nil || heartbeatDecision.Action != SessionRespondTest || !heartbeatDecision.ReuseConnection {
		return EvidenceSummary{}, ErrFixtureVerification
	}
	var heartbeatResponse struct {
		Method string `json:"method"`
	}
	if err := decodeRPC(heartbeatDecision.Response, &heartbeatResponse); err != nil || heartbeatResponse.Method != "public/test" {
		return EvidenceSummary{}, ErrFixtureVerification
	}
	creditDecision, err := session.Inspect(fixtures["credit_exhausted"].raw, 0)
	if !errors.Is(err, ErrReconnectRequired) || creditDecision.Action != SessionReconnectAfterCredit || creditDecision.ReuseConnection {
		return EvidenceSummary{}, ErrFixtureVerification
	}

	inverse, err := ParseInstrument(fixtures["instrument_inverse"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	linear, err := ParseInstrument(fixtures["instrument_linear"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	option, err := ParseInstrument(fixtures["instrument_option"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	inverseTerms, err := inverse.Terms(fixtureInstrumentEvidence("deribit:BTC-PERPETUAL:1", fixtures["instrument_inverse"]))
	if err != nil {
		return EvidenceSummary{}, err
	}
	linearTerms, err := linear.Terms(fixtureInstrumentEvidence("deribit:BTC_USDC-PERPETUAL:1", fixtures["instrument_linear"]))
	if err != nil {
		return EvidenceSummary{}, err
	}
	optionTerms, err := option.Terms(fixtureInstrumentEvidence("deribit:BTC-30DEC27-50000-C:1", fixtures["instrument_option"]))
	if err != nil {
		return EvidenceSummary{}, err
	}
	inverseUnit, err := normalize.InferDeribitAmountUnit(inverseTerms, DocumentationAccessedAtNS+1)
	if err != nil {
		return EvidenceSummary{}, err
	}
	linearUnit, err := normalize.InferDeribitAmountUnit(linearTerms, DocumentationAccessedAtNS+1)
	if err != nil {
		return EvidenceSummary{}, err
	}
	optionUnit, err := normalize.InferDeribitAmountUnit(optionTerms, DocumentationAccessedAtNS+1)
	if err != nil {
		return EvidenceSummary{}, err
	}
	provisionalTerms := inverseTerms
	provisionalTerms.CatalogGeneration = 0
	provisionalTerms.MetadataRawSHA256 = normalize.Hash{}
	provisional, err := normalize.InferDeribitAmountUnit(provisionalTerms, DocumentationAccessedAtNS+1)
	if err != nil || provisional.State != normalize.DeribitInferenceProvisional {
		return EvidenceSummary{}, ErrFixtureVerification
	}

	book, err := orderbook.NewDeribitBook(inverseTerms.InstrumentUID)
	if err != nil {
		return EvidenceSummary{}, err
	}
	for _, role := range []string{"book_snapshot", "book_gap", "book_recovery"} {
		native, err := ParseBook(fixtures[role].raw)
		if err != nil {
			return EvidenceSummary{}, err
		}
		update, err := native.Normalized(inverseTerms)
		if err != nil {
			return EvidenceSummary{}, err
		}
		transition, applyErr := book.Apply(update)
		switch role {
		case "book_snapshot":
			if applyErr != nil || !transition.Applied {
				return EvidenceSummary{}, ErrFixtureVerification
			}
		case "book_gap":
			if !errors.Is(applyErr, orderbook.ErrDeribitChangeIDGap) || transition.Recovery != orderbook.DeribitRecoveryResubscribe {
				return EvidenceSummary{}, ErrFixtureVerification
			}
		case "book_recovery":
			if applyErr != nil || transition.Recovery != orderbook.DeribitRecoverySnapshot || transition.Authority != capture.RuleAdapterPolicyInference || transition.SourceGuarantee {
				return EvidenceSummary{}, ErrFixtureVerification
			}
		}
	}
	trades, err := ParseTrades(fixtures["trade_liquidation"].raw)
	if err != nil || len(trades) != 1 {
		return EvidenceSummary{}, ErrFixtureVerification
	}
	trade, err := trades[0].Normalized(inverseTerms)
	if err != nil || trade.Liquidation == nil || trade.Liquidation.Completeness != normalize.LiquidationTradeFlagOnly || trade.Liquidation.PublicChannel {
		return EvidenceSummary{}, ErrFixtureVerification
	}
	quote, err := ParseQuote(fixtures["quote"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	if _, err := quote.Normalized(inverseTerms); err != nil {
		return EvidenceSummary{}, err
	}
	linearTicker, err := ParseTicker(fixtures["ticker_linear"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	if _, err := linearTicker.Normalized(linearTerms); err != nil {
		return EvidenceSummary{}, err
	}
	optionTicker, err := ParseTicker(fixtures["ticker_option"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	if _, err := optionTicker.NormalizedOption(option, optionTerms); err != nil {
		return EvidenceSummary{}, err
	}
	funding, err := ParseFunding(fixtures["funding"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	if _, err := funding.Normalized(inverseTerms.InstrumentUID); err != nil {
		return EvidenceSummary{}, err
	}
	index, err := ParseIndex(fixtures["index"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	if _, _, err := index.Normalized("BTC", "USD"); err != nil {
		return EvidenceSummary{}, err
	}
	lifecycle, err := ParseLifecycle(fixtures["lifecycle_state"].raw)
	if err != nil {
		return EvidenceSummary{}, err
	}
	if _, err := lifecycle.Normalized(inverseTerms.InstrumentUID); err != nil {
		return EvidenceSummary{}, err
	}
	fixtureProvenance := make([]FixtureProvenanceEvidence, 0, len(roles))
	for _, role := range roles {
		entry := fixtures[role].entry
		fixtureProvenance = append(fixtureProvenance, FixtureProvenanceEvidence{
			Role: role, Classification: entry.Classification, OfficialURL: entry.OfficialURL,
			Section: entry.Section, DerivedFrom: entry.DerivedFrom,
		})
	}
	manifestDigest := sha256.Sum256(manifestRaw)
	return EvidenceSummary{
		Version: FixtureManifestVersion, Venue: manifest.Venue, AccessDate: manifest.AccessDate,
		ManifestSHA256: hex.EncodeToString(manifestDigest[:]), FixtureCount: uint32(len(fixtures)), Roles: roles,
		ClassificationCounts: []FixtureClassificationCount{{
			Classification: normalize.DeribitFixtureClassificationSynthetic, Count: uint32(len(fixtures)),
		}},
		FixtureProvenance: fixtureProvenance,
		SubscribeMissing:  slices.Clone(subscribeDecision.Reconciliation.Missing), HeartbeatResponseMethod: "public/test",
		CreditExhaustionAction: string(creditDecision.Action), ContinuityRecovery: string(orderbook.DeribitRecoverySnapshot),
		ContinuityAuthority: "adapter_policy_inference", UnitInferenceVersion: normalize.DeribitUnitInferenceVersion,
		InverseUnit: string(inverseUnit.Unit.Kind), LinearUnit: string(linearUnit.Unit.Kind), OptionUnit: string(optionUnit.Unit.Kind),
		ProvisionalUnitState: string(provisional.State), LiquidationCompleteness: string(trade.Liquidation.Completeness),
		LiquidationSourceRole: trade.Liquidation.NativeSourceRole,
	}, nil
}

func fixtureInstrumentEvidence(uid string, fixture verifiedFixture) InstrumentEvidence {
	return InstrumentEvidence{
		InstrumentUID: uid, CatalogGeneration: 1, MetadataRawSHA256: fixture.digest,
		ValidFromNS:           DocumentationAccessedAtNS,
		FixtureClassification: fixture.entry.Classification, OfficialURL: fixture.entry.OfficialURL,
		Section: fixture.entry.Section, DerivedFrom: fixture.entry.DerivedFrom,
	}
}

func expectedFixtureProvenance(role string) (string, string, bool) {
	switch role {
	case "subscribe_partial":
		return SubscribeDocumentationURI, "response.result.successfully_subscribed_channels", true
	case "heartbeat_test_request":
		return HeartbeatDocumentationURI, "notifications.test_request", true
	case "credit_exhausted":
		return RateDocumentationURI, "credit-based-system.code-10028", true
	case "instrument_inverse", "instrument_linear", "instrument_option":
		return normalize.DeribitInstrumentProvenanceURL, normalize.DeribitInstrumentProvenanceSection, true
	case "book_snapshot", "book_gap", "book_recovery":
		return BookDocumentationURI, "subscription.params.data.snapshot-change-continuity", true
	case "trade_liquidation":
		return TradeDocumentationURI, "subscription.params.data.liquidation", true
	case "quote":
		return CollectionDocumentationURI, "quote", true
	case "ticker_linear", "ticker_option":
		return CollectionDocumentationURI, "ticker", true
	case "funding":
		return CollectionDocumentationURI, "funding", true
	case "index":
		return CollectionDocumentationURI, "index", true
	case "lifecycle_state":
		return InstrumentDocumentationURI, "components.schemas.instrument.properties.state", true
	default:
		return "", "", false
	}
}

func fixtureRootAndManifest(input string) (string, string, error) {
	if input == "" || strings.IndexByte(input, 0) >= 0 {
		return "", "", ErrFixtureVerification
	}
	absolute, err := filepath.Abs(input)
	if err != nil {
		return "", "", ErrFixtureVerification
	}
	info, err := os.Lstat(absolute)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return "", "", ErrFixtureVerification
	}
	root := absolute
	manifest := filepath.Join(absolute, "manifest.json")
	if !info.IsDir() {
		if !info.Mode().IsRegular() || filepath.Base(absolute) != "manifest.json" {
			return "", "", ErrFixtureVerification
		}
		root, manifest = filepath.Dir(absolute), absolute
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", ErrFixtureVerification
	}
	rootInfo, err := os.Stat(resolvedRoot)
	if err != nil || !rootInfo.IsDir() {
		return "", "", ErrFixtureVerification
	}
	return resolvedRoot, manifest, nil
}

func readContainedRegular(root, path string, maximum int64) ([]byte, error) {
	absolute, err := filepath.Abs(path)
	if err != nil || !pathWithinRoot(root, absolute) || containsSymlink(root, absolute) {
		return nil, ErrFixtureVerification
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || !pathWithinRoot(root, resolved) {
		return nil, ErrFixtureVerification
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, ErrFixtureVerification
	}
	file, err := os.Open(resolved)
	if err != nil {
		return nil, ErrFixtureVerification
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) > maximum || int64(len(raw)) != info.Size() {
		return nil, ErrFixtureVerification
	}
	return raw, nil
}

func pathWithinRoot(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func containsSymlink(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return true
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

func decodeFixtureHash(value string) (normalize.Hash, error) {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return normalize.Hash{}, ErrFixtureVerification
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return normalize.Hash{}, ErrFixtureVerification
	}
	var digest normalize.Hash
	copy(digest[:], decoded)
	return digest, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing manifest data", ErrFixtureVerification)
	}
	return nil
}
