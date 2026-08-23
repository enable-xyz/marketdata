package hyperliquid

import (
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
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	FixtureManifestVersion = 1
	EvidenceVersion        = 1
	maximumFixtureCount    = 64
	maximumFixtureBytes    = MaxRawPayloadBytes
)

type FixtureManifest struct {
	Version      uint16         `json:"version"`
	Venue        string         `json:"venue"`
	AccessDate   string         `json:"access_date"`
	FixtureClaim string         `json:"fixture_claim"`
	Fixtures     []FixtureEntry `json:"fixtures"`
}

type FixtureEntry struct {
	ID             string `json:"id"`
	File           string `json:"file"`
	Role           string `json:"role"`
	Classification string `json:"classification"`
	SourceURL      string `json:"source_url"`
	SourceSection  string `json:"source_section"`
	DerivedFrom    string `json:"derived_from"`
	ByteLength     uint32 `json:"byte_length"`
	SHA256         string `json:"sha256"`
}

type EvidenceCheck struct {
	Name       string   `json:"name"`
	FixtureIDs []string `json:"fixture_ids"`
}

type EvidenceSummary struct {
	Version        uint16          `json:"version"`
	Venue          string          `json:"venue"`
	AccessDate     string          `json:"access_date"`
	ManifestSHA256 string          `json:"manifest_sha256"`
	FixtureCount   uint32          `json:"fixture_count"`
	SyntheticCount uint32          `json:"synthetic_count"`
	Checks         []EvidenceCheck `json:"checks"`
	EvidenceSHA256 string          `json:"evidence_sha256"`
}

type fixtureRoleContract struct {
	ID             string
	Classification string
	SourceURL      string
	SourceSection  string
	DerivedFrom    string
}

var fixtureRoleContracts = map[string]fixtureRoleContract{
	"perp_dexs":                {ID: "perp-dexs", Classification: "synthetic_documented_projection", SourceURL: PerpetualDocumentationURI, SourceSection: "Retrieve all perpetual dexs", DerivedFrom: "Hand-authored values matching the documented response shape"},
	"main_meta_contexts":       {ID: "main-meta-contexts", Classification: "synthetic_documented_projection", SourceURL: PerpetualDocumentationURI, SourceSection: "Retrieve perpetuals asset contexts", DerivedFrom: "Hand-authored main DEX metadata and positional contexts"},
	"hip3_meta_contexts":       {ID: "hip3-meta-contexts", Classification: "synthetic_documented_projection", SourceURL: PerpetualDocumentationURI, SourceSection: "HIP-3 metaAndAssetCtxs response", DerivedFrom: "Hand-authored HIP-3 generation and context values"},
	"hip3_positional_mismatch": {ID: "hip3-positional-mismatch", Classification: "synthetic_fault_projection", SourceURL: PerpetualDocumentationURI, SourceSection: "metaAndAssetCtxs positional arrays", DerivedFrom: "Synthetic context truncation used only to prove fail-closed association"},
	"spot_meta_contexts":       {ID: "spot-meta-contexts", Classification: "synthetic_documented_projection", SourceURL: SpotDocumentationURI, SourceSection: "Retrieve spot asset contexts", DerivedFrom: "Hand-authored spot tokens, pair metadata, and positional context"},
	"duplicate_trades":         {ID: "duplicate-trades", Classification: "synthetic_fault_projection", SourceURL: SubscriptionDocumentationURI, SourceSection: "WsTrade and globally unique key guidance", DerivedFrom: "Synthetic exact duplicate native trade key retained twice"},
	"slow_book_initial":        {ID: "slow-book-initial", Classification: "synthetic_documented_projection", SourceURL: SubscriptionDocumentationURI, SourceSection: "l2Book slow 20-level snapshot", DerivedFrom: "Hand-authored full slow snapshot"},
	"slow_book_replacement":    {ID: "slow-book-replacement", Classification: "synthetic_fault_projection", SourceURL: SubscriptionDocumentationURI, SourceSection: "WsBook snapshot", DerivedFrom: "Synthetic second full snapshot with disjoint prior level"},
	"fast_book":                {ID: "fast-book", Classification: "synthetic_documented_projection", SourceURL: SubscriptionDocumentationURI, SourceSection: "l2Book fast 5-level snapshot", DerivedFrom: "Hand-authored full fast snapshot at its five-level bound"},
	"bbo":                      {ID: "bbo", Classification: "synthetic_documented_projection", SourceURL: SubscriptionDocumentationURI, SourceSection: "WsBbo", DerivedFrom: "Hand-authored block-change BBO observation"},
	"active_asset_context":     {ID: "active-asset-context", Classification: "synthetic_documented_projection", SourceURL: SubscriptionDocumentationURI, SourceSection: "WsActiveAssetCtx", DerivedFrom: "Hand-authored HIP-3 active asset context"},
	"funding_history":          {ID: "funding-history", Classification: "synthetic_documented_projection", SourceURL: PerpetualDocumentationURI, SourceSection: "Retrieve historical funding rates", DerivedFrom: "Hand-authored HIP-3 funding rate observations"},
}

type verifiedFixture struct {
	entry   FixtureEntry
	payload []byte
}

// VerifyFixtures reads one explicit manifest or its exact containing root. It
// performs no network access and rejects symlinks, traversal, undeclared files,
// digest mismatch, incomplete roles, and non-synthetic fixture claims.
func VerifyFixtures(manifestPathOrRoot string) (EvidenceSummary, error) {
	manifestPath, root, err := resolveManifest(manifestPathOrRoot)
	if err != nil {
		return EvidenceSummary{}, err
	}
	canonicalRoot, err := canonicalFixturePath(root)
	if err != nil {
		return EvidenceSummary{}, err
	}
	if err := ensureFixtureContained(canonicalRoot, manifestPath); err != nil {
		return EvidenceSummary{}, err
	}
	manifestBytes, err := readRegularBounded(manifestPath, maximumFixtureBytes)
	if err != nil {
		return EvidenceSummary{}, err
	}
	var manifest FixtureManifest
	if json.Unmarshal(manifestBytes, &manifest) != nil || manifest.Version != FixtureManifestVersion || manifest.Venue != "hyperliquid" ||
		manifest.AccessDate != DocumentationAccessDate || manifest.FixtureClaim == "" || len(manifest.Fixtures) == 0 || len(manifest.Fixtures) > maximumFixtureCount {
		return EvidenceSummary{}, fmt.Errorf("%w: invalid manifest", ErrFixtureBoundary)
	}
	fixtures := make(map[string]verifiedFixture, len(manifest.Fixtures))
	ids := make(map[string]struct{}, len(manifest.Fixtures))
	declaredFiles := map[string]struct{}{"manifest.json": {}}
	declaredDirectories := map[string]struct{}{".": {}}
	for _, entry := range manifest.Fixtures {
		role, ok := fixtureRoleContracts[entry.Role]
		if !ok || entry.ID != role.ID || entry.Classification != role.Classification || !strings.HasPrefix(entry.Classification, "synthetic_") ||
			entry.SourceURL != role.SourceURL || entry.SourceSection != role.SourceSection || entry.DerivedFrom != role.DerivedFrom ||
			entry.File == "" || entry.ByteLength == 0 || len(entry.SHA256) != sha256.Size*2 {
			return EvidenceSummary{}, fmt.Errorf("%w: fixture role identity mismatch for %s", ErrFixtureBoundary, entry.Role)
		}
		if _, duplicate := ids[entry.ID]; duplicate {
			return EvidenceSummary{}, fmt.Errorf("%w: duplicate fixture ID", ErrFixtureBoundary)
		}
		if _, duplicate := fixtures[entry.Role]; duplicate {
			return EvidenceSummary{}, fmt.Errorf("%w: duplicate fixture role", ErrFixtureBoundary)
		}
		ids[entry.ID] = struct{}{}
		clean := filepath.Clean(entry.File)
		if clean != entry.File || filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return EvidenceSummary{}, fmt.Errorf("%w: fixture path traversal", ErrFixtureBoundary)
		}
		if _, duplicate := declaredFiles[clean]; duplicate {
			return EvidenceSummary{}, fmt.Errorf("%w: duplicate fixture file", ErrFixtureBoundary)
		}
		declaredFiles[clean] = struct{}{}
		for parent := filepath.Dir(clean); parent != "."; parent = filepath.Dir(parent) {
			declaredDirectories[parent] = struct{}{}
		}
		path := filepath.Join(root, clean)
		if err := ensureFixtureContained(canonicalRoot, path); err != nil {
			return EvidenceSummary{}, err
		}
		payload, err := readRegularBounded(path, maximumFixtureBytes)
		if err != nil {
			return EvidenceSummary{}, err
		}
		digest := sha256.Sum256(payload)
		if len(payload) != int(entry.ByteLength) || hex.EncodeToString(digest[:]) != entry.SHA256 {
			return EvidenceSummary{}, fmt.Errorf("%w: immutable fixture mismatch for %s", ErrFixtureBoundary, entry.ID)
		}
		fixtures[entry.Role] = verifiedFixture{entry: entry, payload: payload}
	}
	if err := verifyFixtureTree(canonicalRoot, declaredFiles, declaredDirectories); err != nil {
		return EvidenceSummary{}, err
	}
	if len(fixtures) != len(fixtureRoleContracts) {
		return EvidenceSummary{}, fmt.Errorf("%w: incomplete fixture role set", ErrFixtureBoundary)
	}
	checks, err := verifyFixtureContracts(fixtures)
	if err != nil {
		return EvidenceSummary{}, err
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	summary := EvidenceSummary{
		Version: EvidenceVersion, Venue: manifest.Venue, AccessDate: manifest.AccessDate,
		ManifestSHA256: hex.EncodeToString(manifestDigest[:]), FixtureCount: uint32(len(manifest.Fixtures)),
		SyntheticCount: uint32(len(manifest.Fixtures)), Checks: checks,
	}
	material, err := json.Marshal(summary)
	if err != nil {
		return EvidenceSummary{}, err
	}
	evidenceDigest := sha256.Sum256(material)
	summary.EvidenceSHA256 = hex.EncodeToString(evidenceDigest[:])
	return summary, nil
}

func verifyFixtureContracts(fixtures map[string]verifiedFixture) ([]EvidenceCheck, error) {
	dexs, err := ParsePerpDEXs(fixtures["perp_dexs"].payload)
	if err != nil || len(dexs) != 3 || dexs[0].Family() != MainPerpetual || dexs[1].Family() != HIP3 || dexs[1].Name != "xyz" {
		return nil, fixtureVerificationError("perp DEX metadata")
	}
	mainGeneration := fixtureGenerationEvidence(fixtures["main_meta_contexts"].payload, "main-meta-contexts", 1)
	mainMeta, err := ParsePerpMetadataAndContexts(Mainnet, dexs[0], mainGeneration, fixtures["main_meta_contexts"].payload)
	if err != nil || len(mainMeta.Universe) != 2 || len(mainMeta.Contexts) != 2 || mainMeta.Contexts[0].OpenInterest.Text != "688.11" {
		return nil, fixtureVerificationError("main metadata and contexts")
	}
	hip3Generation := fixtureGenerationEvidence(fixtures["hip3_meta_contexts"].payload, "hip3-meta-contexts", 2)
	hip3Meta, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], hip3Generation, fixtures["hip3_meta_contexts"].payload)
	if err != nil || len(hip3Meta.Universe) != 2 || hip3Meta.Universe[0].Identity.Family != catalog.HyperliquidHIP3 || hip3Meta.Contexts[0].OpenInterest.Text != "0.0854" {
		return nil, fixtureVerificationError("HIP-3 metadata and contexts")
	}
	spotGeneration := fixtureGenerationEvidence(fixtures["spot_meta_contexts"].payload, "spot-meta-contexts", 3)
	spotMeta, err := ParseSpotMetadataAndContexts(Mainnet, spotGeneration, fixtures["spot_meta_contexts"].payload)
	if err != nil || len(spotMeta.Universe) != 1 || spotMeta.Universe[0].BaseToken.Name != "PURR" || spotMeta.Contexts[0].MidPrice.Text != "0.209265" {
		return nil, fixtureVerificationError("spot metadata and contexts")
	}
	mismatchGeneration := fixtureGenerationEvidence(fixtures["hip3_positional_mismatch"].payload, "hip3-positional-mismatch", 4)
	if _, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], mismatchGeneration, fixtures["hip3_positional_mismatch"].payload); !errors.Is(err, ErrPositionalMismatch) {
		return nil, fixtureVerificationError("positional mismatch did not fail closed")
	}
	mainBTC := mainMeta.Universe[0].Identity
	hip3BTC := hip3Meta.Universe[0].Identity
	abcBTC, err := catalog.NewHyperliquidInstrumentIdentity(catalog.HyperliquidIdentityInput{
		Network: catalog.HyperliquidNetworkMainnet, Family: catalog.HyperliquidHIP3, DEXName: dexs[2].Name,
		WireCoin: "abc:BTC", MetadataGeneration: hip3BTC.MetadataGeneration, Deployer: dexs[2].Deployer,
		CollateralToken: hip3BTC.CollateralToken, UniverseIndex: hip3BTC.UniverseIndex,
	})
	if err != nil {
		return nil, fixtureVerificationError("second HIP-3 namespace")
	}
	recycledGeneration := fixtureGenerationEvidence(fixtures["hip3_meta_contexts"].payload, "hip3-recycled-generation", 5)
	recycledMeta, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], recycledGeneration, fixtures["hip3_meta_contexts"].payload)
	if err != nil || len(recycledMeta.Universe) == 0 {
		return nil, fixtureVerificationError("recycled generation parse")
	}
	recycledBTC := recycledMeta.Universe[0].Identity
	if mainBTC.InstrumentUID == hip3BTC.InstrumentUID || hip3BTC.InstrumentUID == abcBTC.InstrumentUID || hip3BTC.InstrumentUID == recycledBTC.InstrumentUID {
		return nil, fixtureVerificationError("namespace or generation collision")
	}
	trades, err := ParseTrades(fixtures["duplicate_trades"].payload)
	if err != nil || len(trades) != 2 || trades[0].Key() != trades[1].Key() || trades[0].MessageOrdinal == trades[1].MessageOrdinal ||
		trades[0].NativeDuplicatePolicy != DuplicatePolicyPreserveUnassessed || !slices.Equal(trades[0].Evidence.Bytes(), fixtures["duplicate_trades"].payload) {
		return nil, fixtureVerificationError("duplicate trade preservation or source fidelity")
	}
	slowDepth := BookDepthContract{}
	slowSubscription := Subscription{Type: SubscriptionL2Book, Coin: hip3BTC.WireCoin, Book: slowDepth}
	slowInitialEnvelope, err := newReceiveEnvelope(fixtures["slow_book_initial"].payload, 1, HIP3, dexs[1].Name, slowSubscription, true)
	if err != nil {
		return nil, fixtureVerificationError("slow book capture envelope")
	}
	slowInitial, err := ParseBookSnapshot(slowInitialEnvelope)
	if err != nil || len(slowInitial.Bids) != 2 || slowInitial.Depth.MaximumLevels() != MaximumLevelsSlow {
		return nil, fixtureVerificationError("slow book depth contract")
	}
	slowReplacementEnvelope, err := newReceiveEnvelope(fixtures["slow_book_replacement"].payload, 2, HIP3, dexs[1].Name, slowSubscription, true)
	if err != nil {
		return nil, fixtureVerificationError("slow replacement envelope")
	}
	slowReplacement, err := ParseBookSnapshot(slowReplacementEnvelope)
	if err != nil {
		return nil, fixtureVerificationError("slow replacement snapshot")
	}
	fastDepth := BookDepthContract{Fast: true}
	fastSubscription := Subscription{Type: SubscriptionL2Book, Coin: hip3BTC.WireCoin, Book: fastDepth}
	fastEnvelope, err := newReceiveEnvelope(fixtures["fast_book"].payload, 3, HIP3, dexs[1].Name, fastSubscription, true)
	if err != nil {
		return nil, fixtureVerificationError("fast book capture envelope")
	}
	fast, err := ParseBookSnapshot(fastEnvelope)
	if err != nil || len(fast.Bids) != MaximumLevelsFast || len(fast.Asks) != MaximumLevelsFast || fast.Depth.MaximumLevels() != MaximumLevelsFast {
		return nil, fixtureVerificationError("fast book depth contract")
	}
	book, err := NewBook(hip3BTC, slowDepth)
	if err != nil {
		return nil, fixtureVerificationError("book construction")
	}
	first, err := book.Apply(slowInitial)
	if err != nil || first.ReplacedPrior || first.Gap.SequenceDetectable || first.Gap.DeltaClaim {
		return nil, fixtureVerificationError("first full snapshot")
	}
	second, err := book.Apply(slowReplacement)
	view := book.Snapshot()
	if err != nil || !second.ReplacedPrior || second.Gap.State != "uncertain" || second.Gap.Reason != BookContinuityNoSequence || second.Gap.SequenceDetectable || second.Gap.DeltaClaim ||
		len(view.Bids) != 1 || view.Bids[0].Price != "113005.0" || view.ReplacementCount != 2 {
		return nil, fixtureVerificationError("full replacement and no-sequence uncertainty")
	}
	quote, err := ParseBBO(fixtures["bbo"].payload)
	if err != nil || quote.Bid == nil || quote.Ask == nil || quote.Bid.Price != "113004.0" || quote.Ask.Price != "113005.0" {
		return nil, fixtureVerificationError("BBO source fidelity")
	}
	active, err := ParseActiveAssetContext(hip3BTC, fixtures["active_asset_context"].payload)
	if err != nil || active.Perp == nil || active.Spot != nil || active.Perp.MarkPrice.Text != "113010.0" {
		return nil, fixtureVerificationError("active asset context")
	}
	funding, err := ParseFundingHistory(fixtures["funding_history"].payload)
	if err != nil || len(funding) != 2 || funding[0].FundingRate != "-0.00022196" || funding[1].TimeMS != 1787360400068 {
		return nil, fixtureVerificationError("funding history")
	}
	provenance := normalize.FieldProvenance{
		SourceTimeNS: normalize.OptionalInt64{Value: DocumentationAccessTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond,
		AgeNS: normalize.OptionalUint64{Value: 0, Valid: true},
	}
	resolved, err := mainMeta.Contexts[0].OpenInterestValue("BTC", provenance)
	if err != nil {
		return nil, fixtureVerificationError("resolved main unit")
	}
	provisional, err := hip3Meta.Contexts[0].OpenInterestValue("ignored-for-provisional", provenance)
	if err != nil || provisional.EligibleForStrictTotal() {
		return nil, fixtureVerificationError("HIP-3 provisional unit")
	}
	total, err := normalize.StrictHyperliquidTotal([]normalize.HyperliquidEconomicValue{resolved, provisional})
	if err != nil || !total.HasValue || total.Included != 1 || total.Excluded != 1 || total.Value.Decimal != resolved.Normalized.Value.Decimal {
		return nil, fixtureVerificationError("provisional exclusion from strict total")
	}
	importSupport, ok := Supports(HIP3, RoleNativeHistoryImport)
	if !ok || importSupport.Support != capture.SupportUnsupported {
		return nil, fixtureVerificationError("v1 native-history import assertion")
	}
	checks := []EvidenceCheck{
		{Name: "bbo_context_funding_source_fidelity", FixtureIDs: fixtureIDs(fixtures["bbo"], fixtures["active_asset_context"], fixtures["funding_history"])},
		{Name: "book_depth_full_replacement_no_sequence", FixtureIDs: fixtureIDs(fixtures["slow_book_initial"], fixtures["slow_book_replacement"], fixtures["fast_book"])},
		{Name: "duplicate_trade_key_preserved", FixtureIDs: fixtureIDs(fixtures["duplicate_trades"])},
		{Name: "metadata_namespace_generation_and_position", FixtureIDs: fixtureIDs(fixtures["perp_dexs"], fixtures["main_meta_contexts"], fixtures["hip3_meta_contexts"], fixtures["hip3_positional_mismatch"], fixtures["spot_meta_contexts"])},
		{Name: "no_import_and_provisional_exclusion", FixtureIDs: fixtureIDs(fixtures["main_meta_contexts"], fixtures["hip3_meta_contexts"])},
	}
	slices.SortFunc(checks, func(a, b EvidenceCheck) int { return strings.Compare(a.Name, b.Name) })
	return checks, nil
}

func fixtureIDs(fixtures ...verifiedFixture) []string {
	ids := make([]string, len(fixtures))
	for index, fixture := range fixtures {
		ids[index] = fixture.entry.ID
	}
	slices.Sort(ids)
	return ids
}

func resolveManifest(pathOrRoot string) (string, string, error) {
	if pathOrRoot == "" {
		return "", "", fmt.Errorf("%w: manifest path is required", ErrFixtureBoundary)
	}
	clean := filepath.Clean(pathOrRoot)
	info, err := os.Lstat(clean)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("%w: explicit path is unavailable or a symlink", ErrFixtureBoundary)
	}
	if info.IsDir() {
		return filepath.Join(clean, "manifest.json"), clean, nil
	}
	if !info.Mode().IsRegular() || filepath.Base(clean) != "manifest.json" {
		return "", "", fmt.Errorf("%w: expected manifest.json or its root", ErrFixtureBoundary)
	}
	return clean, filepath.Dir(clean), nil
}

func canonicalFixturePath(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("%w: canonical fixture path is unavailable", ErrFixtureBoundary)
	}
	absolute, err := filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("%w: canonical fixture path is invalid", ErrFixtureBoundary)
	}
	return filepath.Clean(absolute), nil
}

func ensureFixtureContained(canonicalRoot, path string) error {
	canonicalPath, err := canonicalFixturePath(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(canonicalRoot, canonicalPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: fixture escaped root", ErrFixtureBoundary)
	}
	return nil
}

func readRegularBounded(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maximum {
		return nil, fmt.Errorf("%w: non-regular or oversized file %s", ErrFixtureBoundary, filepath.Base(path))
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(payload)) > maximum {
		return nil, fmt.Errorf("%w: bounded read failed", ErrFixtureBoundary)
	}
	return slices.Clone(payload), nil
}

func verifyFixtureTree(root string, declaredFiles, declaredDirectories map[string]struct{}) error {
	seen := make(map[string]struct{}, len(declaredFiles))
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("%w: fixture tree cannot be enumerated", ErrFixtureBoundary)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%w: fixture tree escaped root", ErrFixtureBoundary)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink %s", ErrFixtureBoundary, relative)
		}
		if entry.IsDir() {
			if _, declared := declaredDirectories[relative]; !declared {
				return fmt.Errorf("%w: undeclared directory %s", ErrFixtureBoundary, relative)
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("%w: special file %s", ErrFixtureBoundary, relative)
		}
		if _, declared := declaredFiles[relative]; !declared {
			return fmt.Errorf("%w: undeclared file %s", ErrFixtureBoundary, relative)
		}
		seen[relative] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	if len(seen) != len(declaredFiles) {
		return fmt.Errorf("%w: declared fixture file missing", ErrFixtureBoundary)
	}
	return nil
}

func fixtureVerificationError(message string) error {
	return fmt.Errorf("%w: %s", ErrFixtureBoundary, message)
}

func fixtureGenerationEvidence(payload []byte, epoch string, ordinal uint64) catalog.HyperliquidGenerationEvidence {
	evidence := catalog.HyperliquidGenerationEvidence{
		EvidenceScope: catalog.RawEvidenceInMemoryProjection, SourceID: "hyperliquid-fixture-verifier", EpochID: epoch,
		ArrivalOrdinal: ordinal, GenerationStartNS: DocumentationAccessTimeNS + int64(ordinal),
		RawPayloadSHA256: sha256.Sum256(payload),
	}
	var pair []json.RawMessage
	if json.Unmarshal(payload, &pair) == nil && len(pair) == 2 && len(pair[0]) > 0 && pair[0][0] == '{' && len(pair[1]) > 0 && pair[1][0] == '[' {
		evidence.RawPayloadSHA256 = sha256.Sum256(pair[0])
		evidence.ContextPayloadSHA256 = sha256.Sum256(pair[1])
		evidence.EnvelopePayloadSHA256 = sha256.Sum256(payload)
	}
	return evidence
}
