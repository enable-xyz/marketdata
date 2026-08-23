package okx

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
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/orderbook"
)

const (
	NativeManifestVersion  = 1
	FixtureManifestVersion = 2
	VenueEvidenceVersion   = 1
	maximumManifestEntries = 256
	maximumManifestBytes   = 1 << 20
	maximumNativeFileBytes = 1 << 30
)

type NativeFileState string

const (
	NativeFileNotDue         NativeFileState = "not_due"
	NativeFileMissing        NativeFileState = "missing_after_publication_date"
	NativeFilePublishedEmpty NativeFileState = "published_empty"
	NativeFilePublished      NativeFileState = "published_with_observed_coverage"
)

type NativeFileManifest struct {
	Version    uint16            `json:"version"`
	Venue      string            `json:"venue"`
	AccessDate string            `json:"access_date"`
	Entries    []NativeFileEntry `json:"entries"`
}

type NativeFileEntry struct {
	Module                  string          `json:"module"`
	Instrument              string          `json:"instrument"`
	MarketDate              string          `json:"market_date"`
	PublicationLagDays      uint8           `json:"publication_lag_days"`
	ExpectedPublicationDate string          `json:"expected_publication_date"`
	ObservedAtNS            int64           `json:"observed_at_ns"`
	State                   NativeFileState `json:"state"`
	ExpectedFile            string          `json:"expected_file"`
	ByteLength              uint64          `json:"byte_length"`
	SHA256                  string          `json:"sha256"`
	ObservedCoverageStartNS *int64          `json:"observed_coverage_start_ns,omitempty"`
	ObservedCoverageEndNS   *int64          `json:"observed_coverage_end_ns,omitempty"`
}

type NativeManifestConfig struct {
	Root                 string
	ManifestRelativePath string
	AsOfDate             string
}

type NativeFileEvidence struct {
	Identity                string          `json:"identity"`
	Module                  string          `json:"module"`
	Instrument              string          `json:"instrument"`
	MarketDate              string          `json:"market_date"`
	PublicationLagDays      uint8           `json:"publication_lag_days"`
	ExpectedPublicationDate string          `json:"expected_publication_date"`
	State                   NativeFileState `json:"state"`
	ObservedAtNS            int64           `json:"observed_at_ns"`
	ByteLength              uint64          `json:"byte_length"`
	SHA256                  string          `json:"sha256"`
	ObservedCoverageStartNS *int64          `json:"observed_coverage_start_ns,omitempty"`
	ObservedCoverageEndNS   *int64          `json:"observed_coverage_end_ns,omitempty"`
}

type NativeManifestEvidence struct {
	Version        uint16               `json:"version"`
	Venue          string               `json:"venue"`
	AccessDate     string               `json:"access_date"`
	AsOfDate       string               `json:"as_of_date"`
	ManifestSHA256 string               `json:"manifest_sha256"`
	Files          []NativeFileEvidence `json:"files"`
	EvidenceSHA256 string               `json:"evidence_sha256"`
}

// VerifyNativeManifest verifies exactly one caller-selected manifest beneath an
// explicit caller-owned root. It hashes declared files but never parses or imports
// native contents. Missing, empty, published, and not-yet-due remain distinct.
func VerifyNativeManifest(config NativeManifestConfig) (NativeManifestEvidence, error) {
	root, manifestPath, err := resolveCallerPath(config.Root, config.ManifestRelativePath, true)
	if err != nil {
		return NativeManifestEvidence{}, err
	}
	asOf, err := parseDate(config.AsOfDate)
	if err != nil {
		return NativeManifestEvidence{}, fmt.Errorf("%w: invalid as-of date", ErrNativeManifest)
	}
	manifestBytes, err := readRegularBounded(manifestPath, maximumManifestBytes)
	if err != nil {
		return NativeManifestEvidence{}, err
	}
	var manifest NativeFileManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil || manifest.Version != NativeManifestVersion || manifest.Venue != "okx-v5" || manifest.AccessDate == "" || len(manifest.Entries) == 0 || len(manifest.Entries) > maximumManifestEntries {
		return NativeManifestEvidence{}, fmt.Errorf("%w: invalid manifest header", ErrNativeManifest)
	}
	accessDate, err := parseDate(manifest.AccessDate)
	if err != nil || accessDate.After(asOf) {
		return NativeManifestEvidence{}, fmt.Errorf("%w: invalid manifest access date", ErrNativeManifest)
	}
	seen := make(map[string]struct{}, len(manifest.Entries))
	files := make([]NativeFileEvidence, 0, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		identity := entry.Module + "\x00" + entry.Instrument + "\x00" + entry.MarketDate
		if _, duplicate := seen[identity]; duplicate {
			return NativeManifestEvidence{}, fmt.Errorf("%w: duplicate publication opportunity", ErrNativeManifest)
		}
		seen[identity] = struct{}{}
		evidence, err := verifyNativeEntry(root, asOf, entry)
		if err != nil {
			return NativeManifestEvidence{}, err
		}
		files = append(files, evidence)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	summary := NativeManifestEvidence{Version: VenueEvidenceVersion, Venue: manifest.Venue, AccessDate: manifest.AccessDate, AsOfDate: config.AsOfDate, ManifestSHA256: hex.EncodeToString(manifestDigest[:]), Files: files}
	material, err := json.Marshal(summary)
	if err != nil {
		return NativeManifestEvidence{}, err
	}
	digest := sha256.Sum256(material)
	summary.EvidenceSHA256 = hex.EncodeToString(digest[:])
	return summary, nil
}

func verifyNativeEntry(root string, asOf time.Time, entry NativeFileEntry) (NativeFileEvidence, error) {
	if !validIdentifier(entry.Module, 128) || !validIdentifier(entry.Instrument, 128) || entry.ObservedAtNS < 0 || (entry.PublicationLagDays != 2 && entry.PublicationLagDays != 3) {
		return NativeFileEvidence{}, fmt.Errorf("%w: incomplete file identity", ErrNativeManifest)
	}
	marketDate, err := parseDate(entry.MarketDate)
	if err != nil {
		return NativeFileEvidence{}, fmt.Errorf("%w: invalid market date", ErrNativeManifest)
	}
	expectedDate, err := parseDate(entry.ExpectedPublicationDate)
	if err != nil || !expectedDate.Equal(marketDate.AddDate(0, 0, int(entry.PublicationLagDays))) {
		return NativeFileEvidence{}, fmt.Errorf("%w: publication date does not match declared T+%d lag", ErrNativeManifest, entry.PublicationLagDays)
	}
	_, path, err := resolveCallerPath(root, entry.ExpectedFile, false)
	if err != nil {
		return NativeFileEvidence{}, err
	}
	due := !asOf.Before(expectedDate)
	if time.Unix(0, entry.ObservedAtNS).UTC().Format("2006-01-02") != asOf.Format("2006-01-02") {
		return NativeFileEvidence{}, fmt.Errorf("%w: observation timestamp differs from verification date", ErrNativeManifest)
	}
	evidence := NativeFileEvidence{Identity: entry.Module + "/" + entry.Instrument + "/" + entry.MarketDate, Module: entry.Module, Instrument: entry.Instrument, MarketDate: entry.MarketDate,
		PublicationLagDays: entry.PublicationLagDays, ExpectedPublicationDate: entry.ExpectedPublicationDate, State: entry.State, ObservedAtNS: entry.ObservedAtNS,
		ObservedCoverageStartNS: cloneInt64(entry.ObservedCoverageStartNS), ObservedCoverageEndNS: cloneInt64(entry.ObservedCoverageEndNS)}
	switch entry.State {
	case NativeFileNotDue:
		if due || entry.ByteLength != 0 || entry.SHA256 != "" || entry.ObservedCoverageStartNS != nil || entry.ObservedCoverageEndNS != nil || pathExists(path) {
			return NativeFileEvidence{}, fmt.Errorf("%w: not-due state conflicts with observed evidence", ErrNativeManifest)
		}
	case NativeFileMissing:
		if !due || entry.ByteLength != 0 || entry.SHA256 != "" || entry.ObservedCoverageStartNS != nil || entry.ObservedCoverageEndNS != nil || pathExists(path) {
			return NativeFileEvidence{}, fmt.Errorf("%w: missing state conflicts with file evidence", ErrNativeManifest)
		}
	case NativeFilePublishedEmpty:
		if !due || entry.ByteLength != 0 || entry.SHA256 != emptySHA256() || entry.ObservedCoverageStartNS != nil || entry.ObservedCoverageEndNS != nil {
			return NativeFileEvidence{}, fmt.Errorf("%w: empty publication became a zero observation", ErrNativeManifest)
		}
		length, digest, err := hashRegularBounded(path, 1)
		if err != nil || length != 0 || digest != entry.SHA256 {
			return NativeFileEvidence{}, fmt.Errorf("%w: declared empty file is unavailable or nonempty", ErrNativeManifest)
		}
		evidence.SHA256 = entry.SHA256
	case NativeFilePublished:
		if !due || entry.ByteLength == 0 || entry.ByteLength > maximumNativeFileBytes || len(entry.SHA256) != sha256.Size*2 || entry.ObservedCoverageStartNS == nil || entry.ObservedCoverageEndNS == nil || *entry.ObservedCoverageStartNS < 0 || *entry.ObservedCoverageEndNS < *entry.ObservedCoverageStartNS {
			return NativeFileEvidence{}, fmt.Errorf("%w: incomplete published-file evidence", ErrNativeManifest)
		}
		length, digest, err := hashRegularBounded(path, int64(entry.ByteLength))
		if err != nil || length != entry.ByteLength {
			return NativeFileEvidence{}, fmt.Errorf("%w: published file length mismatch", ErrNativeManifest)
		}
		if digest != entry.SHA256 {
			return NativeFileEvidence{}, fmt.Errorf("%w: published file digest mismatch", ErrNativeManifest)
		}
		evidence.ByteLength = entry.ByteLength
		evidence.SHA256 = entry.SHA256
	default:
		return NativeFileEvidence{}, fmt.Errorf("%w: unknown publication state", ErrNativeManifest)
	}
	return evidence, nil
}

type SourceContractFixture struct {
	Version          uint16                      `json:"version"`
	PublicEndpoint   string                      `json:"public_endpoint"`
	BusinessEndpoint string                      `json:"business_endpoint"`
	RESTEndpoint     string                      `json:"rest_endpoint"`
	Roles            []SourceContractFixtureRole `json:"roles"`
	HandshakeBudget  SourceContractFixtureBudget `json:"handshake_budget"`
	OperationBudget  SourceContractFixtureBudget `json:"operation_budget"`
}

type SourceContractFixtureRole struct {
	Role              string `json:"role"`
	EndpointOrChannel string `json:"endpoint_or_channel"`
	Transport         string `json:"transport"`
	Entitlement       string `json:"entitlement"`
	Support           string `json:"support"`
}

type SourceContractFixtureBudget struct {
	Scope            string `json:"scope"`
	Capacity         uint32 `json:"capacity"`
	RefillTokens     uint32 `json:"refill_tokens"`
	RefillIntervalNS uint64 `json:"refill_interval_ns"`
	Cost             uint32 `json:"cost"`
}

func ExpectedSourceContractFixture() SourceContractFixture {
	roles := make([]SourceContractFixtureRole, 0, len(SupportMatrix()))
	for _, role := range SupportMatrix() {
		endpoint, _ := capabilityIdentity(role.Role)
		transport := "rest"
		switch {
		case role.Socket != "":
			transport = string(role.Socket)
		case role.Role == RoleNativeFileManifest:
			transport = "caller_owned_filesystem"
		}
		support := "available"
		switch role.Support {
		case capture.SupportUnsupported:
			support = "unsupported"
		case capture.SupportAmbiguous:
			support = "ambiguous"
		}
		roles = append(roles, SourceContractFixtureRole{Role: string(role.Role), EndpointOrChannel: endpoint, Transport: transport, Entitlement: role.Entitlement, Support: support})
	}
	handshake := HandshakeRatePolicy()
	operation := OperationRatePolicy()
	return SourceContractFixture{
		Version: 1, PublicEndpoint: PublicWebSocketEndpoint, BusinessEndpoint: BusinessWebSocketEndpoint, RESTEndpoint: PublicRESTEndpoint, Roles: roles,
		HandshakeBudget: SourceContractFixtureBudget{Scope: "shared_ip_handshake", Capacity: handshake.Capacity, RefillTokens: handshake.RefillTokens, RefillIntervalNS: handshake.RefillIntervalNS, Cost: handshake.ConnectionCost},
		OperationBudget: SourceContractFixtureBudget{Scope: "per_connection_subscribe_unsubscribe_login", Capacity: operation.Capacity, RefillTokens: operation.RefillTokens, RefillIntervalNS: operation.RefillIntervalNS, Cost: operation.RequestCost},
	}
}

var acceptanceFixtureRoles = []string{
	"source_contract",
	"trades_all",
	"book_snapshot",
	"book_no_change",
	"book_reset",
	"book_checksum_pre",
	"book_checksum_post",
	"books5",
	"vip_denial",
	"market_mapping",
	"option_mapping",
	"lifecycle_mapping",
	"liquidation_mapping",
}

type fixtureProvenance struct {
	SourceURL     string
	SourceSection string
}

var acceptanceFixtureProvenance = map[string]fixtureProvenance{
	"source_contract":     {SourceURL: GuideDocumentationURI, SourceSection: "API overview and public WebSocket/REST market data contracts"},
	"trades_all":          {SourceURL: GuideDocumentationURI + "#order-book-trading-market-data-ws-all-trades-channel", SourceSection: "All trades channel"},
	"book_snapshot":       {SourceURL: BookDocumentationURI, SourceSection: "Order book channel snapshot"},
	"book_no_change":      {SourceURL: BookDocumentationURI, SourceSection: "Order book channel sequence rules"},
	"book_reset":          {SourceURL: BookDocumentationURI, SourceSection: "Order book channel sequence reset"},
	"book_checksum_pre":   {SourceURL: ChecksumDocumentationURI, SourceSection: "Books checksum change before 2026-06-23 cutover"},
	"book_checksum_post":  {SourceURL: ChecksumDocumentationURI, SourceSection: "Books checksum change at 2026-06-23 cutover"},
	"books5":              {SourceURL: BookDocumentationURI, SourceSection: "Books5 channel"},
	"vip_denial":          {SourceURL: BookDocumentationURI, SourceSection: "VIP order-book entitlement denial"},
	"market_mapping":      {SourceURL: GuideDocumentationURI + "#order-book-trading-market-data-ws-tickers-channel", SourceSection: "Tickers channel"},
	"option_mapping":      {SourceURL: GuideDocumentationURI + "#public-data-websocket-option-summary-channel", SourceSection: "Option summary channel"},
	"lifecycle_mapping":   {SourceURL: GuideDocumentationURI + "#public-data-rest-api-get-instruments", SourceSection: "Get instruments"},
	"liquidation_mapping": {SourceURL: LiquidationDocumentationURI, SourceSection: "Liquidation orders channel"},
}

type FixtureManifest struct {
	Version      uint16         `json:"version"`
	Venue        string         `json:"venue"`
	AccessDate   string         `json:"access_date"`
	FixtureClaim string         `json:"fixture_claim"`
	Fixtures     []FixtureEntry `json:"fixtures"`
}

type FixtureEntry struct {
	ID             string `json:"id"`
	Role           string `json:"role"`
	File           string `json:"file"`
	Classification string `json:"classification"`
	SourceURL      string `json:"source_url"`
	SourceSection  string `json:"source_section"`
	DerivedFrom    string `json:"derived_from"`
	ByteLength     uint32 `json:"byte_length"`
	SHA256         string `json:"sha256"`
}

type FixtureEvidence struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	SHA256 string `json:"sha256"`
}

type FixtureSummary struct {
	Version        uint16            `json:"version"`
	Venue          string            `json:"venue"`
	AccessDate     string            `json:"access_date"`
	EvidenceScope  string            `json:"evidence_scope"`
	ManifestSHA256 string            `json:"manifest_sha256"`
	Fixtures       []FixtureEvidence `json:"fixtures"`
	EvidenceSHA256 string            `json:"evidence_sha256"`
}

type VenueVerificationConfig struct {
	Root                    string
	FixtureManifestRelative string
	NativeManifestRelative  string
	AsOfDate                string
}

type VenueEvidence struct {
	Version        uint16                 `json:"version"`
	Venue          string                 `json:"venue"`
	Fixtures       FixtureSummary         `json:"fixtures"`
	NativeFiles    NativeManifestEvidence `json:"native_files"`
	EvidenceSHA256 string                 `json:"evidence_sha256"`
}

// VerifyVenue is the offline coordinator hook. Both exact manifests are chosen
// by the caller under one caller-owned root; it performs no discovery or network I/O.
func VerifyVenue(config VenueVerificationConfig) (VenueEvidence, error) {
	fixtures, err := VerifyFixtures(config.Root, config.FixtureManifestRelative)
	if err != nil {
		return VenueEvidence{}, err
	}
	nativeFiles, err := VerifyNativeManifest(NativeManifestConfig{Root: config.Root, ManifestRelativePath: config.NativeManifestRelative, AsOfDate: config.AsOfDate})
	if err != nil {
		return VenueEvidence{}, err
	}
	evidence := VenueEvidence{Version: VenueEvidenceVersion, Venue: "okx-v5", Fixtures: fixtures, NativeFiles: nativeFiles}
	material, err := json.Marshal(evidence)
	if err != nil {
		return VenueEvidence{}, err
	}
	digest := sha256.Sum256(material)
	evidence.EvidenceSHA256 = hex.EncodeToString(digest[:])
	return evidence, nil
}

// VerifyFixtures accepts only digest-bound synthetic JSON fixtures. Primary or
// live payload classification is rejected at this offline boundary.
func VerifyFixtures(root, manifestRelativePath string) (FixtureSummary, error) {
	canonicalRoot, manifestPath, err := resolveCallerPath(root, manifestRelativePath, true)
	if err != nil {
		return FixtureSummary{}, err
	}
	manifestBytes, err := readRegularBounded(manifestPath, maximumManifestBytes)
	if err != nil {
		return FixtureSummary{}, err
	}
	var manifest FixtureManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return FixtureSummary{}, fmt.Errorf("%w: invalid fixture manifest", ErrFixtureBoundary)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF ||
		manifest.Version != FixtureManifestVersion || manifest.Venue != "okx-v5" ||
		manifest.AccessDate != DocumentationAccessDate || manifest.FixtureClaim == "" ||
		len(manifest.FixtureClaim) > 1024 || strings.IndexByte(manifest.FixtureClaim, 0) >= 0 ||
		len(manifest.Fixtures) != len(acceptanceFixtureRoles) {
		return FixtureSummary{}, fmt.Errorf("%w: invalid fixture manifest", ErrFixtureBoundary)
	}
	seen := make(map[string]struct{}, len(manifest.Fixtures))
	seenRoles := make(map[string]struct{}, len(manifest.Fixtures))
	fixtures := make([]FixtureEvidence, 0, len(manifest.Fixtures))
	for _, entry := range manifest.Fixtures {
		provenance, knownRole := acceptanceFixtureProvenance[entry.Role]
		if !validIdentifier(entry.ID, 128) || entry.ID != "okx-"+entry.Role || !validIdentifier(entry.Role, 128) ||
			entry.Classification != "synthetic_parseable_projection" ||
			entry.SourceURL != provenance.SourceURL || entry.SourceSection != provenance.SourceSection ||
			entry.DerivedFrom == "" || len(entry.DerivedFrom) > 512 || strings.IndexByte(entry.DerivedFrom, 0) >= 0 ||
			entry.ByteLength == 0 || len(entry.SHA256) != sha256.Size*2 {
			return FixtureSummary{}, fmt.Errorf("%w: fixture is not source-labelled synthetic evidence", ErrFixtureBoundary)
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return FixtureSummary{}, fmt.Errorf("%w: duplicate fixture ID", ErrFixtureBoundary)
		}
		seen[entry.ID] = struct{}{}
		if !knownRole || !slices.Contains(acceptanceFixtureRoles, entry.Role) {
			return FixtureSummary{}, fmt.Errorf("%w: unknown acceptance fixture role %q", ErrFixtureBoundary, entry.Role)
		}
		if _, duplicate := seenRoles[entry.Role]; duplicate {
			return FixtureSummary{}, fmt.Errorf("%w: duplicate acceptance fixture role %q", ErrFixtureBoundary, entry.Role)
		}
		seenRoles[entry.Role] = struct{}{}
		_, path, err := resolveCallerPath(canonicalRoot, entry.File, true)
		if err != nil {
			return FixtureSummary{}, err
		}
		payload, err := readRegularBounded(path, maximumManifestBytes)
		if err != nil || len(payload) != int(entry.ByteLength) || !json.Valid(payload) {
			return FixtureSummary{}, fmt.Errorf("%w: invalid synthetic fixture %s", ErrFixtureBoundary, entry.ID)
		}
		digest := sha256.Sum256(payload)
		if hex.EncodeToString(digest[:]) != entry.SHA256 {
			return FixtureSummary{}, fmt.Errorf("%w: fixture digest mismatch", ErrFixtureBoundary)
		}
		if err := verifySyntheticFixture(entry.Role, payload); err != nil {
			return FixtureSummary{}, fmt.Errorf("%w: fixture %s contract failed: %v", ErrFixtureBoundary, entry.ID, err)
		}
		fixtures = append(fixtures, FixtureEvidence{ID: entry.ID, Role: entry.Role, SHA256: entry.SHA256})
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	summary := FixtureSummary{
		Version: VenueEvidenceVersion, Venue: manifest.Venue, AccessDate: manifest.AccessDate,
		EvidenceScope: "offline_repository_fixture", ManifestSHA256: hex.EncodeToString(manifestDigest[:]), Fixtures: fixtures,
	}
	material, err := json.Marshal(summary)
	if err != nil {
		return FixtureSummary{}, err
	}
	digest := sha256.Sum256(material)
	summary.EvidenceSHA256 = hex.EncodeToString(digest[:])
	return summary, nil
}

func verifySyntheticFixture(role string, payload []byte) error {
	switch role {
	case "source_contract":
		return verifySourceContractFixture(payload)
	case "trades_all":
		trades, err := ParseTradesAll(payload)
		if err != nil || len(trades) != 1 {
			return fmt.Errorf("single trades-all match: %w", err)
		}
		metadata, err := fixtureMetadata(normalize.TradeSchemaName, normalize.TradeSchemaVersion, "okx-btc-usdt", trades[0].TimestampMS*1_000_000, payload)
		if err != nil {
			return err
		}
		_, err = trades[0].Normalized(metadata, normalize.SpotPriceUnit("BTC", "USDT"), normalize.BaseAssetUnit("BTC"))
		return err
	case "book_snapshot", "book_no_change", "book_reset", "book_checksum_pre", "book_checksum_post", "books5":
		return verifyBookInvariantFixture(role, payload)
	case "vip_denial":
		return verifyVIPFixture(payload)
	case "market_mapping":
		return verifyMarketMappingFixture(payload)
	case "option_mapping":
		return verifyOptionMappingFixture(payload)
	case "lifecycle_mapping":
		return verifyLifecycleMappingFixture(payload)
	case "liquidation_mapping":
		return verifyLiquidationMappingFixture(payload)
	default:
		return fmt.Errorf("unknown synthetic fixture role %q", role)
	}
}

func verifySourceContractFixture(payload []byte) error {
	var observed SourceContractFixture
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&observed); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("source contract fixture has trailing content")
	}
	if !reflect.DeepEqual(observed, ExpectedSourceContractFixture()) {
		return fmt.Errorf("source contract fixture does not match endpoints, roles, entitlements, and budgets")
	}
	if _, err := PublicSourceContract(PublicSocket); err != nil {
		return err
	}
	if _, err := PublicSourceContract(BusinessSocket); err != nil {
		return err
	}
	if _, err := RESTSourceContract(); err != nil {
		return err
	}
	if _, err := NewHandshakeBudget(0); err != nil {
		return err
	}
	_, err := NewOperationBudget(0)
	return err
}

func verifyBookInvariantFixture(role string, payload []byte) error {
	message, err := ParseBook(payload)
	if err != nil {
		return err
	}
	sourceTimeNS := message.TimestampMS * 1_000_000
	if sourceTimeNS > 1<<63-1-1_000_000 {
		return fmt.Errorf("book fixture source time overflows receive time")
	}
	receivedTimeNS := sourceTimeNS + 1_000_000
	if role == "books5" {
		metadata, err := fixtureMetadata(normalize.BookUpdateSchemaName, normalize.BookUpdateSchemaVersion, "okx-btc-usdt", sourceTimeNS, payload)
		if err != nil {
			return err
		}
		projection, err := message.Normalized(metadata, normalize.SpotPriceUnit("BTC", "USDT"), normalize.BaseAssetUnit("BTC"))
		if err != nil || !projection.SnapshotReplacement || projection.Reconstructable || projection.DepthContract != "books5" {
			return fmt.Errorf("books5 replacement projection: %w", err)
		}
		return nil
	}
	book, err := orderbook.NewOKXBook(message.Channel, message.InstrumentID, 400)
	if err != nil {
		return err
	}
	if role == "book_no_change" || role == "book_reset" {
		if message.Action != "update" || message.PreviousSeq < 0 || sourceTimeNS < ChecksumCutoverTimeNS {
			return fmt.Errorf("invalid sequence-exception fixture")
		}
		_, err = book.Apply(orderbook.OKXUpdate{Channel: message.Channel, InstrumentID: message.InstrumentID, Action: "snapshot", SourceTimeNS: sourceTimeNS, ReceivedTimeNS: receivedTimeNS, PreviousSeqID: -1, SeqID: message.PreviousSeq, Checksum: 0})
		if err != nil {
			return err
		}
	}
	result, err := book.Apply(message.ReconstructionUpdate(receivedTimeNS))
	if err != nil {
		return err
	}
	switch role {
	case "book_snapshot":
		if result.Kind != orderbook.OKXAppliedSnapshot {
			return fmt.Errorf("baseline snapshot was not applied")
		}
	case "book_no_change":
		if result.Kind != orderbook.OKXAppliedNoChange {
			return fmt.Errorf("documented no-change exception was not applied")
		}
	case "book_reset":
		if result.Kind != orderbook.OKXAppliedMaintenance {
			return fmt.Errorf("documented maintenance reset was not applied")
		}
	case "book_checksum_pre":
		if result.ChecksumStatus != orderbook.OKXChecksumValidated {
			return fmt.Errorf("pre-cutover checksum was not validated")
		}
	case "book_checksum_post":
		if result.ChecksumStatus != orderbook.OKXChecksumUnavailable {
			return fmt.Errorf("post-cutover checksum was not unavailable")
		}
	}
	return nil
}

func verifyVIPFixture(payload []byte) error {
	vip := SubscriptionArg{Channel: "books-l2-tbt", InstrumentID: "BTC-USDT"}
	if _, err := NewSubscriptionSession(PublicSocket, Entitlement{}, []SubscriptionArg{vip}); !errors.Is(err, ErrVIPEntitlement) {
		return fmt.Errorf("VIP subscription without entitlement was not denied")
	}
	session, err := NewSubscriptionSession(PublicSocket, Entitlement{LoggedIn: true, VIPLevel: 4, LoginEvidence: "synthetic-login-evidence", SourceIdentity: "synthetic-vip-source"}, []SubscriptionArg{vip})
	if err != nil {
		return err
	}
	messages, err := session.Messages()
	if err != nil || len(messages) != 1 {
		return fmt.Errorf("entitled VIP subscription was not operable: %w", err)
	}
	if err := session.ValidateMessage(messages[0]); err != nil {
		return err
	}
	_, err = session.Acknowledge(payload)
	var rejection *SubscriptionRejection
	if !errors.As(err, &rejection) || !rejection.Terminal || rejection.Code != "64003" || session.Pending() != 0 || len(session.TerminalDenials()) != 1 {
		return fmt.Errorf("VIP 64003 denial was not terminal")
	}
	reconnect, err := session.ReconnectMessages()
	if err != nil || len(reconnect) != 0 || len(session.TerminalDenials()) != 1 {
		return fmt.Errorf("terminal VIP denial was re-emitted")
	}
	return nil
}

func verifyMarketMappingFixture(payload []byte) error {
	observations, err := ParseMarketObservations(payload)
	if err != nil || len(observations) != 1 || observations[0].Channel != "tickers" {
		return fmt.Errorf("market fixture: %w", err)
	}
	sourceTimeNS, err := observations[0].sourceTimeNS(normalize.Metadata{})
	if err != nil {
		return err
	}
	metadata, err := fixtureMetadata(normalize.TickerSchemaName, normalize.TickerSchemaVersion, "okx-btc-usdt", sourceTimeNS, payload)
	if err != nil {
		return err
	}
	ticker, err := observations[0].NormalizedSpotTicker(metadata, SpotUnitContract{Price: normalize.SpotPriceUnit("BTC", "USDT"), BaseAmount: normalize.BaseAssetUnit("BTC"), QuoteAmount: normalize.QuoteAssetUnit("USDT")})
	if err != nil || ticker.BidPrice.State != normalize.SourceEmpty || ticker.BidAmount.State != normalize.SourceMissing || ticker.AskAmount.State != normalize.SourceValue || !ticker.AskAmount.Value.Decimal.IsZero() {
		return fmt.Errorf("spot ticker missing/empty/zero mapping: %w", err)
	}
	return nil
}

func verifyOptionMappingFixture(payload []byte) error {
	observations, err := ParseMarketObservations(payload)
	if err != nil || len(observations) != 1 || observations[0].Channel != "opt-summary" {
		return fmt.Errorf("option fixture: %w", err)
	}
	sourceTimeNS, err := observations[0].sourceTimeNS(normalize.Metadata{})
	if err != nil {
		return err
	}
	instrument := observations[0].InstrumentID.Text
	metadata, err := fixtureMetadata(normalize.OptionSummarySchemaName, normalize.OptionSummarySchemaVersion, instrument, sourceTimeNS, payload)
	if err != nil {
		return err
	}
	contracts := normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrument}
	greek := normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "okx_native_greek"}
	option, err := observations[0].NormalizedOption(metadata,
		OptionTerms{InstrumentUID: instrument, UnderlyingID: "BTC-USD", IndexID: "BTC-USD", ExpiryMS: "1790294400000", Strike: "65000", CallPut: "C", ObservedAtNS: sourceTimeNS},
		OptionUnitContract{Price: normalize.SpotPriceUnit("BTC", "USD"), Greek: greek, OpenInterest: contracts, Volume: contracts})
	if err != nil || option.CallPut.Value != normalize.OptionCall || option.Gamma.State != normalize.SourceMissing {
		return fmt.Errorf("option terms/Greek mapping: %w", err)
	}
	return nil
}

func verifyLifecycleMappingFixture(payload []byte) error {
	observations, err := ParseInstruments(payload)
	if err != nil || len(observations) != 1 {
		return fmt.Errorf("lifecycle fixture: %w", err)
	}
	state, _, err := observations[0].LifecycleState()
	if err != nil || state != normalize.InstrumentStateContinuousTrading {
		return fmt.Errorf("lifecycle state mapping: %w", err)
	}
	instrument := observations[0].Fields["instId"].Text
	generation, err := strconv.ParseUint(observations[0].Fields["metadataGeneration"].Text, 10, 64)
	if err != nil {
		return fmt.Errorf("lifecycle metadata generation")
	}
	listingMS, err := strconv.ParseInt(observations[0].Fields["listTime"].Text, 10, 64)
	if err != nil || listingMS < 0 || listingMS > (1<<63-1)/1_000_000 {
		return fmt.Errorf("lifecycle listing time")
	}
	sourceTimeNS := listingMS * 1_000_000
	metadata, err := fixtureMetadata(normalize.InstrumentEventSchemaName, normalize.InstrumentEventSchemaVersion, instrument, sourceTimeNS, payload)
	if err != nil {
		return err
	}
	provenance := normalize.FieldProvenance{SourceTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond, AgeNS: normalize.OptionalUint64{Value: uint64(metadata.ReceivedTimeNS - sourceTimeNS), Valid: true}}
	missing := normalize.FieldProvenance{SourceTimeResolution: normalize.ResolutionAbsent}
	missingTime := normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent, Provenance: missing}
	missingNumeric := normalize.NumericField{State: normalize.SourceMissing, Provenance: missing}
	missingNative := normalize.NativeNumericField{State: normalize.SourceMissing, Provenance: missing}
	event, err := normalize.MapOKXLifecycle(metadata, normalize.OKXLifecycleInput{
		MetadataGeneration: normalize.Uint64Field{State: normalize.SourceValue, Value: generation, Provenance: provenance},
		NativeStateBefore:  normalize.InstrumentStateField{State: normalize.SourceMissing, Provenance: missing},
		NativeStateAfter:   normalize.InstrumentStateField{State: normalize.SourceValue, Value: state, Provenance: provenance},
		ListingTime:        normalize.TimeField{State: normalize.SourceValue, ValueNS: sourceTimeNS, Resolution: normalize.ResolutionMillisecond, Provenance: provenance},
		ContinuousTime:     missingTime, ExpiryTime: missingTime, DeliveryTime: missingTime, DelistingTime: missingTime,
		TickSize:           normalize.NumericChange{Old: missingNumeric, New: missingNumeric},
		LotSize:            normalize.NativeNumericChange{Old: missingNative, New: missingNative},
		ContractMultiplier: normalize.NativeNumericChange{Old: missingNative, New: missingNative},
		Payoff:             normalize.TextChange{Old: normalize.TextField{State: normalize.SourceMissing, Provenance: missing}, New: normalize.TextField{State: normalize.SourceMissing, Provenance: missing}},
		OldRawHash:         normalize.HashField{State: normalize.SourceMissing, Provenance: missing},
		NewRawHash:         normalize.HashField{State: normalize.SourceValue, Value: normalize.Hash(sha256.Sum256(payload)), Provenance: provenance},
		ResolutionStatus:   normalize.InstrumentResolutionField{State: normalize.SourceValue, Value: normalize.InstrumentResolved, Provenance: provenance},
	})
	if err != nil || event.NativeStateAfter.Value != normalize.InstrumentStateContinuousTrading {
		return fmt.Errorf("lifecycle event mapping: %w", err)
	}
	return nil
}

func verifyLiquidationMappingFixture(payload []byte) error {
	batches, err := ParseLiquidations(payload)
	if err != nil || len(batches) != 1 || len(batches[0].Details) != 2 || batches[0].Details[0].Side.Text != "sell" || batches[0].Details[1].Side.Text != "buy" {
		return fmt.Errorf("liquidation source order: %w", err)
	}
	sourceTimeMS, err := strconv.ParseInt(batches[0].Details[0].TimestampMS.Text, 10, 64)
	if err != nil || sourceTimeMS < 0 || sourceTimeMS > (1<<63-1)/1_000_000 {
		return fmt.Errorf("liquidation source timestamp")
	}
	instrument := batches[0].InstrumentID.Text
	metadata, err := fixtureMetadata(normalize.LiquidationSchemaName, normalize.LiquidationSchemaVersion, instrument, sourceTimeMS*1_000_000, payload)
	if err != nil {
		return err
	}
	event, err := batches[0].Details[0].Normalized(metadata, "synthetic-batch-0-detail-0", normalize.SpotPriceUnit("BTC", "USDT"), normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrument})
	if err != nil || event.Completeness != normalize.LiquidationPartialNonchronological || event.Window.Selection != normalize.LiquidationWindowSelectionUnknown {
		return fmt.Errorf("incomplete liquidation mapping: %w", err)
	}
	return nil
}

func fixtureMetadata(schema string, version uint16, instrument string, sourceTimeNS int64, payload []byte) (normalize.Metadata, error) {
	if sourceTimeNS < 0 || sourceTimeNS > 1<<63-1-1_000_000 {
		return normalize.Metadata{}, fmt.Errorf("invalid fixture source time")
	}
	epoch := [16]byte{1}
	envelope := capture.EnvelopeV1{EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket, SourceID: "okx-v5-public", ChannelOrEndpoint: "synthetic-okx", ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true}, ArrivalOrdinal: 1, ExchangeTimeNS: capture.OptionalInt64{Value: sourceTimeNS, Valid: true}, ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: sourceTimeNS + 1_000_000, ClockEpochID: "okx-synthetic-clock", MonotonicNSSinceClockEpoch: 1, PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved, RecorderVersion: "okx-fixture-verifier-v1"}
	envelope.SetRawPayload(payload)
	record, err := normalize.BindRawRecord(envelope, normalize.Hash(sha256.Sum256([]byte("okx-synthetic-segment"))), 1, nil)
	if err != nil {
		return normalize.Metadata{}, err
	}
	return normalize.NewMetadata(normalize.MetadataInput{Record: record, SchemaName: schema, SchemaVersion: version, InstrumentUID: instrument, ExchangeTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, ExchangeTimeResolution: normalize.ResolutionMillisecond, SourceEventTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond, SourceSchemaFingerprint: normalize.Hash(sha256.Sum256([]byte("okx-synthetic-schema"))), MapperVersion: "okx-v5-mapper-v1", MapperBindingID: normalize.Hash(sha256.Sum256([]byte("okx-synthetic-binding"))), CatalogSnapshotID: normalize.Hash(sha256.Sum256([]byte("okx-synthetic-catalog")))})
}

func resolveCallerPath(root, relative string, mustExist bool) (string, string, error) {
	if root == "" || relative == "" {
		return "", "", fmt.Errorf("%w: caller root and relative path are required", ErrFixtureBoundary)
	}
	cleanRoot := filepath.Clean(root)
	rootInfo, err := os.Lstat(cleanRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("%w: caller root is not a real directory", ErrFixtureBoundary)
	}
	canonicalRoot, err := canonicalPath(cleanRoot)
	if err != nil {
		return "", "", err
	}
	clean := filepath.Clean(relative)
	if clean != relative || filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("%w: path traversal", ErrFixtureBoundary)
	}
	path := canonicalRoot
	components := strings.Split(clean, string(filepath.Separator))
	for index, component := range components {
		candidate := filepath.Join(path, component)
		info, statErr := os.Lstat(candidate)
		if statErr != nil {
			if !os.IsNotExist(statErr) {
				return "", "", statErr
			}
			if mustExist {
				return "", "", fmt.Errorf("%w: selected file is unavailable", ErrFixtureBoundary)
			}
			unresolved := filepath.Join(append([]string{candidate}, components[index+1:]...)...)
			if err := ensureContained(canonicalRoot, unresolved); err != nil {
				return "", "", err
			}
			return canonicalRoot, unresolved, nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", "", fmt.Errorf("%w: selected path contains a symlink", ErrFixtureBoundary)
		}
		if index < len(components)-1 && !info.IsDir() {
			return "", "", fmt.Errorf("%w: selected path parent is not a directory", ErrFixtureBoundary)
		}
		path = candidate
	}
	if err := ensureContained(canonicalRoot, path); err != nil {
		return "", "", err
	}
	if mustExist {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return "", "", fmt.Errorf("%w: selected file is unavailable", ErrFixtureBoundary)
		}
	}
	return canonicalRoot, path, nil
}

func canonicalPath(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}

func ensureContained(root, path string) error {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: selected file escaped caller root", ErrFixtureBoundary)
	}
	return nil
}

func readRegularBounded(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maximum {
		return nil, fmt.Errorf("%w: non-regular or oversized file", ErrFixtureBoundary)
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

func hashRegularBounded(path string, maximum int64) (uint64, string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maximum {
		return 0, "", fmt.Errorf("%w: non-regular or oversized file", ErrFixtureBoundary)
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	digest := sha256.New()
	length, err := io.Copy(digest, io.LimitReader(file, maximum+1))
	if err != nil || length > maximum {
		return 0, "", fmt.Errorf("%w: bounded hash failed", ErrFixtureBoundary)
	}
	return uint64(length), hex.EncodeToString(digest.Sum(nil)), nil
}

func parseDate(value string) (time.Time, error) {
	if len(value) != len("2006-01-02") {
		return time.Time{}, fmt.Errorf("invalid date")
	}
	return time.Parse("2006-01-02", value)
}

func pathExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func emptySHA256() string {
	digest := sha256.Sum256(nil)
	return hex.EncodeToString(digest[:])
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
