package binance

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
)

const (
	USDMFixtureManifestVersion uint16 = 1
	USDMMaxFixtureCount               = 32
	USDMMaxManifestBytes              = 64 << 10
	USDMMaxFixtureBytes               = 1 << 20
	USDMMaxFixtureCorpusBytes         = 16 << 20
)

var ErrUSDMFixtureVerification = errors.New("binance: USD-M fixture verification failed")

type USDMVerifiedFixture struct {
	ID                string
	SHA256            [sha256.Size]byte
	ByteLength        uint32
	OfficialDerived   bool
	SyntheticMutation bool
}

type USDMRoutingEvidence struct {
	AllowlistedStreams int
	PublicRouteStreams int
	MarketRouteStreams int
	WrongRouteRejected bool
}

type USDMBookEvidence struct {
	SnapshotBridgeApplied bool
	PUContinuityEnforced  bool
	GapClosedEpoch        bool
	ZeroDeleteRetained    bool
}

type USDMTickerEvidence struct {
	NativeBBOChecked          bool
	GenericTickerChecked      bool
	DerivativeTickerChecked   bool
	IndexPriceStreamChecked   bool
	IndependentOptionalFields bool
}

type USDMOpenInterestEvidence struct {
	RESTPollObservationChecked bool
	NativeUnitRetained         bool
	SidednessUnspecified       bool
	WrongSymbolRejected        bool
	SharedFAPIRatePoolChecked  bool
}

type USDMLiquidationEvidence struct {
	LargestPerSymbolWindowChecked bool
	OrderSideRetained             bool
	CompleteTapeClaimAbsent       bool
}

type USDMRPIEvidence struct {
	CandidateRoute           USDMRoute
	SupportRemainsCandidate  bool
	LiveSubscriptionRejected bool
	RoutedPayloadProof       bool
}

type USDMFixtureEvidence struct {
	Version                 uint16
	ManifestSHA256          [sha256.Size]byte
	AccessedAtNS            int64
	OfficialDerivedCount    int
	SyntheticMutationCount  int
	Fixtures                []USDMVerifiedFixture
	Routing                 USDMRoutingEvidence
	AggregateTradeRole      USDMStreamRole
	AggregateTradeCeilingNS uint64
	QuantityQRetained       bool
	QuantityNQRetained      bool
	MetadataChecked         bool
	Book                    USDMBookEvidence
	Ticker                  USDMTickerEvidence
	OpenInterest            USDMOpenInterestEvidence
	Liquidation             USDMLiquidationEvidence
	RPI                     USDMRPIEvidence
}

type usdmFixtureManifest struct {
	Version        uint16                     `json:"version"`
	AccessedAtNS   int64                      `json:"accessed_at_ns"`
	SourceFaithful bool                       `json:"source_faithful"`
	Documentation  []string                   `json:"documentation"`
	Fixtures       []usdmFixtureManifestEntry `json:"fixtures"`
}

type usdmFixtureManifestEntry struct {
	ID                string `json:"id"`
	Path              string `json:"path"`
	SHA256            string `json:"sha256"`
	Bytes             uint32 `json:"bytes"`
	Provenance        string `json:"provenance"`
	SourceReference   string `json:"source_reference"`
	DerivedFrom       string `json:"derived_from"`
	SyntheticMutation bool   `json:"synthetic_mutation"`
}

var requiredUSDMFixtureIDs = []string{
	"official.agg_trade", "official.book_ticker", "official.depth_snapshot", "official.depth_update", "official.exchange_info", "official.index_price", "official.liquidation", "official.mark_price", "official.open_interest", "official.routing", "official.ticker",
	"synthetic.liquidation_completeness", "synthetic.open_interest_wrong_symbol", "synthetic.pu_gap", "synthetic.q_nq_direction", "synthetic.rpi_candidate", "synthetic.wrong_route",
}

// VerifyUSDMFixtures deterministically verifies the immutable, access-dated
// ELMD-015 corpus rooted at either an explicit manifest path or its directory.
// It performs no network access and returns typed evidence rather than prose.
func VerifyUSDMFixtures(manifestPathOrRoot string) (USDMFixtureEvidence, error) {
	manifestPath, root, manifestBytes, err := loadUSDMManifest(manifestPathOrRoot)
	if err != nil {
		return USDMFixtureEvidence{}, err
	}
	_ = manifestPath
	var manifest usdmFixtureManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return USDMFixtureEvidence{}, fmt.Errorf("%w: malformed manifest", ErrUSDMFixtureVerification)
	}
	if err := validateUSDMManifest(manifest); err != nil {
		return USDMFixtureEvidence{}, err
	}
	payloads := make(map[string][]byte, len(manifest.Fixtures))
	evidence := USDMFixtureEvidence{Version: USDMFixtureManifestVersion, ManifestSHA256: sha256.Sum256(manifestBytes), AccessedAtNS: manifest.AccessedAtNS}
	var total uint64
	for _, entry := range manifest.Fixtures {
		payload, digest, err := loadUSDMFixture(root, entry)
		if err != nil {
			return USDMFixtureEvidence{}, err
		}
		total += uint64(len(payload))
		if total > USDMMaxFixtureCorpusBytes {
			return USDMFixtureEvidence{}, fmt.Errorf("%w: corpus byte bound exceeded", ErrUSDMFixtureVerification)
		}
		payloads[entry.ID] = payload
		verified := USDMVerifiedFixture{ID: entry.ID, SHA256: digest, ByteLength: uint32(len(payload)), OfficialDerived: entry.Provenance == "official_derived", SyntheticMutation: entry.SyntheticMutation}
		evidence.Fixtures = append(evidence.Fixtures, verified)
		if verified.OfficialDerived {
			evidence.OfficialDerivedCount++
		} else {
			evidence.SyntheticMutationCount++
		}
	}
	slices.SortFunc(evidence.Fixtures, func(a, b USDMVerifiedFixture) int { return strings.Compare(a.ID, b.ID) })
	if err := verifyUSDMContracts(payloads, &evidence); err != nil {
		return USDMFixtureEvidence{}, err
	}
	return evidence, nil
}

func loadUSDMManifest(path string) (string, string, []byte, error) {
	if path == "" {
		return "", "", nil, fmt.Errorf("%w: explicit manifest path or root is required", ErrUSDMFixtureVerification)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", "", nil, fmt.Errorf("%w: manifest stat: %v", ErrUSDMFixtureVerification, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", "", nil, fmt.Errorf("%w: manifest root symlink is not allowed", ErrUSDMFixtureVerification)
	}
	manifestPath := path
	if info.IsDir() {
		manifestPath = filepath.Join(path, "manifest.json")
	}
	payload, err := readUSDMRegularFile(manifestPath, USDMMaxManifestBytes)
	if err != nil {
		return "", "", nil, err
	}
	return manifestPath, filepath.Dir(manifestPath), payload, nil
}

func readUSDMRegularFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maximum {
		return nil, fmt.Errorf("%w: fixture is not a bounded regular file", ErrUSDMFixtureVerification)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open fixture: %v", ErrUSDMFixtureVerification, err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(payload)) != info.Size() {
		return nil, fmt.Errorf("%w: fixture read changed or failed", ErrUSDMFixtureVerification)
	}
	return payload, nil
}

func validateUSDMManifest(manifest usdmFixtureManifest) error {
	if manifest.Version != USDMFixtureManifestVersion || manifest.AccessedAtNS != USDMAccessedAtNS || !manifest.SourceFaithful || len(manifest.Fixtures) != len(requiredUSDMFixtureIDs) || len(manifest.Fixtures) > USDMMaxFixtureCount {
		return fmt.Errorf("%w: manifest identity or fixture count", ErrUSDMFixtureVerification)
	}
	exactDocs := map[string]struct{}{USDMConnectionDocumentationURI: {}, USDMBookDocumentationURI: {}, USDMRESTDocumentationURI: {}, USDMStreamDocumentationURI: {}, USDMChangeLogURI: {}}
	if len(manifest.Documentation) != len(exactDocs) {
		return fmt.Errorf("%w: documentation allowlist mismatch", ErrUSDMFixtureVerification)
	}
	for _, document := range manifest.Documentation {
		if _, ok := exactDocs[document]; !ok {
			return fmt.Errorf("%w: undeclared documentation reference", ErrUSDMFixtureVerification)
		}
		delete(exactDocs, document)
	}
	seen := make(map[string]usdmFixtureManifestEntry, len(manifest.Fixtures))
	for _, entry := range manifest.Fixtures {
		if entry.ID == "" || entry.Path == "" || entry.Bytes == 0 || entry.Bytes > USDMMaxFixtureBytes || len(entry.SHA256) != sha256.Size*2 {
			return fmt.Errorf("%w: invalid fixture identity", ErrUSDMFixtureVerification)
		}
		if _, ok := seen[entry.ID]; ok {
			return fmt.Errorf("%w: duplicate fixture identity", ErrUSDMFixtureVerification)
		}
		if entry.Provenance == "official_derived" {
			if entry.SyntheticMutation || entry.SourceReference == "" || entry.DerivedFrom != "" {
				return fmt.Errorf("%w: invalid official-derived provenance", ErrUSDMFixtureVerification)
			}
		} else if entry.Provenance == "synthetic" {
			if !entry.SyntheticMutation || entry.DerivedFrom == "" || entry.SourceReference != "" {
				return fmt.Errorf("%w: synthetic mutation is not explicitly derived", ErrUSDMFixtureVerification)
			}
		} else {
			return fmt.Errorf("%w: unknown fixture provenance", ErrUSDMFixtureVerification)
		}
		seen[entry.ID] = entry
	}
	for _, id := range requiredUSDMFixtureIDs {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("%w: required fixture %s missing", ErrUSDMFixtureVerification, id)
		}
	}
	for _, entry := range seen {
		if entry.Provenance == "synthetic" {
			parent, ok := seen[entry.DerivedFrom]
			if !ok || parent.Provenance != "official_derived" {
				return fmt.Errorf("%w: synthetic fixture parent is not official-derived", ErrUSDMFixtureVerification)
			}
		}
	}
	return nil
}

func loadUSDMFixture(root string, entry usdmFixtureManifestEntry) ([]byte, [sha256.Size]byte, error) {
	clean := filepath.Clean(entry.Path)
	if clean != entry.Path || filepath.IsAbs(clean) || clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: fixture path escapes root", ErrUSDMFixtureVerification)
	}
	path := filepath.Join(root, clean)
	rootResolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: fixture root resolution", ErrUSDMFixtureVerification)
	}
	pathResolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: fixture path resolution", ErrUSDMFixtureVerification)
	}
	relative, err := filepath.Rel(rootResolved, pathResolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: resolved fixture escapes root", ErrUSDMFixtureVerification)
	}
	payload, err := readUSDMRegularFile(path, USDMMaxFixtureBytes)
	if err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	digest := sha256.Sum256(payload)
	want, err := hex.DecodeString(entry.SHA256)
	if err != nil || len(payload) != int(entry.Bytes) || !bytes.Equal(digest[:], want) {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: fixture digest or length mismatch for %s", ErrUSDMFixtureVerification, entry.ID)
	}
	return payload, digest, nil
}

func verifyUSDMContracts(payloads map[string][]byte, evidence *USDMFixtureEvidence) error {
	var routes struct {
		Streams []struct {
			Stream  string    `json:"stream"`
			Route   USDMRoute `json:"route"`
			Support string    `json:"support"`
		} `json:"streams"`
	}
	if json.Unmarshal(payloads["official.routing"], &routes) != nil || len(routes.Streams) == 0 {
		return ErrUSDMFixtureVerification
	}
	for _, route := range routes.Streams {
		contract, err := USDMValidateRoutedStream(route.Route, route.Stream)
		if err != nil || contract.Support != capture.SupportAvailable || route.Support != "supported" {
			return fmt.Errorf("%w: routed allowlist", ErrUSDMFixtureVerification)
		}
		evidence.Routing.AllowlistedStreams++
		if route.Route == USDMRoutePublic {
			evidence.Routing.PublicRouteStreams++
		} else {
			evidence.Routing.MarketRouteStreams++
		}
	}
	var wrongRoute struct {
		Stream string    `json:"stream"`
		Route  USDMRoute `json:"route"`
		Want   string    `json:"want_error"`
	}
	if json.Unmarshal(payloads["synthetic.wrong_route"], &wrongRoute) != nil {
		return ErrUSDMFixtureVerification
	}
	_, err := USDMValidateRoutedStream(wrongRoute.Route, wrongRoute.Stream)
	evidence.Routing.WrongRouteRejected = wrongRoute.Want == "wrong_route" && errors.Is(err, ErrUSDMWrongRoute)

	trade, err := ParseUSDMAggregateTrade(payloads["official.agg_trade"])
	if err != nil {
		return err
	}
	evidence.AggregateTradeRole, evidence.AggregateTradeCeilingNS = trade.NativeSourceRole, trade.AggregationCadenceCeilingNS
	evidence.QuantityQRetained = trade.Quantity == "0.252"
	evidence.QuantityNQRetained = trade.NormalQuantityExcludingRPI.State == normalize.SourceValue && trade.NormalQuantityExcludingRPI.Text == "0.250"
	var direction struct {
		Event         json.RawMessage `json:"event"`
		WantQ         string          `json:"want_q"`
		WantNQ        string          `json:"want_nq"`
		WantAggressor string          `json:"want_aggressor"`
	}
	if json.Unmarshal(payloads["synthetic.q_nq_direction"], &direction) != nil {
		return ErrUSDMFixtureVerification
	}
	mutatedTrade, err := ParseUSDMAggregateTrade(direction.Event)
	if err != nil || mutatedTrade.Quantity != direction.WantQ || mutatedTrade.NormalQuantityExcludingRPI.Text != direction.WantNQ || string(mutatedTrade.AggressorSide()) != direction.WantAggressor {
		return ErrUSDMFixtureVerification
	}

	exchangeInfo, err := ParseUSDMExchangeInfo(payloads["official.exchange_info"])
	if err != nil || len(exchangeInfo.Instruments) != 1 {
		return ErrUSDMFixtureVerification
	}
	pool, err := NewUSDMRatePoolFromExchangeInfo(exchangeInfo, 0)
	if err != nil {
		return err
	}
	first, err := pool.Acquire(0, USDMRESTExchangeInfo, 0)
	if err != nil {
		return err
	}
	second, err := pool.Acquire(0, USDMRESTOpenInterest, 0)
	if err != nil || !first.Allowed || !second.Allowed || second.RemainingTokens+1 != first.RemainingTokens {
		return ErrUSDMFixtureVerification
	}
	evidence.MetadataChecked, evidence.OpenInterest.SharedFAPIRatePoolChecked = true, true

	snapshot, err := ParseUSDMDepthSnapshot(payloads["official.depth_snapshot"])
	if err != nil {
		return err
	}
	update, err := ParseUSDMDepthUpdate(payloads["official.depth_update"])
	if err != nil {
		return err
	}
	book, _ := NewUSDMBookSynchronizer("BTCUSDT", 8)
	if _, err = book.ApplyUpdate(update); err != nil {
		return err
	}
	transition, err := book.Seed(snapshot)
	if err != nil || transition.State != USDMBookLive {
		return ErrUSDMFixtureVerification
	}
	quantity, _ := book.Level(normalize.SideBuy, "94000.00")
	_, deleted := book.Level(normalize.SideBuy, "93999.90")
	evidence.Book.SnapshotBridgeApplied = quantity == "1.200"
	evidence.Book.ZeroDeleteRetained = !deleted
	var gap struct {
		Next json.RawMessage `json:"next"`
		Want string          `json:"want"`
	}
	if json.Unmarshal(payloads["synthetic.pu_gap"], &gap) != nil {
		return ErrUSDMFixtureVerification
	}
	gapUpdate, err := ParseUSDMDepthUpdate(gap.Next)
	if err != nil {
		return err
	}
	gapTransition, gapErr := book.ApplyUpdate(gapUpdate)
	evidence.Book.PUContinuityEnforced = errors.Is(gapErr, ErrUSDMBookGap)
	evidence.Book.GapClosedEpoch = gap.Want == string(USDMBookGap) && gapTransition.State == USDMBookGap && gapTransition.ClosedEpoch != 0

	bbo, err := ParseUSDMBookTicker(payloads["official.book_ticker"])
	if err != nil {
		return err
	}
	ticker, err := ParseUSDMTicker24h(payloads["official.ticker"])
	if err != nil {
		return err
	}
	identity := normalize.InstrumentIdentity{InstrumentUID: "binance-usdm-btcusdt", NativeID: "BTCUSDT", BaseAssetID: "BTC", QuoteAssetID: "USDT"}
	derivative, err := ParseUSDMDerivativeTicker(payloads["official.mark_price"], 1767225600500000000, identity)
	if err != nil {
		return err
	}
	indexPrice, err := ParseUSDMIndexPriceUpdate(payloads["official.index_price"], 1767225600500000000, identity)
	if err != nil {
		return err
	}
	evidence.Ticker.NativeBBOChecked = bbo.RPIInclusion == "excluded"
	evidence.Ticker.GenericTickerChecked = ticker.NativeSourceRole == USDMRoleGenericTicker
	evidence.Ticker.DerivativeTickerChecked = derivative.MarkPrice.State == normalize.SourceValue && derivative.IndexPrice.State == normalize.SourceValue && derivative.FundingRate.State == normalize.SourceValue
	evidence.Ticker.IndependentOptionalFields = derivative.LastPrice.State == normalize.SourceMissing && derivative.SettlementPrice.State == normalize.SourceValue && derivative.Basis.State == normalize.SourceMissing
	evidence.Ticker.IndexPriceStreamChecked = indexPrice.NativeSourceRole == string(USDMRoleIndexPrice) && indexPrice.IndexPrice.State == normalize.SourceValue && indexPrice.MarkPrice.State == normalize.SourceMissing && indexPrice.FundingRate.State == normalize.SourceMissing && indexPrice.SettlementPrice.State == normalize.SourceMissing

	poll := USDMPollObservation{OperationID: "fixture-open-interest-1", PollCycleID: [16]byte{1}, Method: "GET", Path: USDMOpenInterestPath, Symbol: "BTCUSDT", ScheduledTimeNS: 1767225600400000000, RequestTimeNS: 1767225600450000000, ReceivedTimeNS: 1767225600600000000}
	oi, err := ParseUSDMOpenInterest(payloads["official.open_interest"], poll)
	if err != nil {
		return err
	}
	evidence.OpenInterest.RESTPollObservationChecked = oi.Normalized.State == normalize.SourceValue
	evidence.OpenInterest.NativeUnitRetained = oi.Normalized.Native.Unit.Kind == normalize.NativeUnitVenueUnspecified
	evidence.OpenInterest.SidednessUnspecified = oi.Normalized.Sidedness == normalize.OpenInterestUnspecified
	var wrongOI struct {
		PollSymbol string          `json:"poll_symbol"`
		Response   json.RawMessage `json:"response"`
	}
	if json.Unmarshal(payloads["synthetic.open_interest_wrong_symbol"], &wrongOI) != nil {
		return ErrUSDMFixtureVerification
	}
	wrongPoll := poll
	wrongPoll.Symbol = wrongOI.PollSymbol
	_, err = ParseUSDMOpenInterest(wrongOI.Response, wrongPoll)
	evidence.OpenInterest.WrongSymbolRejected = errors.Is(err, ErrUSDMInvalidPoll)

	liquidation, err := ParseUSDMLiquidation(payloads["official.liquidation"])
	if err != nil {
		return err
	}
	var lossy struct {
		Event   json.RawMessage `json:"event"`
		Want    string          `json:"want_completeness"`
		MustNot string          `json:"must_not_claim"`
	}
	if json.Unmarshal(payloads["synthetic.liquidation_completeness"], &lossy) != nil {
		return ErrUSDMFixtureVerification
	}
	mutatedLiquidation, err := ParseUSDMLiquidation(lossy.Event)
	if err != nil {
		return err
	}
	evidence.Liquidation.LargestPerSymbolWindowChecked = liquidation.Completeness == normalize.LiquidationLargestInWindow && string(mutatedLiquidation.Completeness) == lossy.Want
	evidence.Liquidation.OrderSideRetained = liquidation.Side == normalize.SideSell
	evidence.Liquidation.CompleteTapeClaimAbsent = lossy.MustNot == string(normalize.LiquidationComplete) && liquidation.Completeness != normalize.LiquidationComplete && mutatedLiquidation.Completeness != normalize.LiquidationComplete

	var rpi struct {
		Stream             string    `json:"stream"`
		ExpectedRoute      USDMRoute `json:"expected_route"`
		ExpectedSupport    string    `json:"expected_support"`
		RoutedPayloadProof bool      `json:"routed_payload_proof"`
		WantLive           string    `json:"want_live_subscription"`
	}
	if json.Unmarshal(payloads["synthetic.rpi_candidate"], &rpi) != nil {
		return ErrUSDMFixtureVerification
	}
	rpiContract, err := USDMStreamContractFor(rpi.Stream)
	if err != nil {
		return err
	}
	_, liveErr := USDMValidateRoutedStream(rpi.ExpectedRoute, rpi.Stream)
	evidence.RPI = USDMRPIEvidence{CandidateRoute: rpiContract.Route, SupportRemainsCandidate: rpi.ExpectedSupport == "candidate" && rpiContract.Support == capture.SupportAmbiguous, LiveSubscriptionRejected: rpi.WantLive == "rejected" && errors.Is(liveErr, ErrUSDMCandidateStream), RoutedPayloadProof: rpi.RoutedPayloadProof}

	if !completeUSDMFixtureEvidence(*evidence) {
		return ErrUSDMFixtureVerification
	}
	return nil
}

func completeUSDMFixtureEvidence(e USDMFixtureEvidence) bool {
	return e.Routing.WrongRouteRejected && e.QuantityQRetained && e.QuantityNQRetained && e.MetadataChecked && e.Book.SnapshotBridgeApplied && e.Book.PUContinuityEnforced && e.Book.GapClosedEpoch && e.Book.ZeroDeleteRetained && e.Ticker.NativeBBOChecked && e.Ticker.GenericTickerChecked && e.Ticker.DerivativeTickerChecked && e.Ticker.IndexPriceStreamChecked && e.Ticker.IndependentOptionalFields && e.OpenInterest.RESTPollObservationChecked && e.OpenInterest.NativeUnitRetained && e.OpenInterest.SidednessUnspecified && e.OpenInterest.WrongSymbolRejected && e.OpenInterest.SharedFAPIRatePoolChecked && e.Liquidation.LargestPerSymbolWindowChecked && e.Liquidation.OrderSideRetained && e.Liquidation.CompleteTapeClaimAbsent && e.RPI.CandidateRoute == USDMRoutePublic && e.RPI.SupportRemainsCandidate && e.RPI.LiveSubscriptionRejected && !e.RPI.RoutedPayloadProof
}
