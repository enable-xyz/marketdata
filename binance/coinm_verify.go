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

	"github.com/enable-xyz/marketdata/normalize"
)

const (
	CoinMFixtureManifestVersion uint16 = 1
	CoinMMaxFixtureCount               = 32
	CoinMMaxManifestBytes              = 64 << 10
	CoinMMaxFixtureBytes               = 1 << 20
	CoinMMaxFixtureCorpusBytes         = 16 << 20
)

var ErrCoinMFixtureVerification = errors.New("binance: COIN-M fixture verification failed")
var coinMReviewedFixtureManifestSHA256 = [sha256.Size]byte{
	0x7c, 0xf0, 0x69, 0x7e, 0xdf, 0x75, 0x81, 0x4d,
	0xb5, 0x91, 0x17, 0xe6, 0x36, 0xa0, 0xf9, 0x24,
	0xef, 0x8a, 0xcf, 0xe0, 0x5e, 0x99, 0x17, 0x9a,
	0x74, 0x20, 0xa1, 0xce, 0x47, 0x24, 0xaf, 0x1e,
}

type CoinMVerifiedFixture struct {
	ID                string
	SHA256            [sha256.Size]byte
	ByteLength        uint32
	OfficialDerived   bool
	SyntheticMutation bool
}

type CoinMFixtureEvidence struct {
	Version                 uint16
	ManifestSHA256          [sha256.Size]byte
	AccessedAtNS            int64
	OfficialDerivedCount    int
	SyntheticMutationCount  int
	Fixtures                []CoinMVerifiedFixture
	DistinctSource          bool
	DAPIContract            bool
	CoinMST2Routed          bool
	USDMST1Routed           bool
	InconsistencyRejected   bool
	RejectedRawRetained     bool
	RejectedRawSHA256       [sha256.Size]byte
	TradeContracts          bool
	BookContracts           bool
	BookZeroDelete          bool
	BBOContracts            bool
	TickerContractsAndBase  bool
	OpenInterestContracts   bool
	TemporalVersionsChecked bool
	OldVersionRejected      bool
	ContractSizeChanged     bool
	InversePayoffRequired   bool
	USDNotionalV1           normalize.Decimal
	BaseNotionalV1          normalize.Decimal
	USDNotionalV2           normalize.Decimal
	BaseNotionalV2          normalize.Decimal
	OpenInterestUSD         normalize.Decimal
	DeliveryFundingEmpty    bool
	DeliveryFundingZero     bool
	EmptyAndZeroDistinct    bool
	ZeroFundingTimeRetained bool
}

type coinMFixtureManifest struct {
	Version        uint16                      `json:"version"`
	AccessedAtNS   int64                       `json:"accessed_at_ns"`
	SourceFaithful bool                        `json:"source_faithful"`
	Documentation  []string                    `json:"documentation"`
	Fixtures       []coinMFixtureManifestEntry `json:"fixtures"`
}

type coinMFixtureManifestEntry struct {
	ID                string `json:"id"`
	Path              string `json:"path"`
	SHA256            string `json:"sha256"`
	Bytes             uint32 `json:"bytes"`
	Provenance        string `json:"provenance"`
	SourceReference   string `json:"source_reference"`
	DerivedFrom       string `json:"derived_from"`
	SyntheticMutation bool   `json:"synthetic_mutation"`
}

var requiredCoinMFixtureIDs = []string{
	"official.agg_trade", "official.book_ticker", "official.delivery_funding_empty",
	"official.depth_snapshot", "official.depth_update", "official.exchange_info",
	"official.merged_inconsistency", "official.merged_routing", "official.open_interest",
	"official.ticker", "synthetic.contract_size_change", "synthetic.delivery_funding_zero",
	"synthetic.payoff_mismatch",
}

// VerifyCoinMFixtures verifies an explicit immutable ELMD-016 fixture manifest
// or root. It performs no network access and cannot succeed on hashes alone.
func VerifyCoinMFixtures(manifestPathOrRoot string) (CoinMFixtureEvidence, error) {
	root, manifestBytes, err := loadCoinMManifest(manifestPathOrRoot)
	if err != nil {
		return CoinMFixtureEvidence{}, err
	}
	manifestSHA256 := sha256.Sum256(manifestBytes)
	if manifestSHA256 != coinMReviewedFixtureManifestSHA256 {
		return CoinMFixtureEvidence{}, coinMVerificationError("manifest identity is not the reviewed fixture corpus")
	}
	var manifest coinMFixtureManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return CoinMFixtureEvidence{}, coinMVerificationError("malformed manifest")
	}
	if err := validateCoinMManifest(manifest); err != nil {
		return CoinMFixtureEvidence{}, err
	}
	payloads := make(map[string][]byte, len(manifest.Fixtures))
	evidence := CoinMFixtureEvidence{Version: CoinMFixtureManifestVersion, ManifestSHA256: manifestSHA256, AccessedAtNS: manifest.AccessedAtNS}
	var total uint64
	for _, entry := range manifest.Fixtures {
		payload, digest, err := loadCoinMFixture(root, entry)
		if err != nil {
			return CoinMFixtureEvidence{}, err
		}
		total += uint64(len(payload))
		if total > CoinMMaxFixtureCorpusBytes {
			return CoinMFixtureEvidence{}, coinMVerificationError("corpus byte bound exceeded")
		}
		payloads[entry.ID] = payload
		fixture := CoinMVerifiedFixture{ID: entry.ID, SHA256: digest, ByteLength: uint32(len(payload)), OfficialDerived: entry.Provenance == "official_derived", SyntheticMutation: entry.SyntheticMutation}
		evidence.Fixtures = append(evidence.Fixtures, fixture)
		if fixture.OfficialDerived {
			evidence.OfficialDerivedCount++
		} else {
			evidence.SyntheticMutationCount++
		}
	}
	slices.SortFunc(evidence.Fixtures, func(left, right CoinMVerifiedFixture) int { return strings.Compare(left.ID, right.ID) })
	if err := verifyCoinMSemantics(payloads, &evidence); err != nil {
		return CoinMFixtureEvidence{}, err
	}
	if !completeCoinMFixtureEvidence(evidence) {
		return CoinMFixtureEvidence{}, coinMVerificationError("semantic evidence incomplete")
	}
	return evidence, nil
}

func loadCoinMManifest(path string) (string, []byte, error) {
	if path == "" {
		return "", nil, coinMVerificationError("explicit manifest path or root is required")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, coinMVerificationError("manifest root is unavailable or a symlink")
	}
	manifestPath := path
	if info.IsDir() {
		manifestPath = filepath.Join(path, "manifest.json")
	}
	payload, err := readCoinMRegularFile(manifestPath, CoinMMaxManifestBytes)
	if err != nil {
		return "", nil, err
	}
	return filepath.Dir(manifestPath), payload, nil
}

func readCoinMRegularFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maximum {
		return nil, coinMVerificationError("fixture is not a bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, coinMVerificationError("open fixture failed")
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(payload)) != info.Size() {
		return nil, coinMVerificationError("fixture read changed or failed")
	}
	return payload, nil
}

func validateCoinMManifest(manifest coinMFixtureManifest) error {
	if manifest.Version != CoinMFixtureManifestVersion || manifest.AccessedAtNS != CoinMAccessedAtNS || !manifest.SourceFaithful || len(manifest.Fixtures) != len(requiredCoinMFixtureIDs) || len(manifest.Fixtures) > CoinMMaxFixtureCount {
		return coinMVerificationError("manifest identity or fixture count")
	}
	docs := map[string]struct{}{CoinMConnectionDocumentationURI: {}, CoinMBookDocumentationURI: {}, CoinMIntegrationNoticeURI: {}, CoinMRESTDocumentationURI: {}, CoinMStreamDocumentationURI: {}}
	if len(manifest.Documentation) != len(docs) {
		return coinMVerificationError("documentation allowlist mismatch")
	}
	for _, document := range manifest.Documentation {
		if _, ok := docs[document]; !ok {
			return coinMVerificationError("undeclared documentation reference")
		}
		delete(docs, document)
	}
	seen := make(map[string]coinMFixtureManifestEntry, len(manifest.Fixtures))
	for _, entry := range manifest.Fixtures {
		if entry.ID == "" || entry.Path == "" || entry.Bytes == 0 || entry.Bytes > CoinMMaxFixtureBytes || len(entry.SHA256) != sha256.Size*2 {
			return coinMVerificationError("invalid fixture identity")
		}
		if _, ok := seen[entry.ID]; ok {
			return coinMVerificationError("duplicate fixture identity")
		}
		if entry.Provenance == "official_derived" {
			if entry.SyntheticMutation || entry.SourceReference == "" || entry.DerivedFrom != "" {
				return coinMVerificationError("invalid official-derived provenance")
			}
		} else if entry.Provenance == "synthetic" {
			if !entry.SyntheticMutation || entry.DerivedFrom == "" || entry.SourceReference != "" {
				return coinMVerificationError("invalid synthetic provenance")
			}
		} else {
			return coinMVerificationError("unknown fixture provenance")
		}
		seen[entry.ID] = entry
	}
	for _, id := range requiredCoinMFixtureIDs {
		if _, ok := seen[id]; !ok {
			return coinMVerificationError("required fixture missing")
		}
	}
	for _, entry := range seen {
		if entry.Provenance == "synthetic" {
			parent, ok := seen[entry.DerivedFrom]
			if !ok || parent.Provenance != "official_derived" {
				return coinMVerificationError("synthetic parent is not official-derived")
			}
		}
	}
	return nil
}

func loadCoinMFixture(root string, entry coinMFixtureManifestEntry) ([]byte, [sha256.Size]byte, error) {
	clean := filepath.Clean(entry.Path)
	if clean != entry.Path || filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, [sha256.Size]byte{}, coinMVerificationError("fixture path escapes root")
	}
	current := root
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, [sha256.Size]byte{}, coinMVerificationError("fixture path contains a symlink or is unavailable")
		}
	}
	rootResolved, rootErr := filepath.EvalSymlinks(root)
	pathResolved, pathErr := filepath.EvalSymlinks(current)
	relative, relErr := filepath.Rel(rootResolved, pathResolved)
	if rootErr != nil || pathErr != nil || relErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, [sha256.Size]byte{}, coinMVerificationError("resolved fixture escapes root")
	}
	payload, err := readCoinMRegularFile(current, CoinMMaxFixtureBytes)
	if err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	digest := sha256.Sum256(payload)
	want, err := hex.DecodeString(entry.SHA256)
	if err != nil || len(payload) != int(entry.Bytes) || !bytes.Equal(digest[:], want) {
		return nil, [sha256.Size]byte{}, coinMVerificationError("fixture digest or length mismatch")
	}
	return payload, digest, nil
}

func verifyCoinMSemantics(payloads map[string][]byte, evidence *CoinMFixtureEvidence) error {
	if CoinMSourceID == USDMSourceID {
		return coinMVerificationError("source identity aliases USD-M")
	}
	ws := CoinMSourceContract()
	rest, err := CoinMRESTSourceContract(CoinMRESTOpenInterest, 0)
	source, version, channels := CoinMCatalogContract()
	if err != nil || ws.Validate() != nil || ws.SourceID != CoinMSourceID || rest.SourceID != CoinMSourceID || source.SourceID != CoinMSourceID || source.ProductFamily != "coinm" || version.OfficialAPIVersion == "" || len(channels) != 2 {
		return coinMVerificationError("source, DAPI or catalog contract invalid")
	}
	evidence.DistinctSource, evidence.DAPIContract = true, true

	routes, err := RouteCoinMMergedRecords(payloads["official.merged_routing"])
	if err != nil || len(routes) != 2 {
		return coinMVerificationError("merged routing failed")
	}
	for _, route := range routes {
		evidence.CoinMST2Routed = evidence.CoinMST2Routed || route.NativeSymbolType == 2 && route.SourceID == CoinMSourceID
		evidence.USDMST1Routed = evidence.USDMST1Routed || route.NativeSymbolType == 1 && route.SourceID == USDMSourceID
	}
	inconsistent, err := RouteCoinMMergedRecords(payloads["official.merged_inconsistency"])
	if err != nil || len(inconsistent) != 1 || inconsistent[0].Route != CoinMMergedRouteRejected || inconsistent[0].Rejection != "st_symbol_family_inconsistency" {
		return coinMVerificationError("official inconsistency not rejected")
	}
	var wrapper struct {
		Stream string            `json:"stream"`
		Data   []json.RawMessage `json:"data"`
	}
	if json.Unmarshal(payloads["official.merged_inconsistency"], &wrapper) != nil || len(wrapper.Data) != 1 || !bytes.Equal(inconsistent[0].Raw, wrapper.Data[0]) {
		return coinMVerificationError("rejected bytes not retained")
	}
	evidence.InconsistencyRejected, evidence.RejectedRawRetained = true, true
	evidence.RejectedRawSHA256 = sha256.Sum256(inconsistent[0].Raw)

	perpetual := normalize.InstrumentIdentity{InstrumentUID: "binance-coinm:BTCUSD_PERP:0", NativeID: "BTCUSD_PERP", BaseAssetID: "BTC", QuoteAssetID: "USD"}
	delivery := normalize.InstrumentIdentity{InstrumentUID: "binance-coinm:BTCUSD_201225:0", NativeID: "BTCUSD_201225", BaseAssetID: "BTC", QuoteAssetID: "USD"}
	infoV1, err := ParseCoinMExchangeInfo(payloads["official.exchange_info"])
	if err != nil || len(infoV1.Instruments) != 1 {
		return coinMVerificationError("official contract metadata failed")
	}
	infoV2, err := ParseCoinMExchangeInfo(payloads["synthetic.contract_size_change"])
	if err != nil || len(infoV2.Instruments) != 1 {
		return coinMVerificationError("changed contract metadata failed")
	}
	fromV1, fromV2 := infoV1.ServerTimeMS*1_000_000, infoV2.ServerTimeMS*1_000_000
	termsV1, err := infoV1.Instruments[0].ContractTerms(perpetual.InstrumentUID, "coinm-catalog-v1", fromV1, normalize.OptionalInt64{Valid: true, Value: fromV2}, normalize.CoinMPayoffInverseQuote)
	if err != nil {
		return coinMVerificationError("first terms failed")
	}
	termsV2, err := infoV2.Instruments[0].ContractTerms(perpetual.InstrumentUID, "coinm-catalog-v2", fromV2, normalize.OptionalInt64{}, normalize.CoinMPayoffInverseQuote)
	if err != nil || termsV1.ContractSize == termsV2.ContractSize {
		return coinMVerificationError("second terms failed")
	}

	trade, err := ParseCoinMAggregateTrade(payloads["official.agg_trade"], perpetual)
	if err != nil || trade.SourceID != CoinMSourceID || trade.Contracts.Unit.Kind != normalize.NativeUnitContracts {
		return coinMVerificationError("trade contracts failed")
	}
	convertedV1, err := trade.ContractConversion(fromV1, termsV1)
	if err != nil {
		return coinMVerificationError("first conversion failed")
	}
	convertedV2, err := trade.ContractConversion(fromV2, termsV2)
	if err != nil {
		return coinMVerificationError("second conversion failed")
	}
	if _, err := trade.ContractConversion(fromV2, termsV1); !errors.Is(err, normalize.ErrInvalidCoinMContract) {
		return coinMVerificationError("expired terms accepted")
	}
	if convertedV1.DerivedUSD.Field.Value.Decimal.Coefficient != "200000000000000000000" || convertedV1.DerivedBase.Field.Value.Decimal.Coefficient != "10000000000000000" || convertedV2.DerivedUSD.Field.Value.Decimal.Coefficient != "20000000000000000000" || convertedV2.DerivedBase.Field.Value.Decimal.Coefficient != "1000000000000000" {
		return coinMVerificationError("conversion values wrong")
	}
	evidence.TradeContracts, evidence.TemporalVersionsChecked, evidence.OldVersionRejected, evidence.ContractSizeChanged = true, true, true, true
	evidence.USDNotionalV1, evidence.BaseNotionalV1 = convertedV1.DerivedUSD.Field.Value.Decimal, convertedV1.DerivedBase.Field.Value.Decimal
	evidence.USDNotionalV2, evidence.BaseNotionalV2 = convertedV2.DerivedUSD.Field.Value.Decimal, convertedV2.DerivedBase.Field.Value.Decimal

	var mismatch struct {
		InstrumentUID  string `json:"instrument_uid"`
		CatalogVersion string `json:"catalog_version"`
		ValidFromNS    int64  `json:"valid_from_ns"`
		ContractSize   string `json:"contract_size"`
		Payoff         string `json:"payoff"`
		Expected       string `json:"expected"`
	}
	if coinMUnmarshalBoundedStrict(payloads["synthetic.payoff_mismatch"], &mismatch) != nil || mismatch.Expected != "reject" || mismatch.ValidFromNS != fromV1 || mismatch.ContractSize != "100" {
		return coinMVerificationError("payoff fixture malformed")
	}
	badTerms, badErr := infoV1.Instruments[0].ContractTerms(mismatch.InstrumentUID, mismatch.CatalogVersion, mismatch.ValidFromNS, normalize.OptionalInt64{}, normalize.CoinMPayoffKind(mismatch.Payoff))
	if badErr == nil || badTerms != (normalize.CoinMContractTerms{}) || !errors.Is(badErr, normalize.ErrInvalidCoinMContract) {
		return coinMVerificationError("unproven payoff accepted")
	}
	evidence.InversePayoffRequired = true

	snapshot, err := ParseCoinMDepthSnapshot(payloads["official.depth_snapshot"], perpetual)
	if err != nil || snapshot.Bids[0].Contracts.Unit.Kind != normalize.NativeUnitContracts {
		return coinMVerificationError("depth snapshot failed")
	}
	update, err := ParseCoinMDepthUpdate(payloads["official.depth_update"], perpetual)
	if err != nil || update.Bids[0].Contracts.Unit.Kind != normalize.NativeUnitContracts || !update.Asks[0].Contracts.Decimal.IsZero() {
		return coinMVerificationError("depth update failed")
	}
	book, err := NewCoinMBookSynchronizer(perpetual.NativeID, 16)
	if err != nil {
		return coinMVerificationError("book construction failed")
	}
	_, applyErr := book.ApplyUpdate(update)
	transition, seedErr := book.Seed(snapshot)
	_, askPresent := book.Level(normalize.SideSell, "20001")
	if applyErr != nil || seedErr != nil || transition.State != CoinMBookLive || askPresent {
		return coinMVerificationError("book bridge or zero delete failed")
	}
	evidence.BookContracts, evidence.BookZeroDelete = true, true

	bbo, err := ParseCoinMBookTicker(payloads["official.book_ticker"], perpetual)
	if err != nil || bbo.BidContracts.Unit.Kind != normalize.NativeUnitContracts || bbo.AskContracts.Unit.Kind != normalize.NativeUnitContracts {
		return coinMVerificationError("BBO contracts failed")
	}
	evidence.BBOContracts = true
	ticker, err := ParseCoinMTicker24h(payloads["official.ticker"], perpetual)
	if err != nil || ticker.ContractVolume.Unit.Kind != normalize.NativeUnitContracts || ticker.BaseAssetVolume.Unit.Kind != normalize.NativeUnitBaseAsset {
		return coinMVerificationError("ticker units failed")
	}
	evidence.TickerContractsAndBase = true

	poll := CoinMPollObservation{OperationID: "coinm-oi-1", PollCycleID: [16]byte{1}, Method: "GET", Path: CoinMOpenInterestPath, Symbol: perpetual.NativeID, ScheduledTimeNS: fromV1, RequestTimeNS: fromV1, ReceivedTimeNS: fromV1}
	oi, err := ParseCoinMOpenInterest(payloads["official.open_interest"], poll, perpetual, termsV1)
	if err != nil || oi.Normalized.Native.Unit.Kind != normalize.NativeUnitContracts || oi.Normalized.DerivedBase.Field.State != normalize.SourceMissing || oi.Normalized.DerivedUSD.Field.State != normalize.SourceValue {
		return coinMVerificationError("open-interest units failed")
	}
	evidence.OpenInterestContracts, evidence.OpenInterestUSD = true, oi.Normalized.DerivedUSD.Field.Value.Decimal

	receivedNS := int64(1596095725000) * 1_000_000
	empty, err := ParseCoinMDerivativeTicker(payloads["official.delivery_funding_empty"], receivedNS, delivery)
	if err != nil || empty.FundingRate.State != normalize.SourceEmpty || empty.NextFundingTime.State != normalize.SourceValue || empty.NextFundingTime.ValueNS != 0 {
		return coinMVerificationError("delivery empty funding failed")
	}
	zero, err := ParseCoinMDerivativeTicker(payloads["synthetic.delivery_funding_zero"], receivedNS, delivery)
	if err != nil || zero.FundingRate.State != normalize.SourceValue || !zero.FundingRate.Value.Decimal.IsZero() || zero.NextFundingTime.ValueNS != 0 {
		return coinMVerificationError("delivery zero funding failed")
	}
	evidence.DeliveryFundingEmpty, evidence.DeliveryFundingZero, evidence.ZeroFundingTimeRetained = true, true, true
	evidence.EmptyAndZeroDistinct = empty.FundingRate.State != zero.FundingRate.State
	return nil
}

func completeCoinMFixtureEvidence(e CoinMFixtureEvidence) bool {
	return e.DistinctSource && e.DAPIContract && e.CoinMST2Routed && e.USDMST1Routed && e.InconsistencyRejected && e.RejectedRawRetained && e.RejectedRawSHA256 != ([sha256.Size]byte{}) && e.TradeContracts && e.BookContracts && e.BookZeroDelete && e.BBOContracts && e.TickerContractsAndBase && e.OpenInterestContracts && e.TemporalVersionsChecked && e.OldVersionRejected && e.ContractSizeChanged && e.InversePayoffRequired && e.USDNotionalV1 != (normalize.Decimal{}) && e.BaseNotionalV1 != (normalize.Decimal{}) && e.USDNotionalV2 != (normalize.Decimal{}) && e.BaseNotionalV2 != (normalize.Decimal{}) && e.OpenInterestUSD != (normalize.Decimal{}) && e.DeliveryFundingEmpty && e.DeliveryFundingZero && e.EmptyAndZeroDistinct && e.ZeroFundingTimeRetained
}

func coinMVerificationError(message string) error {
	return fmt.Errorf("%w: %s", ErrCoinMFixtureVerification, message)
}
