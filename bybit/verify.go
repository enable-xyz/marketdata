package bybit

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
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	FixtureManifestVersion = 1
	EvidenceVersion        = 1
	maximumFixtureCount    = 64
	maximumFixtureBytes    = 1 << 20
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
	SourceSection  string `json:"source_section,omitempty"`
	DerivedFrom    string `json:"derived_from,omitempty"`
	ByteLength     uint32 `json:"byte_length"`
	SHA256         string `json:"sha256"`
}

type EvidenceCheck struct {
	Name       string   `json:"name"`
	FixtureIDs []string `json:"fixture_ids"`
}

type EvidenceSummary struct {
	Version            uint16          `json:"version"`
	Venue              string          `json:"venue"`
	AccessDate         string          `json:"access_date"`
	ManifestSHA256     string          `json:"manifest_sha256"`
	FixtureCount       uint32          `json:"fixture_count"`
	PrimarySourceCount uint32          `json:"primary_source_count"`
	SyntheticCount     uint32          `json:"synthetic_count"`
	Checks             []EvidenceCheck `json:"checks"`
	EvidenceSHA256     string          `json:"evidence_sha256"`
}

type manifestRoleContract struct {
	ID             string
	Classification string
}

var manifestRoleContracts = map[string]manifestRoleContract{
	"trade":               {ID: "trade", Classification: "primary_source_value_projection"},
	"full_delta":          {ID: "full-delta", Classification: "primary_source_value_projection"},
	"rpi_delta":           {ID: "rpi-delta", Classification: "primary_source_value_projection"},
	"ticker_official":     {ID: "ticker-official", Classification: "primary_source_value_projection"},
	"liquidation_array":   {ID: "liquidation-array", Classification: "primary_source_value_projection"},
	"metadata":            {ID: "instrument-linear", Classification: "primary_source_value_projection"},
	"bounded_snapshot":    {ID: "bounded-snapshot", Classification: "synthetic_parseable_projection"},
	"bounded_gap_delta":   {ID: "bounded-gap", Classification: "synthetic_sequence_mutation"},
	"full_next_delta":     {ID: "full-next", Classification: "synthetic_sequence_continuation"},
	"full_gap_delta":      {ID: "full-gap", Classification: "synthetic_sequence_mutation"},
	"rpi_snapshot":        {ID: "rpi-snapshot", Classification: "synthetic_snapshot_projection"},
	"ticker_oi_snapshot":  {ID: "ticker-oi", Classification: "synthetic_changelog_extension"},
	"ticker_sparse_delta": {ID: "ticker-sparse", Classification: "synthetic_sparse_delta"},
	"liquidation_object":  {ID: "liquidation-object", Classification: "synthetic_documented_schema_shape"},
}

type verifiedFixture struct {
	entry   FixtureEntry
	payload []byte
}

// VerifyFixtures is the deterministic coordinator hook for `verify venue`.
// The one caller-supplied argument is either an explicit manifest path or its
// containing root. Only regular, digest-bound files declared by that manifest
// are read; symlinks, traversal, undeclared discovery, and network access are rejected.
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
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil || manifest.Version != FixtureManifestVersion || manifest.Venue != "bybit-v5" || manifest.AccessDate == "" || len(manifest.Fixtures) == 0 || len(manifest.Fixtures) > maximumFixtureCount {
		return EvidenceSummary{}, fmt.Errorf("%w: invalid manifest", ErrFixtureBoundary)
	}
	fixtures := make(map[string]verifiedFixture, len(manifest.Fixtures))
	ids := make(map[string]struct{}, len(manifest.Fixtures))
	roles := make(map[string]struct{}, len(manifest.Fixtures))
	primary, synthetic := uint32(0), uint32(0)
	for _, entry := range manifest.Fixtures {
		if entry.ID == "" || entry.Role == "" || entry.File == "" || entry.ByteLength == 0 || len(entry.SHA256) != sha256.Size*2 {
			return EvidenceSummary{}, fmt.Errorf("%w: incomplete fixture entry", ErrFixtureBoundary)
		}
		if _, ok := ids[entry.ID]; ok {
			return EvidenceSummary{}, fmt.Errorf("%w: duplicate fixture ID %s", ErrFixtureBoundary, entry.ID)
		}
		ids[entry.ID] = struct{}{}
		if _, ok := roles[entry.Role]; ok {
			return EvidenceSummary{}, fmt.Errorf("%w: duplicate fixture role %s", ErrFixtureBoundary, entry.Role)
		}
		roles[entry.Role] = struct{}{}
		roleContract, ok := manifestRoleContracts[entry.Role]
		if !ok || entry.ID != roleContract.ID || entry.Classification != roleContract.Classification {
			return EvidenceSummary{}, fmt.Errorf("%w: fixture role identity mismatch for %s", ErrFixtureBoundary, entry.Role)
		}
		if strings.HasPrefix(entry.Classification, "synthetic_") {
			if entry.DerivedFrom == "" {
				return EvidenceSummary{}, fmt.Errorf("%w: synthetic fixture %s lacks label/provenance", ErrFixtureBoundary, entry.ID)
			}
			synthetic++
		} else if entry.Classification == "primary_source_value_projection" && entry.SourceURL != "" {
			primary++
		} else {
			return EvidenceSummary{}, fmt.Errorf("%w: invalid classification for %s", ErrFixtureBoundary, entry.ID)
		}
		clean := filepath.Clean(entry.File)
		if clean != entry.File || filepath.IsAbs(clean) || clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return EvidenceSummary{}, fmt.Errorf("%w: fixture path traversal", ErrFixtureBoundary)
		}
		path := filepath.Join(root, clean)
		if err := ensureFixtureContained(canonicalRoot, path); err != nil {
			return EvidenceSummary{}, err
		}
		payload, readErr := readRegularBounded(path, maximumFixtureBytes)
		if readErr != nil {
			return EvidenceSummary{}, readErr
		}
		digest := sha256.Sum256(payload)
		if len(payload) != int(entry.ByteLength) || hex.EncodeToString(digest[:]) != entry.SHA256 {
			return EvidenceSummary{}, fmt.Errorf("%w: immutable fixture mismatch for %s", ErrFixtureBoundary, entry.ID)
		}
		fixtures[entry.Role] = verifiedFixture{entry: entry, payload: payload}
	}
	if len(fixtures) != len(manifestRoleContracts) {
		return EvidenceSummary{}, fmt.Errorf("%w: incomplete fixture role set", ErrFixtureBoundary)
	}
	checks, err := verifyFixtureContracts(fixtures)
	if err != nil {
		return EvidenceSummary{}, err
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	summary := EvidenceSummary{Version: EvidenceVersion, Venue: manifest.Venue, AccessDate: manifest.AccessDate, ManifestSHA256: hex.EncodeToString(manifestDigest[:]), FixtureCount: uint32(len(manifest.Fixtures)), PrimarySourceCount: primary, SyntheticCount: synthetic, Checks: checks}
	material, err := json.Marshal(summary)
	if err != nil {
		return EvidenceSummary{}, err
	}
	evidenceDigest := sha256.Sum256(material)
	summary.EvidenceSHA256 = hex.EncodeToString(evidenceDigest[:])
	return summary, nil
}

func verifyFixtureContracts(fixtures map[string]verifiedFixture) ([]EvidenceCheck, error) {
	need := func(role string) (verifiedFixture, error) {
		fixture, ok := fixtures[role]
		if !ok {
			return verifiedFixture{}, fmt.Errorf("%w: missing role %s", ErrFixtureBoundary, role)
		}
		return fixture, nil
	}
	for _, category := range []Category{Spot, Linear, Inverse} {
		contract, err := PublicSourceContract(category)
		if err != nil || contract.Validate() != nil || contract.SourceID != category.SourceID() || contract.ContractID == "" || contract.Topology.Transport != capture.TransportWebSocket {
			return nil, fmt.Errorf("%w: %s source contract", ErrFixtureBoundary, category)
		}
	}
	spotLiquidation, _ := Supports(Spot, RoleAllLiquidation)
	if spotLiquidation.Support != capture.SupportUnsupported || Spot.PublicEndpoint() == Linear.PublicEndpoint() || Linear.PublicEndpoint() == Inverse.PublicEndpoint() {
		return nil, fmt.Errorf("%w: source/socket role separation", ErrFixtureBoundary)
	}
	tradeFixture, err := need("trade")
	if err != nil {
		return nil, err
	}
	if trades, parseErr := ParsePublicTrades(Linear, tradeFixture.payload); parseErr != nil || len(trades) != 1 {
		return nil, fmt.Errorf("%w: trade contract", ErrFixtureBoundary)
	}
	boundedSnapshotFixture, err := need("bounded_snapshot")
	if err != nil {
		return nil, err
	}
	boundedGapFixture, err := need("bounded_gap_delta")
	if err != nil {
		return nil, err
	}
	boundedSnapshot, err := ParseBoundedOrderbook(Spot, boundedSnapshotFixture.payload)
	if err != nil {
		return nil, err
	}
	boundedGap, err := ParseBoundedOrderbook(Spot, boundedGapFixture.payload)
	if err != nil {
		return nil, err
	}
	bounded, _ := NewBoundedBook(Spot, "BTCUSDT", 50)
	if bounded.Apply(boundedSnapshot) != nil || bounded.Apply(boundedGap) != nil || bounded.Snapshot().UpdateID != 500 {
		return nil, fmt.Errorf("%w: bounded book incorrectly imposed full sequencing", ErrFixtureBoundary)
	}
	fullFixture, err := need("full_delta")
	if err != nil {
		return nil, err
	}
	fullNextFixture, err := need("full_next_delta")
	if err != nil {
		return nil, err
	}
	fullGapFixture, err := need("full_gap_delta")
	if err != nil {
		return nil, err
	}
	fullFirst, err := ParseFullOrderbookDelta(Linear, fullFixture.payload)
	if err != nil {
		return nil, err
	}
	fullNext, err := ParseFullOrderbookDelta(Linear, fullNextFixture.payload)
	if err != nil {
		return nil, err
	}
	fullGap, err := ParseFullOrderbookDelta(Linear, fullGapFixture.payload)
	if err != nil {
		return nil, err
	}
	full, _ := NewFullBook(Linear, "MNTUSDT")
	if full.Accept(fullFirst) != nil || full.Accept(fullNext) != nil || full.Seed(FullBookSnapshot{Category: Linear, Symbol: "MNTUSDT", UpdateID: fullFirst.UpdateID, CrossSequence: fullFirst.CrossSequence, Bids: fullFirst.Bids, Asks: fullFirst.Asks, MaximumLevelsPerSide: 10000}) != nil || !errors.Is(full.Accept(fullGap), ErrFullBookGap) {
		return nil, fmt.Errorf("%w: full book sequencing", ErrFixtureBoundary)
	}
	rpiFixture, err := need("rpi_snapshot")
	if err != nil {
		return nil, err
	}
	officialRPIFixture, err := need("rpi_delta")
	if err != nil {
		return nil, err
	}
	rpiMessage, err := ParseRPIOrderbook(Spot, rpiFixture.payload)
	if err != nil {
		return nil, err
	}
	officialRPIMessage, err := ParseRPIOrderbook(Spot, officialRPIFixture.payload)
	if err != nil || officialRPIMessage.Kind != BookDelta {
		return nil, fmt.Errorf("%w: official RPI delta", ErrFixtureBoundary)
	}
	rpi, _ := NewRPIBook(Spot, "BTCUSDT")
	if rpi.Apply(rpiMessage) != nil || rpi.Apply(officialRPIMessage) != nil {
		return nil, fmt.Errorf("%w: RPI source role", ErrFixtureBoundary)
	}
	rpiSnapshot := rpi.Snapshot()
	if rpiSnapshot.Bids["121975.1"].RPIAmount != "0" || rpiSnapshot.Bids["121960.5"].RPIAmount != "0.163986" || bounded.Snapshot().RPIIncluded {
		return nil, fmt.Errorf("%w: RPI source role", ErrFixtureBoundary)
	}
	units := TickerUnitContract{
		Price:             normalize.SpotPriceUnit("BTC", "USDT"),
		OpenInterestSize:  normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "bybit_open_interest_size"},
		OpenInterestValue: normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "bybit_open_interest_value"},
	}
	officialTickerFixture, err := need("ticker_official")
	if err != nil {
		return nil, err
	}
	officialTicker, _ := NewDerivativeTickerState(Linear, "BTCUSDT", "official-epoch")
	if err := officialTicker.Apply(officialTickerFixture.payload); err != nil {
		return nil, fmt.Errorf("%w: official ticker snapshot", ErrFixtureBoundary)
	}
	officialMetadata, err := officialTickerFixtureMetadata(officialTickerFixture.payload)
	if err != nil {
		return nil, fmt.Errorf("%w: official ticker metadata: %v", ErrFixtureBoundary, err)
	}
	officialEvent, err := officialTicker.Normalized(officialMetadata, units)
	if err != nil {
		return nil, fmt.Errorf("%w: official ticker normalization: %v", ErrFixtureBoundary, err)
	}
	officialFields := officialTicker.Fields()
	if officialFields.LastPrice.Text != "66666.60" ||
		officialFields.MarkPrice.Text != "66666.60" ||
		officialFields.IndexPrice.Text != "115418.19" ||
		officialFields.OpenInterest.Text != "492373.72" ||
		officialFields.OpenInterestValue.Text != "32824881841.75" ||
		officialFields.FundingRate.Text != "-0.005" ||
		officialFields.NextFundingTime.Text != "1760342400000" {
		return nil, fmt.Errorf("%w: official ticker values", ErrFixtureBoundary)
	}
	if officialEvent.LastPrice.State != normalize.SourceValue ||
		officialEvent.MarkPrice.State != normalize.SourceValue ||
		officialEvent.IndexPrice.State != normalize.SourceValue ||
		officialEvent.FundingRate.State != normalize.SourceValue ||
		officialEvent.NextFundingTime.State != normalize.SourceValue ||
		officialEvent.NextFundingTime.ValueNS != 1760342400000*int64(1e6) ||
		officialEvent.NextFundingTime.Resolution != normalize.ResolutionMillisecond ||
		len(officialEvent.OpenInterest) != 4 ||
		officialEvent.OpenInterest[0].State != normalize.SourceValue ||
		officialEvent.OpenInterest[1].State != normalize.SourceValue ||
		officialEvent.OpenInterest[2].State != normalize.SourceMissing ||
		officialEvent.OpenInterest[3].State != normalize.SourceMissing {
		return nil, fmt.Errorf("%w: official ticker normalized fields", ErrFixtureBoundary)
	}
	tickerSnapshotFixture, err := need("ticker_oi_snapshot")
	if err != nil {
		return nil, err
	}
	tickerDeltaFixture, err := need("ticker_sparse_delta")
	if err != nil {
		return nil, err
	}
	ticker, _ := NewDerivativeTickerState(Linear, "BTCUSDT", "epoch-1")
	if ticker.Apply(tickerSnapshotFixture.payload) != nil {
		return nil, fmt.Errorf("%w: ticker seed", ErrFixtureBoundary)
	}
	mark := ticker.Fields().MarkPrice
	if ticker.Apply(tickerDeltaFixture.payload) != nil || ticker.Fields().MarkPrice != mark || ticker.Fields().FundingRate.State != normalize.SourceEmpty {
		return nil, fmt.Errorf("%w: sparse ticker state", ErrFixtureBoundary)
	}
	oi, err := ticker.OpenInterest(1760325053000*int64(1e6), units)
	if err != nil || len(oi) != 4 || oi[0].Sidedness != normalize.OpenInterestBothSides || oi[2].Sidedness != normalize.OpenInterestSingleSide || oi[0].Native.Decimal == oi[2].Native.Decimal {
		return nil, fmt.Errorf("%w: OI meanings substituted", ErrFixtureBoundary)
	}
	if ticker.BeginConnection("epoch-2") != nil || !errors.Is(ticker.Apply(tickerDeltaFixture.payload), ErrUnseededTicker) {
		return nil, fmt.Errorf("%w: reconnect leaked ticker state", ErrFixtureBoundary)
	}
	arrayFixture, err := need("liquidation_array")
	if err != nil {
		return nil, err
	}
	objectFixture, err := need("liquidation_object")
	if err != nil {
		return nil, err
	}
	arrayBatch, err := ParseAllLiquidation(Linear, arrayFixture.payload)
	if err != nil {
		return nil, err
	}
	objectBatch, err := ParseAllLiquidation(Linear, objectFixture.payload)
	if err != nil || arrayBatch.DataShape != LiquidationDataArray || objectBatch.DataShape != LiquidationDataObject {
		return nil, fmt.Errorf("%w: liquidation object/array drift", ErrFixtureBoundary)
	}
	if _, err := ParseAllLiquidation(Spot, arrayFixture.payload); !errors.Is(err, ErrUnsupportedRole) {
		return nil, fmt.Errorf("%w: Spot liquidation support leak", ErrFixtureBoundary)
	}
	metadataFixture, err := need("metadata")
	if err != nil {
		return nil, err
	}
	if page, err := ParseInstrumentInfo(Linear, metadataFixture.payload); err != nil || len(page.Instruments) != 1 {
		return nil, fmt.Errorf("%w: metadata contract", ErrFixtureBoundary)
	}
	return []EvidenceCheck{
		{Name: "distinct_spot_linear_inverse_sources_and_sockets"},
		{Name: "public_trade_contract", FixtureIDs: verifiedFixtureIDs(tradeFixture)},
		{Name: "bounded_and_full_book_non_interchangeability", FixtureIDs: verifiedFixtureIDs(boundedSnapshotFixture, boundedGapFixture, fullFixture, fullNextFixture, fullGapFixture)},
		{Name: "regular_and_rpi_roles_separate", FixtureIDs: verifiedFixtureIDs(rpiFixture, officialRPIFixture)},
		{Name: "official_sparse_ticker_seed_empty_and_reconnect_reset", FixtureIDs: verifiedFixtureIDs(officialTickerFixture, tickerSnapshotFixture, tickerDeltaFixture)},
		{Name: "both_and_single_side_open_interest_distinct", FixtureIDs: verifiedFixtureIDs(tickerSnapshotFixture)},
		{Name: "derivatives_only_liquidation_object_array_drift", FixtureIDs: verifiedFixtureIDs(arrayFixture, objectFixture)},
		{Name: "instrument_metadata", FixtureIDs: verifiedFixtureIDs(metadataFixture)},
	}, nil
}

func verifiedFixtureIDs(fixtures ...verifiedFixture) []string {
	ids := make([]string, len(fixtures))
	for i, fixture := range fixtures {
		ids[i] = fixture.entry.ID
	}
	return ids
}

func officialTickerFixtureMetadata(payload []byte) (normalize.Metadata, error) {
	const (
		sourceTimeNS   int64 = 1760325052630 * 1_000_000
		receivedTimeNS int64 = 1760325053000 * 1_000_000
	)
	epoch := [16]byte{1}
	envelope := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindWebSocket,
		SourceID:                   Linear.SourceID(),
		ChannelOrEndpoint:          "tickers.BTCUSDT",
		ConnectionEpoch:            capture.OptionalEpoch{Value: epoch, Valid: true},
		ArrivalOrdinal:             1,
		ExchangeTimeNS:             capture.OptionalInt64{Value: sourceTimeNS, Valid: true},
		ExchangeTimeResolution:     capture.ExchangeTimeMillisecond,
		ReceivedWallTimeNS:         receivedTimeNS,
		ClockEpochID:               "bybit-fixture-verification",
		MonotonicNSSinceClockEpoch: 1,
		PayloadEncoding:            capture.PayloadEncodingJSON,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "bybit-fixture-verifier-v1",
	}
	envelope.SetRawPayload(payload)
	record, err := normalize.BindRawRecord(
		envelope,
		normalize.Hash(sha256.Sum256([]byte("bybit-v5-official-ticker-fixture-segment-v1"))),
		1,
		nil,
	)
	if err != nil {
		return normalize.Metadata{}, err
	}
	return normalize.NewMetadata(normalize.MetadataInput{
		Record:                  record,
		SchemaName:              normalize.DerivativeTickerSchemaName,
		SchemaVersion:           normalize.DerivativeTickerSchemaVersion,
		InstrumentUID:           "bybit-v5-linear-btcusdt",
		ExchangeTimeNS:          normalize.OptionalInt64{Value: sourceTimeNS, Valid: true},
		ExchangeTimeResolution:  normalize.ResolutionMillisecond,
		SourceEventTimeNS:       normalize.OptionalInt64{Value: sourceTimeNS, Valid: true},
		SourceTimeResolution:    normalize.ResolutionMillisecond,
		SourceSchemaFingerprint: normalize.Hash(sha256.Sum256([]byte("bybit-v5-ticker-schema-v1"))),
		MapperVersion:           "bybit-v5-ticker-fixture-verifier-v1",
		MapperBindingID:         normalize.Hash(sha256.Sum256([]byte("bybit-v5-ticker-fixture-binding-v1"))),
		CatalogSnapshotID:       normalize.Hash(sha256.Sum256([]byte("bybit-v5-ticker-fixture-catalog-v1"))),
	})
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
