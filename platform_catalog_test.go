package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/deribit"
	"github.com/enable-xyz/marketdata/hyperliquid"
	"github.com/enable-xyz/marketdata/okx"
)

const (
	platformTestAdapterVersion  = "platform-test-adapter-v1"
	platformTestValidityStartNS = int64(1_787_443_200_000_000_000)
)

func TestNewPlatformDeclarationTemplateCoversExactCandidates(t *testing.T) {
	template, err := NewPlatformDeclarationTemplate(platformTestAdapterVersion, platformTestValidityStartNS, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidateByKey, keys, err := indexedPlatformCandidates(platformTestAdapterVersion)
	if err != nil {
		t.Fatal(err)
	}
	if len(template.Tuples) != len(keys) {
		t.Fatalf("template tuple count = %d, want %d", len(template.Tuples), len(keys))
	}
	for _, key := range keys {
		candidate := candidateByKey[key]
		tupleEvidence, ok := template.Tuples[key]
		if !ok {
			t.Fatalf("template missing candidate %s", key)
		}
		candidateTransition, ok := tupleEvidence.Lifecycle[catalog.LifecycleCandidate]
		if !ok || candidateTransition.EffectiveAtNS != platformTestValidityStartNS {
			t.Fatalf("candidate transition %s = %+v", key, candidateTransition)
		}
		assertPlatformTemplateHash(t, candidateTransition.EvidenceSHA256, candidate)
		switch candidate.support {
		case capture.SupportAvailable:
			if len(tupleEvidence.Lifecycle) != 1 {
				t.Fatalf("available candidate %s has %d transitions", key, len(tupleEvidence.Lifecycle))
			}
		case capture.SupportUnsupported, capture.SupportAmbiguous:
			state := catalog.LifecycleUnsupported
			if candidate.support == capture.SupportAmbiguous {
				state = catalog.LifecycleAmbiguous
			}
			terminal, ok := tupleEvidence.Lifecycle[state]
			if !ok || terminal.EffectiveAtNS != platformTestValidityStartNS+1 || len(tupleEvidence.Lifecycle) != 2 {
				t.Fatalf("terminal candidate %s = %+v", key, tupleEvidence.Lifecycle)
			}
			assertPlatformTemplateHash(t, terminal.EvidenceSHA256, candidate)
		default:
			t.Fatalf("candidate %s has unknown support %d", key, candidate.support)
		}
	}
	if _, _, err := BuildPlatformDeclarations(template); err != nil {
		t.Fatalf("BuildPlatformDeclarations(template) error = %v", err)
	}
	second, err := NewPlatformDeclarationTemplate(platformTestAdapterVersion, platformTestValidityStartNS, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(template)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("template bytes are not deterministic")
	}
}

func TestNewPlatformDeclarationTemplateRequiresTerminalTimeInsideCoverage(t *testing.T) {
	tooShortEnd := platformTestValidityStartNS + 1
	if _, err := NewPlatformDeclarationTemplate(platformTestAdapterVersion, platformTestValidityStartNS, &tooShortEnd); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("short terminal interval error = %v", err)
	}
	validEnd := platformTestValidityStartNS + 2
	template, err := NewPlatformDeclarationTemplate(platformTestAdapterVersion, platformTestValidityStartNS, &validEnd)
	if err != nil {
		t.Fatal(err)
	}
	validEnd++
	if template.ValidityEndNS == nil || *template.ValidityEndNS != platformTestValidityStartNS+2 {
		t.Fatalf("template validity end aliases caller input: %v", template.ValidityEndNS)
	}
	if _, _, err := BuildPlatformDeclarations(template); err != nil {
		t.Fatalf("bounded template error = %v", err)
	}
}

func TestBuildPlatformDeclarationsAccountsForEveryVenueSupportRow(t *testing.T) {
	evidence := completePlatformTestEvidence(t)
	declarations, report, err := BuildPlatformDeclarations(evidence)
	if err != nil {
		t.Fatalf("BuildPlatformDeclarations() error = %v", err)
	}
	tuples := flattenPlatformTuples(declarations)
	if report.TupleCount != len(tuples) || report.DeclarationCount != len(declarations) || report.TupleCount == 0 {
		t.Fatalf("report counts = declarations %d tuples %d, actual %d/%d", report.DeclarationCount, report.TupleCount, len(declarations), len(tuples))
	}

	assertPlatformProduct(t, tuples, binance.SpotSourceID, "spot")
	assertPlatformProduct(t, tuples, binance.USDMSourceID, "usdm")
	assertPlatformProduct(t, tuples, binance.CoinMSourceID, "coinm")
	for _, category := range []bybit.Category{bybit.Spot, bybit.Linear, bybit.Inverse} {
		assertPlatformProduct(t, tuples, category.SourceID(), string(category))
		matrix, err := bybit.SupportMatrix(category)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range matrix {
			want := 1
			if row.Role == bybit.RoleInstrumentMetadata {
				want = 2
			}
			assertPlatformRoleCount(t, tuples, category.SourceID(), string(category), string(row.Role), want)
		}
		instrumentContract, err := bybit.InstrumentSourceContract(category)
		if err != nil {
			t.Fatal(err)
		}
		assertCapabilityRowsPresent(t, tuples, string(category), instrumentContract)
	}
	assertPlatformProduct(t, tuples, bybit.OptionSourceID, "option")
	for _, row := range bybit.OptionSupportMatrix() {
		assertPlatformRoleCount(t, tuples, bybit.OptionSourceID, "option", string(row.Role), 1)
	}

	for _, product := range []okx.InstrumentType{okx.Spot, okx.Swap, okx.Futures, okx.Option} {
		for _, row := range okx.SupportMatrix() {
			assertPlatformRoleCount(t, tuples, "", string(product), string(row.Role), 1)
		}
	}
	for _, row := range deribit.SupportMatrix() {
		assertPlatformRoleCount(t, tuples, deribit.SourceID, "v2", string(row.Role), 3)
	}

	hyperliquidFamilies := []struct {
		product string
		family  hyperliquid.Family
		dex     string
	}{
		{product: "main", family: hyperliquid.MainPerpetual},
		{product: "spot", family: hyperliquid.Spot},
		{product: "hip-3", family: hyperliquid.HIP3, dex: "xyz"},
	}
	for _, item := range hyperliquidFamilies {
		contract, err := hyperliquid.PublicSourceContract(hyperliquid.Mainnet, item.family, item.dex)
		if err != nil {
			t.Fatal(err)
		}
		matrix, err := hyperliquid.SupportMatrix(item.family)
		if err != nil {
			t.Fatal(err)
		}
		infoContract, err := hyperliquid.InfoSourceContract(hyperliquid.Mainnet, item.family, item.dex)
		if err != nil {
			t.Fatal(err)
		}
		assertCapabilityRowsPresent(t, tuples, item.product, contract)
		assertCapabilityRowsPresent(t, tuples, item.product, infoContract)
		for _, row := range matrix {
			want := 1
			if row.Role == hyperliquid.RoleAssetContext {
				want = 2
			}
			assertPlatformRoleCount(t, tuples, contract.SourceID, item.product, string(row.Role), want)
		}
	}

	spotSource, _, spotChannels := binance.SpotCatalogContract()
	assertCatalogChannelsPresent(t, tuples, spotSource.ProductFamily, spotSource.SourceID, spotChannels)
	usdmSource, _, usdmChannels := binance.USDMCatalogContract()
	assertCatalogChannelsPresent(t, tuples, usdmSource.ProductFamily, usdmSource.SourceID, usdmChannels)
	coinmSource, _, coinmChannels := binance.CoinMCatalogContract()
	assertCatalogChannelsPresent(t, tuples, coinmSource.ProductFamily, coinmSource.SourceID, coinmChannels)
	assertCapabilityRowsPresent(t, tuples, "spot", binance.SpotWSSourceContract())
	assertCapabilityRowsPresent(t, tuples, "usdm", binance.USDMPublicSourceContract())
	assertCapabilityRowsPresent(t, tuples, "usdm", binance.USDMMarketSourceContract())
	assertCapabilityRowsPresent(t, tuples, "coinm", binance.CoinMSourceContract())
}

func TestBuildPlatformDeclarationsKeepsTerminalRowsAndExactLimitations(t *testing.T) {
	declarations, _, err := BuildPlatformDeclarations(completePlatformTestEvidence(t))
	if err != nil {
		t.Fatal(err)
	}
	tuples := flattenPlatformTuples(declarations)
	candidates, err := platformCandidates(platformTestAdapterVersion)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range candidates {
		var want catalog.TupleLifecycle
		switch candidate.support {
		case capture.SupportUnsupported:
			want = catalog.LifecycleUnsupported
		case capture.SupportAmbiguous:
			want = catalog.LifecycleAmbiguous
		default:
			continue
		}
		key, err := candidate.series.Key()
		if err != nil {
			t.Fatal(err)
		}
		tuple := tupleBySeriesKey(t, declarations, key)
		if tuple.State != want || tuple.Limitation != candidate.limitation || len(tuple.TransitionHistory) != 2 {
			t.Fatalf("terminal tuple %s = state %s limitation %q history %d", key, tuple.State, tuple.Limitation, len(tuple.TransitionHistory))
		}
	}

	assertTerminalPlatformRole(t, tuples, bybit.Spot.SourceID(), "spot", string(bybit.RoleDerivativeTicker), catalog.LifecycleUnsupported,
		"Spot ticker is snapshot-only and has no derivative state contract")
	assertTerminalPlatformRole(t, tuples, "", string(okx.Spot), string(okx.RoleRPIBook), catalog.LifecycleAmbiguous,
		"distinct reconstructable snapshot-plus-incremental RPI stream; never merged with regular liquidity and not promoted without caller access evidence")
	assertTerminalPlatformRole(t, tuples, "", "spot", string(hyperliquid.RoleFundingHistory), catalog.LifecycleUnsupported,
		"funding is a perpetual-only family")
	assertTerminalPlatformRole(t, tuples, "", "hip-3", string(hyperliquid.RoleStrictEconomicUnits), catalog.LifecycleAmbiguous,
		"contract-generation economic units are provisional and excluded from strict normalized totals")
}

func TestBuildPlatformDeclarationsPreservesCadenceDepthAndEntitlement(t *testing.T) {
	declarations, _, err := BuildPlatformDeclarations(completePlatformTestEvidence(t))
	if err != nil {
		t.Fatal(err)
	}
	tuples := flattenPlatformTuples(declarations)
	assertPlatformTupleDetail(t, tuples, binance.SpotSourceID, "spot", binance.SpotDepthChannel, "book-snapshot", "depth=5000", "public")
	assertPlatformTupleDetail(t, tuples, binance.USDMSourceID, "usdm", "@depth@250ms", string(binance.USDMRoleDiffDepth), "cadence_ceiling_ns=250000000", "public")
	assertPlatformTupleDetail(t, tuples, binance.CoinMSourceID, "coinm", "@depth@500ms", string(binance.CoinMRoleDiffDepth), "cadence_ceiling_ns=500000000", "public")
	assertPlatformTupleDetail(t, tuples, deribit.SourceID, "v2", "book.{instrument}.raw", "full_book_l2", "raw;fallback=100ms", "authorized_raw_1ms_aggregation")

	foundOptionDepth := false
	foundVIP := false
	for _, tuple := range tuples {
		granularity := platformTupleGranularity(t, tuple)
		if tuple.SourceID == bybit.OptionSourceID && granularity.Role == string(bybit.RoleBoundedOrderbook) {
			foundOptionDepth = tuple.Limitation == "only documented option depths 25 and 100; snapshots replace state"
		}
		if granularity.Product == string(okx.Spot) && granularity.Role == string(okx.RoleVIPBook50) {
			foundVIP = tuple.Entitlement == "login_plus_vip4" && tuple.Limitation == "10 ms entitlement row; no silent downgrade"
		}
	}
	if !foundOptionDepth || !foundVIP {
		t.Fatalf("depth/entitlement limitations preserved = option:%t vip:%t", foundOptionDepth, foundVIP)
	}
}

func TestBuildPlatformDeclarationsRequiresConsecutivePromotionAndOperationalProof(t *testing.T) {
	base := completePlatformTestEvidence(t)
	key := firstAvailablePlatformKey(t)
	tupleEvidence := base.Tuples[key]
	tupleEvidence.Lifecycle = map[catalog.TupleLifecycle]PlatformLifecycleEvidence{
		catalog.LifecycleCandidate:     platformTestTransition(key+":candidate", platformTestValidityStartNS),
		catalog.LifecycleCaptured:      platformTestTransition(key+":captured", platformTestValidityStartNS+1),
		catalog.LifecycleReplayable:    platformTestTransition(key+":replayable", platformTestValidityStartNS+2),
		catalog.LifecycleNormalized:    platformTestTransition(key+":normalized", platformTestValidityStartNS+3),
		catalog.LifecycleReconstructed: platformTestTransition(key+":reconstructed", platformTestValidityStartNS+4),
		catalog.LifecycleSupported:     platformTestTransition(key+":supported", platformTestValidityStartNS+5),
	}
	base.Tuples[key] = tupleEvidence
	if _, _, err := BuildPlatformDeclarations(base); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "operational-gate") {
		t.Fatalf("missing operational proof error = %v", err)
	}

	tupleEvidence.OperationalGateSHA256 = platformTestHash(key + ":gate")
	base.Tuples[key] = tupleEvidence
	if _, _, err := BuildPlatformDeclarations(base); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "canary") {
		t.Fatalf("missing canary proof error = %v", err)
	}

	tupleEvidence.CanarySHA256 = platformTestHash(key + ":canary-26h")
	base.Tuples[key] = tupleEvidence
	declarations, _, err := BuildPlatformDeclarations(base)
	if err != nil {
		t.Fatalf("supported declaration error = %v", err)
	}
	got := tupleBySeriesKey(t, declarations, key)
	if got.State != catalog.LifecycleSupported || len(got.TransitionHistory) != 6 {
		t.Fatalf("supported tuple lifecycle = %s history=%d", got.State, len(got.TransitionHistory))
	}
	for i, state := range []catalog.TupleLifecycle{catalog.LifecycleCandidate, catalog.LifecycleCaptured, catalog.LifecycleReplayable, catalog.LifecycleNormalized, catalog.LifecycleReconstructed, catalog.LifecycleSupported} {
		if got.TransitionHistory[i].State != state {
			t.Fatalf("transition %d = %s, want %s", i, got.TransitionHistory[i].State, state)
		}
	}

	gap := completePlatformTestEvidence(t)
	gapEvidence := gap.Tuples[key]
	gapEvidence.Lifecycle[catalog.LifecycleReplayable] = platformTestTransition(key+":replayable-without-capture", platformTestValidityStartNS+2)
	gap.Tuples[key] = gapEvidence
	if _, _, err := BuildPlatformDeclarations(gap); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "missing prior state") {
		t.Fatalf("promotion-gap error = %v", err)
	}

	nonMonotonic := completePlatformTestEvidence(t)
	nonMonotonicEvidence := nonMonotonic.Tuples[key]
	nonMonotonicEvidence.Lifecycle[catalog.LifecycleCaptured] = platformTestTransition(key+":captured-at-candidate-time", platformTestValidityStartNS)
	nonMonotonic.Tuples[key] = nonMonotonicEvidence
	if _, _, err := BuildPlatformDeclarations(nonMonotonic); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "strictly later") {
		t.Fatalf("non-monotonic transition error = %v", err)
	}
}

func TestBuildPlatformDeclarationsRejectsMissingAndUnknownEvidence(t *testing.T) {
	missing := completePlatformTestEvidence(t)
	key := firstMapKey(missing.Tuples)
	delete(missing.Tuples, key)
	if _, _, err := BuildPlatformDeclarations(missing); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "missing source-contract candidate evidence") {
		t.Fatalf("missing candidate error = %v", err)
	}

	unknown := completePlatformTestEvidence(t)
	unknownSeries := PlatformTupleSeriesIdentity{SourceID: "unknown", APIVersion: "v1", Entitlement: "public", ChannelOrEndpoint: "unknown", DataFamily: "unknown", NativeGranularity: "unknown", AdapterVersion: platformTestAdapterVersion}
	unknownKey, err := unknownSeries.Key()
	if err != nil {
		t.Fatal(err)
	}
	unknown.Tuples[unknownKey] = PlatformTupleEvidence{Lifecycle: map[catalog.TupleLifecycle]PlatformLifecycleEvidence{
		catalog.LifecycleCandidate: platformTestTransition("unknown", platformTestValidityStartNS),
	}}
	if _, _, err := BuildPlatformDeclarations(unknown); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "unknown tuple") {
		t.Fatalf("unknown tuple error = %v", err)
	}

	wrongStart := completePlatformTestEvidence(t)
	wrongKey := firstMapKey(wrongStart.Tuples)
	wrongTupleEvidence := wrongStart.Tuples[wrongKey]
	candidateEvidence := wrongTupleEvidence.Lifecycle[catalog.LifecycleCandidate]
	candidateEvidence.EffectiveAtNS++
	wrongTupleEvidence.Lifecycle[catalog.LifecycleCandidate] = candidateEvidence
	wrongStart.Tuples[wrongKey] = wrongTupleEvidence
	if _, _, err := BuildPlatformDeclarations(wrongStart); !errors.Is(err, ErrInvalidPlatformDeclaration) || !strings.Contains(err.Error(), "validity start") {
		t.Fatalf("candidate effective time error = %v", err)
	}
}

func TestBuildPlatformDeclarationsStableUnderEvidenceMapOrder(t *testing.T) {
	first := completePlatformTestEvidence(t)
	keys := make([]string, 0, len(first.Tuples))
	for key := range first.Tuples {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	secondMap := make(map[string]PlatformTupleEvidence, len(keys))
	for i := len(keys) - 1; i >= 0; i-- {
		secondMap[keys[i]] = first.Tuples[keys[i]]
	}
	second := first
	second.Tuples = secondMap

	firstDeclarations, firstReport, err := BuildPlatformDeclarations(first)
	if err != nil {
		t.Fatal(err)
	}
	secondDeclarations, secondReport, err := BuildPlatformDeclarations(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstReport.SHA256 != secondReport.SHA256 || !slices.Equal(firstReport.TupleIDs, secondReport.TupleIDs) {
		t.Fatalf("reports differ by map order: %s/%s", firstReport.SHA256, secondReport.SHA256)
	}
	firstJSON, err := json.Marshal(firstDeclarations)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(secondDeclarations)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("declaration order changed with evidence map order")
	}
}

func TestPlatformDeclarationEvidenceHasNoVenueWideSupportBoolean(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[PlatformDeclarationEvidence](), reflect.TypeFor[PlatformTupleEvidence]()} {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Type.Kind() == reflect.Bool || strings.EqualFold(field.Name, "supported") || strings.EqualFold(field.Name, "support") {
				t.Fatalf("%s exposes venue-wide support field %s", typ.Name(), field.Name)
			}
		}
	}
}

func completePlatformTestEvidence(t *testing.T) PlatformDeclarationEvidence {
	t.Helper()
	evidence, err := NewPlatformDeclarationTemplate(platformTestAdapterVersion, platformTestValidityStartNS, nil)
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func firstAvailablePlatformKey(t *testing.T) string {
	t.Helper()
	candidates, err := platformCandidates(platformTestAdapterVersion)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range candidates {
		if candidate.support == capture.SupportAvailable {
			key, err := candidate.series.Key()
			if err != nil {
				t.Fatal(err)
			}
			return key
		}
	}
	t.Fatal("no available platform candidate")
	return ""
}

func platformTestHash(material string) string {
	digest := sha256.Sum256([]byte(material))
	return hex.EncodeToString(digest[:])
}

func assertPlatformTemplateHash(t *testing.T, got string, candidate platformCandidate) {
	t.Helper()
	material, err := json.Marshal(platformTemplateEvidenceMaterial{
		Series: candidate.series, Support: candidate.support, Limitation: candidate.limitation,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(material)
	want := hex.EncodeToString(digest[:])
	if got != want {
		t.Fatalf("template evidence hash = %s, want %s", got, want)
	}
}

func platformTestTransition(material string, effectiveAtNS int64) PlatformLifecycleEvidence {
	return PlatformLifecycleEvidence{EvidenceSHA256: platformTestHash(material), EffectiveAtNS: effectiveAtNS}
}

func flattenPlatformTuples(declarations []catalog.DeclaredSource) []catalog.DeclaredTuple {
	var tuples []catalog.DeclaredTuple
	for _, declaration := range declarations {
		tuples = append(tuples, declaration.Tuples...)
	}
	return tuples
}

func platformTupleGranularity(t *testing.T, tuple catalog.DeclaredTuple) platformGranularity {
	t.Helper()
	var granularity platformGranularity
	if err := json.Unmarshal([]byte(tuple.NativeGranularity), &granularity); err != nil {
		t.Fatalf("native granularity %q: %v", tuple.NativeGranularity, err)
	}
	return granularity
}

func assertPlatformProduct(t *testing.T, tuples []catalog.DeclaredTuple, sourceID, product string) {
	t.Helper()
	for _, tuple := range tuples {
		if tuple.SourceID == sourceID && platformTupleGranularity(t, tuple).Product == product {
			return
		}
	}
	t.Fatalf("missing source/product %s/%s", sourceID, product)
}

func assertPlatformRoleCount(t *testing.T, tuples []catalog.DeclaredTuple, sourceID, product, role string, want int) {
	t.Helper()
	got := 0
	for _, tuple := range tuples {
		granularity := platformTupleGranularity(t, tuple)
		if (sourceID == "" || tuple.SourceID == sourceID) && granularity.Product == product && granularity.Role == role {
			got++
		}
	}
	if got != want {
		t.Fatalf("role count source=%q product=%q role=%q = %d, want %d", sourceID, product, role, got, want)
	}
}

func assertTerminalPlatformRole(t *testing.T, tuples []catalog.DeclaredTuple, sourceID, product, role string, state catalog.TupleLifecycle, limitation string) {
	t.Helper()
	for _, tuple := range tuples {
		granularity := platformTupleGranularity(t, tuple)
		if (sourceID == "" || tuple.SourceID == sourceID) && granularity.Product == product && granularity.Role == role {
			if tuple.State != state || tuple.Limitation != limitation {
				t.Fatalf("terminal row %s/%s = state %s limitation %q", product, role, tuple.State, tuple.Limitation)
			}
			return
		}
	}
	t.Fatalf("missing terminal row %s/%s", product, role)
}

func assertCatalogChannelsPresent(t *testing.T, tuples []catalog.DeclaredTuple, product, sourceID string, channels []catalog.ChannelContract) {
	t.Helper()
	for _, channel := range channels {
		found := false
		for _, tuple := range tuples {
			if tuple.SourceID == sourceID && tuple.ChannelOrEndpoint == channel.ChannelID && tuple.DataFamily == channel.DataFamily && platformTupleGranularity(t, tuple).Product == product {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing catalog channel %s/%s", product, channel.ChannelID)
		}
	}
}

func assertCapabilityRowsPresent(t *testing.T, tuples []catalog.DeclaredTuple, product string, contract capture.SourceContract) {
	t.Helper()
	for _, capability := range contract.Capabilities {
		found := false
		for _, tuple := range tuples {
			if tuple.SourceID == contract.SourceID && tuple.APIVersion == contract.APIVersion && tuple.ChannelOrEndpoint == capability.ChannelOrEndpoint && tuple.DataFamily == capability.DataFamily && tuple.Entitlement == capability.Entitlement && platformTupleGranularity(t, tuple).Product == product {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing source capability %s/%s/%s", product, capability.ChannelOrEndpoint, capability.DataFamily)
		}
	}
}

func assertPlatformTupleDetail(t *testing.T, tuples []catalog.DeclaredTuple, sourceID, product, channel, family, cadenceFragment, entitlement string) {
	t.Helper()
	for _, tuple := range tuples {
		granularity := platformTupleGranularity(t, tuple)
		if tuple.SourceID == sourceID && granularity.Product == product && tuple.ChannelOrEndpoint == channel &&
			tuple.DataFamily == family && strings.Contains(granularity.Cadence, cadenceFragment) && tuple.Entitlement == entitlement {
			return
		}
	}
	t.Fatalf("missing exact tuple detail %s/%s/%s cadence %q entitlement %q", sourceID, product, channel, cadenceFragment, entitlement)
}

func tupleBySeriesKey(t *testing.T, declarations []catalog.DeclaredSource, want string) catalog.DeclaredTuple {
	t.Helper()
	for _, tuple := range flattenPlatformTuples(declarations) {
		series := PlatformTupleSeriesIdentity{
			SourceID: tuple.SourceID, APIVersion: tuple.APIVersion, Entitlement: tuple.Entitlement,
			ChannelOrEndpoint: tuple.ChannelOrEndpoint, DataFamily: tuple.DataFamily,
			NativeGranularity: tuple.NativeGranularity, AdapterVersion: tuple.AdapterVersion,
		}
		key, err := series.Key()
		if err != nil {
			t.Fatal(err)
		}
		if key == want {
			return tuple
		}
	}
	t.Fatalf("missing tuple key %s", want)
	return catalog.DeclaredTuple{}
}

func firstMapKey[V any](values map[string]V) string {
	for key := range values {
		return key
	}
	return ""
}

func TestRunCheckPlatformCatalogEmitsFullVerifiedDeclarations(t *testing.T) {
	evidence, err := NewPlatformDeclarationTemplate(platformTestAdapterVersion, platformTestValidityStartNS, nil)
	if err != nil {
		t.Fatal(err)
	}
	declarations, report, err := BuildPlatformDeclarations(evidence)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(platformCatalogTemplateDocument{SchemaVersion: 1, Evidence: evidence, Report: report})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "platform-evidence.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runCheckPlatformCatalog(t.Context(), path, report.SHA256, &output); err != nil {
		t.Fatal(err)
	}
	var document platformCatalogDocument
	if err := decodeStrictJSON(output.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.SchemaVersion != 1 || !reflect.DeepEqual(document.Declarations, declarations) ||
		!reflect.DeepEqual(document.Report, report) || !validSHA256Hex(document.EvidenceSHA256) {
		t.Fatalf("platform catalog document = %#v", document)
	}
	claimed := document.EvidenceSHA256
	document.EvidenceSHA256 = ""
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if claimed != hex.EncodeToString(digest[:]) {
		t.Fatalf("document hash = %s", claimed)
	}
	if err := runCheckPlatformCatalog(t.Context(), path, strings.Repeat("a", 64), &bytes.Buffer{}); err == nil {
		t.Fatal("wrong platform report hash was accepted")
	}
	var templateOutput bytes.Buffer
	if err := writePlatformCatalogTemplate(t.Context(), platformTestAdapterVersion, platformTestValidityStartNS, nil, &templateOutput); err != nil {
		t.Fatal(err)
	}
	var template platformCatalogTemplateDocument
	if err := decodeStrictJSON(templateOutput.Bytes(), &template); err != nil {
		t.Fatal(err)
	}
	if template.SchemaVersion != 1 || !reflect.DeepEqual(template.Evidence, evidence) || !reflect.DeepEqual(template.Report, report) {
		t.Fatalf("platform template = %#v", template)
	}
}
