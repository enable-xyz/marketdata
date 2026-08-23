package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/deribit"
	"github.com/enable-xyz/marketdata/hyperliquid"
	"github.com/enable-xyz/marketdata/okx"
)

var ErrInvalidPlatformDeclaration = errors.New("platform catalog: invalid declaration evidence")

const noAdditionalPlatformLimitation = "source contract declares no additional tuple limitation"

// PlatformTupleSeriesIdentity is the coverage-independent identity used as an
// evidence-map key. Key returns its canonical JSON representation.
type PlatformTupleSeriesIdentity struct {
	SourceID          string `json:"source_id"`
	APIVersion        string `json:"api_version"`
	Entitlement       string `json:"entitlement"`
	ChannelOrEndpoint string `json:"channel_or_endpoint"`
	DataFamily        string `json:"data_family"`
	NativeGranularity string `json:"native_granularity"`
	AdapterVersion    string `json:"adapter_version"`
}

func (s PlatformTupleSeriesIdentity) Key() (string, error) {
	encoded, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("%w: encode tuple series identity: %v", ErrInvalidPlatformDeclaration, err)
	}
	return string(encoded), nil
}

// PlatformLifecycleEvidence is caller-owned immutable evidence for one exact
// lifecycle transition.
type PlatformLifecycleEvidence struct {
	EvidenceSHA256 string `json:"evidence_sha256"`
	EffectiveAtNS  int64  `json:"effective_at_ns"`
}

// PlatformTupleEvidence contains caller-owned immutable evidence for one exact
// tuple series. Lifecycle may only describe consecutive lifecycle states.
type PlatformTupleEvidence struct {
	Lifecycle             map[catalog.TupleLifecycle]PlatformLifecycleEvidence `json:"lifecycle"`
	OperationalGateSHA256 string                                               `json:"operational_gate_sha256,omitempty"`
	CanarySHA256          string                                               `json:"canary_sha256,omitempty"`
}

// PlatformDeclarationEvidence binds every declaration to one explicit validity
// interval and adapter version. Tuples is keyed by PlatformTupleSeriesIdentity.Key.
type PlatformDeclarationEvidence struct {
	ValidityStartNS int64                            `json:"validity_start_ns"`
	ValidityEndNS   *int64                           `json:"validity_end_ns,omitempty"`
	AdapterVersion  string                           `json:"adapter_version"`
	Tuples          map[string]PlatformTupleEvidence `json:"tuples"`
}

type platformCandidate struct {
	series     PlatformTupleSeriesIdentity
	support    capture.SupportLevel
	limitation string
}

type platformGranularity struct {
	Product     string          `json:"product"`
	Role        string          `json:"role"`
	Cadence     string          `json:"cadence,omitempty"`
	Depth       json.RawMessage `json:"depth,omitempty"`
	Aggregation json.RawMessage `json:"aggregation,omitempty"`
	Detail      any             `json:"detail,omitempty"`
}

// BuildPlatformDeclarations is the single cross-venue assembly boundary. It
// obtains every row from venue-owned contracts/support matrices, joins it to
// caller-owned evidence, and returns catalog's deterministic check report.
func BuildPlatformDeclarations(evidence PlatformDeclarationEvidence) ([]catalog.DeclaredSource, catalog.DeclarationCheckReport, error) {
	if evidence.ValidityStartNS < 0 || evidence.AdapterVersion == "" || strings.TrimSpace(evidence.AdapterVersion) != evidence.AdapterVersion {
		return nil, catalog.DeclarationCheckReport{}, fmt.Errorf("%w: validity start and adapter version are required", ErrInvalidPlatformDeclaration)
	}
	if evidence.ValidityEndNS != nil && *evidence.ValidityEndNS <= evidence.ValidityStartNS {
		return nil, catalog.DeclarationCheckReport{}, fmt.Errorf("%w: validity end must be greater than start", ErrInvalidPlatformDeclaration)
	}
	if evidence.Tuples == nil {
		return nil, catalog.DeclarationCheckReport{}, fmt.Errorf("%w: tuple evidence map is required", ErrInvalidPlatformDeclaration)
	}

	candidateByKey, keys, err := indexedPlatformCandidates(evidence.AdapterVersion)
	if err != nil {
		return nil, catalog.DeclarationCheckReport{}, err
	}
	for key := range evidence.Tuples {
		if _, known := candidateByKey[key]; !known {
			return nil, catalog.DeclarationCheckReport{}, fmt.Errorf("%w: evidence supplied for unknown tuple %s", ErrInvalidPlatformDeclaration, key)
		}
	}

	bySource := make(map[string][]catalog.DeclaredTuple)
	for _, key := range keys {
		candidate := candidateByKey[key]
		tupleEvidence, exists := evidence.Tuples[key]
		if !exists {
			return nil, catalog.DeclarationCheckReport{}, fmt.Errorf("%w: missing source-contract candidate evidence for %s", ErrInvalidPlatformDeclaration, key)
		}
		state, history, err := platformTransitionHistory(candidate.support, tupleEvidence, evidence.ValidityStartNS, evidence.ValidityEndNS)
		if err != nil {
			return nil, catalog.DeclarationCheckReport{}, fmt.Errorf("%s: %w", key, err)
		}
		limitation := candidate.limitation
		if limitation == "" {
			limitation = noAdditionalPlatformLimitation
		}
		var coverageEnd *int64
		if evidence.ValidityEndNS != nil {
			end := *evidence.ValidityEndNS
			coverageEnd = &end
		}
		tuple := catalog.DeclaredTuple{
			SourceID: candidate.series.SourceID, APIVersion: candidate.series.APIVersion,
			Entitlement: candidate.series.Entitlement, ChannelOrEndpoint: candidate.series.ChannelOrEndpoint,
			DataFamily: candidate.series.DataFamily, NativeGranularity: candidate.series.NativeGranularity,
			Coverage:       catalog.CoverageInterval{StartNS: evidence.ValidityStartNS, EndNS: coverageEnd},
			AdapterVersion: candidate.series.AdapterVersion, State: state, Limitation: limitation,
			TransitionHistory: history,
		}
		bySource[tuple.SourceID] = append(bySource[tuple.SourceID], tuple)
	}

	sourceIDs := make([]string, 0, len(bySource))
	for sourceID := range bySource {
		sourceIDs = append(sourceIDs, sourceID)
	}
	slices.Sort(sourceIDs)
	declarations := make([]catalog.DeclaredSource, 0, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		declarations = append(declarations, catalog.DeclaredSource{SourceID: sourceID, Tuples: bySource[sourceID]})
	}
	report, err := catalog.CheckDeclarations(declarations)
	if err != nil {
		return nil, catalog.DeclarationCheckReport{}, fmt.Errorf("%w: %v", ErrInvalidPlatformDeclaration, err)
	}
	return declarations, report, nil
}

// NewPlatformDeclarationTemplate materializes every exact venue-owned
// candidate with deterministic static-contract evidence. It performs no
// operational promotion.
func NewPlatformDeclarationTemplate(adapterVersion string, validityStartNS int64, validityEndNS *int64) (PlatformDeclarationEvidence, error) {
	if validityStartNS < 0 || adapterVersion == "" || strings.TrimSpace(adapterVersion) != adapterVersion {
		return PlatformDeclarationEvidence{}, fmt.Errorf("%w: validity start and adapter version are required", ErrInvalidPlatformDeclaration)
	}
	if validityEndNS != nil && *validityEndNS <= validityStartNS {
		return PlatformDeclarationEvidence{}, fmt.Errorf("%w: validity end must be greater than start", ErrInvalidPlatformDeclaration)
	}
	candidateByKey, keys, err := indexedPlatformCandidates(adapterVersion)
	if err != nil {
		return PlatformDeclarationEvidence{}, err
	}
	terminalAtNS := validityStartNS + 1
	if terminalAtNS <= validityStartNS {
		return PlatformDeclarationEvidence{}, fmt.Errorf("%w: validity start leaves no representable terminal transition time", ErrInvalidPlatformDeclaration)
	}
	if validityEndNS != nil && terminalAtNS >= *validityEndNS {
		return PlatformDeclarationEvidence{}, fmt.Errorf("%w: bounded validity interval is too short for terminal support rows", ErrInvalidPlatformDeclaration)
	}
	tuples := make(map[string]PlatformTupleEvidence, len(candidateByKey))
	for _, key := range keys {
		candidate := candidateByKey[key]
		candidateTransition, err := platformTemplateTransition(candidate, validityStartNS)
		if err != nil {
			return PlatformDeclarationEvidence{}, err
		}
		lifecycle := map[catalog.TupleLifecycle]PlatformLifecycleEvidence{
			catalog.LifecycleCandidate: candidateTransition,
		}
		switch candidate.support {
		case capture.SupportAvailable:
		case capture.SupportUnsupported:
			terminal, err := platformTemplateTransition(candidate, terminalAtNS)
			if err != nil {
				return PlatformDeclarationEvidence{}, err
			}
			lifecycle[catalog.LifecycleUnsupported] = terminal
		case capture.SupportAmbiguous:
			terminal, err := platformTemplateTransition(candidate, terminalAtNS)
			if err != nil {
				return PlatformDeclarationEvidence{}, err
			}
			lifecycle[catalog.LifecycleAmbiguous] = terminal
		default:
			return PlatformDeclarationEvidence{}, fmt.Errorf("%w: unknown source support level %d", ErrInvalidPlatformDeclaration, candidate.support)
		}
		tuples[key] = PlatformTupleEvidence{Lifecycle: lifecycle}
	}
	var end *int64
	if validityEndNS != nil {
		value := *validityEndNS
		end = &value
	}
	return PlatformDeclarationEvidence{
		ValidityStartNS: validityStartNS, ValidityEndNS: end,
		AdapterVersion: adapterVersion, Tuples: tuples,
	}, nil
}

type platformTemplateEvidenceMaterial struct {
	Series     PlatformTupleSeriesIdentity `json:"series"`
	Support    capture.SupportLevel        `json:"support"`
	Limitation string                      `json:"limitation"`
}

func platformTemplateTransition(candidate platformCandidate, effectiveAtNS int64) (PlatformLifecycleEvidence, error) {
	material, err := json.Marshal(platformTemplateEvidenceMaterial{
		Series: candidate.series, Support: candidate.support, Limitation: candidate.limitation,
	})
	if err != nil {
		return PlatformLifecycleEvidence{}, fmt.Errorf("%w: encode template transition evidence: %v", ErrInvalidPlatformDeclaration, err)
	}
	digest := sha256.Sum256(material)
	return PlatformLifecycleEvidence{EvidenceSHA256: hex.EncodeToString(digest[:]), EffectiveAtNS: effectiveAtNS}, nil
}

func platformTransitionHistory(support capture.SupportLevel, evidence PlatformTupleEvidence, validityStartNS int64, validityEndNS *int64) (catalog.TupleLifecycle, []catalog.LifecycleEvidence, error) {
	if evidence.Lifecycle == nil {
		return "", nil, fmt.Errorf("%w: lifecycle evidence is required", ErrInvalidPlatformDeclaration)
	}
	for state, transition := range evidence.Lifecycle {
		if !platformLifecycleKnown(state) || !validPlatformSHA256(transition.EvidenceSHA256) {
			return "", nil, fmt.Errorf("%w: invalid %q lifecycle evidence", ErrInvalidPlatformDeclaration, state)
		}
		if transition.EffectiveAtNS < validityStartNS || validityEndNS != nil && transition.EffectiveAtNS >= *validityEndNS {
			return "", nil, fmt.Errorf("%w: %q lifecycle evidence is outside tuple coverage", ErrInvalidPlatformDeclaration, state)
		}
	}
	candidateEvidence, exists := evidence.Lifecycle[catalog.LifecycleCandidate]
	if !exists || candidateEvidence.EffectiveAtNS != validityStartNS {
		return "", nil, fmt.Errorf("%w: candidate evidence is required at validity start", ErrInvalidPlatformDeclaration)
	}
	history := []catalog.LifecycleEvidence{{
		State: catalog.LifecycleCandidate, EffectiveAtNS: candidateEvidence.EffectiveAtNS,
		EvidenceSHA256: candidateEvidence.EvidenceSHA256,
	}}
	appendTransition := func(state catalog.TupleLifecycle, transition PlatformLifecycleEvidence) error {
		if transition.EffectiveAtNS <= history[len(history)-1].EffectiveAtNS {
			return fmt.Errorf("%w: %s transition time must be strictly later than its prior state", ErrInvalidPlatformDeclaration, state)
		}
		history = append(history, catalog.LifecycleEvidence{
			State: state, EffectiveAtNS: transition.EffectiveAtNS, EvidenceSHA256: transition.EvidenceSHA256,
		})
		return nil
	}

	if support == capture.SupportUnsupported || support == capture.SupportAmbiguous {
		terminal := catalog.LifecycleUnsupported
		if support == capture.SupportAmbiguous {
			terminal = catalog.LifecycleAmbiguous
		}
		terminalEvidence, ok := evidence.Lifecycle[terminal]
		if !ok {
			return "", nil, fmt.Errorf("%w: %s source row requires matching terminal evidence", ErrInvalidPlatformDeclaration, terminal)
		}
		if len(evidence.Lifecycle) != 2 || evidence.OperationalGateSHA256 != "" || evidence.CanarySHA256 != "" {
			return "", nil, fmt.Errorf("%w: terminal source row contains a promotion claim", ErrInvalidPlatformDeclaration)
		}
		if err := appendTransition(terminal, terminalEvidence); err != nil {
			return "", nil, err
		}
		return terminal, history, nil
	}
	if support != capture.SupportAvailable {
		return "", nil, fmt.Errorf("%w: unknown source support level %d", ErrInvalidPlatformDeclaration, support)
	}
	if _, claimed := evidence.Lifecycle[catalog.LifecycleUnsupported]; claimed {
		return "", nil, fmt.Errorf("%w: available source row cannot claim unsupported", ErrInvalidPlatformDeclaration)
	}
	if _, claimed := evidence.Lifecycle[catalog.LifecycleAmbiguous]; claimed {
		return "", nil, fmt.Errorf("%w: available source row cannot claim ambiguous", ErrInvalidPlatformDeclaration)
	}

	promotionOrder := [...]catalog.TupleLifecycle{
		catalog.LifecycleCaptured,
		catalog.LifecycleReplayable,
		catalog.LifecycleNormalized,
		catalog.LifecycleReconstructed,
		catalog.LifecycleSupported,
	}
	state := catalog.LifecycleCandidate
	gap := false
	for _, promotion := range promotionOrder {
		transition, claimed := evidence.Lifecycle[promotion]
		if !claimed {
			gap = true
			continue
		}
		if gap {
			return "", nil, fmt.Errorf("%w: claimed %s promotion has a missing prior state", ErrInvalidPlatformDeclaration, promotion)
		}
		if promotion == catalog.LifecycleSupported {
			if !validPlatformSHA256(evidence.OperationalGateSHA256) || !validPlatformSHA256(evidence.CanarySHA256) {
				return "", nil, fmt.Errorf("%w: supported promotion requires passed operational-gate and 26-hour/live canary hashes", ErrInvalidPlatformDeclaration)
			}
		}
		if err := appendTransition(promotion, transition); err != nil {
			return "", nil, err
		}
		state = promotion
	}
	if state != catalog.LifecycleSupported && (evidence.OperationalGateSHA256 != "" || evidence.CanarySHA256 != "") {
		return "", nil, fmt.Errorf("%w: operational or canary evidence supplied without a supported promotion", ErrInvalidPlatformDeclaration)
	}
	return state, history, nil
}

func platformLifecycleKnown(state catalog.TupleLifecycle) bool {
	switch state {
	case catalog.LifecycleCandidate, catalog.LifecycleCaptured, catalog.LifecycleReplayable,
		catalog.LifecycleNormalized, catalog.LifecycleReconstructed, catalog.LifecycleSupported,
		catalog.LifecycleUnsupported, catalog.LifecycleAmbiguous:
		return true
	default:
		return false
	}
}

func validPlatformSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 || value != strings.ToLower(value) {
		return false
	}
	for _, b := range decoded {
		if b != 0 {
			return true
		}
	}
	return false
}

func indexedPlatformCandidates(adapterVersion string) (map[string]platformCandidate, []string, error) {
	candidates, err := platformCandidates(adapterVersion)
	if err != nil {
		return nil, nil, err
	}
	candidateByKey := make(map[string]platformCandidate, len(candidates))
	for _, candidate := range candidates {
		key, err := candidate.series.Key()
		if err != nil {
			return nil, nil, err
		}
		if previous, exists := candidateByKey[key]; exists {
			if previous.support != candidate.support || previous.limitation != candidate.limitation {
				return nil, nil, fmt.Errorf("%w: conflicting source-contract rows for %s", ErrInvalidPlatformDeclaration, key)
			}
			continue
		}
		candidateByKey[key] = candidate
	}
	keys := make([]string, 0, len(candidateByKey))
	for key := range candidateByKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return candidateByKey, keys, nil
}

func platformCandidates(adapterVersion string) ([]platformCandidate, error) {
	var candidates []platformCandidate
	appendCatalog := func(source catalog.Source, version catalog.SourceVersion, channels []catalog.ChannelContract) error {
		if err := source.Validate(); err != nil {
			return err
		}
		if err := version.Validate(); err != nil {
			return err
		}
		for _, channel := range channels {
			if err := channel.Validate(); err != nil {
				return err
			}
			granularity, err := marshalPlatformGranularity(platformGranularity{
				Product: source.ProductFamily, Role: channel.Role, Cadence: channel.CadenceSource,
				Depth: channel.Depth, Aggregation: channel.Aggregation,
			})
			if err != nil {
				return err
			}
			support := capture.SupportAvailable
			switch channel.SupportState {
			case "supported", "limited":
			case "unsupported":
				support = capture.SupportUnsupported
			case "quarantined":
				support = capture.SupportAmbiguous
			default:
				return fmt.Errorf("%w: unmapped catalog support state %q", ErrInvalidPlatformDeclaration, channel.SupportState)
			}
			candidates = append(candidates, platformCandidate{series: PlatformTupleSeriesIdentity{
				SourceID: source.SourceID, APIVersion: version.OfficialAPIVersion, Entitlement: "public",
				ChannelOrEndpoint: channel.ChannelID, DataFamily: channel.DataFamily,
				NativeGranularity: granularity, AdapterVersion: adapterVersion,
			}, support: support, limitation: channel.Limitation})
		}
		return nil
	}
	spotSource, spotVersion, spotChannels := binance.SpotCatalogContract()
	if err := appendCatalog(spotSource, spotVersion, spotChannels); err != nil {
		return nil, fmt.Errorf("%w: Binance Spot catalog contract: %v", ErrInvalidPlatformDeclaration, err)
	}
	usdmSource, usdmVersion, usdmChannels := binance.USDMCatalogContract()
	if err := appendCatalog(usdmSource, usdmVersion, usdmChannels); err != nil {
		return nil, fmt.Errorf("%w: Binance USD-M catalog contract: %v", ErrInvalidPlatformDeclaration, err)
	}
	coinmSource, coinmVersion, coinmChannels := binance.CoinMCatalogContract()
	if err := appendCatalog(coinmSource, coinmVersion, coinmChannels); err != nil {
		return nil, fmt.Errorf("%w: Binance COIN-M catalog contract: %v", ErrInvalidPlatformDeclaration, err)
	}

	var err error
	candidates, err = appendBinanceSourceCandidates(candidates, adapterVersion)
	if err != nil {
		return nil, err
	}
	candidates, err = appendBybitCandidates(candidates, adapterVersion)
	if err != nil {
		return nil, err
	}
	candidates, err = appendOKXCandidates(candidates, adapterVersion)
	if err != nil {
		return nil, err
	}
	candidates, err = appendDeribitCandidates(candidates, adapterVersion)
	if err != nil {
		return nil, err
	}
	candidates, err = appendHyperliquidCandidates(candidates, adapterVersion)
	if err != nil {
		return nil, err
	}
	return candidates, nil
}

func appendBinanceSourceCandidates(candidates []platformCandidate, adapterVersion string) ([]platformCandidate, error) {
	spotContracts := []capture.SourceContract{binance.SpotWSSourceContract()}
	for _, depth := range []int{100, 500, 1000, 5000} {
		contract, err := binance.SpotDepthSourceContract(depth)
		if err != nil {
			return nil, err
		}
		spotContracts = append(spotContracts, contract)
		for _, capability := range contract.Capabilities {
			candidates, err = appendCapabilityCandidate(candidates, adapterVersion, "spot", capability.DataFamily, fmt.Sprintf("depth=%d", depth), contract, capability, "")
			if err != nil {
				return nil, err
			}
		}
	}
	for _, capability := range spotContracts[0].Capabilities {
		var err error
		candidates, err = appendCapabilityCandidate(candidates, adapterVersion, "spot", capability.DataFamily, "", spotContracts[0], capability, "")
		if err != nil {
			return nil, err
		}
	}

	usdmContracts := []capture.SourceContract{binance.USDMPublicSourceContract(), binance.USDMMarketSourceContract()}
	for _, operation := range []binance.USDMRESTOperation{binance.USDMRESTExchangeInfo, binance.USDMRESTDepth, binance.USDMRESTOpenInterest} {
		depth := 0
		if operation == binance.USDMRESTDepth {
			depth = 1000
		}
		contract, err := binance.USDMRESTSourceContract(operation, depth)
		if err != nil {
			return nil, err
		}
		usdmContracts = append(usdmContracts, contract)
	}
	for _, contract := range usdmContracts {
		for _, capability := range contract.Capabilities {
			detail := ""
			for _, stream := range binance.USDMStreamAllowlist() {
				if capability.ChannelOrEndpoint == stream.Suffix {
					detail = fmt.Sprintf("role=%s;cadence_ceiling_ns=%d;rpi=%s;completeness=%s", stream.Role, stream.CadenceCeilingNS, stream.RPIInclusion, stream.Completeness)
					break
				}
			}
			var err error
			candidates, err = appendCapabilityCandidate(candidates, adapterVersion, "usdm", capability.DataFamily, detail, contract, capability, "")
			if err != nil {
				return nil, err
			}
		}
	}

	coinmContracts := []capture.SourceContract{binance.CoinMSourceContract()}
	for _, operation := range []binance.CoinMRESTOperation{binance.CoinMRESTExchangeInfo, binance.CoinMRESTDepth, binance.CoinMRESTOpenInterest} {
		depth := 0
		if operation == binance.CoinMRESTDepth {
			depth = 1000
		}
		contract, err := binance.CoinMRESTSourceContract(operation, depth)
		if err != nil {
			return nil, err
		}
		coinmContracts = append(coinmContracts, contract)
	}
	for _, contract := range coinmContracts {
		for _, capability := range contract.Capabilities {
			detail := ""
			for _, stream := range binance.CoinMStreamAllowlist() {
				if capability.ChannelOrEndpoint == stream.Selector {
					detail = fmt.Sprintf("role=%s;cadence_ceiling_ns=%d;merged_universe=%t;native_amount=%s", stream.Role, stream.CadenceCeilingNS, stream.MergedUniverse, stream.NativeAmountUnit)
					break
				}
			}
			var err error
			candidates, err = appendCapabilityCandidate(candidates, adapterVersion, "coinm", capability.DataFamily, detail, contract, capability, "")
			if err != nil {
				return nil, err
			}
		}
	}
	return candidates, nil
}

func appendBybitCandidates(candidates []platformCandidate, adapterVersion string) ([]platformCandidate, error) {
	for _, category := range []bybit.Category{bybit.Spot, bybit.Linear, bybit.Inverse} {
		matrix, err := bybit.SupportMatrix(category)
		if err != nil {
			return nil, err
		}
		contract, err := bybit.PublicSourceContract(category)
		if err != nil {
			return nil, err
		}
		if len(matrix) != len(contract.Capabilities) {
			return nil, fmt.Errorf("%w: Bybit %s support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration, category)
		}
		for i, support := range matrix {
			capability := contract.Capabilities[i]
			if capability.Support != support.Support {
				return nil, fmt.Errorf("%w: Bybit %s support matrix/source contract state mismatch", ErrInvalidPlatformDeclaration, category)
			}
			candidates, err = appendCapabilityCandidate(candidates, adapterVersion, string(category), string(support.Role), "", contract, capability, support.Limitation)
			if err != nil {
				return nil, err
			}
		}
		instrumentContract, instrumentErr := bybit.InstrumentSourceContract(category)
		if instrumentErr != nil {
			return nil, instrumentErr
		}
		metadataSupport, ok := bybit.Supports(category, bybit.RoleInstrumentMetadata)
		if !ok || len(instrumentContract.Capabilities) != 1 {
			return nil, fmt.Errorf("%w: Bybit %s instrument source contract is not represented by its support matrix", ErrInvalidPlatformDeclaration, category)
		}
		candidates, err = appendCapabilityCandidate(candidates, adapterVersion, string(category), string(metadataSupport.Role), "", instrumentContract, instrumentContract.Capabilities[0], metadataSupport.Limitation)
		if err != nil {
			return nil, err
		}
	}

	matrix := bybit.OptionSupportMatrix()
	ws := bybit.OptionPublicSourceContract()
	rest := bybit.OptionInstrumentSourceContract()
	if err := ws.Validate(); err != nil {
		return nil, err
	}
	if err := rest.Validate(); err != nil {
		return nil, err
	}
	capabilityIndex := 0
	for _, support := range matrix {
		contract := ws
		var capability capture.Capability
		if support.Role == bybit.RoleInstrumentMetadata {
			contract = rest
			capability = rest.Capabilities[0]
		} else {
			if capabilityIndex >= len(ws.Capabilities) {
				return nil, fmt.Errorf("%w: Bybit Option support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration)
			}
			capability = ws.Capabilities[capabilityIndex]
			capabilityIndex++
		}
		var err error
		candidates, err = appendCapabilityCandidate(candidates, adapterVersion, "option", string(support.Role), "", contract, capability, support.Limitation)
		if err != nil {
			return nil, err
		}
	}
	if capabilityIndex != len(ws.Capabilities) {
		return nil, fmt.Errorf("%w: unaccounted Bybit Option source capability", ErrInvalidPlatformDeclaration)
	}
	return candidates, nil
}

func appendOKXCandidates(candidates []platformCandidate, adapterVersion string) ([]platformCandidate, error) {
	matrix := okx.SupportMatrix()
	publicContract, err := okx.PublicSourceContract(okx.PublicSocket)
	if err != nil {
		return nil, err
	}
	businessContract, err := okx.PublicSourceContract(okx.BusinessSocket)
	if err != nil {
		return nil, err
	}
	restContract, err := okx.RESTSourceContract()
	if err != nil {
		return nil, err
	}
	native := okx.NativeFileContract()
	for _, product := range []okx.InstrumentType{okx.Spot, okx.Swap, okx.Futures, okx.Option} {
		publicIndex, businessIndex, restIndex := 0, 0, 0
		for _, support := range matrix {
			if support.Role == okx.RoleNativeFileManifest {
				granularity, err := marshalPlatformGranularity(platformGranularity{Product: string(product), Role: string(support.Role), Detail: map[string]any{"publication_lag_days": native.PublicationLagDays, "manifest_only": native.ManifestOnly}})
				if err != nil {
					return nil, err
				}
				candidates = append(candidates, platformCandidate{series: PlatformTupleSeriesIdentity{
					SourceID: native.SourceID, APIVersion: fmt.Sprintf("OKX native file publication contract v%d", native.Version), Entitlement: support.Entitlement,
					ChannelOrEndpoint: string(support.Role), DataFamily: string(support.Role), NativeGranularity: granularity, AdapterVersion: adapterVersion,
				}, support: support.Support, limitation: support.Limitation})
				continue
			}
			contract := restContract
			var capability capture.Capability
			switch support.Socket {
			case okx.PublicSocket:
				if publicIndex >= len(publicContract.Capabilities) {
					return nil, fmt.Errorf("%w: OKX public support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration)
				}
				contract, capability = publicContract, publicContract.Capabilities[publicIndex]
				publicIndex++
			case okx.BusinessSocket:
				if businessIndex >= len(businessContract.Capabilities) {
					return nil, fmt.Errorf("%w: OKX business support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration)
				}
				contract, capability = businessContract, businessContract.Capabilities[businessIndex]
				businessIndex++
			default:
				if restIndex >= len(restContract.Capabilities) {
					return nil, fmt.Errorf("%w: OKX REST support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration)
				}
				capability = restContract.Capabilities[restIndex]
				restIndex++
			}
			candidates, err = appendCapabilityCandidate(candidates, adapterVersion, string(product), string(support.Role), "", contract, capability, support.Limitation)
			if err != nil {
				return nil, err
			}
		}
		if publicIndex != len(publicContract.Capabilities) || businessIndex != len(businessContract.Capabilities) || restIndex != len(restContract.Capabilities) {
			return nil, fmt.Errorf("%w: unaccounted OKX source capability", ErrInvalidPlatformDeclaration)
		}
	}
	return candidates, nil
}

func appendDeribitCandidates(candidates []platformCandidate, adapterVersion string) ([]platformCandidate, error) {
	matrix := deribit.SupportMatrix()
	policies := []deribit.CadencePolicy{
		{Requested: deribit.CadenceRaw, Fallback: deribit.Cadence100MS, Authorized: true},
		{Requested: deribit.Cadence100MS},
		{Requested: deribit.CadenceAgg2},
	}
	for _, policy := range policies {
		contract, err := deribit.SourceContract(policy)
		if err != nil {
			return nil, err
		}
		if len(matrix) != len(contract.Capabilities) {
			return nil, fmt.Errorf("%w: Deribit support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration)
		}
		cadence := string(policy.Requested)
		if policy.Fallback != "" {
			cadence += ";fallback=" + string(policy.Fallback)
		}
		for i, support := range matrix {
			var appendErr error
			candidates, appendErr = appendCapabilityCandidate(candidates, adapterVersion, "v2", string(support.Role), cadence, contract, contract.Capabilities[i], support.Limitation)
			if appendErr != nil {
				return nil, appendErr
			}
		}
	}
	return candidates, nil
}

func appendHyperliquidCandidates(candidates []platformCandidate, adapterVersion string) ([]platformCandidate, error) {
	families := []struct {
		product string
		family  hyperliquid.Family
		dex     string
	}{
		{product: "main", family: hyperliquid.MainPerpetual},
		{product: "spot", family: hyperliquid.Spot},
		{product: "hip-3", family: hyperliquid.HIP3, dex: "xyz"},
	}
	for _, item := range families {
		matrix, err := hyperliquid.SupportMatrix(item.family)
		if err != nil {
			return nil, err
		}
		ws, err := hyperliquid.PublicSourceContract(hyperliquid.Mainnet, item.family, item.dex)
		if err != nil {
			return nil, err
		}
		info, err := hyperliquid.InfoSourceContract(hyperliquid.Mainnet, item.family, item.dex)
		if err != nil {
			return nil, err
		}
		assetContextSupport, ok := hyperliquid.Supports(item.family, hyperliquid.RoleAssetContext)
		if !ok || len(info.Capabilities) == 0 || info.Capabilities[0].DataFamily != "asset_context" {
			return nil, fmt.Errorf("%w: Hyperliquid Info asset-context capability is not represented by its support matrix", ErrInvalidPlatformDeclaration)
		}
		candidates, err = appendCapabilityCandidate(candidates, adapterVersion, item.product, string(assetContextSupport.Role), "", info, info.Capabilities[0], assetContextSupport.Limitation)
		if err != nil {
			return nil, err
		}
		wsIndex, infoIndex := 0, 0
		for _, support := range matrix {
			contract := ws
			var capability capture.Capability
			switch support.Role {
			case hyperliquid.RoleTrades, hyperliquid.RoleSlowBook, hyperliquid.RoleFastBook, hyperliquid.RoleBBO, hyperliquid.RoleAssetContext:
				if wsIndex >= len(ws.Capabilities) {
					return nil, fmt.Errorf("%w: Hyperliquid WebSocket support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration)
				}
				capability = ws.Capabilities[wsIndex]
				wsIndex++
			case hyperliquid.RoleMetadata, hyperliquid.RoleFundingHistory, hyperliquid.RoleNativeHistoryImport, hyperliquid.RoleStrictEconomicUnits:
				contract = info
				for infoIndex < len(info.Capabilities) && info.Capabilities[infoIndex].DataFamily == "asset_context" {
					infoIndex++
				}
				if infoIndex >= len(info.Capabilities) {
					return nil, fmt.Errorf("%w: Hyperliquid Info support matrix/source contract row mismatch", ErrInvalidPlatformDeclaration)
				}
				capability = info.Capabilities[infoIndex]
				infoIndex++
			default:
				capability = capture.Capability{ChannelOrEndpoint: "unsupported:" + string(support.Role), DataFamily: string(support.Role), Entitlement: "public", Support: support.Support, Declaration: support.Limitation}
			}
			candidates, err = appendCapabilityCandidate(candidates, adapterVersion, item.product, string(support.Role), "", contract, capability, support.Limitation)
			if err != nil {
				return nil, err
			}
		}
		if wsIndex != len(ws.Capabilities) || infoIndex != len(info.Capabilities) {
			return nil, fmt.Errorf("%w: unaccounted Hyperliquid source capability", ErrInvalidPlatformDeclaration)
		}
	}
	return candidates, nil
}

func appendCapabilityCandidate(candidates []platformCandidate, adapterVersion, product, role, cadence string, contract capture.SourceContract, capability capture.Capability, limitation string) ([]platformCandidate, error) {
	if err := contract.Validate(); err != nil {
		return nil, fmt.Errorf("%w: source contract %q: %v", ErrInvalidPlatformDeclaration, contract.ContractID, err)
	}
	if capability.Support != capture.SupportAvailable && limitation == "" {
		limitation = capability.Declaration
	}
	granularity, err := marshalPlatformGranularity(platformGranularity{Product: product, Role: role, Cadence: cadence})
	if err != nil {
		return nil, err
	}
	return append(candidates, platformCandidate{series: PlatformTupleSeriesIdentity{
		SourceID: contract.SourceID, APIVersion: contract.APIVersion, Entitlement: capability.Entitlement,
		ChannelOrEndpoint: capability.ChannelOrEndpoint, DataFamily: capability.DataFamily,
		NativeGranularity: granularity, AdapterVersion: adapterVersion,
	}, support: capability.Support, limitation: limitation}), nil
}

func marshalPlatformGranularity(value platformGranularity) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("%w: encode native granularity: %v", ErrInvalidPlatformDeclaration, err)
	}
	return string(encoded), nil
}
