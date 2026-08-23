package normalize

import (
	"errors"
	"fmt"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	DeribitUnitInferenceVersion           = "deribit_temporal_instrument_type_contract_size_v1"
	DeribitFixtureClassificationSynthetic = "synthetic"
	DeribitInstrumentProvenanceURL        = "https://docs.deribit.com/api-reference/market-data/public-get_instruments"
	DeribitInstrumentProvenanceSection    = "components.schemas.instrument.properties.contract_size"
	DeribitInstrumentDerivedFrom          = DeribitInstrumentProvenanceURL + "#" + DeribitInstrumentProvenanceSection
)

var ErrInvalidDeribitInference = errors.New("normalize: invalid Deribit unit inference")

type DeribitInstrumentKind string

const (
	DeribitInstrumentFuture DeribitInstrumentKind = "future"
	DeribitInstrumentOption DeribitInstrumentKind = "option"
	DeribitInstrumentSpot   DeribitInstrumentKind = "spot"
)

type DeribitInstrumentType string

const (
	DeribitInstrumentLinear   DeribitInstrumentType = "linear"
	DeribitInstrumentReversed DeribitInstrumentType = "reversed"
)

type DeribitInferenceState string

const (
	DeribitInferenceProvisional   DeribitInferenceState = "provisional"
	DeribitInferenceFixtureProven DeribitInferenceState = "fixture_proven"
)

// DeribitInstrumentTerms is one caller-selected temporal catalog generation.
// InstrumentType and ContractSize are source observations; the amount-unit rule
// derived from them remains mapper inference rather than a source guarantee.
type DeribitInstrumentTerms struct {
	InstrumentUID         string
	InstrumentName        string
	Kind                  DeribitInstrumentKind
	InstrumentType        DeribitInstrumentType
	BaseAssetID           string
	QuoteAssetID          string
	CounterAssetID        string
	SettlementAssetID     string
	ContractSize          Decimal
	ContractSizeState     SourceState
	ValidFromNS           int64
	ValidUntilNS          OptionalInt64
	CatalogGeneration     uint64
	MetadataRawSHA256     Hash
	DocumentationAtNS     int64
	FixtureClassification string
	ProvenanceURL         string
	ProvenanceSection     string
	DerivedFrom           string
}

func (t DeribitInstrumentTerms) Validate() error {
	for _, value := range []string{t.InstrumentUID, t.InstrumentName, t.BaseAssetID, t.QuoteAssetID, t.CounterAssetID, t.SettlementAssetID} {
		if value == "" || len(value) > MaxUnitIDBytes || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: incomplete temporal instrument identity", ErrInvalidDeribitInference)
		}
	}
	if t.Kind != DeribitInstrumentFuture && t.Kind != DeribitInstrumentOption && t.Kind != DeribitInstrumentSpot {
		return fmt.Errorf("%w: unsupported instrument kind", ErrInvalidDeribitInference)
	}
	if t.InstrumentType != "" && t.InstrumentType != DeribitInstrumentLinear && t.InstrumentType != DeribitInstrumentReversed {
		return fmt.Errorf("%w: unsupported instrument_type", ErrInvalidDeribitInference)
	}
	if t.ValidFromNS < 0 || (!t.ValidUntilNS.Valid && t.ValidUntilNS.Value != 0) ||
		(t.ValidUntilNS.Valid && t.ValidUntilNS.Value <= t.ValidFromNS) {
		return fmt.Errorf("%w: invalid temporal interval", ErrInvalidDeribitInference)
	}
	if t.DocumentationAtNS <= 0 {
		return fmt.Errorf("%w: documentation access time is required", ErrInvalidDeribitInference)
	}
	for _, value := range []string{t.FixtureClassification, t.ProvenanceURL, t.ProvenanceSection, t.DerivedFrom} {
		if len(value) > 512 || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: invalid fixture provenance", ErrInvalidDeribitInference)
		}
	}
	switch t.ContractSizeState {
	case SourceMissing, SourceNull:
		if t.ContractSize != (Decimal{}) {
			return fmt.Errorf("%w: unavailable contract_size has a value", ErrInvalidDeribitInference)
		}
	case SourceValue:
		if err := t.ContractSize.Validate(); err != nil || t.ContractSize.Scale != CanonicalAmountScale || !positiveDeribitDecimal(t.ContractSize) {
			return fmt.Errorf("%w: invalid contract_size", ErrInvalidDeribitInference)
		}
	default:
		return fmt.Errorf("%w: invalid contract_size source state", ErrInvalidDeribitInference)
	}
	return nil
}

func (t DeribitInstrumentTerms) Contains(observedTimeNS int64) bool {
	return observedTimeNS >= t.ValidFromNS && (!t.ValidUntilNS.Valid || observedTimeNS < t.ValidUntilNS.Value)
}

// PremiumPriceUnit returns the native price dimension for executable/book
// prices. Deribit options quote premiums in the base coin; futures and spot use
// the venue's quote-per-base price.
func (t DeribitInstrumentTerms) PremiumPriceUnit() (Unit, error) {
	if err := t.Validate(); err != nil {
		return Unit{}, err
	}
	if t.Kind == DeribitInstrumentOption {
		unit := BaseAssetUnit(t.BaseAssetID)
		return unit, unit.Validate()
	}
	unit := SpotPriceUnit(t.BaseAssetID, t.QuoteAssetID)
	return unit, unit.Validate()
}

// ReferencePriceUnit returns the dimension for strike, index, and underlying
// reference prices, which use counter_currency rather than an option's premium
// quote currency.
func (t DeribitInstrumentTerms) ReferencePriceUnit() (Unit, error) {
	if err := t.Validate(); err != nil {
		return Unit{}, err
	}
	unit := SpotPriceUnit(t.BaseAssetID, t.CounterAssetID)
	return unit, unit.Validate()
}

type DeribitUnitInference struct {
	Version               string
	State                 DeribitInferenceState
	Authority             capture.RuleAuthority
	Unit                  NativeUnit
	InstrumentUID         string
	CatalogGeneration     uint64
	MetadataRawSHA256     Hash
	DocumentationAtNS     int64
	Reason                string
	FixtureClassification string
	ProvenanceURL         string
	ProvenanceSection     string
	DerivedFrom           string
}

func (i DeribitUnitInference) Validate() error {
	if i.Version != DeribitUnitInferenceVersion || i.Authority != capture.RuleAdapterPolicyInference ||
		i.InstrumentUID == "" || len(i.InstrumentUID) > MaxUnitIDBytes || strings.IndexByte(i.InstrumentUID, 0) >= 0 ||
		i.DocumentationAtNS <= 0 || len(i.Reason) > MaxNativeUnitLabelBytes || strings.IndexByte(i.Reason, 0) >= 0 {
		return fmt.Errorf("%w: invalid inference identity", ErrInvalidDeribitInference)
	}
	if err := i.Unit.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDeribitInference, err)
	}
	switch i.State {
	case DeribitInferenceProvisional:
		if i.Unit.Kind != NativeUnitVenueUnspecified || i.CatalogGeneration != 0 || i.MetadataRawSHA256 != (Hash{}) ||
			i.FixtureClassification != "" || i.ProvenanceURL != "" || i.ProvenanceSection != "" || i.DerivedFrom != "" ||
			i.Reason == "" {
			return fmt.Errorf("%w: provisional inference presents evidence-backed support", ErrInvalidDeribitInference)
		}
	case DeribitInferenceFixtureProven:
		if i.CatalogGeneration == 0 || i.MetadataRawSHA256 == (Hash{}) || i.Reason != "" ||
			i.FixtureClassification != DeribitFixtureClassificationSynthetic ||
			i.ProvenanceURL != DeribitInstrumentProvenanceURL ||
			i.ProvenanceSection != DeribitInstrumentProvenanceSection ||
			i.DerivedFrom != DeribitInstrumentDerivedFrom ||
			(i.Unit.Kind != NativeUnitUSD && i.Unit.Kind != NativeUnitBaseAsset) {
			return fmt.Errorf("%w: proven inference lacks temporal evidence", ErrInvalidDeribitInference)
		}
	default:
		return fmt.Errorf("%w: invalid inference state", ErrInvalidDeribitInference)
	}
	return nil
}

// InferDeribitAmountUnit applies the access-dated mapper rule to one exact
// temporal metadata generation. Missing generation/hash/type/contract-size
// evidence yields a provisional venue-native unit and never normalized support.
func InferDeribitAmountUnit(terms DeribitInstrumentTerms, observedTimeNS int64) (DeribitUnitInference, error) {
	if err := terms.Validate(); err != nil {
		return DeribitUnitInference{}, err
	}
	if observedTimeNS < 0 || !terms.Contains(observedTimeNS) {
		return DeribitUnitInference{}, fmt.Errorf("%w: no active temporal instrument generation", ErrInvalidDeribitInference)
	}
	provisional := func(reason string) (DeribitUnitInference, error) {
		inference := DeribitUnitInference{
			Version: DeribitUnitInferenceVersion, State: DeribitInferenceProvisional,
			Authority:     capture.RuleAdapterPolicyInference,
			Unit:          NativeUnit{Kind: NativeUnitVenueUnspecified, VenueLabel: "deribit_amount_unit_unresolved"},
			InstrumentUID: terms.InstrumentUID, DocumentationAtNS: terms.DocumentationAtNS, Reason: reason,
		}
		return inference, inference.Validate()
	}
	if terms.CatalogGeneration == 0 || terms.MetadataRawSHA256 == (Hash{}) {
		return provisional("temporal catalog generation or raw metadata hash is unavailable")
	}
	if terms.FixtureClassification != DeribitFixtureClassificationSynthetic ||
		terms.ProvenanceURL != DeribitInstrumentProvenanceURL ||
		terms.ProvenanceSection != DeribitInstrumentProvenanceSection ||
		terms.DerivedFrom != DeribitInstrumentDerivedFrom {
		return provisional("fixture provenance is not bound to the access-dated official contract_size section")
	}
	if terms.ContractSizeState != SourceValue {
		return provisional("contract_size is unavailable")
	}
	var unit NativeUnit
	switch {
	case terms.Kind == DeribitInstrumentFuture && terms.InstrumentType == DeribitInstrumentReversed:
		unit = NativeUnit{Kind: NativeUnitUSD, AssetID: "USD"}
	case terms.Kind == DeribitInstrumentFuture && terms.InstrumentType == DeribitInstrumentLinear:
		unit = NativeUnit{Kind: NativeUnitBaseAsset, AssetID: terms.BaseAssetID}
	case terms.Kind == DeribitInstrumentOption:
		unit = NativeUnit{Kind: NativeUnitBaseAsset, AssetID: terms.BaseAssetID}
	case terms.Kind == DeribitInstrumentSpot && terms.InstrumentType == DeribitInstrumentLinear:
		unit = NativeUnit{Kind: NativeUnitBaseAsset, AssetID: terms.BaseAssetID}
	default:
		return provisional("instrument_type and kind do not prove the amount unit")
	}
	inference := DeribitUnitInference{
		Version: DeribitUnitInferenceVersion, State: DeribitInferenceFixtureProven,
		Authority: capture.RuleAdapterPolicyInference, Unit: unit,
		InstrumentUID: terms.InstrumentUID, CatalogGeneration: terms.CatalogGeneration,
		MetadataRawSHA256: terms.MetadataRawSHA256, DocumentationAtNS: terms.DocumentationAtNS,
		FixtureClassification: terms.FixtureClassification, ProvenanceURL: terms.ProvenanceURL,
		ProvenanceSection: terms.ProvenanceSection, DerivedFrom: terms.DerivedFrom,
	}
	return inference, inference.Validate()
}

func DeribitNativeAmount(text string, inference DeribitUnitInference) (NativeValue, error) {
	if err := inference.Validate(); err != nil {
		return NativeValue{}, err
	}
	value, err := ParseDecimal(text, CanonicalAmountScale, DefaultDecimalBounds())
	if err != nil || strings.HasPrefix(value.Coefficient, "-") {
		return NativeValue{}, fmt.Errorf("%w: invalid native amount", ErrInvalidDeribitInference)
	}
	native := NativeValue{Decimal: value, Unit: inference.Unit}
	if err := native.Validate(); err != nil {
		return NativeValue{}, fmt.Errorf("%w: %v", ErrInvalidDeribitInference, err)
	}
	return native, nil
}

type DeribitBookUpdateKind string

const (
	DeribitBookSnapshot DeribitBookUpdateKind = "snapshot"
	DeribitBookChange   DeribitBookUpdateKind = "change"
)

type DeribitBookAction string

const (
	DeribitBookNew    DeribitBookAction = "new"
	DeribitBookModify DeribitBookAction = "change"
	DeribitBookDelete DeribitBookAction = "delete"
)

type DeribitBookLevel struct {
	Action        DeribitBookAction
	Price         Numeric
	Amount        NativeValue
	SourceOrdinal uint32
}

type DeribitBookUpdate struct {
	InstrumentUID string
	Channel       string
	Kind          DeribitBookUpdateKind
	ChangeID      uint64
	PreviousID    OptionalUint64
	SourceTimeNS  int64
	GroupedView   bool
	PriceUnit     Unit
	UnitInference DeribitUnitInference
	Bids          []DeribitBookLevel
	Asks          []DeribitBookLevel
}

func (u DeribitBookUpdate) Validate() error {
	if u.InstrumentUID == "" || u.Channel == "" || u.ChangeID == 0 || u.SourceTimeNS < 0 ||
		(u.Kind != DeribitBookSnapshot && u.Kind != DeribitBookChange) {
		return fmt.Errorf("%w: invalid book update identity", ErrInvalidDeribitInference)
	}
	if u.Kind == DeribitBookSnapshot && u.PreviousID.Valid {
		return fmt.Errorf("%w: snapshot carries prev_change_id", ErrInvalidDeribitInference)
	}
	if u.Kind == DeribitBookChange && (!u.PreviousID.Valid || u.PreviousID.Value == 0) {
		return fmt.Errorf("%w: change lacks prev_change_id", ErrInvalidDeribitInference)
	}
	if err := u.PriceUnit.Validate(); err != nil {
		return fmt.Errorf("%w: invalid book price unit", ErrInvalidDeribitInference)
	}
	if err := u.UnitInference.Validate(); err != nil || u.UnitInference.InstrumentUID != u.InstrumentUID {
		return fmt.Errorf("%w: invalid book unit inference", ErrInvalidDeribitInference)
	}
	for _, side := range [][]DeribitBookLevel{u.Bids, u.Asks} {
		for index, level := range side {
			if level.SourceOrdinal != uint32(index) ||
				(level.Action != DeribitBookNew && level.Action != DeribitBookModify && level.Action != DeribitBookDelete) {
				return fmt.Errorf("%w: invalid source-order book level", ErrInvalidDeribitInference)
			}
			if err := level.Price.Validate(); err != nil || errNative(level.Amount) != nil ||
				level.Price.Decimal.Scale != CanonicalPriceScale || level.Amount.Decimal.Scale != CanonicalAmountScale {
				return fmt.Errorf("%w: invalid book level", ErrInvalidDeribitInference)
			}
			if level.Price.Unit != u.PriceUnit || level.Amount.Unit != u.UnitInference.Unit ||
				(level.Action == DeribitBookDelete) != level.Amount.Decimal.IsZero() {
				return fmt.Errorf("%w: inconsistent book action or unit", ErrInvalidDeribitInference)
			}
		}
	}
	return nil
}

func errNative(value NativeValue) error {
	return value.Validate()
}

func positiveDeribitDecimal(value Decimal) bool {
	return value.Coefficient != "0" && !strings.HasPrefix(value.Coefficient, "-")
}
