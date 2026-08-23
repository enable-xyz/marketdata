package normalize

import (
	"fmt"
	"strings"
)

const (
	DerivativeTickerSchemaName           = "DerivativeTickerV1"
	DerivativeTickerSchemaVersion uint16 = 1
	MaxDerivativeSourceRoleBytes         = 128
	MaxNativeUnitLabelBytes              = 128
	MaxReportedVariantNameBytes          = 128
	MaxOpenInterestVariants              = 16
)

const UnitRate UnitKind = "rate"

func RateUnit() Unit { return Unit{Kind: UnitRate} }

type FieldProvenance struct {
	SourceTimeNS         OptionalInt64
	SourceTimeResolution TimeResolution
	AgeNS                OptionalUint64
}

func (p FieldProvenance) Validate(observed bool) error {
	if !observed {
		if p == (FieldProvenance{}) || p == (FieldProvenance{SourceTimeResolution: ResolutionAbsent}) {
			return nil
		}
		return fmt.Errorf("%w: unobserved field has provenance", ErrInvalidNormalized)
	}
	if p.SourceTimeNS.Valid != (p.SourceTimeResolution != ResolutionAbsent) || !validTimeResolution(p.SourceTimeResolution) {
		return fmt.Errorf("%w: field source time resolution mismatch", ErrInvalidNormalized)
	}
	if !p.SourceTimeNS.Valid || p.SourceTimeNS.Value < 0 || !p.AgeNS.Valid {
		return fmt.Errorf("%w: observed field lacks valid source time or age", ErrInvalidNormalized)
	}
	return nil
}

type NumericField struct {
	State      SourceState
	Value      Numeric
	Provenance FieldProvenance
}

func (f NumericField) Validate() error {
	present := f.State == SourceValue
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid numeric field source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if !present {
		if f.Value != (Numeric{}) {
			return fmt.Errorf("%w: unavailable numeric field has a value", ErrInvalidNormalized)
		}
		return nil
	}
	return f.Value.Validate()
}

type TimeField struct {
	State      SourceState
	ValueNS    int64
	Resolution TimeResolution
	Provenance FieldProvenance
}

func (f TimeField) Validate() error {
	present := f.State == SourceValue
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid time field source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if !present {
		if f.ValueNS != 0 || f.Resolution != ResolutionAbsent {
			return fmt.Errorf("%w: unavailable time field has a value", ErrInvalidNormalized)
		}
		return nil
	}
	if f.ValueNS < 0 || f.Resolution == ResolutionAbsent || !validTimeResolution(f.Resolution) {
		return fmt.Errorf("%w: invalid time field value", ErrInvalidNormalized)
	}
	return nil
}

func validFieldState(state SourceState) bool {
	switch state {
	case SourceMissing, SourceNull, SourceEmpty, SourceValue:
		return true
	default:
		return false
	}
}

type NativeUnitKind string

const (
	NativeUnitBaseAsset         NativeUnitKind = "base_asset"
	NativeUnitQuoteAsset        NativeUnitKind = "quote_asset"
	NativeUnitContracts         NativeUnitKind = "contracts"
	NativeUnitUSD               NativeUnitKind = "usd"
	NativeUnitRate              NativeUnitKind = "rate"
	NativeUnitImpliedVolatility NativeUnitKind = "implied_volatility"
	NativeUnitVenueUnspecified  NativeUnitKind = "venue_native_unspecified"
)

type NativeUnit struct {
	Kind          NativeUnitKind
	AssetID       string
	InstrumentUID string
	VenueLabel    string
}

func (u NativeUnit) Validate() error {
	for _, value := range []string{u.AssetID, u.InstrumentUID, u.VenueLabel} {
		if len(value) > MaxUnitIDBytes || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: invalid native unit identity", ErrInvalidNormalized)
		}
	}
	switch u.Kind {
	case NativeUnitBaseAsset, NativeUnitQuoteAsset:
		if u.AssetID == "" || u.InstrumentUID != "" || u.VenueLabel != "" {
			return fmt.Errorf("%w: invalid native asset unit", ErrInvalidNormalized)
		}
	case NativeUnitContracts:
		if u.AssetID != "" || u.InstrumentUID == "" || u.VenueLabel != "" {
			return fmt.Errorf("%w: invalid native contract unit", ErrInvalidNormalized)
		}
	case NativeUnitUSD:
		if u.AssetID != "USD" || u.InstrumentUID != "" || u.VenueLabel != "" {
			return fmt.Errorf("%w: invalid native USD unit", ErrInvalidNormalized)
		}
	case NativeUnitRate, NativeUnitImpliedVolatility:
		if u.AssetID != "" || u.InstrumentUID != "" || u.VenueLabel != "" {
			return fmt.Errorf("%w: invalid dimensionless native unit", ErrInvalidNormalized)
		}
	case NativeUnitVenueUnspecified:
		if u.AssetID != "" || u.InstrumentUID != "" || u.VenueLabel == "" || len(u.VenueLabel) > MaxNativeUnitLabelBytes {
			return fmt.Errorf("%w: invalid venue-native unit", ErrInvalidNormalized)
		}
	default:
		return fmt.Errorf("%w: unknown native unit", ErrInvalidNormalized)
	}
	return nil
}

type NativeValue struct {
	Decimal Decimal
	Unit    NativeUnit
}

func (v NativeValue) Validate() error {
	if err := v.Decimal.Validate(); err != nil {
		return err
	}
	return v.Unit.Validate()
}

type OpenInterestSidedness string

const (
	OpenInterestSingleSide  OpenInterestSidedness = "single_side"
	OpenInterestBothSides   OpenInterestSidedness = "both_sides"
	OpenInterestUnspecified OpenInterestSidedness = "unspecified"
)

type ReportedValue struct {
	Name  string
	Value NativeValue
}

type ConversionConfidence string

const (
	ConversionConfidenceExact       ConversionConfidence = "exact"
	ConversionConfidenceProvisional ConversionConfidence = "provisional"
)

type DerivedValue struct {
	Field          NumericField
	FormulaVersion string
	CatalogVersion string
	Confidence     ConversionConfidence
}

func (v DerivedValue) Validate() error {
	if err := v.Field.Validate(); err != nil {
		return err
	}
	present := v.Field.State == SourceValue
	if !present {
		if v.FormulaVersion != "" || v.CatalogVersion != "" || v.Confidence != "" {
			return fmt.Errorf("%w: unavailable derived value has derivation metadata", ErrInvalidNormalized)
		}
		return nil
	}
	if v.FormulaVersion == "" || v.CatalogVersion == "" || (v.Confidence != ConversionConfidenceExact && v.Confidence != ConversionConfidenceProvisional) {
		return fmt.Errorf("%w: incomplete derived value provenance", ErrInvalidNormalized)
	}
	return nil
}

type OpenInterestObservation struct {
	State                    SourceState
	Variant                  string
	Native                   NativeValue
	Sidedness                OpenInterestSidedness
	ReportedVariants         []ReportedValue
	MultiplierCatalogVersion string
	DerivedBase              DerivedValue
	DerivedQuote             DerivedValue
	DerivedUSD               DerivedValue
	Provenance               FieldProvenance
}

func (o OpenInterestObservation) Validate() error {
	present := o.State == SourceValue
	if !validFieldState(o.State) || o.Variant == "" || len(o.Variant) > MaxReportedVariantNameBytes || strings.IndexByte(o.Variant, 0) >= 0 {
		return fmt.Errorf("%w: invalid open-interest identity", ErrInvalidNormalized)
	}
	if err := o.Provenance.Validate(o.State != SourceMissing); err != nil {
		return err
	}
	if !present {
		if o.Native != (NativeValue{}) || o.Sidedness != "" || len(o.ReportedVariants) != 0 || o.MultiplierCatalogVersion != "" {
			return fmt.Errorf("%w: unavailable open interest has a native value", ErrInvalidNormalized)
		}
	} else {
		if err := o.Native.Validate(); err != nil {
			return err
		}
		if o.Sidedness != OpenInterestSingleSide && o.Sidedness != OpenInterestBothSides && o.Sidedness != OpenInterestUnspecified {
			return fmt.Errorf("%w: invalid open-interest sidedness", ErrInvalidNormalized)
		}
		if len(o.ReportedVariants) > MaxOpenInterestVariants {
			return fmt.Errorf("%w: too many reported open-interest variants", ErrInvalidNormalized)
		}
		seen := make(map[string]struct{}, len(o.ReportedVariants))
		for _, variant := range o.ReportedVariants {
			if variant.Name == "" || len(variant.Name) > MaxReportedVariantNameBytes || strings.IndexByte(variant.Name, 0) >= 0 {
				return fmt.Errorf("%w: invalid reported open-interest variant", ErrInvalidNormalized)
			}
			if _, ok := seen[variant.Name]; ok {
				return fmt.Errorf("%w: duplicate reported open-interest variant", ErrInvalidNormalized)
			}
			seen[variant.Name] = struct{}{}
			if err := variant.Value.Validate(); err != nil {
				return err
			}
		}
	}
	for _, derived := range []DerivedValue{o.DerivedBase, o.DerivedQuote, o.DerivedUSD} {
		if err := derived.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type DerivativeTickerV1 struct {
	Metadata         Metadata
	NativeSourceRole string
	LastPrice        NumericField
	MarkPrice        NumericField
	IndexPrice       NumericField
	FundingRate      NumericField
	NextFundingTime  TimeField
	OpenInterest     []OpenInterestObservation
	SettlementPrice  NumericField
	Basis            NumericField
	Premium          NumericField
}

func (e DerivativeTickerV1) Validate() error {
	if err := validateSchema(e.Metadata, DerivativeTickerSchemaName, DerivativeTickerSchemaVersion); err != nil {
		return err
	}
	if e.NativeSourceRole == "" || len(e.NativeSourceRole) > MaxDerivativeSourceRoleBytes || strings.IndexByte(e.NativeSourceRole, 0) >= 0 {
		return fmt.Errorf("%w: invalid derivative ticker source role", ErrInvalidNormalized)
	}
	for _, field := range []NumericField{e.LastPrice, e.MarkPrice, e.IndexPrice, e.FundingRate, e.SettlementPrice, e.Basis, e.Premium} {
		if err := field.Validate(); err != nil {
			return err
		}
	}
	if err := e.NextFundingTime.Validate(); err != nil {
		return err
	}
	if len(e.OpenInterest) > MaxOpenInterestVariants {
		return fmt.Errorf("%w: too many open-interest fields", ErrInvalidNormalized)
	}
	seen := make(map[string]struct{}, len(e.OpenInterest))
	for _, observation := range e.OpenInterest {
		if _, ok := seen[observation.Variant]; ok {
			return fmt.Errorf("%w: duplicate open-interest field", ErrInvalidNormalized)
		}
		seen[observation.Variant] = struct{}{}
		if err := observation.Validate(); err != nil {
			return err
		}
	}
	return nil
}
