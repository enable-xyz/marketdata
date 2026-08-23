package normalize

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

var ErrInvalidHyperliquidUnit = errors.New("normalize: invalid Hyperliquid economic unit")

type HyperliquidUnitResolution string

const (
	HyperliquidUnitResolved    HyperliquidUnitResolution = "resolved"
	HyperliquidUnitProvisional HyperliquidUnitResolution = "provisional"
)

// HyperliquidEconomicValue keeps the venue-native decimal for every family.
// Normalized is populated only when temporal catalog evidence resolves the
// economic unit. HIP-3 values remain provisional and therefore cannot enter a
// strict normalized total.
type HyperliquidEconomicValue struct {
	InstrumentUID     string
	NativeRole        string
	CatalogGeneration string
	Resolution        HyperliquidUnitResolution
	Native            NativeValue
	Normalized        NumericField
}

func NewHyperliquidResolvedEconomicValue(decimal Decimal, unit Unit, instrumentUID, nativeRole, catalogGeneration string, provenance FieldProvenance) (HyperliquidEconomicValue, error) {
	nativeUnit, err := hyperliquidResolvedNativeUnit(unit)
	if err != nil {
		return HyperliquidEconomicValue{}, err
	}
	value := HyperliquidEconomicValue{
		InstrumentUID:     instrumentUID,
		NativeRole:        nativeRole,
		CatalogGeneration: catalogGeneration,
		Resolution:        HyperliquidUnitResolved,
		Native:            NativeValue{Decimal: decimal, Unit: nativeUnit},
		Normalized:        NumericField{State: SourceValue, Value: Numeric{Decimal: decimal, Unit: unit}, Provenance: provenance},
	}
	if err := value.Validate(); err != nil {
		return HyperliquidEconomicValue{}, err
	}
	return value, nil
}

// NewHyperliquidHIP3ProvisionalValue is deliberately not configurable with a
// canonical unit. Resolving HIP-3 requires a future evidence-bearing contract
// generation; callers cannot opt provisional observations into strict totals.
func NewHyperliquidHIP3ProvisionalValue(decimal Decimal, instrumentUID, nativeRole, catalogGeneration string) (HyperliquidEconomicValue, error) {
	value := HyperliquidEconomicValue{
		InstrumentUID:     instrumentUID,
		NativeRole:        nativeRole,
		CatalogGeneration: catalogGeneration,
		Resolution:        HyperliquidUnitProvisional,
		Native: NativeValue{
			Decimal: decimal,
			Unit:    NativeUnit{Kind: NativeUnitVenueUnspecified, VenueLabel: "hyperliquid_hip3_" + nativeRole},
		},
		Normalized: NumericField{State: SourceMissing, Provenance: FieldProvenance{SourceTimeResolution: ResolutionAbsent}},
	}
	if err := value.Validate(); err != nil {
		return HyperliquidEconomicValue{}, err
	}
	return value, nil
}

func (v HyperliquidEconomicValue) Validate() error {
	if !validHyperliquidEconomicText(v.InstrumentUID, MaxUnitIDBytes) ||
		!validHyperliquidEconomicText(v.NativeRole, MaxNativeUnitLabelBytes-17) ||
		!validHyperliquidGeneration(v.CatalogGeneration) {
		return fmt.Errorf("%w: incomplete evidence identity", ErrInvalidHyperliquidUnit)
	}
	if err := v.Native.Validate(); err != nil {
		return fmt.Errorf("%w: native value: %v", ErrInvalidHyperliquidUnit, err)
	}
	if err := v.Normalized.Validate(); err != nil {
		return fmt.Errorf("%w: normalized value: %v", ErrInvalidHyperliquidUnit, err)
	}
	switch v.Resolution {
	case HyperliquidUnitResolved:
		if v.Native.Unit.Kind != NativeUnitBaseAsset && v.Native.Unit.Kind != NativeUnitQuoteAsset {
			return fmt.Errorf("%w: resolved value has non-economic native unit", ErrInvalidHyperliquidUnit)
		}
		if v.Normalized.State != SourceValue || v.Normalized.Value.Decimal != v.Native.Decimal {
			return fmt.Errorf("%w: resolved native and normalized values disagree", ErrInvalidHyperliquidUnit)
		}
		if (v.Native.Unit.Kind == NativeUnitBaseAsset && (v.Normalized.Value.Unit.Kind != UnitBaseAssetAmount || v.Native.Unit.AssetID != v.Normalized.Value.Unit.AssetID)) ||
			(v.Native.Unit.Kind == NativeUnitQuoteAsset && (v.Normalized.Value.Unit.Kind != UnitQuoteAssetAmount || v.Native.Unit.AssetID != v.Normalized.Value.Unit.AssetID)) {
			return fmt.Errorf("%w: resolved native and normalized units disagree", ErrInvalidHyperliquidUnit)
		}
	case HyperliquidUnitProvisional:
		if v.Native.Unit.Kind != NativeUnitVenueUnspecified || v.Native.Unit.VenueLabel != "hyperliquid_hip3_"+v.NativeRole ||
			v.Normalized.State != SourceMissing {
			return fmt.Errorf("%w: provisional value leaked a normalized unit", ErrInvalidHyperliquidUnit)
		}
	default:
		return fmt.Errorf("%w: unknown resolution", ErrInvalidHyperliquidUnit)
	}
	return nil
}

func (v HyperliquidEconomicValue) EligibleForStrictTotal() bool {
	return v.Resolution == HyperliquidUnitResolved && v.Validate() == nil
}

type HyperliquidStrictTotal struct {
	HasValue bool
	Value    Numeric
	Included uint32
	Excluded uint32
}

// StrictHyperliquidTotal sums only exact, unit-resolved observations. A
// provisional HIP-3 value is counted as excluded and never influences Value.
func StrictHyperliquidTotal(values []HyperliquidEconomicValue) (HyperliquidStrictTotal, error) {
	var total HyperliquidStrictTotal
	var coefficient big.Int
	for index, value := range values {
		if err := value.Validate(); err != nil {
			return HyperliquidStrictTotal{}, fmt.Errorf("%w: value %d: %v", ErrInvalidHyperliquidUnit, index, err)
		}
		if value.Resolution == HyperliquidUnitProvisional {
			total.Excluded++
			continue
		}
		if !total.HasValue {
			total.HasValue = true
			total.Value = value.Normalized.Value
			if _, ok := coefficient.SetString(total.Value.Decimal.Coefficient, 10); !ok {
				return HyperliquidStrictTotal{}, ErrInvalidHyperliquidUnit
			}
			total.Included = 1
			continue
		}
		if value.Normalized.Value.Unit != total.Value.Unit || value.Normalized.Value.Decimal.Scale != total.Value.Decimal.Scale {
			return HyperliquidStrictTotal{}, fmt.Errorf("%w: mixed units or scales", ErrInvalidHyperliquidUnit)
		}
		var next big.Int
		if _, ok := next.SetString(value.Normalized.Value.Decimal.Coefficient, 10); !ok {
			return HyperliquidStrictTotal{}, ErrInvalidHyperliquidUnit
		}
		coefficient.Add(&coefficient, &next)
		total.Included++
	}
	if total.HasValue {
		text := coefficient.String()
		if len(strings.TrimPrefix(text, "-")) > MaxCanonicalCoefficientDigits {
			return HyperliquidStrictTotal{}, fmt.Errorf("%w: strict total overflow", ErrInvalidHyperliquidUnit)
		}
		total.Value.Decimal.Coefficient = text
		if err := total.Value.Validate(); err != nil {
			return HyperliquidStrictTotal{}, err
		}
	}
	return total, nil
}

func hyperliquidResolvedNativeUnit(unit Unit) (NativeUnit, error) {
	if err := unit.Validate(); err != nil {
		return NativeUnit{}, fmt.Errorf("%w: canonical unit: %v", ErrInvalidHyperliquidUnit, err)
	}
	switch unit.Kind {
	case UnitBaseAssetAmount:
		return NativeUnit{Kind: NativeUnitBaseAsset, AssetID: unit.AssetID}, nil
	case UnitQuoteAssetAmount:
		return NativeUnit{Kind: NativeUnitQuoteAsset, AssetID: unit.AssetID}, nil
	default:
		return NativeUnit{}, fmt.Errorf("%w: strict economic values require base or quote amount units", ErrInvalidHyperliquidUnit)
	}
}

func validHyperliquidEconomicText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.IndexByte(value, 0) < 0
}

func validHyperliquidGeneration(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
