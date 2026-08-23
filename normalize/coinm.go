package normalize

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

const CoinMContractFormulaVersion = "binance_coinm_inverse_quote_v1"

var ErrInvalidCoinMContract = errors.New("normalize: invalid Binance COIN-M contract conversion")

type CoinMPayoffKind string

const CoinMPayoffInverseQuote CoinMPayoffKind = "inverse_quote"

// CoinMContractTerms is one temporal catalog version. ContractSize is the
// quote-asset face value of one native contract; it is never inferred from a
// symbol, host, trade, book level, or open-interest payload.
type CoinMContractTerms struct {
	InstrumentUID     string
	BaseAssetID       string
	QuoteAssetID      string
	SettlementAssetID string
	CatalogVersion    string
	ValidFromNS       int64
	ValidUntilNS      OptionalInt64
	ContractSize      Decimal
	Payoff            CoinMPayoffKind
}

func (t CoinMContractTerms) Validate() error {
	for _, value := range []string{t.InstrumentUID, t.BaseAssetID, t.QuoteAssetID, t.SettlementAssetID, t.CatalogVersion} {
		if value == "" || len(value) > MaxUnitIDBytes || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: incomplete temporal catalog identity", ErrInvalidCoinMContract)
		}
	}
	if t.BaseAssetID == t.QuoteAssetID || t.ValidFromNS < 0 ||
		(!t.ValidUntilNS.Valid && t.ValidUntilNS.Value != 0) ||
		(t.ValidUntilNS.Valid && t.ValidUntilNS.Value <= t.ValidFromNS) {
		return fmt.Errorf("%w: invalid temporal interval", ErrInvalidCoinMContract)
	}
	if err := t.ContractSize.Validate(); err != nil || t.ContractSize.Scale != CanonicalAmountScale || decimalSign(t.ContractSize) <= 0 {
		return fmt.Errorf("%w: invalid contractSize", ErrInvalidCoinMContract)
	}
	if t.Payoff != CoinMPayoffInverseQuote || t.QuoteAssetID != "USD" || t.SettlementAssetID != t.BaseAssetID {
		return fmt.Errorf("%w: unsupported or inconsistent payoff metadata", ErrInvalidCoinMContract)
	}
	return nil
}

func (t CoinMContractTerms) Contains(observedTimeNS int64) bool {
	return observedTimeNS >= t.ValidFromNS && (!t.ValidUntilNS.Valid || observedTimeNS < t.ValidUntilNS.Value)
}

type CoinMContractConversion struct {
	NativeContracts NativeValue
	DerivedUSD      DerivedValue
	DerivedBase     DerivedValue
	DerivedQuote    DerivedValue
}

// ConvertCoinMContracts converts native contracts using the one temporal
// contract version active at observedTimeNS. For the inverse-quote payoff:
// quote/USD notional = contracts * contractSize, and base notional =
// quote notional / price. Only exact scale-18 results are emitted.
func ConvertCoinMContracts(contracts, price Decimal, observedTimeNS int64, provenance FieldProvenance, terms CoinMContractTerms) (CoinMContractConversion, error) {
	return convertCoinMContracts(contracts, &price, observedTimeNS, provenance, terms)
}

// ConvertCoinMContractsWithoutPrice preserves native contracts and derives
// quote/USD face value while leaving base value unavailable.
func ConvertCoinMContractsWithoutPrice(contracts Decimal, observedTimeNS int64, provenance FieldProvenance, terms CoinMContractTerms) (CoinMContractConversion, error) {
	return convertCoinMContracts(contracts, nil, observedTimeNS, provenance, terms)
}

func convertCoinMContracts(contracts Decimal, price *Decimal, observedTimeNS int64, provenance FieldProvenance, terms CoinMContractTerms) (CoinMContractConversion, error) {
	if err := terms.Validate(); err != nil || !terms.Contains(observedTimeNS) {
		return CoinMContractConversion{}, fmt.Errorf("%w: no active temporal contract version", ErrInvalidCoinMContract)
	}
	if err := contracts.Validate(); err != nil || contracts.Scale != CanonicalAmountScale || decimalSign(contracts) < 0 {
		return CoinMContractConversion{}, fmt.Errorf("%w: invalid native contract amount", ErrInvalidCoinMContract)
	}
	if err := provenance.Validate(true); err != nil {
		return CoinMContractConversion{}, fmt.Errorf("%w: invalid observation provenance", ErrInvalidCoinMContract)
	}
	quote, err := multiplyCoinMDecimal(contracts, terms.ContractSize)
	if err != nil {
		return CoinMContractConversion{}, err
	}
	conversion := CoinMContractConversion{
		NativeContracts: NativeValue{Decimal: contracts, Unit: NativeUnit{Kind: NativeUnitContracts, InstrumentUID: terms.InstrumentUID}},
		DerivedUSD:      coinMDerived(quote, QuoteAssetUnit("USD"), provenance, terms.CatalogVersion),
		DerivedQuote:    coinMDerived(quote, QuoteAssetUnit(terms.QuoteAssetID), provenance, terms.CatalogVersion),
		DerivedBase:     missingCoinMDerived(),
	}
	if price != nil {
		if err := price.Validate(); err != nil || price.Scale != CanonicalPriceScale || decimalSign(*price) <= 0 {
			return CoinMContractConversion{}, fmt.Errorf("%w: invalid conversion price", ErrInvalidCoinMContract)
		}
		base, err := divideCoinMDecimal(quote, *price)
		if err != nil {
			return CoinMContractConversion{}, err
		}
		conversion.DerivedBase = coinMDerived(base, BaseAssetUnit(terms.BaseAssetID), provenance, terms.CatalogVersion)
	}
	if err := conversion.NativeContracts.Validate(); err != nil {
		return CoinMContractConversion{}, err
	}
	for _, derived := range []DerivedValue{conversion.DerivedUSD, conversion.DerivedBase, conversion.DerivedQuote} {
		if err := derived.Validate(); err != nil {
			return CoinMContractConversion{}, err
		}
	}
	return conversion, nil
}

func coinMDerived(decimal Decimal, unit Unit, provenance FieldProvenance, catalogVersion string) DerivedValue {
	return DerivedValue{
		Field:          NumericField{State: SourceValue, Value: Numeric{Decimal: decimal, Unit: unit}, Provenance: provenance},
		FormulaVersion: CoinMContractFormulaVersion,
		CatalogVersion: catalogVersion,
		Confidence:     ConversionConfidenceExact,
	}
}

func missingCoinMDerived() DerivedValue {
	return DerivedValue{Field: NumericField{State: SourceMissing, Provenance: FieldProvenance{SourceTimeResolution: ResolutionAbsent}}}
}

func multiplyCoinMDecimal(left, right Decimal) (Decimal, error) {
	if left.Scale != CanonicalAmountScale || right.Scale != CanonicalAmountScale {
		return Decimal{}, fmt.Errorf("%w: multiplication scale mismatch", ErrInvalidCoinMContract)
	}
	leftWide, leftOK := new(big.Int).SetString(left.Coefficient, 10)
	rightWide, rightOK := new(big.Int).SetString(right.Coefficient, 10)
	if !leftOK || !rightOK {
		return Decimal{}, fmt.Errorf("%w: invalid multiplication operand", ErrInvalidCoinMContract)
	}
	product := new(big.Int).Mul(leftWide, rightWide)
	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(CanonicalAmountScale)), nil)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(product, scaleFactor, remainder)
	if remainder.Sign() != 0 {
		return Decimal{}, fmt.Errorf("%w: inexact quote conversion", ErrInvalidCoinMContract)
	}
	return boundedCoinMDecimal(quotient, CanonicalAmountScale)
}

func divideCoinMDecimal(numerator, denominator Decimal) (Decimal, error) {
	if numerator.Scale != CanonicalAmountScale || denominator.Scale != CanonicalPriceScale {
		return Decimal{}, fmt.Errorf("%w: division scale mismatch", ErrInvalidCoinMContract)
	}
	numeratorWide, numeratorOK := new(big.Int).SetString(numerator.Coefficient, 10)
	denominatorWide, denominatorOK := new(big.Int).SetString(denominator.Coefficient, 10)
	if !numeratorOK || !denominatorOK || denominatorWide.Sign() <= 0 {
		return Decimal{}, fmt.Errorf("%w: invalid division operand", ErrInvalidCoinMContract)
	}
	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(CanonicalPriceScale)), nil)
	scaled := new(big.Int).Mul(numeratorWide, scaleFactor)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(scaled, denominatorWide, remainder)
	if remainder.Sign() != 0 {
		return Decimal{}, fmt.Errorf("%w: inexact base conversion", ErrInvalidCoinMContract)
	}
	return boundedCoinMDecimal(quotient, CanonicalAmountScale)
}

func boundedCoinMDecimal(coefficient *big.Int, scale uint8) (Decimal, error) {
	text := coefficient.String()
	if len(strings.TrimPrefix(text, "-")) > MaxCanonicalCoefficientDigits {
		return Decimal{}, fmt.Errorf("%w: converted coefficient exceeds bound", ErrInvalidCoinMContract)
	}
	result := Decimal{Coefficient: text, Scale: scale}
	if err := result.Validate(); err != nil {
		return Decimal{}, fmt.Errorf("%w: invalid converted decimal", ErrInvalidCoinMContract)
	}
	return result, nil
}

func decimalSign(decimal Decimal) int {
	wide, ok := new(big.Int).SetString(decimal.Coefficient, 10)
	if !ok {
		return 0
	}
	return wide.Sign()
}
