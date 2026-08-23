package normalize

import (
	"errors"
	"testing"
)

func TestCoinMContractConversionTemporalPayoffAndUnits(t *testing.T) {
	t.Parallel()
	contracts := coinMTestDecimal(t, "2", CanonicalAmountScale)
	price := coinMTestDecimal(t, "20000", CanonicalPriceScale)
	terms := CoinMContractTerms{
		InstrumentUID:     "binance-coinm:BTCUSD_PERP:0",
		BaseAssetID:       "BTC",
		QuoteAssetID:      "USD",
		SettlementAssetID: "BTC",
		CatalogVersion:    "coinm-catalog-v1",
		ValidFromNS:       100,
		ValidUntilNS:      OptionalInt64{Valid: true, Value: 200},
		ContractSize:      coinMTestDecimal(t, "100", CanonicalAmountScale),
		Payoff:            CoinMPayoffInverseQuote,
	}
	provenance := FieldProvenance{
		SourceTimeNS:         OptionalInt64{Valid: true, Value: 120},
		SourceTimeResolution: ResolutionNanosecond,
		AgeNS:                OptionalUint64{Valid: true, Value: 30},
	}
	conversion, err := ConvertCoinMContracts(contracts, price, 150, provenance, terms)
	if err != nil {
		t.Fatalf("ConvertCoinMContracts: %v", err)
	}
	if conversion.NativeContracts.Unit.Kind != NativeUnitContracts || conversion.NativeContracts.Unit.InstrumentUID != terms.InstrumentUID {
		t.Fatalf("native contracts = %#v", conversion.NativeContracts)
	}
	if conversion.DerivedUSD.Field.Value.Decimal.Coefficient != "200000000000000000000" || conversion.DerivedUSD.Field.Value.Unit != QuoteAssetUnit("USD") {
		t.Fatalf("USD conversion = %#v", conversion.DerivedUSD)
	}
	if conversion.DerivedQuote.Field.Value.Decimal != conversion.DerivedUSD.Field.Value.Decimal || conversion.DerivedQuote.Field.Value.Unit != QuoteAssetUnit("USD") {
		t.Fatalf("quote conversion = %#v", conversion.DerivedQuote)
	}
	if conversion.DerivedBase.Field.Value.Decimal.Coefficient != "10000000000000000" || conversion.DerivedBase.Field.Value.Unit != BaseAssetUnit("BTC") {
		t.Fatalf("base conversion = %#v", conversion.DerivedBase)
	}
	if conversion.DerivedUSD.CatalogVersion != terms.CatalogVersion || conversion.DerivedUSD.FormulaVersion != CoinMContractFormulaVersion || conversion.DerivedUSD.Confidence != ConversionConfidenceExact {
		t.Fatalf("conversion provenance = %#v", conversion.DerivedUSD)
	}
	if _, err := ConvertCoinMContracts(contracts, price, 200, provenance, terms); !errors.Is(err, ErrInvalidCoinMContract) {
		t.Fatalf("expired terms error = %v", err)
	}
}

func TestCoinMConversionWithoutPriceAndUnprovenPayoff(t *testing.T) {
	t.Parallel()
	contracts := coinMTestDecimal(t, "10659", CanonicalAmountScale)
	terms := CoinMContractTerms{
		InstrumentUID:     "binance-coinm:BTCUSD_PERP:0",
		BaseAssetID:       "BTC",
		QuoteAssetID:      "USD",
		SettlementAssetID: "BTC",
		CatalogVersion:    "coinm-catalog-v1",
		ValidFromNS:       100,
		ContractSize:      coinMTestDecimal(t, "100", CanonicalAmountScale),
		Payoff:            CoinMPayoffInverseQuote,
	}
	provenance := FieldProvenance{SourceTimeNS: OptionalInt64{Valid: true, Value: 100}, SourceTimeResolution: ResolutionNanosecond, AgeNS: OptionalUint64{Valid: true, Value: 50}}
	conversion, err := ConvertCoinMContractsWithoutPrice(contracts, 150, provenance, terms)
	if err != nil {
		t.Fatalf("ConvertCoinMContractsWithoutPrice: %v", err)
	}
	if conversion.DerivedUSD.Field.Value.Decimal.Coefficient != "1065900000000000000000000" || conversion.DerivedQuote.Field.State != SourceValue || conversion.DerivedBase.Field.State != SourceMissing {
		t.Fatalf("without-price conversion = %#v", conversion)
	}
	badPayoff := terms
	badPayoff.Payoff = CoinMPayoffKind("linear_quote")
	if _, err := ConvertCoinMContractsWithoutPrice(contracts, 150, provenance, badPayoff); !errors.Is(err, ErrInvalidCoinMContract) {
		t.Fatalf("unproven payoff error = %v", err)
	}
	inexactPrice := coinMTestDecimal(t, "3", CanonicalPriceScale)
	if _, err := ConvertCoinMContracts(coinMTestDecimal(t, "2", CanonicalAmountScale), inexactPrice, 150, provenance, terms); !errors.Is(err, ErrInvalidCoinMContract) {
		t.Fatalf("inexact base conversion error = %v", err)
	}
}

func coinMTestDecimal(t *testing.T, text string, scale uint8) Decimal {
	t.Helper()
	decimal, err := ParseDecimal(text, scale, DefaultDecimalBounds())
	if err != nil {
		t.Fatal(err)
	}
	return decimal
}
