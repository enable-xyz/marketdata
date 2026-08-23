package normalize

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

const (
	CanonicalPriceScale           uint8 = 18
	CanonicalAmountScale          uint8 = 18
	CanonicalPercentScale         uint8 = 8
	MaxDecimalInputBytes                = 128
	MaxCanonicalCoefficientDigits       = 38
	MaxCanonicalScale             uint8 = 38
)

var ErrInvalidDecimal = errors.New("normalize: invalid canonical decimal")

type DecimalBounds struct {
	MaxInputBytes        int
	MaxCoefficientDigits int
}

func DefaultDecimalBounds() DecimalBounds {
	return DecimalBounds{MaxInputBytes: MaxDecimalInputBytes, MaxCoefficientDigits: MaxCanonicalCoefficientDigits}
}

// Decimal stores a canonical signed base-10 coefficient at a schema-fixed
// scale. Coefficient is a string so values wider than machine integers remain
// exact and value copies do not share mutable big.Int words.
type Decimal struct {
	Coefficient string
	Scale       uint8
}

func ParseDecimal(text string, scale uint8, bounds DecimalBounds) (Decimal, error) {
	if bounds.MaxInputBytes <= 0 || bounds.MaxInputBytes > MaxDecimalInputBytes ||
		bounds.MaxCoefficientDigits <= 0 || bounds.MaxCoefficientDigits > MaxCanonicalCoefficientDigits ||
		scale > MaxCanonicalScale {
		return Decimal{}, fmt.Errorf("%w: invalid parser bounds", ErrInvalidDecimal)
	}
	if text == "" || len(text) > bounds.MaxInputBytes {
		return Decimal{}, fmt.Errorf("%w: empty or oversized input", ErrInvalidDecimal)
	}
	negative := false
	start := 0
	if text[0] == '-' {
		negative = true
		start = 1
		if start == len(text) {
			return Decimal{}, fmt.Errorf("%w: sign without digits", ErrInvalidDecimal)
		}
	} else if text[0] == '+' {
		return Decimal{}, fmt.Errorf("%w: leading plus is not canonical source syntax", ErrInvalidDecimal)
	}
	dot := -1
	digits := 0
	for i := start; i < len(text); i++ {
		c := text[i]
		if c == '.' {
			if dot >= 0 {
				return Decimal{}, fmt.Errorf("%w: repeated decimal point", ErrInvalidDecimal)
			}
			dot = i
			continue
		}
		if c < '0' || c > '9' {
			return Decimal{}, fmt.Errorf("%w: non-decimal character", ErrInvalidDecimal)
		}
		digits++
	}
	if digits == 0 || dot == start || dot == len(text)-1 {
		return Decimal{}, fmt.Errorf("%w: malformed decimal", ErrInvalidDecimal)
	}
	fractional := 0
	if dot >= 0 {
		fractional = len(text) - dot - 1
	}
	if fractional > int(scale) {
		return Decimal{}, fmt.Errorf("%w: source precision exceeds schema scale", ErrInvalidDecimal)
	}
	coefficientDigits := digits + int(scale) - fractional
	if coefficientDigits > bounds.MaxCoefficientDigits {
		return Decimal{}, fmt.Errorf("%w: coefficient exceeds digit bound", ErrInvalidDecimal)
	}
	var builder strings.Builder
	builder.Grow(coefficientDigits + 1)
	if negative {
		builder.WriteByte('-')
	}
	for i := start; i < len(text); i++ {
		if text[i] != '.' {
			builder.WriteByte(text[i])
		}
	}
	for range int(scale) - fractional {
		builder.WriteByte('0')
	}
	wide, ok := new(big.Int).SetString(builder.String(), 10)
	if !ok {
		return Decimal{}, fmt.Errorf("%w: arbitrary-width conversion failed", ErrInvalidDecimal)
	}
	coefficient := wide.String()
	if len(strings.TrimPrefix(coefficient, "-")) > bounds.MaxCoefficientDigits {
		return Decimal{}, fmt.Errorf("%w: coefficient exceeds digit bound", ErrInvalidDecimal)
	}
	return Decimal{Coefficient: coefficient, Scale: scale}, nil
}

func (d Decimal) Validate() error {
	if d.Coefficient == "" || d.Scale > MaxCanonicalScale || len(d.Coefficient) > MaxCanonicalCoefficientDigits+1 {
		return fmt.Errorf("%w: coefficient or scale bound", ErrInvalidDecimal)
	}
	wide, ok := new(big.Int).SetString(d.Coefficient, 10)
	if !ok || wide.String() != d.Coefficient || len(strings.TrimPrefix(d.Coefficient, "-")) > MaxCanonicalCoefficientDigits {
		return fmt.Errorf("%w: non-canonical coefficient", ErrInvalidDecimal)
	}
	return nil
}

func (d Decimal) IsZero() bool { return d.Coefficient == "0" }
