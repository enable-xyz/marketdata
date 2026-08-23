package normalize

import (
	"fmt"
	"strings"
)

const (
	OptionSummarySchemaName           = "OptionSummaryV1"
	OptionSummarySchemaVersion uint16 = 1
	MaxOptionIdentityBytes            = 256
	MaxOptionSourceRoleBytes          = 128
)

// TextField preserves missing, null, empty, and supplied native text while
// carrying the source time and age of every observation.
type TextField struct {
	State      SourceState
	Value      string
	Provenance FieldProvenance
}

func (f TextField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid text field source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != "" {
			return fmt.Errorf("%w: unavailable text field has a value", ErrInvalidNormalized)
		}
		return nil
	}
	if f.Value == "" || len(f.Value) > MaxOptionIdentityBytes || strings.IndexByte(f.Value, 0) >= 0 {
		return fmt.Errorf("%w: invalid text field value", ErrInvalidNormalized)
	}
	return nil
}

// NativeNumericField keeps a venue-native unit for quantities and Greeks whose
// dimensions cannot be represented by the canonical spot price/amount units.
type NativeNumericField struct {
	State      SourceState
	Value      NativeValue
	Provenance FieldProvenance
}

func (f NativeNumericField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid native numeric field source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != (NativeValue{}) {
			return fmt.Errorf("%w: unavailable native numeric field has a value", ErrInvalidNormalized)
		}
		return nil
	}
	return f.Value.Validate()
}

type OptionKind string

const (
	OptionCall OptionKind = "call"
	OptionPut  OptionKind = "put"
)

type OptionKindField struct {
	State      SourceState
	Value      OptionKind
	Provenance FieldProvenance
}

func (f OptionKindField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid option-kind field source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != "" {
			return fmt.Errorf("%w: unavailable option-kind field has a value", ErrInvalidNormalized)
		}
		return nil
	}
	if f.Value != OptionCall && f.Value != OptionPut {
		return fmt.Errorf("%w: invalid call/put value", ErrInvalidNormalized)
	}
	return nil
}

// OptionSummaryV1 is a snapshot contract. Every supplied field carries its own
// source time and age; an unsupported Greek is SourceMissing with no value or
// provenance, never a numeric zero.
type OptionSummaryV1 struct {
	Metadata         Metadata
	NativeSourceRole string

	Instrument TextField
	Underlying TextField
	Index      TextField
	Expiry     TimeField
	Strike     NumericField
	CallPut    OptionKindField

	BidPrice  NumericField
	AskPrice  NumericField
	LastPrice NumericField
	MarkPrice NumericField

	BidIV  NumericField
	AskIV  NumericField
	MarkIV NumericField

	Delta NativeNumericField
	Gamma NativeNumericField
	Vega  NativeNumericField
	Theta NativeNumericField
	Rho   NativeNumericField

	OpenInterest    NativeNumericField
	Volume          NativeNumericField
	ForwardPrice    NumericField
	UnderlyingPrice NumericField
	IndexPrice      NumericField
}

func (e OptionSummaryV1) Validate() error {
	if err := validateSchema(e.Metadata, OptionSummarySchemaName, OptionSummarySchemaVersion); err != nil {
		return err
	}
	if e.NativeSourceRole == "" || len(e.NativeSourceRole) > MaxOptionSourceRoleBytes || strings.IndexByte(e.NativeSourceRole, 0) >= 0 {
		return fmt.Errorf("%w: invalid option source role", ErrInvalidNormalized)
	}
	for _, field := range []TextField{e.Instrument, e.Underlying, e.Index} {
		if err := field.Validate(); err != nil {
			return err
		}
		if field.State != SourceValue {
			return fmt.Errorf("%w: option identity is unavailable", ErrInvalidNormalized)
		}
	}
	if e.Instrument.Value != e.Metadata.InstrumentUID {
		return fmt.Errorf("%w: option instrument identity differs from common metadata", ErrInvalidNormalized)
	}
	if err := e.Expiry.Validate(); err != nil {
		return err
	}
	if e.Expiry.State != SourceValue {
		return fmt.Errorf("%w: option expiry is unavailable", ErrInvalidNormalized)
	}
	if err := e.Strike.Validate(); err != nil {
		return err
	}
	if e.Strike.State != SourceValue {
		return fmt.Errorf("%w: option strike is unavailable", ErrInvalidNormalized)
	}
	if err := e.CallPut.Validate(); err != nil {
		return err
	}
	if e.CallPut.State != SourceValue {
		return fmt.Errorf("%w: option call/put is unavailable", ErrInvalidNormalized)
	}
	for _, field := range []NumericField{
		e.BidPrice, e.AskPrice, e.LastPrice, e.MarkPrice,
		e.BidIV, e.AskIV, e.MarkIV, e.ForwardPrice, e.UnderlyingPrice, e.IndexPrice,
	} {
		if err := field.Validate(); err != nil {
			return err
		}
	}
	for _, iv := range []NumericField{e.BidIV, e.AskIV, e.MarkIV} {
		if iv.State == SourceValue && iv.Value.Unit != RateUnit() {
			return fmt.Errorf("%w: option IV must use rate units", ErrInvalidNormalized)
		}
	}
	for _, field := range []NativeNumericField{e.Delta, e.Gamma, e.Vega, e.Theta, e.Rho, e.OpenInterest, e.Volume} {
		if err := field.Validate(); err != nil {
			return err
		}
	}
	return nil
}
