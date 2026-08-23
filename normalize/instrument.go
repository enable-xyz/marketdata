package normalize

import "fmt"

const (
	InstrumentEventSchemaName           = "InstrumentEventV1"
	InstrumentEventSchemaVersion uint16 = 1
)

type Uint64Field struct {
	State      SourceState
	Value      uint64
	Provenance FieldProvenance
}

func (f Uint64Field) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid uint64 field source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue && f.Value != 0 {
		return fmt.Errorf("%w: unavailable uint64 field has a value", ErrInvalidNormalized)
	}
	return nil
}

type HashField struct {
	State      SourceState
	Value      Hash
	Provenance FieldProvenance
}

func (f HashField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid hash field source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != (Hash{}) {
			return fmt.Errorf("%w: unavailable hash field has a value", ErrInvalidNormalized)
		}
		return nil
	}
	if f.Value == (Hash{}) {
		return fmt.Errorf("%w: supplied hash field is zero", ErrInvalidNormalized)
	}
	return nil
}

type InstrumentLifecycleState string

const (
	InstrumentStateUnknown           InstrumentLifecycleState = "unknown"
	InstrumentStatePreListing        InstrumentLifecycleState = "pre_listing"
	InstrumentStateListed            InstrumentLifecycleState = "listed"
	InstrumentStateAuction           InstrumentLifecycleState = "auction"
	InstrumentStateContinuousTrading InstrumentLifecycleState = "continuous_trading"
	InstrumentStateSuspended         InstrumentLifecycleState = "suspended"
	InstrumentStateExpired           InstrumentLifecycleState = "expired"
	InstrumentStateDelivery          InstrumentLifecycleState = "delivery"
	InstrumentStateDelivered         InstrumentLifecycleState = "delivered"
	InstrumentStateDelisted          InstrumentLifecycleState = "delisted"
)

type InstrumentStateField struct {
	State      SourceState
	Value      InstrumentLifecycleState
	Provenance FieldProvenance
}

func (f InstrumentStateField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid instrument-state source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != "" {
			return fmt.Errorf("%w: unavailable instrument state has a value", ErrInvalidNormalized)
		}
		return nil
	}
	switch f.Value {
	case InstrumentStateUnknown, InstrumentStatePreListing, InstrumentStateListed, InstrumentStateAuction,
		InstrumentStateContinuousTrading, InstrumentStateSuspended, InstrumentStateExpired,
		InstrumentStateDelivery, InstrumentStateDelivered, InstrumentStateDelisted:
		return nil
	default:
		return fmt.Errorf("%w: unknown instrument lifecycle state", ErrInvalidNormalized)
	}
}

type InstrumentResolutionStatus string

const (
	InstrumentResolved    InstrumentResolutionStatus = "resolved"
	InstrumentUnresolved  InstrumentResolutionStatus = "unresolved"
	InstrumentAmbiguous   InstrumentResolutionStatus = "ambiguous"
	InstrumentUnsupported InstrumentResolutionStatus = "unsupported"
)

type InstrumentResolutionField struct {
	State      SourceState
	Value      InstrumentResolutionStatus
	Provenance FieldProvenance
}

func (f InstrumentResolutionField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid instrument-resolution source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != "" {
			return fmt.Errorf("%w: unavailable resolution has a value", ErrInvalidNormalized)
		}
		return nil
	}
	switch f.Value {
	case InstrumentResolved, InstrumentUnresolved, InstrumentAmbiguous, InstrumentUnsupported:
		return nil
	default:
		return fmt.Errorf("%w: unknown instrument resolution status", ErrInvalidNormalized)
	}
}

type NumericChange struct {
	Old NumericField
	New NumericField
}

func (c NumericChange) Validate() error {
	if err := c.Old.Validate(); err != nil {
		return err
	}
	if err := c.New.Validate(); err != nil {
		return err
	}
	if c.Old.State != SourceMissing || c.New.State != SourceMissing {
		if c.Old.State == c.New.State && c.Old.Value == c.New.Value {
			return fmt.Errorf("%w: numeric change does not change source state or value", ErrInvalidNormalized)
		}
	}
	return nil
}

func (c NumericChange) observed() bool {
	return c.Old.State != SourceMissing || c.New.State != SourceMissing
}

type NativeNumericChange struct {
	Old NativeNumericField
	New NativeNumericField
}

func (c NativeNumericChange) Validate() error {
	if err := c.Old.Validate(); err != nil {
		return err
	}
	if err := c.New.Validate(); err != nil {
		return err
	}
	if c.Old.State != SourceMissing || c.New.State != SourceMissing {
		if c.Old.State == c.New.State && c.Old.Value == c.New.Value {
			return fmt.Errorf("%w: native numeric change does not change source state or value", ErrInvalidNormalized)
		}
	}
	return nil
}

func (c NativeNumericChange) observed() bool {
	return c.Old.State != SourceMissing || c.New.State != SourceMissing
}

type TextChange struct {
	Old TextField
	New TextField
}

func (c TextChange) Validate() error {
	if err := c.Old.Validate(); err != nil {
		return err
	}
	if err := c.New.Validate(); err != nil {
		return err
	}
	if c.Old.State != SourceMissing || c.New.State != SourceMissing {
		if c.Old.State == c.New.State && c.Old.Value == c.New.Value {
			return fmt.Errorf("%w: text change does not change source state or value", ErrInvalidNormalized)
		}
	}
	return nil
}

func (c TextChange) observed() bool {
	return c.Old.State != SourceMissing || c.New.State != SourceMissing
}

// InstrumentEventV1 records one catalog metadata generation and the exact
// native transition or term change that produced it.
type InstrumentEventV1 struct {
	Metadata              Metadata
	MetadataGeneration    Uint64Field
	NativeStateBefore     InstrumentStateField
	NativeStateAfter      InstrumentStateField
	ListingTime           TimeField
	ContinuousTradingTime TimeField
	ExpiryTime            TimeField
	DeliveryTime          TimeField
	DelistingTime         TimeField
	TickSize              NumericChange
	LotSize               NativeNumericChange
	ContractMultiplier    NativeNumericChange
	Payoff                TextChange
	OldRawHash            HashField
	NewRawHash            HashField
	ResolutionStatus      InstrumentResolutionField
}

func (e InstrumentEventV1) Validate() error {
	if err := validateSchema(e.Metadata, InstrumentEventSchemaName, InstrumentEventSchemaVersion); err != nil {
		return err
	}
	if err := e.MetadataGeneration.Validate(); err != nil || e.MetadataGeneration.State != SourceValue {
		return fmt.Errorf("%w: instrument metadata generation is unavailable", ErrInvalidNormalized)
	}
	if err := e.NativeStateBefore.Validate(); err != nil {
		return err
	}
	if err := e.NativeStateAfter.Validate(); err != nil {
		return err
	}
	for _, field := range []TimeField{e.ListingTime, e.ContinuousTradingTime, e.ExpiryTime, e.DeliveryTime, e.DelistingTime} {
		if err := field.Validate(); err != nil {
			return err
		}
	}
	if err := e.TickSize.Validate(); err != nil {
		return err
	}
	if err := e.LotSize.Validate(); err != nil {
		return err
	}
	if err := e.ContractMultiplier.Validate(); err != nil {
		return err
	}
	if err := e.Payoff.Validate(); err != nil {
		return err
	}
	if err := e.OldRawHash.Validate(); err != nil {
		return err
	}
	if err := e.NewRawHash.Validate(); err != nil || e.NewRawHash.State != SourceValue {
		return fmt.Errorf("%w: instrument event lacks new raw metadata hash", ErrInvalidNormalized)
	}
	if err := e.ResolutionStatus.Validate(); err != nil || e.ResolutionStatus.State != SourceValue {
		return fmt.Errorf("%w: instrument event lacks resolution status", ErrInvalidNormalized)
	}
	changed := e.NativeStateBefore.State != SourceMissing || e.NativeStateAfter.State != SourceMissing ||
		e.ListingTime.State != SourceMissing || e.ContinuousTradingTime.State != SourceMissing ||
		e.ExpiryTime.State != SourceMissing || e.DeliveryTime.State != SourceMissing || e.DelistingTime.State != SourceMissing ||
		e.TickSize.observed() || e.LotSize.observed() || e.ContractMultiplier.observed() || e.Payoff.observed()
	if !changed {
		return fmt.Errorf("%w: instrument event contains no transition or term change", ErrInvalidNormalized)
	}
	if e.NativeStateBefore.State == SourceValue && e.NativeStateAfter.State == SourceValue && e.NativeStateBefore.Value == e.NativeStateAfter.Value &&
		!e.TickSize.observed() && !e.LotSize.observed() && !e.ContractMultiplier.observed() && !e.Payoff.observed() {
		return fmt.Errorf("%w: instrument native state did not transition", ErrInvalidNormalized)
	}
	return nil
}
