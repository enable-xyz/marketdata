package normalize

import (
	"fmt"
	"strings"
)

const (
	LiquidationSchemaName             = "LiquidationV1"
	LiquidationSchemaVersion   uint16 = 1
	MaxLiquidationRoleBytes           = 128
	MaxLiquidationBatchIDBytes        = 256
)

type LiquidationNativeRole string

const (
	LiquidationNativeEvent    LiquidationNativeRole = "event"
	LiquidationNativeSnapshot LiquidationNativeRole = "snapshot"
)

type LiquidationSideSemantics string

const (
	LiquidationOrderSide              LiquidationSideSemantics = "order_side"
	LiquidationLiquidatedPositionSide LiquidationSideSemantics = "liquidated_position_side"
	LiquidationSideUnspecified        LiquidationSideSemantics = "unspecified"
)

type LiquidationPriceType string

const (
	LiquidationOrderPrice       LiquidationPriceType = "order_price"
	LiquidationAverageFillPrice LiquidationPriceType = "average_fill_price"
	LiquidationBankruptcyPrice  LiquidationPriceType = "bankruptcy_price"
	LiquidationMarkPrice        LiquidationPriceType = "mark_price"
	LiquidationPriceUnspecified LiquidationPriceType = "unspecified"
)

type LiquidationCompleteness string

const (
	LiquidationComplete                LiquidationCompleteness = "complete"
	LiquidationLargestInWindow         LiquidationCompleteness = "largest_in_window"
	LiquidationPartialNonchronological LiquidationCompleteness = "partial_nonchronological"
	LiquidationTradeFlagOnly           LiquidationCompleteness = "trade_flag_only"
)

type LiquidationWindowSelection string

const (
	LiquidationAllObserved            LiquidationWindowSelection = "all_observed"
	LiquidationLargestPerSymbol       LiquidationWindowSelection = "largest_per_symbol"
	LiquidationWindowSelectionUnknown LiquidationWindowSelection = "unknown"
)

type LiquidationWindow struct {
	StartTimeNS OptionalInt64
	EndTimeNS   OptionalInt64
	DurationNS  uint64
	Selection   LiquidationWindowSelection
	PerSymbol   bool
	BatchID     string
}

func (w LiquidationWindow) Validate(completeness LiquidationCompleteness) error {
	if w.StartTimeNS.Valid != w.EndTimeNS.Valid || (!w.StartTimeNS.Valid && (w.StartTimeNS.Value != 0 || w.EndTimeNS.Value != 0)) {
		return fmt.Errorf("%w: liquidation window bounds mismatch", ErrInvalidNormalized)
	}
	if w.StartTimeNS.Valid && (w.StartTimeNS.Value < 0 || w.EndTimeNS.Value < w.StartTimeNS.Value || uint64(w.EndTimeNS.Value-w.StartTimeNS.Value) != w.DurationNS) {
		return fmt.Errorf("%w: invalid liquidation window bounds", ErrInvalidNormalized)
	}
	if len(w.BatchID) > MaxLiquidationBatchIDBytes || strings.IndexByte(w.BatchID, 0) >= 0 {
		return fmt.Errorf("%w: invalid liquidation batch identity", ErrInvalidNormalized)
	}
	if w.Selection != LiquidationAllObserved && w.Selection != LiquidationLargestPerSymbol && w.Selection != LiquidationWindowSelectionUnknown {
		return fmt.Errorf("%w: unknown liquidation window selection", ErrInvalidNormalized)
	}
	switch completeness {
	case LiquidationLargestInWindow:
		if w.DurationNS == 0 || w.Selection != LiquidationLargestPerSymbol || !w.PerSymbol {
			return fmt.Errorf("%w: largest-in-window liquidation lacks bounded selection", ErrInvalidNormalized)
		}
	case LiquidationComplete:
		if w.Selection != LiquidationAllObserved || w.DurationNS != 0 || w.PerSymbol {
			return fmt.Errorf("%w: complete liquidation cannot use lossy window selection", ErrInvalidNormalized)
		}
	case LiquidationPartialNonchronological, LiquidationTradeFlagOnly:
		if w.Selection == "" {
			return fmt.Errorf("%w: partial liquidation lacks source selection", ErrInvalidNormalized)
		}
	default:
		return fmt.Errorf("%w: unknown liquidation completeness", ErrInvalidNormalized)
	}
	return nil
}

type LiquidationV1 struct {
	Metadata         Metadata
	NativeSourceRole string
	NativeRole       LiquidationNativeRole
	Side             Side
	SideSemantics    LiquidationSideSemantics
	Amount           NativeValue
	Price            NumericField
	PriceType        LiquidationPriceType
	Completeness     LiquidationCompleteness
	Window           LiquidationWindow
}

func (e LiquidationV1) Validate() error {
	if err := validateSchema(e.Metadata, LiquidationSchemaName, LiquidationSchemaVersion); err != nil {
		return err
	}
	if e.NativeSourceRole == "" || len(e.NativeSourceRole) > MaxLiquidationRoleBytes || strings.IndexByte(e.NativeSourceRole, 0) >= 0 {
		return fmt.Errorf("%w: invalid liquidation source role", ErrInvalidNormalized)
	}
	if e.NativeRole != LiquidationNativeEvent && e.NativeRole != LiquidationNativeSnapshot {
		return fmt.Errorf("%w: invalid liquidation native role", ErrInvalidNormalized)
	}
	if e.Side != SideBuy && e.Side != SideSell {
		return fmt.Errorf("%w: invalid liquidation side", ErrInvalidNormalized)
	}
	if e.SideSemantics != LiquidationOrderSide && e.SideSemantics != LiquidationLiquidatedPositionSide && e.SideSemantics != LiquidationSideUnspecified {
		return fmt.Errorf("%w: invalid liquidation side semantics", ErrInvalidNormalized)
	}
	if err := e.Amount.Validate(); err != nil {
		return err
	}
	if err := e.Price.Validate(); err != nil {
		return err
	}
	if e.Price.State != SourceValue || (e.PriceType != LiquidationOrderPrice && e.PriceType != LiquidationAverageFillPrice && e.PriceType != LiquidationBankruptcyPrice && e.PriceType != LiquidationMarkPrice && e.PriceType != LiquidationPriceUnspecified) {
		return fmt.Errorf("%w: invalid liquidation price", ErrInvalidNormalized)
	}
	return e.Window.Validate(e.Completeness)
}
