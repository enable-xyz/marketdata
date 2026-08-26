package orderbook

import (
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

const DeribitContinuityRuleVersion = "deribit_prev_change_id_resubscribe_v1"

var (
	ErrDeribitBookInvalid      = errors.New("orderbook: invalid Deribit book update")
	ErrDeribitGroupedBook      = errors.New("orderbook: grouped Deribit view is not reconstructable")
	ErrDeribitChangeIDGap      = errors.New("orderbook: Deribit change-ID continuity gap")
	ErrDeribitProvisionalUnit  = errors.New("orderbook: Deribit amount unit is provisional")
	ErrDeribitMutationMismatch = errors.New("orderbook: Deribit action does not match local state")
)

type DeribitRecoveryAction string

const (
	DeribitRecoveryNone        DeribitRecoveryAction = "none"
	DeribitRecoveryResubscribe DeribitRecoveryAction = "resubscribe"
	DeribitRecoverySnapshot    DeribitRecoveryAction = "snapshot_recovered"
)

type DeribitTransition struct {
	Applied         bool
	Seeded          bool
	PriorChangeID   uint64
	ChangeID        uint64
	Recovery        DeribitRecoveryAction
	Authority       capture.RuleAuthority
	RuleVersion     string
	SourceGuarantee bool
}

type DeribitLevel struct {
	Price  normalize.Numeric
	Amount normalize.NativeValue
}

type DeribitSnapshot struct {
	InstrumentUID string
	ChangeID      uint64
	Bids          []DeribitLevel
	Asks          []DeribitLevel
}

// DeribitBook applies only the reconstructable book.{instrument}.{interval}
// stream. Resubscription on mismatch is an adapter-policy inference; it is
// deliberately carried on every transition and never labelled a source rule.
type DeribitBook struct {
	instrumentUID string
	seeded        bool
	invalidated   bool
	changeID      uint64
	bids          map[string]DeribitLevel
	asks          map[string]DeribitLevel
}

func NewDeribitBook(instrumentUID string) (*DeribitBook, error) {
	if instrumentUID == "" {
		return nil, ErrDeribitBookInvalid
	}
	return &DeribitBook{
		instrumentUID: instrumentUID,
		bids:          make(map[string]DeribitLevel), asks: make(map[string]DeribitLevel),
	}, nil
}

func (b *DeribitBook) Apply(update normalize.DeribitBookUpdate) (DeribitTransition, error) {
	transition := DeribitTransition{
		Recovery: DeribitRecoveryNone, Authority: capture.RuleAdapterPolicyInference,
		RuleVersion: DeribitContinuityRuleVersion, SourceGuarantee: false,
	}
	if b == nil || update.InstrumentUID != b.instrumentUID {
		return transition, ErrDeribitBookInvalid
	}
	if err := update.Validate(); err != nil {
		return transition, fmt.Errorf("%w: %v", ErrDeribitBookInvalid, err)
	}
	if update.GroupedView {
		return transition, ErrDeribitGroupedBook
	}
	if update.UnitInference.State != normalize.DeribitInferenceFixtureProven {
		return transition, ErrDeribitProvisionalUnit
	}
	transition.PriorChangeID = b.changeID
	transition.ChangeID = update.ChangeID
	if update.Kind == normalize.DeribitBookSnapshot {
		bids, asks, err := buildDeribitSnapshot(update)
		if err != nil {
			b.invalidate()
			transition.Recovery = DeribitRecoveryResubscribe
			return transition, err
		}
		wasInvalid := b.invalidated
		b.bids, b.asks = bids, asks
		b.seeded, b.invalidated, b.changeID = true, false, update.ChangeID
		transition.Applied, transition.Seeded = true, true
		if wasInvalid {
			transition.Recovery = DeribitRecoverySnapshot
		}
		return transition, nil
	}
	if !b.seeded || b.invalidated || !update.PreviousID.Valid || update.PreviousID.Value != b.changeID {
		b.invalidate()
		transition.Recovery = DeribitRecoveryResubscribe
		return transition, ErrDeribitChangeIDGap
	}
	bids := cloneDeribitSide(b.bids)
	asks := cloneDeribitSide(b.asks)
	if err := applyDeribitLevels(bids, update.Bids); err != nil {
		b.invalidate()
		transition.Recovery = DeribitRecoveryResubscribe
		return transition, err
	}
	if err := applyDeribitLevels(asks, update.Asks); err != nil {
		b.invalidate()
		transition.Recovery = DeribitRecoveryResubscribe
		return transition, err
	}
	b.bids, b.asks, b.changeID = bids, asks, update.ChangeID
	transition.Applied, transition.Seeded = true, true
	return transition, nil
}

func (b *DeribitBook) invalidate() {
	b.seeded, b.invalidated, b.changeID = false, true, 0
	clear(b.bids)
	clear(b.asks)
}

func buildDeribitSnapshot(update normalize.DeribitBookUpdate) (map[string]DeribitLevel, map[string]DeribitLevel, error) {
	bids := make(map[string]DeribitLevel, len(update.Bids))
	asks := make(map[string]DeribitLevel, len(update.Asks))
	for _, side := range []struct {
		destination map[string]DeribitLevel
		levels      []normalize.DeribitBookLevel
	}{{bids, update.Bids}, {asks, update.Asks}} {
		for _, level := range side.levels {
			if level.Action == normalize.DeribitBookDelete || level.Amount.Decimal.IsZero() {
				return nil, nil, ErrDeribitMutationMismatch
			}
			key := deribitPriceKey(level.Price)
			if _, exists := side.destination[key]; exists {
				return nil, nil, ErrDeribitMutationMismatch
			}
			side.destination[key] = DeribitLevel{Price: level.Price, Amount: level.Amount}
		}
	}
	return bids, asks, nil
}

func applyDeribitLevels(side map[string]DeribitLevel, levels []normalize.DeribitBookLevel) error {
	for _, level := range levels {
		key := deribitPriceKey(level.Price)
		_, exists := side[key]
		switch level.Action {
		case normalize.DeribitBookNew:
			if exists {
				return ErrDeribitMutationMismatch
			}
			side[key] = DeribitLevel{Price: level.Price, Amount: level.Amount}
		case normalize.DeribitBookModify:
			if !exists {
				return ErrDeribitMutationMismatch
			}
			side[key] = DeribitLevel{Price: level.Price, Amount: level.Amount}
		case normalize.DeribitBookDelete:
			if !exists {
				return ErrDeribitMutationMismatch
			}
			delete(side, key)
		default:
			return ErrDeribitBookInvalid
		}
	}
	return nil
}

func cloneDeribitSide(source map[string]DeribitLevel) map[string]DeribitLevel {
	clone := make(map[string]DeribitLevel, len(source))
	for key, level := range source {
		clone[key] = level
	}
	return clone
}

func deribitPriceKey(price normalize.Numeric) string {
	return fmt.Sprintf("%d:%s", price.Decimal.Scale, price.Decimal.Coefficient)
}

func (b *DeribitBook) Snapshot() (DeribitSnapshot, error) {
	if b == nil || !b.seeded || b.invalidated {
		return DeribitSnapshot{}, ErrDeribitBookInvalid
	}
	bids := slices.Collect(mapDeribitLevels(b.bids))
	asks := slices.Collect(mapDeribitLevels(b.asks))
	slices.SortFunc(bids, func(left, right DeribitLevel) int { return -compareDeribitPrice(left.Price, right.Price) })
	slices.SortFunc(asks, func(left, right DeribitLevel) int { return compareDeribitPrice(left.Price, right.Price) })
	return DeribitSnapshot{InstrumentUID: b.instrumentUID, ChangeID: b.changeID, Bids: bids, Asks: asks}, nil
}

func mapDeribitLevels(source map[string]DeribitLevel) func(func(DeribitLevel) bool) {
	return func(yield func(DeribitLevel) bool) {
		for _, level := range source {
			if !yield(level) {
				return
			}
		}
	}
}

func compareDeribitPrice(left, right normalize.Numeric) int {
	leftWide, leftOK := new(big.Int).SetString(left.Decimal.Coefficient, 10)
	rightWide, rightOK := new(big.Int).SetString(right.Decimal.Coefficient, 10)
	if !leftOK || !rightOK {
		return 0
	}
	return leftWide.Cmp(rightWide)
}
