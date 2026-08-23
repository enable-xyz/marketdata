package orderbook

import (
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type storedLevel struct {
	price       normalize.Decimal
	amount      normalize.Decimal
	coefficient *big.Int
}

type levelChange struct {
	key    string
	delete bool
	level  storedLevel
}

type bookState struct {
	bids map[string]storedLevel
	asks map[string]storedLevel
}

func prepareSnapshot(config Config, snapshot SnapshotObservation) (bookState, error) {
	if uint32(len(snapshot.Bids)) > config.Bounds.MaxLevelsPerSide || uint32(len(snapshot.Asks)) > config.Bounds.MaxLevelsPerSide {
		return bookState{}, ErrLevelLimit
	}
	candidate := bookState{
		bids: make(map[string]storedLevel, len(snapshot.Bids)),
		asks: make(map[string]storedLevel, len(snapshot.Asks)),
	}
	for _, level := range snapshot.Bids {
		stored, err := validateStoredLevel(config, level.Price, level.Amount, false)
		if err != nil {
			return bookState{}, fmt.Errorf("snapshot bid: %w", err)
		}
		candidate.bids[stored.price.Coefficient] = stored
	}
	for _, level := range snapshot.Asks {
		stored, err := validateStoredLevel(config, level.Price, level.Amount, false)
		if err != nil {
			return bookState{}, fmt.Errorf("snapshot ask: %w", err)
		}
		candidate.asks[stored.price.Coefficient] = stored
	}
	if uint32(len(candidate.bids)) > config.Bounds.MaxLevelsPerSide || uint32(len(candidate.asks)) > config.Bounds.MaxLevelsPerSide {
		return bookState{}, ErrLevelLimit
	}
	if config.RejectCrossed && crossed(candidate.bids, candidate.asks, nil, nil) {
		return bookState{}, ErrCrossedBook
	}
	return candidate, nil
}

func applyEventAtomically(config Config, state *bookState, event normalize.BookUpdateV1) error {
	bidChanges, finalBids, err := prepareChanges(config, event.Bids, normalize.SideBuy)
	if err != nil {
		return err
	}
	askChanges, finalAsks, err := prepareChanges(config, event.Asks, normalize.SideSell)
	if err != nil {
		return err
	}
	if projectedLevelCount(state.bids, finalBids) > int(config.Bounds.MaxLevelsPerSide) ||
		projectedLevelCount(state.asks, finalAsks) > int(config.Bounds.MaxLevelsPerSide) {
		return ErrLevelLimit
	}
	if config.RejectCrossed && crossed(state.bids, state.asks, finalBids, finalAsks) {
		return ErrCrossedBook
	}
	applyChanges(state.bids, bidChanges)
	applyChanges(state.asks, askChanges)
	return nil
}

func prepareChanges(config Config, levels []normalize.BookLevel, side normalize.Side) ([]levelChange, map[string]levelChange, error) {
	if uint32(len(levels)) > config.Bounds.MaxLevelsPerSide {
		return nil, nil, ErrLevelLimit
	}
	changes := make([]levelChange, 0, len(levels))
	final := make(map[string]levelChange, len(levels))
	for index, level := range levels {
		if level.Side != side || level.LevelOrdinal != uint32(index) {
			return nil, nil, fmt.Errorf("%w: source order", ErrInvalidLevel)
		}
		deleting := level.Action == normalize.LevelDelete
		if !deleting && level.Action != normalize.LevelUpsert {
			return nil, nil, fmt.Errorf("%w: action", ErrInvalidLevel)
		}
		stored, err := validateStoredLevel(config, level.Price, level.Amount, deleting)
		if err != nil {
			return nil, nil, err
		}
		change := levelChange{key: stored.price.Coefficient, delete: deleting, level: stored}
		changes = append(changes, change)
		final[change.key] = change
	}
	return changes, final, nil
}

func validateStoredLevel(config Config, price, amount normalize.Numeric, deleting bool) (storedLevel, error) {
	if err := price.Validate(); err != nil {
		return storedLevel{}, fmt.Errorf("%w: price: %v", ErrInvalidLevel, err)
	}
	if err := amount.Validate(); err != nil {
		return storedLevel{}, fmt.Errorf("%w: amount: %v", ErrInvalidLevel, err)
	}
	priceCoefficient := price.Decimal.Coefficient
	amountCoefficient := amount.Decimal.Coefficient
	if strings.HasPrefix(priceCoefficient, "-") || strings.HasPrefix(amountCoefficient, "-") {
		return storedLevel{}, ErrNegativeLevel
	}
	if priceCoefficient == "0" || deleting != (amountCoefficient == "0") {
		return storedLevel{}, fmt.Errorf("%w: non-positive price or action/amount mismatch", ErrInvalidLevel)
	}
	if price.Decimal.Scale != normalize.CanonicalPriceScale || amount.Decimal.Scale != normalize.CanonicalAmountScale ||
		price.Unit != normalize.SpotPriceUnit(config.Instrument.BaseAssetID, config.Instrument.QuoteAssetID) ||
		amount.Unit != normalize.BaseAssetUnit(config.Instrument.BaseAssetID) {
		return storedLevel{}, fmt.Errorf("%w: canonical scale or unit mismatch", ErrInvalidLevel)
	}
	wide, ok := new(big.Int).SetString(priceCoefficient, 10)
	if !ok || wide.Sign() <= 0 {
		return storedLevel{}, fmt.Errorf("%w: price coefficient", ErrInvalidLevel)
	}
	return storedLevel{price: price.Decimal, amount: amount.Decimal, coefficient: wide}, nil
}

func projectedLevelCount(levels map[string]storedLevel, changes map[string]levelChange) int {
	count := len(levels)
	for key, change := range changes {
		_, exists := levels[key]
		switch {
		case change.delete && exists:
			count--
		case !change.delete && !exists:
			count++
		}
	}
	return count
}

func applyChanges(levels map[string]storedLevel, changes []levelChange) {
	for _, change := range changes {
		if change.delete {
			delete(levels, change.key)
			continue
		}
		levels[change.key] = change.level
	}
}

func crossed(bids, asks map[string]storedLevel, bidChanges, askChanges map[string]levelChange) bool {
	bestBid := effectiveBest(bids, bidChanges, true)
	bestAsk := effectiveBest(asks, askChanges, false)
	return bestBid != nil && bestAsk != nil && bestBid.Cmp(bestAsk) >= 0
}

func effectiveBest(levels map[string]storedLevel, changes map[string]levelChange, maximum bool) *big.Int {
	var best *big.Int
	for key, level := range levels {
		if _, changed := changes[key]; changed {
			continue
		}
		best = chooseBest(best, level.coefficient, maximum)
	}
	for _, change := range changes {
		if change.delete {
			continue
		}
		best = chooseBest(best, change.level.coefficient, maximum)
	}
	return best
}

func chooseBest(current, candidate *big.Int, maximum bool) *big.Int {
	if current == nil || (maximum && candidate.Cmp(current) > 0) || (!maximum && candidate.Cmp(current) < 0) {
		return candidate
	}
	return current
}

func projectedLevels(levels map[string]storedLevel, descending bool) []Level {
	ordered := make([]storedLevel, 0, len(levels))
	for _, level := range levels {
		ordered = append(ordered, level)
	}
	slices.SortFunc(ordered, func(left, right storedLevel) int {
		comparison := left.coefficient.Cmp(right.coefficient)
		if descending {
			return -comparison
		}
		return comparison
	})
	result := make([]Level, len(ordered))
	for index, level := range ordered {
		result[index] = Level{Price: level.price, Amount: level.amount}
	}
	return result
}
