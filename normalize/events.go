package normalize

import (
	"fmt"
	"slices"
)

type EventKind string

const (
	EventTrade      EventKind = "trade"
	EventBookUpdate EventKind = "book_update"
	EventQuote      EventKind = "quote"
	EventTicker     EventKind = "ticker"
)

// Row is a closed v1 union. Exactly one event pointer is present and its
// logical hash covers the complete event, including immutable raw coordinates.
type Row struct {
	Kind                   EventKind
	Trade                  *TradeV1
	BookUpdate             *BookUpdateV1
	Quote                  *QuoteV1
	Ticker                 *TickerV1
	LogicalEncodingVersion uint16
	LogicalHash            Hash
}

func NewTradeRow(event TradeV1) (Row, error) {
	row := Row{Kind: EventTrade, Trade: &event, LogicalEncodingVersion: LogicalEncodingVersion}
	return finalizeRow(row)
}

func NewBookUpdateRow(event BookUpdateV1) (Row, error) {
	event.Bids = slices.Clone(event.Bids)
	event.Asks = slices.Clone(event.Asks)
	row := Row{Kind: EventBookUpdate, BookUpdate: &event, LogicalEncodingVersion: LogicalEncodingVersion}
	return finalizeRow(row)
}

func NewQuoteRow(event QuoteV1) (Row, error) {
	row := Row{Kind: EventQuote, Quote: &event, LogicalEncodingVersion: LogicalEncodingVersion}
	return finalizeRow(row)
}

func NewTickerRow(event TickerV1) (Row, error) {
	row := Row{Kind: EventTicker, Ticker: &event, LogicalEncodingVersion: LogicalEncodingVersion}
	return finalizeRow(row)
}

func finalizeRow(row Row) (Row, error) {
	if err := row.validateEvent(); err != nil {
		return Row{}, err
	}
	row.LogicalHash = logicalHash(row)
	if err := row.Validate(); err != nil {
		return Row{}, err
	}
	return row, nil
}

func (r Row) Common() Metadata {
	switch r.Kind {
	case EventTrade:
		if r.Trade != nil {
			return r.Trade.Metadata
		}
	case EventBookUpdate:
		if r.BookUpdate != nil {
			return r.BookUpdate.Metadata
		}
	case EventQuote:
		if r.Quote != nil {
			return r.Quote.Metadata
		}
	case EventTicker:
		if r.Ticker != nil {
			return r.Ticker.Metadata
		}
	}
	return Metadata{}
}

func (r Row) Validate() error {
	if r.LogicalEncodingVersion != LogicalEncodingVersion || r.LogicalHash == (Hash{}) {
		return fmt.Errorf("%w: invalid logical encoding identity", ErrInvalidNormalized)
	}
	if err := r.validateEvent(); err != nil {
		return err
	}
	if r.LogicalHash != logicalHash(r) {
		return fmt.Errorf("%w: logical hash mismatch", ErrInvalidNormalized)
	}
	return nil
}

func (r Row) validateEvent() error {
	present := 0
	if r.Trade != nil {
		present++
	}
	if r.BookUpdate != nil {
		present++
	}
	if r.Quote != nil {
		present++
	}
	if r.Ticker != nil {
		present++
	}
	if present != 1 {
		return fmt.Errorf("%w: row must contain exactly one event", ErrInvalidNormalized)
	}
	switch r.Kind {
	case EventTrade:
		return r.Trade.Validate()
	case EventBookUpdate:
		return r.BookUpdate.Validate()
	case EventQuote:
		return r.Quote.Validate()
	case EventTicker:
		return r.Ticker.Validate()
	default:
		return fmt.Errorf("%w: unknown row kind", ErrInvalidNormalized)
	}
}

func validateSchema(metadata Metadata, name string, version uint16) error {
	if err := metadata.Validate(); err != nil {
		return err
	}
	if metadata.SchemaName != name || metadata.SchemaVersion != version {
		return fmt.Errorf("%w: schema identity mismatch", ErrInvalidNormalized)
	}
	return nil
}

func (e TradeV1) Validate() error {
	if err := validateSchema(e.Metadata, TradeSchemaName, TradeSchemaVersion); err != nil {
		return err
	}
	if e.Metadata.ExchangeTimeResolution != e.Metadata.SourceTimeResolution ||
		(e.AggressorSide != SideBuy && e.AggressorSide != SideSell) {
		return fmt.Errorf("%w: invalid trade side", ErrInvalidNormalized)
	}
	if e.AggregationKind != AggregationSingleMatch || e.NativeDuplicateStatus != DuplicateUnassessed {
		return fmt.Errorf("%w: invalid trade contract", ErrInvalidNormalized)
	}
	if err := e.Price.Validate(); err != nil {
		return err
	}
	if err := e.Amount.Validate(); err != nil {
		return err
	}
	if e.Price.Unit.Kind != UnitQuotePerBase || e.Amount.Unit.Kind != UnitBaseAssetAmount ||
		e.Price.Unit.BaseAssetID != e.Amount.Unit.AssetID {
		return fmt.Errorf("%w: inconsistent trade units", ErrInvalidNormalized)
	}
	return nil
}

func validateLevels(levels []BookLevel, side Side) error {
	if len(levels) > MaxBookLevelsPerSide {
		return fmt.Errorf("%w: book side exceeds level bound", ErrInvalidNormalized)
	}
	for i, level := range levels {
		if level.Side != side || level.LevelOrdinal != uint32(i) || (level.Action != LevelUpsert && level.Action != LevelDelete) {
			return fmt.Errorf("%w: invalid source-order book level", ErrInvalidNormalized)
		}
		if err := level.Price.Validate(); err != nil {
			return err
		}
		if err := level.Amount.Validate(); err != nil {
			return err
		}
		if level.Price.Unit.Kind != UnitQuotePerBase || level.Amount.Unit.Kind != UnitBaseAssetAmount ||
			level.Price.Unit.BaseAssetID != level.Amount.Unit.AssetID {
			return fmt.Errorf("%w: inconsistent book level units", ErrInvalidNormalized)
		}
		if (level.Action == LevelDelete) != level.Amount.Decimal.IsZero() {
			return fmt.Errorf("%w: book action does not match zero amount", ErrInvalidNormalized)
		}
	}
	return nil
}

func (e BookUpdateV1) Validate() error {
	if err := validateSchema(e.Metadata, BookUpdateSchemaName, BookUpdateSchemaVersion); err != nil {
		return err
	}
	if e.Metadata.ExchangeTimeResolution != e.Metadata.SourceTimeResolution ||
		e.UpdateKind != UpdateDelta || e.DepthContract != "diff_depth" || e.AggregationContract != "100ms" ||
		e.AmountSemantics != "absolute_base_asset_quantity" || e.ReconstructionEligibility != "eligible_with_rest_snapshot_bridge" ||
		e.FirstSequence > e.LastSequence || e.Checksum != SourceMissing {
		return fmt.Errorf("%w: invalid Binance Spot book contract", ErrInvalidNormalized)
	}
	if e.PreviousSequence.Valid {
		return fmt.Errorf("%w: Binance Spot diff depth has no native previous sequence", ErrInvalidNormalized)
	}
	if err := validateLevels(e.Bids, SideBuy); err != nil {
		return err
	}
	return validateLevels(e.Asks, SideSell)
}

func (e QuoteV1) Validate() error {
	if err := validateSchema(e.Metadata, QuoteSchemaName, QuoteSchemaVersion); err != nil {
		return err
	}
	if e.NativeSourceRole != "bookTicker_native_bbo" || e.RPIInclusionState != RPINotApplicable || e.SourceTimeNS.Valid {
		return fmt.Errorf("%w: invalid native quote role", ErrInvalidNormalized)
	}
	for _, value := range []Numeric{e.BidPrice, e.BidAmount, e.AskPrice, e.AskAmount} {
		if err := value.Validate(); err != nil {
			return err
		}
	}
	if e.BidPrice.Unit.Kind != UnitQuotePerBase || e.AskPrice.Unit != e.BidPrice.Unit ||
		e.BidAmount.Unit.Kind != UnitBaseAssetAmount || e.AskAmount.Unit != e.BidAmount.Unit ||
		e.BidPrice.Unit.BaseAssetID != e.BidAmount.Unit.AssetID {
		return fmt.Errorf("%w: inconsistent quote units", ErrInvalidNormalized)
	}
	return nil
}

func (e TickerV1) Validate() error {
	if err := validateSchema(e.Metadata, TickerSchemaName, TickerSchemaVersion); err != nil {
		return err
	}
	if e.NativeSourceRole != "24hrTicker_statistics_not_bbo" || e.WindowKind != WindowRolling24Hours ||
		e.WindowOpenSemantics != "native_statistics_open_time" || e.WindowCloseSemantics != "native_statistics_close_time" ||
		e.WindowOpenTimeNS < 0 || e.WindowCloseTimeNS < e.WindowOpenTimeNS ||
		uint64(e.WindowCloseTimeNS-e.WindowOpenTimeNS) != e.NominalWindowDurationNS ||
		e.WindowTimeResolution == ResolutionAbsent || e.WindowTimeResolution != e.Metadata.SourceTimeResolution ||
		e.Metadata.ExchangeTimeResolution != e.Metadata.SourceTimeResolution ||
		e.NominalWindowDurationNS != 86_400_000_000_000 || e.FirstTradeID > e.LastTradeID {
		return fmt.Errorf("%w: invalid ticker window or sequence contract", ErrInvalidNormalized)
	}
	prices := []Numeric{e.PriceChange, e.WeightedAveragePrice, e.FirstTradeBeforeWindowPrice, e.LastPrice,
		e.NativeBestBidPrice, e.NativeBestAskPrice, e.OpenPrice, e.HighPrice, e.LowPrice}
	for _, value := range prices {
		if err := value.Validate(); err != nil || value.Unit.Kind != UnitQuotePerBase {
			return fmt.Errorf("%w: invalid ticker price", ErrInvalidNormalized)
		}
	}
	if err := e.PriceChangePercent.Validate(); err != nil || e.PriceChangePercent.Unit.Kind != UnitPercent {
		return fmt.Errorf("%w: invalid ticker percentage", ErrInvalidNormalized)
	}
	baseAmounts := []Numeric{e.LastAmount, e.NativeBestBidAmount, e.NativeBestAskAmount, e.BaseVolume}
	for _, value := range baseAmounts {
		if err := value.Validate(); err != nil || value.Unit.Kind != UnitBaseAssetAmount {
			return fmt.Errorf("%w: invalid ticker base amount", ErrInvalidNormalized)
		}
	}
	if err := e.QuoteVolume.Validate(); err != nil || e.QuoteVolume.Unit.Kind != UnitQuoteAssetAmount {
		return fmt.Errorf("%w: invalid ticker quote amount", ErrInvalidNormalized)
	}
	priceUnit := e.LastPrice.Unit
	for _, value := range prices {
		if value.Unit != priceUnit {
			return fmt.Errorf("%w: ticker price unit mismatch", ErrInvalidNormalized)
		}
	}
	baseUnit := e.BaseVolume.Unit
	for _, value := range baseAmounts {
		if value.Unit != baseUnit {
			return fmt.Errorf("%w: ticker base unit mismatch", ErrInvalidNormalized)
		}
	}
	if priceUnit.BaseAssetID != baseUnit.AssetID || priceUnit.QuoteAssetID != e.QuoteVolume.Unit.AssetID {
		return fmt.Errorf("%w: ticker asset identity mismatch", ErrInvalidNormalized)
	}
	return nil
}
