package hyperliquid

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/normalize"
)

const (
	DuplicatePolicyPreserveUnassessed = "preserve_unassessed_raw_rows_v1"
	BookUpdateClaimFullReplacement    = "full_depth_limited_replacement_snapshot"
	BookContinuityNoSequence          = "no_native_sequence_continuity_unobservable"
)

type TradeKey struct {
	BlockTimeMS int64
	Coin        string
	TradeID     uint64
}

type Trade struct {
	Coin                  string
	Side                  string
	Price                 string
	Size                  string
	TransactionHash       string
	TimeMS                int64
	TradeID               uint64
	Users                 [2]string
	NativeDuplicatePolicy string
	MessageOrdinal        uint32
	Evidence              *RawEvidence
	binding               [sha256.Size]byte
}

func (t Trade) Key() TradeKey {
	return TradeKey{BlockTimeMS: t.TimeMS, Coin: t.Coin, TradeID: t.TradeID}
}

func ParseTrades(payload []byte) ([]Trade, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return nil, err
	}
	var message struct {
		Channel string            `json:"channel"`
		Data    []json.RawMessage `json:"data"`
	}
	if json.Unmarshal(payload, &message) != nil || message.Channel != "trades" || len(message.Data) == 0 || len(message.Data) > 4096 {
		return nil, ErrInvalidPayload
	}
	trades := make([]Trade, len(message.Data))
	for index, raw := range message.Data {
		var native struct {
			Coin  string   `json:"coin"`
			Side  string   `json:"side"`
			Price string   `json:"px"`
			Size  string   `json:"sz"`
			Hash  string   `json:"hash"`
			Time  *int64   `json:"time"`
			TID   *uint64  `json:"tid"`
			Users []string `json:"users"`
		}
		if json.Unmarshal(raw, &native) != nil || !validCoin(native.Coin) || (native.Side != "A" && native.Side != "B") ||
			!validDecimalText(native.Price) || !validDecimalText(native.Size) || !validTransactionHash(native.Hash) ||
			native.Time == nil || *native.Time < 0 || native.TID == nil || *native.TID >= 1<<50 || len(native.Users) != 2 ||
			!validHyperliquidAddress(native.Users[0]) || !validHyperliquidAddress(native.Users[1]) {
			return nil, fmt.Errorf("%w: trade ordinal %d", ErrInvalidPayload, index)
		}
		trade := Trade{
			Coin: native.Coin, Side: native.Side, Price: native.Price, Size: native.Size,
			TransactionHash: native.Hash, TimeMS: *native.Time, TradeID: *native.TID,
			Users: [2]string{native.Users[0], native.Users[1]}, NativeDuplicatePolicy: DuplicatePolicyPreserveUnassessed,
			MessageOrdinal: uint32(index), Evidence: evidence,
		}
		trade.binding = tradeBinding(trade)
		trades[index] = trade
	}
	return trades, nil
}

func (t Trade) validateEvidenceBinding() error {
	if !t.Evidence.Valid() || t.binding == ([sha256.Size]byte{}) || t.binding != tradeBinding(t) {
		return ErrInvalidPayload
	}
	return nil
}
func (t Trade) AggressorSide() (normalize.Side, error) {
	if err := t.validateEvidenceBinding(); err != nil {
		return "", err
	}
	switch t.Side {
	case "B":
		return normalize.SideBuy, nil
	case "A":
		return normalize.SideSell, nil
	default:
		return "", ErrInvalidPayload
	}
}

func (t Trade) PriceDecimal() (normalize.Decimal, error) {
	if err := t.validateEvidenceBinding(); err != nil {
		return normalize.Decimal{}, err
	}
	return normalize.ParseDecimal(t.Price, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
}

func (t Trade) AmountValue(identity catalog.HyperliquidInstrumentIdentity, baseAssetID string, provenance normalize.FieldProvenance) (normalize.HyperliquidEconomicValue, error) {
	if err := identity.Validate(); err != nil || identity.WireCoin != t.Coin || t.NativeDuplicatePolicy != DuplicatePolicyPreserveUnassessed || t.validateEvidenceBinding() != nil {
		return normalize.HyperliquidEconomicValue{}, ErrInvalidPayload
	}
	decimal, err := normalize.ParseDecimal(t.Size, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.HyperliquidEconomicValue{}, err
	}
	if identity.Family == catalog.HyperliquidHIP3 {
		return normalize.NewHyperliquidHIP3ProvisionalValue(decimal, identity.InstrumentUID, "trade_size", identity.GenerationHex())
	}
	return normalize.NewHyperliquidResolvedEconomicValue(decimal, normalize.BaseAssetUnit(baseAssetID), identity.InstrumentUID, "trade_size", identity.GenerationHex(), provenance)
}

type BookLevel struct {
	Price      string
	Size       string
	OrderCount uint32
}

type BookSnapshot struct {
	Coin                  string
	TimeMS                int64
	Bids                  []BookLevel
	Asks                  []BookLevel
	Depth                 BookDepthContract
	UpdateClaim           string
	ContinuityUncertainty string
	Evidence              *RawEvidence
	captureIdentity       BookCaptureIdentity
	binding               [sha256.Size]byte
}

func ParseBookSnapshot(envelope ReceiveEnvelope) (BookSnapshot, error) {
	captureIdentity, err := envelope.bookCaptureIdentity()
	if err != nil {
		return BookSnapshot{}, err
	}
	payload := envelope.Bytes()
	depth := captureIdentity.Subscription().Book
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return BookSnapshot{}, err
	}
	data, err := unwrapChannel(payload, "l2Book")
	if err != nil {
		return BookSnapshot{}, err
	}
	fields, err := decodeObject(data)
	if err != nil {
		return BookSnapshot{}, err
	}
	for _, forbidden := range []string{"seq", "sequence", "u", "pu", "prevSeqId"} {
		if _, present := fields[forbidden]; present {
			return BookSnapshot{}, fmt.Errorf("%w: undocumented sequence field %s", ErrInvalidPayload, forbidden)
		}
	}
	if _, present := fields["levels"]; !present {
		return BookSnapshot{}, ErrBookDepthContract
	}
	wireCoin, err := subscriptionWireCoin(captureIdentity.Family(), captureIdentity.DEXName(), captureIdentity.Subscription())
	if err != nil {
		return BookSnapshot{}, ErrBookDepthContract
	}
	var native struct {
		Coin   string              `json:"coin"`
		Time   *int64              `json:"time"`
		Levels [][]json.RawMessage `json:"levels"`
	}
	if json.Unmarshal(data, &native) != nil || native.Coin != wireCoin || native.Time == nil || *native.Time < 0 ||
		len(native.Levels) != 2 || len(native.Levels[0]) > depth.MaximumLevels() || len(native.Levels[1]) > depth.MaximumLevels() {
		return BookSnapshot{}, ErrBookDepthContract
	}
	bids, err := parseBookLevels(native.Levels[0])
	if err != nil {
		return BookSnapshot{}, err
	}
	asks, err := parseBookLevels(native.Levels[1])
	if err != nil {
		return BookSnapshot{}, err
	}
	snapshot := BookSnapshot{
		Coin: native.Coin, TimeMS: *native.Time, Bids: bids, Asks: asks, Depth: depth,
		UpdateClaim: BookUpdateClaimFullReplacement, ContinuityUncertainty: BookContinuityNoSequence, Evidence: evidence,
		captureIdentity: captureIdentity,
	}
	snapshot.binding = bookSnapshotBinding(snapshot)
	return snapshot, nil
}

func (b BookSnapshot) CaptureIdentity() BookCaptureIdentity { return b.captureIdentity }

func (b BookSnapshot) validateEvidenceBinding() error {
	wireCoin, err := subscriptionWireCoin(b.captureIdentity.Family(), b.captureIdentity.DEXName(), b.captureIdentity.Subscription())
	if err != nil || wireCoin != b.Coin || !b.Evidence.Valid() ||
		b.binding == ([sha256.Size]byte{}) || b.binding != bookSnapshotBinding(b) {
		return ErrInvalidPayload
	}
	return nil
}

func parseBookLevels(raw []json.RawMessage) ([]BookLevel, error) {
	levels := make([]BookLevel, len(raw))
	for index, entry := range raw {
		var native struct {
			Price string  `json:"px"`
			Size  string  `json:"sz"`
			Count *uint32 `json:"n"`
		}
		if json.Unmarshal(entry, &native) != nil || !validDecimalText(native.Price) || !validDecimalText(native.Size) || native.Count == nil || *native.Count == 0 {
			return nil, fmt.Errorf("%w: book level ordinal %d", ErrInvalidPayload, index)
		}
		levels[index] = BookLevel{Price: native.Price, Size: native.Size, OrderCount: *native.Count}
	}
	return levels, nil
}

func (b BookSnapshot) AmountValues(identity catalog.HyperliquidInstrumentIdentity, baseAssetID string, provenance normalize.FieldProvenance) ([]normalize.HyperliquidEconomicValue, error) {
	if err := identity.Validate(); err != nil || identity.WireCoin != b.Coin || b.UpdateClaim != BookUpdateClaimFullReplacement || b.ContinuityUncertainty != BookContinuityNoSequence || b.validateEvidenceBinding() != nil {
		return nil, ErrInvalidPayload
	}
	values := make([]normalize.HyperliquidEconomicValue, 0, len(b.Bids)+len(b.Asks))
	for _, levels := range [][]BookLevel{b.Bids, b.Asks} {
		for _, level := range levels {
			decimal, err := normalize.ParseDecimal(level.Size, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
			if err != nil {
				return nil, err
			}
			var value normalize.HyperliquidEconomicValue
			if identity.Family == catalog.HyperliquidHIP3 {
				value, err = normalize.NewHyperliquidHIP3ProvisionalValue(decimal, identity.InstrumentUID, "book_level_size", identity.GenerationHex())
			} else {
				value, err = normalize.NewHyperliquidResolvedEconomicValue(decimal, normalize.BaseAssetUnit(baseAssetID), identity.InstrumentUID, "book_level_size", identity.GenerationHex(), provenance)
			}
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	return values, nil
}

type BBO struct {
	Coin     string
	TimeMS   int64
	Bid      *BookLevel
	Ask      *BookLevel
	Evidence *RawEvidence
}

func ParseBBO(payload []byte) (BBO, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return BBO{}, err
	}
	data, err := unwrapChannel(payload, "bbo")
	if err != nil {
		return BBO{}, err
	}
	var native struct {
		Coin string            `json:"coin"`
		Time *int64            `json:"time"`
		BBO  []json.RawMessage `json:"bbo"`
	}
	if json.Unmarshal(data, &native) != nil || !validCoin(native.Coin) || native.Time == nil || *native.Time < 0 || len(native.BBO) != 2 {
		return BBO{}, ErrInvalidPayload
	}
	bid, err := parseOptionalBookLevel(native.BBO[0])
	if err != nil {
		return BBO{}, err
	}
	ask, err := parseOptionalBookLevel(native.BBO[1])
	if err != nil {
		return BBO{}, err
	}
	return BBO{Coin: native.Coin, TimeMS: *native.Time, Bid: bid, Ask: ask, Evidence: evidence}, nil
}

func parseOptionalBookLevel(raw json.RawMessage) (*BookLevel, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	levels, err := parseBookLevels([]json.RawMessage{raw})
	if err != nil {
		return nil, err
	}
	return &levels[0], nil
}

type ActiveAssetContext struct {
	Coin     string
	Perp     *PerpAssetContext
	Spot     *SpotAssetContext
	Evidence *RawEvidence
}

func ParseActiveAssetContext(identity catalog.HyperliquidInstrumentIdentity, payload []byte) (ActiveAssetContext, error) {
	if err := identity.Validate(); err != nil {
		return ActiveAssetContext{}, err
	}
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return ActiveAssetContext{}, err
	}
	data, err := unwrapChannel(payload, "activeAssetCtx")
	if err != nil {
		return ActiveAssetContext{}, err
	}
	var native struct {
		Coin string          `json:"coin"`
		Ctx  json.RawMessage `json:"ctx"`
	}
	if json.Unmarshal(data, &native) != nil || native.Coin != identity.WireCoin || len(native.Ctx) == 0 {
		return ActiveAssetContext{}, ErrInvalidPayload
	}
	observation := ActiveAssetContext{Coin: native.Coin, Evidence: evidence}
	if identity.Family == catalog.HyperliquidSpot {
		context, err := parseSpotContext(identity, native.Ctx)
		if err != nil {
			return ActiveAssetContext{}, err
		}
		context.Evidence = evidence
		context.binding = spotContextBinding(context)
		observation.Spot = &context
	} else {
		context, err := parsePerpContext(identity, native.Ctx)
		if err != nil {
			return ActiveAssetContext{}, err
		}
		context.Evidence = evidence
		context.binding = perpContextBinding(context)
		observation.Perp = &context
	}
	return observation, nil
}

type Funding struct {
	Coin        string
	FundingRate string
	Premium     string
	TimeMS      int64
	Ordinal     uint32
	Evidence    *RawEvidence
}

func ParseFundingHistory(payload []byte) ([]Funding, error) {
	evidence, err := newRawEvidence(payload)
	if err != nil {
		return nil, err
	}
	var entries []json.RawMessage
	if json.Unmarshal(payload, &entries) != nil || len(entries) > 10000 {
		return nil, ErrInvalidPayload
	}
	funding := make([]Funding, len(entries))
	for index, raw := range entries {
		var native struct {
			Coin        string `json:"coin"`
			FundingRate string `json:"fundingRate"`
			Premium     string `json:"premium"`
			Time        *int64 `json:"time"`
		}
		if json.Unmarshal(raw, &native) != nil || !validCoin(native.Coin) || !validDecimalText(native.FundingRate) || !validDecimalText(native.Premium) || native.Time == nil || *native.Time < 0 {
			return nil, fmt.Errorf("%w: funding ordinal %d", ErrInvalidPayload, index)
		}
		funding[index] = Funding{Coin: native.Coin, FundingRate: native.FundingRate, Premium: native.Premium, TimeMS: *native.Time, Ordinal: uint32(index), Evidence: evidence}
	}
	return funding, nil
}

func (f Funding) RateDecimal() (normalize.Decimal, error) {
	return normalize.ParseDecimal(f.FundingRate, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
}

func (c PerpAssetContext) OpenInterestValue(baseAssetID string, provenance normalize.FieldProvenance) (normalize.HyperliquidEconomicValue, error) {
	if !c.Evidence.Valid() || c.binding == ([sha256.Size]byte{}) || c.binding != perpContextBinding(c) {
		return normalize.HyperliquidEconomicValue{}, ErrInvalidPayload
	}
	return contextEconomicValue(c.Identity, c.OpenInterest, "open_interest", normalize.BaseAssetUnit(baseAssetID), provenance)
}

func (c PerpAssetContext) DayNotionalValue(collateralAssetID string, provenance normalize.FieldProvenance) (normalize.HyperliquidEconomicValue, error) {
	if !c.Evidence.Valid() || c.binding == ([sha256.Size]byte{}) || c.binding != perpContextBinding(c) {
		return normalize.HyperliquidEconomicValue{}, ErrInvalidPayload
	}
	return contextEconomicValue(c.Identity, c.DayNotionalVolume, "day_notional_volume", normalize.QuoteAssetUnit(collateralAssetID), provenance)
}

func (c SpotAssetContext) DayNotionalValue(quoteAssetID string, provenance normalize.FieldProvenance) (normalize.HyperliquidEconomicValue, error) {
	if !c.Evidence.Valid() || c.binding == ([sha256.Size]byte{}) || c.binding != spotContextBinding(c) {
		return normalize.HyperliquidEconomicValue{}, ErrInvalidPayload
	}
	return contextEconomicValue(c.Identity, c.DayNotionalVolume, "day_notional_volume", normalize.QuoteAssetUnit(quoteAssetID), provenance)
}

func (c SpotAssetContext) CirculatingSupplyValue(baseAssetID string, provenance normalize.FieldProvenance) (normalize.HyperliquidEconomicValue, error) {
	if !c.Evidence.Valid() || c.binding == ([sha256.Size]byte{}) || c.binding != spotContextBinding(c) {
		return normalize.HyperliquidEconomicValue{}, ErrInvalidPayload
	}
	return contextEconomicValue(c.Identity, c.CirculatingSupply, "circulating_supply", normalize.BaseAssetUnit(baseAssetID), provenance)
}

func contextEconomicValue(identity catalog.HyperliquidInstrumentIdentity, field NativeTextField, role string, unit normalize.Unit, provenance normalize.FieldProvenance) (normalize.HyperliquidEconomicValue, error) {
	if err := identity.Validate(); err != nil || field.State != NativeValue || field.Validate() != nil {
		return normalize.HyperliquidEconomicValue{}, ErrInvalidPayload
	}
	decimal, err := normalize.ParseDecimal(field.Text, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.HyperliquidEconomicValue{}, err
	}
	if identity.Family == catalog.HyperliquidHIP3 {
		return normalize.NewHyperliquidHIP3ProvisionalValue(decimal, identity.InstrumentUID, role, identity.GenerationHex())
	}
	return normalize.NewHyperliquidResolvedEconomicValue(decimal, unit, identity.InstrumentUID, role, identity.GenerationHex(), provenance)
}

func unwrapChannel(payload []byte, channel string) (json.RawMessage, error) {
	var message struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if json.Unmarshal(payload, &message) == nil && message.Channel != "" {
		if message.Channel != channel || len(message.Data) == 0 {
			return nil, ErrInvalidPayload
		}
		return slices.Clone(message.Data), nil
	}
	if channel == "l2Book" {
		var direct map[string]json.RawMessage
		if json.Unmarshal(payload, &direct) == nil && direct["coin"] != nil && direct["levels"] != nil {
			return slices.Clone(payload), nil
		}
	}
	return nil, ErrInvalidPayload
}

// SourceTimeNS values are exact conversions of the native millisecond block
// time. Receive order remains the capture-order authority.
func (t Trade) SourceTimeNS() (int64, error) {
	if err := t.validateEvidenceBinding(); err != nil {
		return 0, err
	}
	return millisecondsToNanoseconds(t.TimeMS)
}

func (b BookSnapshot) SourceTimeNS() (int64, error) {
	if err := b.validateEvidenceBinding(); err != nil {
		return 0, err
	}
	return millisecondsToNanoseconds(b.TimeMS)
}
func (q BBO) SourceTimeNS() (int64, error)     { return millisecondsToNanoseconds(q.TimeMS) }
func (f Funding) SourceTimeNS() (int64, error) { return millisecondsToNanoseconds(f.TimeMS) }

func validTransactionHash(value string) bool {
	if len(value) != 66 || len(value) < 2 || value[:2] != "0x" {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}

func millisecondsToNanoseconds(value int64) (int64, error) {
	if value < 0 || value > math.MaxInt64/int64(1_000_000) {
		return 0, ErrInvalidPayload
	}
	return value * 1_000_000, nil
}

func tradeBinding(trade Trade) [sha256.Size]byte {
	trade.binding = [sha256.Size]byte{}
	return evidenceBoundDigest(trade.Evidence, trade)
}

func bookSnapshotBinding(snapshot BookSnapshot) [sha256.Size]byte {
	snapshot.binding = [sha256.Size]byte{}
	return evidenceBoundDigest(snapshot.Evidence, snapshot)
}

func perpContextBinding(context PerpAssetContext) [sha256.Size]byte {
	context.binding = [sha256.Size]byte{}
	return evidenceBoundDigest(context.Evidence, context)
}

func spotContextBinding(context SpotAssetContext) [sha256.Size]byte {
	context.binding = [sha256.Size]byte{}
	return evidenceBoundDigest(context.Evidence, context)
}

func evidenceBoundDigest(evidence *RawEvidence, value any) [sha256.Size]byte {
	if !evidence.Valid() {
		return [sha256.Size]byte{}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}
	}
	hasher := sha256.New()
	digest := evidence.SHA256()
	_, _ = hasher.Write(digest[:])
	_, _ = hasher.Write(encoded)
	var bound [sha256.Size]byte
	copy(bound[:], hasher.Sum(nil))
	return bound
}
