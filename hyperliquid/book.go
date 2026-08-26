package hyperliquid

import (
	"slices"

	"github.com/enable-xyz/marketdata/catalog"
)

type BookStreamIdentity struct {
	InstrumentUID string
	WireCoin      string
	NSigFigs      uint8
	Mantissa      uint8
	Fast          bool
}

type BookGapObservation struct {
	State              string
	Reason             string
	SequenceDetectable bool
	DeltaClaim         bool
}

type BookApplyResult struct {
	ReplacedPrior bool
	Gap           BookGapObservation
}

type BookView struct {
	Stream                BookStreamIdentity
	Seeded                bool
	ReplacementCount      uint64
	LastTimeMS            int64
	Bids                  []BookLevel
	Asks                  []BookLevel
	ContinuityUncertainty string
	LastEvidence          *RawEvidence
}

// Book holds only the latest independent depth-limited snapshot. Apply always
// discards prior levels; it never merges levels or infers continuity from time.
type Book struct {
	identity         catalog.HyperliquidInstrumentIdentity
	depth            BookDepthContract
	seeded           bool
	replacementCount uint64
	lastTimeMS       int64
	bids             []BookLevel
	asks             []BookLevel
	lastEvidence     *RawEvidence
}

func NewBook(identity catalog.HyperliquidInstrumentIdentity, depth BookDepthContract) (*Book, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if err := depth.Validate(); err != nil {
		return nil, err
	}
	return &Book{identity: identity, depth: depth}, nil
}

func (b *Book) Apply(snapshot BookSnapshot) (BookApplyResult, error) {
	if b == nil || b.identity.Validate() != nil || snapshot.Coin != b.identity.WireCoin || snapshot.Depth != b.depth || snapshot.TimeMS < 0 ||
		snapshot.UpdateClaim != BookUpdateClaimFullReplacement || snapshot.ContinuityUncertainty != BookContinuityNoSequence ||
		snapshot.validateEvidenceBinding() != nil || !validBookSnapshotLevels(snapshot.Bids, b.depth.MaximumLevels()) || !validBookSnapshotLevels(snapshot.Asks, b.depth.MaximumLevels()) {
		return BookApplyResult{}, ErrBookStreamMismatch
	}
	replaced := b.seeded
	b.bids = slices.Clone(snapshot.Bids)
	b.asks = slices.Clone(snapshot.Asks)
	b.lastTimeMS = snapshot.TimeMS
	b.lastEvidence = snapshot.Evidence
	b.seeded = true
	b.replacementCount++
	return BookApplyResult{
		ReplacedPrior: replaced,
		Gap: BookGapObservation{
			State: "uncertain", Reason: BookContinuityNoSequence,
			SequenceDetectable: false, DeltaClaim: false,
		},
	}, nil
}

func (b *Book) Reset() {
	if b == nil {
		return
	}
	b.seeded = false
	b.replacementCount = 0
	b.lastTimeMS = 0
	b.bids = nil
	b.asks = nil
	b.lastEvidence = nil
}

func (b *Book) Snapshot() BookView {
	if b == nil {
		return BookView{}
	}
	return BookView{
		Stream: BookStreamIdentity{
			InstrumentUID: b.identity.InstrumentUID, WireCoin: b.identity.WireCoin,
			NSigFigs: b.depth.NSigFigs, Mantissa: b.depth.Mantissa, Fast: b.depth.Fast,
		},
		Seeded: b.seeded, ReplacementCount: b.replacementCount, LastTimeMS: b.lastTimeMS,
		Bids: slices.Clone(b.bids), Asks: slices.Clone(b.asks), ContinuityUncertainty: BookContinuityNoSequence,
		LastEvidence: b.lastEvidence,
	}
}

func validBookSnapshotLevels(levels []BookLevel, maximum int) bool {
	if len(levels) > maximum {
		return false
	}
	for _, level := range levels {
		if !validDecimalText(level.Price) || !validDecimalText(level.Size) || level.OrderCount == 0 {
			return false
		}
	}
	return true
}
