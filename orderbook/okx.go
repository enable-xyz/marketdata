package orderbook

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math/big"
	"slices"
	"strings"
)

const OKXChecksumCutoverTimeNS = int64(1782172800000000000)

var (
	ErrOKXBookConfiguration = errors.New("orderbook: invalid OKX book configuration")
	ErrOKXBookUnseeded      = errors.New("orderbook: OKX book update requires WebSocket snapshot")
	ErrOKXBookGap           = errors.New("orderbook: OKX prevSeqId continuity gap")
	ErrOKXBookChecksum      = errors.New("orderbook: OKX checksum mismatch")
)

type OKXBookState string

const (
	OKXBookUnseeded         OKXBookState = "unseeded"
	OKXBookLive             OKXBookState = "live"
	OKXBookNeedsResubscribe OKXBookState = "needs_resubscribe"
)

type OKXLiquidityKind string

const (
	OKXRegularLiquidity OKXLiquidityKind = "regular"
	OKXRPILiquidity     OKXLiquidityKind = "rpi"
)

type OKXChecksumStatus string

const (
	OKXChecksumValidated   OKXChecksumStatus = "validated"
	OKXChecksumUnavailable OKXChecksumStatus = "unavailable_after_2026_06_23"
)

type OKXApplyKind string

const (
	OKXAppliedSnapshot    OKXApplyKind = "snapshot"
	OKXAppliedUpdate      OKXApplyKind = "update"
	OKXAppliedNoChange    OKXApplyKind = "documented_no_change"
	OKXAppliedMaintenance OKXApplyKind = "documented_maintenance_reset"
	OKXResubscribe        OKXApplyKind = "resubscribe"
)

type OKXLevel struct {
	Price            string
	Size             string
	DeprecatedOrders string
	OrderCount       string
}

type OKXUpdate struct {
	Channel        string
	InstrumentID   string
	Action         string
	SourceTimeNS   int64
	ReceivedTimeNS int64
	PreviousSeqID  int64
	SeqID          int64
	Checksum       int32
	Bids           []OKXLevel
	Asks           []OKXLevel
}

type OKXApplyResult struct {
	Kind           OKXApplyKind
	State          OKXBookState
	Sequence       int64
	Checksum       int32
	ChecksumStatus OKXChecksumStatus
}

type OKXBookView struct {
	State          OKXBookState
	Channel        string
	InstrumentID   string
	LiquidityKind  OKXLiquidityKind
	Sequence       int64
	ChecksumStatus OKXChecksumStatus
	Bids           []OKXLevel
	Asks           []OKXLevel
}

// OKXBook is seeded only by a WebSocket snapshot. There is intentionally no
// REST seed API: OKX REST books are comparison observations, not a splice rule.
type OKXBook struct {
	channel        string
	instrumentID   string
	liquidityKind  OKXLiquidityKind
	maximumDepth   int
	state          OKXBookState
	sequence       int64
	checksumStatus OKXChecksumStatus
	bids           map[string]OKXLevel
	asks           map[string]OKXLevel
}

func NewOKXBook(channel, instrumentID string, maximumDepth int) (*OKXBook, error) {
	if !validOKXChannel(channel) || !validOKXIdentity(instrumentID) || maximumDepth < 1 || maximumDepth > 400 {
		return nil, ErrOKXBookConfiguration
	}
	kind := OKXRegularLiquidity
	if channel == "books-rpi-tbt" {
		kind = OKXRPILiquidity
	}
	return &OKXBook{channel: channel, instrumentID: instrumentID, liquidityKind: kind, maximumDepth: maximumDepth, state: OKXBookUnseeded, bids: make(map[string]OKXLevel), asks: make(map[string]OKXLevel)}, nil
}

func (b *OKXBook) Apply(update OKXUpdate) (OKXApplyResult, error) {
	if b == nil || update.Channel != b.channel || update.InstrumentID != b.instrumentID || update.SourceTimeNS < 0 || update.ReceivedTimeNS < 0 || update.SeqID < 0 || update.PreviousSeqID < -1 ||
		(update.Action != "snapshot" && update.Action != "update") || len(update.Bids) > b.maximumDepth || len(update.Asks) > b.maximumDepth {
		return b.fail(OKXResubscribe, ErrOKXBookConfiguration)
	}
	if err := validateOKXLevels(update.Bids); err != nil {
		return b.fail(OKXResubscribe, err)
	}
	if err := validateOKXLevels(update.Asks); err != nil {
		return b.fail(OKXResubscribe, err)
	}
	if update.Action == "snapshot" {
		if update.PreviousSeqID != -1 {
			return b.fail(OKXResubscribe, ErrOKXBookGap)
		}
		bids := make(map[string]OKXLevel, len(update.Bids))
		asks := make(map[string]OKXLevel, len(update.Asks))
		applyOKXLevels(bids, update.Bids)
		applyOKXLevels(asks, update.Asks)
		if len(bids) > b.maximumDepth || len(asks) > b.maximumDepth {
			return b.fail(OKXResubscribe, ErrOKXBookConfiguration)
		}
		status, err := validateOKXChecksum(update.SourceTimeNS, update.Checksum, bids, asks)
		if err != nil {
			return b.fail(OKXResubscribe, err)
		}
		b.bids, b.asks = bids, asks
		b.sequence = update.SeqID
		b.checksumStatus = status
		b.state = OKXBookLive
		return OKXApplyResult{Kind: OKXAppliedSnapshot, State: b.state, Sequence: b.sequence, Checksum: update.Checksum, ChecksumStatus: status}, nil
	}
	if b.state != OKXBookLive {
		return b.fail(OKXResubscribe, ErrOKXBookUnseeded)
	}
	if update.PreviousSeqID != b.sequence {
		return b.fail(OKXResubscribe, ErrOKXBookGap)
	}
	noChange := len(update.Bids) == 0 && len(update.Asks) == 0 && update.SeqID == update.PreviousSeqID
	maintenanceReset := update.SeqID < update.PreviousSeqID
	if !noChange && !maintenanceReset && update.SeqID <= update.PreviousSeqID {
		return b.fail(OKXResubscribe, ErrOKXBookGap)
	}
	bids := cloneOKXMap(b.bids)
	asks := cloneOKXMap(b.asks)
	applyOKXLevels(bids, update.Bids)
	applyOKXLevels(asks, update.Asks)
	if len(bids) > b.maximumDepth || len(asks) > b.maximumDepth {
		return b.fail(OKXResubscribe, ErrOKXBookConfiguration)
	}
	status, err := validateOKXChecksum(update.SourceTimeNS, update.Checksum, bids, asks)
	if err != nil {
		return b.fail(OKXResubscribe, err)
	}
	b.bids, b.asks = bids, asks
	b.sequence = update.SeqID
	b.checksumStatus = status
	b.state = OKXBookLive
	kind := OKXAppliedUpdate
	if noChange {
		kind = OKXAppliedNoChange
	} else if maintenanceReset {
		kind = OKXAppliedMaintenance
	}
	return OKXApplyResult{Kind: kind, State: b.state, Sequence: b.sequence, Checksum: update.Checksum, ChecksumStatus: status}, nil
}

func (b *OKXBook) Reconnect() {
	if b == nil {
		return
	}
	b.state = OKXBookUnseeded
	b.sequence = 0
	b.checksumStatus = ""
	clear(b.bids)
	clear(b.asks)
}

func (b *OKXBook) View() OKXBookView {
	if b == nil {
		return OKXBookView{}
	}
	return OKXBookView{State: b.state, Channel: b.channel, InstrumentID: b.instrumentID, LiquidityKind: b.liquidityKind, Sequence: b.sequence, ChecksumStatus: b.checksumStatus, Bids: sortedOKXLevels(b.bids, true), Asks: sortedOKXLevels(b.asks, false)}
}

func (b *OKXBook) fail(kind OKXApplyKind, err error) (OKXApplyResult, error) {
	if b != nil {
		b.state = OKXBookNeedsResubscribe
		b.sequence = 0
		b.checksumStatus = ""
		clear(b.bids)
		clear(b.asks)
		return OKXApplyResult{Kind: kind, State: b.state}, err
	}
	return OKXApplyResult{Kind: kind, State: OKXBookNeedsResubscribe}, err
}

func validOKXChannel(channel string) bool {
	switch channel {
	case "books", "books50-l2-tbt", "books-l2-tbt", "books-rpi-tbt":
		return true
	default:
		return false
	}
}

func validOKXIdentity(value string) bool {
	if value == "" || len(value) > 128 || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, r := range value {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func validateOKXLevels(levels []OKXLevel) error {
	seen := make(map[string]struct{}, len(levels))
	for _, level := range levels {
		if !validOKXDecimal(level.Price, false) || !validOKXDecimal(level.Size, true) || !validOKXDecimal(level.DeprecatedOrders, true) || !validOKXDecimal(level.OrderCount, true) {
			return ErrOKXBookConfiguration
		}
		if _, exists := seen[level.Price]; exists {
			return fmt.Errorf("%w: duplicate price", ErrOKXBookConfiguration)
		}
		seen[level.Price] = struct{}{}
	}
	return nil
}

func validOKXDecimal(value string, zeroAllowed bool) bool {
	if value == "" || len(value) > 128 || value[0] == '+' {
		return false
	}
	start := 0
	if value[0] == '-' {
		if len(value) == 1 {
			return false
		}
		start = 1
	}
	dot, digits := -1, 0
	for index := start; index < len(value); index++ {
		switch {
		case value[index] >= '0' && value[index] <= '9':
			digits++
		case value[index] == '.' && dot < 0:
			dot = index
		default:
			return false
		}
	}
	if digits == 0 || dot == start || dot == len(value)-1 {
		return false
	}
	decimal, ok := new(big.Rat).SetString(value)
	if !ok {
		return false
	}
	if zeroAllowed {
		return decimal.Sign() >= 0
	}
	return decimal.Sign() > 0
}

func applyOKXLevels(book map[string]OKXLevel, levels []OKXLevel) {
	for _, level := range levels {
		amount, _ := new(big.Rat).SetString(level.Size)
		if amount.Sign() == 0 {
			delete(book, level.Price)
			continue
		}
		book[level.Price] = level
	}
}

func cloneOKXMap(source map[string]OKXLevel) map[string]OKXLevel {
	clone := make(map[string]OKXLevel, len(source))
	for price, level := range source {
		clone[price] = level
	}
	return clone
}

func sortedOKXLevels(source map[string]OKXLevel, descending bool) []OKXLevel {
	levels := make([]OKXLevel, 0, len(source))
	for _, level := range source {
		levels = append(levels, level)
	}
	slices.SortFunc(levels, func(a, b OKXLevel) int {
		left, _ := new(big.Rat).SetString(a.Price)
		right, _ := new(big.Rat).SetString(b.Price)
		comparison := left.Cmp(right)
		if descending {
			return -comparison
		}
		return comparison
	})
	return levels
}

func validateOKXChecksum(sourceTimeNS int64, supplied int32, bids, asks map[string]OKXLevel) (OKXChecksumStatus, error) {
	if sourceTimeNS >= OKXChecksumCutoverTimeNS {
		if supplied != 0 {
			return "", fmt.Errorf("%w: post-cutover field is %d, want documented zero", ErrOKXBookChecksum, supplied)
		}
		return OKXChecksumUnavailable, nil
	}
	computed := OKXChecksum(sortedOKXLevels(bids, true), sortedOKXLevels(asks, false))
	if computed != supplied {
		return "", fmt.Errorf("%w: got %d want %d", ErrOKXBookChecksum, supplied, computed)
	}
	return OKXChecksumValidated, nil
}

// OKXChecksum calculates the documented signed CRC32 over at most 25 bid/ask
// levels, alternating bid price/size then ask price/size in native text form.
func OKXChecksum(bids, asks []OKXLevel) int32 {
	bidCount := min(len(bids), 25)
	askCount := min(len(asks), 25)
	parts := make([]string, 0, 2*(bidCount+askCount))
	for index := range max(bidCount, askCount) {
		if index < bidCount {
			parts = append(parts, bids[index].Price, bids[index].Size)
		}
		if index < askCount {
			parts = append(parts, asks[index].Price, asks[index].Size)
		}
	}
	return int32(crc32.ChecksumIEEE([]byte(strings.Join(parts, ":"))))
}
