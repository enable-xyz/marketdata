package warehouse

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaximumQuerySources     = 64
	MaximumQueryChannels    = 64
	MaximumQueryInstruments = 256
	MaximumQueryRows        = 10_000
	MaximumQueryReferences  = 32_768
)

var (
	ErrInvalidQuery        = errors.New("warehouse: invalid declarative query")
	ErrUnstableQueryResult = errors.New("warehouse: query result is not in stable order")
)

// Dataset identifies the immutable generation and schema selected before a
// query starts. QueryAdapter rejects rows from any other generation or schema.
type Dataset struct {
	ID                GenerationID `json:"-"`
	Family            string       `json:"family"`
	CatalogSnapshotID Hash         `json:"-"`
	SchemaName        string       `json:"schema_name"`
	SchemaVersion     uint16       `json:"schema_version"`
}

func (d Dataset) Validate() error {
	if d.ID == (GenerationID{}) || d.CatalogSnapshotID == (Hash{}) || !validQueryText(d.Family) ||
		!validQueryText(d.SchemaName) || d.SchemaVersion == 0 {
		return fmt.Errorf("%w: incomplete dataset identity", ErrInvalidQuery)
	}
	return nil
}

func (d Dataset) IDString() string { return d.ID.String() }

func (d Dataset) CatalogSnapshotIDString() string { return d.CatalogSnapshotID.String() }

// QuerySpec is the only warehouse query grammar exposed to serving code. A
// storage adapter receives values, never client-authored SQL or identifiers.
type QuerySpec struct {
	Dataset             Dataset
	SourceIDs           []string
	ChannelIDs          []string
	InstrumentUIDs      []string
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	Limit               int
	After               *SortKey
}

func (q QuerySpec) Validate() error {
	if err := q.Dataset.Validate(); err != nil {
		return err
	}
	if err := validateSortedIDs("source_ids", q.SourceIDs, 1, MaximumQuerySources); err != nil {
		return err
	}
	if err := validateSortedIDs("channel_ids", q.ChannelIDs, 0, MaximumQueryChannels); err != nil {
		return err
	}
	if err := validateSortedIDs("instrument_uids", q.InstrumentUIDs, 0, MaximumQueryInstruments); err != nil {
		return err
	}
	if q.StartReceivedTimeNS >= q.EndReceivedTimeNS {
		return fmt.Errorf("%w: received-time interval", ErrInvalidQuery)
	}
	if q.Limit < 1 || q.Limit > MaximumQueryRows {
		return fmt.Errorf("%w: limit must be from 1 through %d", ErrInvalidQuery, MaximumQueryRows)
	}
	if q.After != nil {
		if err := q.After.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// SortKey extends the ClickHouse physical order with row_id as a final stable
// tie-breaker. The byte arrays are compared bytewise, not as host integers.
type SortKey struct {
	SourceID        string   `json:"source_id"`
	InstrumentUID   string   `json:"instrument_uid"`
	ReceivedTimeNS  int64    `json:"received_time_ns"`
	ConnectionEpoch [16]byte `json:"connection_epoch"`
	ArrivalOrdinal  uint64   `json:"arrival_ordinal"`
	MessageOrdinal  uint32   `json:"message_ordinal"`
	RowID           Hash     `json:"row_id"`
}

func (k SortKey) Validate() error {
	if !validQueryText(k.SourceID) || len(k.InstrumentUID) > MaxIdentityBytes || strings.IndexByte(k.InstrumentUID, 0) >= 0 ||
		k.ConnectionEpoch == ([16]byte{}) || k.RowID == (Hash{}) {
		return fmt.Errorf("%w: incomplete stable sort key", ErrInvalidQuery)
	}
	return nil
}

func CompareSortKey(left, right SortKey) int {
	if n := strings.Compare(left.SourceID, right.SourceID); n != 0 {
		return n
	}
	if n := strings.Compare(left.InstrumentUID, right.InstrumentUID); n != 0 {
		return n
	}
	if left.ReceivedTimeNS < right.ReceivedTimeNS {
		return -1
	}
	if left.ReceivedTimeNS > right.ReceivedTimeNS {
		return 1
	}
	if n := bytes.Compare(left.ConnectionEpoch[:], right.ConnectionEpoch[:]); n != 0 {
		return n
	}
	if left.ArrivalOrdinal < right.ArrivalOrdinal {
		return -1
	}
	if left.ArrivalOrdinal > right.ArrivalOrdinal {
		return 1
	}
	if left.MessageOrdinal < right.MessageOrdinal {
		return -1
	}
	if left.MessageOrdinal > right.MessageOrdinal {
		return 1
	}
	return compareHash(left.RowID, right.RowID)
}

type Tuple struct {
	SourceID      string `json:"source_id"`
	ChannelID     string `json:"channel_id"`
	InstrumentUID string `json:"instrument_uid"`
}

func (t Tuple) validate() error {
	if !validQueryText(t.SourceID) || !validQueryText(t.ChannelID) || len(t.InstrumentUID) > MaxIdentityBytes ||
		strings.IndexByte(t.InstrumentUID, 0) >= 0 {
		return fmt.Errorf("%w: invalid tuple", ErrInvalidQuery)
	}
	return nil
}

type CoverageReference struct {
	ID                  string `json:"id"`
	Tuple               Tuple  `json:"tuple"`
	StartReceivedTimeNS int64  `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64  `json:"end_received_time_ns"`
	State               string `json:"state"`
}

type GapReference struct {
	ID                  string `json:"id"`
	Tuple               Tuple  `json:"tuple"`
	StartReceivedTimeNS int64  `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64  `json:"end_received_time_ns"`
	Kind                string `json:"kind"`
}

// QueryCandidate keeps tuple-level coverage and gap evidence attached to each
// record while the page adapter verifies that every referenced identity exists.
type QueryCandidate struct {
	Row            Row
	CoverageRefIDs []string
	GapRefIDs      []string
}

type QueryCursor interface {
	Next(context.Context) (QueryCandidate, error)
	References() ([]CoverageReference, []GapReference, error)
	Close() error
}

type QueryReader interface {
	OpenQuery(context.Context, QuerySpec) (QueryCursor, error)
}

type QueryAdapter struct {
	reader QueryReader
}

func NewQueryAdapter(reader QueryReader) (*QueryAdapter, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: query reader is required", ErrInvalidQuery)
	}
	return &QueryAdapter{reader: reader}, nil
}

type QueryRow struct {
	DatasetID                   string   `json:"dataset_id"`
	CatalogSnapshotID           string   `json:"catalog_snapshot_id"`
	SchemaName                  string   `json:"schema_name"`
	SchemaVersion               uint16   `json:"schema_version"`
	EventID                     string   `json:"event_id"`
	RowID                       string   `json:"row_id"`
	Family                      string   `json:"family"`
	SourceID                    string   `json:"source_id"`
	ChannelID                   string   `json:"channel_id"`
	InstrumentUID               string   `json:"instrument_uid"`
	ConnectionEpoch             string   `json:"connection_epoch"`
	ReceivedTimeNS              int64    `json:"received_time_ns"`
	ArrivalOrdinal              uint64   `json:"arrival_ordinal"`
	MessageOrdinal              uint32   `json:"message_ordinal"`
	RawSegmentSHA256            string   `json:"raw_segment_sha256"`
	RawRecordOrdinal            uint64   `json:"raw_record_ordinal"`
	RawPayloadSHA256            string   `json:"raw_payload_sha256"`
	CoverageRefIDs              []string `json:"coverage_ref_ids"`
	GapRefIDs                   []string `json:"gap_ref_ids"`
	Price                       string   `json:"price,omitzero"`
	Amount                      string   `json:"amount,omitzero"`
	BidPrice                    string   `json:"bid_price,omitzero"`
	BidAmount                   string   `json:"bid_amount,omitzero"`
	AskPrice                    string   `json:"ask_price,omitzero"`
	AskAmount                   string   `json:"ask_amount,omitzero"`
	PriceChange                 string   `json:"price_change,omitzero"`
	PriceChangePercent          string   `json:"price_change_percent,omitzero"`
	WeightedAveragePrice        string   `json:"weighted_average_price,omitzero"`
	FirstTradeBeforeWindowPrice string   `json:"first_trade_before_window_price,omitzero"`
	LastPrice                   string   `json:"last_price,omitzero"`
	LastAmount                  string   `json:"last_amount,omitzero"`
	NativeBestBidPrice          string   `json:"native_best_bid_price,omitzero"`
	NativeBestBidAmount         string   `json:"native_best_bid_amount,omitzero"`
	NativeBestAskPrice          string   `json:"native_best_ask_price,omitzero"`
	NativeBestAskAmount         string   `json:"native_best_ask_amount,omitzero"`
	OpenPrice                   string   `json:"open_price,omitzero"`
	HighPrice                   string   `json:"high_price,omitzero"`
	LowPrice                    string   `json:"low_price,omitzero"`
	BaseVolume                  string   `json:"base_volume,omitzero"`
	QuoteVolume                 string   `json:"quote_volume,omitzero"`
}

func (r QueryRow) SortKey() (SortKey, error) {
	rowID, err := ParseHash(r.RowID)
	if err != nil {
		return SortKey{}, err
	}
	epoch, err := hex.DecodeString(r.ConnectionEpoch)
	if err != nil || len(epoch) != 16 {
		return SortKey{}, fmt.Errorf("%w: connection epoch", ErrInvalidQuery)
	}
	key := SortKey{SourceID: r.SourceID, InstrumentUID: r.InstrumentUID, ReceivedTimeNS: r.ReceivedTimeNS,
		ArrivalOrdinal: r.ArrivalOrdinal, MessageOrdinal: r.MessageOrdinal, RowID: rowID}
	copy(key.ConnectionEpoch[:], epoch)
	return key, key.Validate()
}

type Page struct {
	Dataset  Dataset
	Rows     []QueryRow
	Coverage []CoverageReference
	Gaps     []GapReference
	HasMore  bool
	LastKey  *SortKey
}

func (a *QueryAdapter) Page(ctx context.Context, spec QuerySpec) (page Page, err error) {
	if a == nil || a.reader == nil {
		return Page{}, fmt.Errorf("%w: query adapter is not initialized", ErrInvalidQuery)
	}
	if err := spec.Validate(); err != nil {
		return Page{}, err
	}
	cursor, err := a.reader.OpenQuery(ctx, spec)
	if err != nil {
		return Page{}, err
	}
	defer func() { err = errors.Join(err, cursor.Close()) }()

	coverage, gaps, err := cursor.References()
	if err != nil {
		return Page{}, err
	}
	coverageByID, gapsByID, err := validateReferences(spec, coverage, gaps)
	if err != nil {
		return Page{}, err
	}
	page = Page{Dataset: spec.Dataset, Coverage: coverage, Gaps: gaps, Rows: make([]QueryRow, 0, spec.Limit)}
	var previous *SortKey
	for len(page.Rows) <= spec.Limit {
		candidate, nextErr := cursor.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return Page{}, nextErr
		}
		row, key, convertErr := convertQueryCandidate(spec, candidate, coverageByID, gapsByID)
		if convertErr != nil {
			return Page{}, convertErr
		}
		if spec.After != nil && CompareSortKey(key, *spec.After) <= 0 {
			return Page{}, fmt.Errorf("%w: row did not advance page cursor", ErrUnstableQueryResult)
		}
		if previous != nil && CompareSortKey(key, *previous) <= 0 {
			return Page{}, ErrUnstableQueryResult
		}
		previous = &key
		if len(page.Rows) == spec.Limit {
			page.HasMore = true
			break
		}
		page.Rows = append(page.Rows, row)
	}
	if len(page.Rows) != 0 {
		key, keyErr := page.Rows[len(page.Rows)-1].SortKey()
		if keyErr != nil {
			return Page{}, keyErr
		}
		page.LastKey = &key
	}
	return page, nil
}

func validateReferences(spec QuerySpec, coverage []CoverageReference, gaps []GapReference) (map[string]CoverageReference, map[string]GapReference, error) {
	if len(coverage)+len(gaps) > MaximumQueryReferences {
		return nil, nil, fmt.Errorf("%w: too many tuple references", ErrInvalidQuery)
	}
	coverageByID := make(map[string]CoverageReference, len(coverage))
	for index, ref := range coverage {
		if !validQueryText(ref.ID) || !validQueryText(ref.State) || ref.Tuple.validate() != nil ||
			(index > 0 && ref.ID <= coverage[index-1].ID) || ref.StartReceivedTimeNS >= ref.EndReceivedTimeNS ||
			!tupleSelected(spec, ref.Tuple) || ref.EndReceivedTimeNS <= spec.StartReceivedTimeNS ||
			ref.StartReceivedTimeNS >= spec.EndReceivedTimeNS {
			return nil, nil, fmt.Errorf("%w: invalid or unstable coverage reference", ErrInvalidQuery)
		}
		if _, exists := coverageByID[ref.ID]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate coverage reference", ErrInvalidQuery)
		}
		coverageByID[ref.ID] = ref
	}
	gapsByID := make(map[string]GapReference, len(gaps))
	for index, ref := range gaps {
		if !validQueryText(ref.ID) || !validQueryText(ref.Kind) || ref.Tuple.validate() != nil ||
			(index > 0 && ref.ID <= gaps[index-1].ID) || ref.StartReceivedTimeNS >= ref.EndReceivedTimeNS ||
			!tupleSelected(spec, ref.Tuple) || ref.EndReceivedTimeNS <= spec.StartReceivedTimeNS ||
			ref.StartReceivedTimeNS >= spec.EndReceivedTimeNS {
			return nil, nil, fmt.Errorf("%w: invalid or unstable gap reference", ErrInvalidQuery)
		}
		if _, exists := gapsByID[ref.ID]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate gap reference", ErrInvalidQuery)
		}
		gapsByID[ref.ID] = ref
	}
	return coverageByID, gapsByID, nil
}

func convertQueryCandidate(spec QuerySpec, candidate QueryCandidate, coverage map[string]CoverageReference, gaps map[string]GapReference) (QueryRow, SortKey, error) {
	row := candidate.Row
	if err := row.validate(); err != nil {
		return QueryRow{}, SortKey{}, err
	}
	if row.GenerationID != spec.Dataset.ID || row.CatalogSnapshotID != spec.Dataset.CatalogSnapshotID ||
		row.SchemaName != spec.Dataset.SchemaName || row.SchemaVersion != spec.Dataset.SchemaVersion || row.Family != spec.Dataset.Family ||
		row.ReceivedTimeNS < spec.StartReceivedTimeNS || row.ReceivedTimeNS >= spec.EndReceivedTimeNS ||
		!containsSorted(spec.SourceIDs, row.SourceID) || (len(spec.ChannelIDs) != 0 && !containsSorted(spec.ChannelIDs, row.ChannelID)) ||
		(len(spec.InstrumentUIDs) != 0 && !containsSorted(spec.InstrumentUIDs, row.InstrumentUID)) {
		return QueryRow{}, SortKey{}, fmt.Errorf("%w: result escaped declarative filter", ErrInvalidQuery)
	}
	tuple := Tuple{SourceID: row.SourceID, ChannelID: row.ChannelID, InstrumentUID: row.InstrumentUID}
	coverageIDs, err := validateCandidateReferenceIDs(candidate.CoverageRefIDs, tuple, coverage)
	if err != nil {
		return QueryRow{}, SortKey{}, err
	}
	gapIDs, err := validateCandidateReferenceIDs(candidate.GapRefIDs, tuple, gaps)
	if err != nil {
		return QueryRow{}, SortKey{}, err
	}
	result := QueryRow{
		DatasetID: spec.Dataset.ID.String(), CatalogSnapshotID: row.CatalogSnapshotID.String(), SchemaName: row.SchemaName,
		SchemaVersion: row.SchemaVersion, EventID: row.EventID.String(), RowID: row.RowID.String(), Family: row.Family,
		SourceID: row.SourceID, ChannelID: row.ChannelID, InstrumentUID: row.InstrumentUID,
		ConnectionEpoch: hex.EncodeToString(row.ConnectionEpoch[:]), ReceivedTimeNS: row.ReceivedTimeNS,
		ArrivalOrdinal: row.ArrivalOrdinal, MessageOrdinal: row.MessageOrdinal, RawSegmentSHA256: row.RawSegmentSHA256.String(),
		RawRecordOrdinal: row.RawRecordOrdinal, RawPayloadSHA256: row.RawPayloadSHA256.String(),
		CoverageRefIDs: coverageIDs, GapRefIDs: gapIDs,
	}
	result.Price, result.Amount = decimalString(row.Price), decimalString(row.Amount)
	result.BidPrice, result.BidAmount = decimalString(row.BidPrice), decimalString(row.BidAmount)
	result.AskPrice, result.AskAmount = decimalString(row.AskPrice), decimalString(row.AskAmount)
	result.PriceChange, result.PriceChangePercent = decimalString(row.PriceChange), decimalString(row.PriceChangePercent)
	result.WeightedAveragePrice = decimalString(row.WeightedAveragePrice)
	result.FirstTradeBeforeWindowPrice = decimalString(row.FirstTradeBeforeWindowPrice)
	result.LastPrice, result.LastAmount = decimalString(row.LastPrice), decimalString(row.LastAmount)
	result.NativeBestBidPrice, result.NativeBestBidAmount = decimalString(row.NativeBestBidPrice), decimalString(row.NativeBestBidAmount)
	result.NativeBestAskPrice, result.NativeBestAskAmount = decimalString(row.NativeBestAskPrice), decimalString(row.NativeBestAskAmount)
	result.OpenPrice, result.HighPrice, result.LowPrice = decimalString(row.OpenPrice), decimalString(row.HighPrice), decimalString(row.LowPrice)
	result.BaseVolume, result.QuoteVolume = decimalString(row.BaseVolume), decimalString(row.QuoteVolume)
	key := SortKey{SourceID: row.SourceID, InstrumentUID: row.InstrumentUID, ReceivedTimeNS: row.ReceivedTimeNS,
		ConnectionEpoch: row.ConnectionEpoch, ArrivalOrdinal: row.ArrivalOrdinal, MessageOrdinal: row.MessageOrdinal, RowID: row.RowID}
	return result, key, nil
}

func validateCandidateReferenceIDs[T CoverageReference | GapReference](ids []string, tuple Tuple, references map[string]T) ([]string, error) {
	if !slices.IsSorted(ids) {
		return nil, fmt.Errorf("%w: reference IDs must be sorted", ErrInvalidQuery)
	}
	result := slices.Clone(ids)
	for i, id := range result {
		if !validQueryText(id) || (i > 0 && id == result[i-1]) {
			return nil, fmt.Errorf("%w: invalid candidate reference ID", ErrInvalidQuery)
		}
		ref, ok := references[id]
		if !ok {
			return nil, fmt.Errorf("%w: unknown candidate reference ID", ErrInvalidQuery)
		}
		var referenced Tuple
		switch value := any(ref).(type) {
		case CoverageReference:
			referenced = value.Tuple
		case GapReference:
			referenced = value.Tuple
		}
		if referenced != tuple {
			return nil, fmt.Errorf("%w: reference tuple mismatch", ErrInvalidQuery)
		}
	}
	return result, nil
}

func tupleSelected(spec QuerySpec, tuple Tuple) bool {
	return containsSorted(spec.SourceIDs, tuple.SourceID) &&
		(len(spec.ChannelIDs) == 0 || containsSorted(spec.ChannelIDs, tuple.ChannelID)) &&
		(len(spec.InstrumentUIDs) == 0 || containsSorted(spec.InstrumentUIDs, tuple.InstrumentUID))
}

func validateSortedIDs(name string, ids []string, minimum, maximum int) error {
	if len(ids) < minimum || len(ids) > maximum || !slices.IsSorted(ids) {
		return fmt.Errorf("%w: %s must be sorted and contain %d..%d values", ErrInvalidQuery, name, minimum, maximum)
	}
	for i, id := range ids {
		if !validQueryText(id) || (i > 0 && id == ids[i-1]) {
			return fmt.Errorf("%w: invalid or duplicate %s", ErrInvalidQuery, name)
		}
	}
	return nil
}

func containsSorted(values []string, value string) bool {
	_, ok := slices.BinarySearch(values, value)
	return ok
}

func validQueryText(value string) bool {
	return value != "" && len(value) <= MaxIdentityBytes && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func decimalString(value *Decimal) string {
	if value == nil {
		return ""
	}
	coefficient := value.Coefficient
	negative := coefficient[0] == '-'
	if negative {
		coefficient = coefficient[1:]
	}
	scale := int(value.Scale)
	var text string
	if scale == 0 {
		text = coefficient
	} else if len(coefficient) <= scale {
		text = "0." + strings.Repeat("0", scale-len(coefficient)) + coefficient
	} else {
		cut := len(coefficient) - scale
		text = coefficient[:cut] + "." + coefficient[cut:]
	}
	if negative {
		text = "-" + text
	}
	return text
}
