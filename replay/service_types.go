package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/warehouse"
)

const (
	ServiceVersionV1          = uint16(1)
	NativeStreamContentTypeV1 = "application/vnd.enable.marketdata.segment-stream.v1"
	NormalizedContentTypeV1   = "application/x-ndjson; profile=enable-marketdata-normalized-v1"
	MaximumServiceSources     = 64
	MaximumServiceChannels    = 64
	MaximumServiceInstruments = 256
)

var ErrInvalidServiceRequest = errors.New("replay: invalid service request")

// ServiceRequest is the closed, pinned request passed from the authenticated
// HTTP boundary to replay implementations. It contains no query language,
// storage identifier, or mutable cursor.
type ServiceRequest struct {
	DatasetID           string
	Family              string
	CatalogSnapshotID   string
	SchemaName          string
	SchemaVersion       uint16
	SourceIDs           []string
	ChannelIDs          []string
	InstrumentUIDs      []string
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
}

func (r ServiceRequest) Validate() error {
	if !validServiceText(r.DatasetID) || !validServiceText(r.Family) || !validServiceText(r.CatalogSnapshotID) ||
		!validServiceText(r.SchemaName) || r.SchemaVersion == 0 || r.StartReceivedTimeNS >= r.EndReceivedTimeNS {
		return fmt.Errorf("%w: incomplete replay identity or interval", ErrInvalidServiceRequest)
	}
	if err := validateServiceIDs(r.SourceIDs, 1, MaximumServiceSources); err != nil {
		return err
	}
	if err := validateServiceIDs(r.ChannelIDs, 0, MaximumServiceChannels); err != nil {
		return err
	}
	return validateServiceIDs(r.InstrumentUIDs, 0, MaximumServiceInstruments)
}

type NativeStream interface {
	Next(context.Context) ([]byte, error)
	Close() error
}

type NativeOpener interface {
	OpenNative(context.Context, ServiceRequest) (NativeStream, error)
}

type NormalizedStream interface {
	Next(context.Context) (NormalizedItem, error)
	Close() error
}

type NormalizedOpener interface {
	OpenNormalized(context.Context, ServiceRequest) (NormalizedStream, error)
}

type NormalizedKind string

const (
	NormalizedRecordKind        NormalizedKind = "record"
	NormalizedDiscontinuityKind NormalizedKind = "discontinuity"
	NormalizedGapKind           NormalizedKind = "gap"
)

type NormalizedDiscontinuity struct {
	Kind             DiscontinuityKind `json:"kind"`
	Reason           IntegrityReason   `json:"reason"`
	SourceID         string            `json:"source_id"`
	ChannelID        string            `json:"channel_id"`
	InstrumentUID    string            `json:"instrument_uid"`
	ReceivedTimeNS   int64             `json:"received_time_ns"`
	StreamEpochID    string            `json:"stream_epoch_id"`
	ArrivalOrdinal   uint64            `json:"arrival_ordinal"`
	MessageOrdinal   uint32            `json:"message_ordinal"`
	SegmentID        string            `json:"segment_id"`
	FirstOrdinal     uint64            `json:"first_ordinal"`
	LastOrdinal      uint64            `json:"last_ordinal"`
	FrameOrdinal     uint32            `json:"frame_ordinal"`
	CompressedOffset uint64            `json:"compressed_offset"`
}

type NormalizedItem struct {
	Version           uint16                   `json:"version"`
	Type              NormalizedKind           `json:"type"`
	DatasetID         string                   `json:"dataset_id"`
	CatalogSnapshotID string                   `json:"catalog_snapshot_id"`
	SchemaName        string                   `json:"schema_name"`
	SchemaVersion     uint16                   `json:"schema_version"`
	Record            *warehouse.QueryRow      `json:"record,omitzero"`
	Discontinuity     *NormalizedDiscontinuity `json:"discontinuity,omitzero"`
	Gap               *warehouse.GapReference  `json:"gap,omitzero"`
}

func (i NormalizedItem) ValidateFor(request ServiceRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if i.Version != ServiceVersionV1 || i.DatasetID != request.DatasetID || i.CatalogSnapshotID != request.CatalogSnapshotID ||
		i.SchemaName != request.SchemaName || i.SchemaVersion != request.SchemaVersion {
		return fmt.Errorf("%w: normalized item identity", ErrInvalidServiceRequest)
	}
	present := 0
	if i.Record != nil {
		present++
	}
	if i.Discontinuity != nil {
		present++
	}
	if i.Gap != nil {
		present++
	}
	if present != 1 {
		return fmt.Errorf("%w: normalized item union", ErrInvalidServiceRequest)
	}
	switch i.Type {
	case NormalizedRecordKind:
		if i.Record == nil || i.Record.DatasetID != request.DatasetID || i.Record.CatalogSnapshotID != request.CatalogSnapshotID ||
			i.Record.Family != request.Family || i.Record.SchemaName != request.SchemaName || i.Record.SchemaVersion != request.SchemaVersion ||
			!validServiceText(i.Record.SourceID) || !validServiceText(i.Record.ChannelID) || !validOptionalServiceText(i.Record.InstrumentUID) ||
			!validQueryRowDecimals(*i.Record) || i.Record.ReceivedTimeNS < request.StartReceivedTimeNS ||
			i.Record.ReceivedTimeNS >= request.EndReceivedTimeNS ||
			!serviceTupleSelected(request, i.Record.SourceID, i.Record.ChannelID, i.Record.InstrumentUID) {
			return fmt.Errorf("%w: normalized record escaped request", ErrInvalidServiceRequest)
		}
	case NormalizedDiscontinuityKind:
		if i.Discontinuity == nil || i.Discontinuity.Kind < DiscontinuityEpochBoundary ||
			i.Discontinuity.Kind > DiscontinuityOrdinalOverlap || !validServiceText(i.Discontinuity.SourceID) ||
			!validServiceText(i.Discontinuity.ChannelID) || !validOptionalServiceText(i.Discontinuity.InstrumentUID) ||
			i.Discontinuity.ReceivedTimeNS < request.StartReceivedTimeNS || i.Discontinuity.ReceivedTimeNS >= request.EndReceivedTimeNS ||
			!serviceTupleSelected(request, i.Discontinuity.SourceID, i.Discontinuity.ChannelID, i.Discontinuity.InstrumentUID) {
			return fmt.Errorf("%w: normalized discontinuity escaped request", ErrInvalidServiceRequest)
		}
	case NormalizedGapKind:
		if i.Gap == nil || !validServiceText(i.Gap.ID) || !validServiceText(i.Gap.Kind) ||
			!validServiceText(i.Gap.Tuple.SourceID) || !validServiceText(i.Gap.Tuple.ChannelID) ||
			!validOptionalServiceText(i.Gap.Tuple.InstrumentUID) || i.Gap.StartReceivedTimeNS >= i.Gap.EndReceivedTimeNS ||
			i.Gap.EndReceivedTimeNS <= request.StartReceivedTimeNS || i.Gap.StartReceivedTimeNS >= request.EndReceivedTimeNS ||
			!serviceTupleSelected(request, i.Gap.Tuple.SourceID, i.Gap.Tuple.ChannelID, i.Gap.Tuple.InstrumentUID) {
			return fmt.Errorf("%w: normalized gap escaped request", ErrInvalidServiceRequest)
		}
	default:
		return fmt.Errorf("%w: normalized item type", ErrInvalidServiceRequest)
	}
	return nil
}
func validQueryRowDecimals(row warehouse.QueryRow) bool {
	for _, value := range []string{row.Price, row.Amount, row.BidPrice, row.BidAmount, row.AskPrice, row.AskAmount,
		row.PriceChange, row.PriceChangePercent, row.WeightedAveragePrice, row.FirstTradeBeforeWindowPrice,
		row.LastPrice, row.LastAmount, row.NativeBestBidPrice, row.NativeBestBidAmount, row.NativeBestAskPrice,
		row.NativeBestAskAmount, row.OpenPrice, row.HighPrice, row.LowPrice, row.BaseVolume, row.QuoteVolume} {
		if value != "" && !validDecimalString(value) {
			return false
		}
	}
	return true
}

func validDecimalString(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	index := 0
	if value[0] == '-' {
		index = 1
	}
	if index == len(value) {
		return false
	}
	digits, points := 0, 0
	for ; index < len(value); index++ {
		switch {
		case value[index] >= '0' && value[index] <= '9':
			digits++
		case value[index] == '.':
			points++
			if points > 1 {
				return false
			}
		default:
			return false
		}
	}
	return digits > 0
}

func validOptionalServiceText(value string) bool {
	return len(value) <= warehouse.MaxIdentityBytes && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func (i NormalizedItem) AppendNDJSON(dst []byte, request ServiceRequest) ([]byte, error) {
	if err := i.ValidateFor(request); err != nil {
		return dst, err
	}
	encoded, err := json.Marshal(i)
	if err != nil {
		return dst, err
	}
	dst = append(dst, encoded...)
	dst = append(dst, '\n')
	return dst, nil
}

// SliceNativeStream and SliceNormalizedStream are deterministic synthetic
// adapters used by release gates and embedders. They retain no caller buffers.
type SliceNativeStream struct {
	Frames [][]byte
	next   int
}

func (s *SliceNativeStream) Next(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.next == len(s.Frames) {
		return nil, io.EOF
	}
	frame := slices.Clone(s.Frames[s.next])
	s.next++
	return frame, nil
}

func (*SliceNativeStream) Close() error { return nil }

type SliceNormalizedStream struct {
	Items []NormalizedItem
	next  int
}

func (s *SliceNormalizedStream) Next(ctx context.Context) (NormalizedItem, error) {
	if err := ctx.Err(); err != nil {
		return NormalizedItem{}, err
	}
	if s.next == len(s.Items) {
		return NormalizedItem{}, io.EOF
	}
	item := s.Items[s.next]
	s.next++
	return item, nil
}

func (*SliceNormalizedStream) Close() error { return nil }

func validateServiceIDs(values []string, minimum, maximum int) error {
	if len(values) < minimum || len(values) > maximum || !slices.IsSorted(values) {
		return fmt.Errorf("%w: identifier list bound or order", ErrInvalidServiceRequest)
	}
	for index, value := range values {
		if !validServiceText(value) || (index > 0 && value == values[index-1]) {
			return fmt.Errorf("%w: invalid or duplicate identifier", ErrInvalidServiceRequest)
		}
	}
	return nil
}

func validServiceText(value string) bool {
	return value != "" && len(value) <= warehouse.MaxIdentityBytes && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func serviceTupleSelected(request ServiceRequest, sourceID, channelID, instrumentUID string) bool {
	_, source := slices.BinarySearch(request.SourceIDs, sourceID)
	_, channel := slices.BinarySearch(request.ChannelIDs, channelID)
	_, instrument := slices.BinarySearch(request.InstrumentUIDs, instrumentUID)
	return source && (len(request.ChannelIDs) == 0 || channel) && (len(request.InstrumentUIDs) == 0 || instrument)
}
