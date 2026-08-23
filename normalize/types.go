package normalize

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	EventIDEncodingVersion  uint16 = 1
	LogicalEncodingVersion  uint16 = 1
	SchemaQuarantineVersion uint16 = 1

	TradeSchemaName      = "TradeV1"
	BookUpdateSchemaName = "BookUpdateV1"
	QuoteSchemaName      = "QuoteV1"
	TickerSchemaName     = "TickerV1"

	TradeSchemaVersion      uint16 = 1
	BookUpdateSchemaVersion uint16 = 1
	QuoteSchemaVersion      uint16 = 1
	TickerSchemaVersion     uint16 = 1

	MaxQualityFlags       = 64
	MaxBookLevelsPerSide  = 10_000
	MaxMapperVersionBytes = 128
	MaxSchemaNameBytes    = 64
	MaxUnitIDBytes        = 256
)

var (
	ErrInvalidNormalized = errors.New("normalize: invalid normalized value")
	ErrInvalidRawRecord  = errors.New("normalize: invalid raw record")
)

type Hash [sha256.Size]byte

type OptionalInt64 struct {
	Value int64
	Valid bool
}

type OptionalUint64 struct {
	Value uint64
	Valid bool
}

type SourceState string

const (
	SourceMissing SourceState = "missing"
	SourceNull    SourceState = "null"
	SourceEmpty   SourceState = "empty"
	SourceValue   SourceState = "value"
)

type EpochKind string

const (
	ConnectionEpoch EpochKind = "connection"
	PollCycleEpoch  EpochKind = "poll_cycle"
)

type TimeResolution string

const (
	ResolutionAbsent      TimeResolution = "absent"
	ResolutionNanosecond  TimeResolution = "nanosecond"
	ResolutionMicrosecond TimeResolution = "microsecond"
	ResolutionMillisecond TimeResolution = "millisecond"
	ResolutionSecond      TimeResolution = "second"
)

type QualityFlag string

const (
	QualityClockUncertain           QualityFlag = "clock_uncertain"
	QualityExchangeTimeRegression   QualityFlag = "exchange_time_regression"
	QualitySchemaAdditiveField      QualityFlag = "schema_additive_field"
	QualitySourceDuplicateCandidate QualityFlag = "source_duplicate_candidate"
)

var validQualityFlags = map[QualityFlag]struct{}{
	QualityClockUncertain: {}, QualityExchangeTimeRegression: {},
	QualitySchemaAdditiveField: {}, QualitySourceDuplicateCandidate: {},
}

// RawCoordinate is the immutable identity required to recover one captured
// payload. Arrays and scalar values deliberately avoid aliases to caller memory.
type RawCoordinate struct {
	SourceID         string
	ChannelID        string
	EpochKind        EpochKind
	EpochID          [16]byte
	ArrivalOrdinal   uint64
	MessageOrdinal   uint32
	RawSegmentSHA256 Hash
	RawRecordOrdinal uint64
	RawPayloadSHA256 Hash
}

// RawRecord binds an exact capture envelope to its committed segment
// coordinate. QualityFlags are observations supplied by replay, not mapper
// output; the consuming boundary sorts and copies them.
type RawRecord struct {
	Envelope     capture.EnvelopeV1
	Coordinate   RawCoordinate
	QualityFlags []QualityFlag
}

func BindRawRecord(envelope capture.EnvelopeV1, rawSegmentSHA256 Hash, rawRecordOrdinal uint64, flags []QualityFlag) (RawRecord, error) {
	if err := envelope.Validate(); err != nil {
		return RawRecord{}, fmt.Errorf("%w: envelope", ErrInvalidRawRecord)
	}
	epoch, err := envelope.StreamEpoch()
	if err != nil {
		return RawRecord{}, fmt.Errorf("%w: stream epoch", ErrInvalidRawRecord)
	}
	kind := ConnectionEpoch
	if epoch.Kind == capture.EpochPollCycle {
		kind = PollCycleEpoch
	}
	record := RawRecord{
		Envelope: cloneEnvelope(envelope),
		Coordinate: RawCoordinate{
			SourceID: envelope.SourceID, ChannelID: envelope.ChannelOrEndpoint,
			EpochKind: kind, EpochID: epoch.ID, ArrivalOrdinal: envelope.ArrivalOrdinal,
			MessageOrdinal: envelope.MessageOrdinal, RawSegmentSHA256: rawSegmentSHA256,
			RawRecordOrdinal: rawRecordOrdinal, RawPayloadSHA256: Hash(envelope.RawPayloadSHA256),
		},
		QualityFlags: slices.Clone(flags),
	}
	if err := record.Validate(); err != nil {
		return RawRecord{}, err
	}
	return record, nil
}

func (r RawRecord) Validate() error {
	if err := r.Envelope.Validate(); err != nil {
		return fmt.Errorf("%w: envelope", ErrInvalidRawRecord)
	}
	if r.Coordinate.SourceID != r.Envelope.SourceID || r.Coordinate.ChannelID != r.Envelope.ChannelOrEndpoint ||
		r.Coordinate.ArrivalOrdinal != r.Envelope.ArrivalOrdinal || r.Coordinate.MessageOrdinal != r.Envelope.MessageOrdinal ||
		r.Coordinate.RawPayloadSHA256 != Hash(r.Envelope.RawPayloadSHA256) {
		return fmt.Errorf("%w: coordinate does not match envelope", ErrInvalidRawRecord)
	}
	epoch, err := r.Envelope.StreamEpoch()
	if err != nil || epoch.ID != r.Coordinate.EpochID {
		return fmt.Errorf("%w: epoch does not match envelope", ErrInvalidRawRecord)
	}
	if (epoch.Kind == capture.EpochConnection) != (r.Coordinate.EpochKind == ConnectionEpoch) {
		return fmt.Errorf("%w: epoch kind does not match envelope", ErrInvalidRawRecord)
	}
	if r.Coordinate.RawSegmentSHA256 == (Hash{}) {
		return fmt.Errorf("%w: raw segment SHA-256 is required", ErrInvalidRawRecord)
	}
	if len(r.QualityFlags) > MaxQualityFlags {
		return fmt.Errorf("%w: quality flag count exceeds bound", ErrInvalidRawRecord)
	}
	for _, flag := range r.QualityFlags {
		if _, ok := validQualityFlags[flag]; !ok {
			return fmt.Errorf("%w: unknown quality flag", ErrInvalidRawRecord)
		}
	}
	return nil
}

func cloneEnvelope(envelope capture.EnvelopeV1) capture.EnvelopeV1 {
	envelope.RawPayload = slices.Clone(envelope.RawPayload)
	envelope.Extensions = slices.Clone(envelope.Extensions)
	return envelope
}

type Metadata struct {
	EventID                 Hash
	EventIDEncodingVersion  uint16
	SchemaName              string
	SchemaVersion           uint16
	SourceID                string
	ChannelID               string
	InstrumentUID           string
	EpochKind               EpochKind
	EpochID                 [16]byte
	ArrivalOrdinal          uint64
	MessageOrdinal          uint32
	ExchangeTimeNS          OptionalInt64
	ExchangeTimeResolution  TimeResolution
	SourceEventTimeNS       OptionalInt64
	SourceTimeResolution    TimeResolution
	ReceivedTimeNS          int64
	RawSegmentSHA256        Hash
	RawRecordOrdinal        uint64
	RawPayloadSHA256        Hash
	SourceSchemaFingerprint Hash
	MapperVersion           string
	MapperBindingID         Hash
	CatalogSnapshotID       Hash
	QualityFlags            []QualityFlag
}

type MetadataInput struct {
	Record                  RawRecord
	SchemaName              string
	SchemaVersion           uint16
	InstrumentUID           string
	ExchangeTimeNS          OptionalInt64
	ExchangeTimeResolution  TimeResolution
	SourceEventTimeNS       OptionalInt64
	SourceTimeResolution    TimeResolution
	SourceSchemaFingerprint Hash
	MapperVersion           string
	MapperBindingID         Hash
	CatalogSnapshotID       Hash
	AdditionalQualityFlags  []QualityFlag
}

func NewMetadata(in MetadataInput) (Metadata, error) {
	if err := in.Record.Validate(); err != nil {
		return Metadata{}, err
	}
	flags := append(slices.Clone(in.Record.QualityFlags), in.AdditionalQualityFlags...)
	slices.Sort(flags)
	flags = slices.Compact(flags)
	metadata := Metadata{
		EventIDEncodingVersion: EventIDEncodingVersion,
		SchemaName:             in.SchemaName, SchemaVersion: in.SchemaVersion,
		SourceID: in.Record.Coordinate.SourceID, ChannelID: in.Record.Coordinate.ChannelID,
		InstrumentUID: in.InstrumentUID, EpochKind: in.Record.Coordinate.EpochKind,
		EpochID: in.Record.Coordinate.EpochID, ArrivalOrdinal: in.Record.Coordinate.ArrivalOrdinal,
		MessageOrdinal: in.Record.Coordinate.MessageOrdinal, ExchangeTimeNS: in.ExchangeTimeNS,
		ExchangeTimeResolution: in.ExchangeTimeResolution, SourceEventTimeNS: in.SourceEventTimeNS,
		SourceTimeResolution:    in.SourceTimeResolution,
		ReceivedTimeNS:          in.Record.Envelope.ReceivedWallTimeNS,
		RawSegmentSHA256:        in.Record.Coordinate.RawSegmentSHA256,
		RawRecordOrdinal:        in.Record.Coordinate.RawRecordOrdinal,
		RawPayloadSHA256:        in.Record.Coordinate.RawPayloadSHA256,
		SourceSchemaFingerprint: in.SourceSchemaFingerprint, MapperVersion: in.MapperVersion,
		MapperBindingID: in.MapperBindingID, CatalogSnapshotID: in.CatalogSnapshotID,
		QualityFlags: flags,
	}
	metadata.EventID = eventID(metadata)
	if err := metadata.Validate(); err != nil {
		return Metadata{}, err
	}
	return metadata, nil
}

func (m Metadata) Validate() error {
	_, registered := LookupSchema(m.SchemaName, m.SchemaVersion)
	instrumentValid := (m.InstrumentUID != "" || schemaAllowsEmptyInstrument(m.SchemaName, m.SchemaVersion)) &&
		len(m.InstrumentUID) <= capture.MaxIdentityBytes && strings.IndexByte(m.InstrumentUID, 0) < 0
	if m.EventIDEncodingVersion != EventIDEncodingVersion || !registered ||
		m.SchemaName == "" || len(m.SchemaName) > MaxSchemaNameBytes || strings.IndexByte(m.SchemaName, 0) >= 0 ||
		m.SourceID == "" || len(m.SourceID) > capture.MaxSourceIDBytes ||
		m.ChannelID == "" || len(m.ChannelID) > capture.MaxContractIDBytes ||
		!instrumentValid ||
		m.ArrivalOrdinal == 0 || m.ReceivedTimeNS < 0 || m.MapperVersion == "" ||
		len(m.MapperVersion) > MaxMapperVersionBytes || strings.IndexByte(m.MapperVersion, 0) >= 0 ||
		(m.EpochKind != ConnectionEpoch && m.EpochKind != PollCycleEpoch) || m.EpochID == ([16]byte{}) {
		return fmt.Errorf("%w: invalid common metadata", ErrInvalidNormalized)
	}
	if m.RawSegmentSHA256 == (Hash{}) || m.RawPayloadSHA256 == (Hash{}) || m.SourceSchemaFingerprint == (Hash{}) ||
		m.MapperBindingID == (Hash{}) || m.CatalogSnapshotID == (Hash{}) {
		return fmt.Errorf("%w: missing immutable identity", ErrInvalidNormalized)
	}
	if (!m.ExchangeTimeNS.Valid && m.ExchangeTimeNS.Value != 0) ||
		(!m.SourceEventTimeNS.Valid && m.SourceEventTimeNS.Value != 0) ||
		m.ExchangeTimeNS.Valid != (m.ExchangeTimeResolution != ResolutionAbsent) ||
		!validTimeResolution(m.ExchangeTimeResolution) ||
		m.SourceTimeResolution == ResolutionAbsent || !validTimeResolution(m.SourceTimeResolution) {
		return fmt.Errorf("%w: exchange/source time resolution mismatch", ErrInvalidNormalized)
	}
	if len(m.QualityFlags) > MaxQualityFlags || !slices.IsSorted(m.QualityFlags) {
		return fmt.Errorf("%w: quality flags are not bounded and sorted", ErrInvalidNormalized)
	}
	for i, flag := range m.QualityFlags {
		if _, ok := validQualityFlags[flag]; !ok || (i > 0 && flag == m.QualityFlags[i-1]) {
			return fmt.Errorf("%w: quality flags contain unknown or duplicate value", ErrInvalidNormalized)
		}
	}
	if m.EventID != eventID(m) {
		return fmt.Errorf("%w: event ID mismatch", ErrInvalidNormalized)
	}
	return nil
}

func validTimeResolution(value TimeResolution) bool {
	switch value {
	case ResolutionAbsent, ResolutionNanosecond, ResolutionMicrosecond, ResolutionMillisecond, ResolutionSecond:
		return true
	default:
		return false
	}
}

type UnitKind string

const (
	UnitBaseAssetAmount   UnitKind = "base_asset"
	UnitQuoteAssetAmount  UnitKind = "quote_asset"
	UnitQuotePerBase      UnitKind = "quote_asset_per_base_asset"
	UnitPercent           UnitKind = "percent"
	UnitImpliedVolatility UnitKind = "implied_volatility"
)

type Unit struct {
	Kind         UnitKind
	AssetID      string
	BaseAssetID  string
	QuoteAssetID string
}

func BaseAssetUnit(assetID string) Unit  { return Unit{Kind: UnitBaseAssetAmount, AssetID: assetID} }
func QuoteAssetUnit(assetID string) Unit { return Unit{Kind: UnitQuoteAssetAmount, AssetID: assetID} }
func SpotPriceUnit(baseAssetID, quoteAssetID string) Unit {
	return Unit{Kind: UnitQuotePerBase, BaseAssetID: baseAssetID, QuoteAssetID: quoteAssetID}
}
func PercentUnit() Unit           { return Unit{Kind: UnitPercent} }
func ImpliedVolatilityUnit() Unit { return Unit{Kind: UnitImpliedVolatility} }

func (u Unit) Validate() error {
	for _, id := range []string{u.AssetID, u.BaseAssetID, u.QuoteAssetID} {
		if len(id) > MaxUnitIDBytes || strings.IndexByte(id, 0) >= 0 {
			return fmt.Errorf("%w: invalid unit identity", ErrInvalidNormalized)
		}
	}
	switch u.Kind {
	case UnitBaseAssetAmount, UnitQuoteAssetAmount:
		if u.AssetID == "" || u.BaseAssetID != "" || u.QuoteAssetID != "" {
			return fmt.Errorf("%w: invalid asset amount unit", ErrInvalidNormalized)
		}
	case UnitQuotePerBase:
		if u.AssetID != "" || u.BaseAssetID == "" || u.QuoteAssetID == "" || u.BaseAssetID == u.QuoteAssetID {
			return fmt.Errorf("%w: invalid spot price unit", ErrInvalidNormalized)
		}
	case UnitPercent, UnitRate, UnitImpliedVolatility:
		if u.AssetID != "" || u.BaseAssetID != "" || u.QuoteAssetID != "" {
			return fmt.Errorf("%w: invalid dimensionless unit", ErrInvalidNormalized)
		}
	default:
		return fmt.Errorf("%w: unknown unit", ErrInvalidNormalized)
	}
	return nil
}

type Numeric struct {
	Decimal Decimal
	Unit    Unit
}

func (n Numeric) Validate() error {
	if err := n.Decimal.Validate(); err != nil {
		return err
	}
	return n.Unit.Validate()
}

type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

type AggregationKind string

const AggregationSingleMatch AggregationKind = "single_match"

type DuplicateStatus string

const DuplicateUnassessed DuplicateStatus = "unassessed_raw_row"

type TradeV1 struct {
	Metadata              Metadata
	NativeTradeID         uint64
	AggressorSide         Side
	BuyerIsMaker          bool
	NativeIgnoreFlag      bool
	Price                 Numeric
	Amount                Numeric
	AggregationKind       AggregationKind
	NativeDuplicateStatus DuplicateStatus
}

type UpdateKind string

const UpdateDelta UpdateKind = "delta"

type LevelAction string

const (
	LevelUpsert LevelAction = "upsert_absolute"
	LevelDelete LevelAction = "delete_zero_amount"
)

type BookLevel struct {
	Side         Side
	LevelOrdinal uint32
	Action       LevelAction
	Price        Numeric
	Amount       Numeric
}

type BookUpdateV1 struct {
	Metadata                  Metadata
	UpdateKind                UpdateKind
	DepthContract             string
	AggregationContract       string
	FirstSequence             uint64
	LastSequence              uint64
	PreviousSequence          OptionalUint64
	Checksum                  SourceState
	Bids                      []BookLevel
	Asks                      []BookLevel
	AmountSemantics           string
	ReconstructionEligibility string
}

type RPIInclusionState string

const RPINotApplicable RPIInclusionState = "not_applicable"

type QuoteV1 struct {
	Metadata          Metadata
	NativeSourceRole  string
	UpdateID          uint64
	BidPrice          Numeric
	BidAmount         Numeric
	AskPrice          Numeric
	AskAmount         Numeric
	RPIInclusionState RPIInclusionState
	SourceTimeNS      OptionalInt64
}

type WindowKind string

const WindowRolling24Hours WindowKind = "rolling_previous_24_hours_not_utc_day"

type TickerV1 struct {
	Metadata                    Metadata
	NativeSourceRole            string
	WindowKind                  WindowKind
	WindowOpenSemantics         string
	WindowCloseSemantics        string
	WindowOpenTimeNS            int64
	WindowCloseTimeNS           int64
	WindowTimeResolution        TimeResolution
	NominalWindowDurationNS     uint64
	PriceChange                 Numeric
	PriceChangePercent          Numeric
	WeightedAveragePrice        Numeric
	FirstTradeBeforeWindowPrice Numeric
	LastPrice                   Numeric
	LastAmount                  Numeric
	NativeBestBidPrice          Numeric
	NativeBestBidAmount         Numeric
	NativeBestAskPrice          Numeric
	NativeBestAskAmount         Numeric
	OpenPrice                   Numeric
	HighPrice                   Numeric
	LowPrice                    Numeric
	BaseVolume                  Numeric
	QuoteVolume                 Numeric
	FirstTradeID                uint64
	LastTradeID                 uint64
	TradeCount                  uint64
}
