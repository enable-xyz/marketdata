package deribit

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type Instrument struct {
	InstrumentName        string
	Kind                  normalize.DeribitInstrumentKind
	InstrumentType        normalize.DeribitInstrumentType
	BaseCurrency          string
	QuoteCurrency         string
	CounterCurrency       string
	SettlementCurrency    string
	State                 string
	ContractSize          SourceNumber
	CreationTimestampMS   int64
	ExpirationTimestampMS int64
	PriceIndex            string
	OptionType            string
	Strike                SourceNumber
	IsActiveState         normalize.SourceState
	IsActive              bool
}

type InstrumentEvidence struct {
	InstrumentUID         string
	CatalogGeneration     uint64
	MetadataRawSHA256     normalize.Hash
	ValidFromNS           int64
	ValidUntilNS          normalize.OptionalInt64
	FixtureClassification string
	OfficialURL           string
	Section               string
	DerivedFrom           string
}

func ParseInstrument(payload []byte) (Instrument, error) {
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return Instrument{}, ErrInvalidRPC
	}
	return parseInstrumentObject(json.RawMessage(payload))
}

func ParseInstrumentResult(payload []byte) ([]Instrument, error) {
	var response struct {
		JSONRPC string            `json:"jsonrpc"`
		Result  []json.RawMessage `json:"result"`
		Error   *rpcError         `json:"error"`
	}
	if err := decodeRPC(payload, &response); err != nil || response.JSONRPC != JSONRPCVersion || response.Error != nil || len(response.Result) == 0 {
		return nil, ErrInvalidRPC
	}
	instruments := make([]Instrument, 0, len(response.Result))
	for _, raw := range response.Result {
		instrument, err := parseInstrumentObject(raw)
		if err != nil {
			return nil, err
		}
		instruments = append(instruments, instrument)
	}
	return instruments, nil
}

func parseInstrumentObject(raw json.RawMessage) (Instrument, error) {
	object, err := decodeObject(raw)
	if err != nil {
		return Instrument{}, err
	}
	name, err := requiredString(object, "instrument_name")
	if err != nil {
		return Instrument{}, err
	}
	kindText, err := requiredString(object, "kind")
	if err != nil {
		return Instrument{}, err
	}
	kind := normalize.DeribitInstrumentKind(kindText)
	if kind != normalize.DeribitInstrumentFuture && kind != normalize.DeribitInstrumentOption && kind != normalize.DeribitInstrumentSpot {
		return Instrument{}, ErrInvalidRPC
	}
	typeText, err := requiredString(object, "instrument_type")
	if err != nil {
		return Instrument{}, err
	}
	instrumentType := normalize.DeribitInstrumentType(typeText)
	if instrumentType != normalize.DeribitInstrumentLinear && instrumentType != normalize.DeribitInstrumentReversed {
		return Instrument{}, ErrInvalidRPC
	}
	base, err := requiredString(object, "base_currency")
	if err != nil {
		return Instrument{}, err
	}
	quote, err := requiredString(object, "quote_currency")
	if err != nil {
		return Instrument{}, err
	}
	counter, err := requiredString(object, "counter_currency")
	if err != nil {
		return Instrument{}, err
	}
	settlement, err := requiredString(object, "settlement_currency")
	if err != nil {
		return Instrument{}, err
	}
	state, err := requiredString(object, "state")
	if err != nil || !validBookState(state) {
		return Instrument{}, ErrInvalidRPC
	}
	contractSize, err := sourceNumber(object, "contract_size")
	if err != nil || (contractSize.State != normalize.SourceValue && contractSize.State != normalize.SourceNull) {
		return Instrument{}, ErrInvalidRPC
	}
	created, err := requiredInt64(object, "creation_timestamp")
	if err != nil {
		return Instrument{}, err
	}
	expires, err := requiredInt64(object, "expiration_timestamp")
	if err != nil {
		return Instrument{}, err
	}
	_, priceIndex, err := sourceString(object, "price_index")
	if err != nil {
		return Instrument{}, err
	}
	_, optionType, err := sourceString(object, "option_type")
	if err != nil || (kind == normalize.DeribitInstrumentOption && optionType != "call" && optionType != "put") ||
		(kind != normalize.DeribitInstrumentOption && optionType != "") {
		return Instrument{}, ErrInvalidRPC
	}
	strike, err := sourceNumber(object, "strike")
	if err != nil || (kind == normalize.DeribitInstrumentOption && strike.State != normalize.SourceValue) ||
		(kind != normalize.DeribitInstrumentOption && strike.State == normalize.SourceValue) {
		return Instrument{}, ErrInvalidRPC
	}
	activeState, active, err := sourceBool(object, "is_active")
	if err != nil {
		return Instrument{}, err
	}
	return Instrument{
		InstrumentName: name, Kind: kind, InstrumentType: instrumentType,
		BaseCurrency: base, QuoteCurrency: quote, CounterCurrency: counter,
		SettlementCurrency: settlement, State: state,
		ContractSize: contractSize, CreationTimestampMS: created, ExpirationTimestampMS: expires,
		PriceIndex: priceIndex, OptionType: optionType, Strike: strike,
		IsActiveState: activeState, IsActive: active,
	}, nil
}

func (i Instrument) Terms(evidence InstrumentEvidence) (normalize.DeribitInstrumentTerms, error) {
	if evidence.InstrumentUID == "" || evidence.ValidFromNS < 0 {
		return normalize.DeribitInstrumentTerms{}, ErrInvalidRPC
	}
	contractState := i.ContractSize.State
	var contractSize normalize.Decimal
	if contractState == normalize.SourceValue {
		value, err := i.ContractSize.Decimal(normalize.CanonicalAmountScale)
		if err != nil {
			return normalize.DeribitInstrumentTerms{}, err
		}
		contractSize = value
	}
	terms := normalize.DeribitInstrumentTerms{
		InstrumentUID: evidence.InstrumentUID, InstrumentName: i.InstrumentName,
		Kind: i.Kind, InstrumentType: i.InstrumentType,
		BaseAssetID: i.BaseCurrency, QuoteAssetID: i.QuoteCurrency, CounterAssetID: i.CounterCurrency, SettlementAssetID: i.SettlementCurrency,
		ContractSize: contractSize, ContractSizeState: contractState,
		ValidFromNS: evidence.ValidFromNS, ValidUntilNS: evidence.ValidUntilNS,
		CatalogGeneration: evidence.CatalogGeneration, MetadataRawSHA256: evidence.MetadataRawSHA256,
		DocumentationAtNS:     DocumentationAccessedAtNS,
		FixtureClassification: evidence.FixtureClassification, ProvenanceURL: evidence.OfficialURL,
		ProvenanceSection: evidence.Section, DerivedFrom: evidence.DerivedFrom,
	}
	if err := terms.Validate(); err != nil {
		return normalize.DeribitInstrumentTerms{}, err
	}
	return terms, nil
}

type LifecycleKind string

const (
	LifecycleCreation LifecycleKind = "creation"
	LifecycleState    LifecycleKind = "state"
)

type LifecycleMessage struct {
	Channel        string
	Kind           LifecycleKind
	InstrumentName string
	TimestampMS    int64
	NativeState    string
	Instrument     *Instrument
}

type MappedLifecycle struct {
	InstrumentUID string
	Kind          LifecycleKind
	SourceTimeNS  int64
	NativeState   string
	State         normalize.InstrumentLifecycleState
	Instrument    *Instrument
}

func ParseLifecycle(payload []byte) (LifecycleMessage, error) {
	channel, raw, err := notificationData(payload)
	if err != nil || !strings.HasPrefix(channel, "instrument.") {
		return LifecycleMessage{}, ErrInvalidRPC
	}
	if strings.HasPrefix(channel, "instrument.creation.") {
		instrument, err := parseInstrumentObject(raw)
		if err != nil {
			return LifecycleMessage{}, err
		}
		return LifecycleMessage{
			Channel: channel, Kind: LifecycleCreation, InstrumentName: instrument.InstrumentName,
			TimestampMS: instrument.CreationTimestampMS, NativeState: instrument.State, Instrument: &instrument,
		}, nil
	}
	if !strings.HasPrefix(channel, "instrument.state.") {
		return LifecycleMessage{}, ErrInvalidRPC
	}
	object, err := decodeObject(raw)
	if err != nil {
		return LifecycleMessage{}, err
	}
	instrument, err := requiredString(object, "instrument_name")
	if err != nil {
		return LifecycleMessage{}, err
	}
	state, err := requiredString(object, "state")
	if err != nil || !validBookState(state) {
		return LifecycleMessage{}, ErrInvalidRPC
	}
	timestamp, err := requiredInt64(object, "timestamp")
	if err != nil {
		return LifecycleMessage{}, err
	}
	return LifecycleMessage{
		Channel: channel, Kind: LifecycleState, InstrumentName: instrument,
		TimestampMS: timestamp, NativeState: state,
	}, nil
}

func (m LifecycleMessage) Normalized(instrumentUID string) (MappedLifecycle, error) {
	if instrumentUID == "" || m.InstrumentName == "" {
		return MappedLifecycle{}, ErrInvalidRPC
	}
	timestampNS, err := millisecondsToNanoseconds(m.TimestampMS)
	if err != nil {
		return MappedLifecycle{}, err
	}
	state, err := normalizedBookState(m.NativeState)
	if err != nil {
		return MappedLifecycle{}, err
	}
	return MappedLifecycle{
		InstrumentUID: instrumentUID, Kind: m.Kind, SourceTimeNS: timestampNS,
		NativeState: m.NativeState, State: state, Instrument: m.Instrument,
	}, nil
}

func normalizedBookState(state string) (normalize.InstrumentLifecycleState, error) {
	switch state {
	case "open":
		return normalize.InstrumentStateContinuousTrading, nil
	case "settlement":
		return normalize.InstrumentStateDelivery, nil
	case "delivered":
		return normalize.InstrumentStateDelivered, nil
	case "inactive", "locked", "halted":
		return normalize.InstrumentStateSuspended, nil
	case "archivized":
		return normalize.InstrumentStateDelisted, nil
	default:
		return "", fmt.Errorf("%w: unknown instrument state", ErrInvalidRPC)
	}
}

func validBookState(state string) bool {
	_, err := normalizedBookState(state)
	return err == nil
}
