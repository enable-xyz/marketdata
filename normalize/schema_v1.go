package normalize

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
)

const MaxSchemaContractBytes = 16 * 1024

// SchemaDescriptor freezes the logical contract associated with one normalized
// row identity. Contract is a canonical, ordered field description; Fingerprint
// commits to every descriptor field and the logical encoding version.
type SchemaDescriptor struct {
	Kind                   EventKind
	Name                   string
	Version                uint16
	LogicalEncodingVersion uint16
	Contract               string
	Fingerprint            Hash
}

var schemaV1 = mustSchemaV1Registry([]SchemaDescriptor{
	newSchemaDescriptor(EventTrade, TradeSchemaName, TradeSchemaVersion, "metadata;native_trade_id;aggressor_side;buyer_is_maker;native_ignore_flag;price(decimal,unit);amount(decimal,unit);aggregation_kind;native_duplicate_status"),
	newSchemaDescriptor(EventBookUpdate, BookUpdateSchemaName, BookUpdateSchemaVersion, "metadata;update_kind;depth_contract;aggregation_contract;first_sequence;last_sequence;previous_sequence;checksum;bids[](side,ordinal,action,price,amount);asks[](side,ordinal,action,price,amount);amount_semantics;reconstruction_eligibility"),
	newSchemaDescriptor(EventQuote, QuoteSchemaName, QuoteSchemaVersion, "metadata;native_source_role;update_id;bid_price;bid_amount;ask_price;ask_amount;rpi_inclusion_state;source_time_ns"),
	newSchemaDescriptor(EventTicker, TickerSchemaName, TickerSchemaVersion, "metadata;native_source_role;window_kind;window_open_semantics;window_close_semantics;window_open_time_ns;window_close_time_ns;window_time_resolution;nominal_window_duration_ns;price_change;price_change_percent;weighted_average_price;first_trade_before_window_price;last_price;last_amount;native_best_bid_price;native_best_bid_amount;native_best_ask_price;native_best_ask_amount;open_price;high_price;low_price;base_volume;quote_volume;first_trade_id;last_trade_id;trade_count"),
	newSchemaDescriptor(EventDerivativeTicker, DerivativeTickerSchemaName, DerivativeTickerSchemaVersion, "metadata;native_source_role;last_price(field);mark_price(field);index_price(field);funding_rate(field);next_funding_time(field);open_interest[](state,variant,native,sidedness,reported_variants,multiplier_catalog_version,derived_base,derived_quote,derived_usd,provenance);settlement_price(field);basis(field);premium(field)"),
	newSchemaDescriptor(EventLiquidation, LiquidationSchemaName, LiquidationSchemaVersion, "metadata;native_source_role;native_role;side;side_semantics;amount(native_decimal,unit);price(field);price_type;completeness;window(start,end,duration,selection,per_symbol,batch_id)"),
	newSchemaDescriptor(EventOptionSummary, OptionSummarySchemaName, OptionSummarySchemaVersion, "metadata;native_source_role;instrument(field);underlying(field);index(field);expiry(field);strike(field);call_put(field);bid_price(field);ask_price(field);last_price(field);mark_price(field);bid_iv(field);ask_iv(field);mark_iv(field);delta(native_field);gamma(native_field);vega(native_field);theta(native_field);rho(native_field);open_interest(native_field);volume(native_field);forward_price(field);underlying_price(field);index_price(field)"),
	newSchemaDescriptor(EventInstrument, InstrumentEventSchemaName, InstrumentEventSchemaVersion, "metadata;metadata_generation(field);native_state_before(field);native_state_after(field);listing_time(field);continuous_trading_time(field);expiry_time(field);delivery_time(field);delisting_time(field);tick_size(change);lot_size(change);contract_multiplier(change);payoff(change);old_raw_hash(field);new_raw_hash(field);resolution_status(field)"),
	newSchemaDescriptor(EventSourceHealth, SourceHealthSchemaName, SourceHealthSchemaVersion, "metadata(source-level-instrument-nullable);dimension;scope;component;native_role;previous_status(field);current_status(field);native_previous_state(field);native_current_state(field);previous_measurement(field);current_measurement(field);window_start(field);window_end(field);native_code(field);detail(field)"),
})

func newSchemaDescriptor(kind EventKind, name string, version uint16, contract string) SchemaDescriptor {
	descriptor := SchemaDescriptor{Kind: kind, Name: name, Version: version, LogicalEncodingVersion: LogicalEncodingVersion, Contract: contract}
	descriptor.Fingerprint = schemaDescriptorFingerprint(descriptor)
	return descriptor
}

func schemaDescriptorFingerprint(descriptor SchemaDescriptor) Hash {
	payload := make([]byte, 0, len(descriptor.Kind)+len(descriptor.Name)+len(descriptor.Contract)+64)
	appendString := func(value string) {
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(value)))
		payload = append(payload, value...)
	}
	appendString("normalized-schema-descriptor")
	appendString(string(descriptor.Kind))
	appendString(descriptor.Name)
	payload = binary.BigEndian.AppendUint16(payload, descriptor.Version)
	payload = binary.BigEndian.AppendUint16(payload, descriptor.LogicalEncodingVersion)
	appendString(descriptor.Contract)
	return Hash(sha256.Sum256(payload))
}

func mustSchemaV1Registry(descriptors []SchemaDescriptor) []SchemaDescriptor {
	if err := ValidateSchemaRegistry(descriptors); err != nil {
		panic(err)
	}
	return slices.Clone(descriptors)
}

// SchemaV1Registry returns the nine immutable v1 descriptors in canonical row
// order. The returned slice is independent of the package registry.
func SchemaV1Registry() []SchemaDescriptor { return slices.Clone(schemaV1) }

// LookupSchema fails closed for every name/version pair not frozen in v1.
func LookupSchema(name string, version uint16) (SchemaDescriptor, bool) {
	for _, descriptor := range schemaV1 {
		if descriptor.Name == name && descriptor.Version == version {
			return descriptor, true
		}
	}
	return SchemaDescriptor{}, false
}

// ValidateSchemaRegistry checks descriptor integrity and rejects duplicate
// schema identities or event kinds.
func ValidateSchemaRegistry(descriptors []SchemaDescriptor) error {
	if len(descriptors) == 0 {
		return fmt.Errorf("%w: empty schema registry", ErrInvalidNormalized)
	}
	type schemaIdentity struct {
		name    string
		version uint16
	}
	identities := make(map[schemaIdentity]struct{}, len(descriptors))
	kinds := make(map[EventKind]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor.Kind == "" || descriptor.Name == "" || len(descriptor.Name) > MaxSchemaNameBytes ||
			strings.IndexByte(descriptor.Name, 0) >= 0 || descriptor.Version == 0 ||
			descriptor.LogicalEncodingVersion != LogicalEncodingVersion || descriptor.Contract == "" ||
			len(descriptor.Contract) > MaxSchemaContractBytes || strings.IndexByte(descriptor.Contract, 0) >= 0 ||
			descriptor.Fingerprint == (Hash{}) || descriptor.Fingerprint != schemaDescriptorFingerprint(descriptor) {
			return fmt.Errorf("%w: invalid schema descriptor", ErrInvalidNormalized)
		}
		identity := schemaIdentity{name: descriptor.Name, version: descriptor.Version}
		if _, exists := identities[identity]; exists {
			return fmt.Errorf("%w: duplicate schema identity", ErrInvalidNormalized)
		}
		identities[identity] = struct{}{}
		if _, exists := kinds[descriptor.Kind]; exists {
			return fmt.Errorf("%w: duplicate schema event kind", ErrInvalidNormalized)
		}
		kinds[descriptor.Kind] = struct{}{}
	}
	return nil
}

func schemaAllowsEmptyInstrument(name string, version uint16) bool {
	descriptor, ok := LookupSchema(name, version)
	return ok && descriptor.Kind == EventSourceHealth
}
