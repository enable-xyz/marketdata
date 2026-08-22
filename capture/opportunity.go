package capture

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidOpportunity    = errors.New("capture: invalid opportunity record")
	ErrStochasticDenominator = errors.New("capture: stochastic source cannot have an opportunity denominator")
)

type OpportunityExpectation uint8

const (
	OpportunityScheduledRESTPoll OpportunityExpectation = iota + 1
	OpportunityAcknowledgementDeadline
	OpportunityHeartbeatDeadline
	OpportunityPeriodicPublication
	OpportunitySubscriptionInventory
	OpportunitySequenceInterval
	OpportunityMetadataDiscovery
	OpportunityNativeFilePublication
)

// These values are named only so generic ingestion boundaries can reject a
// source contract explicitly instead of silently treating it as unknown.
const (
	OpportunityStochasticTradeSilence OpportunityExpectation = 0x80 + iota
	OpportunityChangeTriggeredBBOSilence
	OpportunityUndocumentedTickerSilence
)

func (e OpportunityExpectation) String() string {
	switch e {
	case OpportunityScheduledRESTPoll:
		return "scheduled_rest_poll"
	case OpportunityAcknowledgementDeadline:
		return "acknowledgement_deadline"
	case OpportunityHeartbeatDeadline:
		return "heartbeat_deadline"
	case OpportunityPeriodicPublication:
		return "periodic_publication"
	case OpportunitySubscriptionInventory:
		return "subscription_inventory"
	case OpportunitySequenceInterval:
		return "sequence_interval"
	case OpportunityMetadataDiscovery:
		return "metadata_discovery"
	case OpportunityNativeFilePublication:
		return "native_file_publication"
	case OpportunityStochasticTradeSilence:
		return "stochastic_trade_silence"
	case OpportunityChangeTriggeredBBOSilence:
		return "change_triggered_bbo_silence"
	case OpportunityUndocumentedTickerSilence:
		return "undocumented_ticker_silence"
	default:
		return fmt.Sprintf("opportunity_expectation(%d)", e)
	}
}

type OpportunityOutcome uint8

const (
	OpportunityOutcomeObserved OpportunityOutcome = iota + 1
	OpportunityOutcomeObservedUnchanged
	OpportunityOutcomeVenueUnavailable
	OpportunityOutcomeSourceStale
	OpportunityOutcomeSequenceGap
	OpportunityOutcomeRateLimited
	OpportunityOutcomeMalformed
	OpportunityOutcomeSchemaRejected
	OpportunityOutcomeCollectorFailed
	OpportunityOutcomeIntentionallyExcluded
	OpportunityOutcomeUnknown
)

func (o OpportunityOutcome) String() string {
	switch o {
	case OpportunityOutcomeObserved:
		return "observed"
	case OpportunityOutcomeObservedUnchanged:
		return "observed_unchanged"
	case OpportunityOutcomeVenueUnavailable:
		return "venue_unavailable"
	case OpportunityOutcomeSourceStale:
		return "source_stale"
	case OpportunityOutcomeSequenceGap:
		return "sequence_gap"
	case OpportunityOutcomeRateLimited:
		return "rate_limited"
	case OpportunityOutcomeMalformed:
		return "malformed"
	case OpportunityOutcomeSchemaRejected:
		return "schema_rejected"
	case OpportunityOutcomeCollectorFailed:
		return "collector_failed"
	case OpportunityOutcomeIntentionallyExcluded:
		return "intentionally_excluded"
	case OpportunityOutcomeUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("opportunity_outcome(%d)", o)
	}
}

func ParseOpportunityOutcome(value string) (OpportunityOutcome, error) {
	switch value {
	case "observed":
		return OpportunityOutcomeObserved, nil
	case "observed_unchanged":
		return OpportunityOutcomeObservedUnchanged, nil
	case "venue_unavailable":
		return OpportunityOutcomeVenueUnavailable, nil
	case "source_stale":
		return OpportunityOutcomeSourceStale, nil
	case "sequence_gap":
		return OpportunityOutcomeSequenceGap, nil
	case "rate_limited":
		return OpportunityOutcomeRateLimited, nil
	case "malformed":
		return OpportunityOutcomeMalformed, nil
	case "schema_rejected":
		return OpportunityOutcomeSchemaRejected, nil
	case "collector_failed":
		return OpportunityOutcomeCollectorFailed, nil
	case "intentionally_excluded":
		return OpportunityOutcomeIntentionallyExcluded, nil
	case "unknown":
		return OpportunityOutcomeUnknown, nil
	default:
		return 0, opportunityError("terminal_outcome", fmt.Sprintf("unsupported value %q", value))
	}
}

type OptionalUint64 struct {
	Value uint64
	Valid bool
}

// OpportunityRecord is a terminal record for one defensible expected event or
// window. It intentionally has no constructor for stochastic trade arrivals,
// change-triggered BBO, or undocumented ticker silence.
type OpportunityRecord struct {
	OpportunityID                string
	Expectation                  OpportunityExpectation
	SourceID                     string
	ChannelOrEndpoint            string
	NativeSymbol                 OptionalString
	InstrumentUID                OptionalString
	ConnectionEpoch              OptionalEpoch
	PollCycleID                  OptionalEpoch
	SubscriptionOrRequestID      OptionalString
	ScheduledAtNS                OptionalInt64
	DeadlineAtNS                 OptionalInt64
	ExpectedSequenceStart        OptionalUint64
	ExpectedSequenceEnd          OptionalUint64
	ExpectedInventoryFingerprint OptionalHash
	TerminalOutcome              OpportunityOutcome
	TerminalAtNS                 OptionalInt64
	RecorderVersion              string
}

// NewOpportunity validates a terminal opportunity without manufacturing any
// timing, sequence, inventory, or source expectation.
func NewOpportunity(record OpportunityRecord) (OpportunityRecord, error) {
	if err := record.Validate(); err != nil {
		return OpportunityRecord{}, err
	}
	return record, nil
}

func (r OpportunityRecord) Validate() error {
	if err := validateOpportunityRequiredString("opportunity_id", r.OpportunityID, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateOpportunityRequiredString("source_id", r.SourceID, MaxSourceIDBytes); err != nil {
		return err
	}
	if err := validateOpportunityRequiredString("channel_or_endpoint", r.ChannelOrEndpoint, MaxContractIDBytes); err != nil {
		return err
	}
	if err := validateOpportunityRequiredString("recorder_version", r.RecorderVersion, MaxRecorderVersionBytes); err != nil {
		return err
	}
	if err := validateOpportunityOptionalString("native_symbol", r.NativeSymbol, MaxSymbolBytes); err != nil {
		return err
	}
	if err := validateOpportunityOptionalString("instrument_uid", r.InstrumentUID, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateOpportunityOptionalString("subscription_or_request_id", r.SubscriptionOrRequestID, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateOpportunityOptionalEpoch("connection_epoch", r.ConnectionEpoch); err != nil {
		return err
	}
	if err := validateOpportunityOptionalEpoch("poll_cycle_id", r.PollCycleID); err != nil {
		return err
	}
	epoch, err := opportunityEpoch(r.ConnectionEpoch, r.PollCycleID)
	if err != nil {
		return err
	}
	if err := validateOpportunityOptionalInt64("scheduled_at_ns", r.ScheduledAtNS); err != nil {
		return err
	}
	if err := validateOpportunityOptionalInt64("deadline_at_ns", r.DeadlineAtNS); err != nil {
		return err
	}
	if err := validateOpportunityOptionalInt64("terminal_at_ns", r.TerminalAtNS); err != nil {
		return err
	}
	if !r.TerminalAtNS.Valid {
		return opportunityError("terminal_at_ns", "is required for a terminal opportunity")
	}
	if err := validateOptionalOpportunityUint64("expected_sequence_start", r.ExpectedSequenceStart); err != nil {
		return err
	}
	if err := validateOptionalOpportunityUint64("expected_sequence_end", r.ExpectedSequenceEnd); err != nil {
		return err
	}
	if !r.ExpectedInventoryFingerprint.Valid && r.ExpectedInventoryFingerprint.Value != [32]byte{} {
		return opportunityError("expected_inventory_fingerprint", "absent value must have canonical zero storage")
	}
	if r.TerminalOutcome < OpportunityOutcomeObserved || r.TerminalOutcome > OpportunityOutcomeUnknown {
		return opportunityError("terminal_outcome", fmt.Sprintf("unsupported value %d", r.TerminalOutcome))
	}
	return validateExpectation(r, epoch)
}

func validateExpectation(r OpportunityRecord, epoch StreamEpoch) error {
	sequencePresent := r.ExpectedSequenceStart.Valid || r.ExpectedSequenceEnd.Valid
	inventoryPresent := r.ExpectedInventoryFingerprint.Valid
	switch r.Expectation {
	case OpportunityScheduledRESTPoll:
		if epoch.Kind != EpochPollCycle || !r.ScheduledAtNS.Valid {
			return opportunityError("expectation", "scheduled REST poll requires a poll cycle and scheduled_at_ns")
		}
	case OpportunityAcknowledgementDeadline, OpportunityHeartbeatDeadline:
		if epoch.Kind != EpochConnection || !r.DeadlineAtNS.Valid {
			return opportunityError("expectation", r.Expectation.String()+" requires a connection epoch and deadline_at_ns")
		}
	case OpportunityPeriodicPublication:
		if !r.ScheduledAtNS.Valid || !r.DeadlineAtNS.Valid {
			return opportunityError("expectation", "periodic publication requires scheduled_at_ns and deadline_at_ns")
		}
	case OpportunitySubscriptionInventory:
		if epoch.Kind != EpochConnection || !r.DeadlineAtNS.Valid || !inventoryPresent {
			return opportunityError("expectation", "subscription inventory requires a connection epoch, deadline_at_ns, and expected inventory fingerprint")
		}
	case OpportunitySequenceInterval:
		if !r.ExpectedSequenceStart.Valid || !r.ExpectedSequenceEnd.Valid {
			return opportunityError("expectation", "sequence interval requires both sequence bounds")
		}
		if r.ExpectedSequenceStart.Value > r.ExpectedSequenceEnd.Value {
			return opportunityError("expected sequence", "start exceeds end")
		}
	case OpportunityMetadataDiscovery, OpportunityNativeFilePublication:
		if epoch.Kind != EpochPollCycle || !r.ScheduledAtNS.Valid {
			return opportunityError("expectation", r.Expectation.String()+" requires a poll cycle and scheduled_at_ns")
		}
	case OpportunityStochasticTradeSilence, OpportunityChangeTriggeredBBOSilence, OpportunityUndocumentedTickerSilence:
		return opportunityErrorWithCause("expectation", r.Expectation.String()+" has no defensible denominator", ErrStochasticDenominator)
	default:
		return opportunityError("expectation", fmt.Sprintf("unsupported value %d", r.Expectation))
	}
	if r.Expectation != OpportunitySequenceInterval && sequencePresent {
		return opportunityError("expected sequence", "is valid only for a sequence interval")
	}
	if r.Expectation != OpportunitySubscriptionInventory && inventoryPresent {
		return opportunityError("expected_inventory_fingerprint", "is valid only for subscription inventory")
	}
	return nil
}

func opportunityEpoch(connection, poll OptionalEpoch) (StreamEpoch, error) {
	if connection.Valid == poll.Valid {
		return StreamEpoch{}, opportunityErrorWithCause("stream epoch", "exactly one of connection_epoch or poll_cycle_id must be present", ErrInvalidEpoch)
	}
	if connection.Valid {
		return StreamEpoch{Kind: EpochConnection, ID: connection.Value}, nil
	}
	return StreamEpoch{Kind: EpochPollCycle, ID: poll.Value}, nil
}

func validateOpportunityOptionalInt64(field string, value OptionalInt64) error {
	if !value.Valid && value.Value != 0 {
		return opportunityError(field, "absent value must have canonical zero storage")
	}
	return nil
}

func validateOptionalOpportunityUint64(field string, value OptionalUint64) error {
	if !value.Valid && value.Value != 0 {
		return opportunityError(field, "absent value must have canonical zero storage")
	}
	return nil
}

func validateOpportunityRequiredString(field, value string, maximum int) error {
	if value == "" {
		return opportunityError(field, "must not be empty")
	}
	if len(value) > maximum {
		return opportunityError(field, fmt.Sprintf("has %d bytes, maximum is %d", len(value), maximum))
	}
	return nil
}

func validateOpportunityOptionalString(field string, value OptionalString, maximum int) error {
	if !value.Valid && value.Value != "" {
		return opportunityError(field, "absent value must have canonical zero storage")
	}
	if value.Valid && len(value.Value) > maximum {
		return opportunityError(field, fmt.Sprintf("has %d bytes, maximum is %d", len(value.Value), maximum))
	}
	return nil
}

func validateOpportunityOptionalEpoch(field string, value OptionalEpoch) error {
	if !value.Valid && value.Value != [16]byte{} {
		return opportunityErrorWithCause(field, "absent value must have canonical zero storage", ErrInvalidEpoch)
	}
	if value.Valid && value.Value == [16]byte{} {
		return opportunityErrorWithCause(field, "present epoch must not be all zero", ErrInvalidEpoch)
	}
	return nil
}

func opportunityError(field, problem string) error {
	return opportunityErrorWithCause(field, problem)
}

func opportunityErrorWithCause(field, problem string, causes ...error) error {
	all := make([]error, 0, len(causes)+1)
	all = append(all, ErrInvalidOpportunity)
	all = append(all, causes...)
	return &ValidationError{Field: field, Problem: problem, Cause: errors.Join(all...)}
}
