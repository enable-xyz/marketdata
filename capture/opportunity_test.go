package capture

import (
	"errors"
	"strings"
	"testing"
)

func TestOpportunity(t *testing.T) {
	t.Run("defensible expectations validate", func(t *testing.T) {
		expectations := []OpportunityExpectation{
			OpportunityScheduledRESTPoll,
			OpportunityAcknowledgementDeadline,
			OpportunityHeartbeatDeadline,
			OpportunityPeriodicPublication,
			OpportunitySubscriptionInventory,
			OpportunitySequenceInterval,
			OpportunityMetadataDiscovery,
			OpportunityNativeFilePublication,
		}
		for _, expectation := range expectations {
			record := validOpportunity(expectation)
			got, err := NewOpportunity(record)
			if err != nil {
				t.Fatalf("%s: NewOpportunity() error = %v", expectation, err)
			}
			if got != record {
				t.Fatalf("%s: constructor changed record\ngot:  %#v\nwant: %#v", expectation, got, record)
			}
		}
	})

	t.Run("terminal outcomes have exact stable round trip", func(t *testing.T) {
		outcomes := []OpportunityOutcome{
			OpportunityOutcomeObserved,
			OpportunityOutcomeObservedUnchanged,
			OpportunityOutcomeVenueUnavailable,
			OpportunityOutcomeSourceStale,
			OpportunityOutcomeSequenceGap,
			OpportunityOutcomeRateLimited,
			OpportunityOutcomeMalformed,
			OpportunityOutcomeSchemaRejected,
			OpportunityOutcomeCollectorFailed,
			OpportunityOutcomeIntentionallyExcluded,
			OpportunityOutcomeUnknown,
		}
		for _, outcome := range outcomes {
			encoded := outcome.String()
			decoded, err := ParseOpportunityOutcome(encoded)
			if err != nil {
				t.Fatalf("%s: ParseOpportunityOutcome() error = %v", encoded, err)
			}
			if decoded != outcome {
				t.Fatalf("ParseOpportunityOutcome(%q) = %d, want %d", encoded, decoded, outcome)
			}
			record := validOpportunity(OpportunityScheduledRESTPoll)
			record.TerminalOutcome = outcome
			if err := record.Validate(); err != nil {
				t.Fatalf("outcome %s: Validate() error = %v", outcome, err)
			}
		}
		if _, err := ParseOpportunityOutcome("disconnected"); !errors.Is(err, ErrInvalidOpportunity) {
			t.Fatalf("ParseOpportunityOutcome(disconnected) error = %v, want ErrInvalidOpportunity", err)
		}
	})

	t.Run("stochastic denominators are rejected", func(t *testing.T) {
		for _, expectation := range []OpportunityExpectation{
			OpportunityStochasticTradeSilence,
			OpportunityChangeTriggeredBBOSilence,
			OpportunityUndocumentedTickerSilence,
		} {
			record := validOpportunity(OpportunityPeriodicPublication)
			record.Expectation = expectation
			_, err := NewOpportunity(record)
			if !errors.Is(err, ErrStochasticDenominator) {
				t.Fatalf("%s: NewOpportunity() error = %v, want ErrStochasticDenominator", expectation, err)
			}
		}
	})

	t.Run("invalid evidence and bounds are rejected", func(t *testing.T) {
		tests := []struct {
			name   string
			base   OpportunityExpectation
			mutate func(*OpportunityRecord)
			cause  error
		}{
			{
				name: "unknown expectation",
				base: OpportunityScheduledRESTPoll,
				mutate: func(r *OpportunityRecord) {
					r.Expectation = 0x7f
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "unknown terminal outcome",
				base: OpportunityScheduledRESTPoll,
				mutate: func(r *OpportunityRecord) {
					r.TerminalOutcome = 0
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "missing terminal time",
				base: OpportunityScheduledRESTPoll,
				mutate: func(r *OpportunityRecord) {
					r.TerminalAtNS = OptionalInt64{}
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "scheduled poll without schedule",
				base: OpportunityScheduledRESTPoll,
				mutate: func(r *OpportunityRecord) {
					r.ScheduledAtNS = OptionalInt64{}
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "acknowledgement on poll epoch",
				base: OpportunityAcknowledgementDeadline,
				mutate: func(r *OpportunityRecord) {
					r.ConnectionEpoch = OptionalEpoch{}
					r.PollCycleID = OptionalEpoch{Value: epochValue(0x72), Valid: true}
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "sequence missing end",
				base: OpportunitySequenceInterval,
				mutate: func(r *OpportunityRecord) {
					r.ExpectedSequenceEnd = OptionalUint64{}
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "sequence reversed",
				base: OpportunitySequenceInterval,
				mutate: func(r *OpportunityRecord) {
					r.ExpectedSequenceStart.Value = 102
					r.ExpectedSequenceEnd.Value = 101
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "sequence fabricated for heartbeat",
				base: OpportunityHeartbeatDeadline,
				mutate: func(r *OpportunityRecord) {
					r.ExpectedSequenceStart = OptionalUint64{Value: 1, Valid: true}
					r.ExpectedSequenceEnd = OptionalUint64{Value: 1, Valid: true}
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "inventory without fingerprint",
				base: OpportunitySubscriptionInventory,
				mutate: func(r *OpportunityRecord) {
					r.ExpectedInventoryFingerprint = OptionalHash{}
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "both epochs",
				base: OpportunityPeriodicPublication,
				mutate: func(r *OpportunityRecord) {
					r.PollCycleID = OptionalEpoch{Value: epochValue(0x73), Valid: true}
				},
				cause: ErrInvalidEpoch,
			},
			{
				name: "identity bound",
				base: OpportunityScheduledRESTPoll,
				mutate: func(r *OpportunityRecord) {
					r.OpportunityID = strings.Repeat("o", MaxIdentityBytes+1)
				},
				cause: ErrInvalidOpportunity,
			},
			{
				name: "noncanonical null",
				base: OpportunityScheduledRESTPoll,
				mutate: func(r *OpportunityRecord) {
					r.InstrumentUID = OptionalString{Value: "discarded", Valid: false}
				},
				cause: ErrInvalidOpportunity,
			},
		}
		for _, test := range tests {
			record := validOpportunity(test.base)
			test.mutate(&record)
			if err := record.Validate(); !errors.Is(err, test.cause) {
				t.Fatalf("%s: Validate() error = %v, want cause %v", test.name, err, test.cause)
			}
		}
	})
}

func validOpportunity(expectation OpportunityExpectation) OpportunityRecord {
	record := OpportunityRecord{
		OpportunityID:           "opportunity-0001",
		Expectation:             expectation,
		SourceID:                "source-api-v1",
		ChannelOrEndpoint:       "contract-v2",
		NativeSymbol:            OptionalString{Value: "BTCUSDT", Valid: true},
		InstrumentUID:           OptionalString{Value: "instrument-17", Valid: true},
		ConnectionEpoch:         OptionalEpoch{Value: epochValue(0x71), Valid: true},
		SubscriptionOrRequestID: OptionalString{Value: "native-identity-4", Valid: true},
		TerminalOutcome:         OpportunityOutcomeObserved,
		TerminalAtNS:            OptionalInt64{Value: 1_700_000_000_009_000_000, Valid: true},
		RecorderVersion:         "recorder-build-abc123",
	}
	switch expectation {
	case OpportunityScheduledRESTPoll, OpportunityMetadataDiscovery, OpportunityNativeFilePublication:
		record.ConnectionEpoch = OptionalEpoch{}
		record.PollCycleID = OptionalEpoch{Value: epochValue(0x72), Valid: true}
		record.ScheduledAtNS = OptionalInt64{Value: 1_700_000_000_000_000_000, Valid: true}
	case OpportunityAcknowledgementDeadline, OpportunityHeartbeatDeadline:
		record.DeadlineAtNS = OptionalInt64{Value: 1_700_000_000_008_000_000, Valid: true}
	case OpportunityPeriodicPublication:
		record.ScheduledAtNS = OptionalInt64{Value: 1_700_000_000_000_000_000, Valid: true}
		record.DeadlineAtNS = OptionalInt64{Value: 1_700_000_000_008_000_000, Valid: true}
	case OpportunitySubscriptionInventory:
		record.DeadlineAtNS = OptionalInt64{Value: 1_700_000_000_008_000_000, Valid: true}
		record.ExpectedInventoryFingerprint = OptionalHash{Value: PayloadHash([]byte("inventory-v1")), Valid: true}
	case OpportunitySequenceInterval:
		record.ExpectedSequenceStart = OptionalUint64{Value: 100, Valid: true}
		record.ExpectedSequenceEnd = OptionalUint64{Value: 101, Valid: true}
	}
	return record
}
