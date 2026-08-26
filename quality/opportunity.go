package quality

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
)

const MaxQualityStringBytes = 4096

var (
	ErrInvalidOpportunity      = errors.New("quality: invalid opportunity")
	ErrNoDefensibleDenominator = errors.New("quality: source contract has no defensible opportunity denominator")
	ErrOpportunityConflict     = errors.New("quality: immutable opportunity conflict")
)

type OpportunityExpectation = capture.OpportunityExpectation

const (
	OpportunityScheduledRESTPoll       = capture.OpportunityScheduledRESTPoll
	OpportunityAcknowledgementDeadline = capture.OpportunityAcknowledgementDeadline
	OpportunityHeartbeatDeadline       = capture.OpportunityHeartbeatDeadline
	OpportunityPeriodicPublication     = capture.OpportunityPeriodicPublication
	OpportunitySubscriptionInventory   = capture.OpportunitySubscriptionInventory
	OpportunitySequenceInterval        = capture.OpportunitySequenceInterval
	OpportunityMetadataDiscovery       = capture.OpportunityMetadataDiscovery
	OpportunityNativeFilePublication   = capture.OpportunityNativeFilePublication

	OpportunityStochasticTradeSilence    = capture.OpportunityStochasticTradeSilence
	OpportunityChangeTriggeredBBOSilence = capture.OpportunityChangeTriggeredBBOSilence
	OpportunityUndocumentedTickerSilence = capture.OpportunityUndocumentedTickerSilence
)

type OpportunityOutcome = capture.OpportunityOutcome

const (
	OpportunityOutcomeObserved              = capture.OpportunityOutcomeObserved
	OpportunityOutcomeObservedUnchanged     = capture.OpportunityOutcomeObservedUnchanged
	OpportunityOutcomeVenueUnavailable      = capture.OpportunityOutcomeVenueUnavailable
	OpportunityOutcomeSourceStale           = capture.OpportunityOutcomeSourceStale
	OpportunityOutcomeSequenceGap           = capture.OpportunityOutcomeSequenceGap
	OpportunityOutcomeRateLimited           = capture.OpportunityOutcomeRateLimited
	OpportunityOutcomeMalformed             = capture.OpportunityOutcomeMalformed
	OpportunityOutcomeSchemaRejected        = capture.OpportunityOutcomeSchemaRejected
	OpportunityOutcomeCollectorFailed       = capture.OpportunityOutcomeCollectorFailed
	OpportunityOutcomeIntentionallyExcluded = capture.OpportunityOutcomeIntentionallyExcluded
	OpportunityOutcomeUnknown               = capture.OpportunityOutcomeUnknown
)

// DefensibleOpportunity reports whether a source contract can supply a real
// denominator. It deliberately rejects stochastic arrivals and undocumented
// silence rather than turning silence into synthetic misses.
func DefensibleOpportunity(expectation OpportunityExpectation) bool {
	switch expectation {
	case OpportunityScheduledRESTPoll,
		OpportunityAcknowledgementDeadline,
		OpportunityHeartbeatDeadline,
		OpportunityPeriodicPublication,
		OpportunitySubscriptionInventory,
		OpportunitySequenceInterval,
		OpportunityMetadataDiscovery,
		OpportunityNativeFilePublication:
		return true
	default:
		return false
	}
}

func ParseOpportunityExpectation(value string) (OpportunityExpectation, error) {
	switch value {
	case "scheduled_rest_poll":
		return OpportunityScheduledRESTPoll, nil
	case "acknowledgement_deadline":
		return OpportunityAcknowledgementDeadline, nil
	case "heartbeat_deadline":
		return OpportunityHeartbeatDeadline, nil
	case "periodic_publication":
		return OpportunityPeriodicPublication, nil
	case "subscription_inventory":
		return OpportunitySubscriptionInventory, nil
	case "sequence_interval":
		return OpportunitySequenceInterval, nil
	case "metadata_discovery":
		return OpportunityMetadataDiscovery, nil
	case "native_file_publication":
		return OpportunityNativeFilePublication, nil
	default:
		return 0, fmt.Errorf("%w: expectation %q", ErrInvalidOpportunity, value)
	}
}

// Opportunity is one immutable expected event or window. Open rows have no
// terminal fields. Once Terminal is true, the outcome and evidence are part of
// the row identity and may only be archived or deleted after a committed spill.
type Opportunity struct {
	OpportunityID    string
	LedgerPartition  string
	SourceID         string
	ChannelID        string
	InstrumentUID    string
	Expectation      OpportunityExpectation
	ExpectedTimeNS   int64
	WindowStartNS    int64
	WindowEndNS      int64
	Scope            json.RawMessage
	Terminal         bool
	TerminalTimeNS   int64
	TerminalOutcome  OpportunityOutcome
	TerminalEvidence json.RawMessage
	CreatedTimeNS    int64
}

func (o Opportunity) Validate() error {
	for field, value := range map[string]string{
		"opportunity_id":   o.OpportunityID,
		"ledger_partition": o.LedgerPartition,
		"source_id":        o.SourceID,
		"channel_id":       o.ChannelID,
	} {
		if err := validateQualityString(field, value, true); err != nil {
			return errors.Join(ErrInvalidOpportunity, err)
		}
	}
	if err := validateQualityString("instrument_uid", o.InstrumentUID, false); err != nil {
		return errors.Join(ErrInvalidOpportunity, err)
	}
	if !DefensibleOpportunity(o.Expectation) {
		return fmt.Errorf("%w: %s: %w", ErrInvalidOpportunity, o.Expectation, ErrNoDefensibleDenominator)
	}
	if o.ExpectedTimeNS < 0 || o.WindowStartNS < 0 || o.WindowEndNS < o.WindowStartNS ||
		o.ExpectedTimeNS < o.WindowStartNS || o.ExpectedTimeNS > o.WindowEndNS || o.CreatedTimeNS < 0 {
		return fmt.Errorf("%w: invalid expected/window/created times", ErrInvalidOpportunity)
	}
	canonicalScope, err := canonicalJSONObject(o.Scope)
	if err != nil || !bytes.Equal(canonicalScope, o.Scope) {
		return fmt.Errorf("%w: scope must be a canonical JSON object", ErrInvalidOpportunity)
	}
	if !o.Terminal {
		if o.TerminalTimeNS != 0 || o.TerminalOutcome != 0 || len(o.TerminalEvidence) != 0 {
			return fmt.Errorf("%w: open row has terminal fields", ErrInvalidOpportunity)
		}
		return nil
	}
	if o.TerminalTimeNS < 0 || o.TerminalOutcome < OpportunityOutcomeObserved || o.TerminalOutcome > OpportunityOutcomeUnknown {
		return fmt.Errorf("%w: invalid terminal outcome", ErrInvalidOpportunity)
	}
	canonicalEvidence, err := canonicalJSONObject(o.TerminalEvidence)
	if err != nil || !bytes.Equal(canonicalEvidence, o.TerminalEvidence) {
		return fmt.Errorf("%w: terminal evidence must be a canonical JSON object", ErrInvalidOpportunity)
	}
	return nil
}

func CanonicalOpportunityObject(value json.RawMessage) (json.RawMessage, error) {
	return canonicalJSONObject(value)
}

func canonicalJSONObject(value json.RawMessage) (json.RawMessage, error) {
	if len(value) == 0 {
		return nil, errors.New("quality: empty JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("quality: trailing JSON value")
	}
	if _, ok := decoded.(map[string]any); !ok {
		return nil, errors.New("quality: JSON value is not an object")
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}

func (o Opportunity) LogicalHash() ([sha256.Size]byte, error) {
	if err := o.Validate(); err != nil {
		return [sha256.Size]byte{}, err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("quality-opportunity-v1\x00"))
	writeHashString(hasher, o.OpportunityID)
	writeHashString(hasher, o.LedgerPartition)
	writeHashString(hasher, o.SourceID)
	writeHashString(hasher, o.ChannelID)
	writeHashString(hasher, o.InstrumentUID)
	writeHashString(hasher, o.Expectation.String())
	writeHashInt64(hasher, o.ExpectedTimeNS)
	writeHashInt64(hasher, o.WindowStartNS)
	writeHashInt64(hasher, o.WindowEndNS)
	writeHashBytes(hasher, o.Scope)
	if o.Terminal {
		_, _ = hasher.Write([]byte{1})
		writeHashInt64(hasher, o.TerminalTimeNS)
		writeHashString(hasher, o.TerminalOutcome.String())
		writeHashBytes(hasher, o.TerminalEvidence)
	} else {
		_, _ = hasher.Write([]byte{0})
	}
	writeHashInt64(hasher, o.CreatedTimeNS)
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result, nil
}

func sameOpportunity(left, right Opportunity) bool {
	leftHash, leftErr := left.LogicalHash()
	rightHash, rightErr := right.LogicalHash()
	return leftErr == nil && rightErr == nil && leftHash == rightHash
}

type CoverageScope struct {
	LedgerPartition string
	SourceID        string
	ChannelID       string
	InstrumentUID   string
	Expectation     OpportunityExpectation
}

type CoverageCount struct {
	Scope    CoverageScope
	Total    uint64
	Open     uint64
	Outcomes map[OpportunityOutcome]uint64
}

type CoverageReport struct {
	Opportunities       uint64
	Counts              []CoverageCount
	IncidentAnnotations []string
}

type OpportunityArchive struct {
	Generation SpillGeneration
	Rows       []Opportunity
}

// RecomputeCoverage reads the logical union. Every supplied archive must be a
// committed, fully validated generation. Overlap is deduplicated only when the
// immutable logical row identity is exact.
func RecomputeCoverage(current []Opportunity, archives []OpportunityArchive, incidents []Incident) (CoverageReport, error) {
	union := make(map[string]Opportunity, len(current))
	add := func(row Opportunity) error {
		if err := row.Validate(); err != nil {
			return err
		}
		if existing, ok := union[row.OpportunityID]; ok {
			if !sameOpportunity(existing, row) {
				return fmt.Errorf("%w: %s", ErrOpportunityConflict, row.OpportunityID)
			}
			return nil
		}
		union[row.OpportunityID] = row
		return nil
	}
	for _, row := range current {
		if err := add(row); err != nil {
			return CoverageReport{}, err
		}
	}
	for _, archive := range archives {
		if err := archive.Generation.Validate(); err != nil {
			return CoverageReport{}, err
		}
		if archive.Generation.State != SpillCommitted || archive.Generation.Archive == nil {
			return CoverageReport{}, fmt.Errorf("%w: archive generation is not committed", ErrInvalidSpill)
		}
		if len(archive.Rows) != len(archive.Generation.RowIdentities) {
			return CoverageReport{}, fmt.Errorf("%w: archive row count mismatch", ErrInvalidSpill)
		}
		for index, row := range archive.Rows {
			identity := archive.Generation.RowIdentities[index]
			logicalHash, err := row.LogicalHash()
			if err != nil || !row.Terminal || row.LedgerPartition != archive.Generation.Partition ||
				row.OpportunityID != identity.OpportunityID || row.TerminalTimeNS != identity.TerminalTimeNS ||
				logicalHash != identity.LogicalHash {
				return CoverageReport{}, fmt.Errorf("%w: archive row identity mismatch", ErrInvalidSpill)
			}
		}
		logicalHash, err := OpportunityArchiveLogicalHash(archive.Rows)
		if err != nil || logicalHash != archive.Generation.Archive.LogicalHash {
			return CoverageReport{}, fmt.Errorf("%w: archive aggregate logical hash mismatch", ErrInvalidSpill)
		}
		for _, row := range archive.Rows {
			if err := add(row); err != nil {
				return CoverageReport{}, err
			}
		}
	}

	counts := make(map[CoverageScope]*CoverageCount)
	for _, row := range union {
		scope := CoverageScope{LedgerPartition: row.LedgerPartition, SourceID: row.SourceID, ChannelID: row.ChannelID,
			InstrumentUID: row.InstrumentUID, Expectation: row.Expectation}
		count := counts[scope]
		if count == nil {
			count = &CoverageCount{Scope: scope, Outcomes: make(map[OpportunityOutcome]uint64)}
			counts[scope] = count
		}
		count.Total++
		if row.Terminal {
			count.Outcomes[row.TerminalOutcome]++
		} else {
			count.Open++
		}
	}
	report := CoverageReport{Opportunities: uint64(len(union)), Counts: make([]CoverageCount, 0, len(counts))}
	for _, count := range counts {
		report.Counts = append(report.Counts, *count)
	}
	slices.SortFunc(report.Counts, compareCoverageCount)
	for _, incident := range incidents {
		if err := incident.Validate(); err != nil {
			return CoverageReport{}, err
		}
		report.IncidentAnnotations = append(report.IncidentAnnotations, incident.ID())
	}
	slices.Sort(report.IncidentAnnotations)
	report.IncidentAnnotations = slices.Compact(report.IncidentAnnotations)
	return report, nil
}

func compareCoverageCount(left, right CoverageCount) int {
	leftParts := []string{left.Scope.LedgerPartition, left.Scope.SourceID, left.Scope.ChannelID, left.Scope.InstrumentUID, left.Scope.Expectation.String()}
	rightParts := []string{right.Scope.LedgerPartition, right.Scope.SourceID, right.Scope.ChannelID, right.Scope.InstrumentUID, right.Scope.Expectation.String()}
	return strings.Compare(strings.Join(leftParts, "\x00"), strings.Join(rightParts, "\x00"))
}

func validateQualityString(field, value string, required bool) error {
	if required && value == "" {
		return fmt.Errorf("quality: %s is required", field)
	}
	if len(value) > MaxQualityStringBytes || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("quality: invalid %s", field)
	}
	return nil
}

type hashWriter interface{ Write([]byte) (int, error) }

func writeHashString(writer hashWriter, value string) { writeHashBytes(writer, []byte(value)) }
func writeHashBytes(writer hashWriter, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write(value)
}
func writeHashInt64(writer hashWriter, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = writer.Write(encoded[:])
}
