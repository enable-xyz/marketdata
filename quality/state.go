package quality

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
)

var (
	ErrInvalidGap           = errors.New("quality: invalid gap")
	ErrInvalidGapTransition = errors.New("quality: invalid gap transition")
	ErrReservedGapState     = errors.New("quality: backfilled_explicitly is reserved and cannot be emitted in v1")
	ErrInvalidIncident      = errors.New("quality: invalid incident")
	ErrInvalidCorrection    = errors.New("quality: invalid correction")
	ErrInvalidHealth        = errors.New("quality: invalid source-health transition")
)

type GapState string

const (
	GapOpen                  GapState = "open"
	GapRecoveredCurrentState GapState = "recovered_current_state"
	GapBackfilledExplicitly  GapState = "backfilled_explicitly"
	GapPermanent             GapState = "permanent"
)

type Gap struct {
	GapID               string
	SourceID            string
	ChannelID           string
	InstrumentUID       string
	RangeStartNS        int64
	RangeEndNS          int64
	DetectionBasis      string
	FirstGoodCoordinate json.RawMessage
	LastGoodCoordinate  json.RawMessage
	AffectedFamilies    []string
	Confidence          float64
	Evidence            json.RawMessage
	State               GapState
	DetectedTimeNS      int64
	ResolvedTimeNS      int64
}

func NewGap(gap Gap) (Gap, error) {
	gap.AffectedFamilies = slices.Clone(gap.AffectedFamilies)
	if gap.State == "" {
		gap.State = GapOpen
	}
	if gap.State != GapOpen || gap.ResolvedTimeNS != 0 {
		return Gap{}, fmt.Errorf("%w: a new gap must be open", ErrInvalidGap)
	}
	if err := gap.Validate(); err != nil {
		return Gap{}, err
	}
	return gap, nil
}

func (g Gap) Validate() error {
	for _, field := range []struct {
		name, value string
		required    bool
	}{
		{"gap_id", g.GapID, true}, {"source_id", g.SourceID, true}, {"channel_id", g.ChannelID, true},
		{"instrument_uid", g.InstrumentUID, false}, {"detection_basis", g.DetectionBasis, true},
	} {
		if err := validateQualityString(field.name, field.value, field.required); err != nil {
			return errors.Join(ErrInvalidGap, err)
		}
	}
	if g.RangeStartNS < 0 || g.RangeEndNS < g.RangeStartNS || g.DetectedTimeNS < 0 || g.Confidence < 0 || g.Confidence > 1 {
		return fmt.Errorf("%w: invalid range, detection time, or confidence", ErrInvalidGap)
	}
	if len(g.AffectedFamilies) == 0 || !slices.IsSorted(g.AffectedFamilies) {
		return fmt.Errorf("%w: affected families must be nonempty and sorted", ErrInvalidGap)
	}
	for index, family := range g.AffectedFamilies {
		if err := validateQualityString("affected_family", family, true); err != nil || (index > 0 && family == g.AffectedFamilies[index-1]) {
			return fmt.Errorf("%w: affected families must be unique and valid", ErrInvalidGap)
		}
	}
	for name, value := range map[string]json.RawMessage{
		"first_good_coordinate": g.FirstGoodCoordinate,
		"last_good_coordinate":  g.LastGoodCoordinate,
		"evidence":              g.Evidence,
	} {
		canonical, err := canonicalJSONObject(value)
		if err != nil || !bytes.Equal(canonical, value) {
			return fmt.Errorf("%w: %s must be a canonical JSON object", ErrInvalidGap, name)
		}
	}
	switch g.State {
	case GapOpen:
		if g.ResolvedTimeNS != 0 {
			return fmt.Errorf("%w: open gap has resolution time", ErrInvalidGap)
		}
	case GapRecoveredCurrentState, GapPermanent:
		if g.ResolvedTimeNS < g.DetectedTimeNS {
			return fmt.Errorf("%w: terminal gap has invalid resolution time", ErrInvalidGap)
		}
	case GapBackfilledExplicitly:
		return ErrReservedGapState
	default:
		return fmt.Errorf("%w: unsupported state %q", ErrInvalidGap, g.State)
	}
	return nil
}

// TransitionGap performs the only v1 lifecycle edge: open to either current-
// state recovery (the blind interval remains) or permanent.
func TransitionGap(gap Gap, state GapState, resolvedTimeNS int64) (Gap, error) {
	if gap.State != GapOpen {
		return Gap{}, fmt.Errorf("%w: terminal gaps cannot transition", ErrInvalidGapTransition)
	}
	if state == GapBackfilledExplicitly {
		return Gap{}, ErrReservedGapState
	}
	if state != GapRecoveredCurrentState && state != GapPermanent {
		return Gap{}, fmt.Errorf("%w: open can resolve only to recovered_current_state or permanent", ErrInvalidGapTransition)
	}
	resolved := gap
	resolved.State = state
	resolved.ResolvedTimeNS = resolvedTimeNS
	if err := resolved.Validate(); err != nil {
		return Gap{}, err
	}
	return resolved, nil
}

type IncidentInput struct {
	IncidentID     string
	Annotation     string
	ReportSource   string
	AffectedTuples json.RawMessage
	HasRange       bool
	RangeStartNS   int64
	RangeEndNS     int64
	ReportedTimeNS int64
	CreatedTimeNS  int64
}

// Incident has no mutation API. PostgreSQL additionally rejects updates to all
// incident columns, so an annotation can never rewrite an opportunity outcome.
type Incident struct{ input IncidentInput }

func NewIncident(input IncidentInput) (Incident, error) {
	input.AffectedTuples = slices.Clone(input.AffectedTuples)
	incident := Incident{input: input}
	if err := incident.Validate(); err != nil {
		return Incident{}, err
	}
	return incident, nil
}

func (i Incident) Validate() error {
	for _, field := range []struct{ name, value string }{
		{"incident_id", i.input.IncidentID}, {"annotation", i.input.Annotation}, {"report_source", i.input.ReportSource},
	} {
		if err := validateQualityString(field.name, field.value, true); err != nil {
			return errors.Join(ErrInvalidIncident, err)
		}
	}
	canonical, err := canonicalJSONArray(i.input.AffectedTuples)
	if err != nil || !bytes.Equal(canonical, i.input.AffectedTuples) {
		return fmt.Errorf("%w: affected tuples must be a canonical JSON array", ErrInvalidIncident)
	}
	if i.input.ReportedTimeNS < 0 || i.input.CreatedTimeNS < 0 ||
		(i.input.HasRange && (i.input.RangeStartNS < 0 || i.input.RangeEndNS < i.input.RangeStartNS)) ||
		(!i.input.HasRange && (i.input.RangeStartNS != 0 || i.input.RangeEndNS != 0)) {
		return fmt.Errorf("%w: invalid time range", ErrInvalidIncident)
	}
	return nil
}

func (i Incident) ID() string                      { return i.input.IncidentID }
func (i Incident) Annotation() string              { return i.input.Annotation }
func (i Incident) ReportSource() string            { return i.input.ReportSource }
func (i Incident) AffectedTuples() json.RawMessage { return slices.Clone(i.input.AffectedTuples) }
func (i Incident) HasRange() bool                  { return i.input.HasRange }
func (i Incident) RangeStartNS() int64             { return i.input.RangeStartNS }
func (i Incident) RangeEndNS() int64               { return i.input.RangeEndNS }
func (i Incident) ReportedTimeNS() int64           { return i.input.ReportedTimeNS }
func (i Incident) CreatedTimeNS() int64            { return i.input.CreatedTimeNS }

type CorrectionInput struct {
	CorrectionID           string
	OriginalRawSegmentID   string
	OriginalGapID          string
	ReplacementDatasetID   string
	MapperReleaseID        string
	SupersedesCorrectionID string
	Reason                 string
	Lineage                json.RawMessage
	CreatedTimeNS          int64
}

type Correction struct{ input CorrectionInput }

func NewCorrection(input CorrectionInput) (Correction, error) {
	input.Lineage = slices.Clone(input.Lineage)
	correction := Correction{input: input}
	if err := correction.Validate(); err != nil {
		return Correction{}, err
	}
	return correction, nil
}

func (c Correction) Validate() error {
	for _, field := range []struct {
		name, value string
		required    bool
	}{
		{"correction_id", c.input.CorrectionID, true}, {"original_raw_segment_id", c.input.OriginalRawSegmentID, true},
		{"original_gap_id", c.input.OriginalGapID, false}, {"replacement_dataset_id", c.input.ReplacementDatasetID, true},
		{"mapper_release_id", c.input.MapperReleaseID, true}, {"supersedes_correction_id", c.input.SupersedesCorrectionID, false},
		{"reason", c.input.Reason, true},
	} {
		if err := validateQualityString(field.name, field.value, field.required); err != nil {
			return errors.Join(ErrInvalidCorrection, err)
		}
	}
	if c.input.CorrectionID == c.input.SupersedesCorrectionID || c.input.CreatedTimeNS < 0 {
		return fmt.Errorf("%w: invalid correction lineage", ErrInvalidCorrection)
	}
	canonical, err := canonicalJSONObject(c.input.Lineage)
	if err != nil || !bytes.Equal(canonical, c.input.Lineage) {
		return fmt.Errorf("%w: lineage must be a canonical JSON object", ErrInvalidCorrection)
	}
	return nil
}

func (c Correction) Input() CorrectionInput {
	value := c.input
	value.Lineage = slices.Clone(value.Lineage)
	return value
}

type HealthState string

const (
	HealthHealthy     HealthState = "healthy"
	HealthDegraded    HealthState = "degraded"
	HealthUnavailable HealthState = "unavailable"
	HealthStale       HealthState = "stale"
	HealthRecovering  HealthState = "recovering"
	HealthQuarantined HealthState = "quarantined"
)

type HealthTransition struct {
	TransitionID   string
	SourceID       string
	ChannelID      string
	Dimension      string
	FromState      HealthState
	ToState        HealthState
	ObservedTimeNS int64
	Evidence       json.RawMessage
}

func (h HealthTransition) Validate() error {
	for _, field := range []struct {
		name, value string
		required    bool
	}{
		{"transition_id", h.TransitionID, true}, {"source_id", h.SourceID, true}, {"channel_id", h.ChannelID, false},
		{"dimension", h.Dimension, true},
	} {
		if err := validateQualityString(field.name, field.value, field.required); err != nil {
			return errors.Join(ErrInvalidHealth, err)
		}
	}
	if !validHealthState(h.FromState) || !validHealthState(h.ToState) || h.FromState == h.ToState || h.ObservedTimeNS < 0 {
		return fmt.Errorf("%w: invalid state edge or observation time", ErrInvalidHealth)
	}
	canonical, err := canonicalJSONObject(h.Evidence)
	if err != nil || !bytes.Equal(canonical, h.Evidence) {
		return fmt.Errorf("%w: evidence must be a canonical JSON object", ErrInvalidHealth)
	}
	return nil
}

func validHealthState(state HealthState) bool {
	switch state {
	case HealthHealthy, HealthDegraded, HealthUnavailable, HealthStale, HealthRecovering, HealthQuarantined:
		return true
	default:
		return false
	}
}

func canonicalJSONArray(value json.RawMessage) (json.RawMessage, error) {
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
	if _, ok := decoded.([]any); !ok {
		return nil, errors.New("quality: JSON value is not an array")
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}
