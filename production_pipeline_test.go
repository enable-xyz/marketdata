package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/pipeline"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/google/uuid"
)

type recordingProductionIncidentRecorder struct {
	attempts  int
	records   map[string]quality.Incident
	accepted  []quality.Incident
	recordErr error
}

func (r *recordingProductionIncidentRecorder) RecordIncident(_ context.Context, incident quality.Incident) error {
	r.attempts++
	if r.recordErr != nil {
		return r.recordErr
	}
	if r.records == nil {
		r.records = make(map[string]quality.Incident)
	}
	if existing, ok := r.records[incident.ID()]; ok && !sameProductionIncident(existing, incident) {
		return errors.New("conflicting incident replay")
	}
	r.records[incident.ID()] = incident
	r.accepted = append(r.accepted, incident)
	return nil
}

func sameProductionIncident(left, right quality.Incident) bool {
	return left.ID() == right.ID() &&
		left.Annotation() == right.Annotation() &&
		left.ReportSource() == right.ReportSource() &&
		bytes.Equal(left.AffectedTuples(), right.AffectedTuples()) &&
		left.HasRange() == right.HasRange() &&
		left.RangeStartNS() == right.RangeStartNS() &&
		left.RangeEndNS() == right.RangeEndNS() &&
		left.ReportedTimeNS() == right.ReportedTimeNS() &&
		left.CreatedTimeNS() == right.CreatedTimeNS()
}

type productionErrorWriter struct{ err error }

func (w productionErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestHandleProductionExportErrorRecordsIdempotentDegradedHour(t *testing.T) {
	start := int64(2 * productionPartitionWindow)
	end := start + int64(productionPartitionWindow)
	rawErrorText := "secret token and /private/runtime/path"
	exportErr := fmt.Errorf("%s: %w", rawErrorText, pipeline.ErrPipelineBound)
	recorder := &recordingProductionIncidentRecorder{}
	var output bytes.Buffer

	for attempt := range 2 {
		advance, err := handleProductionExportError(t.Context(), recorder, &output, start, end, exportErr)
		if err != nil {
			t.Fatalf("attempt %d: handleProductionExportError() error = %v", attempt+1, err)
		}
		if !advance {
			t.Fatalf("attempt %d: handleProductionExportError() advance = false, want true", attempt+1)
		}
	}
	if recorder.attempts != 2 {
		t.Fatalf("RecordIncident attempts = %d, want 2", recorder.attempts)
	}
	if len(recorder.records) != 1 {
		t.Fatalf("persisted incidents = %d, want 1", len(recorder.records))
	}
	if len(recorder.accepted) != 2 || !sameProductionIncident(recorder.accepted[0], recorder.accepted[1]) {
		t.Fatal("retry did not replay the exact same incident")
	}
	incident := recorder.accepted[0]
	if _, err := uuid.Parse(incident.ID()); err != nil {
		t.Fatalf("incident ID %q is not a UUID: %v", incident.ID(), err)
	}
	if !incident.HasRange() || incident.RangeStartNS() != start || incident.RangeEndNS() != end {
		t.Fatalf("incident range = [%d,%d], want exact half-open hour [%d,%d)", incident.RangeStartNS(), incident.RangeEndNS(), start, end)
	}
	if incident.ReportedTimeNS() != end || incident.CreatedTimeNS() != end {
		t.Fatalf("incident deterministic times = (%d,%d), want (%d,%d)", incident.ReportedTimeNS(), incident.CreatedTimeNS(), end, end)
	}
	var tuples []map[string]string
	if err := json.Unmarshal(incident.AffectedTuples(), &tuples); err != nil {
		t.Fatalf("decoding incident affected tuples: %v", err)
	}
	if len(tuples) != 1 || tuples[0]["source_id"] != binance.SpotSourceID || tuples[0]["channel_id"] != binance.SpotRawChannel {
		t.Fatalf("affected tuples = %s, want exact production source/channel", incident.AffectedTuples())
	}

	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if len(lines) != 2 || !bytes.Equal(lines[0], lines[1]) {
		t.Fatalf("retry output is not deterministic: %q", output.Bytes())
	}
	if strings.Contains(output.String(), rawErrorText) {
		t.Fatal("degraded result exposed raw export error text")
	}
	var result productionPipelineBoundResult
	if err := json.Unmarshal(lines[0], &result); err != nil {
		t.Fatalf("decoding degraded result: %v", err)
	}
	if result.Complete || !result.Degraded || result.Role != "dataset-builder" ||
		result.FailureClass != productionPipelineBoundFailureClass || result.IncidentID != incident.ID() ||
		result.SourceID != binance.SpotSourceID || result.ChannelID != binance.SpotRawChannel ||
		result.WindowStartNS != start || result.WindowEndNS != end {
		t.Fatalf("degraded result = %+v", result)
	}
}

func TestHandleProductionExportErrorFailsClosed(t *testing.T) {
	start := int64(4 * productionPartitionWindow)
	end := start + int64(productionPartitionWindow)
	exportErr := fmt.Errorf("bounded: %w", pipeline.ErrPipelineBound)

	t.Run("incident persistence", func(t *testing.T) {
		recordErr := errors.New("incident store unavailable")
		recorder := &recordingProductionIncidentRecorder{recordErr: recordErr}
		var output bytes.Buffer
		advance, err := handleProductionExportError(t.Context(), recorder, &output, start, end, exportErr)
		if advance {
			t.Fatal("handleProductionExportError() advance = true after incident persistence failure")
		}
		if !errors.Is(err, recordErr) {
			t.Fatalf("handleProductionExportError() error = %v, want persistence error", err)
		}
		if output.Len() != 0 {
			t.Fatalf("output written before incident persistence: %q", output.Bytes())
		}
	})

	t.Run("structured output", func(t *testing.T) {
		writeErr := errors.New("output unavailable")
		recorder := &recordingProductionIncidentRecorder{}
		advance, err := handleProductionExportError(t.Context(), recorder, productionErrorWriter{err: writeErr}, start, end, exportErr)
		if advance {
			t.Fatal("handleProductionExportError() advance = true after output failure")
		}
		if !errors.Is(err, writeErr) {
			t.Fatalf("handleProductionExportError() error = %v, want output error", err)
		}
		if len(recorder.records) != 1 {
			t.Fatalf("persisted incidents = %d, want 1 before output", len(recorder.records))
		}
	})
}

func TestHandleProductionExportErrorLeavesNonBoundErrorTerminal(t *testing.T) {
	start := int64(6 * productionPartitionWindow)
	end := start + int64(productionPartitionWindow)
	exportErr := errors.New("catalog snapshot unavailable")
	recorder := &recordingProductionIncidentRecorder{}
	var output bytes.Buffer

	advance, err := handleProductionExportError(t.Context(), recorder, &output, start, end, exportErr)
	if advance {
		t.Fatal("handleProductionExportError() advance = true for non-bound error")
	}
	if !errors.Is(err, exportErr) {
		t.Fatalf("handleProductionExportError() error = %v, want original export error", err)
	}
	if recorder.attempts != 0 || len(recorder.records) != 0 {
		t.Fatalf("non-bound error recorded an incident: attempts=%d records=%d", recorder.attempts, len(recorder.records))
	}
	if output.Len() != 0 {
		t.Fatalf("non-bound error wrote output: %q", output.Bytes())
	}
}
