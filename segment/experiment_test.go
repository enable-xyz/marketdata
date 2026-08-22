package segment

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestFullX1RequiresExplicitAuthorization(t *testing.T) {
	validShape := X1Config{
		CorpusBytes:   X1CorpusBytes,
		FrameSizes:    X1FrameSizes[:],
		Concurrencies: X1Concurrencies[:],
		Seed:          0x454c4d440003,
	}
	var report bytes.Buffer
	if _, err := RunFullX1(t.Context(), validShape, &report); err == nil {
		t.Fatal("full X1 ran without the explicit consent token")
	}
	if report.Len() != 0 {
		t.Fatal("rejected full X1 wrote a report")
	}
	validShape.Consent = FullX1Consent
	validShape.CorpusBytes--
	if _, err := RunFullX1(t.Context(), validShape, &report); err == nil {
		t.Fatal("full X1 accepted a corpus other than fixed 10 GiB")
	}
	if report.Len() != 0 {
		t.Fatal("rejected corpus wrote a report")
	}
}

func TestCommittedX1Evidence(t *testing.T) {
	data, err := os.ReadFile("testdata/x1_full_report.json")
	if os.IsNotExist(err) {
		t.Skip("regenerate and commit X1 evidence with the explicit full experiment after framing changes")
	}
	if err != nil {
		t.Fatal(err)
	}
	var report X1Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if !report.ObservedMeasurements || report.CorpusBytes != X1CorpusBytes {
		t.Fatalf("committed X1 report is not observed fixed-corpus evidence: %+v", report)
	}
	if len(report.Runs) != len(X1FrameSizes)*len(X1Concurrencies) {
		t.Fatalf("committed X1 run count = %d", len(report.Runs))
	}
	if err := validateX1Determinism(report.Runs); err != nil {
		t.Fatal(err)
	}
	for _, run := range report.Runs {
		if run.PayloadBytes != X1CorpusBytes || run.TruncationsDetected != run.TruncationsInjected ||
			run.CorruptionsInjected != 100 || run.CorruptionsDetected != run.CorruptionsInjected ||
			run.PrefixRecoveriesDetected != run.PrefixRecoveriesInjected {
			t.Fatalf("incomplete committed X1 run: %+v", run)
		}
	}
}

func TestFullX1(t *testing.T) {
	if os.Getenv("MARKETDATA_X1_FULL") != FullX1Consent {
		t.Skip("set MARKETDATA_X1_FULL to the explicit consent token to run the fixed 10 GiB X1 experiment")
	}
	path := os.Getenv("MARKETDATA_X1_REPORT")
	if path == "" {
		t.Fatal("MARKETDATA_X1_REPORT must explicitly name the report destination")
	}
	reportFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reportFile.Close()
	config := X1Config{
		Consent:       FullX1Consent,
		CorpusBytes:   X1CorpusBytes,
		FrameSizes:    X1FrameSizes[:],
		Concurrencies: X1Concurrencies[:],
		Seed:          0x454c4d440003,
	}
	report, err := RunFullX1(t.Context(), config, reportFile)
	if err != nil {
		t.Fatal(err)
	}
	if !report.ObservedMeasurements || len(report.Runs) != len(X1FrameSizes)*len(X1Concurrencies) {
		t.Fatalf("incomplete observed X1 report: %+v", report)
	}
	for _, run := range report.Runs {
		if run.PayloadBytes != X1CorpusBytes || run.SegmentCount == 0 || run.TruncationsDetected != run.TruncationsInjected ||
			run.CorruptionsInjected != 100 || run.CorruptionsDetected != run.CorruptionsInjected ||
			run.PrefixRecoveriesDetected != run.PrefixRecoveriesInjected {
			t.Fatalf("incomplete X1 run: %+v", run)
		}
	}
}
