package objectstore

import (
	"slices"
	"testing"
)

func TestCompatibilityMatrixCoversX3AndIgnoresETag(t *testing.T) {
	client := newFakeClient()
	report, err := RunCompatibility(t.Context(), CompatibilityProvider{
		Name:   "deterministic-s3-fake",
		Client: client,
		Faults: client,
	}, CompatibilityConfig{
		Prefix: "compatibility/x3-qualified",
		Sizes:  []int64{0, 1, 63, 64, 65, 257},
		Verify: VerifyPolicy{FullReadLimit: 64, SampleBytes: 8, SampleCount: 3},
	})
	if err != nil {
		t.Fatalf("RunCompatibility() error = %v", err)
	}
	if !report.Qualified {
		t.Fatalf("provider disqualified: %s", report.Disqualification)
	}
	if !report.SinglePUTDefault || report.MultipartEnabled {
		t.Fatalf("single PUT decision = %#v", report)
	}
	caseNames := make([]string, 0, len(report.Cases))
	for _, matrixCase := range report.Cases {
		if !matrixCase.Passed {
			t.Fatalf("compatibility case failed: %#v", matrixCase)
		}
		caseNames = append(caseNames, matrixCase.Name)
	}
	for _, required := range []string{
		"size_0",
		"conditional_race",
		"range_read",
		"multipart_abort",
		"clock_skew",
		"dropped_response",
		"multipart_decision",
	} {
		if !slices.Contains(caseNames, required) {
			t.Fatalf("compatibility cases = %q, missing %q", caseNames, required)
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.rangeReads == 0 || client.aborts == 0 {
		t.Fatalf("matrix range reads=%d aborts=%d", client.rangeReads, client.aborts)
	}
	for key, object := range client.objects {
		if object.etag == "" {
			t.Fatalf("fake object %q did not carry deliberately opaque ETag", key)
		}
	}
}

func TestCompatibilityDisqualifiesUnsupportedConditionalCreate(t *testing.T) {
	client := newFakeClient()
	client.conditionalUnsupported = true
	report, err := RunCompatibility(t.Context(), CompatibilityProvider{
		Name:   "missing-conditional-create",
		Client: client,
		Faults: client,
	}, CompatibilityConfig{
		Prefix: "compatibility/x3-disqualified",
		Sizes:  []int64{0},
		Verify: VerifyPolicy{FullReadLimit: 8, SampleBytes: 2, SampleCount: 2},
	})
	if err != nil {
		t.Fatalf("RunCompatibility() error = %v", err)
	}
	if report.Qualified || report.Disqualification == "" {
		t.Fatalf("unsupported provider report = %#v", report)
	}
}

func TestCompatibilityAllowsExplicitImmutableManifestReconciler(t *testing.T) {
	client := newFakeClient()
	client.conditionalUnsupported = true
	report, err := RunCompatibility(t.Context(), CompatibilityProvider{
		Name:       "manifest-reconciled-provider",
		Client:     client,
		Reconciler: client,
		Faults:     client,
	}, CompatibilityConfig{
		Prefix: "compatibility/x3-reconciled",
		Sizes:  []int64{0, 1, 65},
		Verify: VerifyPolicy{FullReadLimit: 64, SampleBytes: 8, SampleCount: 3},
	})
	if err != nil {
		t.Fatalf("RunCompatibility() error = %v", err)
	}
	if !report.Qualified {
		t.Fatalf("explicit reconciler provider disqualified: %s", report.Disqualification)
	}
}

func TestCompatibilityRejectsMultipartDecisionWithoutEvidence(t *testing.T) {
	client := newFakeClient()
	report, err := RunCompatibility(t.Context(), CompatibilityProvider{
		Name:   "unmeasured-multipart",
		Client: client,
		Faults: client,
		Multipart: MultipartDecision{
			Enabled:        true,
			ThresholdBytes: 8 << 20,
		},
	}, CompatibilityConfig{Prefix: "compatibility/x3-multipart-decision"})
	if err != nil {
		t.Fatalf("RunCompatibility() error = %v", err)
	}
	if report.Qualified {
		t.Fatalf("unmeasured multipart provider unexpectedly qualified: %#v", report)
	}
}

func TestCompatibilityDefaultBoundsIncludeEmptyThrough64MiB(t *testing.T) {
	if len(DefaultCompatibilitySizes) == 0 || DefaultCompatibilitySizes[0] != 0 {
		t.Fatalf("DefaultCompatibilitySizes starts with %v, want empty object", DefaultCompatibilitySizes)
	}
	if !slices.Contains(DefaultCompatibilitySizes, int64(64<<20)) {
		t.Fatalf("DefaultCompatibilitySizes = %v, missing 64 MiB", DefaultCompatibilitySizes)
	}
	var total int64
	for _, size := range DefaultCompatibilitySizes {
		total += size
	}
	if total > MaximumCompatibilityTransfer {
		t.Fatalf("default compatibility transfer = %d, exceeds %d", total, MaximumCompatibilityTransfer)
	}
}

func TestCompatibilityDisqualifiesClockResetFailure(t *testing.T) {
	client := newFakeClient()
	client.failClockReset = true
	report, err := RunCompatibility(t.Context(), CompatibilityProvider{
		Name:   "clock-reset-failure",
		Client: client,
		Faults: client,
	}, CompatibilityConfig{
		Prefix: "compatibility/x3-clock-reset",
		Sizes:  []int64{0},
		Verify: VerifyPolicy{FullReadLimit: 64, SampleBytes: 8, SampleCount: 3},
	})
	if err != nil {
		t.Fatalf("RunCompatibility() error = %v", err)
	}
	if report.Qualified || report.Disqualification == "" {
		t.Fatalf("clock reset failure report = %#v", report)
	}
	client.mu.Lock()
	attempts := client.clockResetAttempts
	hadDeadline := client.clockResetHadDeadline
	client.mu.Unlock()
	if attempts != 1 || !hadDeadline {
		t.Fatalf("clock reset attempts=%d deadline=%v, want one bounded cleanup", attempts, hadDeadline)
	}
}
