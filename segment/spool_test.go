package segment

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var errCrash = errors.New("synthetic crash")

func TestCrashMatrix(t *testing.T) {
	points := []FaultPoint{
		FaultTemporaryCreated,
		FaultBeforeFrameWrite,
		FaultAfterFramePrefix,
		FaultAfterFrameWrite,
		FaultBeforeFrameClose,
		FaultAfterFrameClose,
		FaultBeforeSegmentSync,
		FaultAfterSegmentSync,
		FaultBeforeSegmentClose,
		FaultAfterSegmentClose,
		FaultBeforeClosedRename,
		FaultAfterClosedRename,
		FaultBeforeSegmentExpose,
		FaultAfterSegmentExpose,
		FaultBeforeManifestWrite,
		FaultAfterManifestWrite,
		FaultBeforeManifestSync,
		FaultAfterManifestSync,
		FaultBeforeManifestClose,
		FaultAfterManifestClose,
		FaultBeforeManifestExpose,
		FaultAfterManifestExpose,
		FaultBeforeClosedDirectorySync,
		FaultAfterClosedDirectorySync,
		FaultBeforeSegmentDirectorySync,
		FaultAfterSegmentDirectorySync,
		FaultBeforeManifestDirectorySync,
		FaultAfterManifestDirectorySync,
		FaultBeforeReadyVerify,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			spool := newTestSpool(t)
			fired := false
			writer := newTestWriter(t, spool, WriterOptions{Fault: func(got FaultPoint) error {
				if !fired && got == point {
					fired = true
					return errCrash
				}
				return nil
			}})
			_, writeErr := writer.Write(testEnvelope(1, 64))
			if writeErr == nil {
				_, writeErr = writer.Shutdown()
			}
			if !fired || writeErr == nil || !errors.Is(writeErr, errCrash) {
				t.Fatalf("fault %s: fired=%v error=%v", point, fired, writeErr)
			}
			ready, readyErr := spool.Ready()
			if readyErr != nil {
				t.Fatalf("Ready exposed an unverifiable partial pair: %v", readyErr)
			}
			if len(ready) > 1 {
				t.Fatalf("Ready returned %d pairs after one crashed segment", len(ready))
			}
			report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			for _, item := range report.Items {
				if item.Ready != nil {
					if _, err := spool.VerifyReady(filepath.Base(item.Ready.ManifestPath)); err != nil {
						t.Fatalf("recovery returned an unverified pair: %v", err)
					}
				}
			}
		})
	}
}

func TestCorruption(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, ReadySegment)
	}{
		{"segment bit flip", func(t *testing.T, ready ReadySegment) {
			data := mustRead(t, ready.SegmentPath)
			data[len(data)/2] ^= 0x80
			mustWrite(t, ready.SegmentPath, data)
		}},
		{"segment truncation", func(t *testing.T, ready ReadySegment) {
			data := mustRead(t, ready.SegmentPath)
			mustWrite(t, ready.SegmentPath, data[:len(data)-1])
		}},
		{"manifest bit flip", func(t *testing.T, ready ReadySegment) {
			data := mustRead(t, ready.ManifestPath)
			data[len(data)/2] ^= 1
			mustWrite(t, ready.ManifestPath, data)
		}},
		{"manifest noncanonical", func(t *testing.T, ready ReadySegment) {
			data := mustRead(t, ready.ManifestPath)
			mustWrite(t, ready.ManifestPath, append([]byte(" "), data...))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spool := newTestSpool(t)
			ready := writeReady(t, spool, testEnvelope(1, 128))
			test.mutate(t, ready)
			if _, err := spool.VerifyReady(filepath.Base(ready.ManifestPath)); err == nil {
				t.Fatal("corruption was accepted")
			}
		})
	}

	t.Run("partial frame retains earlier complete frames only for diagnosis", func(t *testing.T) {
		spool := newTestSpool(t)
		frameWrites := 0
		writer := newTestWriter(t, spool, WriterOptions{Fault: func(point FaultPoint) error {
			if point == FaultAfterFramePrefix {
				frameWrites++
				if frameWrites == 2 {
					return errCrash
				}
			}
			return nil
		}})
		if _, err := writer.Write(testEnvelope(1, 700<<10)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(testEnvelope(2, 700<<10)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(testEnvelope(3, 700<<10)); !errors.Is(err, errCrash) {
			t.Fatalf("third write error = %v", err)
		}
		report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, item := range report.Items {
			if item.State == RecoveryOpen || item.State == RecoveryTruncated || item.State == RecoveryCorrupt {
				found = true
				if item.CompleteFrames != 1 || item.CompleteBytes == 0 || item.Ready != nil {
					t.Fatalf("partial diagnosis = %+v", item)
				}
			}
		}
		if !found {
			t.Fatal("recovery did not classify the partial frame")
		}
	})
}

func TestRecovery(t *testing.T) {
	t.Run("missing root fails closed", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "missing")
		_, err := OpenSpool(testSpoolConfig(root))
		if !errors.Is(err, ErrSpoolRoot) {
			t.Fatalf("OpenSpool error = %v", err)
		}
		if _, statErr := os.Stat(root); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("missing root was created: %v", statErr)
		}
	})

	t.Run("path parts cannot traverse", func(t *testing.T) {
		root := t.TempDir()
		for _, value := range []string{"../escape", "/absolute", "a\\b", "..", " leading"} {
			config := testSpoolConfig(root)
			config.SourceID = value
			if _, err := OpenSpool(config); err == nil {
				t.Fatalf("source path part %q was accepted", value)
			}
		}
	})

	t.Run("closed exact bytes reconstruct once", func(t *testing.T) {
		spool := newTestSpool(t)
		writer := newTestWriter(t, spool, WriterOptions{Fault: failOnceAt(FaultAfterClosedRename)})
		if _, err := writer.Write(testEnvelope(1, 64)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Shutdown(); !errors.Is(err, errCrash) {
			t.Fatalf("Shutdown error = %v", err)
		}
		first, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err != nil {
			t.Fatal(err)
		}
		if countReadyItems(first) != 1 {
			t.Fatalf("first recovery = %+v", first)
		}
		second, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err != nil {
			t.Fatal(err)
		}
		if countReadyItems(second) != 1 {
			t.Fatalf("duplicate recovery = %+v", second)
		}
	})

	t.Run("segment only never becomes ready", func(t *testing.T) {
		spool := newTestSpool(t)
		writer := newTestWriter(t, spool, WriterOptions{Fault: failOnceAt(FaultAfterSegmentExpose)})
		if _, err := writer.Write(testEnvelope(1, 64)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Shutdown(); !errors.Is(err, errCrash) {
			t.Fatalf("Shutdown error = %v", err)
		}
		ready, err := spool.Ready()
		if err != nil || len(ready) != 0 {
			t.Fatalf("partial segment became visible: %d, %v", len(ready), err)
		}
		report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err != nil {
			t.Fatal(err)
		}
		if !hasState(report, RecoverySegmentOnly) {
			t.Fatalf("recovery = %+v", report)
		}
	})

	t.Run("manifest only is quarantined and invisible", func(t *testing.T) {
		spool := newTestSpool(t)
		ready := writeReady(t, spool, testEnvelope(1, 64))
		if err := os.Remove(ready.SegmentPath); err != nil {
			t.Fatal(err)
		}
		if _, err := spool.Ready(); err == nil {
			t.Fatal("manifest without its exact segment was accepted")
		}
		report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err != nil {
			t.Fatal(err)
		}
		if !hasState(report, RecoveryManifestOnly) {
			t.Fatalf("recovery = %+v", report)
		}
	})

	t.Run("corrupt manifest cannot quarantine valid referenced segment", func(t *testing.T) {
		spool := newTestSpool(t)
		ready := writeReady(t, spool, testEnvelope(1, 64))
		corruptName := "manifest=" + strings.Repeat("0", 64) + "-" + strings.Repeat("0", 64) + ".ready.json"
		corruptPath := filepath.Join(spool.readyDir, corruptName)
		mustWrite(t, corruptPath, mustRead(t, ready.ManifestPath))

		report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := spool.VerifyReady(filepath.Base(ready.ManifestPath)); err != nil {
			t.Fatalf("valid pair was damaged by unchecked manifest reference: %v", err)
		}
		if _, err := os.Stat(ready.SegmentPath); err != nil {
			t.Fatalf("valid referenced segment was quarantined: %v", err)
		}
		foundCorrupt := false
		for _, item := range report.Items {
			if len(item.Paths) > 0 && item.Paths[0] == corruptPath {
				foundCorrupt = true
				if len(item.Paths) != 1 || len(item.Quarantined) != 1 {
					t.Fatalf("corrupt manifest touched its valid segment reference: %+v", item)
				}
			}
		}
		if !foundCorrupt {
			t.Fatalf("corrupt manifest was not classified: %+v", report)
		}
	})

	t.Run("quarantine faults return unresolved state and moved evidence", func(t *testing.T) {
		points := []FaultPoint{
			FaultBeforeQuarantine,
			FaultBeforeQuarantineSourceSync,
			FaultAfterQuarantineSourceSync,
			FaultBeforeQuarantineDirectorySync,
			FaultAfterQuarantineDirectorySync,
			FaultAfterQuarantine,
		}
		for _, point := range points {
			t.Run(string(point), func(t *testing.T) {
				spool := newTestSpool(t)
				source := leaveSegmentOnly(t, spool)
				report, err := spool.Recover(RecoveryOptions{
					FrameBytes:    1 << 20,
					WriterVersion: "test-writer",
					Fault:         failOnceAt(point),
				})
				if !errors.Is(err, errCrash) {
					t.Fatalf("Recover error = %v", err)
				}
				if len(report.Items) == 0 {
					t.Fatal("recovery error omitted unresolved item evidence")
				}
				item := report.Items[len(report.Items)-1]
				if point == FaultBeforeQuarantine {
					if len(item.Quarantined) != 0 {
						t.Fatalf("pre-rename fault reported moved paths: %+v", item)
					}
					if _, statErr := os.Stat(source); statErr != nil {
						t.Fatalf("pre-rename fault moved source: %v", statErr)
					}
				} else {
					if len(item.Quarantined) != 1 {
						t.Fatalf("post-rename fault lost moved-path evidence: %+v", item)
					}
					if _, statErr := os.Stat(item.Quarantined[0]); statErr != nil {
						t.Fatalf("reported quarantine path is absent: %v", statErr)
					}
				}
			})
		}
	})

	t.Run("quarantine rename failure is returned", func(t *testing.T) {
		spool := newTestSpool(t)
		source := leaveSegmentOnly(t, spool)
		name := string(RecoverySegmentOnly) + "-" + filepath.Base(source)
		ext := filepath.Ext(name)
		stem := strings.TrimSuffix(name, ext)
		for i := range 101 {
			candidate := name
			if i > 0 {
				candidate = fmt.Sprintf("%s-%d%s", stem, i, ext)
			}
			mustWrite(t, filepath.Join(spool.quarantineDir, candidate), []byte{byte(i)})
		}
		report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err == nil {
			t.Fatal("quarantine rename conflict was reported as success")
		}
		if len(report.Items) == 0 || len(report.Items[len(report.Items)-1].Quarantined) != 0 {
			t.Fatalf("rename failure evidence = %+v", report)
		}
		if _, statErr := os.Stat(source); statErr != nil {
			t.Fatalf("rename failure lost source: %v", statErr)
		}
	})

	t.Run("quarantine source validation failure is returned", func(t *testing.T) {
		spool := newTestSpool(t)
		unexpected := filepath.Join(spool.readyDir, "unexpected")
		if err := os.Mkdir(unexpected, 0o700); err != nil {
			t.Fatal(err)
		}
		report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err == nil {
			t.Fatal("invalid quarantine source was reported as success")
		}
		if len(report.Items) == 0 || report.Items[len(report.Items)-1].Paths[0] != unexpected {
			t.Fatalf("source-validation failure evidence = %+v", report)
		}
	})

	t.Run("overlapping ready ranges are conflicting", func(t *testing.T) {
		spool := newTestSpool(t)
		writeReady(t, spool, testEnvelope(1, 64))
		different := testEnvelope(1, 65)
		different.RawPayload[0] ^= 0xff
		writeReady(t, spool, different)
		report, err := spool.Recover(RecoveryOptions{FrameBytes: 1 << 20, WriterVersion: "test-writer"})
		if err != nil {
			t.Fatal(err)
		}
		if !hasState(report, RecoveryConflicting) {
			t.Fatalf("recovery = %+v", report)
		}
		ready, err := spool.Ready()
		if err != nil || len(ready) != 0 {
			t.Fatalf("conflicting ranges stayed visible: %d, %v", len(ready), err)
		}
	})

	t.Run("conflict safe returned paths are authoritative", func(t *testing.T) {
		spool := newTestSpool(t)
		first := writeReady(t, spool, testEnvelope(1, 64))
		original := mustRead(t, first.SegmentPath)
		mustWrite(t, first.SegmentPath, bytes.Repeat([]byte{0xff}, len(original)))
		second := writeReady(t, spool, testEnvelope(1, 64))
		if first.SegmentPath == second.SegmentPath || !strings.Contains(filepath.Base(second.SegmentPath), "-1.zst") {
			t.Fatalf("fileflow returned path was not retained: first=%s second=%s", first.SegmentPath, second.SegmentPath)
		}
		if second.Manifest.SegmentFile != filepath.Base(second.SegmentPath) {
			t.Fatalf("manifest ignored fileflow final path: %q != %q", second.Manifest.SegmentFile, filepath.Base(second.SegmentPath))
		}
		if _, err := spool.VerifyReady(filepath.Base(second.ManifestPath)); err != nil {
			t.Fatalf("conflict-safe pair does not verify: %v", err)
		}
	})

	t.Run("homogeneity order and bounds reject before append", func(t *testing.T) {
		spool := newTestSpool(t)
		writer := newTestWriter(t, spool, WriterOptions{})
		if _, err := writer.Write(testEnvelope(2, 64)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(testEnvelope(2, 64)); !errors.Is(err, ErrBounds) {
			t.Fatalf("duplicate ordinal error = %v", err)
		}
		wrong := testEnvelope(3, 64)
		wrong.ChannelOrEndpoint = "other"
		if _, err := writer.Write(wrong); !errors.Is(err, ErrBounds) {
			t.Fatalf("cross-channel error = %v", err)
		}
		oversized := testEnvelope(3, 1<<20)
		if _, err := writer.Write(oversized); !errors.Is(err, ErrBounds) {
			t.Fatalf("frame-bound error = %v", err)
		}
		if writer.BufferedBytes() > writer.BufferedLimit() {
			t.Fatalf("buffer %d exceeded limit %d", writer.BufferedBytes(), writer.BufferedLimit())
		}
	})

	t.Run("reader yields verified exact order", func(t *testing.T) {
		spool := newTestSpool(t)
		ready := writeReady(t, spool, testEnvelope(7, 64), testEnvelope(8, 64))
		var ordinals []uint64
		got, err := spool.ReadReady(filepath.Base(ready.ManifestPath), func(record Envelope) error {
			ordinals = append(ordinals, record.ArrivalOrdinal)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.ManifestSHA256 != ready.ManifestSHA256 || len(ordinals) != 2 || ordinals[0] != 7 || ordinals[1] != 8 {
			t.Fatalf("ReadReady = %+v, ordinals=%v", got, ordinals)
		}
	})

	t.Run("size age epoch and shutdown rotations seal", func(t *testing.T) {
		spool := newTestSpool(t)
		now := time.Unix(100, 0)
		writer := newTestWriter(t, spool, WriterOptions{SegmentBytes: 1 << 20, MaxAge: time.Minute, Now: func() time.Time { return now }})
		if ready, err := writer.Write(testEnvelope(1, 600<<10)); err != nil || ready != nil {
			t.Fatalf("first write = %+v, %v", ready, err)
		}
		if ready, err := writer.Write(testEnvelope(2, 600<<10)); err != nil || ready == nil || ready.Manifest.RotationReason != RotationSize {
			t.Fatalf("size rotation = %+v, %v", ready, err)
		}
		now = now.Add(time.Minute)
		if ready, err := writer.Write(testEnvelope(3, 64)); err != nil || ready == nil || ready.Manifest.RotationReason != RotationAge {
			t.Fatalf("age rotation = %+v, %v", ready, err)
		}
		if ready, err := writer.EndEpoch(); err != nil || ready == nil || ready.Manifest.RotationReason != RotationEpochEnd {
			t.Fatalf("epoch rotation = %+v, %v", ready, err)
		}

		shutdown := newTestWriter(t, spool, WriterOptions{})
		if _, err := shutdown.Write(testEnvelope(4, 64)); err != nil {
			t.Fatal(err)
		}
		if ready, err := shutdown.Shutdown(); err != nil || ready == nil || ready.Manifest.RotationReason != RotationShutdown {
			t.Fatalf("shutdown rotation = %+v, %v", ready, err)
		}
	})
}

func newTestSpool(t *testing.T) *Spool {
	t.Helper()
	spool, err := OpenSpool(testSpoolConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	return spool
}

func testSpoolConfig(root string) SpoolConfig {
	var epoch [16]byte
	copy(epoch[:], "synthetic-epoch!")
	return SpoolConfig{Root: root, SourceID: "synthetic-source", ChannelID: "synthetic-channel-v1", EpochKind: EpochConnection, EpochID: epoch}
}

func newTestWriter(t *testing.T, spool *Spool, overrides WriterOptions) *Writer {
	t.Helper()
	options := WriterOptions{FrameBytes: 1 << 20, SegmentBytes: DefaultSegmentBytes, MaxAge: DefaultSegmentAge, WriterVersion: "test-writer"}
	if overrides.FrameBytes != 0 {
		options.FrameBytes = overrides.FrameBytes
	}
	if overrides.SegmentBytes != 0 {
		options.SegmentBytes = overrides.SegmentBytes
	}
	if overrides.MaxAge != 0 {
		options.MaxAge = overrides.MaxAge
	}
	if overrides.WriterVersion != "" {
		options.WriterVersion = overrides.WriterVersion
	}
	if overrides.Now != nil {
		options.Now = overrides.Now
	}
	options.Fault = overrides.Fault
	writer, err := spool.NewWriter(options)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func testEnvelope(ordinal uint64, payloadBytes int) Envelope {
	config := testSpoolConfig("/unused")
	return Envelope{
		Kind:               RecordKindWebSocket,
		SourceID:           config.SourceID,
		ChannelOrEndpoint:  config.ChannelID,
		ConnectionEpoch:    OptionalEpoch{Value: config.EpochID, Valid: true},
		ArrivalOrdinal:     ordinal,
		ReceivedWallTimeNS: int64(1_700_000_000_000_000_000 + ordinal),
		ClockEpochID:       "clock-1",
		PayloadEncoding:    PayloadEncodingBinary,
		RawPayload:         bytes.Repeat([]byte{byte(ordinal)}, payloadBytes),
		TerminalOutcome:    OutcomeObserved,
		RecorderVersion:    "capture-test",
	}
}

func writeReady(t *testing.T, spool *Spool, records ...Envelope) ReadySegment {
	t.Helper()
	writer := newTestWriter(t, spool, WriterOptions{})
	for _, record := range records {
		if ready, err := writer.Write(record); err != nil || ready != nil {
			t.Fatalf("Write = %+v, %v", ready, err)
		}
	}
	ready, err := writer.Shutdown()
	if err != nil || ready == nil {
		t.Fatalf("Shutdown = %+v, %v", ready, err)
	}
	return *ready
}
func leaveSegmentOnly(t *testing.T, spool *Spool) string {
	t.Helper()
	writer := newTestWriter(t, spool, WriterOptions{Fault: failOnceAt(FaultAfterSegmentExpose)})
	if _, err := writer.Write(testEnvelope(1, 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Shutdown(); !errors.Is(err, errCrash) {
		t.Fatalf("Shutdown error = %v", err)
	}
	entries, err := os.ReadDir(spool.readyDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.Contains(entry.Name(), ".emseg") {
			return filepath.Join(spool.readyDir, entry.Name())
		}
	}
	t.Fatal("segment-only crash left no segment")
	return ""
}

func failOnceAt(want FaultPoint) FaultInjector {
	fired := false
	return func(got FaultPoint) error {
		if !fired && got == want {
			fired = true
			return errCrash
		}
		return nil
	}
}

func countReadyItems(report RecoveryReport) int {
	count := 0
	for _, item := range report.Items {
		if item.Ready != nil {
			count++
		}
	}
	return count
}

func hasState(report RecoveryReport, want RecoveryState) bool {
	for _, item := range report.Items {
		if item.State == want {
			return true
		}
	}
	return false
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
