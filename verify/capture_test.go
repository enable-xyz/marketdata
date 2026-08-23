package verify

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

func TestCaptureAggregateBoundsAndCompletion(t *testing.T) {
	budget := &aggregateCaptureBudget{maxMessages: 2, maxBytes: 30}
	if !budget.consume(10) || !budget.consume(20) {
		t.Fatal("aggregate budget rejected authorized payloads")
	}
	if budget.consume(0) || budget.consume(1) {
		t.Fatal("aggregate budget admitted records beyond the configured limits")
	}

	byteBound := &aggregateCaptureBudget{maxMessages: 5, maxBytes: 10}
	if !byteBound.consume(8) || byteBound.consume(3) {
		t.Fatal("aggregate byte budget was not enforced across writes")
	}

	inventory := newSpotEvidenceInventory([]string{"BTCUSDT", "ETHUSDT"}, false)
	inventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"result":null,"id":1}`)})
	inventory.observe(capture.EnvelopeV1{RecordKind: capture.RecordKindControl, ControlKind: capture.OptionalControlKind{Value: capture.ControlHeartbeat, Valid: true}})
	for _, symbol := range []string{"BTCUSDT", "ETHUSDT"} {
		inventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"trade","s":"` + symbol + `"}`)})
		inventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"depthUpdate","s":"` + symbol + `"}`)})
		inventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"u":1,"s":"` + symbol + `"}`)})
		if inventory.complete() {
			t.Fatal("inventory completed before the ticker observation")
		}
		inventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"24hrTicker","s":"` + symbol + `"}`)})
	}
	if !inventory.complete() {
		t.Fatal("inventory did not complete after all required evidence")
	}

	liveInventory := newSpotEvidenceInventory([]string{"BTCUSDT", "ETHUSDT"}, true)
	liveInventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"result":null,"id":1}`)})
	liveInventory.observe(capture.EnvelopeV1{RecordKind: capture.RecordKindControl, ControlKind: capture.OptionalControlKind{Value: capture.ControlHeartbeat, Valid: true}})
	for _, symbol := range []string{"BTCUSDT", "ETHUSDT"} {
		liveInventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"trade","s":"` + symbol + `"}`)})
		liveInventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"depthUpdate","s":"` + symbol + `"}`)})
		liveInventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"u":1,"s":"` + symbol + `"}`)})
		liveInventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"24hrTicker","s":"` + symbol + `"}`)})
	}
	liveInventory.markSnapshotReady()
	liveInventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"depthUpdate","s":"BTCUSDT"}`)})
	if liveInventory.complete() {
		t.Fatal("live inventory completed before every symbol had a post-snapshot depth update")
	}
	liveInventory.observe(capture.EnvelopeV1{PayloadEncoding: capture.PayloadEncodingJSON, RawPayload: []byte(`{"e":"depthUpdate","s":"ETHUSDT"}`)})
	if !liveInventory.complete() {
		t.Fatal("live inventory did not complete after every symbol had a post-snapshot depth update")
	}
}

func TestContainedFixturePathRejectsSymlinkedAncestor(t *testing.T) {
	root := t.TempDir()
	inside := filepath.Join(root, "inside.json")
	if err := os.WriteFile(inside, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := containedFixturePath(root, inside); err != nil || got != inside {
		t.Fatalf("regular contained path = %q, %v", got, err)
	}

	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "outside.json")
	if err := os.WriteFile(outsideFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkedDirectory := filepath.Join(root, "linked")
	if err := os.Symlink(outside, linkedDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := containedFixturePath(root, filepath.Join(linkedDirectory, "outside.json")); err == nil {
		t.Fatal("fixture path accepted a symlinked ancestor escaping fixture_root")
	}
}
