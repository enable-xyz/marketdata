package catalog

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var _ QueryDatabase = (*pgxpool.Pool)(nil)

func TestRawSegmentFilterRejectsUnboundedAndUnstableInputs(t *testing.T) {
	valid := RawSegmentFilter{
		DatasetID:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SourceIDs:           []string{"00000000-0000-0000-0000-000000000001"},
		StartReceivedTimeNS: 10, EndReceivedTimeNS: 20, Limit: 1, MaxManifestBytes: 1,
	}
	tests := []struct {
		name   string
		mutate func(*RawSegmentFilter)
	}{
		{"missing source", func(value *RawSegmentFilter) { value.SourceIDs = nil }},
		{"unstable source order", func(value *RawSegmentFilter) { value.SourceIDs = []string{"b", "a"} }},
		{"duplicate channel", func(value *RawSegmentFilter) { value.ChannelIDs = []string{"trades", "trades"} }},
		{"closed interval", func(value *RawSegmentFilter) { value.EndReceivedTimeNS = value.StartReceivedTimeNS }},
		{"zero limit", func(value *RawSegmentFilter) { value.Limit = 0 }},
		{"excess limit", func(value *RawSegmentFilter) { value.Limit = MaximumRawSegmentResults + 1 }},
		{"zero manifest byte limit", func(value *RawSegmentFilter) { value.MaxManifestBytes = 0 }},
		{"negative manifest byte limit", func(value *RawSegmentFilter) { value.MaxManifestBytes = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			if err := validateRawSegmentFilter(value); !errors.Is(err, ErrInvalidQueryProjection) {
				t.Fatalf("validateRawSegmentFilter() error = %v, want ErrInvalidQueryProjection", err)
			}
		})
	}
	if err := validateRawSegmentFilter(valid); err != nil {
		t.Fatalf("valid filter error = %v", err)
	}
}

func TestCheckpointNormalizationBindsExactState(t *testing.T) {
	state := []byte(`{"cursor":1}`)
	value := RuntimeCheckpoint{
		Key: "normalize-v1", SourceID: "00000000-0000-0000-0000-000000000001", ChannelID: "trades",
		ReceivedTimeNS: 100, StreamEpochID: "00000000-0000-0000-0000-000000000002",
		ArrivalOrdinal: 1, MessageOrdinal: 2, StateSHA256: sha256.Sum256(state), StateBytes: state,
		UpdatedAt: time.Unix(1, 1234).UTC(),
	}
	normalized, err := normalizeCheckpoint(value)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.UpdatedAt.Nanosecond()%1_000 != 0 {
		t.Fatalf("checkpoint timestamp was not normalized to PostgreSQL precision: %s", normalized.UpdatedAt)
	}
	value.StateBytes[0] = 'x'
	if string(normalized.StateBytes) != `{"cursor":1}` {
		t.Fatal("normalized checkpoint retained caller byte alias")
	}
	value.StateBytes = []byte(`{"cursor":2}`)
	if _, err := normalizeCheckpoint(value); !errors.Is(err, ErrInvalidQueryProjection) {
		t.Fatalf("changed state with stale hash error = %v", err)
	}
}

func TestCommittedRESTEvidenceDerivesEnvelopeClaims(t *testing.T) {
	request, response := queryRESTEvidenceEnvelopes(t)
	manifest := []byte(`{"manifest_version":1}`)
	publication := RawSegmentPublication{
		SegmentID: "00000000-0000-0000-0000-000000007101", SourceID: querySourceID,
		ChannelID: "trades", EpochID: "00000000-0000-0000-0000-000000007201",
		ReceivedStartNS: 100, ReceivedEndNS: 199, OrdinalStart: 1, OrdinalEnd: 2,
		ObjectKey: "raw/synthetic/1.emseg.zst", ContentSHA256: sha256.Sum256([]byte("segment")),
		ByteLength: 100, ManifestVersion: 1, ManifestSHA256: sha256.Sum256(manifest),
		ManifestBytes: manifest, State: RawSegmentCommitted,
	}
	evidence, err := deriveCommittedRESTEvidence(publication, request, response)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.requestID != "exchange-info-1" || evidence.arrivalOrdinal != 2 ||
		evidence.messageOrdinal != 0 || evidence.envelopeVersion != 1 ||
		evidence.payloadSHA256 != sha256.Sum256(response.RawPayload) ||
		evidence.payloadBytes != len(response.RawPayload) {
		t.Fatalf("derived REST evidence = %+v", evidence)
	}
	beforeRequest := response
	beforeRequest.ArrivalOrdinal = request.ArrivalOrdinal
	if _, err := deriveCommittedRESTEvidence(publication, request, beforeRequest); !errors.Is(err, ErrInvalidQueryProjection) {
		t.Fatalf("response-before-request error = %v", err)
	}
	tamperedPayload := response
	tamperedPayload.RawPayload[0] ^= 1
	if _, err := deriveCommittedRESTEvidence(publication, request, tamperedPayload); !errors.Is(err, ErrInvalidQueryProjection) {
		t.Fatalf("payload with caller-authored stale hash error = %v", err)
	}
}
