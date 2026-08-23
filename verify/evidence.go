// Package verify composes executable venue evidence from the authoritative
// capture, segment, publication, replay, normalization, book, and dataset APIs.
package verify

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	EvidenceSchemaVersion = 1
	EvidenceFileName      = "binance-spot-evidence-v1.json"
	GapLifecycleDeferred  = "deferred_elmd_020"
	VerifierVersion       = "elmd-014.v1"
)

type BuildInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

type Evidence struct {
	SchemaVersion       uint16                  `json:"schema_version"`
	Status              string                  `json:"status"`
	Venue               string                  `json:"venue"`
	Mode                string                  `json:"mode"`
	GapLifecycleStatus  string                  `json:"gap_lifecycle_status"`
	VerifierBuild       BuildInfo               `json:"verifier_build"`
	Components          []ComponentEvidence     `json:"components"`
	Counts              EvidenceCounts          `json:"counts"`
	Hashes              EvidenceHashes          `json:"hashes"`
	Segments            []SegmentEvidence       `json:"segments"`
	Discontinuities     []DiscontinuityEvidence `json:"discontinuities"`
	Datasets            []DatasetEvidence       `json:"datasets"`
	OpportunityOutcomes []OutcomeCount          `json:"opportunity_outcomes"`
	BodySHA256          string                  `json:"body_sha256"`
}

type ComponentEvidence struct {
	Name           string `json:"name"`
	Version        string `json:"version"`
	ConfigSHA256   string `json:"config_sha256"`
	InputSHA256    string `json:"input_sha256"`
	LogicalSHA256  string `json:"logical_sha256"`
	PhysicalSHA256 string `json:"physical_sha256"`
}

type EvidenceCounts struct {
	Symbols               int    `json:"symbols"`
	RawRecords            uint64 `json:"raw_records"`
	WebSocketRecords      uint64 `json:"websocket_records"`
	RESTRecords           uint64 `json:"rest_records"`
	ControlRecords        uint64 `json:"control_records"`
	Acknowledgements      uint64 `json:"acknowledgements"`
	Heartbeats            uint64 `json:"heartbeats"`
	Disconnects           uint64 `json:"disconnects"`
	Opportunities         uint64 `json:"opportunities"`
	CommittedSegments     int    `json:"committed_segments"`
	ReplayRecords         uint64 `json:"replay_records"`
	ReplayDiscontinuities uint64 `json:"replay_discontinuities"`
	NormalizedRows        int    `json:"normalized_rows"`
	Trades                int    `json:"trades"`
	BookUpdates           int    `json:"book_updates"`
	Quotes                int    `json:"quotes"`
	Tickers               int    `json:"tickers"`
	BookSnapshots         int    `json:"book_snapshots"`
	ParquetPartitions     int    `json:"parquet_partitions"`
	ParquetRows           uint64 `json:"parquet_rows"`
}

type EvidenceHashes struct {
	ConfigurationSHA256     string `json:"configuration_sha256"`
	FixtureManifestSHA256   string `json:"fixture_manifest_sha256"`
	FixtureInputSHA256      string `json:"fixture_input_sha256"`
	CatalogSnapshotSHA256   string `json:"catalog_snapshot_sha256"`
	InputManifestSetSHA256  string `json:"input_manifest_set_sha256"`
	RawRecordSHA256         string `json:"raw_record_sha256"`
	NativeReplaySHA256      string `json:"native_replay_sha256"`
	NormalizedLogicalSHA256 string `json:"normalized_logical_sha256"`
	BookLogicalSHA256       string `json:"book_logical_sha256"`
	DatasetLogicalSHA256    string `json:"dataset_logical_sha256"`
	DatasetPhysicalSHA256   string `json:"dataset_physical_sha256"`
}

type SegmentEvidence struct {
	SegmentID      string `json:"segment_id"`
	ChannelID      string `json:"channel_id"`
	EpochID        string `json:"epoch_id"`
	ObjectKey      string `json:"object_key"`
	ContentSHA256  string `json:"content_sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
	FirstOrdinal   uint64 `json:"first_ordinal"`
	LastOrdinal    uint64 `json:"last_ordinal"`
	RecordCount    uint64 `json:"record_count"`
}

type DiscontinuityEvidence struct {
	Kind                  string `json:"kind"`
	Reason                string `json:"reason"`
	SegmentID             string `json:"segment_id"`
	PreviousStreamEpochID string `json:"previous_stream_epoch_id"`
	FirstOrdinal          uint64 `json:"first_ordinal"`
	LastOrdinal           uint64 `json:"last_ordinal"`
	FrameOrdinal          uint32 `json:"frame_ordinal"`
	CompressedOffset      uint64 `json:"compressed_offset"`
}

type DatasetEvidence struct {
	Family         string `json:"family"`
	ManifestFile   string `json:"manifest_file"`
	ManifestSHA256 string `json:"manifest_sha256"`
	LogicalSHA256  string `json:"logical_sha256"`
	PhysicalSHA256 string `json:"physical_sha256"`
	InputRows      uint64 `json:"input_rows"`
	ParquetRows    uint64 `json:"parquet_rows"`
}

type OutcomeCount struct {
	Outcome string `json:"outcome"`
	Count   uint64 `json:"count"`
}

func marshalEvidence(packet Evidence) ([]byte, error) {
	packet.BodySHA256 = ""
	body, err := json.Marshal(packet)
	if err != nil {
		return nil, fmt.Errorf("verify: encode evidence body: %w", err)
	}
	packet.BodySHA256 = hashHex(body)
	encoded, err := json.Marshal(packet)
	if err != nil {
		return nil, fmt.Errorf("verify: encode evidence packet: %w", err)
	}
	return append(encoded, '\n'), nil
}

func unmarshalEvidence(encoded []byte) (Evidence, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var packet Evidence
	if err := decoder.Decode(&packet); err != nil {
		return Evidence{}, fmt.Errorf("verify: decode evidence packet: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Evidence{}, errors.New("verify: evidence packet has trailing JSON")
	}
	claimed := packet.BodySHA256
	packet.BodySHA256 = ""
	body, err := json.Marshal(packet)
	if err != nil || claimed == "" || claimed != hashHex(body) {
		return Evidence{}, errors.New("verify: evidence body hash mismatch")
	}
	packet.BodySHA256 = claimed
	return packet, nil
}

func hashHex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func hashesHex(values ...[]byte) string {
	hasher := sha256.New()
	for _, value := range values {
		var length [8]byte
		for i := range 8 {
			length[7-i] = byte(uint64(len(value)) >> (i * 8))
		}
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write(value)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
