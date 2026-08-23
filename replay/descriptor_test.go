package replay

import (
	"errors"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/segment"
)

func TestInputDescriptorAcceptsExactAdapterSourceIDs(t *testing.T) {
	epochID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	sourceIDs := []string{
		"bybit-v5-option-public",
		"okx-v5-public",
		"deribit-json-rpc-v2",
		"hyperliquid-mainnet-main_perpetual-main",
		testSourceA,
	}

	for index, sourceID := range sourceIDs {
		t.Run(sourceID, func(t *testing.T) {
			publication := testPublication(t, 700+index, sourceID, epochID, []segment.Envelope{
				testRecord(t, sourceID, epochID, 1, 0, 1, []byte("source-identity")),
			})
			descriptor, err := NewInputDescriptor(publication)
			if err != nil {
				t.Fatalf("NewInputDescriptor() error = %v", err)
			}
			if descriptor.SourceID() != sourceID {
				t.Fatalf("SourceID() = %q, want exact %q", descriptor.SourceID(), sourceID)
			}
		})
	}
}

func TestInputDescriptorRejectsInvalidSourceIDs(t *testing.T) {
	epochID := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	invalidSourceIDs := []struct {
		name     string
		sourceID string
	}{
		{name: "blank", sourceID: ""},
		{name: "whitespace only", sourceID: " "},
		{name: "leading whitespace", sourceID: " bybit-v5-option-public"},
		{name: "trailing whitespace", sourceID: "okx-v5-public\t"},
		{name: "NUL", sourceID: "deribit\x00-json-rpc-v2"},
		{name: "malformed UTF-8", sourceID: string([]byte{'h', 0xff, 'l'})},
		{name: "oversized", sourceID: strings.Repeat("a", segment.MaxSourceIDBytes+1)},
	}

	for index, test := range invalidSourceIDs {
		t.Run(test.name, func(t *testing.T) {
			publication := testPublication(t, 710+index, testSourceA, epochID, []segment.Envelope{
				testRecord(t, testSourceA, epochID, 1, 0, 1, []byte("invalid-source")),
			})
			publication.SourceID = test.sourceID
			if _, err := NewInputDescriptor(publication); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("NewInputDescriptor() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestInputDescriptorRejectsManifestSourceAlias(t *testing.T) {
	epochID := "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	publication, _, manifest := testPublicationBytes(t, 720, "bybit-v5-option-public", epochID, []segment.Envelope{
		testRecord(t, "bybit-v5-option-public", epochID, 1, 0, 1, []byte("alias")),
	})
	manifest.SourceID = "bybit-v5-linear-public"
	bindManifest(t, &publication, manifest)

	if _, err := NewInputDescriptor(publication); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("NewInputDescriptor() error = %v, want ErrIntegrity", err)
	}
}

func TestInputDescriptorStillRequiresCanonicalUUIDSegmentAndEpochIDs(t *testing.T) {
	epochID := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

	t.Run("segment ID", func(t *testing.T) {
		publication := testPublication(t, 730, "okx-v5-public", epochID, []segment.Envelope{
			testRecord(t, "okx-v5-public", epochID, 1, 0, 1, []byte("segment-id")),
		})
		publication.SegmentID = "not-a-uuid"
		if _, err := NewInputDescriptor(publication); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("NewInputDescriptor() error = %v, want ErrInvalidInput", err)
		}
	})

	t.Run("epoch ID", func(t *testing.T) {
		publication := testPublication(t, 731, "deribit-json-rpc-v2", epochID, []segment.Envelope{
			testRecord(t, "deribit-json-rpc-v2", epochID, 1, 0, 1, []byte("epoch-id")),
		})
		publication.EpochID = "not-a-uuid"
		if _, err := NewInputDescriptor(publication); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("NewInputDescriptor() error = %v, want ErrInvalidInput", err)
		}
	})
}
