package catalog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
)

type boundedRawSegmentRows struct {
	values []RawSegmentPublication
	next   int
	scans  int
	closed bool
}

func (r *boundedRawSegmentRows) Close() {
	r.closed = true
}

func (r *boundedRawSegmentRows) Err() error {
	return nil
}

func (r *boundedRawSegmentRows) Next() bool {
	if r.closed || r.next == len(r.values) {
		return false
	}
	r.next++
	return true
}

func (r *boundedRawSegmentRows) Scan(dest ...any) error {
	if len(dest) != 15 || r.next < 1 || r.next > len(r.values) {
		return fmt.Errorf("unexpected raw segment scan")
	}
	r.scans++
	value := r.values[r.next-1]
	*dest[0].(*string) = value.SegmentID
	*dest[1].(*string) = value.SourceID
	*dest[2].(*string) = value.ChannelID
	*dest[3].(*string) = value.EpochID
	*dest[4].(*int64) = value.ReceivedStartNS
	*dest[5].(*int64) = value.ReceivedEndNS
	*dest[6].(*int64) = int64(value.OrdinalStart)
	*dest[7].(*int64) = int64(value.OrdinalEnd)
	*dest[8].(*string) = value.ObjectKey
	*dest[9].(*[]byte) = bytes.Clone(value.ContentSHA256[:])
	*dest[10].(*int64) = value.ByteLength
	*dest[11].(*int16) = int16(value.ManifestVersion)
	*dest[12].(*[]byte) = bytes.Clone(value.ManifestSHA256[:])
	*dest[13].(*[]byte) = bytes.Clone(value.ManifestBytes)
	*dest[14].(*RawSegmentState) = value.State
	return nil
}

func TestCommittedRawSegmentsStopsAtCumulativeManifestBound(t *testing.T) {
	first := boundedRawSegmentPublication("first", []byte("1234"))
	second := boundedRawSegmentPublication("second", []byte("56789"))
	second.ManifestSHA256 = [sha256.Size]byte{}
	second.State = RawSegmentQuarantined
	third := boundedRawSegmentPublication("third", []byte("unscanned lineage"))
	rows := &boundedRawSegmentRows{values: []RawSegmentPublication{first, second, third}}

	result, err := (&QueryStore{}).scanCommittedRawSegments(rows, RawSegmentFilter{
		Limit: 3, MaxManifestBytes: 8,
	})
	if !errors.Is(err, ErrQueryBound) || result != nil {
		t.Fatalf("scanCommittedRawSegments() = %+v, %v; want nil, ErrQueryBound", result, err)
	}
	if rows.scans != 2 || rows.next != 2 {
		t.Fatalf("scanned %d rows and advanced %d; want stream abandoned after overflowing second row", rows.scans, rows.next)
	}
	if !rows.closed {
		t.Fatal("overflowing raw segment cursor was not closed")
	}
}

func boundedRawSegmentPublication(id string, manifest []byte) RawSegmentPublication {
	return RawSegmentPublication{
		SegmentID: id, SourceID: "source", ChannelID: "trades", EpochID: "epoch",
		ReceivedStartNS: 1, ReceivedEndNS: 2, OrdinalStart: 1, OrdinalEnd: 1,
		ObjectKey: id + ".emseg.zst", ContentSHA256: sha256.Sum256([]byte(id)), ByteLength: 1,
		ManifestVersion: 1, ManifestSHA256: sha256.Sum256(manifest), ManifestBytes: manifest,
		State: RawSegmentCommitted,
	}
}
