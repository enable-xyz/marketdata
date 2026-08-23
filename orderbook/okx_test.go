package orderbook

import (
	"errors"
	"testing"
)

func TestOKXBookNoChangeSequenceException(t *testing.T) {
	book := newOKXTestBook(t, "books")
	seedOKXAfterCutover(t, book, 100)
	result, err := book.Apply(OKXUpdate{Channel: "books", InstrumentID: "BTC-USDT", Action: "update", SourceTimeNS: OKXChecksumCutoverTimeNS, ReceivedTimeNS: OKXChecksumCutoverTimeNS, PreviousSeqID: 100, SeqID: 100, Checksum: 0})
	if err != nil || result.Kind != OKXAppliedNoChange || result.Sequence != 100 || book.View().State != OKXBookLive {
		t.Fatalf("no-change Apply() = %#v, %v", result, err)
	}
}

func TestOKXBookMaintenanceResetSequenceException(t *testing.T) {
	book := newOKXTestBook(t, "books")
	seedOKXAfterCutover(t, book, 100)
	result, err := book.Apply(OKXUpdate{Channel: "books", InstrumentID: "BTC-USDT", Action: "update", SourceTimeNS: OKXChecksumCutoverTimeNS + 1, ReceivedTimeNS: OKXChecksumCutoverTimeNS + 1, PreviousSeqID: 100, SeqID: 7, Checksum: 0, Bids: []OKXLevel{{Price: "65000", Size: "2", DeprecatedOrders: "0", OrderCount: "2"}}})
	if err != nil || result.Kind != OKXAppliedMaintenance || result.Sequence != 7 || book.View().Bids[0].Size != "2" {
		t.Fatalf("maintenance reset Apply() = %#v, view=%#v, err=%v", result, book.View(), err)
	}
}

func TestOKXBookNonExceptionMismatchResubscribes(t *testing.T) {
	book := newOKXTestBook(t, "books")
	seedOKXAfterCutover(t, book, 100)
	result, err := book.Apply(OKXUpdate{Channel: "books", InstrumentID: "BTC-USDT", Action: "update", SourceTimeNS: OKXChecksumCutoverTimeNS + 1, ReceivedTimeNS: OKXChecksumCutoverTimeNS + 1, PreviousSeqID: 99, SeqID: 101, Checksum: 0})
	if !errors.Is(err, ErrOKXBookGap) || result.Kind != OKXResubscribe || book.View().State != OKXBookNeedsResubscribe || len(book.View().Bids) != 0 {
		t.Fatalf("gap Apply() = %#v, view=%#v, err=%v", result, book.View(), err)
	}
	book.Reconnect()
	if book.View().State != OKXBookUnseeded {
		t.Fatalf("Reconnect() state = %s", book.View().State)
	}
}

func TestOKXBookChecksumZeroCutover(t *testing.T) {
	book := newOKXTestBook(t, "books")
	result, err := book.Apply(OKXUpdate{Channel: "books", InstrumentID: "BTC-USDT", Action: "snapshot", SourceTimeNS: OKXChecksumCutoverTimeNS, ReceivedTimeNS: OKXChecksumCutoverTimeNS, PreviousSeqID: -1, SeqID: 1, Checksum: 0, Bids: []OKXLevel{{Price: "65000", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}, Asks: []OKXLevel{{Price: "65001", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}})
	if err != nil || result.ChecksumStatus != OKXChecksumUnavailable || book.View().ChecksumStatus != OKXChecksumUnavailable {
		t.Fatalf("post-cutover Apply() = %#v, %v", result, err)
	}
	changed := newOKXTestBook(t, "books")
	result, err = changed.Apply(OKXUpdate{Channel: "books", InstrumentID: "BTC-USDT", Action: "snapshot", SourceTimeNS: OKXChecksumCutoverTimeNS, ReceivedTimeNS: OKXChecksumCutoverTimeNS, PreviousSeqID: -1, SeqID: 1, Checksum: 1})
	if !errors.Is(err, ErrOKXBookChecksum) || result.Kind != OKXResubscribe {
		t.Fatalf("post-cutover nonzero checksum = %#v, %v", result, err)
	}
}

func TestOKXBookPreCutoverChecksumValidation(t *testing.T) {
	book := newOKXTestBook(t, "books")
	bids := []OKXLevel{{Price: "65000", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}
	asks := []OKXLevel{{Price: "65001", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}
	checksum := OKXChecksum(bids, asks)
	result, err := book.Apply(OKXUpdate{Channel: "books", InstrumentID: "BTC-USDT", Action: "snapshot", SourceTimeNS: OKXChecksumCutoverTimeNS - 1, ReceivedTimeNS: OKXChecksumCutoverTimeNS + 1, PreviousSeqID: -1, SeqID: 1, Checksum: checksum, Bids: bids, Asks: asks})
	if err != nil || result.ChecksumStatus != OKXChecksumValidated {
		t.Fatalf("pre-cutover snapshot = %#v, %v", result, err)
	}
	result, err = book.Apply(OKXUpdate{Channel: "books", InstrumentID: "BTC-USDT", Action: "update", SourceTimeNS: OKXChecksumCutoverTimeNS - 1, ReceivedTimeNS: OKXChecksumCutoverTimeNS + 1, PreviousSeqID: 1, SeqID: 2, Checksum: checksum + 1})
	if !errors.Is(err, ErrOKXBookChecksum) || result.Kind != OKXResubscribe || book.View().State != OKXBookNeedsResubscribe {
		t.Fatalf("checksum mismatch = %#v, %v", result, err)
	}
}

func TestOKXRegularAndRPIBooksReconstructSeparatelyAndCannotCrossApply(t *testing.T) {
	regular := newOKXTestBook(t, "books")
	rpi := newOKXTestBook(t, "books-rpi-tbt")
	seedOKXAfterCutover(t, regular, 1)
	seedOKXAfterCutover(t, rpi, 1)
	if regular.View().LiquidityKind != OKXRegularLiquidity || rpi.View().LiquidityKind != OKXRPILiquidity {
		t.Fatalf("liquidity kinds: regular=%#v rpi=%#v", regular.View(), rpi.View())
	}
	result, err := rpi.Apply(OKXUpdate{Channel: "books-rpi-tbt", InstrumentID: "BTC-USDT", Action: "update", SourceTimeNS: OKXChecksumCutoverTimeNS + 1, ReceivedTimeNS: OKXChecksumCutoverTimeNS + 1, PreviousSeqID: 1, SeqID: 2, Checksum: 0, Bids: []OKXLevel{{Price: "65000", Size: "2", DeprecatedOrders: "0", OrderCount: "2"}}})
	if err != nil || result.Kind != OKXAppliedUpdate || rpi.View().Sequence != 2 || regular.View().Sequence != 1 {
		t.Fatalf("RPI update = %#v regular=%#v rpi=%#v err=%v", result, regular.View(), rpi.View(), err)
	}
	result, err = regular.Apply(OKXUpdate{Channel: "books-rpi-tbt", InstrumentID: "BTC-USDT", Action: "update", SourceTimeNS: OKXChecksumCutoverTimeNS + 2, ReceivedTimeNS: OKXChecksumCutoverTimeNS + 2, PreviousSeqID: 2, SeqID: 3, Checksum: 0})
	if !errors.Is(err, ErrOKXBookConfiguration) || result.Kind != OKXResubscribe || rpi.View().State != OKXBookLive || rpi.View().Sequence != 2 {
		t.Fatalf("cross apply = %#v regular=%#v rpi=%#v err=%v", result, regular.View(), rpi.View(), err)
	}
}

func newOKXTestBook(t *testing.T, channel string) *OKXBook {
	t.Helper()
	book, err := NewOKXBook(channel, "BTC-USDT", 400)
	if err != nil {
		t.Fatal(err)
	}
	return book
}

func seedOKXAfterCutover(t *testing.T, book *OKXBook, sequence int64) {
	t.Helper()
	result, err := book.Apply(OKXUpdate{Channel: book.channel, InstrumentID: "BTC-USDT", Action: "snapshot", SourceTimeNS: OKXChecksumCutoverTimeNS, ReceivedTimeNS: OKXChecksumCutoverTimeNS, PreviousSeqID: -1, SeqID: sequence, Checksum: 0, Bids: []OKXLevel{{Price: "65000", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}, Asks: []OKXLevel{{Price: "65001", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}})
	if err != nil || result.Kind != OKXAppliedSnapshot {
		t.Fatalf("seed Apply() = %#v, %v", result, err)
	}
}
