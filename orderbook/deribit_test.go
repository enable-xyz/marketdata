package orderbook_test

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/deribit"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/orderbook"
)

func TestDeribitChangeIDGapAndRecovery(t *testing.T) {
	terms := deribitBookTerms(t)
	book, err := orderbook.NewDeribitBook(terms.InstrumentUID)
	if err != nil {
		t.Fatalf("NewDeribitBook: %v", err)
	}
	snapshot := deribitBookUpdate(t, "book-snapshot.json", terms)
	transition, err := book.Apply(snapshot)
	if err != nil || !transition.Applied || transition.ChangeID != 100 || transition.Recovery != orderbook.DeribitRecoveryNone {
		t.Fatalf("seed transition = %#v, %v", transition, err)
	}
	continuous := deribitBookUpdate(t, "book-gap.json", terms)
	continuous.PreviousID.Value = 100
	continuous.ChangeID = 102
	transition, err = book.Apply(continuous)
	if err != nil || !transition.Applied || transition.PriorChangeID != 100 || transition.ChangeID != 102 ||
		transition.Recovery != orderbook.DeribitRecoveryNone {
		t.Fatalf("continuous transition = %#v, %v", transition, err)
	}
	gap := deribitBookUpdate(t, "book-gap.json", terms)
	transition, err = book.Apply(gap)
	if !errors.Is(err, orderbook.ErrDeribitChangeIDGap) || transition.Applied ||
		transition.Recovery != orderbook.DeribitRecoveryResubscribe || transition.PriorChangeID != 102 ||
		transition.Authority != capture.RuleAdapterPolicyInference || transition.SourceGuarantee {
		t.Fatalf("gap transition = %#v, %v", transition, err)
	}
	if _, err := book.Snapshot(); !errors.Is(err, orderbook.ErrDeribitBookInvalid) {
		t.Fatalf("invalidated snapshot error = %v", err)
	}
	recovery := deribitBookUpdate(t, "book-recovery.json", terms)
	transition, err = book.Apply(recovery)
	if err != nil || !transition.Applied || transition.Recovery != orderbook.DeribitRecoverySnapshot ||
		transition.RuleVersion != orderbook.DeribitContinuityRuleVersion || transition.SourceGuarantee {
		t.Fatalf("recovery transition = %#v, %v", transition, err)
	}
	state, err := book.Snapshot()
	if err != nil || state.ChangeID != 104 || len(state.Bids) != 1 || len(state.Asks) != 1 {
		t.Fatalf("recovered state = %#v, %v", state, err)
	}
}

func TestDeribitInferenceBlocksProvisionalAndGroupedReconstruction(t *testing.T) {
	terms := deribitBookTerms(t)
	book, err := orderbook.NewDeribitBook(terms.InstrumentUID)
	if err != nil {
		t.Fatalf("NewDeribitBook: %v", err)
	}
	grouped := deribitBookUpdate(t, "book-snapshot.json", terms)
	grouped.GroupedView = true
	if _, err := book.Apply(grouped); !errors.Is(err, orderbook.ErrDeribitGroupedBook) {
		t.Fatalf("grouped error = %v", err)
	}
	provisionalTerms := terms
	provisionalTerms.CatalogGeneration = 0
	provisionalTerms.MetadataRawSHA256 = normalize.Hash{}
	provisional := deribitBookUpdate(t, "book-snapshot.json", provisionalTerms)
	if provisional.UnitInference.State != normalize.DeribitInferenceProvisional {
		t.Fatalf("inference state = %s", provisional.UnitInference.State)
	}
	if _, err := book.Apply(provisional); !errors.Is(err, orderbook.ErrDeribitProvisionalUnit) {
		t.Fatalf("provisional error = %v", err)
	}
}

func deribitBookTerms(t *testing.T) normalize.DeribitInstrumentTerms {
	t.Helper()
	raw := deribitBookFixture(t, "instrument-inverse.json")
	instrument, err := deribit.ParseInstrument(raw)
	if err != nil {
		t.Fatalf("ParseInstrument: %v", err)
	}
	terms, err := instrument.Terms(deribit.InstrumentEvidence{
		InstrumentUID: "deribit:BTC-PERPETUAL:1", CatalogGeneration: 1,
		MetadataRawSHA256: normalize.Hash(sha256.Sum256(raw)), ValidFromNS: deribit.DocumentationAccessedAtNS,
		FixtureClassification: normalize.DeribitFixtureClassificationSynthetic,
		OfficialURL:           normalize.DeribitInstrumentProvenanceURL,
		Section:               normalize.DeribitInstrumentProvenanceSection,
		DerivedFrom:           normalize.DeribitInstrumentDerivedFrom,
	})
	if err != nil {
		t.Fatalf("Instrument.Terms: %v", err)
	}
	return terms
}

func deribitBookUpdate(t *testing.T, name string, terms normalize.DeribitInstrumentTerms) normalize.DeribitBookUpdate {
	t.Helper()
	message, err := deribit.ParseBook(deribitBookFixture(t, name))
	if err != nil {
		t.Fatalf("ParseBook: %v", err)
	}
	update, err := message.Normalized(terms)
	if err != nil {
		t.Fatalf("BookMessage.Normalized: %v", err)
	}
	return update
}

func deribitBookFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "deribit", "testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}
