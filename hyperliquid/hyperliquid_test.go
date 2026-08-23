package hyperliquid

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/normalize"
)

func TestHyperliquid(t *testing.T) {
	t.Run("offline verification preserves source fidelity", func(t *testing.T) {
		summary, err := VerifyFixtures("testdata")
		if err != nil {
			t.Fatalf("VerifyFixtures() error = %v", err)
		}
		if summary.Venue != "hyperliquid" || summary.FixtureCount != 12 || summary.SyntheticCount != 12 || len(summary.Checks) != 5 || summary.ManifestSHA256 == "" || summary.EvidenceSHA256 == "" {
			t.Fatalf("VerifyFixtures() summary = %+v", summary)
		}
	})

	t.Run("BBO contexts and funding retain exact source values", func(t *testing.T) {
		dexs := testPerpDEXs(t)
		hip3 := testHIP3Metadata(t, dexs[1])
		bboPayload := testFixture(t, "bbo.json")
		quote, err := ParseBBO(bboPayload)
		if err != nil {
			t.Fatalf("ParseBBO() error = %v", err)
		}
		if quote.Coin != "xyz:BTC" || quote.Bid == nil || quote.Ask == nil || quote.Bid.Price != "113004.0" || quote.Ask.Size != "1.5" || !slices.Equal(quote.Evidence.Bytes(), bboPayload) {
			t.Fatalf("ParseBBO() = %+v", quote)
		}
		activePayload := testFixture(t, "active_asset_context.json")
		active, err := ParseActiveAssetContext(hip3.Universe[0].Identity, activePayload)
		if err != nil {
			t.Fatalf("ParseActiveAssetContext() error = %v", err)
		}
		if active.Coin != "xyz:BTC" || active.Perp == nil || active.Spot != nil || active.Perp.Funding.Text != "0.0002110251" || active.Perp.MidPrice.State != NativeNull || !slices.Equal(active.Evidence.Bytes(), activePayload) {
			t.Fatalf("ParseActiveAssetContext() = %+v", active)
		}
		fundingPayload := testFixture(t, "funding_history.json")
		funding, err := ParseFundingHistory(fundingPayload)
		if err != nil {
			t.Fatalf("ParseFundingHistory() error = %v", err)
		}
		if len(funding) != 2 || funding[0].FundingRate != "-0.00022196" || funding[1].Premium != "0.00020125" || !slices.Equal(funding[0].Evidence.Bytes(), fundingPayload) {
			t.Fatalf("ParseFundingHistory() = %+v", funding)
		}
	})

	t.Run("v1 has no native history import contract", func(t *testing.T) {
		support, ok := Supports(HIP3, RoleNativeHistoryImport)
		if !ok || support.Support != capture.SupportUnsupported || support.Limitation == "" {
			t.Fatalf("Supports(HIP3, native import) = %+v, %v", support, ok)
		}
		if _, err := InfoRequestWeight("nativeHistoryImport"); !errors.Is(err, ErrUnsupportedRole) {
			t.Fatalf("InfoRequestWeight(nativeHistoryImport) error = %v", err)
		}
	})

	t.Run("fixture verifier contains every read to explicit root", func(t *testing.T) {
		manifestBytes := testFixture(t, "manifest.json")
		var manifest FixtureManifest
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			t.Fatal(err)
		}
		manifest.Fixtures[0].File = "../outside.json"
		mutated, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, "manifest.json"), mutated, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
			t.Fatalf("VerifyFixtures(traversal) error = %v", err)
		}
		outsideRoot := t.TempDir()
		if err := os.WriteFile(filepath.Join(outsideRoot, "manifest.json"), manifestBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		absolute, err := filepath.Abs(filepath.Join("testdata", "perp_dexs.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(absolute, filepath.Join(outsideRoot, "perp_dexs.json")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := VerifyFixtures(outsideRoot); !errors.Is(err, ErrFixtureBoundary) {
			t.Fatalf("VerifyFixtures(symlinked fixture) error = %v", err)
		}
	})

	t.Run("source contracts declare access-dated public capabilities", func(t *testing.T) {
		ws, err := PublicSourceContract(Mainnet, HIP3, "xyz")
		if err != nil {
			t.Fatalf("PublicSourceContract() error = %v", err)
		}
		info, err := InfoSourceContract(Mainnet, HIP3, "xyz")
		if err != nil {
			t.Fatalf("InfoSourceContract() error = %v", err)
		}
		if ws.SourceID != "hyperliquid-mainnet-hip3_perpetual-xyz" || ws.SourceID != info.SourceID || ws.ContractID == info.ContractID {
			t.Fatalf("contract identities ws=%q/%q info=%q", ws.SourceID, ws.ContractID, info.ContractID)
		}
		if _, ok := ws.Capability("normalize:economic_units", "strict_normalized_total"); ok {
			t.Fatal("WebSocket contract leaked Info/normalization capability")
		}
		strict, ok := info.Capability("normalize:economic_units", "strict_normalized_total")
		if !ok || strict.Support != capture.SupportAmbiguous || strict.Declaration == "" {
			t.Fatalf("HIP-3 Info strict-unit capability = %+v, %v", strict, ok)
		}
		for _, capability := range ws.Capabilities {
			if len(capability.ChannelOrEndpoint) < 3 || capability.ChannelOrEndpoint[:3] != "ws:" {
				t.Fatalf("non-WebSocket capability leaked into public socket contract: %+v", capability)
			}
		}
		for _, capability := range info.Capabilities {
			if len(capability.ChannelOrEndpoint) >= 3 && capability.ChannelOrEndpoint[:3] == "ws:" {
				t.Fatalf("WebSocket capability leaked into Info contract: %+v", capability)
			}
		}
		mainInfo, err := InfoSourceContract(Mainnet, MainPerpetual, "")
		if err != nil {
			t.Fatal(err)
		}
		mainStrict, ok := mainInfo.Capability("normalize:economic_units", "strict_normalized_total")
		if !ok || mainStrict.Support != capture.SupportAvailable {
			t.Fatalf("main Info strict-unit capability = %+v, %v", mainStrict, ok)
		}
		if _, err := PublicSourceContract(Mainnet, MainPerpetual, "xyz"); !errors.Is(err, ErrInvalidFamily) {
			t.Fatalf("PublicSourceContract(main with dex) error = %v", err)
		}
		weight, err := InfoFinalWeight("fundingHistory", 41)
		if err != nil || weight != 23 {
			t.Fatalf("InfoFinalWeight(fundingHistory, 41) = %d, %v", weight, err)
		}
	})

	t.Run("subscription wire and acknowledgements preserve explicit identity", func(t *testing.T) {
		hip3 := Subscription{
			Type: SubscriptionL2Book,
			Coin: "BTC",
			DEX:  "xyz",
			Book: BookDepthContract{Fast: true, NSigFigs: 5, Mantissa: 2},
		}
		messages, err := SubscriptionMessages(HIP3, "xyz", []Subscription{hip3})
		if err != nil || len(messages) != 1 {
			t.Fatalf("SubscriptionMessages() = %q, %v", messages, err)
		}
		const wantSubscribe = `{"method":"subscribe","subscription":{"type":"l2Book","coin":"xyz:BTC","nSigFigs":5,"mantissa":2,"fast":true}}`
		if string(messages[0]) != wantSubscribe {
			t.Fatalf("HIP-3 subscribe = %s", messages[0])
		}
		unsubscribe, err := encodeSubscriptionOperation("unsubscribe", HIP3, "xyz", hip3)
		if err != nil {
			t.Fatal(err)
		}
		const wantUnsubscribe = `{"method":"unsubscribe","subscription":{"type":"l2Book","coin":"xyz:BTC","nSigFigs":5,"mantissa":2,"fast":true}}`
		if string(unsubscribe) != wantUnsubscribe {
			t.Fatalf("HIP-3 unsubscribe = %s", unsubscribe)
		}

		main, err := SubscriptionMessages(MainPerpetual, "", []Subscription{{Type: SubscriptionTrades, Coin: "BTC"}})
		if err != nil || len(main) != 1 || string(main[0]) != `{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}` {
			t.Fatalf("main subscribe = %q, %v", main, err)
		}
		spot, err := SubscriptionMessages(Spot, "", []Subscription{{Type: SubscriptionBBO, Coin: "@1"}})
		if err != nil || len(spot) != 1 || string(spot[0]) != `{"method":"subscribe","subscription":{"type":"bbo","coin":"@1"}}` {
			t.Fatalf("spot subscribe = %q, %v", spot, err)
		}

		ack, err := ParseSubscriptionACK(HIP3, "xyz", []byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"xyz:BTC","nSigFigs":5,"mantissa":2,"fast":true}}}`))
		if err != nil {
			t.Fatalf("ParseSubscriptionACK() error = %v", err)
		}
		if ack.Subscription != hip3 || ack.Subscription.StreamIdentity() != hip3.StreamIdentity() {
			t.Fatalf("ACK subscription = %+v", ack.Subscription)
		}
		otherDEX := hip3
		otherDEX.DEX = "abc"
		if otherDEX.StreamIdentity() == hip3.StreamIdentity() {
			t.Fatal("StreamIdentity omitted the explicit DEX")
		}
		invalidACKs := [][]byte{
			[]byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","fast":true}}}`),
			[]byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"abc:BTC","fast":true}}}`),
			[]byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"xyz:BTC","dex":"xyz","fast":true}}}`),
			[]byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"xyz:xyz:BTC","fast":true}}}`),
		}
		for _, payload := range invalidACKs {
			if _, err := ParseSubscriptionACK(HIP3, "xyz", payload); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("ParseSubscriptionACK(%s) error = %v", payload, err)
			}
		}
		if _, err := ParseSubscriptionACK(MainPerpetual, "", []byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC","dex":"xyz"}}}`)); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("main ACK with DEX error = %v", err)
		}

		invalidSubscriptions := []struct {
			family  Family
			dexName string
			value   Subscription
		}{
			{family: HIP3, dexName: "xyz", value: Subscription{Type: SubscriptionTrades, Coin: "BTC"}},
			{family: HIP3, dexName: "xyz", value: Subscription{Type: SubscriptionTrades, Coin: "BTC", DEX: "abc"}},
			{family: HIP3, dexName: "xyz", value: Subscription{Type: SubscriptionTrades, Coin: "xyz:BTC", DEX: "xyz"}},
			{family: MainPerpetual, value: Subscription{Type: SubscriptionTrades, Coin: "BTC", DEX: "xyz"}},
			{family: Spot, value: Subscription{Type: SubscriptionTrades, Coin: "@1", DEX: "xyz"}},
		}
		for _, test := range invalidSubscriptions {
			if err := test.value.Validate(test.family, test.dexName); !errors.Is(err, ErrInvalidSubscription) {
				t.Fatalf("Validate(%s, %q, %+v) error = %v", test.family, test.dexName, test.value, err)
			}
		}
		mainReceive, err := newReceiveEnvelope(
			[]byte(`{"channel":"trades","data":[{"coin":"BTC"}]}`),
			1,
			MainPerpetual,
			"",
			Subscription{Type: SubscriptionTrades, Coin: "BTC"},
			true,
		)
		if err != nil || mainReceive.Coin() != "BTC" || mainReceive.DEXName() != "" {
			t.Fatalf("main receive identity = %+v, %v", mainReceive, err)
		}
		spotReceive, err := newReceiveEnvelope(
			[]byte(`{"channel":"bbo","data":{"coin":"@1"}}`),
			1,
			Spot,
			"",
			Subscription{Type: SubscriptionBBO, Coin: "@1"},
			true,
		)
		if err != nil || spotReceive.Coin() != "@1" || spotReceive.DEXName() != "" {
			t.Fatalf("spot receive identity = %+v, %v", spotReceive, err)
		}
		if _, err := newReceiveEnvelope(
			[]byte(`{"channel":"trades","data":[{"coin":"BTC","dex":"xyz"}]}`),
			1,
			MainPerpetual,
			"",
			Subscription{Type: SubscriptionTrades, Coin: "BTC"},
			true,
		); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("main receive with DEX error = %v", err)
		}
		if _, err := ParsePong([]byte(`{"channel":"pong"}`)); err != nil {
			t.Fatalf("ParsePong() error = %v", err)
		}
	})

	t.Run("spot pairs are namespaced apart from perpetual coins", func(t *testing.T) {
		spotPayload := testFixture(t, "spot_meta_contexts.json")
		spotGeneration := testGenerationEvidence(spotPayload, "spot", 1)
		spot, err := ParseSpotMetadataAndContexts(Mainnet, spotGeneration, spotPayload)
		if err != nil {
			t.Fatalf("ParseSpotMetadataAndContexts() error = %v", err)
		}
		if len(spot.Universe) != 1 || spot.Universe[0].Identity.Family != catalog.HyperliquidSpot || spot.Universe[0].QuoteToken.Name != "USDC" {
			t.Fatalf("spot universe = %+v", spot.Universe)
		}
		provenance := normalize.FieldProvenance{
			SourceTimeNS:         normalize.OptionalInt64{Value: 1787356800000000000, Valid: true},
			SourceTimeResolution: normalize.ResolutionMillisecond,
			AgeNS:                normalize.OptionalUint64{Valid: true},
		}
		supply, err := spot.Contexts[0].CirculatingSupplyValue(spot.Universe[0].BaseToken.Name, provenance)
		if err != nil || !supply.EligibleForStrictTotal() {
			t.Fatalf("spot circulating supply = %+v, %v", supply, err)
		}
		metadataOnly, err := ParseSpotMetadata(Mainnet, testGenerationEvidence(spot.RawMetadata, "spot-metadata-only", 2), spot.RawMetadata)
		if err != nil || metadataOnly.Generation == ([sha256.Size]byte{}) || len(metadataOnly.Contexts) != 0 {
			t.Fatalf("ParseSpotMetadata() = %+v, %v", metadataOnly, err)
		}
		mismatchedEvidence := testGenerationEvidence(spot.RawMetadata, "spot-mismatch", 3)
		mismatchedEvidence.RawPayloadSHA256 = sha256.Sum256([]byte(`{"wrong":true}`))
		if _, err := ParseSpotMetadata(Mainnet, mismatchedEvidence, spot.RawMetadata); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("ParseSpotMetadata(cross-payload evidence) error = %v", err)
		}
	})

	t.Run("info requests carry documented types and weights", func(t *testing.T) {
		dexs, err := PerpDexsRequest()
		if err != nil {
			t.Fatal(err)
		}
		hip3Meta, err := PerpMetadataRequest("xyz", true)
		if err != nil {
			t.Fatal(err)
		}
		spot, err := SpotMetadataRequest(true)
		if err != nil {
			t.Fatal(err)
		}
		book, err := L2BookRequest("xyz:BTC", 5, 2)
		if err != nil {
			t.Fatal(err)
		}
		funding, err := FundingHistoryRequest("xyz:BTC", 1787356800000, nil)
		if err != nil {
			t.Fatal(err)
		}
		if dexs.Weight() != 20 || hip3Meta.Weight() != 20 || spot.Weight() != 20 || book.Weight() != 2 || funding.Weight() != 20 {
			t.Fatalf("weights = %d/%d/%d/%d/%d", dexs.Weight(), hip3Meta.Weight(), spot.Weight(), book.Weight(), funding.Weight())
		}
		if string(hip3Meta.Bytes()) != `{"type":"metaAndAssetCtxs","dex":"xyz"}` || string(book.Bytes()) != `{"type":"l2Book","coin":"xyz:BTC","nSigFigs":5,"mantissa":2}` {
			t.Fatalf("bodies = %s / %s", hip3Meta.Bytes(), book.Bytes())
		}
		if _, err := L2BookRequest("xyz:BTC", 4, 2); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("L2BookRequest(mantissa without nSigFigs 5) error = %v", err)
		}
		if _, err := FundingHistoryRequest("xyz:BTC", 10, &[]int64{5}[0]); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("FundingHistoryRequest(inverted window) error = %v", err)
		}
	})
}

func TestHIP3(t *testing.T) {
	dexs := testPerpDEXs(t)
	mainMeta, err := ParsePerpMetadataAndContexts(Mainnet, dexs[0], testGenerationEvidence(testFixture(t, "main_meta_contexts.json"), "main", 1), testFixture(t, "main_meta_contexts.json"))
	if err != nil {
		t.Fatal(err)
	}
	hip3 := testHIP3Metadata(t, dexs[1])

	t.Run("namespace and metadata generation cannot collide", func(t *testing.T) {
		mainBTC := mainMeta.Universe[0].Identity
		hip3BTC := hip3.Universe[0].Identity
		abcBTC, err := catalog.NewHyperliquidInstrumentIdentity(catalog.HyperliquidIdentityInput{
			Network: catalog.HyperliquidNetworkMainnet, Family: catalog.HyperliquidHIP3,
			DEXName: dexs[2].Name, WireCoin: "abc:BTC", MetadataGeneration: hip3BTC.MetadataGeneration,
			Deployer: dexs[2].Deployer, CollateralToken: hip3BTC.CollateralToken, UniverseIndex: hip3BTC.UniverseIndex,
		})
		if err != nil {
			t.Fatal(err)
		}
		recycledGeneration := testGenerationEvidence(testFixture(t, "hip3_meta_contexts.json"), "hip3-recycled", 2)
		recycledMeta, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], recycledGeneration, testFixture(t, "hip3_meta_contexts.json"))
		if err != nil {
			t.Fatal(err)
		}
		recycled := recycledMeta.Universe[0].Identity
		ids := []string{mainBTC.InstrumentUID, hip3BTC.InstrumentUID, abcBTC.InstrumentUID, recycled.InstrumentUID}
		for i := range ids {
			for j := i + 1; j < len(ids); j++ {
				if ids[i] == ids[j] {
					t.Fatalf("identity collision at %d/%d: %s", i, j, ids[i])
				}
			}
		}
	})

	t.Run("positional metadata mismatch fails closed", func(t *testing.T) {
		_, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], testGenerationEvidence(testFixture(t, "hip3_positional_mismatch.json"), "hip3", 1), testFixture(t, "hip3_positional_mismatch.json"))
		if !errors.Is(err, ErrPositionalMismatch) {
			t.Fatalf("ParsePerpMetadataAndContexts(mismatch) error = %v", err)
		}
	})

	t.Run("duplicate native trade keys remain separate evidence rows", func(t *testing.T) {
		payload := testFixture(t, "trades_duplicate.json")
		trades, err := ParseTrades(payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(trades) != 2 || trades[0].Coin != "xyz:BTC" ||
			trades[0].Key() != trades[1].Key() || trades[0].MessageOrdinal == trades[1].MessageOrdinal ||
			trades[0].NativeDuplicatePolicy != DuplicatePolicyPreserveUnassessed || trades[1].NativeDuplicatePolicy != DuplicatePolicyPreserveUnassessed ||
			!slices.Equal(trades[0].Evidence.Bytes(), payload) || !slices.Equal(trades[1].Evidence.Bytes(), payload) {
			t.Fatalf("ParseTrades() = %+v", trades)
		}
	})

	t.Run("provisional economic units are excluded from strict totals", func(t *testing.T) {
		provenance := normalize.FieldProvenance{
			SourceTimeNS:         normalize.OptionalInt64{Value: 1787356800000000000, Valid: true},
			SourceTimeResolution: normalize.ResolutionMillisecond,
			AgeNS:                normalize.OptionalUint64{Valid: true},
		}
		resolved, err := mainMeta.Contexts[0].OpenInterestValue("BTC", provenance)
		if err != nil {
			t.Fatal(err)
		}
		provisional, err := hip3.Contexts[0].OpenInterestValue("not-used", provenance)
		if err != nil {
			t.Fatal(err)
		}
		if provisional.Normalized.State != normalize.SourceMissing || provisional.EligibleForStrictTotal() {
			t.Fatalf("HIP-3 value unexpectedly strict-eligible: %+v", provisional)
		}
		total, err := normalize.StrictHyperliquidTotal([]normalize.HyperliquidEconomicValue{resolved, provisional})
		if err != nil {
			t.Fatal(err)
		}
		if !total.HasValue || total.Included != 1 || total.Excluded != 1 || total.Value != resolved.Normalized.Value {
			t.Fatalf("StrictHyperliquidTotal() = %+v", total)
		}
	})

	t.Run("trade and book amounts stay provisional under HIP-3", func(t *testing.T) {
		provenance := normalize.FieldProvenance{
			SourceTimeNS:         normalize.OptionalInt64{Value: 1787356800123000000, Valid: true},
			SourceTimeResolution: normalize.ResolutionMillisecond,
			AgeNS:                normalize.OptionalUint64{Valid: true},
		}
		trades, err := ParseTrades(testFixture(t, "trades_duplicate.json"))
		if err != nil {
			t.Fatal(err)
		}
		identity := hip3.Universe[0].Identity
		tradeAmount, err := trades[0].AmountValue(identity, "BTC", provenance)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := ParseBookSnapshot(testBookEnvelope(t, testFixture(t, "book_slow_initial.json"), BookDepthContract{}))
		if err != nil {
			t.Fatal(err)
		}
		bookAmounts, err := snapshot.AmountValues(identity, "BTC", provenance)
		if err != nil {
			t.Fatal(err)
		}
		if tradeAmount.EligibleForStrictTotal() || tradeAmount.Native.Unit.Kind != normalize.NativeUnitVenueUnspecified || len(bookAmounts) != 3 {
			t.Fatalf("trade amount = %+v, book amounts = %d", tradeAmount, len(bookAmounts))
		}
		for index, amount := range bookAmounts {
			if amount.EligibleForStrictTotal() || amount.Normalized.State != normalize.SourceMissing {
				t.Fatalf("book amount %d unexpectedly strict-eligible: %+v", index, amount)
			}
		}
		total, err := normalize.StrictHyperliquidTotal(append([]normalize.HyperliquidEconomicValue{tradeAmount}, bookAmounts...))
		if err != nil {
			t.Fatal(err)
		}
		if total.HasValue || total.Included != 0 || total.Excluded != 4 {
			t.Fatalf("StrictHyperliquidTotal(all provisional) = %+v", total)
		}
	})
}

func TestBookDepthContracts(t *testing.T) {
	dexs := testPerpDEXs(t)
	hip3 := testHIP3Metadata(t, dexs[1])
	identity := hip3.Universe[0].Identity

	t.Run("slow 20 and fast 5 are distinct exact streams", func(t *testing.T) {
		slow, err := ParseBookSnapshot(testBookEnvelope(t, testFixture(t, "book_slow_initial.json"), BookDepthContract{}))
		if err != nil {
			t.Fatal(err)
		}
		fastDepth := BookDepthContract{Fast: true, NSigFigs: 5, Mantissa: 2}
		fast, err := ParseBookSnapshot(testBookEnvelope(t, testFixture(t, "book_fast.json"), fastDepth))
		if err != nil {
			t.Fatal(err)
		}
		if slow.Coin != "xyz:BTC" || slow.Depth.MaximumLevels() != 20 || fast.Depth.MaximumLevels() != 5 || len(fast.Bids) != 5 || len(fast.Asks) != 5 || slow.Depth.Name() == fast.Depth.Name() {
			t.Fatalf("depth contracts slow=%+v fast=%+v", slow.Depth, fast.Depth)
		}
		messages, err := SubscriptionMessages(HIP3, "xyz", []Subscription{
			{Type: SubscriptionL2Book, Coin: "BTC", DEX: "xyz", Book: slow.Depth},
			{Type: SubscriptionL2Book, Coin: "BTC", DEX: "xyz", Book: fast.Depth},
		})
		if err != nil || len(messages) != 2 || slices.Equal(messages[0], messages[1]) {
			t.Fatalf("SubscriptionMessages() = %q, %v", messages, err)
		}
	})

	t.Run("fast depth rejects a sixth source level", func(t *testing.T) {
		var message map[string]any
		if err := json.Unmarshal(testFixture(t, "book_fast.json"), &message); err != nil {
			t.Fatal(err)
		}
		data := message["data"].(map[string]any)
		levels := data["levels"].([]any)
		bids := levels[0].([]any)
		levels[0] = append(bids, map[string]any{"px": "112999.0", "sz": "1.0", "n": float64(1)})
		payload, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseBookSnapshot(testBookEnvelope(t, payload, BookDepthContract{Fast: true})); !errors.Is(err, ErrBookDepthContract) {
			t.Fatalf("ParseBookSnapshot(6 fast levels) error = %v", err)
		}
	})

	t.Run("every snapshot fully replaces and emits no-sequence uncertainty", func(t *testing.T) {
		book, err := NewBook(identity, BookDepthContract{})
		if err != nil {
			t.Fatal(err)
		}
		initial, err := ParseBookSnapshot(testBookEnvelope(t, testFixture(t, "book_slow_initial.json"), BookDepthContract{}))
		if err != nil {
			t.Fatal(err)
		}
		replacement, err := ParseBookSnapshot(testBookEnvelope(t, testFixture(t, "book_slow_replacement.json"), BookDepthContract{}))
		if err != nil {
			t.Fatal(err)
		}
		first, err := book.Apply(initial)
		if err != nil || first.ReplacedPrior {
			t.Fatalf("first Apply() = %+v, %v", first, err)
		}
		second, err := book.Apply(replacement)
		if err != nil {
			t.Fatal(err)
		}
		view := book.Snapshot()
		if !second.ReplacedPrior || second.Gap.State != "uncertain" || second.Gap.Reason != BookContinuityNoSequence || second.Gap.SequenceDetectable || second.Gap.DeltaClaim ||
			len(view.Bids) != 1 || view.Bids[0].Price != "113005.0" || view.ReplacementCount != 2 || view.ContinuityUncertainty != BookContinuityNoSequence {
			t.Fatalf("replacement result=%+v view=%+v", second, view)
		}
	})
}

func testFixture(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func testPerpDEXs(t *testing.T) []PerpDEX {
	t.Helper()
	dexs, err := ParsePerpDEXs(testFixture(t, "perp_dexs.json"))
	if err != nil {
		t.Fatal(err)
	}
	return dexs
}

func testHIP3Metadata(t *testing.T, dex PerpDEX) PerpMetadata {
	t.Helper()
	metadata, err := ParsePerpMetadataAndContexts(Mainnet, dex, testGenerationEvidence(testFixture(t, "hip3_meta_contexts.json"), "hip3", 1), testFixture(t, "hip3_meta_contexts.json"))
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

func testGenerationEvidence(payload []byte, epoch string, ordinal uint64) catalog.HyperliquidGenerationEvidence {
	evidence := catalog.HyperliquidGenerationEvidence{
		EvidenceScope: catalog.RawEvidenceInMemoryProjection, SourceID: "hyperliquid-test", EpochID: epoch,
		ArrivalOrdinal: ordinal, GenerationStartNS: DocumentationAccessTimeNS + int64(ordinal),
		RawPayloadSHA256: sha256.Sum256(payload),
	}
	var pair []json.RawMessage
	if json.Unmarshal(payload, &pair) == nil && len(pair) == 2 && len(pair[0]) > 0 && pair[0][0] == '{' && len(pair[1]) > 0 && pair[1][0] == '[' {
		evidence.RawPayloadSHA256 = sha256.Sum256(pair[0])
		evidence.ContextPayloadSHA256 = sha256.Sum256(pair[1])
		evidence.EnvelopePayloadSHA256 = sha256.Sum256(payload)
	}
	return evidence
}

func testBookEnvelope(t *testing.T, payload []byte, depth BookDepthContract) ReceiveEnvelope {
	t.Helper()
	subscription := Subscription{Type: SubscriptionL2Book, Coin: "BTC", DEX: "xyz", Book: depth}
	envelope, err := newReceiveEnvelope(payload, 1, HIP3, "xyz", subscription, true)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}
