package bybit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

func TestBybitOptionDistinctSourceAndBaseCoinTopics(t *testing.T) {
	contract := OptionPublicSourceContract()
	if err := contract.Validate(); err != nil {
		t.Fatalf("OptionPublicSourceContract: %v", err)
	}
	if contract.SourceID != OptionSourceID || contract.SourceID == Linear.SourceID() || OptionPublicEndpoint == Linear.PublicEndpoint() {
		t.Fatalf("Option source/socket identity is not distinct: %+v", contract)
	}
	instrumentContract := OptionInstrumentSourceContract()
	if err := instrumentContract.Validate(); err != nil {
		t.Fatalf("OptionInstrumentSourceContract: %v", err)
	}
	for _, capability := range contract.Capabilities {
		if capability.ChannelOrEndpoint == InstrumentInfoPath {
			t.Fatal("WebSocket source contract advertised the REST instrument endpoint")
		}
	}
	trade, err := (OptionTopicRequest{Role: RoleTrade, BaseCoin: "BTC"}).Topic()
	if err != nil || trade != "publicTrade.BTC" {
		t.Fatalf("trade base-coin topic = %q, %v", trade, err)
	}
	ticker, err := (OptionTopicRequest{Role: RoleOptionTicker, BaseCoin: "BTC"}).Topic()
	if err != nil || ticker != "tickers.BTC" {
		t.Fatalf("ticker base-coin topic = %q, %v", ticker, err)
	}
	if _, err := (OptionTopicRequest{Role: RoleTrade, Symbol: "BTC-30DEC22-18000-P"}).Topic(); err == nil {
		t.Fatal("option trade accepted instrument topic identity in place of base coin")
	}
	messages, err := OptionSubscriptionMessages([]OptionTopicRequest{
		{Role: RoleOptionTicker, BaseCoin: "BTC"},
		{Role: RoleBoundedOrderbook, Symbol: "BTC-30DEC22-18000-P", Depth: 25},
		{Role: RoleTrade, BaseCoin: "BTC"},
	})
	if err != nil || len(messages) != 1 {
		t.Fatalf("OptionSubscriptionMessages: count=%d err=%v", len(messages), err)
	}
	var request SubscriptionRequest
	if json.Unmarshal(messages[0], &request) != nil || !slices.Equal(request.Arguments, []string{"orderbook.25.BTC-30DEC22-18000-P", "publicTrade.BTC", "tickers.BTC"}) {
		t.Fatalf("deterministic option subscription = %+v", request)
	}
}

func TestBybitOptionMinimumBookDepthAndUnsupportedRoles(t *testing.T) {
	for _, depth := range []int{OptionMinimumBookDepth, OptionMaximumBookDepth} {
		topic, err := (OptionTopicRequest{Role: RoleBoundedOrderbook, Symbol: "BTC-30DEC22-18000-P", Depth: depth}).Topic()
		if err != nil || topic == "" {
			t.Fatalf("documented option depth %d: topic=%q err=%v", depth, topic, err)
		}
	}
	for _, depth := range []int{1, 50, 200, 1000} {
		if _, err := (OptionTopicRequest{Role: RoleBoundedOrderbook, Symbol: "BTC-30DEC22-18000-P", Depth: depth}).Topic(); err == nil {
			t.Fatalf("unsupported option depth %d accepted", depth)
		}
	}
	for _, role := range []SourceRole{RoleBBO, RoleFullOrderbook, RoleRPIOrderbook, RoleAllLiquidation} {
		support, ok := OptionSupports(role)
		if !ok || support.Support != capture.SupportUnsupported || support.Limitation == "" {
			t.Fatalf("role %s not explicitly unsupported: %+v", role, support)
		}
		if _, err := (OptionTopicRequest{Role: role, Symbol: "BTC-30DEC22-18000-P", Depth: 1}).Topic(); !errors.Is(err, ErrUnsupportedRole) {
			t.Fatalf("unsupported role %s topic error = %v", role, err)
		}
	}
}

func TestBybitOptionStrictTradeBookAndSnapshotTicker(t *testing.T) {
	trades, err := ParseOptionTrades(optionFixture(t, "official/trade.json"))
	if err != nil || len(trades) != 1 || trades[0].BaseCoin != "BTC" || trades[0].MarkIV.Text != "0.7567" || trades[0].TradeIV.Text != "0.8000" {
		t.Fatalf("ParseOptionTrades: %+v err=%v", trades, err)
	}
	bookMessage, err := ParseOptionOrderbook(optionFixture(t, "official/book-25-snapshot.json"))
	if err != nil || bookMessage.Depth != 25 || bookMessage.Kind != BookSnapshot {
		t.Fatalf("ParseOptionOrderbook: %+v err=%v", bookMessage, err)
	}
	book, err := NewOptionBook(bookMessage.Symbol, bookMessage.Depth)
	if err != nil || book.Apply(bookMessage) != nil || !book.Snapshot().Seeded || book.Snapshot().Bids["0.0005"] != "12.5" {
		t.Fatalf("option bounded book snapshot: %+v err=%v", book.Snapshot(), err)
	}
	ticker, err := ParseOptionTickerSnapshot(optionFixture(t, "official/ticker-snapshot.json"))
	if err != nil || ticker.BaseCoin != "BTC" || ticker.Fields.Delta.Text != "-0.9876" || ticker.Fields.Rho.State != normalize.SourceMissing {
		t.Fatalf("ParseOptionTickerSnapshot: %+v err=%v", ticker, err)
	}
	if _, err := ParseOptionTickerSnapshot(optionFixture(t, "synthetic/ticker-delta.json")); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("snapshot-only ticker accepted delta: %v", err)
	}
	if _, err := ParseOptionTickerSnapshot(optionFixture(t, "synthetic/greek-type-drift.json")); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("ticker accepted malformed Greek type drift: %v", err)
	}
}

func TestBybitOptionMetadataRequiresPresenceAndPreservesZeroFalse(t *testing.T) {
	payload := optionFixture(t, "official/instrument-option.json")
	for _, field := range []string{"retCode", "time", "unifiedMarginTrade"} {
		t.Run("missing_"+field, func(t *testing.T) {
			mutated := bytes.Replace(payload, []byte(`"`+field+`"`), []byte(`"missing_`+field+`"`), 1)
			if bytes.Equal(mutated, payload) {
				t.Fatalf("field %s mutation did not match", field)
			}
			if _, err := ParseOptionInstrumentInfo(mutated); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("missing required %s error = %v", field, err)
			}
		})
	}
	explicitFalse := bytes.Replace(payload, []byte(`"unifiedMarginTrade": true`), []byte(`"unifiedMarginTrade": false`), 1)
	page, err := ParseOptionInstrumentInfo(explicitFalse)
	if err != nil || len(page.Instruments) != 1 || page.Instruments[0].UnifiedMarginTrade {
		t.Fatalf("explicit false unifiedMarginTrade was not preserved: page=%+v err=%v", page, err)
	}
	explicitZeroTime := bytes.Replace(payload, []byte(`"time": 1672304487000`), []byte(`"time": 0`), 1)
	page, err = ParseOptionInstrumentInfo(explicitZeroTime)
	if err != nil || page.ObservedTimeMS != 0 {
		t.Fatalf("explicit zero response time was not preserved: page=%+v err=%v", page, err)
	}
}

func TestBybitOptionCanonicalUppercaseSymbolAndRequestIdentity(t *testing.T) {
	for _, symbol := range []string{"BTC-30Dec22-18000-P", "BTC-30dec22-18000-P"} {
		if validOptionSymbol(symbol) {
			t.Fatalf("mixed/lowercase expiry symbol accepted: %s", symbol)
		}
		if _, err := (OptionTopicRequest{Role: RoleBoundedOrderbook, Symbol: symbol, Depth: 25}).Topic(); err == nil {
			t.Fatalf("mixed/lowercase option book topic accepted: %s", symbol)
		}
	}
	if _, err := (OptionTopicRequest{Role: RoleTrade, BaseCoin: "btc"}).Topic(); err == nil {
		t.Fatal("lowercase option base-coin topic accepted")
	}
	tickerLowerTopic := bytes.Replace(optionFixture(t, "official/ticker-snapshot.json"), []byte(`"tickers.BTC"`), []byte(`"tickers.btc"`), 1)
	if _, err := ParseOptionTickerSnapshot(tickerLowerTopic); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("lowercase ticker topic error = %v", err)
	}
	tradeMixedSymbol := bytes.Replace(optionFixture(t, "official/trade.json"), []byte("BTC-30DEC22-18000-P"), []byte("BTC-30Dec22-18000-P"), 1)
	if _, err := ParseOptionTrades(tradeMixedSymbol); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("mixed-case trade symbol error = %v", err)
	}
	if _, err := NewOptionInstrumentRequest("ETH", "BTC-30DEC22-18000-P", "", 0); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("baseCoin/symbol mismatch error = %v", err)
	}
	if _, err := NewOptionInstrumentRequest("BTC", "BTC-30DEC22-18000-P", "", 0); err != nil {
		t.Fatalf("matching baseCoin/symbol rejected: %v", err)
	}
}

func TestBybitOptionPayloadBoundsAcrossParserFamilies(t *testing.T) {
	wsPolicy := optionWSPayloadPolicy()
	restPolicy := optionRESTPayloadPolicy()
	wsContract := OptionPublicSourceContract()
	restContract := OptionInstrumentSourceContract()
	if wsContract.Payload != wsPolicy || restContract.Payload != restPolicy {
		t.Fatalf("source contract/parser structural bounds diverged: ws=%+v rest=%+v", wsContract.Payload, restContract.Payload)
	}
	tickerWithExtraFields := func(extra int) []byte {
		data := map[string]any{"symbol": "BTC-30DEC22-18000-P"}
		for i := range extra {
			data[fmt.Sprintf("extra_%04d", i)] = "1"
		}
		payload, err := json.Marshal(map[string]any{
			"topic": "tickers.BTC", "type": "snapshot", "ts": int64(1672304486868), "data": data,
		})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	if _, err := ParseOptionTickerSnapshot(tickerWithExtraFields(1019)); err != nil {
		t.Fatalf("ticker at exact %d-field bound rejected: %v", OptionMaxSchemaFields, err)
	}
	overFields := tickerWithExtraFields(1020)
	if _, err := ParseOptionTickerSnapshot(overFields); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("ticker over %d-field bound error = %v", OptionMaxSchemaFields, err)
	}
	tooDeep := []byte(`{"x":` + strings.Repeat("[", int(OptionMaxSchemaDepth)) + `0` + strings.Repeat("]", int(OptionMaxSchemaDepth)) + `}`)
	tooManyWSArrayElements := []byte(`{"x":[` + strings.Repeat("0,", int(OptionMaxArrayElements)) + `0]}`)
	tooManyRESTArrayElements := []byte(`{"x":[` + strings.Repeat("0,", int(OptionRESTMaxArrayElements)) + `0]}`)
	flatObjectWithFields := func(fields int) []byte {
		value := make(map[string]string, fields)
		for i := range fields {
			value[fmt.Sprintf("field_%04d", i)] = "1"
		}
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	tooManyRESTFields := flatObjectWithFields(int(OptionRESTMaxSchemaFields) + 1)
	for name, test := range map[string]struct {
		payload []byte
		policy  capture.PayloadPolicy
	}{
		"depth":      {payload: tooDeep, policy: wsPolicy},
		"WS array":   {payload: tooManyWSArrayElements, policy: wsPolicy},
		"REST array": {payload: tooManyRESTArrayElements, policy: restPolicy},
	} {
		if err := validateOptionPayload(test.payload, test.policy); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("%s structural error = %v", name, err)
		}
	}
	families := []struct {
		name       string
		parse      func([]byte) error
		overFields []byte
		overArray  []byte
	}{
		{name: "trade", parse: func(payload []byte) error { _, err := ParseOptionTrades(payload); return err }, overFields: overFields, overArray: tooManyWSArrayElements},
		{name: "book", parse: func(payload []byte) error { _, err := ParseOptionOrderbook(payload); return err }, overFields: overFields, overArray: tooManyWSArrayElements},
		{name: "ticker", parse: func(payload []byte) error { _, err := ParseOptionTickerSnapshot(payload); return err }, overFields: overFields, overArray: tooManyWSArrayElements},
		{name: "metadata", parse: func(payload []byte) error { _, err := ParseOptionInstrumentInfo(payload); return err }, overFields: tooManyRESTFields, overArray: tooManyRESTArrayElements},
	}
	for _, family := range families {
		for boundary, payload := range map[string][]byte{
			"depth": tooDeep, "fields": family.overFields, "array": family.overArray,
		} {
			if err := family.parse(payload); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("%s parser over-%s error = %v", family.name, boundary, err)
			}
		}
	}
	oversizedPayload := make([]byte, MaxRawPayloadBytes+1)
	for _, family := range families {
		if err := family.parse(oversizedPayload); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("%s parser oversized-payload error = %v", family.name, err)
		}
	}
}

func TestBybitOptionSummaryV1MapsMetadataGreeksUnitsAndAges(t *testing.T) {
	page, err := ParseOptionInstrumentInfo(optionFixture(t, "official/instrument-option.json"))
	if err != nil || len(page.Instruments) != 1 {
		t.Fatalf("ParseOptionInstrumentInfo: %+v err=%v", page, err)
	}
	tickerBytes := optionFixture(t, "official/ticker-snapshot.json")
	ticker, err := ParseOptionTickerSnapshot(tickerBytes)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := optionFixtureMetadata(tickerBytes)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := ticker.Normalized(
		metadata,
		page.Instruments[0],
		page.ObservedTimeMS*int64(1e6),
		OptionIdentities{InstrumentUID: metadata.InstrumentUID, UnderlyingID: "BTC", IndexID: "BTC-USD"},
		optionFixtureUnits(metadata.InstrumentUID),
	)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Instrument.Value != metadata.InstrumentUID || summary.Underlying.Value != "BTC" || summary.Index.Value != "BTC-USD" || summary.CallPut.Value != normalize.OptionPut {
		t.Fatalf("option identities/contract mapping: %+v", summary)
	}
	if summary.Strike.Value.Decimal.Coefficient != "18000000000000000000000" || summary.Expiry.ValueNS != page.Instruments[0].DeliveryTimeMS*int64(1e6) {
		t.Fatalf("option strike/expiry mapping: strike=%+v expiry=%+v", summary.Strike, summary.Expiry)
	}
	for name, field := range map[string]normalize.NativeNumericField{
		"delta": summary.Delta, "gamma": summary.Gamma, "vega": summary.Vega, "theta": summary.Theta,
	} {
		if field.State != normalize.SourceValue || !field.Provenance.AgeNS.Valid {
			t.Fatalf("supplied Greek %s missing value/age: %+v", name, field)
		}
	}
	if summary.Rho.State != normalize.SourceMissing || summary.Rho.Value != (normalize.NativeValue{}) || summary.Rho.Provenance != (normalize.FieldProvenance{}) {
		t.Fatalf("unsupported rho was represented as zero/observed: %+v", summary.Rho)
	}
	if summary.OpenInterest.State != normalize.SourceValue || summary.OpenInterest.Value.Unit.Kind != normalize.NativeUnitContracts || summary.Volume.State != normalize.SourceValue {
		t.Fatalf("option OI/volume unit mapping: OI=%+v volume=%+v", summary.OpenInterest, summary.Volume)
	}
	if !summary.MarkPrice.Provenance.AgeNS.Valid || !summary.Instrument.Provenance.AgeNS.Valid || summary.MarkPrice.Provenance.AgeNS.Value == summary.Instrument.Provenance.AgeNS.Value {
		t.Fatalf("per-field ticker/metadata ages not retained: mark=%+v instrument=%+v", summary.MarkPrice.Provenance, summary.Instrument.Provenance)
	}
	wrongSource := metadata
	wrongSource.SourceID = "bybit-v5-linear-public"
	if _, err := ticker.Normalized(wrongSource, page.Instruments[0], page.ObservedTimeMS*int64(1e6), OptionIdentities{InstrumentUID: metadata.InstrumentUID, UnderlyingID: "BTC", IndexID: "BTC-USD"}, optionFixtureUnits(metadata.InstrumentUID)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("option ticker accepted mismatched source identity: %v", err)
	}
	wrongChannel := metadata
	wrongChannel.ChannelID = "tickers.ETH"
	if _, err := ticker.Normalized(wrongChannel, page.Instruments[0], page.ObservedTimeMS*int64(1e6), OptionIdentities{InstrumentUID: metadata.InstrumentUID, UnderlyingID: "BTC", IndexID: "BTC-USD"}, optionFixtureUnits(metadata.InstrumentUID)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("option ticker accepted mismatched channel identity: %v", err)
	}
	identities := OptionIdentities{InstrumentUID: metadata.InstrumentUID, UnderlyingID: "BTC", IndexID: "BTC-USD"}
	for _, test := range []struct {
		name   string
		mutate func(*OptionUnitContract)
	}{
		{name: "OI wrong instrument", mutate: func(units *OptionUnitContract) { units.OpenInterest.InstrumentUID = "other-option" }},
		{name: "volume wrong instrument", mutate: func(units *OptionUnitContract) { units.Volume.InstrumentUID = "other-option" }},
		{name: "OI non-contract unit", mutate: func(units *OptionUnitContract) {
			units.OpenInterest = normalize.NativeUnit{Kind: normalize.NativeUnitBaseAsset, AssetID: "BTC"}
		}},
		{name: "volume non-contract unit", mutate: func(units *OptionUnitContract) {
			units.Volume = normalize.NativeUnit{Kind: normalize.NativeUnitBaseAsset, AssetID: "BTC"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			units := optionFixtureUnits(metadata.InstrumentUID)
			test.mutate(&units)
			if _, err := ticker.Normalized(metadata, page.Instruments[0], page.ObservedTimeMS*int64(1e6), identities, units); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("cross-instrument/non-contract quantity unit accepted: %v", err)
			}
		})
	}
}

func TestBybitOptionOfflineVerifierConsumesEveryBoundFixture(t *testing.T) {
	root := filepath.Join("..", "testdata", "bybit", "option")
	fromRoot, err := VerifyOptionFixtures(root)
	if err != nil {
		t.Fatal(err)
	}
	fromManifest, err := VerifyOptionFixtures(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if fromRoot.EvidenceSHA256 != fromManifest.EvidenceSHA256 || fromRoot.ManifestSHA256 != fromManifest.ManifestSHA256 {
		t.Fatalf("root/manifest verifier evidence differs: root=%+v manifest=%+v", fromRoot, fromManifest)
	}
	if fromRoot.Version != OptionEvidenceVersion || fromRoot.FixtureCount != 9 || fromRoot.OfficialDerivedCount != 4 || fromRoot.SyntheticCount != 5 || len(fromRoot.EvidenceSHA256) != 64 {
		t.Fatalf("unexpected option evidence: %+v", fromRoot)
	}
	cited := make(map[string]bool)
	for _, check := range fromRoot.Checks {
		for _, id := range check.FixtureIDs {
			cited[id] = true
		}
	}
	var manifest FixtureManifest
	manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil || json.Unmarshal(manifestBytes, &manifest) != nil {
		t.Fatal("read option fixture manifest")
	}
	for _, fixture := range manifest.Fixtures {
		if !cited[fixture.ID] {
			t.Fatalf("fixture %s was not semantically cited", fixture.ID)
		}
	}
}

func TestBybitOptionVerifierBindsExactFixtureManifestIdentities(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *FixtureManifest)
	}{
		{name: "ID", mutate: func(t *testing.T, manifest *FixtureManifest) {
			optionFixtureEntry(t, manifest, "option-trade").ID = "renamed-option-trade"
		}},
		{name: "role", mutate: func(t *testing.T, manifest *FixtureManifest) {
			optionFixtureEntry(t, manifest, "option-trade").Role = "renamed_option_trade"
		}},
		{name: "classification", mutate: func(t *testing.T, manifest *FixtureManifest) {
			optionFixtureEntry(t, manifest, "option-trade").Classification = "official_other_projection"
		}},
		{name: "digest-valid file substitution", mutate: func(t *testing.T, manifest *FixtureManifest) {
			entry := optionFixtureEntry(t, manifest, "option-trade")
			substitute := optionFixtureEntry(t, manifest, "option-ticker")
			entry.File = substitute.File
			entry.ByteLength = substitute.ByteLength
			entry.SHA256 = substitute.SHA256
		}},
		{name: "source URL", mutate: func(t *testing.T, manifest *FixtureManifest) {
			optionFixtureEntry(t, manifest, "option-trade").SourceURL = OptionTickerDocumentationURI
		}},
		{name: "official source section", mutate: func(t *testing.T, manifest *FixtureManifest) {
			optionFixtureEntry(t, manifest, "option-trade").SourceSection = "Different nonempty source section"
		}},
		{name: "synthetic derivation", mutate: func(t *testing.T, manifest *FixtureManifest) {
			optionFixtureEntry(t, manifest, "minimum-depth").DerivedFrom = "different nonempty synthetic derivation"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, manifest := copyOptionFixtureSet(t)
			test.mutate(t, &manifest)
			writeOptionFixtureManifest(t, root, manifest)
			if _, err := VerifyOptionFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
				t.Fatalf("manifest %s substitution accepted: %v", test.name, err)
			}
		})
	}
}

func TestBybitOptionVerifierRejectsSemanticMutationAndSymlinkEscape(t *testing.T) {
	t.Run("semantic mutation with rebound digest", func(t *testing.T) {
		root, manifest := copyOptionFixtureSet(t)
		entry := optionFixtureEntry(t, &manifest, "option-ticker")
		path := filepath.Join(root, filepath.FromSlash(entry.File))
		payload, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		mutated := bytes.Replace(payload, []byte(`"delta": "-0.9876"`), []byte(`"delta": "-0.5000"`), 1)
		if bytes.Equal(mutated, payload) {
			t.Fatal("ticker semantic mutation did not match")
		}
		if err := os.WriteFile(path, mutated, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(mutated)
		entry.ByteLength = uint32(len(mutated))
		entry.SHA256 = hex.EncodeToString(digest[:])
		writeOptionFixtureManifest(t, root, manifest)
		if _, err := VerifyOptionFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
			t.Fatalf("semantic mutation accepted: %v", err)
		}
	})
	t.Run("intermediate symlink escape", func(t *testing.T) {
		root, manifest := copyOptionFixtureSet(t)
		entry := optionFixtureEntry(t, &manifest, "option-trade")
		path := filepath.Join(root, filepath.FromSlash(entry.File))
		outside := t.TempDir()
		outsideFile := filepath.Join(outside, "trade.json")
		payload, err := os.ReadFile(path)
		if err != nil || os.WriteFile(outsideFile, payload, 0o600) != nil || os.Remove(path) != nil {
			t.Fatal("prepare symlink escape")
		}
		if err := os.Symlink(outsideFile, path); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := VerifyOptionFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
			t.Fatalf("symlink escape accepted: %v", err)
		}
	})
}

func optionFixture(t *testing.T, relative string) []byte {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("..", "testdata", "bybit", "option", filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func copyOptionFixtureSet(t *testing.T) (string, FixtureManifest) {
	t.Helper()
	source := filepath.Join("..", "testdata", "bybit", "option")
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest FixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	return root, manifest
}

func optionFixtureEntry(t *testing.T, manifest *FixtureManifest, id string) *FixtureEntry {
	t.Helper()
	for i := range manifest.Fixtures {
		if manifest.Fixtures[i].ID == id {
			return &manifest.Fixtures[i]
		}
	}
	t.Fatalf("fixture %s not found", id)
	return nil
}

func writeOptionFixtureManifest(t *testing.T, root string, manifest FixtureManifest) {
	t.Helper()
	payload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
