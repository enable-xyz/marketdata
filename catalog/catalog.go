package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	SnapshotVersion       uint16 = 1
	MaxCatalogJSONBytes          = 16 << 20
	MaxCatalogChannels           = 256
	MaxCatalogInstruments        = 100_000
	MaxCatalogAliases            = 32
	MaxCatalogPages              = 64
	MaxCatalogStringBytes        = 4096
	MaxCatalogJSONDepth          = 32
)

var ErrInvalidCatalog = errors.New("catalog: invalid catalog input")

const (
	RawEvidenceCommitted          = "committed_raw_segment"
	RawEvidenceInMemoryProjection = "in_memory_fixture_projection"
)

type Source struct {
	SourceID      string
	Venue         string
	ProductFamily string
	APIFamily     string
	Environment   string
	Lifecycle     string
}

type SourceVersion struct {
	OfficialAPIVersion    string
	DocumentationURI      string
	Endpoints             json.RawMessage
	Topology              json.RawMessage
	Entitlement           json.RawMessage
	Region                string
	RateContract          json.RawMessage
	HeartbeatPolicy       json.RawMessage
	AcknowledgementPolicy json.RawMessage
	ReconnectPolicy       json.RawMessage
}

type ChannelContract struct {
	ChannelID      string
	NativeSelector json.RawMessage
	Role           string
	DataFamily     string
	CadenceSource  string
	Aggregation    json.RawMessage
	Depth          json.RawMessage
	SequenceRules  json.RawMessage
	ChecksumRules  json.RawMessage
	PayloadSchema  json.RawMessage
	SupportState   string
	Limitation     string
}

type InstrumentCandidate struct {
	NativeID                string
	Aliases                 []string
	Lifecycle               string
	LifecycleClosure        bool
	BaseAsset               string
	QuoteAsset              string
	MarginAsset             string
	SettlementAsset         string
	Kind                    string
	Payoff                  json.RawMessage
	Multiplier              string
	TickRules               json.RawMessage
	LotRules                json.RawMessage
	RawMetadata             json.RawMessage
	RawMetadataSHA256       [sha256.Size]byte
	NormalizedSchemaVersion string
}

type RawRecordCoordinate struct {
	EvidenceScope        string `json:"evidence_scope"`
	RawSegmentID         string `json:"raw_segment_id"`
	ObjectKey            string `json:"object_key"`
	RawSegmentSHA256     string `json:"raw_segment_sha256"`
	RawSegmentByteLength int64  `json:"raw_segment_byte_length"`
	PollCycleID          string `json:"poll_cycle_id"`
	ArrivalOrdinal       uint64 `json:"arrival_ordinal"`
	MessageOrdinal       uint32 `json:"message_ordinal"`
	EnvelopeVersion      uint16 `json:"envelope_version"`
}

type SyncPageEvidence struct {
	PageIndex        int                 `json:"page_index"`
	PageCount        int                 `json:"page_count"`
	ChannelID        string              `json:"channel_id"`
	RawRecord        RawRecordCoordinate `json:"raw_record"`
	RequestEvidence  json.RawMessage     `json:"request_evidence"`
	ResponseEvidence json.RawMessage     `json:"response_evidence"`
	RawSHA256        string              `json:"raw_sha256"`
	RawByteLength    int                 `json:"raw_byte_length"`
}

type SyncInput struct {
	ObservedAt    time.Time
	Source        Source
	SourceVersion SourceVersion
	Channels      []ChannelContract
	Instruments   []InstrumentCandidate
	Pages         []SyncPageEvidence
}

type SnapshotInstrument struct {
	InstrumentUID           string          `json:"instrument_uid"`
	NativeID                string          `json:"native_id"`
	ListingGeneration       int64           `json:"listing_generation"`
	Aliases                 []string        `json:"aliases"`
	Lifecycle               string          `json:"lifecycle"`
	BaseAsset               string          `json:"base_asset"`
	QuoteAsset              string          `json:"quote_asset"`
	MarginAsset             string          `json:"margin_asset,omitempty"`
	SettlementAsset         string          `json:"settlement_asset,omitempty"`
	Kind                    string          `json:"kind"`
	Payoff                  json.RawMessage `json:"payoff"`
	Multiplier              string          `json:"multiplier"`
	TickRules               json.RawMessage `json:"tick_rules"`
	LotRules                json.RawMessage `json:"lot_rules"`
	RawMetadataSHA256       string          `json:"raw_metadata_sha256"`
	NormalizedSchemaVersion string          `json:"normalized_schema_version"`
}

type SnapshotInput struct {
	Source        Source
	SourceVersion SourceVersion
	Channels      []ChannelContract
	Instruments   []SnapshotInstrument
}

type Snapshot struct {
	Version         uint16
	SHA256          [sha256.Size]byte
	Bytes           []byte
	InstrumentCount int
}

type snapshotDocument struct {
	Version       uint16                `json:"version"`
	Source        snapshotSource        `json:"source"`
	SourceVersion snapshotSourceVersion `json:"source_version"`
	Channels      []snapshotChannel     `json:"channels"`
	Instruments   []SnapshotInstrument  `json:"instruments"`
}

type snapshotSource struct {
	SourceID      string `json:"source_id"`
	Venue         string `json:"venue"`
	ProductFamily string `json:"product_family"`
	APIFamily     string `json:"api_family"`
	Environment   string `json:"environment"`
	Lifecycle     string `json:"lifecycle"`
}

type snapshotSourceVersion struct {
	OfficialAPIVersion    string          `json:"official_api_version"`
	DocumentationURI      string          `json:"documentation_uri"`
	Endpoints             json.RawMessage `json:"endpoints"`
	Topology              json.RawMessage `json:"topology"`
	Entitlement           json.RawMessage `json:"entitlement"`
	Region                string          `json:"region"`
	RateContract          json.RawMessage `json:"rate_contract"`
	HeartbeatPolicy       json.RawMessage `json:"heartbeat_policy"`
	AcknowledgementPolicy json.RawMessage `json:"acknowledgement_policy"`
	ReconnectPolicy       json.RawMessage `json:"reconnect_policy"`
}

type snapshotChannel struct {
	ChannelID      string          `json:"channel_id"`
	NativeSelector json.RawMessage `json:"native_selector"`
	Role           string          `json:"role"`
	DataFamily     string          `json:"data_family"`
	CadenceSource  string          `json:"cadence_source"`
	Aggregation    json.RawMessage `json:"aggregation"`
	Depth          json.RawMessage `json:"depth"`
	SequenceRules  json.RawMessage `json:"sequence_rules"`
	ChecksumRules  json.RawMessage `json:"checksum_rules"`
	PayloadSchema  json.RawMessage `json:"payload_schema"`
	SupportState   string          `json:"support_state"`
	Limitation     string          `json:"limitation,omitempty"`
}

func (s Source) Validate() error {
	if !validUUID(s.SourceID) {
		return fmt.Errorf("%w: source_id must be a canonical UUID", ErrInvalidCatalog)
	}
	for name, value := range map[string]string{
		"venue": s.Venue, "product_family": s.ProductFamily, "api_family": s.APIFamily,
		"environment": s.Environment,
	} {
		if err := validateCatalogText(name, value); err != nil {
			return err
		}
	}
	if !slices.Contains([]string{"active", "degraded", "disabled", "retired", "quarantined"}, s.Lifecycle) {
		return fmt.Errorf("%w: unsupported source lifecycle %q", ErrInvalidCatalog, s.Lifecycle)
	}
	return nil
}

func (v SourceVersion) Validate() error {
	for name, value := range map[string]string{
		"official_api_version": v.OfficialAPIVersion, "documentation_uri": v.DocumentationURI, "region": v.Region,
	} {
		if err := validateCatalogText(name, value); err != nil {
			return err
		}
	}
	for name, raw := range map[string]json.RawMessage{
		"endpoints": v.Endpoints, "topology": v.Topology, "entitlement": v.Entitlement,
		"rate_contract": v.RateContract, "heartbeat_policy": v.HeartbeatPolicy,
		"acknowledgement_policy": v.AcknowledgementPolicy, "reconnect_policy": v.ReconnectPolicy,
	} {
		if err := validateJSONObject(name, raw); err != nil {
			return err
		}
	}
	return nil
}

func (c ChannelContract) Validate() error {
	for name, value := range map[string]string{
		"channel_id": c.ChannelID, "role": c.Role, "data_family": c.DataFamily, "cadence_source": c.CadenceSource,
	} {
		if err := validateCatalogText(name, value); err != nil {
			return err
		}
	}
	if c.Limitation != "" {
		if err := validateCatalogText("limitation", c.Limitation); err != nil {
			return err
		}
	}
	if !slices.Contains([]string{"supported", "limited", "unsupported", "quarantined"}, c.SupportState) {
		return fmt.Errorf("%w: unsupported channel state %q", ErrInvalidCatalog, c.SupportState)
	}
	if !json.Valid(c.NativeSelector) || len(c.NativeSelector) == 0 || len(c.NativeSelector) > MaxCatalogJSONBytes {
		return fmt.Errorf("%w: native_selector is invalid JSON", ErrInvalidCatalog)
	}
	for name, raw := range map[string]json.RawMessage{
		"aggregation": c.Aggregation, "depth": c.Depth, "sequence_rules": c.SequenceRules,
		"checksum_rules": c.ChecksumRules, "payload_schema": c.PayloadSchema,
	} {
		if err := validateJSONObject(name, raw); err != nil {
			return err
		}
	}
	return nil
}

func (c InstrumentCandidate) Validate() error {
	if err := validateCatalogText("native_id", c.NativeID); err != nil {
		return err
	}
	if len(c.Aliases) == 0 || len(c.Aliases) > MaxCatalogAliases || !slices.IsSorted(c.Aliases) {
		return fmt.Errorf("%w: aliases must be non-empty, bounded, and sorted", ErrInvalidCatalog)
	}
	for i, alias := range c.Aliases {
		if err := validateCatalogText("alias", alias); err != nil {
			return err
		}
		if i > 0 && alias == c.Aliases[i-1] {
			return fmt.Errorf("%w: duplicate alias %q", ErrInvalidCatalog, alias)
		}
	}
	if !slices.Contains(c.Aliases, c.NativeID) {
		return fmt.Errorf("%w: native_id must be an alias", ErrInvalidCatalog)
	}
	if !slices.Contains([]string{"active", "degraded", "disabled", "retired", "quarantined"}, c.Lifecycle) {
		return fmt.Errorf("%w: unsupported instrument lifecycle %q", ErrInvalidCatalog, c.Lifecycle)
	}
	for name, value := range map[string]string{
		"base_asset": c.BaseAsset, "quote_asset": c.QuoteAsset, "settlement_asset": c.SettlementAsset,
		"kind": c.Kind, "normalized_schema_version": c.NormalizedSchemaVersion,
	} {
		if err := validateCatalogText(name, value); err != nil {
			return err
		}
	}
	if c.MarginAsset != "" {
		if err := validateCatalogText("margin_asset", c.MarginAsset); err != nil {
			return err
		}
	}
	if !validPositiveDecimal(c.Multiplier) {
		return fmt.Errorf("%w: multiplier must be an exact positive decimal string", ErrInvalidCatalog)
	}
	for name, raw := range map[string]json.RawMessage{
		"payoff": c.Payoff, "tick_rules": c.TickRules, "lot_rules": c.LotRules, "raw_metadata": c.RawMetadata,
	} {
		if err := validateJSONObject(name, raw); err != nil {
			return err
		}
	}
	canonical, err := CanonicalJSON(c.RawMetadata)
	if err != nil {
		return fmt.Errorf("%w: raw_metadata: %v", ErrInvalidCatalog, err)
	}
	if sha256.Sum256(canonical) != c.RawMetadataSHA256 {
		return fmt.Errorf("%w: raw_metadata hash mismatch", ErrInvalidCatalog)
	}
	return nil
}
func (c RawRecordCoordinate) Validate() error {
	if !slices.Contains([]string{RawEvidenceCommitted, RawEvidenceInMemoryProjection}, c.EvidenceScope) {
		return fmt.Errorf("%w: raw record evidence scope %q is unsupported", ErrInvalidCatalog, c.EvidenceScope)
	}
	if !validUUID(c.RawSegmentID) || !validUUID(c.PollCycleID) {
		return fmt.Errorf("%w: raw record segment and poll-cycle identities must be canonical UUIDs", ErrInvalidCatalog)
	}
	if err := validateCatalogText("raw_record.object_key", c.ObjectKey); err != nil {
		return err
	}
	if c.EvidenceScope == RawEvidenceInMemoryProjection && !strings.HasPrefix(c.ObjectKey, "in-memory://fixture-projection/") {
		return fmt.Errorf("%w: in-memory raw evidence must use the fixture projection object namespace", ErrInvalidCatalog)
	}
	if c.EvidenceScope == RawEvidenceCommitted && strings.HasPrefix(c.ObjectKey, "in-memory://") {
		return fmt.Errorf("%w: committed raw evidence cannot use an in-memory object key", ErrInvalidCatalog)
	}
	decoded, err := hex.DecodeString(c.RawSegmentSHA256)
	if err != nil || len(decoded) != sha256.Size || c.RawSegmentSHA256 != strings.ToLower(c.RawSegmentSHA256) {
		return fmt.Errorf("%w: invalid raw segment SHA-256", ErrInvalidCatalog)
	}
	if c.RawSegmentByteLength < 1 {
		return fmt.Errorf("%w: invalid raw segment byte length", ErrInvalidCatalog)
	}
	if c.ArrivalOrdinal == 0 || c.ArrivalOrdinal > uint64(1<<63-1) {
		return fmt.Errorf("%w: raw record arrival ordinal is outside PostgreSQL bigint bounds", ErrInvalidCatalog)
	}
	if c.MessageOrdinal > uint32(1<<31-1) {
		return fmt.Errorf("%w: raw record message ordinal is outside PostgreSQL integer bounds", ErrInvalidCatalog)
	}
	if c.EnvelopeVersion != 1 {
		return fmt.Errorf("%w: raw record envelope version %d is unsupported", ErrInvalidCatalog, c.EnvelopeVersion)
	}
	return nil
}

func (e SyncPageEvidence) Validate() error {
	if e.PageCount < 1 || e.PageCount > MaxCatalogPages || e.PageIndex < 0 || e.PageIndex >= e.PageCount {
		return fmt.Errorf("%w: invalid page identity %d/%d", ErrInvalidCatalog, e.PageIndex, e.PageCount)
	}
	if err := validateCatalogText("page.channel_id", e.ChannelID); err != nil {
		return err
	}
	if e.RawByteLength < 1 || e.RawByteLength > MaxCatalogJSONBytes {
		return fmt.Errorf("%w: invalid raw page byte length", ErrInvalidCatalog)
	}
	if err := e.RawRecord.Validate(); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(e.RawSHA256)
	if err != nil || len(decoded) != sha256.Size || e.RawSHA256 != strings.ToLower(e.RawSHA256) {
		return fmt.Errorf("%w: invalid raw page SHA-256", ErrInvalidCatalog)
	}
	if err := validateJSONObject("request_evidence", e.RequestEvidence); err != nil {
		return err
	}
	if err := validateJSONObject("response_evidence", e.ResponseEvidence); err != nil {
		return err
	}
	return nil
}

func (in SyncInput) Validate() error {
	if in.ObservedAt.IsZero() || in.ObservedAt.Location() != time.UTC || in.ObservedAt.Nanosecond()%1000 != 0 {
		return fmt.Errorf("%w: observed_at must be explicit UTC with PostgreSQL microsecond precision", ErrInvalidCatalog)
	}
	if err := in.Source.Validate(); err != nil {
		return err
	}
	if err := in.SourceVersion.Validate(); err != nil {
		return err
	}
	if len(in.Channels) == 0 || len(in.Channels) > MaxCatalogChannels {
		return fmt.Errorf("%w: channel count is invalid", ErrInvalidCatalog)
	}
	channelIDs := make(map[string]struct{}, len(in.Channels))
	for _, channel := range in.Channels {
		if err := channel.Validate(); err != nil {
			return err
		}
		if _, exists := channelIDs[channel.ChannelID]; exists {
			return fmt.Errorf("%w: duplicate channel %q", ErrInvalidCatalog, channel.ChannelID)
		}
		channelIDs[channel.ChannelID] = struct{}{}
	}
	if len(in.Instruments) > MaxCatalogInstruments {
		return fmt.Errorf("%w: instrument count exceeds %d", ErrInvalidCatalog, MaxCatalogInstruments)
	}
	nativeIDs := make(map[string]struct{}, len(in.Instruments))
	aliases := make(map[string]string, len(in.Instruments))
	for _, instrument := range in.Instruments {
		if err := instrument.Validate(); err != nil {
			return err
		}
		if _, exists := nativeIDs[instrument.NativeID]; exists {
			return fmt.Errorf("%w: duplicate native_id %q", ErrInvalidCatalog, instrument.NativeID)
		}
		nativeIDs[instrument.NativeID] = struct{}{}
		for _, alias := range instrument.Aliases {
			if owner, exists := aliases[alias]; exists {
				return fmt.Errorf("%w: alias %q belongs to both %q and %q", ErrInvalidCatalog, alias, owner, instrument.NativeID)
			}
			aliases[alias] = instrument.NativeID
		}
	}
	if len(in.Pages) == 0 || len(in.Pages) > MaxCatalogPages {
		return fmt.Errorf("%w: page evidence count is invalid", ErrInvalidCatalog)
	}
	pageCount := in.Pages[0].PageCount
	seenPages := make(map[int]struct{}, len(in.Pages))
	seenRawRecords := make(map[string]struct{}, len(in.Pages))
	var completionBoundaryNS int64 = -1
	for _, page := range in.Pages {
		if err := page.Validate(); err != nil {
			return err
		}
		if _, exists := channelIDs[page.ChannelID]; !exists {
			return fmt.Errorf("%w: page %d references unknown channel %q", ErrInvalidCatalog, page.PageIndex, page.ChannelID)
		}
		if page.PageCount != pageCount {
			return fmt.Errorf("%w: conflicting page counts", ErrInvalidCatalog)
		}
		if _, exists := seenPages[page.PageIndex]; exists {
			return fmt.Errorf("%w: duplicate page %d", ErrInvalidCatalog, page.PageIndex)
		}
		seenPages[page.PageIndex] = struct{}{}
		rawIdentity := fmt.Sprintf("%s/%d/%d", page.RawRecord.RawSegmentID, page.RawRecord.ArrivalOrdinal, page.RawRecord.MessageOrdinal)
		if _, exists := seenRawRecords[rawIdentity]; exists {
			return fmt.Errorf("%w: duplicate raw record coordinate %q", ErrInvalidCatalog, rawIdentity)
		}
		seenRawRecords[rawIdentity] = struct{}{}
		var response struct {
			Version       uint16 `json:"version"`
			Kind          string `json:"kind"`
			CompletedAtNS int64  `json:"completed_at_ns"`
		}
		if err := json.Unmarshal(page.ResponseEvidence, &response); err != nil ||
			response.Version != 1 || response.Kind != "response" || response.CompletedAtNS < 0 {
			return fmt.Errorf("%w: invalid response completion boundary on page %d", ErrInvalidCatalog, page.PageIndex)
		}
		completionBoundaryNS = max(completionBoundaryNS, response.CompletedAtNS)
	}
	if len(in.Pages) != pageCount {
		return fmt.Errorf("%w: incomplete page set: got %d of %d", ErrInvalidCatalog, len(in.Pages), pageCount)
	}
	if in.ObservedAt.UnixNano() != completionBoundaryNS {
		return fmt.Errorf("%w: observed_at must equal the complete response completion boundary", ErrInvalidCatalog)
	}
	return nil
}

func BuildSnapshot(in SnapshotInput) (Snapshot, error) {
	if err := in.Source.Validate(); err != nil {
		return Snapshot{}, err
	}
	if err := in.SourceVersion.Validate(); err != nil {
		return Snapshot{}, err
	}
	if len(in.Channels) == 0 || len(in.Channels) > MaxCatalogChannels || len(in.Instruments) > MaxCatalogInstruments {
		return Snapshot{}, fmt.Errorf("%w: snapshot collection bounds", ErrInvalidCatalog)
	}
	channels := slices.Clone(in.Channels)
	slices.SortFunc(channels, func(a, b ChannelContract) int { return strings.Compare(a.ChannelID, b.ChannelID) })
	channelRows := make([]snapshotChannel, len(channels))
	for i, channel := range channels {
		if err := channel.Validate(); err != nil {
			return Snapshot{}, err
		}
		if i > 0 && channel.ChannelID == channels[i-1].ChannelID {
			return Snapshot{}, fmt.Errorf("%w: duplicate snapshot channel %q", ErrInvalidCatalog, channel.ChannelID)
		}
		var err error
		channelRows[i], err = canonicalSnapshotChannel(channel)
		if err != nil {
			return Snapshot{}, err
		}
	}
	instruments := slices.Clone(in.Instruments)
	for i := range instruments {
		instruments[i].Aliases = slices.Clone(instruments[i].Aliases)
		slices.Sort(instruments[i].Aliases)
		if !validUUID(instruments[i].InstrumentUID) || instruments[i].ListingGeneration < 0 {
			return Snapshot{}, fmt.Errorf("%w: invalid snapshot instrument identity", ErrInvalidCatalog)
		}
		for _, raw := range []json.RawMessage{instruments[i].Payoff, instruments[i].TickRules, instruments[i].LotRules} {
			if !json.Valid(raw) || len(raw) == 0 || len(raw) > MaxCatalogJSONBytes {
				return Snapshot{}, fmt.Errorf("%w: invalid snapshot instrument JSON", ErrInvalidCatalog)
			}
		}
		var err error
		instruments[i].Payoff, err = CanonicalJSON(instruments[i].Payoff)
		if err != nil {
			return Snapshot{}, err
		}
		instruments[i].TickRules, err = CanonicalJSON(instruments[i].TickRules)
		if err != nil {
			return Snapshot{}, err
		}
		instruments[i].LotRules, err = CanonicalJSON(instruments[i].LotRules)
		if err != nil {
			return Snapshot{}, err
		}
	}
	slices.SortFunc(instruments, func(a, b SnapshotInstrument) int {
		if n := strings.Compare(a.NativeID, b.NativeID); n != 0 {
			return n
		}
		if a.ListingGeneration < b.ListingGeneration {
			return -1
		}
		if a.ListingGeneration > b.ListingGeneration {
			return 1
		}
		return strings.Compare(a.InstrumentUID, b.InstrumentUID)
	})
	for i := 1; i < len(instruments); i++ {
		if instruments[i].InstrumentUID == instruments[i-1].InstrumentUID {
			return Snapshot{}, fmt.Errorf("%w: duplicate snapshot instrument UID", ErrInvalidCatalog)
		}
	}
	sourceVersion, err := canonicalSnapshotSourceVersion(in.SourceVersion)
	if err != nil {
		return Snapshot{}, err
	}
	document := snapshotDocument{
		Version: SnapshotVersion,
		Source: snapshotSource{
			SourceID: in.Source.SourceID, Venue: in.Source.Venue, ProductFamily: in.Source.ProductFamily,
			APIFamily: in.Source.APIFamily, Environment: in.Source.Environment, Lifecycle: in.Source.Lifecycle,
		},
		SourceVersion: sourceVersion,
		Channels:      channelRows,
		Instruments:   instruments,
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: encode snapshot: %w", err)
	}
	if len(encoded) > MaxCatalogJSONBytes {
		return Snapshot{}, fmt.Errorf("%w: snapshot exceeds byte bound", ErrInvalidCatalog)
	}
	return Snapshot{Version: SnapshotVersion, SHA256: sha256.Sum256(encoded), Bytes: encoded, InstrumentCount: len(instruments)}, nil
}

func BuildFreshSnapshot(source Source, version SourceVersion, channels []ChannelContract, candidates []InstrumentCandidate) (Snapshot, error) {
	if len(candidates) > MaxCatalogInstruments {
		return Snapshot{}, fmt.Errorf("%w: fresh snapshot instrument count exceeds bounds", ErrInvalidCatalog)
	}
	instruments := make([]SnapshotInstrument, 0, len(candidates))
	for _, candidate := range candidates {
		if err := candidate.Validate(); err != nil {
			return Snapshot{}, err
		}
		if candidate.LifecycleClosure {
			return Snapshot{}, fmt.Errorf("%w: a fresh snapshot cannot contain lifecycle closures", ErrInvalidCatalog)
		}
		instruments = append(instruments, SnapshotInstrument{
			InstrumentUID:           deterministicInstrumentUID(source.SourceID, candidate.NativeID, 0),
			NativeID:                candidate.NativeID,
			ListingGeneration:       0,
			Aliases:                 slices.Clone(candidate.Aliases),
			Lifecycle:               candidate.Lifecycle,
			BaseAsset:               candidate.BaseAsset,
			QuoteAsset:              candidate.QuoteAsset,
			MarginAsset:             candidate.MarginAsset,
			SettlementAsset:         candidate.SettlementAsset,
			Kind:                    candidate.Kind,
			Payoff:                  slices.Clone(candidate.Payoff),
			Multiplier:              candidate.Multiplier,
			TickRules:               slices.Clone(candidate.TickRules),
			LotRules:                slices.Clone(candidate.LotRules),
			RawMetadataSHA256:       HashHex(candidate.RawMetadataSHA256),
			NormalizedSchemaVersion: candidate.NormalizedSchemaVersion,
		})
	}
	return BuildSnapshot(SnapshotInput{
		Source:        source,
		SourceVersion: version,
		Channels:      slices.Clone(channels),
		Instruments:   instruments,
	})
}

func CanonicalJSON(raw []byte) ([]byte, error) {
	if len(raw) == 0 || len(raw) > MaxCatalogJSONBytes || !json.Valid(raw) {
		return nil, fmt.Errorf("%w: invalid or oversized JSON", ErrInvalidCatalog)
	}
	return canonicalJSONValue(bytes.TrimSpace(raw), 0)
}

func canonicalJSONValue(raw []byte, depth int) ([]byte, error) {
	if depth > MaxCatalogJSONDepth {
		return nil, fmt.Errorf("%w: JSON nesting exceeds %d", ErrInvalidCatalog, MaxCatalogJSONDepth)
	}
	switch raw[0] {
	case '{':
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, err
		}
		keys := stringsMapKeys(object)
		slices.Sort(keys)
		var out bytes.Buffer
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			encodedKey, _ := json.Marshal(key)
			out.Write(encodedKey)
			out.WriteByte(':')
			value, err := canonicalJSONValue(bytes.TrimSpace(object[key]), depth+1)
			if err != nil {
				return nil, err
			}
			out.Write(value)
		}
		out.WriteByte('}')
		return out.Bytes(), nil
	case '[':
		var array []json.RawMessage
		if err := json.Unmarshal(raw, &array); err != nil {
			return nil, err
		}
		var out bytes.Buffer
		out.WriteByte('[')
		for i, item := range array {
			if i > 0 {
				out.WriteByte(',')
			}
			value, err := canonicalJSONValue(bytes.TrimSpace(item), depth+1)
			if err != nil {
				return nil, err
			}
			out.Write(value)
		}
		out.WriteByte(']')
		return out.Bytes(), nil
	default:
		var out bytes.Buffer
		if err := json.Compact(&out, raw); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}
}

func canonicalSnapshotSourceVersion(v SourceVersion) (snapshotSourceVersion, error) {
	values := []*json.RawMessage{&v.Endpoints, &v.Topology, &v.Entitlement, &v.RateContract, &v.HeartbeatPolicy, &v.AcknowledgementPolicy, &v.ReconnectPolicy}
	for _, value := range values {
		canonical, err := CanonicalJSON(*value)
		if err != nil {
			return snapshotSourceVersion{}, err
		}
		*value = canonical
	}
	return snapshotSourceVersion{
		OfficialAPIVersion: v.OfficialAPIVersion, DocumentationURI: v.DocumentationURI,
		Endpoints: v.Endpoints, Topology: v.Topology, Entitlement: v.Entitlement, Region: v.Region,
		RateContract: v.RateContract, HeartbeatPolicy: v.HeartbeatPolicy,
		AcknowledgementPolicy: v.AcknowledgementPolicy, ReconnectPolicy: v.ReconnectPolicy,
	}, nil
}

func canonicalSnapshotChannel(c ChannelContract) (snapshotChannel, error) {
	values := []*json.RawMessage{&c.NativeSelector, &c.Aggregation, &c.Depth, &c.SequenceRules, &c.ChecksumRules, &c.PayloadSchema}
	for _, value := range values {
		canonical, err := CanonicalJSON(*value)
		if err != nil {
			return snapshotChannel{}, err
		}
		*value = canonical
	}
	return snapshotChannel{
		ChannelID: c.ChannelID, NativeSelector: c.NativeSelector, Role: c.Role, DataFamily: c.DataFamily,
		CadenceSource: c.CadenceSource, Aggregation: c.Aggregation, Depth: c.Depth,
		SequenceRules: c.SequenceRules, ChecksumRules: c.ChecksumRules, PayloadSchema: c.PayloadSchema,
		SupportState: c.SupportState, Limitation: c.Limitation,
	}, nil
}

func validateJSONObject(name string, raw []byte) error {
	if len(raw) == 0 || len(raw) > MaxCatalogJSONBytes || !json.Valid(raw) || bytes.TrimSpace(raw)[0] != '{' {
		return fmt.Errorf("%w: %s must be a bounded JSON object", ErrInvalidCatalog, name)
	}
	return nil
}

func validateCatalogText(name, value string) error {
	if value == "" || len(value) > MaxCatalogStringBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: %s is empty, oversized, or invalid UTF-8", ErrInvalidCatalog, name)
	}
	return nil
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

func validPositiveDecimal(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	seenDot := false
	seenNonZero := false
	for i := range len(value) {
		switch c := value[i]; {
		case c >= '0' && c <= '9':
			seenNonZero = seenNonZero || c != '0'
		case c == '.' && !seenDot && i > 0 && i < len(value)-1:
			seenDot = true
		default:
			return false
		}
	}
	return seenNonZero
}

func stringsMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func HashHex(hash [sha256.Size]byte) string {
	return hex.EncodeToString(hash[:])
}
