package catalog

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const HyperliquidIdentityVersion uint16 = 1

const (
	HyperliquidNetworkMainnet = "mainnet"
	HyperliquidNetworkTestnet = "testnet"

	HyperliquidProtocolDeployer = "hyperliquid-protocol"
	HyperliquidMainDEX          = "main"
	HyperliquidSpotDEX          = "spot"
)

type HyperliquidFamily string

const (
	HyperliquidMainPerpetual HyperliquidFamily = "main_perpetual"
	HyperliquidSpot          HyperliquidFamily = "spot"
	HyperliquidHIP3          HyperliquidFamily = "hip3_perpetual"
)

type HyperliquidGenerationEvidence struct {
	EvidenceScope         string            `json:"evidence_scope"`
	SourceID              string            `json:"source_id"`
	EpochID               string            `json:"epoch_id"`
	ArrivalOrdinal        uint64            `json:"arrival_ordinal"`
	MessageOrdinal        uint32            `json:"message_ordinal"`
	GenerationStartNS     int64             `json:"generation_start_ns"`
	RawPayloadSHA256      [sha256.Size]byte `json:"raw_payload_sha256"`
	ContextPayloadSHA256  [sha256.Size]byte `json:"context_payload_sha256,omitempty"`
	EnvelopePayloadSHA256 [sha256.Size]byte `json:"envelope_payload_sha256,omitempty"`
}

func (e HyperliquidGenerationEvidence) Validate() error {
	if e.EvidenceScope != RawEvidenceCommitted && e.EvidenceScope != RawEvidenceInMemoryProjection {
		return fmt.Errorf("%w: invalid Hyperliquid generation evidence scope", ErrInvalidCatalog)
	}
	for _, value := range []string{e.SourceID, e.EpochID} {
		if value == "" || len(value) > 128 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: invalid Hyperliquid generation coordinate", ErrInvalidCatalog)
		}
	}
	if e.ArrivalOrdinal == 0 || e.GenerationStartNS <= 0 || e.RawPayloadSHA256 == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: incomplete Hyperliquid generation coordinate", ErrInvalidCatalog)
	}
	return nil
}

type HyperliquidIdentityInput struct {
	Network            string
	Family             HyperliquidFamily
	DEXName            string
	WireCoin           string
	MetadataGeneration [sha256.Size]byte
	Deployer           string
	CollateralToken    string
	UniverseIndex      uint32
}

// HyperliquidInstrumentIdentity is the immutable namespace for one metadata
// generation. Wire symbols are aliases, never identities: every field below is
// part of InstrumentUID and NativeID so equal suffixes on different DEXs and a
// recycled HIP-3 universe position cannot collide.
type HyperliquidInstrumentIdentity struct {
	Version            uint16
	Network            string
	Family             HyperliquidFamily
	DEXName            string
	WireCoin           string
	MetadataGeneration [sha256.Size]byte
	Deployer           string
	CollateralToken    string
	UniverseIndex      uint32
	InstrumentUID      string
	NativeID           string
}

// HyperliquidMetadataGeneration binds canonical metadata content to the first
// committed raw coordinate of this observed deployment/listing generation.
// Context values are excluded, while recycled identical metadata at a later
// generation start necessarily receives a different digest.
func HyperliquidMetadataGeneration(rawMetadata json.RawMessage, evidence HyperliquidGenerationEvidence) ([sha256.Size]byte, error) {
	if err := evidence.Validate(); err != nil {
		return [sha256.Size]byte{}, err
	}
	if evidence.RawPayloadSHA256 != sha256.Sum256(rawMetadata) {
		return [sha256.Size]byte{}, fmt.Errorf("%w: Hyperliquid metadata evidence digest mismatch", ErrInvalidCatalog)
	}
	canonical, err := CanonicalJSON(rawMetadata)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: Hyperliquid metadata generation: %v", ErrInvalidCatalog, err)
	}
	material, err := json.Marshal(struct {
		Metadata json.RawMessage               `json:"metadata"`
		Evidence HyperliquidGenerationEvidence `json:"generation_evidence"`
	}{Metadata: canonical, Evidence: evidence})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: encode Hyperliquid metadata generation: %v", ErrInvalidCatalog, err)
	}
	return sha256.Sum256(material), nil
}

func NewHyperliquidInstrumentIdentity(in HyperliquidIdentityInput) (HyperliquidInstrumentIdentity, error) {
	identity := HyperliquidInstrumentIdentity{
		Version:            HyperliquidIdentityVersion,
		Network:            in.Network,
		Family:             in.Family,
		DEXName:            in.DEXName,
		WireCoin:           in.WireCoin,
		MetadataGeneration: in.MetadataGeneration,
		Deployer:           in.Deployer,
		CollateralToken:    in.CollateralToken,
		UniverseIndex:      in.UniverseIndex,
	}
	material, err := hyperliquidIdentityMaterial(identity)
	if err != nil {
		return HyperliquidInstrumentIdentity{}, err
	}
	identity.InstrumentUID = deterministicCatalogUUID("catalog-hyperliquid-instrument-v1", string(material))
	identity.NativeID = "hyperliquid:v1:" + base64.RawURLEncoding.EncodeToString(material)
	if err := identity.Validate(); err != nil {
		return HyperliquidInstrumentIdentity{}, err
	}
	return identity, nil
}

func (i HyperliquidInstrumentIdentity) Validate() error {
	if i.Version != HyperliquidIdentityVersion || i.MetadataGeneration == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: incomplete Hyperliquid identity version or generation", ErrInvalidCatalog)
	}
	material, err := hyperliquidIdentityMaterial(i)
	if err != nil {
		return err
	}
	wantUID := deterministicCatalogUUID("catalog-hyperliquid-instrument-v1", string(material))
	wantNativeID := "hyperliquid:v1:" + base64.RawURLEncoding.EncodeToString(material)
	if i.InstrumentUID != wantUID || i.NativeID != wantNativeID {
		return fmt.Errorf("%w: Hyperliquid identity does not match its namespace", ErrInvalidCatalog)
	}
	return nil
}

func (i HyperliquidInstrumentIdentity) GenerationHex() string {
	return hex.EncodeToString(i.MetadataGeneration[:])
}

func hyperliquidIdentityMaterial(i HyperliquidInstrumentIdentity) ([]byte, error) {
	if i.Network != HyperliquidNetworkMainnet && i.Network != HyperliquidNetworkTestnet {
		return nil, fmt.Errorf("%w: unsupported Hyperliquid network %q", ErrInvalidCatalog, i.Network)
	}
	fields := []struct {
		name  string
		value string
	}{
		{name: "dex_name", value: i.DEXName},
		{name: "wire_coin", value: i.WireCoin},
		{name: "deployer", value: i.Deployer},
		{name: "collateral_token", value: i.CollateralToken},
	}
	for _, field := range fields {
		if field.value == "" || len(field.value) > 128 || !utf8.ValidString(field.value) || strings.IndexByte(field.value, 0) >= 0 {
			return nil, fmt.Errorf("%w: invalid Hyperliquid %s", ErrInvalidCatalog, field.name)
		}
	}
	if i.UniverseIndex >= MaxCatalogInstruments {
		return nil, fmt.Errorf("%w: Hyperliquid universe index exceeds catalog bound", ErrInvalidCatalog)
	}
	switch i.Family {
	case HyperliquidMainPerpetual:
		if i.DEXName != HyperliquidMainDEX || i.Deployer != HyperliquidProtocolDeployer || strings.Contains(i.WireCoin, ":") {
			return nil, fmt.Errorf("%w: invalid main-perpetual namespace", ErrInvalidCatalog)
		}
	case HyperliquidSpot:
		if i.DEXName != HyperliquidSpotDEX || i.Deployer != HyperliquidProtocolDeployer {
			return nil, fmt.Errorf("%w: invalid spot namespace", ErrInvalidCatalog)
		}
	case HyperliquidHIP3:
		if i.DEXName == HyperliquidMainDEX || i.DEXName == HyperliquidSpotDEX || i.Deployer == HyperliquidProtocolDeployer ||
			i.Deployer != strings.ToLower(i.Deployer) || !validHyperliquidAddress(i.Deployer) ||
			!strings.HasPrefix(i.WireCoin, i.DEXName+":") || len(i.WireCoin) == len(i.DEXName)+1 {
			return nil, fmt.Errorf("%w: invalid HIP-3 namespace", ErrInvalidCatalog)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported Hyperliquid family %q", ErrInvalidCatalog, i.Family)
	}
	generation := hex.EncodeToString(i.MetadataGeneration[:])
	return json.Marshal(struct {
		Version         uint16            `json:"version"`
		Network         string            `json:"network"`
		Family          HyperliquidFamily `json:"family"`
		DEXName         string            `json:"dex_name"`
		WireCoin        string            `json:"wire_coin"`
		Generation      string            `json:"metadata_generation"`
		Deployer        string            `json:"deployer"`
		CollateralToken string            `json:"collateral_token"`
		UniverseIndex   uint32            `json:"universe_index"`
	}{
		Version: i.Version, Network: i.Network, Family: i.Family, DEXName: i.DEXName,
		WireCoin: i.WireCoin, Generation: generation, Deployer: i.Deployer,
		CollateralToken: i.CollateralToken, UniverseIndex: i.UniverseIndex,
	})
}

func validHyperliquidAddress(value string) bool {
	if len(value) != 42 || !strings.HasPrefix(value, "0x") {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}
