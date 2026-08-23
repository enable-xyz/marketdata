package normalize

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/enable-xyz/marketdata/catalog"
)

var ErrInvalidCatalogSnapshot = errors.New("normalize: invalid catalog snapshot")

type InstrumentIdentity struct {
	InstrumentUID string
	NativeID      string
	BaseAssetID   string
	QuoteAssetID  string
}

type CatalogView struct {
	version     uint16
	id          Hash
	sourceID    string
	instruments map[string]InstrumentIdentity
}

type mapperCatalogDocument struct {
	Version uint16 `json:"version"`
	Source  struct {
		SourceID string `json:"source_id"`
	} `json:"source"`
	Instruments []struct {
		InstrumentUID string   `json:"instrument_uid"`
		NativeID      string   `json:"native_id"`
		Aliases       []string `json:"aliases"`
		BaseAsset     string   `json:"base_asset"`
		QuoteAsset    string   `json:"quote_asset"`
	} `json:"instruments"`
}

func NewCatalogView(snapshot catalog.Snapshot) (*CatalogView, error) {
	if snapshot.Version != catalog.SnapshotVersion || snapshot.Version == 0 ||
		len(snapshot.Bytes) == 0 || len(snapshot.Bytes) > catalog.MaxCatalogJSONBytes ||
		sha256.Sum256(snapshot.Bytes) != snapshot.SHA256 {
		return nil, fmt.Errorf("%w: identity or byte bound", ErrInvalidCatalogSnapshot)
	}
	var document mapperCatalogDocument
	if err := json.Unmarshal(snapshot.Bytes, &document); err != nil {
		return nil, fmt.Errorf("%w: malformed snapshot bytes", ErrInvalidCatalogSnapshot)
	}
	if document.Version != snapshot.Version || document.Source.SourceID == "" ||
		len(document.Instruments) != snapshot.InstrumentCount || len(document.Instruments) > catalog.MaxCatalogInstruments {
		return nil, fmt.Errorf("%w: document identity or count mismatch", ErrInvalidCatalogSnapshot)
	}
	view := &CatalogView{
		version: snapshot.Version, id: Hash(snapshot.SHA256), sourceID: document.Source.SourceID,
		instruments: make(map[string]InstrumentIdentity, len(document.Instruments)*2),
	}
	for _, value := range document.Instruments {
		identity := InstrumentIdentity{
			InstrumentUID: value.InstrumentUID, NativeID: value.NativeID,
			BaseAssetID: value.BaseAsset, QuoteAssetID: value.QuoteAsset,
		}
		if identity.InstrumentUID == "" || identity.NativeID == "" || identity.BaseAssetID == "" ||
			identity.QuoteAssetID == "" || identity.BaseAssetID == identity.QuoteAssetID ||
			len(value.Aliases) > catalog.MaxCatalogAliases {
			return nil, fmt.Errorf("%w: incomplete instrument identity", ErrInvalidCatalogSnapshot)
		}
		keys := append(slices.Clone(value.Aliases), identity.NativeID)
		for _, key := range keys {
			if key == "" || len(key) > catalog.MaxCatalogStringBytes {
				return nil, fmt.Errorf("%w: invalid instrument alias", ErrInvalidCatalogSnapshot)
			}
			if existing, ok := view.instruments[key]; ok && existing.InstrumentUID != identity.InstrumentUID {
				return nil, fmt.Errorf("%w: ambiguous instrument alias", ErrInvalidCatalogSnapshot)
			}
			view.instruments[key] = identity
		}
	}
	return view, nil
}

func (v *CatalogView) SnapshotID() Hash {
	if v == nil {
		return Hash{}
	}
	return v.id
}

func (v *CatalogView) SourceID() string {
	if v == nil {
		return ""
	}
	return v.sourceID
}

func (v *CatalogView) Lookup(sourceID, nativeSymbol string) (InstrumentIdentity, bool) {
	if v == nil || sourceID != v.sourceID || nativeSymbol == "" {
		return InstrumentIdentity{}, false
	}
	identity, ok := v.instruments[nativeSymbol]
	return identity, ok
}
