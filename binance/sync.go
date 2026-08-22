package binance

import (
	"fmt"
	"time"

	"github.com/enable-xyz/marketdata/catalog"
)

func SpotSyncInput(composed ComposedExchangeInfo) (catalog.SyncInput, error) {
	source, version, channels := SpotCatalogContract()
	input := catalog.SyncInput{
		ObservedAt:    time.Unix(0, composed.CompletedAtNS).UTC(),
		Source:        source,
		SourceVersion: version,
		Channels:      channels,
		Instruments:   composed.Candidates,
		Pages:         composed.Evidence,
	}
	if err := input.Validate(); err != nil {
		return catalog.SyncInput{}, fmt.Errorf("binance: build Spot catalog sync input: %w", err)
	}
	return input, nil
}
