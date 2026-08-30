package binance

import (
	"encoding/json"

	"github.com/enable-xyz/marketdata/catalog"
)

const (
	SpotSourceID                = "7d1e8644-35af-5fd7-b0ba-3bf1b59ac6fc"
	SpotExchangeInfoChannel     = "rest.exchangeInfo"
	SpotRESTCommit              = "976cc580553890e92031b77306147c0ed1de5a46"
	SpotRESTPath                = "/api/v3/exchangeInfo"
	SpotRESTDocumentationURI    = "https://github.com/binance/binance-spot-api-docs/blob/" + SpotRESTCommit + "/rest-api.md#exchange-information"
	SpotFiltersDocumentationURI = "https://github.com/binance/binance-spot-api-docs/blob/" + SpotRESTCommit + "/filters.md"
)

func SpotCatalogContract() (catalog.Source, catalog.SourceVersion, []catalog.ChannelContract) {
	source := catalog.Source{
		SourceID:      SpotSourceID,
		Venue:         "binance",
		ProductFamily: "spot",
		APIFamily:     "spot-rest-v3",
		Environment:   "production-public",
		Lifecycle:     "active",
	}
	version := catalog.SourceVersion{
		OfficialAPIVersion: "Binance Spot REST API v3",
		DocumentationURI:   SpotRESTDocumentationURI,
		Endpoints: mustJSON(map[string]any{
			"market_data_base":   "https://data-api.binance.vision",
			"exchange_info_path": SpotRESTPath,
		}),
		Topology: mustJSON(map[string]any{
			"transport":                             "REST",
			"official_response_pagination":          "single_response",
			"adapter_page_composition":              "bounded_ordered_pages",
			"adapter_page_composition_is_inference": true,
		}),
		Entitlement: mustJSON(map[string]any{
			"security_type":        "NONE",
			"credentials_required": false,
		}),
		Region: "global-public",
		RateContract: mustJSON(map[string]any{
			"request_weight":  20,
			"data_source":     "Memory",
			"contract_commit": SpotRESTCommit,
		}),
		HeartbeatPolicy:       mustJSON(map[string]any{"applicable": false}),
		AcknowledgementPolicy: mustJSON(map[string]any{"applicable": false}),
		ReconnectPolicy: mustJSON(map[string]any{
			"applicable":                         false,
			"retries_reserved_for_later_runtime": true,
		}),
	}
	channel := catalog.ChannelContract{
		ChannelID: SpotExchangeInfoChannel,
		NativeSelector: mustJSON(map[string]any{
			"method":          "GET",
			"path":            SpotRESTPath,
			"query_parameter": "symbols",
			"scope":           "exact caller-declared symbol selection",
			"security_type":   "NONE",
		}),
		Role:          "instrument_metadata",
		DataFamily:    "catalog",
		CadenceSource: "caller-scheduled REST opportunity",
		Aggregation: mustJSON(map[string]any{
			"kind":                               "complete_configured_symbol_selection",
			"temporary_absence_closes_lifecycle": false,
		}),
		Depth:         mustJSON(map[string]any{"applicable": false}),
		SequenceRules: mustJSON(map[string]any{"server_time_identity_must_match_across_composed_pages": true}),
		ChecksumRules: mustJSON(map[string]any{"raw_payload": "sha256", "snapshot": "sha256"}),
		PayloadSchema: mustJSON(map[string]any{
			"documentation":          SpotRESTDocumentationURI,
			"filters_documentation":  SpotFiltersDocumentationURI,
			"commit":                 SpotRESTCommit,
			"unknown_fields":         "preserve_in_raw_metadata",
			"decimal_representation": "exact_JSON_string",
		}),
		SupportState: "supported",
		Limitation:   "exchangeInfo has no documented pagination; live collection captures one exact caller-declared symbols query whose response must match that selection",
	}
	return source, version, []catalog.ChannelContract{channel}
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
