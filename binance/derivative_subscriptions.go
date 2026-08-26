package binance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	DerivativeMaxSymbols               = 7
	DerivativeSubscriptionBatchLimit   = 4
	DerivativeSubscriptionBatchCount   = 10
	DerivativeControlMessagesPerSecond = 10
	DerivativeMaxControlMessageBytes   = 16 << 10
	DerivativeMaxPingPayloadBytes      = 125

	USDMRawPublicChannel = "ws.usdm.public.raw.v1"
	USDMRawMarketChannel = "ws.usdm.market.raw.v1"
	CoinMRawChannel      = "ws.coinm.raw.v1"

	USDMRawDataFamily  = "native-usdm-market-data"
	CoinMRawDataFamily = "native-coinm-market-data"

	USDMPublicSubscriptionRequestID = "usdm-public-subscribe-v1"
	USDMMarketSubscriptionRequestID = "usdm-market-subscribe-v1"
	CoinMSubscriptionRequestID      = "coinm-subscribe-v1"
)

var (
	ErrDerivativeConfiguration = errors.New("binance: invalid derivative WebSocket configuration")
	ErrDerivativeBounds        = errors.New("binance: derivative WebSocket bound exceeded")
)

type DerivativeProduct string

const (
	DerivativeProductUSDM  DerivativeProduct = "usdm"
	DerivativeProductCoinM DerivativeProduct = "coinm"
)

type DerivativeSubscriptionRequest struct {
	ID      int64
	Streams []string
	Raw     []byte
}

type DerivativeSubscriptionPlan struct {
	Product   DerivativeProduct
	Endpoint  string
	Symbols   []string
	Inventory []string
	Requests  []DerivativeSubscriptionRequest
	Evidence  []byte
}

type derivativeStreamEvidence struct {
	Selector         string `json:"selector"`
	Role             string `json:"role"`
	MergedUniverse   bool   `json:"merged_universe,omitempty"`
	NativeSymbolType uint8  `json:"native_symbol_type,omitempty"`
}

type derivativeRequestEvidence struct {
	ID          int64    `json:"id"`
	Streams     []string `json:"streams"`
	ByteLength  int      `json:"byte_length"`
	RequestHash string   `json:"sha256"`
}

type derivativePlanEvidence struct {
	Version           uint16                      `json:"version"`
	Product           DerivativeProduct           `json:"product"`
	Endpoint          string                      `json:"endpoint"`
	SymbolCount       int                         `json:"symbol_count"`
	SubscriptionCount int                         `json:"subscription_count"`
	InventoryHash     string                      `json:"inventory_sha256"`
	Requests          []derivativeRequestEvidence `json:"requests"`
	Streams           []derivativeStreamEvidence  `json:"streams"`
}

func NewUSDMDerivativeSubscriptionPlan(endpoint string, symbols []string) (DerivativeSubscriptionPlan, error) {
	var suffixes []string
	switch endpoint {
	case USDMPublicEndpoint:
		suffixes = []string{"@depth@100ms", "@bookTicker"}
	case USDMMarketEndpoint:
		suffixes = []string{"@aggTrade", "@ticker", "@markPrice@1s", "@indexPrice", "@forceOrder"}
	default:
		return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: endpoint is not an allowlisted USD-M public route", ErrDerivativeConfiguration)
	}
	normalized, err := normalizeDerivativeSymbols(symbols)
	if err != nil {
		return DerivativeSubscriptionPlan{}, err
	}
	inventory := make([]string, 0, len(normalized)*len(suffixes))
	streamEvidence := make([]derivativeStreamEvidence, 0, cap(inventory))
	for _, symbol := range normalized {
		for _, suffix := range suffixes {
			stream := symbol + suffix
			contract, validateErr := USDMValidateRoutedStream(usdmRouteForEndpoint(endpoint), stream)
			if validateErr != nil {
				return DerivativeSubscriptionPlan{}, validateErr
			}
			inventory = append(inventory, stream)
			streamEvidence = append(streamEvidence, derivativeStreamEvidence{Selector: stream, Role: string(contract.Role)})
		}
	}
	return buildDerivativeSubscriptionPlan(DerivativeProductUSDM, endpoint, normalized, inventory, streamEvidence)
}

func NewCoinMDerivativeSubscriptionPlan(endpoint string, symbols []string) (DerivativeSubscriptionPlan, error) {
	if endpoint != CoinMWebSocketEndpoint {
		return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: endpoint is not the allowlisted COIN-M public route", ErrDerivativeConfiguration)
	}
	normalized, err := normalizeDerivativeSymbols(symbols)
	if err != nil {
		return DerivativeSubscriptionPlan{}, err
	}
	suffixes := []string{"@aggTrade", "@depth@100ms", "@bookTicker", "@ticker", "@markPrice@1s"}
	inventory := make([]string, 0, len(normalized)*len(suffixes)+1)
	streamEvidence := make([]derivativeStreamEvidence, 0, cap(inventory))
	for _, symbol := range normalized {
		for _, suffix := range suffixes {
			stream := symbol + suffix
			contract, validateErr := CoinMStreamContractFor(stream)
			if validateErr != nil || contract.Support != capture.SupportAvailable || contract.MergedUniverse {
				return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: undeclared COIN-M symbol stream %q", ErrDerivativeConfiguration, stream)
			}
			inventory = append(inventory, stream)
			streamEvidence = append(streamEvidence, derivativeStreamEvidence{Selector: stream, Role: string(contract.Role)})
		}
	}
	const liquidationStream = "!forceOrder@arr"
	liquidationContract, err := CoinMStreamContractFor(liquidationStream)
	if err != nil || liquidationContract.Support != capture.SupportAvailable || !liquidationContract.MergedUniverse {
		return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: declared COIN-M merged liquidation route is unavailable", ErrDerivativeConfiguration)
	}
	inventory = append(inventory, liquidationStream)
	streamEvidence = append(streamEvidence, derivativeStreamEvidence{
		Selector: liquidationStream, Role: string(liquidationContract.Role), MergedUniverse: true, NativeSymbolType: 2,
	})
	return buildDerivativeSubscriptionPlan(DerivativeProductCoinM, endpoint, normalized, inventory, streamEvidence)
}

func normalizeDerivativeSymbols(symbols []string) ([]string, error) {
	if len(symbols) == 0 || len(symbols) > DerivativeMaxSymbols {
		return nil, fmt.Errorf("%w: symbol count must be within 1..%d", ErrDerivativeBounds, DerivativeMaxSymbols)
	}
	normalized := make([]string, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for i, symbol := range symbols {
		if symbol == "" || len(symbol) > 64 || !utf8.ValidString(symbol) || strings.TrimSpace(symbol) != symbol || strings.ContainsAny(symbol, "/?&#\x00\r\n\t@!") {
			return nil, fmt.Errorf("%w: symbol %d is invalid", ErrDerivativeConfiguration, i)
		}
		normalizedSymbol := strings.ToLower(symbol)
		if _, duplicate := seen[normalizedSymbol]; duplicate {
			return nil, fmt.Errorf("%w: duplicate symbol %q", ErrDerivativeConfiguration, symbol)
		}
		seen[normalizedSymbol] = struct{}{}
		normalized[i] = normalizedSymbol
	}
	slices.Sort(normalized)
	return normalized, nil
}

func buildDerivativeSubscriptionPlan(product DerivativeProduct, endpoint string, symbols, inventory []string, streamEvidence []derivativeStreamEvidence) (DerivativeSubscriptionPlan, error) {
	if len(inventory) == 0 || len(inventory) > capture.MaxExpectedSubscriptions {
		return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: subscription inventory has %d entries", ErrDerivativeBounds, len(inventory))
	}
	requestCount := (len(inventory) + DerivativeSubscriptionBatchLimit - 1) / DerivativeSubscriptionBatchLimit
	if requestCount == 0 || requestCount > DerivativeSubscriptionBatchCount {
		return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: subscription plan requires %d control messages", ErrDerivativeBounds, requestCount)
	}
	requests := make([]DerivativeSubscriptionRequest, 0, requestCount)
	requestEvidence := make([]derivativeRequestEvidence, 0, requestCount)
	for start := 0; start < len(inventory); start += DerivativeSubscriptionBatchLimit {
		end := min(start+DerivativeSubscriptionBatchLimit, len(inventory))
		streams := slices.Clone(inventory[start:end])
		requestID := int64(len(requests) + 1)
		raw, err := json.Marshal(struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
			ID     int64    `json:"id"`
		}{Method: "SUBSCRIBE", Params: streams, ID: requestID})
		if err != nil {
			return DerivativeSubscriptionPlan{}, fmt.Errorf("binance: encode derivative subscription batch: %w", err)
		}
		if len(raw) == 0 || len(raw) > DerivativeMaxControlMessageBytes {
			return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: subscription control message has %d bytes", ErrDerivativeBounds, len(raw))
		}
		digest := sha256.Sum256(raw)
		requests = append(requests, DerivativeSubscriptionRequest{ID: requestID, Streams: streams, Raw: raw})
		requestEvidence = append(requestEvidence, derivativeRequestEvidence{
			ID: requestID, Streams: slices.Clone(streams), ByteLength: len(raw), RequestHash: hex.EncodeToString(digest[:]),
		})
	}
	inventoryBytes, err := json.Marshal(inventory)
	if err != nil {
		return DerivativeSubscriptionPlan{}, fmt.Errorf("binance: encode derivative inventory: %w", err)
	}
	inventoryDigest := sha256.Sum256(inventoryBytes)
	evidence, err := json.Marshal(derivativePlanEvidence{
		Version: 1, Product: product, Endpoint: endpoint, SymbolCount: len(symbols), SubscriptionCount: len(inventory),
		InventoryHash: hex.EncodeToString(inventoryDigest[:]), Requests: requestEvidence, Streams: streamEvidence,
	})
	if err != nil {
		return DerivativeSubscriptionPlan{}, fmt.Errorf("binance: encode derivative subscription evidence: %w", err)
	}
	if len(evidence) > capture.MaxExtensionBytes-8 {
		return DerivativeSubscriptionPlan{}, fmt.Errorf("%w: subscription evidence has %d bytes", ErrDerivativeBounds, len(evidence))
	}
	return DerivativeSubscriptionPlan{
		Product: product, Endpoint: endpoint, Symbols: slices.Clone(symbols), Inventory: slices.Clone(inventory),
		Requests: requests, Evidence: evidence,
	}, nil
}

func cloneDerivativeSubscriptionPlan(plan DerivativeSubscriptionPlan) DerivativeSubscriptionPlan {
	clone := DerivativeSubscriptionPlan{
		Product: plan.Product, Endpoint: plan.Endpoint, Symbols: slices.Clone(plan.Symbols),
		Inventory: slices.Clone(plan.Inventory), Requests: make([]DerivativeSubscriptionRequest, len(plan.Requests)),
		Evidence: slices.Clone(plan.Evidence),
	}
	for i, request := range plan.Requests {
		clone.Requests[i] = DerivativeSubscriptionRequest{ID: request.ID, Streams: slices.Clone(request.Streams), Raw: slices.Clone(request.Raw)}
	}
	return clone
}

func usdmRouteForEndpoint(endpoint string) USDMRoute {
	if endpoint == USDMPublicEndpoint {
		return USDMRoutePublic
	}
	return USDMRouteMarket
}

func derivativeSubscriptionRequestID(product DerivativeProduct, endpoint string) string {
	switch {
	case product == DerivativeProductUSDM && endpoint == USDMPublicEndpoint:
		return USDMPublicSubscriptionRequestID
	case product == DerivativeProductUSDM && endpoint == USDMMarketEndpoint:
		return USDMMarketSubscriptionRequestID
	case product == DerivativeProductCoinM && endpoint == CoinMWebSocketEndpoint:
		return CoinMSubscriptionRequestID
	default:
		return ""
	}
}

func derivativeChannel(product DerivativeProduct, endpoint string) string {
	switch {
	case product == DerivativeProductUSDM && endpoint == USDMPublicEndpoint:
		return USDMRawPublicChannel
	case product == DerivativeProductUSDM && endpoint == USDMMarketEndpoint:
		return USDMRawMarketChannel
	case product == DerivativeProductCoinM && endpoint == CoinMWebSocketEndpoint:
		return CoinMRawChannel
	default:
		return ""
	}
}

func derivativeDataFamily(product DerivativeProduct) string {
	if product == DerivativeProductUSDM {
		return USDMRawDataFamily
	}
	if product == DerivativeProductCoinM {
		return CoinMRawDataFamily
	}
	return ""
}
