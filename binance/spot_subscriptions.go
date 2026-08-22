package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

var (
	ErrSpotConfiguration = errors.New("binance: invalid Spot adapter configuration")
	ErrSpotBounds        = errors.New("binance: Spot adapter bound exceeded")
)

var spotStreamSuffixes = [...]string{"@trade", "@depth@100ms", "@bookTicker", "@ticker"}

type SpotSubscriptionRequest struct {
	ID      int64
	Streams []string
	Raw     []byte
}

type SpotSubscriptionPlan struct {
	Symbols   []string
	Inventory []string
	Requests  []SpotSubscriptionRequest
	Evidence  []byte
}

func NewSpotSubscriptionPlan(symbols []string) (SpotSubscriptionPlan, error) {
	if len(symbols) == 0 || len(symbols) > SpotAdapterStreamLimit/len(spotStreamSuffixes) {
		return SpotSubscriptionPlan{}, fmt.Errorf("%w: configured symbol count must be within 1..%d", ErrSpotBounds, SpotAdapterStreamLimit/len(spotStreamSuffixes))
	}
	normalized := make([]string, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for i, symbol := range symbols {
		if symbol == "" || len(symbol) > 64 || !utf8.ValidString(symbol) || strings.TrimSpace(symbol) != symbol || strings.ContainsAny(symbol, "/?&#\x00\r\n\t") {
			return SpotSubscriptionPlan{}, fmt.Errorf("%w: symbol %d is invalid", ErrSpotConfiguration, i)
		}
		normalizedSymbol := strings.ToLower(symbol)
		if _, exists := seen[normalizedSymbol]; exists {
			return SpotSubscriptionPlan{}, fmt.Errorf("%w: duplicate symbol %q", ErrSpotConfiguration, symbol)
		}
		seen[normalizedSymbol] = struct{}{}
		normalized[i] = normalizedSymbol
	}
	slices.Sort(normalized)
	inventory := make([]string, 0, len(normalized)*len(spotStreamSuffixes))
	for _, symbol := range normalized {
		for _, suffix := range spotStreamSuffixes {
			inventory = append(inventory, symbol+suffix)
		}
	}
	if len(inventory) > SpotAdapterStreamLimit {
		return SpotSubscriptionPlan{}, fmt.Errorf("%w: stream inventory has %d entries, maximum is %d", ErrSpotBounds, len(inventory), SpotAdapterStreamLimit)
	}
	requestCount := (len(inventory) + SpotSubscriptionBatchLimit - 1) / SpotSubscriptionBatchLimit
	requests := make([]SpotSubscriptionRequest, 0, requestCount)
	evidenceRequests := make([]json.RawMessage, 0, requestCount)
	for start := 0; start < len(inventory); start += SpotSubscriptionBatchLimit {
		end := min(start+SpotSubscriptionBatchLimit, len(inventory))
		streams := slices.Clone(inventory[start:end])
		requestID := int64(len(requests) + 1)
		raw, err := json.Marshal(struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
			ID     int64    `json:"id"`
		}{Method: "SUBSCRIBE", Params: streams, ID: requestID})
		if err != nil {
			return SpotSubscriptionPlan{}, fmt.Errorf("binance: encode subscription batch: %w", err)
		}
		if len(raw) > SpotMaxControlMessageBytes {
			return SpotSubscriptionPlan{}, fmt.Errorf("%w: subscription control message has %d bytes, maximum is %d", ErrSpotBounds, len(raw), SpotMaxControlMessageBytes)
		}
		requests = append(requests, SpotSubscriptionRequest{ID: requestID, Streams: streams, Raw: raw})
		evidenceRequests = append(evidenceRequests, json.RawMessage(raw))
	}
	evidence, err := json.Marshal(struct {
		Version  uint16            `json:"version"`
		Requests []json.RawMessage `json:"requests"`
	}{Version: 1, Requests: evidenceRequests})
	if err != nil {
		return SpotSubscriptionPlan{}, fmt.Errorf("binance: encode subscription evidence: %w", err)
	}
	return SpotSubscriptionPlan{
		Symbols:   normalized,
		Inventory: inventory,
		Requests:  requests,
		Evidence:  evidence,
	}, nil
}
