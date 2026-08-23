package okx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

type RESTRequest struct {
	Endpoint string
	Query    url.Values
}

func (r RESTRequest) validate() error {
	allowed := map[string]map[string]struct{}{
		InstrumentPath:     {"instType": {}, "uly": {}, "instFamily": {}, "instId": {}},
		RESTBookPath:       {"instId": {}, "sz": {}},
		HistoryTradesPath:  {"instId": {}, "type": {}, "after": {}, "before": {}, "limit": {}},
		FundingHistoryPath: {"instId": {}, "after": {}, "before": {}, "limit": {}},
		OptionSummaryPath:  {"uly": {}, "instFamily": {}, "expTime": {}},
		IndexTickersPath:   {"quoteCcy": {}, "instId": {}},
		MarkPricePath:      {"instType": {}, "uly": {}, "instFamily": {}, "instId": {}},
	}
	keys, ok := allowed[r.Endpoint]
	if !ok || len(r.Query) == 0 {
		return ErrInvalidConfiguration
	}
	for key, values := range r.Query {
		if _, ok := keys[key]; !ok || len(values) != 1 || !validRESTValue(values[0]) {
			return fmt.Errorf("%w: invalid query field %q", ErrInvalidConfiguration, key)
		}
	}
	require := func(key string) bool { return len(r.Query[key]) == 1 && r.Query.Get(key) != "" }
	switch r.Endpoint {
	case InstrumentPath:
		if !require("instType") {
			return ErrInvalidConfiguration
		}
		if err := InstrumentType(r.Query.Get("instType")).Validate(); err != nil {
			return err
		}
	case RESTBookPath:
		if !require("instId") {
			return ErrInvalidConfiguration
		}
		if size := r.Query.Get("sz"); size != "" {
			value, err := strconv.Atoi(size)
			if err != nil || value < 1 || value > 400 {
				return ErrInvalidConfiguration
			}
		}
	case HistoryTradesPath, FundingHistoryPath:
		if !require("instId") {
			return ErrInvalidConfiguration
		}
		if limit := r.Query.Get("limit"); limit != "" {
			value, err := strconv.Atoi(limit)
			if err != nil || value < 1 || value > 100 {
				return ErrInvalidConfiguration
			}
		}
	case OptionSummaryPath:
		if require("uly") == require("instFamily") {
			return ErrInvalidConfiguration
		}
	case IndexTickersPath:
		if require("quoteCcy") == require("instId") {
			return ErrInvalidConfiguration
		}
	case MarkPricePath:
		if !require("instType") {
			return ErrInvalidConfiguration
		}
		if err := InstrumentType(r.Query.Get("instType")).Validate(); err != nil || r.Query.Get("instType") == string(Spot) || r.Query.Get("instType") == string(Margin) {
			return ErrInvalidConfiguration
		}
	}
	return nil
}

func validRESTValue(value string) bool {
	if value == "" || len(value) > 128 || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, r := range value {
		if (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '.' {
			return false
		}
	}
	return true
}

type RESTHeader struct {
	Name  string
	Value string
}

type RESTResponse struct {
	StatusCode int
	Headers    []RESTHeader
	Body       []byte
}

// PublicRESTClient can issue only unauthenticated GET requests to the fixed OKX
// public host. The caller owns an explicit bounded HTTP transport and timeout.
type PublicRESTClient struct {
	base   *url.URL
	client *http.Client
}

func NewPublicRESTClient(client *http.Client) (*PublicRESTClient, error) {
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: explicit HTTP transport and timeout are required", ErrInvalidConfiguration)
	}
	base, err := url.Parse(PublicRESTEndpoint)
	if err != nil || base.Scheme != "https" || base.Host != "www.okx.com" || base.Path != "" || base.RawQuery != "" || base.User != nil {
		return nil, ErrInvalidConfiguration
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("okx: REST redirects are disabled") }
	return &PublicRESTClient{base: base, client: &copyClient}, nil
}

func (c *PublicRESTClient) Do(ctx context.Context, request RESTRequest, maximum uint32) (RESTResponse, error) {
	if c == nil || c.base == nil || c.client == nil || maximum == 0 || maximum > MaxRawPayloadBytes {
		return RESTResponse{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return RESTResponse{}, err
	}
	if err := request.validate(); err != nil {
		return RESTResponse{}, err
	}
	endpoint := *c.base
	endpoint.Path = request.Endpoint
	query := make(url.Values, len(request.Query))
	for key, values := range request.Query {
		query[key] = slices.Clone(values)
	}
	endpoint.RawQuery = query.Encode()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return RESTResponse{}, fmt.Errorf("okx: construct public request: %w", err)
	}
	if httpRequest.Header.Get("Authorization") != "" || httpRequest.Header.Get("OK-ACCESS-KEY") != "" {
		return RESTResponse{}, ErrInvalidConfiguration
	}
	response, err := c.client.Do(httpRequest)
	if err != nil {
		return RESTResponse{}, fmt.Errorf("okx: execute public request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(maximum)+1))
	if err != nil {
		return RESTResponse{}, fmt.Errorf("okx: read bounded public response: %w", err)
	}
	if len(body) > int(maximum) {
		return RESTResponse{}, ErrInvalidPayload
	}
	return RESTResponse{StatusCode: response.StatusCode, Headers: publicResponseHeaders(response.Header), Body: slices.Clone(body)}, nil
}

func publicResponseHeaders(headers http.Header) []RESTHeader {
	allowed := []string{"Content-Type", "Date", "Cache-Control", "OK-BEFORE", "OK-AFTER"}
	result := make([]RESTHeader, 0, len(allowed))
	for _, name := range allowed {
		if value := headers.Get(name); value != "" && len(value) <= 512 && strings.IndexByte(value, 0) < 0 {
			result = append(result, RESTHeader{Name: name, Value: value})
		}
	}
	return result
}
