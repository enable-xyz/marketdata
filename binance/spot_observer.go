package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/enable-xyz/marketdata/capture"
)

type SpotRawObserver struct {
	plan    SpotSubscriptionPlan
	policy  capture.PayloadPolicy
	batches map[string]int
	seen    []bool
	depth   bool
}

func NewSpotRawObserver(plan SpotSubscriptionPlan) *SpotRawObserver {
	batches := make(map[string]int, len(plan.Requests))
	for i, request := range plan.Requests {
		batches[strconv.FormatInt(request.ID, 10)] = i
	}
	return &SpotRawObserver{
		plan:    cloneSpotSubscriptionPlan(plan),
		policy:  spotPayloadPolicy(),
		batches: batches,
		seen:    make([]bool, len(plan.Requests)),
	}
}

func newSpotDepthObserver() *SpotRawObserver {
	return &SpotRawObserver{policy: spotPayloadPolicy(), depth: true}
}

func (o *SpotRawObserver) Observe(ctx context.Context, envelope capture.EnvelopeV1) (capture.Observation, error) {
	if err := ctx.Err(); err != nil {
		return capture.Observation{}, err
	}
	if envelope.RecordKind == capture.RecordKindControl && envelope.ControlKind.Valid && envelope.ControlKind.Value == capture.ControlHeartbeat {
		if len(envelope.RawPayload) > SpotMaxPingPayloadBytes {
			return capture.Observation{Role: capture.MessageHeartbeat, Schema: capture.SchemaMalformed}, nil
		}
		return capture.Observation{Role: capture.MessageHeartbeat, Schema: capture.SchemaAccepted, Unchanged: true}, nil
	}
	if o.depth {
		return o.observeDepthResponse(envelope)
	}
	if envelope.PayloadEncoding != capture.PayloadEncodingJSON {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
	}
	value, err := decodeBoundedJSON(envelope.RawPayload, o.policy)
	if err != nil {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaMalformed}, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
	}
	if _, hasResult := object["result"]; hasResult {
		return o.observeACK(object), nil
	}
	if _, hasCode := object["code"]; hasCode {
		return o.observeACK(object), nil
	}
	if event, ok := object["e"].(string); ok {
		switch event {
		case "trade":
			return dataObservation(validateObject(object, tradeSchema)), nil
		case "depthUpdate":
			return dataObservation(validateObject(object, depthSchema)), nil
		case "24hrTicker":
			return dataObservation(validateObject(object, tickerSchema)), nil
		default:
			return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
		}
	}
	if _, hasUpdateID := object["u"]; hasUpdateID {
		return dataObservation(validateObject(object, bookTickerSchema)), nil
	}
	return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaUnknownRole}, nil
}

func (o *SpotRawObserver) observeDepthResponse(envelope capture.EnvelopeV1) (capture.Observation, error) {
	if !envelope.HTTPStatusOrWSState.Valid {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaMalformed}, nil
	}
	status, err := strconv.Atoi(envelope.HTTPStatusOrWSState.Value)
	if err != nil {
		return capture.Observation{Role: capture.MessageUnknown, Schema: capture.SchemaMalformed}, nil
	}
	if status < 200 || status >= 300 {
		return capture.Observation{Role: capture.MessageData, Schema: capture.SchemaAccepted}, nil
	}
	value, err := decodeBoundedJSON(envelope.RawPayload, o.policy)
	if err != nil {
		return capture.Observation{Role: capture.MessageData, Schema: capture.SchemaMalformed}, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return capture.Observation{Role: capture.MessageData, Schema: capture.SchemaSemanticChanged}, nil
	}
	return dataObservation(validateObject(object, depthSnapshotSchema)), nil
}

func (o *SpotRawObserver) observeACK(object map[string]any) capture.Observation {
	actualID := jsonID(object["id"])
	requestID := actualID
	batch, known := o.batches[actualID]
	if known {
		requestID = SpotSubscriptionRequestID
	}
	ack := capture.ACKObservation{RequestID: requestID}
	if code, errorResponse := object["code"]; errorResponse {
		_, codeOK := code.(json.Number)
		_, messageOK := object["msg"].(string)
		ack.Accepted = false
		if !codeOK || !messageOK || actualID == "" {
			ack.RequestID = actualID
		}
		return capture.Observation{Role: capture.MessageAcknowledgement, Schema: capture.SchemaAccepted, ACK: ack}
	}
	result, hasResult := object["result"]
	if !hasResult || result != nil || len(object) != 2 || actualID == "" || !known {
		return capture.Observation{Role: capture.MessageAcknowledgement, Schema: capture.SchemaAccepted, ACK: ack}
	}
	o.seen[batch] = true
	ack.Accepted = true
	ack.Subscriptions = append([]string(nil), o.plan.Requests[batch].Streams...)
	ack.FinalBatch = batch == len(o.plan.Requests)-1
	return capture.Observation{Role: capture.MessageAcknowledgement, Schema: capture.SchemaAccepted, ACK: ack}
}

func dataObservation(disposition capture.SchemaDisposition) capture.Observation {
	return capture.Observation{Role: capture.MessageData, Schema: disposition}
}

type spotFieldKind uint8

const (
	spotString spotFieldKind = iota + 1
	spotInteger
	spotBoolean
	spotLevels
)

var tradeSchema = map[string]spotFieldKind{
	"e": spotString, "E": spotInteger, "s": spotString, "t": spotInteger,
	"p": spotString, "q": spotString, "T": spotInteger, "m": spotBoolean, "M": spotBoolean,
}

var depthSchema = map[string]spotFieldKind{
	"e": spotString, "E": spotInteger, "s": spotString, "U": spotInteger,
	"u": spotInteger, "b": spotLevels, "a": spotLevels,
}

var bookTickerSchema = map[string]spotFieldKind{
	"u": spotInteger, "s": spotString, "b": spotString,
	"B": spotString, "a": spotString, "A": spotString,
}

var tickerSchema = map[string]spotFieldKind{
	"e": spotString, "E": spotInteger, "s": spotString, "p": spotString,
	"P": spotString, "w": spotString, "x": spotString, "c": spotString,
	"Q": spotString, "b": spotString, "B": spotString, "a": spotString,
	"A": spotString, "o": spotString, "h": spotString, "l": spotString,
	"v": spotString, "q": spotString, "O": spotInteger, "C": spotInteger,
	"F": spotInteger, "L": spotInteger, "n": spotInteger,
}

var depthSnapshotSchema = map[string]spotFieldKind{
	"lastUpdateId": spotInteger, "bids": spotLevels, "asks": spotLevels,
}

func validateObject(object map[string]any, schema map[string]spotFieldKind) capture.SchemaDisposition {
	for name, kind := range schema {
		value, ok := object[name]
		if !ok || !validSpotField(value, kind) {
			return capture.SchemaSemanticChanged
		}
	}
	if len(object) > len(schema) {
		return capture.SchemaAdditive
	}
	return capture.SchemaAccepted
}

func validSpotField(value any, kind spotFieldKind) bool {
	switch kind {
	case spotString:
		_, ok := value.(string)
		return ok
	case spotInteger:
		number, ok := value.(json.Number)
		return ok && integerJSON(number.String())
	case spotBoolean:
		_, ok := value.(bool)
		return ok
	case spotLevels:
		levels, ok := value.([]any)
		if !ok {
			return false
		}
		for _, level := range levels {
			pair, ok := level.([]any)
			if !ok || len(pair) != 2 {
				return false
			}
			if _, ok := pair[0].(string); !ok {
				return false
			}
			if _, ok := pair[1].(string); !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func integerJSON(value string) bool {
	if value == "" {
		return false
	}
	start := 0
	if value[0] == '-' {
		start = 1
	}
	if start == len(value) || value[start] == '0' && len(value)-start > 1 {
		return false
	}
	for i := start; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func jsonID(value any) string {
	switch typed := value.(type) {
	case json.Number:
		if integerJSON(typed.String()) {
			return typed.String()
		}
	case string:
		if len(typed) <= 36 {
			return typed
		}
	}
	return ""
}

type jsonBudget struct {
	maxDepth  int
	maxFields int
	maxArray  int
	fields    int
	arrays    int
}

func decodeBoundedJSON(raw []byte, policy capture.PayloadPolicy) (any, error) {
	if len(raw) == 0 || uint32(len(raw)) > policy.MaxRawBytes {
		return nil, ErrSpotBounds
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	budget := jsonBudget{
		maxDepth:  int(policy.MaxSchemaDepth),
		maxFields: int(policy.MaxSchemaFields),
		maxArray:  int(policy.MaxArrayElements),
	}
	value, err := decodeJSONValue(decoder, &budget, 1)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("binance: trailing JSON value")
		}
		return nil, err
	}
	return value, nil
}

func decodeJSONValue(decoder *json.Decoder, budget *jsonBudget, depth int) (any, error) {
	if depth > budget.maxDepth {
		return nil, ErrSpotBounds
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := nameToken.(string)
			if !ok {
				return nil, fmt.Errorf("binance: non-string JSON object key")
			}
			if _, duplicate := object[name]; duplicate {
				return nil, fmt.Errorf("binance: duplicate JSON field %q", name)
			}
			budget.fields++
			if budget.fields > budget.maxFields {
				return nil, ErrSpotBounds
			}
			value, err := decodeJSONValue(decoder, budget, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return nil, fmt.Errorf("binance: invalid JSON object close")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			budget.arrays++
			if budget.arrays > budget.maxArray {
				return nil, ErrSpotBounds
			}
			value, err := decodeJSONValue(decoder, budget, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return nil, fmt.Errorf("binance: invalid JSON array close")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("binance: invalid JSON delimiter %q", delim)
	}
}

func cloneSpotSubscriptionPlan(plan SpotSubscriptionPlan) SpotSubscriptionPlan {
	clone := SpotSubscriptionPlan{
		Symbols:   append([]string(nil), plan.Symbols...),
		Inventory: append([]string(nil), plan.Inventory...),
		Requests:  make([]SpotSubscriptionRequest, len(plan.Requests)),
		Evidence:  append([]byte(nil), plan.Evidence...),
	}
	for i, request := range plan.Requests {
		clone.Requests[i] = SpotSubscriptionRequest{
			ID:      request.ID,
			Streams: append([]string(nil), request.Streams...),
			Raw:     append([]byte(nil), request.Raw...),
		}
	}
	return clone
}
