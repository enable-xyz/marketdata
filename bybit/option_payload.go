package bybit

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/enable-xyz/marketdata/capture"
)

func optionWSPayloadPolicy() capture.PayloadPolicy {
	return capture.PayloadPolicy{
		MaxRawBytes: MaxRawPayloadBytes, MaxSchemaDepth: OptionMaxSchemaDepth,
		MaxSchemaFields: OptionMaxSchemaFields, MaxArrayElements: OptionMaxArrayElements,
	}
}

func optionRESTPayloadPolicy() capture.PayloadPolicy {
	return capture.PayloadPolicy{
		MaxRawBytes: MaxRawPayloadBytes, MaxSchemaDepth: OptionRESTMaxSchemaDepth,
		MaxSchemaFields: OptionRESTMaxSchemaFields, MaxArrayElements: OptionRESTMaxArrayElements,
	}
}

func validateOptionPayload(payload []byte, policy capture.PayloadPolicy) error {
	if len(payload) == 0 || uint64(len(payload)) > uint64(policy.MaxRawBytes) ||
		policy.MaxSchemaDepth == 0 || policy.MaxSchemaFields == 0 || policy.MaxArrayElements == 0 {
		return ErrInvalidPayload
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	remainingFields := int64(policy.MaxSchemaFields)
	if err := consumeOptionJSONValue(decoder, policy, &remainingFields, 0); err != nil {
		return ErrInvalidPayload
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrInvalidPayload
	}
	return nil
}

func consumeOptionJSONValue(decoder *json.Decoder, policy capture.PayloadPolicy, remainingFields *int64, depth uint16) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	depth++
	if depth > policy.MaxSchemaDepth {
		return ErrInvalidPayload
	}
	switch delimiter {
	case '{':
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			if _, ok := key.(string); !ok {
				return ErrInvalidPayload
			}
			*remainingFields = *remainingFields - 1
			if *remainingFields < 0 {
				return ErrInvalidPayload
			}
			if err := consumeOptionJSONValue(decoder, policy, remainingFields, depth); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalidPayload
		}
	case '[':
		elements := uint32(0)
		for decoder.More() {
			elements++
			if elements > policy.MaxArrayElements {
				return ErrInvalidPayload
			}
			if err := consumeOptionJSONValue(decoder, policy, remainingFields, depth); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidPayload
		}
	default:
		return ErrInvalidPayload
	}
	return nil
}
