package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func TestCompatibilityAWSErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		code string
		want error
	}{
		{name: "not found", code: "NoSuchKey", want: ErrNotFound},
		{name: "conditional race", code: "PreconditionFailed", want: ErrPreconditionFailed},
		{name: "conditional conflict", code: "ConditionalRequestConflict", want: ErrPreconditionFailed},
		{name: "conditional unsupported", code: "NotImplemented", want: ErrConditionalCreateUnsupported},
		{name: "permission", code: "AccessDenied", want: ErrPermission},
		{name: "transient", code: "SlowDown", want: ErrTransient},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			providerErr := &smithy.GenericAPIError{Code: test.code, Message: "deterministic provider error"}
			got := classifyAWSError("operation", "key", providerErr)
			if !errors.Is(got, test.want) {
				t.Fatalf("classifyAWSError(%q) = %v, want %v", test.code, got, test.want)
			}
		})
	}
}

type recordingS3API struct {
	s3API
	put *s3.PutObjectInput
}

func (r *recordingS3API) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	r.put = input
	return &s3.PutObjectOutput{}, nil
}

func TestCompatibilityAWSPutUsesConditionalCreateAndApplicationHash(t *testing.T) {
	api := &recordingS3API{}
	client := newAWSClient(api, "bucket")
	payload := []byte("immutable bytes")
	hash := sha256.Sum256(payload)
	if err := client.PutIfAbsent(t.Context(), PutObject{
		Key:    "raw/v1/key",
		Body:   bytes.NewReader(payload),
		Size:   int64(len(payload)),
		SHA256: hash,
	}); err != nil {
		t.Fatalf("PutIfAbsent() error = %v", err)
	}
	if api.put == nil || api.put.IfNoneMatch == nil || *api.put.IfNoneMatch != "*" {
		t.Fatalf("PutObject IfNoneMatch = %#v, want *", api.put)
	}
	if got := api.put.Metadata[ApplicationSHA256Metadata]; got != fmt.Sprintf("%x", hash) {
		t.Fatalf("application SHA-256 metadata = %q, want %x", got, hash)
	}
	if api.put.ContentLength == nil || *api.put.ContentLength != int64(len(payload)) {
		t.Fatalf("PutObject ContentLength = %#v, want %d", api.put.ContentLength, len(payload))
	}
}
