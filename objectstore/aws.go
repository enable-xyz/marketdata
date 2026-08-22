package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type AWSOptions struct {
	UsePathStyle bool
	Endpoint     string
}

type s3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput, ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
}

type AWSClient struct {
	bucket string
	api    s3API
}

var _ Client = (*AWSClient)(nil)

// NewAWSClient constructs an AWS SDK Go v2 S3 adapter without loading
// credentials or configuration from ambient process state. Callers own the
// supplied aws.Config. Endpoint is intended for explicitly configured
// S3-compatible providers; path-style addressing is independently selectable.
func NewAWSClient(config aws.Config, bucket string, options AWSOptions) (*AWSClient, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, fmt.Errorf("objectstore: AWS bucket is required")
	}
	client := s3.NewFromConfig(config, func(s3Options *s3.Options) {
		s3Options.UsePathStyle = options.UsePathStyle
		if options.Endpoint != "" {
			s3Options.BaseEndpoint = aws.String(options.Endpoint)
		}
	})
	return &AWSClient{bucket: bucket, api: client}, nil
}

func newAWSClient(api s3API, bucket string) *AWSClient {
	return &AWSClient{bucket: bucket, api: api}
}

func (c *AWSClient) Head(ctx context.Context, key string) (ObjectInfo, error) {
	output, err := c.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return ObjectInfo{}, classifyAWSError("head", key, err)
	}
	if output.ContentLength == nil || *output.ContentLength < 0 {
		return ObjectInfo{}, errorWithKind("head", key, ErrorInvalidResponse, errors.New("missing content length"))
	}
	return ObjectInfo{
		Key:          key,
		Size:         *output.ContentLength,
		Metadata:     cloneMetadata(output.Metadata),
		ETag:         aws.ToString(output.ETag),
		LastModified: aws.ToTime(output.LastModified),
	}, nil
}

func (c *AWSClient) PutIfAbsent(ctx context.Context, object PutObject) error {
	if err := validatePutObject(object); err != nil {
		return err
	}
	_, err := object.Body.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("objectstore: seek object before PUT: %w", err)
	}
	_, err = c.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(object.Key),
		Body:          object.Body,
		ContentLength: aws.Int64(object.Size),
		IfNoneMatch:   aws.String("*"),
		Metadata: map[string]string{
			ApplicationSHA256Metadata: fmt.Sprintf("%x", object.SHA256),
		},
	})
	if err != nil {
		return classifyAWSError("conditional put", object.Key, err)
	}
	return nil
}

func (c *AWSClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := c.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, classifyAWSError("get", key, err)
	}
	if output.Body == nil {
		return nil, errorWithKind("get", key, ErrorInvalidResponse, errors.New("missing body"))
	}
	return output.Body, nil
}

func (c *AWSClient) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("objectstore: invalid range offset=%d length=%d", offset, length)
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	output, err := c.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, classifyAWSError("range get", key, err)
	}
	if output.Body == nil {
		return nil, errorWithKind("range get", key, ErrorInvalidResponse, errors.New("missing body"))
	}
	return output.Body, nil
}

func (c *AWSClient) List(ctx context.Context, prefix, continuation string) (ListPage, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(MaximumListPageObjects),
	}
	if continuation != "" {
		input.ContinuationToken = aws.String(continuation)
	}
	output, err := c.api.ListObjectsV2(ctx, input)
	if err != nil {
		return ListPage{}, classifyAWSError("list", prefix, err)
	}
	if len(output.Contents) > MaximumListPageObjects {
		return ListPage{}, errorWithKind("list", prefix, ErrorInvalidResponse, errors.New("provider exceeded maximum list page"))
	}
	page := ListPage{Objects: make([]ListedObject, 0, len(output.Contents))}
	for _, object := range output.Contents {
		if object.Key == nil || object.Size == nil || *object.Size < 0 {
			return ListPage{}, errorWithKind("list", prefix, ErrorInvalidResponse, errors.New("object missing key or size"))
		}
		page.Objects = append(page.Objects, ListedObject{
			Key:          *object.Key,
			Size:         *object.Size,
			LastModified: aws.ToTime(object.LastModified),
		})
	}
	page.NextToken = aws.ToString(output.NextContinuationToken)
	if output.IsTruncated != nil && *output.IsTruncated && page.NextToken == "" {
		return ListPage{}, errorWithKind("list", prefix, ErrorInvalidResponse, errors.New("truncated response missing continuation token"))
	}
	return page, nil
}

func (c *AWSClient) StartMultipart(ctx context.Context, key string, hash [32]byte) (string, error) {
	output, err := c.api.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Metadata: map[string]string{
			ApplicationSHA256Metadata: fmt.Sprintf("%x", hash),
		},
	})
	if err != nil {
		return "", classifyAWSError("start multipart", key, err)
	}
	uploadID := aws.ToString(output.UploadId)
	if uploadID == "" {
		return "", errorWithKind("start multipart", key, ErrorInvalidResponse, errors.New("missing upload ID"))
	}
	return uploadID, nil
}

func (c *AWSClient) UploadPart(ctx context.Context, key, uploadID string, number int32, body io.Reader, size int64) (UploadedPart, error) {
	if uploadID == "" || number <= 0 || body == nil || size < 0 {
		return UploadedPart{}, fmt.Errorf("objectstore: invalid multipart part")
	}
	output, err := c.api.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(number),
		Body:          body,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return UploadedPart{}, classifyAWSError("upload part", key, err)
	}
	etag := aws.ToString(output.ETag)
	if etag == "" {
		return UploadedPart{}, errorWithKind("upload part", key, ErrorInvalidResponse, errors.New("missing part ETag"))
	}
	return UploadedPart{Number: number, ETag: etag}, nil
}

func (c *AWSClient) CompleteMultipart(ctx context.Context, key, uploadID string, parts []UploadedPart) error {
	if uploadID == "" || len(parts) == 0 {
		return fmt.Errorf("objectstore: invalid multipart completion")
	}
	completed := make([]types.CompletedPart, len(parts))
	previous := int32(0)
	for index, part := range parts {
		if part.Number <= previous || part.ETag == "" {
			return fmt.Errorf("objectstore: multipart parts are not strictly ordered")
		}
		previous = part.Number
		completed[index] = types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.Number),
		}
	}
	_, err := c.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		UploadId:    aws.String(uploadID),
		IfNoneMatch: aws.String("*"),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		return classifyAWSError("complete multipart", key, err)
	}
	return nil
}

func (c *AWSClient) AbortMultipart(ctx context.Context, key, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("objectstore: multipart upload ID is required")
	}
	_, err := c.api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(c.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return classifyAWSError("abort multipart", key, err)
	}
	return nil
}

// ReconcileMultipart aborts every incomplete upload for the exact immutable
// key. This is used after a lost initiation response, when no upload ID is
// available. Exact-key filtering is mandatory because provider prefix listing
// may include neighboring immutable objects.
func (c *AWSClient) ReconcileMultipart(ctx context.Context, key string) error {
	var keyMarker, uploadMarker *string
	seen := make(map[string]struct{})
	for {
		output, err := c.api.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket:         aws.String(c.bucket),
			Prefix:         aws.String(key),
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadMarker,
		})
		if err != nil {
			return classifyAWSError("list multipart", key, err)
		}
		for _, upload := range output.Uploads {
			if aws.ToString(upload.Key) != key || aws.ToString(upload.UploadId) == "" {
				continue
			}
			if err := c.AbortMultipart(ctx, key, *upload.UploadId); err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
		}
		if output.IsTruncated == nil || !*output.IsTruncated {
			return nil
		}
		nextKey := aws.ToString(output.NextKeyMarker)
		nextUpload := aws.ToString(output.NextUploadIdMarker)
		token := nextKey + "\x00" + nextUpload
		if nextKey == "" || token == "\x00" {
			return errorWithKind("list multipart", key, ErrorInvalidResponse, errors.New("truncated response missing marker"))
		}
		if _, duplicate := seen[token]; duplicate {
			return errorWithKind("list multipart", key, ErrorInvalidResponse, errors.New("duplicate pagination marker"))
		}
		seen[token] = struct{}{}
		keyMarker = aws.String(nextKey)
		uploadMarker = aws.String(nextUpload)
	}
}

func validatePutObject(object PutObject) error {
	if object.Key == "" || object.Body == nil || object.Size < 0 {
		return fmt.Errorf("objectstore: invalid immutable PUT")
	}
	return nil
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func classifyAWSError(op, key string, err error) error {
	if err == nil {
		return nil
	}
	var apiError smithy.APIError
	code := ""
	message := ""
	if errors.As(err, &apiError) {
		code = strings.ToLower(apiError.ErrorCode())
		message = strings.ToLower(apiError.ErrorMessage())
	}
	status := 0
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) {
		status = responseError.HTTPStatusCode()
	}

	kind := ErrorUnknown
	switch {
	case status == 404 || code == "nosuchkey" || code == "notfound" || code == "nosuchupload":
		kind = ErrorNotFound
	case status == 412 || status == 409 || code == "preconditionfailed" || code == "conditionalrequestconflict":
		kind = ErrorPrecondition
	case status == 501 || code == "notimplemented" ||
		(code == "invalidrequest" && (strings.Contains(message, "if-none-match") || strings.Contains(message, "conditional"))):
		kind = ErrorConditionalUnsupported
	case status == 401 || status == 403 || code == "accessdenied" || code == "invalidaccesskeyid" || code == "signaturedoesnotmatch":
		kind = ErrorPermission
	case status == 408 || status == 425 || status == 429 || status >= 500 ||
		code == "requesttimeout" || code == "slowdown" || code == "throttling" ||
		code == "serviceunavailable" || code == "internalerror":
		kind = ErrorTransient
	}
	return errorWithKind(op, key, kind, err)
}
