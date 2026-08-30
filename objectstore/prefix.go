package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// PrefixClient confines every logical object key to one explicit provider
// prefix while preserving the provider-neutral Client contract to callers.
type PrefixClient struct {
	next   Client
	prefix string
}

func NewPrefixClient(next Client, prefix string) (*PrefixClient, error) {
	if next == nil {
		return nil, errors.New("objectstore: prefixed client requires a delegate")
	}
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" && !validRelativeObjectKey(prefix) {
		return nil, errors.New("objectstore: prefix must be one clean relative object path")
	}
	return &PrefixClient{next: next, prefix: prefix}, nil
}

func (c *PrefixClient) providerKey(key string) (string, error) {
	if !validRelativeObjectKey(key) {
		return "", errors.New("objectstore: logical key must be one clean relative object path")
	}
	if c.prefix == "" {
		return key, nil
	}
	return c.prefix + "/" + key, nil
}

func validRelativeObjectKey(value string) bool {
	return value != "" && !strings.HasPrefix(value, "/") && !strings.Contains(value, "\\") &&
		value != "." && value != ".." && !strings.HasPrefix(value, "../") && path.Clean(value) == value
}

func (c *PrefixClient) Head(ctx context.Context, key string) (ObjectInfo, error) {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	value, err := c.next.Head(ctx, providerKey)
	if err != nil {
		return ObjectInfo{}, c.logicalError(err, key)
	}
	value.Key = key
	return value, nil
}

func (c *PrefixClient) PutIfAbsent(ctx context.Context, object PutObject) error {
	key, err := c.providerKey(object.Key)
	if err != nil {
		return err
	}
	object.Key = key
	return c.logicalError(c.next.PutIfAbsent(ctx, object), strings.TrimPrefix(key, c.prefix+"/"))
}

func (c *PrefixClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return nil, err
	}
	reader, err := c.next.Get(ctx, providerKey)
	return reader, c.logicalError(err, key)
}

func (c *PrefixClient) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return nil, err
	}
	reader, err := c.next.GetRange(ctx, providerKey, offset, length)
	return reader, c.logicalError(err, key)
}

func (c *PrefixClient) List(ctx context.Context, prefix, continuation string) (ListPage, error) {
	logicalPrefix := prefix
	if prefix == "" {
		prefix = c.prefix
		if prefix != "" {
			prefix += "/"
		}
	} else {
		trailingSlash := strings.HasSuffix(prefix, "/")
		logicalKey := strings.TrimSuffix(prefix, "/")
		if logicalKey == "" || strings.HasSuffix(logicalKey, "/") {
			return ListPage{}, errors.New("objectstore: logical list prefix must be one clean relative object path")
		}
		providerPrefix, err := c.providerKey(logicalKey)
		if err != nil {
			return ListPage{}, err
		}
		prefix = providerPrefix
		if trailingSlash {
			prefix += "/"
		}
	}
	page, err := c.next.List(ctx, prefix, continuation)
	if err != nil {
		return ListPage{}, c.logicalError(err, logicalPrefix)
	}
	providerPrefix := c.prefix
	if providerPrefix != "" {
		providerPrefix += "/"
	}
	for i := range page.Objects {
		if !strings.HasPrefix(page.Objects[i].Key, providerPrefix) {
			return ListPage{}, fmt.Errorf("objectstore: provider returned an object outside the configured prefix")
		}
		page.Objects[i].Key = strings.TrimPrefix(page.Objects[i].Key, providerPrefix)
	}
	return page, nil
}

func (c *PrefixClient) StartMultipart(ctx context.Context, key string, hash [32]byte) (string, error) {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return "", err
	}
	upload, err := c.next.StartMultipart(ctx, providerKey, hash)
	return upload, c.logicalError(err, key)
}

func (c *PrefixClient) UploadPart(ctx context.Context, key, upload string, number int32, body io.Reader, size int64) (UploadedPart, error) {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return UploadedPart{}, err
	}
	part, err := c.next.UploadPart(ctx, providerKey, upload, number, body, size)
	return part, c.logicalError(err, key)
}

func (c *PrefixClient) CompleteMultipart(ctx context.Context, key, upload string, parts []UploadedPart) error {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return err
	}
	return c.logicalError(c.next.CompleteMultipart(ctx, providerKey, upload, parts), key)
}

func (c *PrefixClient) AbortMultipart(ctx context.Context, key, upload string) error {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return err
	}
	return c.logicalError(c.next.AbortMultipart(ctx, providerKey, upload), key)
}

func (c *PrefixClient) ReconcileMultipart(ctx context.Context, key string) error {
	providerKey, err := c.providerKey(key)
	if err != nil {
		return err
	}
	return c.logicalError(c.next.ReconcileMultipart(ctx, providerKey), key)
}

func (c *PrefixClient) CreateImmutable(ctx context.Context, object PutObject) error {
	reconciler, ok := c.next.(ImmutableCreateReconciler)
	if !ok {
		return ErrConditionalCreateUnsupported
	}
	key, err := c.providerKey(object.Key)
	if err != nil {
		return err
	}
	object.Key = key
	return c.logicalError(reconciler.CreateImmutable(ctx, object), strings.TrimPrefix(key, c.prefix+"/"))
}

func (c *PrefixClient) logicalError(err error, logicalKey string) error {
	if err == nil {
		return nil
	}
	var objectErr *Error
	if !errors.As(err, &objectErr) {
		return err
	}
	clone := *objectErr
	clone.Key = logicalKey
	return &clone
}

var _ Client = (*PrefixClient)(nil)
var _ ImmutableCreateReconciler = (*PrefixClient)(nil)
