package verify

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"

	"github.com/enable-xyz/marketdata/objectstore"
)

type PrefixedObjectClient struct {
	inner  objectstore.Client
	prefix string
}

func NewPrefixedObjectClient(inner objectstore.Client, prefix string) (*PrefixedObjectClient, error) {
	prefix = strings.Trim(prefix, "/")
	if inner == nil || prefix == "" || strings.Contains(prefix, "\\") || path.Clean(prefix) != prefix || prefix == "." || strings.HasPrefix(prefix, "../") {
		return nil, errors.New("verify: explicit contained S3 prefix is required")
	}
	return &PrefixedObjectClient{inner: inner, prefix: prefix + "/"}, nil
}

func (c *PrefixedObjectClient) key(key string) (string, error) {
	if key == "" || strings.Contains(key, "\\") || path.Clean(key) != key || key == "." || strings.HasPrefix(key, "../") || strings.HasPrefix(key, "/") {
		return "", errors.New("verify: invalid logical object key")
	}
	return c.prefix + key, nil
}

func (c *PrefixedObjectClient) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	physical, err := c.key(key)
	if err != nil {
		return objectstore.ObjectInfo{}, err
	}
	info, err := c.inner.Head(ctx, physical)
	if err == nil {
		info.Key = key
	}
	return info, err
}

func (c *PrefixedObjectClient) PutIfAbsent(ctx context.Context, object objectstore.PutObject) error {
	physical, err := c.key(object.Key)
	if err != nil {
		return err
	}
	object.Key = physical
	return c.inner.PutIfAbsent(ctx, object)
}

func (c *PrefixedObjectClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	physical, err := c.key(key)
	if err != nil {
		return nil, err
	}
	return c.inner.Get(ctx, physical)
}

func (c *PrefixedObjectClient) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	physical, err := c.key(key)
	if err != nil {
		return nil, err
	}
	return c.inner.GetRange(ctx, physical, offset, length)
}

func (c *PrefixedObjectClient) List(ctx context.Context, prefix, continuation string) (objectstore.ListPage, error) {
	physical := c.prefix
	if prefix != "" {
		trailingSlash := strings.HasSuffix(prefix, "/")
		logical := strings.TrimSuffix(prefix, "/")
		var err error
		physical, err = c.key(logical)
		if err != nil {
			return objectstore.ListPage{}, err
		}
		if trailingSlash {
			physical += "/"
		}
	}
	page, err := c.inner.List(ctx, physical, continuation)
	if err != nil {
		return objectstore.ListPage{}, err
	}
	for index := range page.Objects {
		if !strings.HasPrefix(page.Objects[index].Key, c.prefix) {
			return objectstore.ListPage{}, errors.New("verify: S3 listing escaped configured prefix")
		}
		page.Objects[index].Key = strings.TrimPrefix(page.Objects[index].Key, c.prefix)
	}
	return page, nil
}

func (c *PrefixedObjectClient) StartMultipart(ctx context.Context, key string, digest [32]byte) (string, error) {
	physical, err := c.key(key)
	if err != nil {
		return "", err
	}
	return c.inner.StartMultipart(ctx, physical, digest)
}

func (c *PrefixedObjectClient) UploadPart(ctx context.Context, key, uploadID string, number int32, body io.Reader, size int64) (objectstore.UploadedPart, error) {
	physical, err := c.key(key)
	if err != nil {
		return objectstore.UploadedPart{}, err
	}
	return c.inner.UploadPart(ctx, physical, uploadID, number, body, size)
}

func (c *PrefixedObjectClient) CompleteMultipart(ctx context.Context, key, uploadID string, parts []objectstore.UploadedPart) error {
	physical, err := c.key(key)
	if err != nil {
		return err
	}
	return c.inner.CompleteMultipart(ctx, physical, uploadID, parts)
}

func (c *PrefixedObjectClient) AbortMultipart(ctx context.Context, key, uploadID string) error {
	physical, err := c.key(key)
	if err != nil {
		return err
	}
	return c.inner.AbortMultipart(ctx, physical, uploadID)
}

func (c *PrefixedObjectClient) ReconcileMultipart(ctx context.Context, key string) error {
	physical, err := c.key(key)
	if err != nil {
		return err
	}
	return c.inner.ReconcileMultipart(ctx, physical)
}

var _ objectstore.Client = (*PrefixedObjectClient)(nil)
