package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type prefixRecordingClient struct {
	objects map[string][]byte
	keys    []string
}

func newPrefixRecordingClient() *prefixRecordingClient {
	return &prefixRecordingClient{objects: make(map[string][]byte)}
}
func (c *prefixRecordingClient) Head(_ context.Context, key string) (ObjectInfo, error) {
	c.keys = append(c.keys, key)
	body, ok := c.objects[key]
	if !ok {
		return ObjectInfo{}, &Error{Op: "head", Key: key, Kind: ErrorNotFound, Err: ErrNotFound}
	}
	return ObjectInfo{Key: key, Size: int64(len(body))}, nil
}
func (c *prefixRecordingClient) PutIfAbsent(_ context.Context, object PutObject) error {
	c.keys = append(c.keys, object.Key)
	body, _ := io.ReadAll(object.Body)
	c.objects[object.Key] = body
	return nil
}
func (c *prefixRecordingClient) Get(_ context.Context, key string) (io.ReadCloser, error) {
	c.keys = append(c.keys, key)
	body, ok := c.objects[key]
	if !ok {
		return nil, &Error{Op: "get", Key: key, Kind: ErrorNotFound, Err: ErrNotFound}
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}
func (c *prefixRecordingClient) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	reader, err := c.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	body, _ := io.ReadAll(reader)
	return io.NopCloser(bytes.NewReader(body[offset : offset+length])), nil
}
func (c *prefixRecordingClient) List(_ context.Context, prefix, continuation string) (ListPage, error) {
	c.keys = append(c.keys, prefix)
	var page ListPage
	for key, body := range c.objects {
		if strings.HasPrefix(key, prefix) {
			page.Objects = append(page.Objects, ListedObject{Key: key, Size: int64(len(body))})
		}
	}
	return page, nil
}
func (c *prefixRecordingClient) StartMultipart(_ context.Context, key string, _ [32]byte) (string, error) {
	c.keys = append(c.keys, key)
	return "upload", nil
}
func (c *prefixRecordingClient) UploadPart(_ context.Context, key, _ string, number int32, _ io.Reader, _ int64) (UploadedPart, error) {
	c.keys = append(c.keys, key)
	return UploadedPart{Number: number}, nil
}
func (c *prefixRecordingClient) CompleteMultipart(_ context.Context, key, _ string, _ []UploadedPart) error {
	c.keys = append(c.keys, key)
	return nil
}
func (c *prefixRecordingClient) AbortMultipart(_ context.Context, key, _ string) error {
	c.keys = append(c.keys, key)
	return nil
}
func (c *prefixRecordingClient) ReconcileMultipart(_ context.Context, key string) error {
	c.keys = append(c.keys, key)
	return nil
}

func TestPrefixClientContainsAndProjectsKeys(t *testing.T) {
	delegate := newPrefixRecordingClient()
	client, err := NewPrefixClient(delegate, "desk/market")
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.NewReader([]byte("payload"))
	if err := client.PutIfAbsent(t.Context(), PutObject{Key: "raw/segment", Body: body, Size: int64(body.Len())}); err != nil {
		t.Fatal(err)
	}
	if delegate.keys[0] != "desk/market/raw/segment" {
		t.Fatalf("provider key = %q", delegate.keys[0])
	}
	info, err := client.Head(t.Context(), "raw/segment")
	if err != nil || info.Key != "raw/segment" {
		t.Fatalf("Head() = %+v, %v", info, err)
	}
	page, err := client.List(t.Context(), "raw", "")
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Key != "raw/segment" {
		t.Fatalf("List() = %+v, %v", page, err)
	}
	if _, err := client.Get(t.Context(), "../escape"); err == nil {
		t.Fatal("traversal key accepted")
	}
	_, err = client.Head(t.Context(), "missing")
	var objectErr *Error
	if !errors.As(err, &objectErr) || objectErr.Key != "missing" || !errors.Is(err, ErrNotFound) {
		t.Fatalf("logical error = %#v", err)
	}
}

func TestPrefixClientEmptyListCannotEscapeToNeighborPrefix(t *testing.T) {
	delegate := newPrefixRecordingClient()
	delegate.objects["desk/market/raw/segment"] = []byte("owned")
	delegate.objects["desk/market-archive/raw/segment"] = []byte("neighbor")
	client, err := NewPrefixClient(delegate, "desk/market")
	if err != nil {
		t.Fatal(err)
	}

	page, err := client.List(t.Context(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "raw/segment" {
		t.Fatalf("List() = %+v, want only configured-prefix object", page)
	}
	if got := delegate.keys[len(delegate.keys)-1]; got != "desk/market/" {
		t.Fatalf("provider prefix = %q, want %q", got, "desk/market/")
	}
}

func TestPrefixClientPreservesTrailingSlashListBoundary(t *testing.T) {
	delegate := newPrefixRecordingClient()
	delegate.objects["desk/market/raw/v1/segment"] = []byte("owned")
	delegate.objects["desk/market/raw/v10/segment"] = []byte("neighbor")
	client, err := NewPrefixClient(delegate, "desk/market")
	if err != nil {
		t.Fatal(err)
	}

	page, err := client.List(t.Context(), "raw/v1/", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "raw/v1/segment" {
		t.Fatalf("List() = %+v, want only slash-delimited prefix", page)
	}
	if got := delegate.keys[len(delegate.keys)-1]; got != "desk/market/raw/v1/" {
		t.Fatalf("provider prefix = %q, want slash-delimited provider prefix", got)
	}
}
