package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/objectstore"
)

type PublicationCatalog interface {
	objectstore.PublicationCatalog
	StreamCommittedRawSegments(context.Context, func(catalog.RawSegmentPublication) error) error
}

// FileObjectClient is an immutable, content-verified local implementation of
// the same provider-neutral contract consumed by S3 publication. It exists for
// offline fixture evidence; live verification receives an AWS S3 client.
type FileObjectClient struct {
	root string
}

func OpenFileObjectClient(root string) (*FileObjectClient, error) {
	if err := requireRealDirectory(root); err != nil {
		return nil, err
	}
	objects := filepath.Join(root, "objects")
	if err := ensureContainedDirectory(root, objects); err != nil {
		return nil, err
	}
	return &FileObjectClient{root: objects}, nil
}

func (c *FileObjectClient) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.ObjectInfo{}, err
	}
	path, err := c.objectPath(key)
	if err != nil {
		return objectstore.ObjectInfo{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return objectstore.ObjectInfo{}, objectError("head", key, objectstore.ErrorNotFound, err)
		}
		return objectstore.ObjectInfo{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return objectstore.ObjectInfo{}, objectError("head", key, objectstore.ErrorInvalidResponse, errors.New("not a regular object"))
	}
	digest, size, err := hashRegularFile(path)
	if err != nil {
		return objectstore.ObjectInfo{}, err
	}
	return objectstore.ObjectInfo{
		Key:          key,
		Size:         size,
		Metadata:     map[string]string{objectstore.ApplicationSHA256Metadata: hex.EncodeToString(digest[:])},
		LastModified: info.ModTime(),
	}, nil
}

func (c *FileObjectClient) PutIfAbsent(ctx context.Context, object objectstore.PutObject) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if object.Body == nil || object.Size <= 0 {
		return objectError("put", object.Key, objectstore.ErrorInvalidResponse, errors.New("invalid object"))
	}
	path, err := c.objectPath(object.Key)
	if err != nil {
		return err
	}
	if err := ensureContainedDirectory(c.root, filepath.Dir(path)); err != nil {
		return err
	}
	if _, err := object.Body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".immutable-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(temporary, hasher), io.LimitReader(object.Body, object.Size+1))
	if copyErr != nil || written != object.Size {
		temporary.Close()
		return objectError("put", object.Key, objectstore.ErrorInvalidResponse, errors.New("object size mismatch"))
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	if digest != object.SHA256 {
		temporary.Close()
		return objectstore.ErrHashMismatch
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return objectError("put", object.Key, objectstore.ErrorPrecondition, objectstore.ErrPreconditionFailed)
		}
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (c *FileObjectClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := c.objectPath(key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, objectError("get", key, objectstore.ErrorNotFound, err)
	}
	return file, err
}

func (c *FileObjectClient) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 {
		return nil, objectError("range", key, objectstore.ErrorInvalidResponse, errors.New("invalid range"))
	}
	file, err := c.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	regular, ok := file.(*os.File)
	if !ok {
		file.Close()
		return nil, objectError("range", key, objectstore.ErrorInvalidResponse, errors.New("invalid file object"))
	}
	return &sectionReadCloser{Reader: io.NewSectionReader(regular, offset, length), Closer: regular}, nil
}

type sectionReadCloser struct {
	io.Reader
	io.Closer
}

func (c *FileObjectClient) List(ctx context.Context, prefix, token string) (objectstore.ListPage, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.ListPage{}, err
	}
	var keys []string
	err := filepath.WalkDir(c.root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == c.root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("verify: symlink in fixture object store")
		}
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".immutable-") {
			return nil
		}
		relative, err := filepath.Rel(c.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(relative)
		if strings.HasPrefix(key, prefix) && key > token {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return objectstore.ListPage{}, err
	}
	slices.Sort(keys)
	page := objectstore.ListPage{}
	for _, key := range keys[:min(len(keys), objectstore.MaximumListPageObjects)] {
		info, err := c.Head(ctx, key)
		if err != nil {
			return objectstore.ListPage{}, err
		}
		page.Objects = append(page.Objects, objectstore.ListedObject{Key: key, Size: info.Size, LastModified: info.LastModified})
	}
	if len(keys) > len(page.Objects) {
		page.NextToken = page.Objects[len(page.Objects)-1].Key
	}
	return page, nil
}

func (c *FileObjectClient) StartMultipart(context.Context, string, [32]byte) (string, error) {
	return "", objectstore.ErrMultipartUnsupported
}
func (c *FileObjectClient) UploadPart(context.Context, string, string, int32, io.Reader, int64) (objectstore.UploadedPart, error) {
	return objectstore.UploadedPart{}, objectstore.ErrMultipartUnsupported
}
func (c *FileObjectClient) CompleteMultipart(context.Context, string, string, []objectstore.UploadedPart) error {
	return objectstore.ErrMultipartUnsupported
}
func (c *FileObjectClient) AbortMultipart(context.Context, string, string) error {
	return objectstore.ErrMultipartUnsupported
}
func (c *FileObjectClient) ReconcileMultipart(context.Context, string) error { return nil }

func (c *FileObjectClient) objectPath(key string) (string, error) {
	if key == "" || filepath.IsAbs(key) || strings.Contains(key, "\\") {
		return "", objectError("path", key, objectstore.ErrorInvalidResponse, errors.New("invalid key"))
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(key)))
	if clean != key || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", objectError("path", key, objectstore.ErrorInvalidResponse, errors.New("unsafe key"))
	}
	path := filepath.Join(c.root, filepath.FromSlash(key))
	relative, err := filepath.Rel(c.root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", objectError("path", key, objectstore.ErrorInvalidResponse, errors.New("escaped key"))
	}
	return path, nil
}

func objectError(op, key string, kind objectstore.ErrorKind, cause error) error {
	return &objectstore.Error{Op: op, Key: key, Kind: kind, Err: cause}
}

type fileCatalogState struct {
	Version      uint16                          `json:"version"`
	Publications []catalog.RawSegmentPublication `json:"publications"`
	Orphans      []catalog.ObjectOrphan          `json:"orphans"`
}

type FileCatalog struct {
	mu    sync.Mutex
	root  string
	path  string
	state fileCatalogState
}

func OpenFileCatalog(root string) (*FileCatalog, error) {
	if err := requireRealDirectory(root); err != nil {
		return nil, err
	}
	directory := filepath.Join(root, "catalog")
	if err := ensureContainedDirectory(root, directory); err != nil {
		return nil, err
	}
	store := &FileCatalog{root: directory, path: filepath.Join(directory, "raw-publications-v1.json"), state: fileCatalogState{Version: 1}}
	encoded, err := os.ReadFile(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&store.state); err != nil || store.state.Version != 1 {
		return nil, errors.New("verify: invalid fixture publication catalog")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("verify: fixture publication catalog has trailing JSON")
	}
	for i := range store.state.Publications {
		store.state.Publications[i].ManifestBytes = slices.Clone(store.state.Publications[i].ManifestBytes)
	}
	return store, nil
}

func (s *FileCatalog) FindRawSegment(ctx context.Context, objectKey string) (catalog.RawSegmentPublication, bool, error) {
	if err := ctx.Err(); err != nil {
		return catalog.RawSegmentPublication{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, publication := range s.state.Publications {
		if publication.ObjectKey == objectKey {
			publication.ManifestBytes = slices.Clone(publication.ManifestBytes)
			return publication, true, nil
		}
	}
	return catalog.RawSegmentPublication{}, false, nil
}

func (s *FileCatalog) StreamCommittedRawSegments(ctx context.Context, visit func(catalog.RawSegmentPublication) error) error {
	if visit == nil {
		return catalog.ErrInvalidPublication
	}
	s.mu.Lock()
	publications := slices.Clone(s.state.Publications)
	s.mu.Unlock()
	slices.SortFunc(publications, func(left, right catalog.RawSegmentPublication) int {
		return strings.Compare(left.SegmentID, right.SegmentID)
	})
	for _, publication := range publications {
		if err := ctx.Err(); err != nil {
			return err
		}
		if publication.State != catalog.RawSegmentCommitted {
			continue
		}
		publication.ManifestBytes = slices.Clone(publication.ManifestBytes)
		if err := visit(publication); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileCatalog) RecordVerified(ctx context.Context, record catalog.RawSegmentPublication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.state.Publications {
		existing := s.state.Publications[i]
		if existing.ObjectKey != record.ObjectKey {
			continue
		}
		if !samePublication(existing, record) || existing.State == catalog.RawSegmentQuarantined || existing.State == catalog.RawSegmentSuperseded {
			return catalog.ErrPublicationConflict
		}
		if existing.State == catalog.RawSegmentPending {
			s.state.Publications[i].State = catalog.RawSegmentVerified
			return s.persistLocked()
		}
		return nil
	}
	record.State = catalog.RawSegmentVerified
	record.ManifestBytes = slices.Clone(record.ManifestBytes)
	s.state.Publications = append(s.state.Publications, record)
	return s.persistLocked()
}

func (s *FileCatalog) CommitRawSegment(ctx context.Context, objectKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.state.Publications {
		if s.state.Publications[i].ObjectKey != objectKey {
			continue
		}
		if s.state.Publications[i].State == catalog.RawSegmentCommitted {
			return nil
		}
		if s.state.Publications[i].State != catalog.RawSegmentVerified {
			return catalog.ErrPublicationState
		}
		s.state.Publications[i].State = catalog.RawSegmentCommitted
		return s.persistLocked()
	}
	return catalog.ErrInvalidPublication
}

func (s *FileCatalog) QuarantineRawSegment(ctx context.Context, objectKey, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.state.Publications {
		if s.state.Publications[i].ObjectKey == objectKey {
			if s.state.Publications[i].State == catalog.RawSegmentCommitted {
				return catalog.ErrPublicationState
			}
			s.state.Publications[i].State = catalog.RawSegmentQuarantined
			return s.persistLocked()
		}
	}
	return catalog.ErrInvalidPublication
}

func (s *FileCatalog) RecordObjectOrphan(ctx context.Context, orphan catalog.ObjectOrphan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.state.Orphans {
		if existing == orphan {
			return nil
		}
	}
	s.state.Orphans = append(s.state.Orphans, orphan)
	slices.SortFunc(s.state.Orphans, func(left, right catalog.ObjectOrphan) int { return strings.Compare(left.ObjectKey, right.ObjectKey) })
	return s.persistLocked()
}

func samePublication(left, right catalog.RawSegmentPublication) bool {
	left.State = ""
	right.State = ""
	return left.SegmentID == right.SegmentID && left.SourceID == right.SourceID && left.ChannelID == right.ChannelID &&
		left.EpochID == right.EpochID && left.ReceivedStartNS == right.ReceivedStartNS && left.ReceivedEndNS == right.ReceivedEndNS &&
		left.OrdinalStart == right.OrdinalStart && left.OrdinalEnd == right.OrdinalEnd && left.ObjectKey == right.ObjectKey &&
		left.ContentSHA256 == right.ContentSHA256 && left.ByteLength == right.ByteLength && left.ManifestVersion == right.ManifestVersion &&
		left.ManifestSHA256 == right.ManifestSHA256 && bytes.Equal(left.ManifestBytes, right.ManifestBytes)
}

func (s *FileCatalog) persistLocked() error {
	slices.SortFunc(s.state.Publications, func(left, right catalog.RawSegmentPublication) int {
		return strings.Compare(left.SegmentID, right.SegmentID)
	})
	encoded, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(s.root, ".catalog-*.tmp")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, s.path); err != nil {
		return err
	}
	return syncDirectory(s.root)
}

func requireRealDirectory(path string) error {
	if path == "" || !filepath.IsAbs(path) {
		return errors.New("verify: explicit absolute root is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("verify: configured root must already exist as a real directory")
	}
	return nil
}

func ensureContainedDirectory(root, directory string) error {
	relative, err := filepath.Rel(root, directory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("verify: directory escaped configured root")
	}
	current := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		if err := os.Mkdir(current, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("verify: directory component is not a real directory")
		}
	}
	return nil
}

func hashRegularFile(path string) ([sha256.Size]byte, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, 0, err
	}
	defer file.Close()
	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return [sha256.Size]byte{}, 0, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, size, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

var _ objectstore.Client = (*FileObjectClient)(nil)
var _ PublicationCatalog = (*FileCatalog)(nil)
