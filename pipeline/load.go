package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/warehouse"
	"github.com/spf13/fileflow"
	"github.com/spf13/pathologize"
)

type Loader struct {
	catalog   LoadCatalog
	objects   objectstore.Client
	warehouse WarehouseLoader
	config    LoaderConfig
}

func NewLoader(loadCatalog LoadCatalog, objects objectstore.Client, generationLoader WarehouseLoader, config LoaderConfig) (*Loader, error) {
	if loadCatalog == nil || objects == nil || generationLoader == nil {
		return nil, fmt.Errorf("%w: dataset catalog, object store, and warehouse loader are required", ErrInvalidPipeline)
	}
	normalized, err := validateLoaderConfig(config)
	if err != nil {
		return nil, err
	}
	return &Loader{catalog: loadCatalog, objects: objects, warehouse: generationLoader, config: normalized}, nil
}

func (l *Loader) RunOnce(ctx context.Context, request LoadRequest) (receipt LoadReceipt, err error) {
	if l == nil || ctx == nil {
		return receipt, fmt.Errorf("%w: loader and context are required", ErrInvalidPipeline)
	}
	if err := validateLoadRequest(request); err != nil {
		return receipt, err
	}
	receipt.DatasetID = request.DatasetID
	publication, found, err := l.catalog.FindDataset(ctx, request.DatasetID)
	if err != nil {
		return receipt, fmt.Errorf("pipeline: find committed dataset: %w", err)
	}
	if !found || publication.State != catalog.DatasetCommitted || publication.DatasetID != request.DatasetID {
		return receipt, fmt.Errorf("%w: %s", ErrDatasetNotCommitted, request.DatasetID)
	}
	if err := validateCommittedDataset(publication, l.config); err != nil {
		return receipt, err
	}
	committed, materialization, err := l.materialize(ctx, request.WorkRoot, publication)
	receipt.Materialization = materialization
	if err != nil {
		return receipt, err
	}
	warehouseReceipt, err := l.warehouse.Load(ctx, committed)
	receipt.Warehouse = warehouseReceipt
	if err != nil {
		return receipt, fmt.Errorf("pipeline: load committed dataset into warehouse: %w", err)
	}
	if warehouseReceipt.GenerationID == (warehouse.GenerationID{}) || warehouseReceipt.ManifestHash != warehouse.Hash(publication.ManifestHash) ||
		warehouseReceipt.InputHash != warehouse.Hash(publication.InputSegmentSetHash) || warehouseReceipt.ExpectedEventCount == 0 ||
		warehouseReceipt.ExpectedRowCount < warehouseReceipt.ExpectedEventCount {
		return receipt, fmt.Errorf("%w: warehouse returned incomplete or mismatched generation receipt", ErrPublicationConflict)
	}
	commit := catalog.DatasetGenerationCommit{
		DatasetID: request.DatasetID, GenerationID: [sha256.Size]byte(warehouseReceipt.GenerationID),
		ManifestHash: publication.ManifestHash, InputHash: [sha256.Size]byte(warehouseReceipt.InputHash),
		ExpectedEventCount: warehouseReceipt.ExpectedEventCount, ExpectedRowCount: warehouseReceipt.ExpectedRowCount,
		Family: publication.DatasetFamily, CatalogSnapshotID: publication.CatalogSnapshotHash,
		SchemaName: publication.SchemaName, SchemaVersion: publication.SchemaVersion,
	}
	if err := l.catalog.CommitDatasetGeneration(ctx, commit); err != nil {
		return receipt, fmt.Errorf("pipeline: bind warehouse generation to committed dataset: %w", err)
	}
	receipt.CoverageCount = len(publication.Coverage)
	receipt.GenerationBound = true
	receipt.Complete = true
	return receipt, nil
}

func validateLoadRequest(request LoadRequest) error {
	if !canonicalUUID(request.DatasetID) || request.WorkRoot == "" {
		return fmt.Errorf("%w: canonical dataset ID and explicit work root are required", ErrInvalidPipeline)
	}
	info, err := os.Lstat(request.WorkRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: work root must be an existing real directory", ErrInvalidPipeline)
	}
	return nil
}

func validateCommittedDataset(publication catalog.DatasetPublication, config LoaderConfig) error {
	if !canonicalUUID(publication.DatasetID) || publication.DatasetFamily == "" || publication.DatasetVersion == "" || publication.SourceID == "" ||
		publication.SchemaName == "" || publication.SchemaVersion == 0 || publication.ManifestVersion == 0 || publication.PartitionKey == "" ||
		publication.RangeStartNS < 0 || publication.RangeEndNS <= publication.RangeStartNS || publication.InputSegmentSetHash == ([sha256.Size]byte{}) ||
		publication.CatalogSnapshotHash == ([sha256.Size]byte{}) || publication.MapperSetHash == ([sha256.Size]byte{}) ||
		publication.LogicalHash == ([sha256.Size]byte{}) || publication.PhysicalHash == ([sha256.Size]byte{}) ||
		publication.ParquetObjectKey == "" || publication.ManifestObjectKey == "" || publication.ParquetBytes < 1 ||
		publication.ParquetBytes > config.MaxParquetBytes || publication.ManifestHash == ([sha256.Size]byte{}) ||
		len(publication.ManifestBytes) == 0 || len(publication.ManifestBytes) > dataset.MaxManifestBytes ||
		len(publication.InputSegmentIDs) == 0 || len(publication.Coverage) == 0 || len(publication.Coverage) > config.MaxCoverage {
		return fmt.Errorf("%w: committed dataset publication is incomplete or exceeds load bounds", ErrPublicationConflict)
	}
	if sha256.Sum256(publication.ManifestBytes) != publication.ManifestHash {
		return fmt.Errorf("%w: catalog manifest bytes differ from committed hash", ErrPublicationConflict)
	}
	if strings.IndexByte(publication.ParquetObjectKey, 0) >= 0 || strings.IndexByte(publication.ManifestObjectKey, 0) >= 0 {
		return fmt.Errorf("%w: object key contains NUL", ErrPublicationConflict)
	}
	return nil
}

func (l *Loader) materialize(ctx context.Context, workRoot string, publication catalog.DatasetPublication) (warehouse.CommittedManifest, MaterializationReceipt, error) {
	var manifest dataset.Manifest
	decoder := json.NewDecoder(bytes.NewReader(publication.ManifestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return warehouse.CommittedManifest{}, MaterializationReceipt{}, fmt.Errorf("%w: decode committed dataset manifest: %v", ErrPublicationConflict, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return warehouse.CommittedManifest{}, MaterializationReceipt{}, fmt.Errorf("%w: committed manifest trailing data", ErrPublicationConflict)
	}
	manifestRangeEnd := manifest.LastReceivedTimeNS + 1
	manifestDatasetID, identityErr := datasetIDFromBuildID(manifest.BuildID)
	if identityErr != nil || manifestRangeEnd <= manifest.LastReceivedTimeNS {
		return warehouse.CommittedManifest{}, MaterializationReceipt{}, fmt.Errorf("%w: invalid manifest dataset or half-open range identity", ErrPublicationConflict)
	}
	if manifestDatasetID != publication.DatasetID || manifest.PhysicalSHA256 != hex.EncodeToString(publication.PhysicalHash[:]) ||
		manifest.LogicalSHA256 != hex.EncodeToString(publication.LogicalHash[:]) ||
		manifest.InputManifestSetID != hex.EncodeToString(publication.InputSegmentSetHash[:]) ||
		manifest.FileBytes != publication.ParquetBytes || manifest.FirstReceivedTimeNS != publication.RangeStartNS ||
		manifestRangeEnd != publication.RangeEndNS || manifest.SchemaName != publication.SchemaName ||
		manifest.SchemaVersion != publication.SchemaVersion || string(manifest.Family) != publication.DatasetFamily ||
		manifest.SourceID != publication.SourceID || manifest.ManifestVersion != publication.ManifestVersion ||
		manifest.DatasetVersion != publication.DatasetVersion {
		return warehouse.CommittedManifest{}, MaterializationReceipt{}, fmt.Errorf("%w: catalog identity differs from manifest", ErrPublicationConflict)
	}
	parquetRelative, err := safeDatasetRelative(manifest.ParquetFile)
	if err != nil {
		return warehouse.CommittedManifest{}, MaterializationReceipt{}, err
	}
	root := pathologize.Join(workRoot, "dataset="+publication.DatasetID)
	parquetPath := filepath.Join(root, filepath.FromSlash(parquetRelative))
	manifestPath := filepath.Join(root, "manifest-"+hex.EncodeToString(publication.ManifestHash[:])+".json")
	if err := ensureRealDirectories(workRoot, filepath.Dir(parquetPath)); err != nil {
		return warehouse.CommittedManifest{}, MaterializationReceipt{}, err
	}
	if err := ensureRealDirectories(workRoot, filepath.Dir(manifestPath)); err != nil {
		return warehouse.CommittedManifest{}, MaterializationReceipt{}, err
	}
	receipt := MaterializationReceipt{Root: root, ManifestPath: manifestPath, ParquetPath: parquetPath,
		ParquetObject:  ObjectReceipt{Key: publication.ParquetObjectKey, SHA256: publication.PhysicalHash, Bytes: publication.ParquetBytes},
		ManifestObject: ObjectReceipt{Key: publication.ManifestObjectKey, SHA256: publication.ManifestHash, Bytes: int64(len(publication.ManifestBytes))}}

	parquetRecovered, err := l.downloadExact(ctx, workRoot, publication.ParquetObjectKey, parquetPath, publication.ParquetBytes, publication.PhysicalHash, nil)
	receipt.ParquetObject.Recovered = parquetRecovered
	if err != nil {
		return warehouse.CommittedManifest{}, receipt, err
	}
	manifestRecovered, err := l.downloadExact(ctx, workRoot, publication.ManifestObjectKey, manifestPath, int64(len(publication.ManifestBytes)), publication.ManifestHash, publication.ManifestBytes)
	receipt.ManifestObject.Recovered = manifestRecovered
	if err != nil {
		return warehouse.CommittedManifest{}, receipt, err
	}
	if _, err := dataset.VerifyManifest(ctx, root, manifestPath); err != nil {
		return warehouse.CommittedManifest{}, receipt, fmt.Errorf("pipeline: verify materialized committed dataset: %w", err)
	}
	return warehouse.CommittedManifest{Root: root, ManifestPath: manifestPath,
		ManifestSHA256: warehouse.Hash(publication.ManifestHash), State: warehouse.ManifestCommitted}, receipt, nil
}

func safeDatasetRelative(value string) (string, error) {
	if value == "" || strings.Contains(value, "\\") || !filepath.IsLocal(filepath.FromSlash(value)) || filepath.ToSlash(filepath.Clean(filepath.FromSlash(value))) != value {
		return "", fmt.Errorf("%w: manifest Parquet path is not clean and relative", ErrPublicationConflict)
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || !pathologize.IsClean(part) {
			return "", fmt.Errorf("%w: unsafe manifest Parquet path segment", ErrPublicationConflict)
		}
	}
	return value, nil
}

func (l *Loader) downloadExact(ctx context.Context, temporaryRoot, key, destination string, expectedBytes int64, expectedHash [sha256.Size]byte, exact []byte) (bool, error) {
	if recovered, err := verifyMaterializedFile(destination, expectedBytes, expectedHash, exact); recovered || err != nil {
		return recovered, err
	}
	body, err := l.objects.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("pipeline: open committed object %q: %w", key, err)
	}
	if body == nil {
		return false, fmt.Errorf("pipeline: object store returned nil body for %q", key)
	}
	temporary, err := os.CreateTemp(temporaryRoot, ".pipeline-download-*")
	if err != nil {
		_ = body.Close()
		return false, fmt.Errorf("pipeline: create materialization staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	keepTemporary := false
	defer func() {
		if !keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(temporary, hasher), io.LimitReader(body, expectedBytes+1))
	closeBodyErr := body.Close()
	if copyErr != nil || closeBodyErr != nil || written != expectedBytes {
		_ = temporary.Close()
		return false, fmt.Errorf("%w: object %q length/read mismatch: bytes=%d copy=%v close=%v", ErrPublicationConflict, key, written, copyErr, closeBodyErr)
	}
	var actualHash [sha256.Size]byte
	copy(actualHash[:], hasher.Sum(nil))
	if actualHash != expectedHash {
		_ = temporary.Close()
		return false, fmt.Errorf("%w: object %q SHA-256 mismatch", ErrPublicationConflict, key)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return false, fmt.Errorf("pipeline: sync materialization staging file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return false, fmt.Errorf("pipeline: close materialization staging file: %w", err)
	}
	if exact != nil {
		contents, err := os.ReadFile(temporaryPath)
		if err != nil || !bytes.Equal(contents, exact) {
			return false, fmt.Errorf("%w: object %q differs from exact catalog manifest bytes", ErrPublicationConflict, key)
		}
	}
	recovered := fileflow.Exists(destination)
	flow := fileflow.Flow{DirMode: 0o700, NoCreateDirs: true}
	finalPath, err := flow.Move(temporaryPath, destination)
	if err != nil {
		return false, fmt.Errorf("pipeline: publish materialized object %q: %w", key, err)
	}
	keepTemporary = true
	if finalPath != destination {
		_ = os.Remove(finalPath)
		return false, fmt.Errorf("%w: destination %q already has different bytes", ErrMaterializedConflict, destination)
	}
	return recovered, nil
}

func verifyMaterializedFile(path string, expectedBytes int64, expectedHash [sha256.Size]byte, exact []byte) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("pipeline: inspect existing materialization: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != expectedBytes {
		return true, fmt.Errorf("%w: existing materialization has the wrong type or length", ErrMaterializedConflict)
	}
	file, err := os.Open(path)
	if err != nil {
		return true, fmt.Errorf("pipeline: open existing materialization: %w", err)
	}
	defer file.Close()
	hasher := sha256.New()
	var contents bytes.Buffer
	writer := io.Writer(hasher)
	if exact != nil {
		writer = io.MultiWriter(hasher, &contents)
	}
	written, err := io.Copy(writer, io.LimitReader(file, expectedBytes+1))
	if err != nil || written != expectedBytes {
		return true, fmt.Errorf("%w: read existing materialization", ErrMaterializedConflict)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	if digest != expectedHash || exact != nil && !bytes.Equal(contents.Bytes(), exact) {
		return true, fmt.Errorf("%w: existing materialization bytes differ", ErrMaterializedConflict)
	}
	return true, nil
}

func ensureRealDirectories(root, target string) error {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absoluteTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(absoluteRoot, absoluteTarget)
	if err != nil || (relative != "." && !filepath.IsLocal(relative)) {
		return fmt.Errorf("%w: materialization directory escapes work root", ErrMaterializedConflict)
	}
	current := absoluteRoot
	if relative == "." {
		return nil
	}
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		if err := os.Mkdir(current, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("pipeline: create materialization directory: %w", err)
		}
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: materialization component is not a real directory", ErrMaterializedConflict)
		}
	}
	return nil
}

func canonicalUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || strings.ToLower(value) != value {
		return false
	}
	raw := value[:8] + value[9:13] + value[14:18] + value[19:23] + value[24:]
	decoded, err := hex.DecodeString(raw)
	return err == nil && len(decoded) == 16 && !bytes.Equal(decoded, make([]byte, 16))
}
