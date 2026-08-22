package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/spf13/pathologize"
)

const (
	maximumManifestBytes          int64 = 4 << 20
	minimumS3PartBytes            int64 = 5 << 20
	defaultPartBytes              int64 = 8 << 20
	maximumMultipartParts         int64 = 10_000
	defaultReconcilePages               = 8
	defaultReconcileObjects             = 4_096
	defaultReconcileResults             = 8_192
	maximumReconcilePages               = 1_024
	maximumReconcileObjects             = 1_000_000
	maximumReconcileResults             = 2_000_000
	maximumContinuationTokenBytes       = 16 << 10
)

type PublicationCatalog interface {
	FindRawSegment(context.Context, string) (catalog.RawSegmentPublication, bool, error)
	RecordVerified(context.Context, catalog.RawSegmentPublication) error
	CommitRawSegment(context.Context, string) error
	QuarantineRawSegment(context.Context, string, string) error
	RecordObjectOrphan(context.Context, catalog.ObjectOrphan) error
}

var _ PublicationCatalog = (*catalog.PublicationStore)(nil)

type ReconcilePolicy struct {
	MaxPages   int
	MaxObjects int
	MaxResults int
}

func (p ReconcilePolicy) normalized() (ReconcilePolicy, error) {
	if p == (ReconcilePolicy{}) {
		return ReconcilePolicy{
			MaxPages:   defaultReconcilePages,
			MaxObjects: defaultReconcileObjects,
			MaxResults: defaultReconcileResults,
		}, nil
	}
	if p.MaxPages <= 0 ||
		p.MaxObjects <= 0 ||
		p.MaxResults < 2 ||
		p.MaxPages > maximumReconcilePages ||
		p.MaxObjects > maximumReconcileObjects ||
		p.MaxResults > maximumReconcileResults {
		return ReconcilePolicy{}, fmt.Errorf("objectstore: invalid reconciliation bounds")
	}
	return p, nil
}

type PublisherConfig struct {
	Verify             VerifyPolicy
	Reconciler         ImmutableCreateReconciler
	MultipartThreshold int64
	MultipartPartBytes int64
	Reconcile          ReconcilePolicy
}

type Publisher struct {
	client  Client
	catalog PublicationCatalog
	config  PublisherConfig
}

// PublishRequest binds the caller-assigned catalog identity to a ReadySegment
// returned by segment.Spool.Ready or segment.Spool.VerifyReady.
type PublishRequest struct {
	SegmentID string
	Ready     segment.ReadySegment
}

type PublishResult struct {
	Object    ObjectInfo
	Recovered bool
	State     catalog.RawSegmentState
}

type ReconcileResult struct {
	Published          []string
	Committed          []string
	Quarantined        []string
	Orphans            []string
	ContinuationCursor string
	Complete           bool
}

func NewPublisher(client Client, publicationCatalog PublicationCatalog, config PublisherConfig) (*Publisher, error) {
	if client == nil || publicationCatalog == nil {
		return nil, fmt.Errorf("objectstore: client and publication catalog are required")
	}
	verify, err := config.Verify.normalized()
	if err != nil {
		return nil, err
	}
	config.Verify = verify
	reconcile, err := config.Reconcile.normalized()
	if err != nil {
		return nil, err
	}
	config.Reconcile = reconcile
	if config.MultipartThreshold < 0 || config.MultipartPartBytes < 0 {
		return nil, fmt.Errorf("objectstore: multipart bounds must not be negative")
	}
	if config.MultipartThreshold > 0 {
		if config.MultipartPartBytes == 0 {
			config.MultipartPartBytes = defaultPartBytes
		}
		if config.MultipartPartBytes < minimumS3PartBytes {
			return nil, fmt.Errorf("objectstore: multipart part size must be at least %d", minimumS3PartBytes)
		}
	}
	return &Publisher{client: client, catalog: publicationCatalog, config: config}, nil
}

func (p *Publisher) Publish(ctx context.Context, request PublishRequest) (PublishResult, error) {
	prepared, err := preparePublication(request)
	if err != nil {
		return PublishResult{}, err
	}
	defer prepared.file.Close()

	object := PutObject{
		Key:    prepared.record.ObjectKey,
		Body:   prepared.file,
		Size:   prepared.record.ByteLength,
		SHA256: prepared.record.ContentSHA256,
	}
	usedMultipart := p.config.MultipartThreshold > 0 && object.Size >= p.config.MultipartThreshold
	var createErr error
	if usedMultipart {
		createErr = p.putMultipart(ctx, prepared.file, object)
	} else {
		createErr = p.putImmutable(ctx, object)
	}
	if errors.Is(createErr, ErrProviderDisqualified) {
		return PublishResult{}, createErr
	}

	info, verifyErr := VerifyObject(
		ctx,
		p.client,
		object.Key,
		object.Size,
		object.SHA256,
		prepared.file,
		p.config.Verify,
	)
	if verifyErr != nil {
		if usedMultipart {
			verifyErr = errors.Join(verifyErr, p.client.ReconcileMultipart(ctx, object.Key))
		}
		if createErr != nil {
			return PublishResult{}, fmt.Errorf("objectstore: create and verification failed: %w", errors.Join(createErr, verifyErr))
		}
		return PublishResult{}, verifyErr
	}
	if usedMultipart && createErr != nil {
		if err := p.client.ReconcileMultipart(ctx, object.Key); err != nil {
			return PublishResult{}, fmt.Errorf("objectstore: reconcile incomplete multipart after exact object recovery: %w", err)
		}
	}

	if err := p.catalog.RecordVerified(ctx, prepared.record); err != nil {
		return PublishResult{}, fmt.Errorf("objectstore: record verified segment: %w", err)
	}
	if err := p.catalog.CommitRawSegment(ctx, object.Key); err != nil {
		return PublishResult{}, fmt.Errorf("objectstore: commit verified segment: %w", err)
	}
	return PublishResult{
		Object:    info,
		Recovered: createErr != nil,
		State:     catalog.RawSegmentCommitted,
	}, nil
}

type preparedPublication struct {
	file   *os.File
	record catalog.RawSegmentPublication
}

func preparePublication(request PublishRequest) (preparedPublication, error) {
	segmentID, valid := canonicalCatalogUUID(request.SegmentID)
	if !valid {
		return preparedPublication{}, fmt.Errorf("objectstore: catalog segment ID must be a UUID")
	}
	ready := request.Ready
	manifest := ready.Manifest
	if ready.SegmentPath == "" || ready.ManifestPath == "" || manifest.ManifestVersion != segment.SpoolManifestVersion || manifest.Segment.FormatVersion != segment.FormatVersion || manifest.SourceID == "" || manifest.ChannelID == "" || manifest.EpochID == "" || manifest.ObjectKey == "" {
		return preparedPublication{}, fmt.Errorf("objectstore: incomplete or unsupported ready segment")
	}
	sourceID, valid := canonicalCatalogUUID(manifest.SourceID)
	if !valid ||
		sourceID != manifest.SourceID ||
		len(manifest.SourceID) > segment.MaxSourceIDBytes ||
		len(manifest.ChannelID) > segment.MaxContractIDBytes ||
		!pathologize.IsClean(manifest.SourceID) ||
		!pathologize.IsClean(manifest.ChannelID) ||
		manifest.SourceID == "." ||
		manifest.SourceID == ".." ||
		manifest.ChannelID == "." ||
		manifest.ChannelID == ".." {
		return preparedPublication{}, fmt.Errorf("objectstore: source or channel is not a validated catalog ID")
	}
	epochID, valid := canonicalCatalogUUID(manifest.EpochID)
	if !valid {
		return preparedPublication{}, fmt.Errorf("objectstore: epoch ID must be a UUID")
	}
	if manifest.Segment.FirstReceivedAtNS < 0 ||
		manifest.Segment.LastReceivedAtNS < manifest.Segment.FirstReceivedAtNS ||
		manifest.Segment.LastOrdinal < manifest.Segment.FirstOrdinal {
		return preparedPublication{}, fmt.Errorf("objectstore: invalid ready segment bounds")
	}
	if manifest.Segment.RecordCount == 0 || manifest.Segment.CompressedBytes == 0 {
		return preparedPublication{}, fmt.Errorf("objectstore: ready segment is empty")
	}
	if expectedKey, err := publicationObjectKey(manifest); err != nil {
		return preparedPublication{}, err
	} else if manifest.ObjectKey != expectedKey {
		return preparedPublication{}, fmt.Errorf("objectstore: ready object key disagrees with segment identity")
	}

	file, err := os.Open(ready.SegmentPath)
	if err != nil {
		return preparedPublication{}, fmt.Errorf("objectstore: open closed segment: %w", err)
	}
	failed := true
	defer func() {
		if failed {
			file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return preparedPublication{}, fmt.Errorf("objectstore: stat closed segment: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() != int64(manifest.Segment.CompressedBytes) {
		return preparedPublication{}, fmt.Errorf("%w: closed segment has %d bytes, manifest declares %d", ErrSizeMismatch, info.Size(), manifest.Segment.CompressedBytes)
	}
	hasher := sha256.New()
	read, err := io.Copy(hasher, file)
	if err != nil {
		return preparedPublication{}, fmt.Errorf("objectstore: hash closed segment: %w", err)
	}
	if read != info.Size() {
		return preparedPublication{}, fmt.Errorf("%w: hashed %d of %d local bytes", ErrSizeMismatch, read, info.Size())
	}
	var contentHash [32]byte
	copy(contentHash[:], hasher.Sum(nil))
	if contentHash != manifest.Segment.CompressedSHA256 {
		return preparedPublication{}, fmt.Errorf("%w: closed segment differs from manifest", ErrHashMismatch)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return preparedPublication{}, fmt.Errorf("objectstore: rewind closed segment: %w", err)
	}

	manifestBytes, err := readManifest(ready)
	if err != nil {
		return preparedPublication{}, err
	}
	failed = false
	return preparedPublication{
		file: file,
		record: catalog.RawSegmentPublication{
			SegmentID:       segmentID,
			SourceID:        sourceID,
			ChannelID:       manifest.ChannelID,
			EpochID:         epochID,
			ReceivedStartNS: manifest.Segment.FirstReceivedAtNS,
			ReceivedEndNS:   manifest.Segment.LastReceivedAtNS,
			OrdinalStart:    manifest.Segment.FirstOrdinal,
			OrdinalEnd:      manifest.Segment.LastOrdinal,
			ObjectKey:       manifest.ObjectKey,
			ContentSHA256:   contentHash,
			ByteLength:      info.Size(),
			ManifestVersion: manifest.ManifestVersion,
			ManifestSHA256:  ready.ManifestSHA256,
			ManifestBytes:   manifestBytes,
			State:           catalog.RawSegmentVerified,
		},
	}, nil
}

func readManifest(ready segment.ReadySegment) ([]byte, error) {
	file, err := os.Open(ready.ManifestPath)
	if err != nil {
		return nil, fmt.Errorf("objectstore: open ready manifest: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("objectstore: stat ready manifest: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumManifestBytes || uint64(info.Size()) != ready.ManifestBytes {
		return nil, fmt.Errorf("objectstore: ready manifest size is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximumManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("objectstore: read ready manifest: %w", err)
	}
	hash := sha256.Sum256(data)
	if hash != ready.ManifestSHA256 {
		return nil, fmt.Errorf("%w: ready manifest bytes changed", ErrHashMismatch)
	}
	canonical, err := json.Marshal(ready.Manifest)
	if err != nil {
		return nil, fmt.Errorf("objectstore: marshal ready manifest identity: %w", err)
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return nil, fmt.Errorf("%w: ready manifest sidecar disagrees with supplied identity", ErrHashMismatch)
	}
	return data, nil
}

func publicationObjectKey(manifest segment.ReadyManifest) (string, error) {
	if len(manifest.EpochID) != 32 {
		return "", fmt.Errorf("objectstore: epoch ID is not 16-byte hexadecimal")
	}
	epoch, err := hex.DecodeString(manifest.EpochID)
	if err != nil || len(epoch) != 16 {
		return "", fmt.Errorf("objectstore: epoch ID is not 16-byte hexadecimal")
	}
	if !pathologize.IsClean(manifest.SourceID) || strings.ContainsAny(manifest.SourceID, "/\\") || manifest.SourceID == "." || manifest.SourceID == ".." {
		return "", fmt.Errorf("objectstore: source ID is not a validated path part")
	}
	first := time.Unix(0, manifest.Segment.FirstReceivedAtNS).UTC()
	return fmt.Sprintf(
		"raw/v1/source=%s/date=%s/hour=%02d/epoch=%x/segment=%d-%d-%x.emseg.zst",
		manifest.SourceID,
		first.Format("2006-01-02"),
		first.Hour(),
		epoch,
		manifest.Segment.FirstOrdinal,
		manifest.Segment.LastOrdinal,
		manifest.Segment.CompressedSHA256,
	), nil
}

func canonicalCatalogUUID(value string) (string, bool) {
	if len(value) == 36 {
		if value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
			return "", false
		}
		value = strings.ReplaceAll(value, "-", "")
	}
	if len(value) != 32 {
		return "", false
	}
	value = strings.ToLower(value)
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return "", false
	}
	return value[0:8] + "-" + value[8:12] + "-" + value[12:16] + "-" + value[16:20] + "-" + value[20:32], true
}

func (p *Publisher) putImmutable(ctx context.Context, object PutObject) error {
	err := p.client.PutIfAbsent(ctx, object)
	if !errors.Is(err, ErrConditionalCreateUnsupported) {
		return err
	}
	if p.config.Reconciler == nil {
		return errors.Join(ErrProviderDisqualified, err)
	}
	if _, seekErr := object.Body.Seek(0, io.SeekStart); seekErr != nil {
		return fmt.Errorf("objectstore: rewind for provider reconciler: %w", seekErr)
	}
	return p.config.Reconciler.CreateImmutable(ctx, object)
}

func (p *Publisher) putMultipart(ctx context.Context, file *os.File, object PutObject) error {
	partCount := (object.Size-1)/p.config.MultipartPartBytes + 1
	if object.Size <= 0 || partCount > maximumMultipartParts {
		return fmt.Errorf("objectstore: multipart object requires %d parts; maximum is %d", partCount, maximumMultipartParts)
	}
	uploadID, err := p.client.StartMultipart(ctx, object.Key, object.SHA256)
	if err != nil {
		return errors.Join(err, p.client.ReconcileMultipart(ctx, object.Key))
	}
	abort := func(cause error) error {
		abortErr := p.client.AbortMultipart(ctx, object.Key, uploadID)
		if errors.Is(abortErr, ErrNotFound) {
			abortErr = nil
		}
		reconcileErr := p.client.ReconcileMultipart(ctx, object.Key)
		return errors.Join(cause, abortErr, reconcileErr)
	}

	parts := make([]UploadedPart, 0, int(partCount))
	for offset, number := int64(0), int32(1); offset < object.Size; number++ {
		partBytes := min(p.config.MultipartPartBytes, object.Size-offset)
		part, err := p.client.UploadPart(
			ctx,
			object.Key,
			uploadID,
			number,
			io.NewSectionReader(file, offset, partBytes),
			partBytes,
		)
		if err != nil {
			return abort(err)
		}
		parts = append(parts, part)
		offset += partBytes
	}
	if len(parts) == 0 {
		return abort(fmt.Errorf("objectstore: multipart selected for empty object"))
	}
	if err := p.client.CompleteMultipart(ctx, object.Key, uploadID, parts); err != nil {
		if errors.Is(err, ErrConditionalCreateUnsupported) {
			if p.config.Reconciler == nil {
				return errors.Join(ErrProviderDisqualified, abort(err))
			}
			if cleanupErr := abort(nil); cleanupErr != nil {
				return cleanupErr
			}
			if _, seekErr := object.Body.Seek(0, io.SeekStart); seekErr != nil {
				return fmt.Errorf("objectstore: rewind for provider reconciler: %w", seekErr)
			}
			return p.config.Reconciler.CreateImmutable(ctx, object)
		}
		return err
	}
	return nil
}

func (p *Publisher) Reconcile(ctx context.Context, requests []PublishRequest, prefix string) (ReconcileResult, error) {
	return p.ReconcilePage(ctx, requests, prefix, "")
}

// ReconcilePage performs one configured, bounded reconciliation pass. Callers
// resume an incomplete pass by supplying ContinuationCursor unchanged with the
// same prefix and request slice.
func (p *Publisher) ReconcilePage(
	ctx context.Context,
	requests []PublishRequest,
	prefix string,
	cursor string,
) (ReconcileResult, error) {
	if !validRawPrefix(prefix) || len(prefix) > 1<<10 {
		return ReconcileResult{}, fmt.Errorf("objectstore: reconciliation prefix must be inside raw/v1")
	}
	position, err := decodeReconcileCursor(cursor, prefix, len(requests))
	if err != nil {
		return ReconcileResult{}, err
	}
	result := ReconcileResult{}
	processedObjects := 0

	if position.Phase == reconcilePhaseExpected {
		for index := position.RequestOffset; index < len(requests); index++ {
			if processedObjects >= p.config.Reconcile.MaxObjects ||
				reconcileResultCount(result)+2 > p.config.Reconcile.MaxResults {
				position.RequestOffset = index
				return finishReconcile(result, &position)
			}
			request := requests[index]
			key := request.Ready.Manifest.ObjectKey
			publication, err := p.Publish(ctx, request)
			if err != nil {
				return ReconcileResult{}, fmt.Errorf("objectstore: reconcile expected key %q: %w", key, err)
			}
			result.Published = append(result.Published, key)
			if publication.State == catalog.RawSegmentCommitted {
				result.Committed = append(result.Committed, key)
			}
			processedObjects++
		}
		position = reconcileCursor{Phase: reconcilePhaseListed, Prefix: prefix}
	}

	pages := 0
	for {
		if processedObjects >= p.config.Reconcile.MaxObjects ||
			reconcileResultCount(result)+2 > p.config.Reconcile.MaxResults ||
			pages >= p.config.Reconcile.MaxPages {
			return finishReconcile(result, &position)
		}
		if len(position.Token) > maximumContinuationTokenBytes {
			return ReconcileResult{}, fmt.Errorf("%w: object listing continuation is too large", ErrInvalidResponse)
		}
		page, err := p.client.List(ctx, prefix, position.Token)
		if err != nil {
			return ReconcileResult{}, fmt.Errorf("objectstore: list reconciliation prefix: %w", err)
		}
		pages++
		if len(page.Objects) > MaximumListPageObjects ||
			position.ObjectOffset < 0 ||
			position.ObjectOffset > len(page.Objects) {
			return ReconcileResult{}, fmt.Errorf("%w: object listing page exceeds reconciliation bounds", ErrInvalidResponse)
		}
		for index := position.ObjectOffset; index < len(page.Objects); index++ {
			if processedObjects >= p.config.Reconcile.MaxObjects ||
				reconcileResultCount(result)+2 > p.config.Reconcile.MaxResults {
				position.ObjectOffset = index
				return finishReconcile(result, &position)
			}
			if err := p.reconcileListed(ctx, page.Objects[index], &result); err != nil {
				return ReconcileResult{}, err
			}
			processedObjects++
		}
		position.ObjectOffset = 0
		if page.NextToken == "" {
			return finishReconcile(result, nil)
		}
		if page.NextToken == position.Token || len(page.NextToken) > maximumContinuationTokenBytes {
			return ReconcileResult{}, fmt.Errorf("%w: invalid repeated or oversized object listing continuation", ErrInvalidResponse)
		}
		position.Token = page.NextToken
	}
}

const (
	reconcilePhaseExpected = "expected"
	reconcilePhaseListed   = "listed"
)

type reconcileCursor struct {
	Phase         string `json:"phase"`
	Prefix        string `json:"prefix"`
	RequestOffset int    `json:"request_offset,omitempty"`
	Token         string `json:"token,omitempty"`
	ObjectOffset  int    `json:"object_offset,omitempty"`
}

func decodeReconcileCursor(encoded, prefix string, requestCount int) (reconcileCursor, error) {
	if encoded == "" {
		return reconcileCursor{Phase: reconcilePhaseExpected, Prefix: prefix}, nil
	}
	if len(encoded) > 2*maximumContinuationTokenBytes {
		return reconcileCursor{}, fmt.Errorf("objectstore: reconciliation cursor is too large")
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return reconcileCursor{}, fmt.Errorf("objectstore: decode reconciliation cursor: %w", err)
	}
	var cursor reconcileCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return reconcileCursor{}, fmt.Errorf("objectstore: parse reconciliation cursor: %w", err)
	}
	if cursor.Prefix != prefix ||
		(cursor.Phase != reconcilePhaseExpected && cursor.Phase != reconcilePhaseListed) ||
		cursor.RequestOffset < 0 ||
		cursor.RequestOffset > requestCount ||
		cursor.ObjectOffset < 0 ||
		len(cursor.Token) > maximumContinuationTokenBytes {
		return reconcileCursor{}, fmt.Errorf("objectstore: reconciliation cursor does not match this pass")
	}
	if cursor.Phase == reconcilePhaseExpected && (cursor.Token != "" || cursor.ObjectOffset != 0) {
		return reconcileCursor{}, fmt.Errorf("objectstore: invalid expected-publication cursor")
	}
	return cursor, nil
}

func finishReconcile(result ReconcileResult, cursor *reconcileCursor) (ReconcileResult, error) {
	slices.Sort(result.Published)
	slices.Sort(result.Committed)
	slices.Sort(result.Quarantined)
	slices.Sort(result.Orphans)
	if cursor == nil {
		result.Complete = true
		return result, nil
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("objectstore: marshal reconciliation cursor: %w", err)
	}
	result.ContinuationCursor = base64.RawURLEncoding.EncodeToString(data)
	if len(result.ContinuationCursor) > 2*maximumContinuationTokenBytes {
		return ReconcileResult{}, fmt.Errorf("objectstore: reconciliation cursor exceeds bound")
	}
	return result, nil
}

func reconcileResultCount(result ReconcileResult) int {
	return len(result.Published) + len(result.Committed) + len(result.Quarantined) + len(result.Orphans)
}

func (p *Publisher) reconcileListed(ctx context.Context, listed ListedObject, result *ReconcileResult) error {
	record, found, err := p.catalog.FindRawSegment(ctx, listed.Key)
	if err != nil {
		return fmt.Errorf("objectstore: find listed object in catalog: %w", err)
	}
	if !found {
		info, err := p.client.Head(ctx, listed.Key)
		if err != nil {
			return fmt.Errorf("objectstore: head orphan %q: %w", listed.Key, err)
		}
		hash, hasHash := applicationHash(info.Metadata)
		orphan := catalog.ObjectOrphan{
			ObjectKey:            listed.Key,
			ByteLength:           info.Size,
			ApplicationSHA256:    hash,
			HasApplicationSHA256: hasHash,
			Reason:               "object has no catalog or local manifest identity",
		}
		if err := p.catalog.RecordObjectOrphan(ctx, orphan); err != nil {
			return fmt.Errorf("objectstore: quarantine orphan %q: %w", listed.Key, err)
		}
		result.Orphans = append(result.Orphans, listed.Key)
		result.Quarantined = append(result.Quarantined, listed.Key)
		return nil
	}

	switch record.State {
	case catalog.RawSegmentCommitted, catalog.RawSegmentQuarantined, catalog.RawSegmentSuperseded:
		return nil
	case catalog.RawSegmentPending, catalog.RawSegmentVerified:
		if record.ManifestVersion == 0 || len(record.ManifestBytes) == 0 {
			if err := p.catalog.QuarantineRawSegment(ctx, record.ObjectKey, "catalog segment has no immutable manifest identity"); err != nil {
				return fmt.Errorf("objectstore: quarantine manifestless catalog segment %q: %w", record.ObjectKey, err)
			}
			result.Quarantined = append(result.Quarantined, record.ObjectKey)
			return nil
		}
		_, verifyErr := VerifyObject(ctx, p.client, record.ObjectKey, record.ByteLength, record.ContentSHA256, nil, p.config.Verify)
		if verifyErr != nil {
			reason := "listed object cannot be verified against its immutable catalog identity: " + verifyErr.Error()
			if err := p.catalog.QuarantineRawSegment(ctx, record.ObjectKey, reason); err != nil {
				return fmt.Errorf("objectstore: quarantine unverifiable catalog segment %q: %w", record.ObjectKey, err)
			}
			result.Quarantined = append(result.Quarantined, record.ObjectKey)
			return nil
		}
		if record.State == catalog.RawSegmentPending {
			if err := p.catalog.RecordVerified(ctx, record); err != nil {
				return fmt.Errorf("objectstore: advance reconciled segment to verified: %w", err)
			}
		}
		if err := p.catalog.CommitRawSegment(ctx, record.ObjectKey); err != nil {
			return fmt.Errorf("objectstore: commit reconciled verified segment: %w", err)
		}
		result.Committed = append(result.Committed, record.ObjectKey)
		return nil
	default:
		return fmt.Errorf("objectstore: unknown catalog segment state %q", record.State)
	}
}

func validRawPrefix(prefix string) bool {
	return prefix == "raw/v1/" ||
		(strings.HasPrefix(prefix, "raw/v1/source=") && !strings.Contains(prefix, "..") && !strings.Contains(prefix, "\\"))
}
