package objectstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const MaximumCompatibilityTransfer int64 = 2 << 30

const compatibilityCleanupTimeout = 5 * time.Second

var DefaultCompatibilitySizes = []int64{
	0,
	1,
	(4 << 10) - 1,
	4 << 10,
	(4 << 10) + 1,
	(64 << 10) - 1,
	64 << 10,
	(64 << 10) + 1,
	1 << 20,
	8 << 20,
	64 << 20,
}

type CompatibilityFaults interface {
	SetClockSkew(context.Context, time.Duration) error
	DropNextCreateResponse(context.Context) error
}

type MultipartDecision struct {
	Enabled               bool
	ThresholdBytes        int64
	MeasuredThroughputWin bool
	RetryabilityPreserved bool
}

type CompatibilityProvider struct {
	Name       string
	Client     Client
	Reconciler ImmutableCreateReconciler
	Faults     CompatibilityFaults
	Multipart  MultipartDecision
}

type CompatibilityConfig struct {
	Prefix string
	Sizes  []int64
	Verify VerifyPolicy
}

type CompatibilityCase struct {
	Name   string
	Passed bool
	Detail string
}

type CompatibilityReport struct {
	Provider             string
	Qualified            bool
	Disqualification     string
	Cases                []CompatibilityCase
	SinglePUTDefault     bool
	MultipartEnabled     bool
	MultipartThreshold   int64
	MaximumTransferBytes int64
}

// RunCompatibility executes the deterministic X3 contract against an injected
// provider. It performs no discovery of credentials or endpoints. A local fake,
// emulator, or caller-owned compatibility endpoint must supply all behavior.
func RunCompatibility(ctx context.Context, provider CompatibilityProvider, config CompatibilityConfig) (CompatibilityReport, error) {
	report := CompatibilityReport{
		Provider:             provider.Name,
		Qualified:            true,
		SinglePUTDefault:     true,
		MultipartEnabled:     provider.Multipart.Enabled,
		MultipartThreshold:   provider.Multipart.ThresholdBytes,
		MaximumTransferBytes: MaximumCompatibilityTransfer,
	}
	if strings.TrimSpace(provider.Name) == "" || provider.Client == nil {
		return report, fmt.Errorf("objectstore: compatibility provider name and client are required")
	}
	if config.Prefix == "" || strings.Contains(config.Prefix, "..") {
		return report, fmt.Errorf("objectstore: isolated compatibility prefix is required")
	}
	policy, err := config.Verify.normalized()
	if err != nil {
		return report, err
	}
	config.Verify = policy
	if len(config.Sizes) == 0 {
		config.Sizes = append([]int64(nil), DefaultCompatibilitySizes...)
	}
	rangeProbeBytes := policy.FullReadLimit + 3*policy.SampleBytes + 1
	estimatedTransfer := int64(3*(64<<10) + 7 + 2*257 + 2*513)
	if rangeProbeBytes <= policy.FullReadLimit || rangeProbeBytes > 64<<20 {
		return report, fmt.Errorf("objectstore: range probe exceeds X3 bounds")
	}
	estimatedTransfer += rangeProbeBytes + int64(policy.SampleCount)*min(rangeProbeBytes, policy.SampleBytes)
	for _, size := range config.Sizes {
		if size < 0 || size > 64<<20 {
			return report, fmt.Errorf("objectstore: compatibility sizes exceed X3 bounds")
		}
		verificationBytes := size
		if size > policy.FullReadLimit {
			verificationBytes = int64(policy.SampleCount) * min(size, policy.SampleBytes)
		}
		objectTransfer := size + verificationBytes
		if objectTransfer < 0 || estimatedTransfer > MaximumCompatibilityTransfer-objectTransfer {
			return report, fmt.Errorf("objectstore: compatibility transfer exceeds 2 GiB X3 bound")
		}
		estimatedTransfer += objectTransfer
	}
	if provider.Multipart.Enabled &&
		(provider.Multipart.ThresholdBytes <= 0 || !provider.Multipart.MeasuredThroughputWin || !provider.Multipart.RetryabilityPreserved) {
		disqualify(&report, "multipart was enabled without measured throughput and retryability evidence")
		return report, nil
	}

	for index, size := range config.Sizes {
		key := compatibilityKey(config.Prefix, fmt.Sprintf("size-%02d-%d", index, size))
		source := newDeterministicSource(size, byte(index+1))
		hash, err := sourceHash(source)
		if err != nil {
			return report, err
		}
		object := PutObject{Key: key, Body: source, Size: size, SHA256: hash}
		if err := compatibilityCreate(ctx, provider, object); err != nil {
			disqualify(&report, fmt.Sprintf("conditional create size %d: %v", size, err))
			return report, nil
		}
		if _, err := VerifyObject(ctx, provider.Client, key, size, hash, source, policy); err != nil {
			disqualify(&report, fmt.Sprintf("verification size %d: %v", size, err))
			return report, nil
		}
		report.Cases = append(report.Cases, CompatibilityCase{
			Name:   fmt.Sprintf("size_%d", size),
			Passed: true,
			Detail: "conditional create and application SHA-256 verification passed",
		})
	}

	if err := compatibilityRace(ctx, provider, config.Prefix, policy); err != nil {
		disqualify(&report, "conditional race: "+err.Error())
		return report, nil
	}
	report.Cases = append(report.Cases, CompatibilityCase{Name: "conditional_race", Passed: true, Detail: "exactly one immutable create won"})

	if err := compatibilityRange(ctx, provider, config.Prefix, policy); err != nil {
		disqualify(&report, "range read: "+err.Error())
		return report, nil
	}
	report.Cases = append(report.Cases, CompatibilityCase{Name: "range_read", Passed: true, Detail: "bounded samples matched local bytes"})

	if err := compatibilityMultipartAbort(ctx, provider.Client, config.Prefix); err != nil {
		disqualify(&report, "multipart abort: "+err.Error())
		return report, nil
	}
	report.Cases = append(report.Cases, CompatibilityCase{Name: "multipart_abort", Passed: true, Detail: "incomplete upload was removed and reconciled"})

	if provider.Faults == nil {
		disqualify(&report, "provider matrix has no deterministic skew/dropped-response fault controller")
		return report, nil
	}
	if err := compatibilitySkew(ctx, provider, config.Prefix, policy); err != nil {
		disqualify(&report, "clock skew: "+err.Error())
		return report, nil
	}
	report.Cases = append(report.Cases, CompatibilityCase{Name: "clock_skew", Passed: true, Detail: "server timestamps did not participate in identity"})

	if err := compatibilityDroppedResponse(ctx, provider, config.Prefix, policy); err != nil {
		disqualify(&report, "dropped response: "+err.Error())
		return report, nil
	}
	report.Cases = append(report.Cases, CompatibilityCase{Name: "dropped_response", Passed: true, Detail: "existing exact bytes reconciled after lost response"})

	decision := "single PUT remains the default; multipart is disabled"
	if provider.Multipart.Enabled {
		decision = fmt.Sprintf("multipart enabled at %d bytes from measured evidence", provider.Multipart.ThresholdBytes)
	}
	report.Cases = append(report.Cases, CompatibilityCase{Name: "multipart_decision", Passed: true, Detail: decision})
	return report, nil
}

func compatibilityCreate(ctx context.Context, provider CompatibilityProvider, object PutObject) error {
	if _, err := object.Body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	err := provider.Client.PutIfAbsent(ctx, object)
	if !errors.Is(err, ErrConditionalCreateUnsupported) {
		return err
	}
	if provider.Reconciler == nil {
		return errors.Join(ErrProviderDisqualified, err)
	}
	if _, err := object.Body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return provider.Reconciler.CreateImmutable(ctx, object)
}

func compatibilityRace(ctx context.Context, provider CompatibilityProvider, prefix string, policy VerifyPolicy) error {
	key := compatibilityKey(prefix, "conditional-race")
	left := newDeterministicSource(64<<10, 0x51)
	right := newDeterministicSource(64<<10, 0xa7)
	leftHash, err := sourceHash(left)
	if err != nil {
		return err
	}
	rightHash, err := sourceHash(right)
	if err != nil {
		return err
	}
	objects := []PutObject{
		{Key: key, Body: left, Size: left.size, SHA256: leftHash},
		{Key: key, Body: right, Size: right.size, SHA256: rightHash},
	}
	start := make(chan struct{})
	results := make(chan error, len(objects))
	var wait sync.WaitGroup
	for _, object := range objects {
		wait.Go(func() {
			<-start
			results <- compatibilityCreate(ctx, provider, object)
		})
	}
	close(start)
	wait.Wait()
	close(results)
	var winner int
	for result := range results {
		switch {
		case result == nil:
			winner++
		case errors.Is(result, ErrPreconditionFailed):
		default:
			return result
		}
	}
	if winner != 1 {
		return fmt.Errorf("got %d winners, want exactly one", winner)
	}
	info, err := provider.Client.Head(ctx, key)
	if err != nil {
		return err
	}
	remoteHash, ok := applicationHash(info.Metadata)
	if !ok {
		return ErrHashMismatch
	}
	for index, hash := range [][32]byte{leftHash, rightHash} {
		if remoteHash == hash {
			_, err := VerifyObject(ctx, provider.Client, key, objects[index].Size, hash, objects[index].Body.(io.ReaderAt), policy)
			return err
		}
	}
	return fmt.Errorf("%w: race winner metadata matches neither candidate", ErrHashMismatch)
}

func compatibilityRange(ctx context.Context, provider CompatibilityProvider, prefix string, policy VerifyPolicy) error {
	size := policy.FullReadLimit + 3*policy.SampleBytes + 1
	if size <= policy.FullReadLimit {
		return fmt.Errorf("invalid range probe size")
	}
	if size > 64<<20 {
		return fmt.Errorf("range probe exceeds 64 MiB X3 bound")
	}
	source := newDeterministicSource(size, 0x39)
	hash, err := sourceHash(source)
	if err != nil {
		return err
	}
	key := compatibilityKey(prefix, "range")
	object := PutObject{Key: key, Body: source, Size: size, SHA256: hash}
	if err := compatibilityCreate(ctx, provider, object); err != nil {
		return err
	}
	_, err = VerifyObject(ctx, provider.Client, key, size, hash, source, policy)
	return err
}

func compatibilityMultipartAbort(ctx context.Context, client Client, prefix string) error {
	key := compatibilityKey(prefix, "multipart-abort")
	hash := sha256.Sum256([]byte("aborted multipart must never become visible"))
	uploadID, err := client.StartMultipart(ctx, key, hash)
	if err != nil {
		return err
	}
	part, err := client.UploadPart(ctx, key, uploadID, 1, strings.NewReader("partial"), int64(len("partial")))
	if err != nil {
		return errors.Join(err, client.AbortMultipart(ctx, key, uploadID), client.ReconcileMultipart(ctx, key))
	}
	if part.Number != 1 {
		return errors.Join(fmt.Errorf("provider returned wrong part number"), client.AbortMultipart(ctx, key, uploadID))
	}
	if err := client.AbortMultipart(ctx, key, uploadID); err != nil {
		return err
	}
	if err := client.ReconcileMultipart(ctx, key); err != nil {
		return err
	}
	_, err = client.Head(ctx, key)
	if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("aborted multipart is visible: %w", err)
	}
	return nil
}

func compatibilitySkew(
	ctx context.Context,
	provider CompatibilityProvider,
	prefix string,
	policy VerifyPolicy,
) (err error) {
	if err := provider.Faults.SetClockSkew(ctx, 36*time.Hour); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compatibilityCleanupTimeout)
		defer cancel()
		if resetErr := provider.Faults.SetClockSkew(cleanupCtx, 0); resetErr != nil {
			err = errors.Join(err, fmt.Errorf("reset provider clock skew: %w", resetErr))
		}
	}()
	source := newDeterministicSource(257, 0x73)
	hash, err := sourceHash(source)
	if err != nil {
		return err
	}
	key := compatibilityKey(prefix, "clock-skew")
	object := PutObject{Key: key, Body: source, Size: source.size, SHA256: hash}
	if err := compatibilityCreate(ctx, provider, object); err != nil {
		return err
	}
	_, err = VerifyObject(ctx, provider.Client, key, source.size, hash, source, policy)
	return err
}

func compatibilityDroppedResponse(ctx context.Context, provider CompatibilityProvider, prefix string, policy VerifyPolicy) error {
	if err := provider.Faults.DropNextCreateResponse(ctx); err != nil {
		return err
	}
	source := newDeterministicSource(513, 0xb2)
	hash, err := sourceHash(source)
	if err != nil {
		return err
	}
	key := compatibilityKey(prefix, "dropped-response")
	object := PutObject{Key: key, Body: source, Size: source.size, SHA256: hash}
	createErr := compatibilityCreate(ctx, provider, object)
	if createErr == nil {
		return fmt.Errorf("fault controller did not drop create response")
	}
	_, verifyErr := VerifyObject(ctx, provider.Client, key, source.size, hash, source, policy)
	return verifyErr
}

func compatibilityKey(prefix, suffix string) string {
	return strings.TrimRight(prefix, "/") + "/" + suffix
}

func disqualify(report *CompatibilityReport, reason string) {
	report.Qualified = false
	report.Disqualification = reason
	report.Cases = append(report.Cases, CompatibilityCase{Name: "provider_capability", Passed: false, Detail: reason})
}

type deterministicSource struct {
	size int64
	seed byte
	at   int64
}

func newDeterministicSource(size int64, seed byte) *deterministicSource {
	return &deterministicSource{size: size, seed: seed}
}

func (s *deterministicSource) Read(buffer []byte) (int, error) {
	if s.at >= s.size {
		return 0, io.EOF
	}
	count := int(min(int64(len(buffer)), s.size-s.at))
	fillDeterministic(buffer[:count], s.at, s.seed)
	s.at += int64(count)
	return count, nil
}

func (s *deterministicSource) ReadAt(buffer []byte, offset int64) (int, error) {
	if offset < 0 || offset >= s.size {
		return 0, io.EOF
	}
	count := int(min(int64(len(buffer)), s.size-offset))
	fillDeterministic(buffer[:count], offset, s.seed)
	if count != len(buffer) {
		return count, io.EOF
	}
	return count, nil
}

func (s *deterministicSource) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = s.at + offset
	case io.SeekEnd:
		next = s.size + offset
	default:
		return 0, fmt.Errorf("objectstore: invalid seek origin")
	}
	if next < 0 {
		return 0, fmt.Errorf("objectstore: negative seek")
	}
	s.at = next
	return next, nil
}

func fillDeterministic(buffer []byte, offset int64, seed byte) {
	for index := range buffer {
		position := uint64(offset + int64(index))
		buffer[index] = byte((position*1315423911+uint64(seed)*2654435761)>>17) ^ seed
	}
}

func sourceHash(source *deterministicSource) ([32]byte, error) {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return [32]byte{}, err
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, source); err != nil {
		return [32]byte{}, err
	}
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	_, err := source.Seek(0, io.SeekStart)
	return result, err
}
