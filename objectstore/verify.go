package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

const (
	DefaultFullReadLimit int64 = 8 << 20
	DefaultSampleBytes   int64 = 64 << 10
	DefaultSampleCount         = 3
)

type VerifyPolicy struct {
	FullReadLimit int64
	SampleBytes   int64
	SampleCount   int
}

func DefaultVerifyPolicy() VerifyPolicy {
	return VerifyPolicy{
		FullReadLimit: DefaultFullReadLimit,
		SampleBytes:   DefaultSampleBytes,
		SampleCount:   DefaultSampleCount,
	}
}

func (p VerifyPolicy) normalized() (VerifyPolicy, error) {
	if p == (VerifyPolicy{}) {
		return DefaultVerifyPolicy(), nil
	}
	if p.FullReadLimit < 0 || p.SampleBytes <= 0 || p.SampleCount < 2 || p.SampleCount > 32 {
		return VerifyPolicy{}, fmt.Errorf("objectstore: invalid verification policy")
	}
	return p, nil
}

// VerifyObject treats the application SHA-256 metadata as authoritative and
// deliberately ignores ETag. Small objects are fully hashed. Large objects
// require the closed local source and are compared over deterministic bounded
// ranges after metadata and length agree.
func VerifyObject(
	ctx context.Context,
	client Client,
	key string,
	size int64,
	expected [32]byte,
	local io.ReaderAt,
	policy VerifyPolicy,
) (ObjectInfo, error) {
	if client == nil || key == "" || size < 0 {
		return ObjectInfo{}, fmt.Errorf("objectstore: invalid verification request")
	}
	policy, err := policy.normalized()
	if err != nil {
		return ObjectInfo{}, err
	}

	info, err := client.Head(ctx, key)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("head before verification: %w", err)
	}
	if info.Size != size {
		return ObjectInfo{}, fmt.Errorf("%w: key %q has %d bytes, expected %d", ErrSizeMismatch, key, info.Size, size)
	}
	metadataHash, ok := applicationHash(info.Metadata)
	if !ok || metadataHash != expected {
		return ObjectInfo{}, fmt.Errorf("%w: key %q metadata is %q, expected %x", ErrHashMismatch, key, metadataValue(info.Metadata), expected)
	}

	if size <= policy.FullReadLimit {
		if err := verifyFull(ctx, client, key, size, expected); err != nil {
			return ObjectInfo{}, err
		}
		return info, nil
	}
	if local == nil {
		return ObjectInfo{}, fmt.Errorf("%w: key %q has %d bytes", ErrVerificationSourceRequired, key, size)
	}
	if err := verifySamples(ctx, client, key, size, local, policy); err != nil {
		return ObjectInfo{}, err
	}
	return info, nil
}

func verifyFull(ctx context.Context, client Client, key string, size int64, expected [32]byte) error {
	body, err := client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get full object: %w", err)
	}
	defer body.Close()

	hasher := sha256.New()
	read, err := io.Copy(hasher, io.LimitReader(body, size+1))
	if err != nil {
		return fmt.Errorf("read full object %q: %w", key, err)
	}
	if read != size {
		return fmt.Errorf("%w: key %q full read returned %d bytes, expected %d", ErrSizeMismatch, key, read, size)
	}
	var got [32]byte
	copy(got[:], hasher.Sum(nil))
	if got != expected {
		return fmt.Errorf("%w: key %q full read is %x, expected %x", ErrHashMismatch, key, got, expected)
	}
	return nil
}

type byteRange struct {
	offset int64
	length int64
}

func verifySamples(ctx context.Context, client Client, key string, size int64, local io.ReaderAt, policy VerifyPolicy) error {
	for _, sample := range sampleRanges(size, policy.SampleBytes, policy.SampleCount) {
		want := make([]byte, sample.length)
		if _, err := local.ReadAt(want, sample.offset); err != nil {
			return fmt.Errorf("read local sample at %d: %w", sample.offset, err)
		}
		body, err := client.GetRange(ctx, key, sample.offset, sample.length)
		if err != nil {
			return fmt.Errorf("get range %d:%d: %w", sample.offset, sample.offset+sample.length, err)
		}
		got, readErr := io.ReadAll(io.LimitReader(body, sample.length+1))
		closeErr := body.Close()
		if readErr != nil {
			return fmt.Errorf("read range at %d: %w", sample.offset, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close range at %d: %w", sample.offset, closeErr)
		}
		if int64(len(got)) != sample.length {
			return fmt.Errorf("%w: key %q range at %d returned %d bytes, expected %d", ErrSizeMismatch, key, sample.offset, len(got), sample.length)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%w: key %q range at %d differs from closed local bytes", ErrHashMismatch, key, sample.offset)
		}
	}
	return nil
}

func sampleRanges(size, width int64, count int) []byteRange {
	width = min(width, size)
	if width == 0 {
		return nil
	}
	lastOffset := size - width
	ranges := make([]byteRange, 0, count)
	for i := range count {
		offset := lastOffset * int64(i) / int64(count-1)
		candidate := byteRange{offset: offset, length: width}
		if len(ranges) == 0 || ranges[len(ranges)-1] != candidate {
			ranges = append(ranges, candidate)
		}
	}
	return ranges
}

func applicationHash(metadata map[string]string) ([32]byte, bool) {
	value := metadataValue(metadata)
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [32]byte{}, false
	}
	var result [32]byte
	copy(result[:], decoded)
	return result, true
}

func metadataValue(metadata map[string]string) string {
	for key, value := range metadata {
		if strings.EqualFold(key, ApplicationSHA256Metadata) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
