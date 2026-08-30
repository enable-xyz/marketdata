package replay

import (
	"context"
	"fmt"
	"slices"

	"github.com/enable-xyz/marketdata/catalog"
)

// NativePublicationLookup resolves the committed raw lineage for one pinned
// dataset request. catalog.QueryStore satisfies this interface.
type NativePublicationLookup interface {
	CommittedRawSegments(context.Context, catalog.RawSegmentFilter) ([]catalog.RawSegmentPublication, error)
}

type NativeResolverOptions struct {
	Replay           Config
	MaxDescriptors   int
	MaxManifestBytes int64
	MaxObjectBytes   int64
}

// NativeResolver binds a pinned request to exact committed manifest bytes and
// the caller-provided immutable object reader.
type NativeResolver struct {
	publications NativePublicationLookup
	reader       ObjectReader
	options      NativeResolverOptions
}

func NewNativeResolver(publications NativePublicationLookup, reader ObjectReader, options NativeResolverOptions) (*NativeResolver, error) {
	if publications == nil || reader == nil {
		return nil, fmt.Errorf("%w: native publication lookup and object reader are required", ErrInvalidServiceRequest)
	}
	config, err := options.Replay.normalized()
	if err != nil {
		return nil, err
	}
	if options.MaxDescriptors < 1 || options.MaxDescriptors > config.MaxSegments || options.MaxDescriptors > catalog.MaximumRawSegmentResults ||
		options.MaxManifestBytes < 1 || options.MaxObjectBytes < 1 {
		return nil, fmt.Errorf("%w: positive native descriptor and byte bounds within replay limits are required", ErrInputBound)
	}
	options.Replay = config
	return &NativeResolver{publications: publications, reader: reader, options: options}, nil
}

func (r *NativeResolver) ResolveNative(ctx context.Context, request ServiceRequest) (NativePlan, error) {
	if r == nil || r.publications == nil || r.reader == nil || ctx == nil {
		return NativePlan{}, fmt.Errorf("%w: native resolver is not initialized", ErrInvalidServiceRequest)
	}
	if err := request.Validate(); err != nil {
		return NativePlan{}, err
	}
	if err := context.Cause(ctx); err != nil {
		return NativePlan{}, err
	}
	filter := catalog.RawSegmentFilter{
		DatasetID:           request.DatasetID,
		SourceIDs:           slices.Clone(request.SourceIDs),
		ChannelIDs:          slices.Clone(request.ChannelIDs),
		InstrumentUIDs:      slices.Clone(request.InstrumentUIDs),
		StartReceivedTimeNS: request.StartReceivedTimeNS,
		EndReceivedTimeNS:   request.EndReceivedTimeNS,
		MaxManifestBytes:    r.options.MaxManifestBytes,
		Limit:               r.options.MaxDescriptors,
	}
	publications, err := r.publications.CommittedRawSegments(ctx, filter)
	if err != nil {
		return NativePlan{}, fmt.Errorf("replay: resolve committed native lineage for dataset %q: %w", request.DatasetID, err)
	}
	if err := context.Cause(ctx); err != nil {
		return NativePlan{}, err
	}
	if len(publications) == 0 {
		return NativePlan{}, fmt.Errorf("%w: no committed native manifests match the pinned request", ErrInvalidServiceRequest)
	}
	if len(publications) > r.options.MaxDescriptors {
		return NativePlan{}, fmt.Errorf("%w: got %d native descriptors, limit is %d", ErrInputBound, len(publications), r.options.MaxDescriptors)
	}

	inputs := make([]InputDescriptor, 0, len(publications))
	var manifestBytes int64
	var objectBytes int64
	for index := range publications {
		if err := context.Cause(ctx); err != nil {
			return NativePlan{}, err
		}
		publication := publications[index]
		if publication.State != catalog.RawSegmentCommitted {
			return NativePlan{}, fmt.Errorf("%w: segment %q is not committed", ErrInvalidInput, publication.SegmentID)
		}
		if !nativePublicationSelected(request, publication) {
			return NativePlan{}, fmt.Errorf("%w: committed segment %q escaped the pinned request", ErrInvalidInput, publication.SegmentID)
		}
		if int64(len(publication.ManifestBytes)) > r.options.MaxManifestBytes-manifestBytes {
			return NativePlan{}, fmt.Errorf("%w: native manifests exceed %d bytes", ErrInputBound, r.options.MaxManifestBytes)
		}
		if publication.ByteLength < 1 || publication.ByteLength > r.options.MaxObjectBytes-objectBytes {
			return NativePlan{}, fmt.Errorf("%w: native objects exceed %d bytes", ErrInputBound, r.options.MaxObjectBytes)
		}
		descriptor, err := NewInputDescriptor(publication)
		if err != nil {
			return NativePlan{}, err
		}
		if descriptor.ByteLength() > r.options.Replay.MaxSegmentBytes {
			return NativePlan{}, fmt.Errorf("%w: segment %q exceeds configured object bound", ErrInputBound, descriptor.SegmentID())
		}
		manifestBytes += int64(len(publication.ManifestBytes))
		objectBytes += publication.ByteLength
		inputs = append(inputs, descriptor)
	}
	return NativePlan{Reader: r.reader, Inputs: inputs, Config: r.options.Replay}, nil
}

func nativePublicationSelected(request ServiceRequest, publication catalog.RawSegmentPublication) bool {
	_, source := slices.BinarySearch(request.SourceIDs, publication.SourceID)
	_, channel := slices.BinarySearch(request.ChannelIDs, publication.ChannelID)
	return source && (len(request.ChannelIDs) == 0 || channel) &&
		publication.ReceivedEndNS >= request.StartReceivedTimeNS && publication.ReceivedStartNS < request.EndReceivedTimeNS
}

var _ NativePlanResolver = (*NativeResolver)(nil)
var _ NativePublicationLookup = (*catalog.QueryStore)(nil)
