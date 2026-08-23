package dataset

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/enable-xyz/marketdata/normalize"
	"github.com/parquet-go/parquet-go"
)

type auxiliaryCoordinate struct {
	instrumentUID  string
	sourceID       string
	channelID      string
	epochKind      string
	epochID        [16]byte
	arrivalOrdinal uint64
	messageOrdinal uint32
	receivedTimeNS int64
}

func BuildQuarantinePartition(ctx context.Context, root string, source QuarantineSource, options WriterOptions) (BuildResult, error) {
	if source == nil {
		return BuildResult{}, fmt.Errorf("%w: nil quarantine source", ErrInvalidInput)
	}
	return buildAuxiliary(ctx, root, FamilySchemaQuarantine, options, source.Next,
		func(value normalize.SchemaQuarantineV1, ordinal uint64) (quarantineParquetRow, auxiliaryCoordinate, [32]byte, error) {
			if err := validateQuarantine(value); err != nil {
				return quarantineParquetRow{}, auxiliaryCoordinate{}, [32]byte{}, err
			}
			digest := quarantineHash(value)
			coordinate := value.Coordinate
			row := quarantineParquetRow{RowOrdinal: ordinal, RowLogicalHash: digest, Version: uint32(value.Version),
				QuarantineID: value.QuarantineID, Code: string(value.Code), Field: value.Field, SourceState: string(value.SourceState),
				FingerprintClass: string(value.FingerprintClass), SourceSchemaFingerprint: value.SourceSchemaFingerprint,
				SourceID: value.SourceID, ChannelID: value.ChannelID, ReceivedTimeNS: value.ReceivedTimeNS,
				EpochKind: string(coordinate.EpochKind), EpochID: coordinate.EpochID, ArrivalOrdinal: coordinate.ArrivalOrdinal,
				MessageOrdinal: coordinate.MessageOrdinal, RawSegmentSHA256: coordinate.RawSegmentSHA256,
				RawRecordOrdinal: coordinate.RawRecordOrdinal, RawPayloadSHA256: coordinate.RawPayloadSHA256,
				MapperVersion: value.MapperVersion, MapperBindingID: value.MapperBindingID,
				SourceTimeResolution: string(value.SourceTimeResolution), CatalogSnapshotID: value.CatalogSnapshotID,
				DatasetPolicyID: options.DatasetPolicyID, ReplayConfigID: options.ReplayConfigID, InputManifestSetID: options.InputManifestSetID}
			meta := auxiliaryCoordinate{sourceID: value.SourceID, channelID: value.ChannelID, epochKind: string(coordinate.EpochKind),
				epochID: coordinate.EpochID, arrivalOrdinal: coordinate.ArrivalOrdinal, messageOrdinal: coordinate.MessageOrdinal,
				receivedTimeNS: value.ReceivedTimeNS}
			return row, meta, digest, nil
		}, func(row *quarantineParquetRow, ordinal uint64) {
			row.RowOrdinal = ordinal
		})
}

func BuildQualityPartition(ctx context.Context, root string, source QualitySource, options WriterOptions) (BuildResult, error) {
	if source == nil {
		return BuildResult{}, fmt.Errorf("%w: nil quality source", ErrInvalidInput)
	}
	return buildAuxiliary(ctx, root, FamilyQuality, options, source.Next,
		func(value QualityRowV1, ordinal uint64) (qualityParquetRow, auxiliaryCoordinate, [32]byte, error) {
			if err := value.Validate(); err != nil {
				return qualityParquetRow{}, auxiliaryCoordinate{}, [32]byte{}, err
			}
			digest := qualityHash(value)
			coordinate := value.Coordinate
			row := qualityParquetRow{RowOrdinal: ordinal, RowLogicalHash: digest, Version: uint32(value.Version), QualityID: value.QualityID,
				Kind: value.Kind, Code: value.Code, SourceState: string(value.SourceState), SourceSchemaFingerprint: value.SourceSchemaFingerprint,
				SchemaName: value.SchemaName, SchemaVersion: uint32(value.SchemaVersion), SourceID: value.SourceID, ChannelID: value.ChannelID,
				InstrumentUID: value.InstrumentUID, InstrumentUIDState: string(value.InstrumentUIDState),
				ReceivedTimeNS: value.ReceivedTimeNS, EpochKind: string(coordinate.EpochKind), EpochID: coordinate.EpochID,
				ArrivalOrdinal: coordinate.ArrivalOrdinal, MessageOrdinal: coordinate.MessageOrdinal, RawSegmentSHA256: coordinate.RawSegmentSHA256,
				RawRecordOrdinal: coordinate.RawRecordOrdinal, RawPayloadSHA256: coordinate.RawPayloadSHA256, MapperVersion: value.MapperVersion,
				MapperBindingID: value.MapperBindingID, CatalogSnapshotID: value.CatalogSnapshotID, PolicyID: value.PolicyID,
				QualityFlags: append([]string(nil), value.QualityFlags...), DatasetPolicyID: options.DatasetPolicyID,
				ReplayConfigID: options.ReplayConfigID, InputManifestSetID: options.InputManifestSetID}
			meta := auxiliaryCoordinate{instrumentUID: value.InstrumentUID, sourceID: value.SourceID, channelID: value.ChannelID,
				epochKind: string(coordinate.EpochKind), epochID: coordinate.EpochID, arrivalOrdinal: coordinate.ArrivalOrdinal,
				messageOrdinal: coordinate.MessageOrdinal, receivedTimeNS: value.ReceivedTimeNS}
			return row, meta, digest, nil
		}, func(row *qualityParquetRow, ordinal uint64) {
			row.RowOrdinal = ordinal
		})
}

type auxiliaryRecord[R any] struct {
	row    R
	meta   auxiliaryCoordinate
	digest [32]byte
}

func buildAuxiliary[I, R any](ctx context.Context, root string, family Family, options WriterOptions,
	next func(context.Context) (I, error), convert func(I, uint64) (R, auxiliaryCoordinate, [32]byte, error),
	setOrdinal func(*R, uint64)) (result BuildResult, err error) {
	if err := options.validate(); err != nil {
		return result, err
	}
	var records []auxiliaryRecord[R]
	var coordinates partitionCoordinates
	epochOrders := make(map[epochKey]epochOrder)
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		value, nextErr := next(ctx)
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return result, nextErr
		}
		if uint64(len(records)) >= options.MaxInputRows {
			return result, fmt.Errorf("%w: auxiliary input row bound", ErrInvalidInput)
		}
		row, meta, digest, err := convert(value, uint64(len(records)))
		if err != nil {
			return result, err
		}
		date, hour := utcParts(meta.receivedTimeNS)
		if len(records) == 0 {
			coordinates = partitionCoordinates{
				family: family, sourceID: meta.sourceID, date: date, hour: hour,
				firstTimeNS: meta.receivedTimeNS, lastTimeNS: meta.receivedTimeNS,
				firstArrival: meta.arrivalOrdinal, lastArrival: meta.arrivalOrdinal,
			}
		} else if meta.sourceID != coordinates.sourceID || date != coordinates.date || hour != coordinates.hour {
			return result, fmt.Errorf("%w at auxiliary row %d", ErrPartitionBoundary, len(records))
		}
		coordinates.include(meta.receivedTimeNS, meta.arrivalOrdinal)
		key := epochKey{kind: meta.epochKind, id: meta.epochID}
		order, ok := epochOrders[key]
		if !ok {
			epochOrders[key] = epochOrder{
				key: key, firstReceivedTimeNS: meta.receivedTimeNS,
				firstArrivalOrdinal: meta.arrivalOrdinal, firstMessageOrdinal: meta.messageOrdinal,
				lastArrivalOrdinal: meta.arrivalOrdinal,
			}
		} else {
			if meta.arrivalOrdinal < order.firstArrivalOrdinal ||
				(meta.arrivalOrdinal == order.firstArrivalOrdinal && meta.messageOrdinal < order.firstMessageOrdinal) {
				order.firstReceivedTimeNS = meta.receivedTimeNS
				order.firstArrivalOrdinal = meta.arrivalOrdinal
				order.firstMessageOrdinal = meta.messageOrdinal
			}
			if meta.arrivalOrdinal > order.lastArrivalOrdinal {
				order.lastArrivalOrdinal = meta.arrivalOrdinal
			}
			epochOrders[key] = order
		}
		records = append(records, auxiliaryRecord[R]{row: row, meta: meta, digest: digest})
	}
	if len(records) == 0 {
		return result, fmt.Errorf("%w: empty partition", ErrInvalidInput)
	}
	coordinates.epochs = make([]ManifestEpoch, 0, len(epochOrders))
	for key, order := range epochOrders {
		coordinates.epochs = append(coordinates.epochs, ManifestEpoch{
			Kind: key.kind, ID: fmt.Sprintf("%x", key.id), FirstReceivedTimeNS: order.firstReceivedTimeNS,
			FirstArrivalOrdinal: order.firstArrivalOrdinal, LastArrivalOrdinal: order.lastArrivalOrdinal,
		})
	}
	slices.SortFunc(coordinates.epochs, func(left, right ManifestEpoch) int {
		if value := cmp.Compare(left.FirstReceivedTimeNS, right.FirstReceivedTimeNS); value != 0 {
			return value
		}
		if value := cmp.Compare(left.Kind, right.Kind); value != 0 {
			return value
		}
		return cmp.Compare(left.ID, right.ID)
	})
	slices.SortFunc(records, func(left, right auxiliaryRecord[R]) int {
		if value := cmp.Compare(left.meta.instrumentUID, right.meta.instrumentUID); value != 0 {
			return value
		}
		leftOrder := epochOrders[epochKey{kind: left.meta.epochKind, id: left.meta.epochID}]
		rightOrder := epochOrders[epochKey{kind: right.meta.epochKind, id: right.meta.epochID}]
		if value := cmp.Compare(leftOrder.firstReceivedTimeNS, rightOrder.firstReceivedTimeNS); value != 0 {
			return value
		}
		if value := cmp.Compare(left.meta.epochKind, right.meta.epochKind); value != 0 {
			return value
		}
		if value := bytes.Compare(left.meta.epochID[:], right.meta.epochID[:]); value != 0 {
			return value
		}
		if value := cmp.Compare(left.meta.arrivalOrdinal, right.meta.arrivalOrdinal); value != 0 {
			return value
		}
		if value := cmp.Compare(left.meta.messageOrdinal, right.meta.messageOrdinal); value != 0 {
			return value
		}
		return bytes.Compare(left.digest[:], right.digest[:])
	})
	for index := 1; index < len(records); index++ {
		previous, current := records[index-1].meta, records[index].meta
		if previous.instrumentUID == current.instrumentUID && previous.epochKind == current.epochKind &&
			previous.epochID == current.epochID && previous.arrivalOrdinal == current.arrivalOrdinal &&
			previous.messageOrdinal == current.messageOrdinal {
			return result, fmt.Errorf("%w: duplicate auxiliary source coordinate", ErrInvalidInput)
		}
	}
	staging, err := newStaging(root)
	if err != nil {
		return result, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(staging)
		}
	}()
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return result, err
	}
	name, version, schema, err := familySchema(family)
	if err != nil {
		_ = file.Close()
		return result, err
	}
	var writer partitionWriter
	writerOptions := parquetWriterOptions(schema, family, options, filepath.Dir(staging))
	switch family {
	case FamilySchemaQuarantine:
		writer = &genericPartitionWriter[quarantineParquetRow]{parquet.NewGenericWriter[quarantineParquetRow](file, writerOptions...)}
	case FamilyQuality:
		writer = &genericPartitionWriter[qualityParquetRow]{parquet.NewGenericWriter[qualityParquetRow](file, writerOptions...)}
	default:
		_ = file.Close()
		return result, fmt.Errorf("%w: auxiliary family", ErrInvalidInput)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
			_ = file.Close()
		}
	}()
	schemaHash := schemaDigest(name, version, schema)
	setStaticMetadata(writer, name, version, schemaHash, options)
	logical := sha256.New()
	_, _ = logical.Write([]byte("dataset-auxiliary-partition-logical-v1\x00" + string(family) + "\x00"))
	var parquetRows uint64
	lastFlushSize := int64(0)
	for index := range records {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		setOrdinal(&records[index].row, uint64(index))
		if _, err := logical.Write(records[index].digest[:]); err != nil {
			return result, err
		}
		if _, err := writer.Write(records[index].row); err != nil {
			return result, err
		}
		parquetRows++
		if parquetRows > options.MaxParquetRows || writer.Size() > MaxPartitionFileBytes {
			return result, fmt.Errorf("%w: auxiliary output bound", ErrInvalidInput)
		}
		if writer.Size()-lastFlushSize >= options.RowGroupTargetBytes {
			if err := writer.Flush(); err != nil {
				return result, err
			}
			lastFlushSize = writer.Size()
		}
	}
	inputRows := uint64(len(records))
	logicalHash := sumHash(logical)
	writer.SetKeyValueMetadata("enable.logical_sha256", fmt.Sprintf("%x", logicalHash))
	writer.SetKeyValueMetadata("enable.input_rows", fmt.Sprint(inputRows))
	writer.SetKeyValueMetadata("enable.parquet_rows", fmt.Sprint(parquetRows))
	if err := writer.Close(); err != nil {
		return result, err
	}
	closed = true
	if err := file.Sync(); err != nil {
		return result, err
	}
	if err := file.Close(); err != nil {
		return result, err
	}
	return finalizePartition(ctx, root, staging, coordinates, name, version, schemaHash, logicalHash, inputRows, parquetRows, options)
}
