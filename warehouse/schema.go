package warehouse

import (
	"fmt"
	"regexp"
	"strings"
)

const defaultTablePrefix = "marketdata"

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type Schema struct {
	Database string
	Prefix   string
	Layout   PartitionLayout
}

func (s Schema) normalized() (Schema, error) {
	if s.Database == "" || !identifierPattern.MatchString(s.Database) {
		return Schema{}, fmt.Errorf("%w: ClickHouse database identifier", ErrInvalidWarehouseInput)
	}
	if s.Prefix == "" {
		s.Prefix = defaultTablePrefix
	}
	if !identifierPattern.MatchString(s.Prefix) {
		return Schema{}, fmt.Errorf("%w: ClickHouse table prefix", ErrInvalidWarehouseInput)
	}
	if s.Layout == "" {
		s.Layout = PartitionMonth
	}
	if !s.Layout.valid() {
		return Schema{}, fmt.Errorf("%w: ClickHouse partition layout", ErrInvalidWarehouseInput)
	}
	return s, nil
}

func (s Schema) table(suffix string) string {
	return "`" + s.Database + "`.`" + s.Prefix + "_" + suffix + "`"
}

func (s Schema) EventsTable() string {
	return s.table("events_v1")
}

func (s Schema) GenerationsTable() string {
	return s.table("load_generations_v1")
}

func (s Schema) ExpectedEventsTable() string {
	return s.table("generation_event_ids_v1")
}

// Statements returns the complete v1 DDL. Every table deliberately uses an
// ordinary MergeTree. Canonical replacement is a synchronous delete followed
// by a full generation rebuild; background ReplacingMergeTree convergence is
// not part of the correctness contract.
func (s Schema) Statements() ([]string, error) {
	normalized, err := s.normalized()
	if err != nil {
		return nil, err
	}
	s = normalized
	partitionExpression := "toYYYYMM(fromUnixTimestamp64Nano(received_time_ns), 'UTC')"
	if s.Layout == PartitionDate {
		partitionExpression = "toYYYYMMDD(fromUnixTimestamp64Nano(received_time_ns), 'UTC')"
	}
	statements := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", s.Database),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    load_generation_id FixedString(32),
    manifest_sha256 FixedString(32),
    row_id FixedString(32),
    event_id FixedString(32),
    logical_hash FixedString(32),
    family LowCardinality(String),
    source_id LowCardinality(String),
    channel_id LowCardinality(String),
    instrument_uid String,
    epoch_kind LowCardinality(String),
    connection_epoch FixedString(16),
    received_time_ns Int64,
    arrival_ordinal UInt64,
    message_ordinal UInt32,
    raw_segment_sha256 FixedString(32),
    raw_record_ordinal UInt64,
    raw_payload_sha256 FixedString(32),
    catalog_snapshot_id FixedString(32),
    schema_name LowCardinality(String),
    schema_version UInt16,
    dataset_policy_id FixedString(32),
    replay_config_id FixedString(32),
    input_manifest_set_id FixedString(32),
    physical_ordinal UInt64,
    price Nullable(Decimal(38, 18)),
    amount Nullable(Decimal(38, 18)),
    bid_price Nullable(Decimal(38, 18)),
    bid_amount Nullable(Decimal(38, 18)),
    ask_price Nullable(Decimal(38, 18)),
    ask_amount Nullable(Decimal(38, 18)),
    price_change Nullable(Decimal(38, 18)),
    price_change_percent Nullable(Decimal(38, 8)),
    weighted_average_price Nullable(Decimal(38, 18)),
    first_trade_before_window_price Nullable(Decimal(38, 18)),
    last_price Nullable(Decimal(38, 18)),
    last_amount Nullable(Decimal(38, 18)),
    native_best_bid_price Nullable(Decimal(38, 18)),
    native_best_bid_amount Nullable(Decimal(38, 18)),
    native_best_ask_price Nullable(Decimal(38, 18)),
    native_best_ask_amount Nullable(Decimal(38, 18)),
    open_price Nullable(Decimal(38, 18)),
    high_price Nullable(Decimal(38, 18)),
    low_price Nullable(Decimal(38, 18)),
    base_volume Nullable(Decimal(38, 18)),
    quote_volume Nullable(Decimal(38, 18))
) ENGINE = MergeTree
PARTITION BY %s
ORDER BY (source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal)`, s.EventsTable(), partitionExpression),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    load_generation_id FixedString(32),
    server_digest String,
    manifest_sha256 FixedString(32),
    input_hash FixedString(32),
    dataset_identity FixedString(32),
    catalog_identity FixedString(32),
    schema_identity FixedString(32),
    family LowCardinality(String),
    source_id LowCardinality(String),
    utc_date FixedString(10),
    partition_value UInt32,
    partition_layout Enum8('month' = 1, 'date' = 2),
    expected_event_set_sha256 FixedString(32),
    expected_event_count UInt64,
    expected_row_count UInt64,
    state Enum8('pending' = 1, 'unknown' = 2, 'committed' = 3, 'failed' = 4),
    last_error String,
    created_at DateTime64(9, 'UTC'),
    updated_at DateTime64(9, 'UTC')
) ENGINE = MergeTree
PARTITION BY partition_value
ORDER BY load_generation_id`, s.GenerationsTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    load_generation_id FixedString(32),
    partition_value UInt32,
    event_id FixedString(32)
) ENGINE = MergeTree
PARTITION BY partition_value
ORDER BY (load_generation_id, event_id)`, s.ExpectedEventsTable()),
	}
	for i := range statements {
		statements[i] = strings.TrimSpace(statements[i])
	}
	return statements, nil
}
