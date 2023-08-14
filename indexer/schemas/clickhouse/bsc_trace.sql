CREATE TABLE IF NOT EXISTS bsc_trace 
(
    transaction_index Nullable(UInt64),
    from_address Nullable(FixedString(42)),
    to_address Nullable(FixedString(42)),
    value UInt256,
    input Nullable(String),
    output Nullable(String),
    trace_type Nullable(String),
    call_type Nullable(String),
    reward_type Nullable(String),
    gas UInt256,
    gas_used UInt256,
    subtraces Nullable(UInt256),
    trace_address Array(UInt64),
    error Nullable(String),
    status Nullable(UInt256),
    transaction_hash Nullable(FixedString(66)),
    block_number UInt64,
    trace_id String,
    trace_index UInt256,
    block_timestamp DateTime,
    block_hash FixedString(66)
)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    ORDER BY (block_number)
    PARTITION BY toYYYYMM(block_timestamp)
    SETTINGS index_granularity = 8192, storage_policy = 's3';