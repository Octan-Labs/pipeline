CREATE TABLE IF NOT EXISTS bsc_token_transfer 
(
    token_address FixedString(42),
    from_address FixedString(42),
    to_address FixedString(42),
    value UInt256,
    transaction_hash FixedString(66),
    log_index UInt64,
    block_number UInt64,
    block_timestamp DateTime,
    block_hash FixedString(66)
)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    ORDER BY (block_number, transaction_hash, log_index)
    PARTITION BY toYYYYMM(block_timestamp)
    SETTINGS index_granularity = 8192, storage_policy = 's3';