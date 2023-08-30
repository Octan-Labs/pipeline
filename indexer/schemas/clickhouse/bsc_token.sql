CREATE TABLE IF NOT EXISTS bsc_token 
(
    address FixedString(42),
    symbol String,
    name String,
    decimals UInt64,
    total_supply UInt256,
    block_number UInt64,
    block_timestamp DateTime,
    block_hash FixedString(66)
)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    ORDER BY (address)
    PARTITION BY toYYYYMM(block_timestamp)
    SETTINGS index_granularity = 8192, storage_policy = 's3';