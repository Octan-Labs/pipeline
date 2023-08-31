CREATE TABLE IF NOT EXISTS cmc_historical (
    id UInt64,
    rank UInt64,
    open Float64,
    close Float64,
    timestamp DateTime,
)

ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (id, rank, timestamp)
SETTINGS index_granularity = 8192, storage_policy = 's3';