CREATE TABLE IF NOT EXISTS eth_contract
(
    address FixedString(42),
    bytecode String,
    function_sighashes Array(FixedString(10)),
    is_erc20 Bool,
    is_erc721 Bool,
    block_number UInt64,
    block_timestamp DateTime,
    block_hash FixedString(66)
)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
    ORDER BY (address)
    PARTITION BY toYYYYMM(block_timestamp)
    SETTINGS index_granularity = 8192, storage_policy = 's3';