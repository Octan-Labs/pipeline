CREATE TABLE IF NOT EXISTS bsc_block 
(
    number UInt64,
    hash FixedString(66),
    parent_hash FixedString(66),
    nonce String,
    sha3_uncles FixedString(66),
    logs_bloom String,
    transactions_root FixedString(66),
    state_root FixedString(66),
    receipts_root FixedString(66),
    miner FixedString(42),
    difficulty UInt256,
    total_difficulty UInt256,
    size UInt256,
    extra_data String,
    gas_limit UInt256,
    gas_used UInt256,
    timestamp DateTime,
    transaction_count UInt64,
    base_fee_per_gas Nullable(UInt256)
) 
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') 
PARTITION BY toYYYYMM(timestamp) 
ORDER BY number SETTINGS index_granularity = 8192