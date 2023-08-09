CREATE TABLE IF NOT EXISTS bsc_transaction (
    hash FixedString(66),
    nonce UInt256,
    transaction_index UInt64,
    from_address FixedString(42),
    to_address FixedString(42),
    value UInt256,
    gas UInt256,
    gas_price UInt256,
    input String,
    block_timestamp DateTime,
    block_number UInt64,
    block_hash FixedString(66),
    max_fee_per_gas UInt256,
    max_priority_fee_per_gas UInt256,
    transaction_type UInt8,
    receipt_cumulative_gas_used UInt256,
    receipt_gas_used UInt256,
    receipt_contract_address FixedString(42),
    receipt_root String,
    receipt_status UInt8,
    receipt_effective_gas_price UInt256
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
PRIMARY KEY (hash, transaction_index);