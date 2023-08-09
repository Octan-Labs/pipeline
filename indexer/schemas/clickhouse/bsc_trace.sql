CREATE TABLE IF NOT EXISTS bsc_trace (
    transaction_index UInt64,
    from_address FixedString(42),
    to_address FixedString(42),
    value UInt256,
    input String,
    output String,
    trace_type String,
    call_type String,
    reward_type String,
    gas UInt256,
    gas_used UInt256,
    subtraces UInt256,
    trace_address FixedString(42),
    error String,
    status UInt256,
    transaction_hash FixedString(66),
    block_number UInt64,
    trace_id String,
    trace_index UInt256,
    block_timestamp DateTime,
    block_hash FixedString(66)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
PRIMARY KEY (transaction_hash, transaction_index);