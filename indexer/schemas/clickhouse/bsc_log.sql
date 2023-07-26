CREATE TABLE IF NOT EXISTS bsc_log {
    log_index UInt64,
    transaction_hash FixedString(66),
    transaction_index UInt64,
    address FixedString(42),
    data String,
    topics Array(String),
    block_number UInt64
    block_timestamp DateTime,
    block_hash FixedString(66)
}