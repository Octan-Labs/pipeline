CREATE TABLE IF NOT EXISTS ethereum_token_transfer (
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
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
PRIMARY KEY (token_address, transaction_hash, log_index);