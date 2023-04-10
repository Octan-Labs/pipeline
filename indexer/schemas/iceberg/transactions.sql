CREATE EXTERNAL TABLE IF NOT EXISTS demo.bsc.transactions (
    hash STRING,
    nonce LONG,
    block_hash STRING,
    block_number LONG,
    block_timestamp TIMESTAMP,
    transaction_index LONG,
    from_address STRING,
    to_address STRING,
    value DECIMAL(38,0),
    gas LONG,
    gas_price LONG,
    input STRING,
    max_fee_per_gas LONG,
    max_priority_fee_per_gas LONG,
    transaction_type LONG
)
PARTITIONED BY (truncate(10000, block_number))