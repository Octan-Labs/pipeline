CREATE TABLE IF NOT EXISTS ethereum_contract (
    address FixedString(42),
    bytecode String,
    function_sighashes String,
    is_erc20 Bool,
    is_erc721 Bool,
    block_number UInt64,
    block_timestamp DateTime,
    block_hash FixedString(66)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
PRIMARY KEY (address);