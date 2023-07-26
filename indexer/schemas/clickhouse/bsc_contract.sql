CREATE TABLE IF NOT EXISTS bsc_contract {
    address FixedString(42),
    bytecode String,
    function_sighashes String,
    is_erc20 Bool,
    is_erc721 Bool,
    block_number UInt64,
    block_timestamp DateTime,
    block_hash FixedString(66)
}