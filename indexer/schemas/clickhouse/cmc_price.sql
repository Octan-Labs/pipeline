CREATE TABLE IF NOT EXISTS cmc_price (
    id UInt32,
    rank UInt32,
    name String,
    symbol FixedString(10),
    slug FixedString(50),
    is_active Bool,
    first_historical_data DateTime('UTC'),
    last_historical_data DateTime('UTC'),
) ENGINE = MergeTree()
ORDER BY id;
