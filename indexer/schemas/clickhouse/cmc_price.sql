CREATE TABLE IF NOT EXISTS cmc_price (
    id UInt32,
    rank UInt32,
    open DateTime('UTC'),
    close DateTime('UTC'),
    timestamp DateTime('UTC'),
) 
ENGINE = MergeTree
ORDER BY id;