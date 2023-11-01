CREATE TABLE IF NOT EXISTS bnb_reputations (
    block UInt32,
    date DateTime CODEC(DoubleDelta),
    rank UInt32,
    address FixedString(42),
    reputation_score Float64,
    total_transfer UInt64,
    total_receive UInt64,
    total_txn UInt64,
    degree UInt64,
    in_degree UInt64,
    out_degree UInt64,
    total_volume Float64,
    total_gas_spent Float64,
    uaw UInt64,
    is_contract Boolean
) ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(date)
ORDER BY (date, address);
CREATE TABLE IF NOT EXISTS bnb_top_reputations (
    rank UInt32,
    date DateTime,
    address FixedString(42),
    reputation_score Float64,
    total_txn UInt64,
    total_volume Float64,
    total_gas_spent Float64,
    is_contract Boolean,
    degree UInt64
) ENGINE = ReplacingMergeTree(date)
ORDER BY address;
CREATE MATERIALIZED VIEW bnb_top_reputations_mv TO bnb_top_reputations AS WITH latest_reputations AS (
    SELECT address,
        max(date) as latest_date
    FROM bnb_reputations
    GROUP BY address
)
SELECT lr.latest_date as date,
    er.rank as rank,
    er.address as address,
    er.reputation_score as reputation_score,
    er.total_txn as total_txn,
    er.total_volume as total_volume,
    er.total_gas_spent as total_gas_spent,
    er.is_contract as is_contract,
    er.degree as degree
FROM latest_reputations AS lr
    JOIN bnb_reputations AS er ON lr.address = er.address
    AND lr.latest_date = er.date;
CREATE TABLE IF NOT EXISTS sorting_bnb_top_reputations (
    rank UInt32,
    date DateTime,
    address FixedString(42),
    reputation_score Float64,
    total_txn UInt64,
    total_volume Float64,
    total_gas_spent Float64,
    is_contract Boolean,
    degree UInt64
) ENGINE = ReplacingMergeTree(date)
ORDER BY address;
CREATE TABLE bnb_projects (
    rank UInt64,
    project_id UInt64,
    project_name String,
    project_category String,
    total_contract UInt64,
    total_grs Float64,
    total_txn UInt64,
    total_gas UInt64,
    total_degree UInt64,
    total_tx_volume UInt64,
    updated_at DateTime,
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);