DROP TABLE IF EXISTS neo_reputations;
CREATE TABLE IF NOT EXISTS neo_reputations (
  block UInt32,
  date DateTime CODEC(DoubleDelta),
  rank UInt32,
  address String,
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
DROP TABLE IF EXISTS neo_top_reputations;
CREATE TABLE IF NOT EXISTS neo_top_reputations (
  rank UInt32,
  date DateTime,
  address String,
  reputation_score Float64,
  total_txn UInt64,
  total_volume Float64,
  total_gas_spent Float64,
  is_contract Boolean,
  degree UInt64
) ENGINE = ReplacingMergeTree(date)
ORDER BY address;
DROP TABLE IF EXISTS sorting_neo_top_reputations;
CREATE TABLE IF NOT EXISTS sorting_neo_top_reputations (
  rank UInt32,
  date DateTime,
  address String,
  reputation_score Float64,
  total_txn UInt64,
  total_volume Float64,
  total_gas_spent Float64,
  is_contract Boolean,
  degree UInt64
) ENGINE = ReplacingMergeTree(date)
ORDER BY address;