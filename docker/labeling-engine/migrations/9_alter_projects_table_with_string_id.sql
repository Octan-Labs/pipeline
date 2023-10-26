-- DROP OLD BNB PROJECT TABLES
DROP TABLE IF EXISTS bnb_projects;
CREATE TABLE IF NOT EXISTS bnb_projects
(
    rank             UInt64,
    project_id       String,
    project_name     String,
    project_category String,
    total_contract   UInt64,
    total_grs        Float64,
    total_txn        UInt64,
    total_gas        UInt64,
    total_degree     UInt64,
    total_tx_volume  UInt64,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);

-- DROP OLD ETH PROJECT TABLES
DROP TABLE IF EXISTS eth_projects;
CREATE TABLE IF NOT EXISTS eth_projects
(
    rank             UInt64,
    project_id       String,
    project_name     String,
    project_category String,
    total_contract   UInt64,
    total_grs        Float64,
    total_txn        UInt64,
    total_gas        UInt64,
    total_degree     UInt64,
    total_tx_volume  UInt64,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);

-- DROP OLD ARB PROJECT TABLES
DROP TABLE IF EXISTS arb_projects;
CREATE TABLE IF NOT EXISTS arb_projects
(
    rank             UInt64,
    project_id       String,
    project_name     String,
    project_category String,
    total_contract   UInt64,
    total_grs        Float64,
    total_txn        UInt64,
    total_gas        UInt64,
    total_degree     UInt64,
    total_tx_volume  UInt64,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);

-- DROP OLD MATIC PROJECT TABLES
DROP TABLE IF EXISTS matic_projects;
CREATE TABLE IF NOT EXISTS matic_projects
(
    rank             UInt64,
    project_id       String,
    project_name     String,
    project_category String,
    total_contract   UInt64,
    total_grs        Float64,
    total_txn        UInt64,
    total_gas        UInt64,
    total_degree     UInt64,
    total_tx_volume  UInt64,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);

-- DROP OLD AURORA PROJECT TABLES
DROP TABLE IF EXISTS aurora_projects;
CREATE TABLE IF NOT EXISTS aurora_projects
(
    rank             UInt64,
    project_id       String,
    project_name     String,
    project_category String,
    total_contract   UInt64,
    total_grs        Float64,
    total_txn        UInt64,
    total_gas        UInt64,
    total_degree     UInt64,
    total_tx_volume  UInt64,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);

-- DROP OLD XRP PROJECT TABLES
DROP TABLE IF EXISTS xrp_projects;
CREATE TABLE IF NOT EXISTS xrp_projects
(
    rank             UInt64,
    project_id       String,
    project_name     String,
    project_category String,
    total_contract   UInt64,
    total_grs        Float64,
    total_txn        UInt64,
    total_gas        UInt64,
    total_degree     UInt64,
    total_tx_volume  UInt64,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);

-- DROP OLD TRX PROJECT TABLES
DROP TABLE IF EXISTS trx_projects;
CREATE TABLE IF NOT EXISTS trx_projects
(
    rank             UInt64,
    project_id       String,
    project_name     String,
    project_category String,
    total_contract   UInt64,
    total_grs        Float64,
    total_txn        UInt64,
    total_gas        UInt64,
    total_degree     UInt64,
    total_tx_volume  UInt64,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, project_category);
