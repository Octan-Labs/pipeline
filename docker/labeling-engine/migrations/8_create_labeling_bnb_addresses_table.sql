CREATE TABLE IF NOT EXISTS labeling_bnb_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS labeling_eth_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS labeling_arb_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS labeling_matic_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS labeling_aurora_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS labeling_xrp_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS labeling_trx_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS labeling_base_addresses (
    id String,
    address String,
    identity String,
    category String,
    is_contract Boolean,
    explorer_tagname String,
    explorer_contractname String,
    crawled_oklink String,
    note String,
    safety String,
    version String,
    project_name Nullable(String),
    project_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);