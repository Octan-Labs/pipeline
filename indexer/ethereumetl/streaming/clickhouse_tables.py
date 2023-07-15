from sqlalchemy import Table, MetaData, Column, PrimaryKeyConstraint
from clickhouse_sqlalchemy.types.common import Boolean, Array, String, DateTime, UInt128, UInt8, UInt256

metadata = MetaData()

# SQL schema is here https://github.com/blockchain-etl/ethereum-etl-postgres/tree/master/schema

BLOCKS = Table(
    'blocks', metadata,
    Column('DateTime', DateTime),
    Column('number', UInt128),
    Column('hash', String, primary_key=True),
    Column('parent_hash', String),
    Column('nonce', String),
    Column('sha3_uncles', String),
    Column('logs_bloom', String),
    Column('transactions_root', String),
    Column('state_root', String),
    Column('receipts_root', String),
    Column('miner', String),
    Column('difficulty', UInt128),
    Column('total_difficulty', UInt128),
    Column('size', UInt128),
    Column('extra_data', String),
    Column('gas_limit', UInt128),
    Column('gas_used', UInt128),
    Column('transaction_count', UInt128),
    Column('base_fee_per_gas', UInt128),
)

TRANSACTIONS = Table(
    'transactions', metadata,
    Column('hash', String, primary_key=True),
    Column('nonce', UInt128),
    Column('transaction_index', UInt128),
    Column('from_address', String),
    Column('to_address', String),
    Column('value', UInt128),
    Column('gas', UInt128),
    Column('gas_price', UInt128),
    Column('input', String),
    Column('receipt_cumulative_gas_used', UInt128),
    Column('receipt_gas_used', UInt128),
    Column('receipt_contract_address', String),
    Column('receipt_root', String),
    Column('receipt_status', UInt128),
    Column('block_DateTime', DateTime),
    Column('block_number', UInt128),
    Column('block_hash', String),
    Column('max_fee_per_gas', UInt128),
    Column('max_priority_fee_per_gas', UInt128),
    Column('transaction_type', UInt128),
    Column('receipt_effective_gas_price', UInt128),
)

LOGS = Table(
    'logs', metadata,
    Column('log_index', UInt128, primary_key=True),
    Column('transaction_hash', String, primary_key=True),
    Column('transaction_index', UInt128),
    Column('address', String),
    Column('data', String),
    Column('topic0', String),
    Column('topic1', String),
    Column('topic2', String),
    Column('topic3', String),
    Column('block_DateTime', DateTime),
    Column('block_number', UInt128),
    Column('block_hash', String),
)

TOKEN_TRANSFERS = Table(
    'token_transfers', metadata,
    Column('token_address', String),
    Column('from_address', String),
    Column('to_address', String),
    Column('value', UInt256),
    Column('transaction_hash', String, primary_key=True),
    Column('log_index', UInt128, primary_key=True),
    Column('block_DateTime', DateTime),
    Column('block_number', UInt128),
    Column('block_hash', String),
)

TRACES = Table(
    'traces', metadata,
    Column('transaction_hash', String),
    Column('transaction_index', UInt128),
    Column('from_address', String),
    Column('to_address', String),
    Column('value', UInt128),
    Column('input', String),
    Column('output', String),
    Column('trace_type', String),
    Column('call_type', String),
    Column('reward_type', String),
    Column('gas', UInt128),
    Column('gas_used', UInt128),
    Column('subtraces', UInt128),
    Column('trace_address', String),
    Column('error', String),
    Column('status', UInt8),
    Column('block_DateTime', DateTime),
    Column('block_number', UInt128),
    Column('block_hash', String),
    Column('trace_id', String, primary_key=True),
)

TOKENS = Table(
    'tokens', metadata,
    Column('address', String(42)),
    Column('name', String),
    Column('symbol', String),
    Column('decimals', UInt8),
    Column('function_sighashes', Array(String)),
    Column('total_supply', UInt256),
    Column('block_number', UInt128),
    PrimaryKeyConstraint('address', 'block_number', name='tokens_pk'),
)

CONTRACTS = Table(
    'contracts', metadata,
    Column('address', String(42)),
    Column('bytecode', String),
    Column('function_sighashes', Array(String)),
    Column('is_erc20', Boolean),
    Column('is_erc721', Boolean),
    Column('block_number', UInt128),
    PrimaryKeyConstraint('address', 'block_number', name='contracts_pk'),
)
