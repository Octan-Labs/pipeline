#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DoubleType
from pyspark.sql import SparkSession


# In[ ]:





# In[ ]:


import argparse, sys

parser=argparse.ArgumentParser()

parser.add_argument("-b", "--base_path", help="S3 bucket base path", required=True)
parser.add_argument("-s", "--start_block", help="Start block to calculate", required=True)
parser.add_argument("-n", "--name", help="Name of chain to calculate [bsc|eth|polygon]", required=True)
parser.add_argument("-c", "--cmc_id", help="CMC ID 's native token to calculate [BNB=1839|ETH=1027|MATIC=3890]", required=True)

args=parser.parse_args()

base_path = args.base_path
start_block = args.start_block
name = args.name # [bsc|eth|polygon]
cmc_id = args.cmc_id  # [BNB=1839|ETH=1027|MATIC=3890]


# In[ ]:


# start_block = "0000010000"


# In[ ]:





# In[2]:


spark = SparkSession \
    .builder \
    .appName("Preprocessdata") \
    .getOrCreate()


# In[3]:


token_transfers_schema = StructType([ \
    StructField("token_address", StringType(), True), \
    StructField("from_address", StringType(), True), \
    StructField("to_address", StringType(), True), \
    StructField("value", DecimalType(38, 0), True), \
    StructField("transaction_hash", StringType(), True), \
    StructField("log_index", LongType(), True), \
    StructField("block_number", LongType(), True) \
  ])


# In[4]:


transactions_schema = StructType([ \
    StructField("hash", StringType(), True), \
    StructField("nonce", LongType(), True), \
    StructField("block_hash", StringType(), True), \
    StructField("block_number", LongType(), True), \
    StructField("transaction_index", LongType(), True), \
    StructField("from_address", StringType(), True), \
    StructField("to_address", StringType(), True), \
    StructField("value", DecimalType(38, 0), True), \
    StructField("gas", LongType(), True), \
    StructField("gas_price", LongType(), True), \
    StructField("input", StringType(), True), \
    StructField("block_timestamp", LongType(), True), \
    StructField("max_fee_per_gas", LongType(), True), \
    StructField("max_priority_fee_per_gas", LongType(), True), \
    StructField("transaction_type", LongType(), True) \
  ])


# In[5]:


receipts_schema = StructType([ \
    StructField("transaction_hash", StringType(), True), \
    StructField("transaction_index", LongType(), True), \
    StructField("block_hash", StringType(), True), \
    StructField("block_number", LongType(), True), \
    StructField("cumulative_gas_used", LongType(), True), \
    StructField("gas_used", LongType(), True), \
    StructField("contract_address", StringType(), True), \
    StructField("root", StringType(), True), \
    StructField("status", LongType(), True), \
    StructField("effective_gas_price", LongType(), True) \
  ])


# In[6]:


tokens_schema = StructType([ \
    StructField("address", StringType(), True), \
    StructField("symbol", StringType(), True), \
    StructField("name", StringType(), True), \
    StructField("decimals", LongType(), True), \
    StructField("total_supply", LongType(), True), \
    StructField("block_number", LongType(), True), \
  ])


# In[7]:


cmc_historical_schema = StructType([ \
    StructField("id", LongType(), True), \
    StructField("rank", LongType(), True), \
    StructField("open", DoubleType(), True), \
    StructField("close", DoubleType(), True), \
    StructField("timestamp", LongType(), True), \
  ])


# In[8]:


# In[ ]:





# In[9]:


# base_path = "s3a://octan-labs-ethereum/export"


# In[ ]:





# In[10]:


token_transfers_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(token_transfers_schema) \
    .load(base_path + "/token_transfers/start_block={start_block}/*/*.csv".format(start_block = start_block))


# In[ ]:


transactions_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(transactions_schema) \
    .load(base_path + "/transactions/start_block={start_block}/*/*.csv".format(start_block = start_block))


# In[ ]:


receipts_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(receipts_schema) \
    .load(base_path + "/receipts/start_block={start_block}/*/*.csv".format(start_block = start_block))


# In[ ]:


tokens_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(tokens_schema) \
    .load("s3a://octan-labs-bsc/cmc_tokens/cmc_{name}_tokens.csv".format(name = name))
cmc_historicals_df = spark.read.format("csv") \
    .schema(cmc_historical_schema) \
    .load("s3a://octan-labs-bsc/cmc_historicals/*.csv")
cmc_addresses_df = spark.read.format("csv") \
    .option("header", True) \
    .load("s3a://octan-labs-bsc/cmc_addresses/*.csv")


# In[ ]:





# In[ ]:


cmc_addresses_df = cmc_addresses_df.dropDuplicates(["{name}".format(name = name)])


# In[ ]:





# In[ ]:


from pyspark.sql.functions import col, format_number, regexp_replace


# In[ ]:


transactions_df = transactions_df.drop(col("input"))


# In[ ]:





# In[ ]:


token_transfers_df.createOrReplaceTempView("token_transfers")
transactions_df.createOrReplaceTempView("transactions")
receipts_df.createOrReplaceTempView("receipts")
tokens_df.createOrReplaceTempView("tokens")
cmc_historicals_df.createOrReplaceTempView("cmc_historicals")
cmc_addresses_df.createOrReplaceTempView("cmc_addresses")


# In[ ]:





# In[ ]:


# change name, symbol foreach networks

import time
from pyspark.sql import functions as F


start_time = time.time()


result_df = spark.sql("""
SELECT 
    tx.block_number,
    tx.from_address,
    tx.to_address,
    tx.gas,
    r.gas_used,
    tx.gas_price,
    tx.value,
    null as token_transfer,
    null as token_contract,
    ((tx.value / POWER(10, 18)) * cmc_h.open) AS volume,
    (r.gas_used * tx.gas_price) / POWER(10,18) as gas_spent,
    ((r.gas_used * tx.gas_price) / POWER(10,18)) * cmc_h.open as gas_spent_usd
FROM transactions tx
LEFT JOIN receipts r ON tx.hash = r.transaction_hash
LEFT JOIN cmc_historicals cmc_h 
    ON (
        cmc_h.id = '{cmc_id}'  AND 
        tx.block_timestamp < cmc_h.timestamp AND 
        tx.block_timestamp >  cmc_h.timestamp - 86400
    )
UNION ALL
SELECT 
    tt.block_number,
    tt.from_address,
    tt.to_address,
    tx.gas,
    r.gas_used,
    tx.gas_price,
    null as value,
    tt.value as token_transfer,
    tt.token_address,
    ((tt.value / POWER(10, t.decimals)) * cmc_h.open) AS volume,
    (r.gas_used * tx.gas_price) / POWER(10,18) as gas_spent,
    ((r.gas_used * tx.gas_price) / POWER(10,18)) * native_cmc_h.open as gas_spent_usd
FROM token_transfers tt
LEFT JOIN transactions tx ON tt.transaction_hash = tx.hash
LEFT JOIN receipts r ON tt.transaction_hash = r.transaction_hash
LEFT JOIN tokens t ON LOWER(tt.token_address) = LOWER(t.address)
LEFT JOIN cmc_addresses cmc_addr 
    ON tt.token_address = cmc_addr.{name}
LEFT JOIN cmc_historicals cmc_h 
    ON (
        cmc_addr.id = cmc_h.id AND 
        tx.block_timestamp < cmc_h.timestamp AND 
        tx.block_timestamp >  cmc_h.timestamp - 86400
    )
LEFT JOIN cmc_historicals native_cmc_h 
    ON (
        native_cmc_h.id = '{cmc_id}'  AND 
        tx.block_timestamp < native_cmc_h.timestamp AND 
        tx.block_timestamp >  native_cmc_h.timestamp - 86400
    )
""".format(name = name, cmc_id = cmc_id)) \
    .withColumn('volume', regexp_replace(format_number("volume", 10), ",", "")) \
    .withColumn('gas_spent', regexp_replace(format_number('gas_spent', 10), ",", "")) \
    .withColumn('gas_spent_usd', regexp_replace(format_number('gas_spent_usd', 10), ",", ""))


time.time() - start_time


# In[ ]:


start_time = time.time()

result_df.repartition(1) \
    .write \
    .option("header",True) \
    .csv("s3a://octan-labs-ethereum/export/pre_tx/start_block={start_block}".format(start_block = start_block))

time.time() - start_time


# In[ ]:


spark.stop()


# In[ ]:




