#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import argparse, sys

parser=argparse.ArgumentParser()

parser.add_argument("-b", "--base_path", help="S3 bucket base path", required=True)
parser.add_argument("-d", "--date", help="Calculate date", required=True)
parser.add_argument("-n", "--name", help="Name of chain to calculate [bsc|eth|polygon]", required=True)
parser.add_argument("-s", "--symbol", help="Symbol of chain to calculate [BNB|ETH|MATIC]", required=True)

args=parser.parse_args()


# In[ ]:


# base_path = "s3a://octan-labs-bsc/export-by-date"
base_path = args.base_path
date = args.date
name =  args.name
symbol =  args.symbol


# In[ ]:


from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DoubleType
from pyspark.sql import SparkSession


# In[ ]:


# In[2]:


spark = SparkSession \
    .builder \
    .appName("Preprocessdata") \
    .getOrCreate()


# In[ ]:





# In[23]:


tokens_schema = StructType([ \
    StructField("address", StringType(), True), \
    StructField("symbol", StringType(), True), \
    StructField("name", StringType(), True), \
    StructField("decimals", LongType(), True), \
    StructField("total_supply", DecimalType(38,0), True), \
    StructField("block_number", LongType(), True), \
  ])


# In[4]:


cmc_historical_schema = StructType([ \
    StructField("id", LongType(), True), \
    StructField("rank", LongType(), True), \
    StructField("name", StringType(), True), \
    StructField("symbol", StringType(), True), \
    StructField("open", DoubleType(), True), \
    StructField("high", DoubleType(), True), \
    StructField("low", DoubleType(), True), \
    StructField("close", DoubleType(), True), \
    StructField("volume", DoubleType(), True), \
    StructField("marketCap", DoubleType(), True), \
    StructField("timestamp", LongType(), True), \
  ])


# In[5]:


cmc_address_schema = StructType([ \
    StructField("rank", LongType(), True), \
    StructField("bsc", StringType(), True), \
    StructField("eth", StringType(), True), \
    StructField("polygon", StringType(), True), \
  ])


# In[6]:





# In[11]:


token_transfers_df = spark.read.format("parquet") \
    .load("{base_path}/token_transfers/date={date}/*/*.parquet".format(base_path = base_path, date = date))


# In[12]:


transactions_df = spark.read.format("parquet") \
    .load("{base_path}/transactions/date={date}/*/*.parquet".format(base_path = base_path, date = date))


# In[24]:


tokens_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(tokens_schema) \
    .load("{base_path}/cmc_tokens/cmc_{name}_tokens.csv".format(base_path = base_path, name = name))


# In[12]:


# cmc_historicals_df = spark.read.format("csv") \
#     .option("header", True) \
#     .schema(cmc_historical_schema) \
#     .load("s3a://octan-labs-bsc/cmc_historicals/*.csv")


# In[13]:


# cmc_addresses_df = spark.read.format("csv") \
#     .option("header", True) \
#     .schema(cmc_address_schema) \
#     .load("s3a://octan-labs-bsc/cmc_addresses/*.csv")


# In[ ]:


cmc_historicals_df = spark.read.format("csv") \
    .schema(cmc_historical_schema) \
    .load("./cmc_historicals/*.csv")
cmc_addresses_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(cmc_address_schema) \
    .load("./cmc_addresses/*.csv")


# In[ ]:





# In[15]:


from pyspark.sql.functions import col, format_number


# In[16]:


transactions_df = transactions_df.drop(col("input"))


# In[ ]:





# In[17]:


token_transfers_df.createOrReplaceTempView("token_transfers")
transactions_df.createOrReplaceTempView("transactions")
tokens_df.createOrReplaceTempView("tokens")
cmc_historicals_df.createOrReplaceTempView("cmc_historicals")
cmc_addresses_df.createOrReplaceTempView("cmc_addresses")


# In[20]:


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
    tx.receipt_gas_used as gas_used,
    tx.gas_price,
    tx.value,
    null as token_transfer,
    null as token_contract,
    ((tx.value / POWER(10, 18)) * cmc_h.open) AS volume,
    (tx.receipt_gas_used * tx.gas_price) / POWER(10,18) as gas_spent,
    ((tx.receipt_gas_used * tx.gas_price) / POWER(10,18)) * cmc_h.open as gas_spent_usd
FROM transactions tx
CROSS JOIN cmc_addresses cmc_addr
LEFT JOIN cmc_historicals cmc_h 
    ON (
         cmc_addr.rank = cmc_h.rank AND
         tx.block_timestamp < timestamp(cmc_h.timestamp) AND 
         tx.block_timestamp >  timestamp(cmc_h.timestamp - 86400)
    )
WHERE cmc_h.symbol = '{symbol}'
UNION ALL
SELECT 
    tt.block_number,
    tt.from_address,
    tt.to_address,
    tx.gas,
    tx.receipt_gas_used as gas_used,
    tx.gas_price,
    null as value,
    tt.value as token_transfer,
    tt.token_address,
    ((tt.value / POWER(10, t.decimals)) * cmc_h.open) AS volume,
    (tx.receipt_gas_used * tx.gas_price) / POWER(10,18) as gas_spent,
    ((tx.receipt_gas_used * tx.gas_price) / POWER(10,18)) * native_cmc_h.open as gas_spent_usd
FROM token_transfers tt
LEFT JOIN transactions tx ON tt.transaction_hash = tx.hash
LEFT JOIN tokens t ON LOWER(tt.token_address) = LOWER(t.address)
LEFT JOIN (SELECT {name}, rank FROM cmc_addresses GROUP BY {name}, rank LIMIT 1) cmc_addr 
    ON tt.token_address = cmc_addr.{name}
LEFT JOIN cmc_historicals cmc_h 
    ON (
        cmc_addr.rank = cmc_h.rank AND 
        tx.block_timestamp < timestamp(cmc_h.timestamp) AND 
        tx.block_timestamp >  timestamp(cmc_h.timestamp - 86400)
    )
LEFT JOIN cmc_historicals native_cmc_h 
    ON (
         native_cmc_h.symbol = '{symbol}' AND 
         tx.block_timestamp < timestamp(native_cmc_h.timestamp) AND 
         tx.block_timestamp >  timestamp(native_cmc_h.timestamp - 86400)
    )
""".format(name = name, symbol = symbol)) \
    .withColumn('volume', format_number('volume', 10)) \
    .withColumn('gas_spent', format_number('gas_spent', 10)) \
    .withColumn('gas_spent_usd', format_number('gas_spent_usd', 10))


time.time() - start_time


# In[ ]:





# In[21]:


start_time = time.time()

result_df.repartition(1) \
    .write \
    .option("header",True) \
    .csv("{base_path}/pre-tx/date={date}/".format(base_path = base_path, date = date))

time.time() - start_time


# In[ ]:





# In[24]:


spark.stop()


# In[ ]:




