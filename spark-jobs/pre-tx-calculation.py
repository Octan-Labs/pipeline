#!/usr/bin/env python
# coding: utf-8

# In[1]:


import argparse, sys

parser=argparse.ArgumentParser()

parser.add_argument("-b", "--base_path", help="S3 bucket base path", required=True)
parser.add_argument("-s", "--start", help="Start date to calculate", required=True)
parser.add_argument("-e", "--end", help="End date to calculate", required=True)
parser.add_argument("-n", "--name", help="Name of chain to calculate [bsc|eth|polygon]", required=True)
parser.add_argument("-c", "--cmc_id", help="CMC ID 's native token to calculate [BNB=1839|ETH=1027|MATIC=3890]", required=True)
parser.add_argument("-ep", "--export_path", help="S3 bucket export path", required=True)
parser.add_argument("-bh", "--by_hour", help="Calculate by hour")

args=parser.parse_args()

base_path = args.base_path
start = args.start
end = args.end
name = args.name # [bsc|eth|polygon]
cmc_id = args.cmc_id  # [BNB=1839|ETH=1027|MATIC=3890]
export_path = args.export_path
pattern_file = '*.parquet'
if args.by_hour is not None and args.by_hour.lower() == 'true':
    pattern_file = '*/*.parquet'

# In[2]:


# base_path = "."
# start = "2023-07-24"
# end = "2023-07-24"
# name = "eth" # [bsc|eth|polygon]
# cmc_id = "1027" # [BNB=1839|ETH=1027|MATIC=3890]


# In[3]:


from datetime import datetime, timedelta

def get_dates_in_range(start, end):
    # Convert start and end strings to datetime objects
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    # Create a list to store the dates within the range
    dates_list = []

    # Loop through the dates and append them to the list
    current_date = start_date
    while current_date <= end_date:
        dates_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return dates_list


# In[4]:


dates = get_dates_in_range(start, end)

num_of_date = len(dates)
time_range = ""

if(num_of_date == 1):
    time_range = "date"
elif(num_of_date < 7 or num_of_date == 7):
    time_range = "week"
else:
    time_range = "month"


# In[ ]:





# In[5]:


from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DoubleType, TimestampType
from pyspark.sql import SparkSession


# In[6]:


spark = SparkSession \
    .builder \
    .appName("PreTxAndVolumeCalculator") \
    .getOrCreate()


# In[ ]:





# In[7]:


token_transfers_schema = StructType([ \
    StructField("token_address", StringType(), True), \
    StructField("from_address", StringType(), True), \
    StructField("to_address", StringType(), True), \
    StructField("value", StringType(), True), \
    StructField("transaction_hash", StringType(), True), \
    StructField("log_index", DecimalType(38, 0), True), \
    StructField("block_number", DecimalType(38, 0), True), \
    StructField("block_timestamp", TimestampType(), True), \
    StructField("block_hash", StringType(), True), \
  ])


# In[8]:


transactions_schema = StructType([ \
    StructField("hash", StringType(), True), \
    StructField("nonce", DecimalType(38, 0), True), \
    StructField("transaction_index", DecimalType(38, 0), True), \
    StructField("from_address", StringType(), True), \
    StructField("to_address", StringType(), True), \
    StructField("value", DecimalType(38, 0), True), \
    StructField("gas", DecimalType(38, 0), True), \
    StructField("gas_price", DecimalType(38, 0), True), \
    StructField("input", StringType(), True), \
    StructField("block_timestamp", TimestampType(), True), \
    StructField("block_number", DecimalType(38, 0), True), \
    StructField("block_hash", StringType(), True), \
    StructField("max_fee_per_gas", DecimalType(38, 0), True), \
    StructField("max_priority_fee_per_gas", DecimalType(38, 0), True), \
    StructField("transaction_type", LongType(), True), \
    StructField("receipt_cumulative_gas_used", DecimalType(38, 0), True), \
    StructField("receipt_gas_used", DecimalType(38, 0), True), \
    StructField("receipt_contract_address", StringType(), True), \
    StructField("receipt_root", StringType(), True), \
    StructField("receipt_status", DecimalType(38, 0), True), \
    StructField("receipt_effective_gas_price", DecimalType(38, 0), True) \
  ])


# In[9]:


tokens_schema = StructType([ \
    StructField("address", StringType(), True), \
    StructField("symbol", StringType(), True), \
    StructField("name", StringType(), True), \
    StructField("decimals", LongType(), True), \
    StructField("total_supply", DecimalType(38,0), True), \
    StructField("block_number", LongType(), True), \
  ])


# In[10]:


cmc_historical_schema = StructType([ \
    StructField("id", LongType(), True), \
    StructField("rank", LongType(), True), \
    StructField("open", DoubleType(), True), \
    StructField("close", DoubleType(), True), \
    StructField("timestamp", LongType(), True), \
  ])


# In[ ]:





# In[11]:


spark.sql("set spark.sql.files.ignoreCorruptFiles=true")


# In[12]:


token_transfers_df = spark.read.format("parquet") \
    .schema(token_transfers_schema) \
    .load(list(map(lambda date: "{base_path}/token_transfers/date={date}/{pattern_file}".format(base_path = base_path, date = date, pattern_file = pattern_file), dates)))


# In[13]:


transactions_df = spark.read.format("parquet") \
    .load(list(map(lambda date: "{base_path}/transactions/date={date}/{pattern_file}".format(base_path = base_path, date = date, pattern_file = pattern_file), dates)))


# In[14]:


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


# In[15]:


# tokens_df = spark.read.format("csv") \
#     .option("header", True) \
#     .schema(tokens_schema) \
#     .load("{base_path}/cmc_tokens/cmc_{name}_tokens.csv".format(base_path = base_path, name = name))
# cmc_historicals_df = spark.read.format("csv") \
#     .schema(cmc_historical_schema) \
#     .load("{base_path}/cmc_historicals/*.csv".format(base_path = base_path))
# cmc_addresses_df = spark.read.format("csv") \
#     .option("header", True) \
#     .load("{base_path}/cmc_addresses/*.csv".format(base_path = base_path))


# In[ ]:





# In[16]:


cmc_addresses_df.printSchema()


# In[ ]:





# In[17]:


cmc_addresses_df = cmc_addresses_df.dropDuplicates(["{name}".format(name = name)])


# In[ ]:





# In[18]:


from pyspark.sql.functions import col, format_number, regexp_replace


# In[19]:


transactions_df = transactions_df.drop(col("input"))


# In[ ]:





# In[20]:


token_transfers_df.createOrReplaceTempView("token_transfers")
transactions_df.createOrReplaceTempView("transactions")
tokens_df.createOrReplaceTempView("tokens")
cmc_historicals_df.createOrReplaceTempView("cmc_historicals")
cmc_addresses_df.createOrReplaceTempView("cmc_addresses")


# In[ ]:





# In[21]:


# change name, cmc_id foreach networks

import time
from pyspark.sql import functions as F


start_time = time.time()


pre_tx_df = spark.sql("""
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
LEFT JOIN cmc_historicals cmc_h 
    ON (
        cmc_h.id = '{cmc_id}'  AND 
        tx.block_timestamp < timestamp(cmc_h.timestamp) AND 
        tx.block_timestamp >  timestamp(cmc_h.timestamp - 86400)
    )
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
LEFT JOIN cmc_addresses cmc_addr 
    ON tt.token_address = cmc_addr.{name}
LEFT JOIN cmc_historicals cmc_h 
    ON (
        cmc_addr.id = cmc_h.id AND 
        tx.block_timestamp < timestamp(cmc_h.timestamp) AND 
        tx.block_timestamp >  timestamp(cmc_h.timestamp - 86400)
    )
LEFT JOIN cmc_historicals native_cmc_h 
    ON (
        native_cmc_h.id = '{cmc_id}'  AND 
        tx.block_timestamp < timestamp(native_cmc_h.timestamp) AND 
        tx.block_timestamp >  timestamp(native_cmc_h.timestamp - 86400)
    )
""".format(name = name, cmc_id = cmc_id)) \
    .withColumn('volume', regexp_replace(format_number("volume", 10), ",", "")) \
    .withColumn('gas_spent', regexp_replace(format_number('gas_spent', 10), ",", "")) \
    .withColumn('gas_spent_usd', regexp_replace(format_number('gas_spent_usd', 10), ",", ""))


time.time() - start_time


# In[ ]:





# In[22]:


start_time = time.time()

pre_tx_df.repartition(1) \
    .write \
    .option("header",True) \
    .csv(
        "{export_path}/pre_tx/{time_range}/start={start}_end={end}/" \
            .format(
                export_path = export_path, \
                time_range = time_range, \
                start = start, \
                end = end \
            ) \
    )


time.time() - start_time


# In[ ]:



# In[26]:


spark.stop()


# In[ ]:




