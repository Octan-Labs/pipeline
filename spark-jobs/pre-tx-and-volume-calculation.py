#!/usr/bin/env python
# coding: utf-8

# In[1]:


import argparse, sys

parser=argparse.ArgumentParser()

parser.add_argument("-b", "--base_path", help="S3 bucket base path", required=True)
parser.add_argument("-s", "--start", help="Start date to calculate", required=True)
parser.add_argument("-e", "--end", help="End date to calculate", required=True)
parser.add_argument("-n", "--name", help="Name of chain to calculate [bsc|eth|polygon]", required=True)
parser.add_argument("-s", "--symbol", help="Symbol of chain to calculate [BNB|ETH|MATIC]", required=True)

args=parser.parse_args()

base_path = args.base_path
start = args.start
end = args.end
name = args.name # [bsc|eth|polygon]
symbol = args.symbol  # [BNB|ETH|MATIC]


# In[2]:


# base_path = "."
# start = "2020-10-31"
# end = "2020-10-31"
# name = "bsc" # [bsc|eth|polygon]
# symbol = "BNB" # [BNB|ETH|MATIC]


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


from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DoubleType
from pyspark.sql import SparkSession


# In[6]:


spark = SparkSession \
    .builder \
    .appName("PreTxAndVolumeCalculator") \
    .getOrCreate()


# In[ ]:





# In[7]:


tokens_schema = StructType([ \
    StructField("address", StringType(), True), \
    StructField("symbol", StringType(), True), \
    StructField("name", StringType(), True), \
    StructField("decimals", LongType(), True), \
    StructField("total_supply", DecimalType(38,0), True), \
    StructField("block_number", LongType(), True), \
  ])


# In[8]:


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


# In[9]:


cmc_address_schema = StructType([ \
    StructField("rank", LongType(), True), \
    StructField("bsc", StringType(), True), \
    StructField("eth", StringType(), True), \
    StructField("polygon", StringType(), True), \
  ])


# In[ ]:





# In[10]:


token_transfers_df = spark.read.format("parquet") \
    .load(list(map(lambda date: "{base_path}/token_transfers/date={date}/*.parquet".format(base_path = base_path, date = date), dates)))


# In[11]:


transactions_df = spark.read.format("parquet") \
    .load(list(map(lambda date: "{base_path}/transactions/date={date}/*.parquet".format(base_path = base_path, date = date), dates)))


# In[12]:


tokens_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(tokens_schema) \
    .load("{base_path}/cmc_tokens/cmc_{name}_tokens.csv".format(base_path = base_path, name = name))


# In[13]:


cmc_historicals_df = spark.read.format("csv") \
    .schema(cmc_historical_schema) \
    .load("./cmc_historicals/*.csv")


# In[14]:


cmc_addresses_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(cmc_address_schema) \
    .load("./cmc_addresses/*.csv")


# In[15]:


cmc_addresses_df = cmc_addresses_df.dropDuplicates(["{name}".format(name = name)])


# In[ ]:





# In[16]:


from pyspark.sql.functions import col, format_number


# In[17]:


transactions_df = transactions_df.drop(col("input"))


# In[ ]:





# In[18]:


token_transfers_df.createOrReplaceTempView("token_transfers")
transactions_df.createOrReplaceTempView("transactions")
tokens_df.createOrReplaceTempView("tokens")
cmc_historicals_df.createOrReplaceTempView("cmc_historicals")
cmc_addresses_df.createOrReplaceTempView("cmc_addresses")


# In[22]:


# change name, symbol foreach networks

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
LEFT JOIN cmc_addresses cmc_addr 
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





# In[20]:


start_time = time.time()

pre_tx_df.repartition(1) \
    .write \
    .option("header",True) \
    .csv(
        "{base_path}/pre_tx/{time_range}/start={start}_end={end}/" \
            .format(
                base_path = base_path, \
                time_range = time_range, \
                start = start, \
                end = end \
            ) \
    )


time.time() - start_time


# In[ ]:





# In[23]:


pre_tx_df.createOrReplaceTempView("pre_tx_df")


# In[ ]:





# In[24]:


# change name, symbol foreach networks

import time
from pyspark.sql.functions import format_number


start_time = time.time()

tx_volume_df = spark.sql("""
SELECT from_address as address, SUM(volume) as volume FROM pre_tx_df
GROUP BY from_address
UNION ALL
SELECT to_address as address, SUM(volume) as volume FROM pre_tx_df
GROUP BY to_address
UNION ALL
SELECT token_contract as address, SUM(volume) as volume FROM pre_tx_df
GROUP BY token_contract
""").withColumn('volume', format_number('volume', 10)) 


time.time() - start_time


# In[ ]:





# In[25]:


start_time = time.time()

tx_volume_df.repartition(1) \
    .write \
    .option("header",True) \
    .csv(
        "{base_path}/tx_volumes/{time_range}/start={start}_end={end}/" \
            .format(
                base_path = base_path, \
                time_range = time_range, \
                start = start, \
                end = end \
            ) \
    )

time.time() - start_time


# In[ ]:





# In[ ]:


spark.stop()


# In[ ]:




