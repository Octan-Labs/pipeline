#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import argparse, sys

parser=argparse.ArgumentParser()

parser.add_argument("-b", "--base_path", help="S3 bucket base path", required=True)
parser.add_argument("-d", "--date", help="Calculate date", required=True)

args=parser.parse_args()


# In[1]:


# base_path = "s3a://octan-labs-bsc/export-by-date"
base_path = args.base_path
date = args.date


# In[ ]:





# In[2]:


from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DoubleType
from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession \
    .builder \
    .appName("TxVolumeCalculator") \
    .getOrCreate()


# In[ ]:





# In[4]:


pre_tx_schema = StructType([ \
    StructField("block_number", LongType(), True), \
    StructField("from_address", StringType(), True), \
    StructField("to_address", StringType(), True), \
    StructField("gas", LongType(), True), \
    StructField("gas_used", LongType(), True), \
    StructField("gas_price", LongType(), True), \
    StructField("value", LongType(), True), \
    StructField("token_transfer", DecimalType(38, 18), True), \
    StructField("token_contract", StringType(), True), \
    StructField("volume", DecimalType(38, 18), True), \
    StructField("gas_spent", DecimalType(38, 18), True), \
    StructField("gas_spent_usd", DecimalType(38, 18), True), \
  ])


# In[ ]:





# In[5]:


pre_tx_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(pre_tx_schema) \
    .load("{base_path}/pre-tx/date={date}/*.csv".format(base_path = base_path, date = date))


# In[6]:


pre_tx_df.show(10)


# In[ ]:





# In[8]:


pre_tx_df.createOrReplaceTempView("pre_tx_df")


# In[23]:


pre_tx_df.printSchema()


# In[ ]:





# In[13]:


# change name, symbol foreach networks

import time
from pyspark.sql.functions import format_number


start_time = time.time()

result_df = spark.sql("""
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


# In[14]:


result_df.show(10, False)


# In[ ]:





# In[15]:


start_time = time.time()

result_df.repartition(1) \
    .write \
    .option("header",True) \
    .csv("{base_path}/tx-volumes/date={date}/".format(base_path = base_path, date = date))

time.time() - start_time


# In[ ]:





# In[ ]:


spark.stop()


# In[ ]:




