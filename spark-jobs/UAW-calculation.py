#!/usr/bin/env python
# coding: utf-8

# In[1]:


import argparse, sys

parser=argparse.ArgumentParser()

parser.add_argument("-b", "--base_path", help="S3 bucket base path", required=True)
parser.add_argument("-s", "--start", help="Start date to calculate", required=True)
parser.add_argument("-e", "--end", help="End date to calculate", required=True)

args=parser.parse_args()

base_path = args.base_path
start = args.start
end = args.end


# In[2]:


# base_path = "."
# start = "2023-07-24"
# end = "2023-07-24"


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


# In[ ]:





# In[6]:


spark = SparkSession \
    .builder \
    .appName("UAW_Calculation") \
    .getOrCreate()


# In[ ]:





# In[7]:


contracts_schema = StructType([ \
    StructField("address", StringType(), True), \
    StructField("bytecode", StringType(), True), \
    StructField("function_sighashes", StringType(), True), \
    StructField("is_erc20", StringType(), True), \
    StructField("is_erc721", StringType(), True), \
    StructField("block_number", DecimalType(38, 0), True), \
    StructField("block_timestamp", TimestampType(), True), \
    StructField("block_hash", StringType(), True), \
  ])


# In[ ]:





# In[8]:


spark.sql("set spark.sql.files.ignoreCorruptFiles=true")


# In[9]:


pre_tx_df = spark.read.format("csv") \
    .option("header", True) \
    .load(
        "{base_path}/pre_tx/{time_range}/start={start}_end={end}/" \
            .format(
                base_path = base_path, \
                time_range = time_range, \
                start = start, \
                end = end \
            ) \
    )


# In[10]:


contract_df = spark.read.format("parquet") \
    .schema(contracts_schema) \
    .load("{base_path}/contracts/*/*.parquet".format(base_path = base_path))


# In[ ]:





# In[11]:


from pyspark.sql.functions import lit, col
import time

# add "True col to contract df"
contract_df = contract_df.withColumnRenamed('address', 'from_address') \
    .withColumn("check", lit(True)) 


# In[ ]:





# In[12]:


start_time = time.time()

uaw_result_df = pre_tx_df \
    .sort('to_address') \
    .select('from_address', 'to_address') \
    .filter(col("from_address") != '0x0000000000000000000000000000000000000000') \
    .filter(col("from_address") != '0x000000000000000000000000000000000000dead') \
    .filter(col("to_address") != '0x0000000000000000000000000000000000000000') \
    .filter(col("to_address") != '0x000000000000000000000000000000000000dead') \
    .distinct() \
    .join(contract_df,on='from_address',how='left') \
    .filter("check is null") \
    .sort("to_address") \
    .groupBy("to_address") \
    .count() \
    .withColumnRenamed('to_address', 'Address') \
    .withColumnRenamed('count', 'UAW') 

time.time() - start_time


# In[ ]:





# In[13]:


# result_df.show(10, False)


# In[ ]:





# In[14]:


# start_time = time.time()

# uw_result_df = pre_tx_df \
#     .sort('to_address') \
#     .select('from_address', 'to_address') \
#     .filter(col("from_address") != '0x0000000000000000000000000000000000000000') \
#     .filter(col("from_address") != '0x000000000000000000000000000000000000dead') \
#     .filter(col("to_address") != '0x0000000000000000000000000000000000000000') \
#     .filter(col("to_address") != '0x000000000000000000000000000000000000dead') \
#     .distinct() \
#     .join(contract_df,on='from_address',how='left') \
#     .filter("check is null") \
#     .sort("to_address") \
#     .groupBy("to_address") \
#     .count() \
#     .withColumnRenamed('to_address', 'Address') \
#     .withColumnRenamed('count', 'UAW') 

# time.time() - start_time


# In[ ]:





# In[15]:


# start_time = time.time()

# from_addr_df = pre_tx_df \
#     .select('from_address') \
#     .distinct() \
#     .withColumnRenamed('from_address', 'Address')

# to_addr_df = pre_tx_df \
#     .select('to_address') \
#     .distinct() \
#     .withColumnRenamed('to_address', 'Address')

# contract_addr_df = pre_tx_df \
#     .select('token_contract') \
#     .distinct() \
#     .withColumnRenamed('token_contract', 'Address')

# uw_df = from_addr_df \
#     .union(to_addr_df) \
#     .union(contract_addr_df) \
#     .distinct()

# start_time = time.time()


# In[ ]:





# In[16]:


# uw_df.count()


# In[ ]:





# In[17]:


start_time = time.time()

uaw_result_df.repartition(1) \
    .write \
    .option("header",True) \
    .csv(
        "{base_path}/uaw/{time_range}/start={start}_end={end}/" \
            .format(
                base_path = base_path, \
                time_range = time_range, \
                start = start, \
                end = end \
            ) \
    )

time.time() - start_time


# In[ ]:





# In[18]:


spark.stop()


# In[ ]:




