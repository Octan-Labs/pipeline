#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession

import argparse

# In[ ]:


spark = SparkSession \
    .builder \
    .appName("MergeHourPartition") \
    .getOrCreate() 


# In[ ]:

parser=argparse.ArgumentParser()

parser.add_argument("-b", "--base_path", help="S3 bucket base path", required=True)
parser.add_argument("-d", "--date", help="Calculate date", required=True)
parser.add_argument("-e", "--entities", help="The list of entity types to merge", required=True)

args=parser.parse_args()

base_path = args.base_path
date = args.date
entities = args.entities

# In[ ]:


class EntityType:
    BLOCK = 'block'
    TRANSACTION = 'transaction'
    LOG = 'log'
    TOKEN_TRANSFER = 'token_transfer'
    TRACE = 'trace'
    CONTRACT = 'contract'
    TOKEN = 'token'


arr = entities.split(",")


# In[ ]:


def repartition(entity_name):
    df = spark.read.parquet("{base_path}/{entity_name}s/date={date}".format(date=date, base_path=base_path, entity_name=entity_name))
    df.repartition(1) \
        .write \
        .option("header",True) \
        .parquet("{base_path}/{entity_name}s/date={date}/repartition".format(date=date, entity_name=entity_name, base_path=base_path))


# In[ ]:


if EntityType.BLOCK in arr:
    repartition(EntityType.BLOCK)


# In[ ]:


if EntityType.TRANSACTION in arr:
    repartition(EntityType.TRANSACTION)


# In[ ]:


if EntityType.LOG in arr:
    repartition(EntityType.LOG)


# In[ ]:


if EntityType.TOKEN_TRANSFER in arr:
    repartition(EntityType.TOKEN_TRANSFER)


# In[ ]:


if EntityType.TRACE in arr:
    repartition(EntityType.TRACE)


# In[ ]:


if EntityType.CONTRACT in arr:
    repartition(EntityType.CONTRACT)


# In[ ]:


if EntityType.TOKEN in arr:
    repartition(EntityType.TOKEN)

