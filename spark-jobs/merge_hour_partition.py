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

base_path = f's3a://{"/".join(args.base_path.split("/")[2:])}'
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
        .parquet("{base_path}/{entity_name}s/date={date}/repartition".format(date=date, entity_name=entity_name, base_path=base_path))


# In[ ]:

for entity in entities:
    if EntityType.BLOCK == entity:
        repartition(EntityType.BLOCK)

    if EntityType.TRANSACTION == entity:
        repartition(EntityType.TRANSACTION)

    if EntityType.LOG == entity:
        repartition(EntityType.LOG)

    if EntityType.TOKEN_TRANSFER == entity:
        repartition(EntityType.TOKEN_TRANSFER)

    if EntityType.TRACE == entity:
        repartition(EntityType.TRACE)

    if EntityType.CONTRACT == entity:
        repartition(EntityType.CONTRACT)

    if EntityType.TOKEN == entity:
        repartition(EntityType.TOKEN)

