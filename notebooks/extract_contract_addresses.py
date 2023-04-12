import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#./bin/spark-submit --master spark://localhost:8080 /home/iceberg/notebooks/notebooks/extract_contract_addresses.py --receipts-logs from_block end_block
#./bin/spark-submit --master spark://localhost:8080 /home/iceberg/notebooks/notebooks/extract_contract_addresses.py --traces file_name

def extract_from_receipts_logs(args):
  spark = SparkSession \
    .Builder() \
    .appName("Jupyter") \
    .getOrCreate() 

  receiptSchema = StructType() \
    .add("transaction_hash", StringType(), True) \
    .add("transaction_index", LongType(), True) \
    .add("block_hash", StringType(), True) \
    .add("block_number", StringType(), True) \
    .add("cumulative_gas_used", LongType(), True) \
    .add("gas_used", LongType(), True) \
    .add("contract_address", StringType(), True) \
    .add("root", StringType(), True) \
    .add("status", LongType(), True) \
    .add("effective_gas_price", LongType(), True) 

  logSchema = StructType() \
    .add("log_index", LongType(), True) \
    .add("transaction_hash", StringType(), True) \
    .add("transaction_index", LongType(), True) \
    .add("block_hash", StringType(), True) \
    .add("block_number", LongType(), True) \
    .add("address", StringType(), True) \
    .add("data", StringType(), True) \
    .add("topics", StringType(), True)

  startBlock = args[1]
  endBlock = args[2]

  receiptDf = spark.read.format("csv") \
    .option("header", "true") \
    .schema(receiptSchema) \
    .load("/home/iceberg/warehouse/receipts/start_block=" + startBlock + "/end_block=" + endBlock + "/receipts_" + startBlock + "_" + endBlock+ ".csv")
  
  logDf = spark.read.format("csv") \
    .option("header", "true") \
    .schema(logSchema) \
    .load("/home/iceberg/warehouse/logs/start_block=" + startBlock + "/end_block=" + endBlock + "/logs_" + startBlock + "_" + endBlock+ ".csv")
  
  mergeDf = receiptDf.select("contract_address").distinct().union( \
    logDf.select(col("address").alias("contract_address")).distinct() \
  ) 
  
  resultReceiptLogDf = mergeDf.filter(col("contract_address").isNotNull())
  
  resultReceiptLogDf \
    .writeTo("demo.bsc.contract_addresses") \
    .append()

  spark.stop()


def extract_from_traces(args):
  spark = SparkSession \
    .Builder() \
    .appName("Jupyter") \
    .getOrCreate() 
  
  traceSchema = StructType() \
    .add("block_number",LongType(), True) \
    .add("transaction_hash",StringType(), True) \
    .add("transaction_index",LongType(), True) \
    .add("from_address",StringType(), True) \
    .add("to_address",StringType(), True) \
    .add("value",DecimalType(38,0), True) \
    .add("input",StringType(), True) \
    .add("output",StringType(), True) \
    .add("trace_type",StringType(), True) \
    .add("call_type",StringType(), True) \
    .add("reward_type",StringType(), True) \
    .add("gas",LongType(), True) \
    .add("gas_used",LongType(), True) \
    .add("subtraces",LongType(), True) \
    .add("trace_address",StringType(), True) \
    .add("error",StringType(), True) \
    .add("status",LongType(), True) \
    .add("trace_id",StringType(), True)
  
  traceFile = args[1]
  
  traceDf = spark.read.format("csv") \
    .option("header", "true") \
    .schema(traceSchema) \
    .load("/home/iceberg/warehouse/" + traceFile)
  
  resultTraceDf = traceDf.filter(col("trace_type") == "create") \
    .select(col("to_address").alias("contract_address"))
  
  resultTraceDf \
    .writeTo("demo.bsc.contract_addresses") \
    .append()
  
  spark.stop()

if __name__ == '__main__':
  args =  sys.argv[1:]

  print("ASAAAAAAAAAA ", args)

  if args[0] == "--receipts-logs" and len(args) == 3:
    extract_from_receipts_logs(args)
  elif args[0] == "--traces" and len(args) == 2:
    extract_from_traces(args)
  else:
    print("ERROR: invalid arguments")