#!/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_csv, window, collect_list, struct, explode, lit
from pyspark.sql.window import *
import happybase
import os

# ------------------------------- Setting Options for pyspark-shell --------------------------------

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession \
            .builder \
            .master('local[5]') \
            .appName('analytical-queries') \
            .enableHiveSupport() \
            .config('spark.shuffle.service.enabled', True) \                              # Enabling external shuffle service
            .config('spark.dynamicAllocation.enabled', True) \                            # Enabling dynamic memory allocation 
            .config('spark.dynamicAllocation.shuffleTracking.enabled', True) \            # An alternative to shuffle service
            .config('spark.shuffle.io.preferDirectBufs', True) \                          # Uses direct memory for execution
            .config('spark.shuffle.sort.bypassMergeThreshold', '200') \                   # Adjusting sorting threshold
            .config('spark.shuffle.compress', True) \                                     # Enabling compression during shuffling
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \   # Using KryoSerializer
            .config("spark.kryoserializer.buffer.max", "256m") \                          # Configuring max buffer usage
            .getOrCreate()

# ------------------------------- Defining the Schema for the dataset -----------------------------

bank_txn_schema = \
'''
transaction_id INT NOT NULL,
customer_id INT,
txn_timestamp TIMESTAMP NOT NULL,
amount DOUBLE,
transaction_type STRING,
status STRING,
txn_day INT NOT NULL,
txn_hour INT NOT NULL,
channel STRING
'''

print(bank_txn_schema)

# ------------------------------- Retriving the data stream from Kafka --------------------------------

bank_txn_stream_df = spark \
                        .readStream \
                        .format('kafka') \
                        .option('kafka.bootstrap.servers', 'master:9092') \
                        .option('subscribe', 'bank_txn_shamithna75gedu') \
                        .option('checkpointLocation', 'chk_bank_txn_shamithna75gedu') \
                        .option('startingOffsets', 'latest') \
                        .load()

# ------------------------------- Applying schema to the streaming data --------------------------------

bank_txn_stream_df = bank_txn_stream_df \
                        .select(
                            from_csv(
                                col('value').cast('string'),
                                bank_txn_schema) \
                            .alias('values')) \
                        .select('values.*')

# ----------------------- Filtering based on time-window-independent conditions ------------------------

suspicious_txn_stream = bank_txn_stream_df \
                            .where(col('transaction_type') == 'withdrawal') \
                            .where(col('amount') > 400)

# --------------------------- Defining a function to process stream batch-wise --------------------------

def write_into_hbase(batch_df, batch_id):
    HBASE_HOST = 'master'
    HBASE_NAMESPACE = 'banking_shamithna75gedu'
    HBASE_TABLE = 'suspicious_transaction'
    
    required_attribute_names = [
        'transaction_count',
        'transaction_id',
        'customer_id',
        'txn_timestamp',
        'amount',
        'channel'
    ]
    
    reason_description = \
        'Withdrew amount more than 400 INR multiple times within small timeframe'

    windowed_df = batch_df \
                    .groupBy(
                        window(col('txn_timestamp'), '1 minute'),
                        col('customer_id')) \
                    .agg(count('transaction_id').alias('transaction_count')) \
                    .where(col('transaction_count') > 2)

    res_df = windowed_df \
                .join(batch_df, on='customer_id', how='inner') \
                .where(col('txn_timestamp') >= col('window.start')) \
                .where(col('txn_timestamp') <= col('window.end')) \
                .withColumn('reason', lit(reason_description)) \
                .select(*required_attribute_names)

    data = res_df.collect()

    connection = happybase.Connection(HBASE_HOST
                                     )  
    table = connection.table(f'{HBASE_NAMESPACE}.{HBASE_TABLE}')

    for row in data:
        row_key = f"{row['customer_id']}_{row['transaction_id']}"
        record_data = { 
            str(f'info:{column}').encode() : str(row[column]).encode()
            for column in required_attribute_names 
        }
        table.put(row_key, record_data)

    connection.close()

# ----------------------- Filtering based on time-window-independent conditions ------------------------

suspicious_txn_stream \
    .writeStream \
    .format('console') \
    .foreachBatch(write_into_hbase) \
    .start() \
    .awaitTermination()

spark.stop()