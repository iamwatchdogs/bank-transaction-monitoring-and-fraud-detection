#!/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, dayofmonth, hour

SRC_PATH = '/user/shamithna75gedu/banking'
CSV_PATH = f'{SRC_PATH}/raw'
PARQUET_PATH = f'{SRC_PATH}/parquet'

DATABASE_NAME = 'banking_shamithna75gedu'

spark = SparkSession \
            .builder \
            .master('local[5]') \
            .appName('data-transformation-with-pyspark') \
            .enableHiveSupport() \
            .getOrCreate()

# ------------------------- Loading the dataset -------------------------------

bank_transaction_df = spark \
                        .read \
                        .csv(
                            path=f'{CSV_PATH}/banking_transaction.csv',
                            inferSchema=True,
                            header=True)

# Primary cleaning
bank_transaction_df = bank_transaction_df \
                            .withColumnRenamed('timestamp', 'txn_timestamp') \
                            .dropna()

# ------------------------- Normalize text fields -------------------------------

bank_transaction_df = bank_transaction_df \
                        .withColumn('transaction_type', lower(col('transaction_type'))) \
                        .withColumn('channel', lower(col('channel')))

# --------------------- Filtering out failed transcations ------------------------

cleaned_bank_transaction = bank_transaction_df \
                            .filter(col('status') != 'failed')

# -------------------------------- Add derived columns -----------------------------

cleaned_bank_transaction = cleaned_bank_transaction \
                                .withColumn('txn_day', dayofmonth(col('txn_timestamp'))) \
                                .withColumn('txn_hour', hour(col('txn_timestamp'))) \
                                .withColumn('high_value_flag', when(col('amount') > 500, 1).otherwise(0))

# ------------------------- Loading data into Hive tables --------------------------------------

def load_table_to_hive(df, table_name, *, partition_by, db=DATABASE_NAME,
                       table_source_location=PARQUET_PATH, mode='overwrite', store_format='parquet'):
    df \
        .write \
        .mode(mode) \
        .format(store_format) \
        .partitionBy(partition_by) \
        .option('path', f'{table_source_location}/{table_name}/') \
        .saveAsTable(f'{db}.{table_name}')

load_table_to_hive(
    df=bank_transaction_df,
    table_name='raw_transactions',
    partition_by='channel'
)

load_table_to_hive(
    df=cleaned_bank_transaction_df,
    table_name='transactions',
    partition_by='channel'
)

spark.stop()