#!/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    month,
    dayofmonth,
    round,
    avg,
    window,
    collect_set,
    size,
)

# --------- GLOBAL CONSTANTS -------------

DATABASE_NAME = 'banking_shamithna75gedu'
RAW_DATA_TABLE = 'raw_transactions'
CLEANED_DATA_TABLE = 'transactions'

TABLE_SOURCE_LOCATION = '/user/shamithna75gedu/banking/parquet'

# ------------ Functions -----------------

def load_from_hive_tables(table, db=DATABASE_NAME):
    df = spark.read.table(f'{db}.{table}')
    return df

def load_table_to_hive(df, table_name, db=DATABASE_NAME,
                       table_source_location=TABLE_SOURCE_LOCATION, mode='append', store_format='parquet'):
    df \
        .write \
        .mode(mode) \
        .format(store_format) \
        .option('path', f'{table_source_location}/{table_name}/') \
        .saveAsTable(f'{db}.{table_name}')

# ----------------------------------------

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


# --------------------------------- Loading data from Hive tables -----------------------------------

raw_bank_transaction_df = load_from_hive_tables(RAW_DATA_TABLE)
bank_transaction_df = load_from_hive_tables(CLEANED_DATA_TABLE)

# Caching

raw_bank_transaction_df.cache()
bank_transaction_df.cache()

# ----------------------------------- Customer Behavior Insights ------------------------------------

# ----------------------------- Top 5 customers by number of transactions ---------------------------

top_five_customers = bank_transaction_df \
                        .groupBy(col('customer_id')) \
                        .agg(count(col('transaction_id')).alias('no_of_transactions')) \
                        .orderBy(col('no_of_transactions').desc()) \
                        .limit(5)
load_table_to_hive(
    df=top_five_customers,
    table_name='top_five_customers'
)

# ------------------------- Customer with the highest total withdrawal amount ------------------------

customer_with_highest_total_withdrawal = bank_transaction_df \
                                                .where(col('transaction_type') == 'withdrawal') \
                                                .groupBy(col('customer_id')) \
                                                .agg(sum(col('amount')).alias('total_withdrawal_amount')) \
                                                .orderBy(col('total_withdrawal_amount').desc()) \
                                                .limit(1)
load_table_to_hive(
    df=customer_with_highest_total_withdrawal,
    table_name='customer_with_highest_total_withdrawal'
)

# ----------------------------- Monthly transaction volume per customer ------------------------------

monthly_txns_volume = bank_transaction_df \
                        .withColumn('txn_month', month(col('txn_timestamp'))) \
                        .groupBy(col('customer_id'), col('txn_month')) \
                        .agg(
                            count(col('transaction_id')).alias('total_no_of_transactions'), 
                            round(sum(col('amount')), 2).alias('total_amount_transfered'))

load_table_to_hive(
    df=monthly_txns_volume,
    table_name='monthly_txns_volume'
)

# ---------------------------------- Channel & Platform Analytics -----------------------------------

# ---------------------------- Average transaction amount by `channel` ------------------------------

avg_txn_amt_by_channel = bank_transaction_df \
                            .groupBy(col('channel')) \
                            .agg(round(avg(col('amount')), 2).alias('avg_amount'))

load_table_to_hive(
    df=avg_txn_amt_by_channel,
    table_name='avg_txn_amt_by_channel'
)

# ---------------------------- Success rate of transactions by `channel` -----------------------------

success_rate_by_channel = raw_bank_transaction_df \
                            .withColumn('success_flag', when(col('status') == 'success', 1).otherwise(0)) \
                            .groupBy(col('channel')) \
                            .agg(round(sum(col('success_flag')) * 100 / count(col('transaction_id')), 2).alias('success_rate'))

load_table_to_hive(
    df=success_rate_by_channel,
    table_name='success_rate_by_channel'
)

# ------------------------------------------ Time-Based Analytics -----------------------------------

# ------------------------------- Hourly transaction trend (peak hours) -----------------------------

hourly_txn_trend = bank_transaction_df \
                        .groupBy(col('txn_hour')) \
                        .agg(
                            count(col('transaction_id')).alias('no_of_transaction'),
                            round(sum(col('amount')), 2).alias('total_amount_transfered')) \
                        .orderBy(col('txn_hour').desc()) \
                        .limit(10)

load_table_to_hive(
    df=hourly_txn_trend,
    table_name='hourly_txn_trend'
)

# -------------------------- Day-wise total transaction volume and amount --------------------------

day_wise_txn_volume = bank_transaction_df \
                            .groupBy(col('txn_day')) \
                            .agg(
                                count(col('transaction_id')).alias('no_of_transaction'),
                                round(sum(col('amount')), 2).alias('total_amount_transfered')) \
                            .orderBy(col('txn_day'))

load_table_to_hive(
    df=day_wise_txn_volume,
    table_name='day_wise_txn_volume'
)

# ---------------------------------- Fraud Pattern Indicators ---------------------------------------

# ------------------ Number of high-value transactions (> ₹500) per customer per day ----------------

high_value_txn_per_day = bank_transaction_df \
                            .where(col('amount') > 500) \
                            .groupBy(col('customer_id'), col('txn_day')) \
                            .agg(count(col('transaction_id')).alias('no_of_transaction'))

load_table_to_hive(
    df=high_value_txn_per_day,
    table_name='high_value_txn_per_day'
)

# -------------------- Customers with more than 3 failed transactions in a day ----------------------

customers_with_more_failed_txn = raw_bank_transaction_df \
                                    .where(col('status') == 'failed') \
                                    .withColumn('txn_day', dayofmonth(col('txn_timestamp'))) \
                                    .groupBy(col('customer_id'), col('txn_day')) \
                                    .agg(count(col('transaction_id')).alias('no_of_failed_txn')) \
                                    .orderBy(col('no_of_failed_txn').desc()) \
                                    .where(col('no_of_failed_txn') > 3) \
                                    .select(col('customer_id'))

load_table_to_hive(
    df=customers_with_more_failed_txn,
    table_name='customers_with_more_failed_txn'
)

# --------- Customers who used all three channels (ATM, Online, Branch) in the last 7 days -----------

no_of_unqiue_channel = bank_transaction_df \
                            .select(col('channel')) \
                            .distinct() \
                            .count()

customers_used_all_channels_in_week = bank_transaction_df \
                                            .groupBy(window(col('txn_timestamp'), '7 day'), col('customer_id')) \
                                            .agg(collect_set(col('channel')).alias('channel_set')) \
                                            .where(size(col('channel_set')) == no_of_unqiue_channel) \
                                            .select(col('customer_id'))

load_table_to_hive(
    df=customers_used_all_channels_in_week,
    table_name='customers_used_all_channels_in_week'
)


spark.stop()