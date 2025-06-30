CREATE DATABASE IF NOT EXISTS banking_shamithna75gedu
LOCATION '/user/shamithna75gedu/banking';

use banking_shamithna75gedu;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_transactions
(
  transaction_id INT,
  customer_id INT,
  txn_timestamp TIMESTAMP,
  amount DOUBLE,
  transaction_type STRING,
  status STRING
)
PARTITIONED BY (channel STRING)
STORED AS PARQUET
LOCATION '/user/shamithna75gedu/banking/parquet/raw_transactions/';

CREATE EXTERNAL TABLE IF NOT EXISTS transactions
 (
  transaction_id INT,
  customer_id INT,
  txn_timestamp TIMESTAMP,
  amount DOUBLE,
  transaction_type STRING,
  status STRING,
  txn_day STRING, 
  txn_hour INT , 
  high_value_flag INT
 ) 
PARTITIONED BY (channel STRING)
STORED AS PARQUET
LOCATION '/user/shamithna75gedu/banking/parquet/transactions/';