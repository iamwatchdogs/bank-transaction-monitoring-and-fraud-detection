#!/bin/bash

hdfs dfs -cat '/user/shamithna75gedu/banking/raw/banking_transactions.csv' | tail +2 | \
kafka-console-producer.sh \
    --bootstrap-server master:9092 \
    --topic bank_txn_shamithna75gedu