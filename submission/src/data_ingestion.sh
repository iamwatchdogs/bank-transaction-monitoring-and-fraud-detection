#!/bin/bash

SOURCE='/home/shamithna75gedu/capstone-project/bank-transaction/dataset/banking_transaction.csv'
DESTINATION='/user/shamithna75gedu/banking/raw/'

hdfs dfs -mkdir -p "$DESTINATION"
hdfs dfs -copyFromLocal "$SOURCE" "$DESTINATION"
hdfs dfs -ls "$DESTINATION"