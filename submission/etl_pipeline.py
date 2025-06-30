from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor

LOCAL_PATH = '/home/shamithna75gedu/capstone-project/bank-transaction/submission/src'
HDFS_PATH = '/user/shamithna75gedu/banking'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 23),
    'email': ['admin@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
        'pipeline',
        default_args=default_args,
        schedule_interval=timedelta(1)
)

hdfs_data_ingestion = BashOperator(
                        task_id='ingestion',
                        bash_command=f'{LOCAL_PATH}/data_ingestion.sh',
                        dag=dag)

check_hdfs_file = HdfsSensor(
                        task_id='wait_for_hdfs_file',
                        filepath=f'{HDFS_PATH}/raw/banking_transactions.csv', 
                        hdfs_conn_id='hdfs_default',  
                        poke_interval=10,  
                        timeout=60)

hive_table_creation = BashOperator(
                            task_id='creating_hive_table',
                            bash_command=f'/opt/hive-3.1.1/bin/hive -f {LOCAL_PATH}/hive_based_reporting.sql',
                            dag=dag)


spark_transform_and_load = BashOperator(
                                task_id='transformations', 
                                bash_command=f'/opt/spark/bin/spark-submit {LOCAL_PATH}/data_transformation.py',
                                dag=dag)

spark_analytics = BashOperator(
                        task_id='spark_analytics',
                        bash_command=f'/opt/spark/bin/spark-submit {LOCAL_PATH}/analytical_queries.py',
                        dag=dag)


hbase_table_creation = BashOperator(
                        task_id='initalize_hbase',
                        bash_command=f'{LOCAL_PATH}/hbase_table_creation.sh',
                        dag=dag)


kafka_producer = BashOperator(
                    task_id='starting_producer', 
                    bash_command=f'{LOCAL_PATH}/start_kafka_as_producer.sh', 
                    dag=dag)

real_time_fraud_detection = BashOperator(
                                task_id='real_time_fraud_detection',
                                bash_command=f'/opt/spark/bin/spark-submit real_time_fraud_detection_pipeline.py',
                                dag=dag)



[hdfs_data_ingestion, hive_table_creation, hbase_table_creation] 
check_hdfs_file << hdfs_data_ingestion
spark_transform_and_load << [check_hdfs_file, hive_table_creation]
spark_analytics << spark_transform_and_load
[kafka_producer, real_time_fraud_detection] << [spark_transform_and_load, hbase_table_creation]