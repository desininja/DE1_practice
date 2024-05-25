from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import os

os.environ.setdefault('HADOOP_CONF_DIR', '/etc/hadoop/conf')
os.environ.setdefault('SPARK_CONF_DIR','/etc/spark/conf')

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 1, 4)
}

sample_spark_dag = DAG(
    'sample_spark_dag',
    default_args=default_args,
    catchup=False,
    description='Demo of Spark Operator',
    schedule_interval="@once"
)

"""
Place the input spark file at /home/hadoop/transactions_per_item.py
NOTE: create spark connection 'spark_default' to use run spark application locally
"""

transactions_per_item = SparkSubmitOperator(task_id='transactions_per_item',
    application='/home/hadoop/transactions_per_item.py',
    conn_id='spark_default',
    spark_binary='spark-submit',
    dag=sample_spark_dag)

transactions_per_item

"""
Output will be at: hdfs:///data/credit_card/transactions_per_item
"""
