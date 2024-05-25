from datetime import datetime
from airflow import DAG
from airflow.operators.hive_operator import HiveOperator


default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 1, 4)
}

sample_hive_dag = DAG(
    'sample_hive_dag',
    default_args=default_args,
    catchup=False,
    description='Demo of Hive Operator',
    schedule_interval="@once"
)


hql_create_hive_database = "CREATE DATABASE IF NOT EXISTS credit_card"

hql_create_raw_table = """CREATE TABLE IF NOT EXISTS credit_card.transactions (
        id int,
        user_id STRING,
        item_id STRING,
        transaction_start BIGINT,
        transaction_end BIGINT,
        transaction_amount DOUBLE
        )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LOCATION '/data/credit_card/transactions'"""

hql_create_filtered_table = """CREATE TABLE IF NOT EXISTS credit_card.filtered_transactions (
        id int,
        user_id STRING,
        item_id STRING,
        transaction_start BIGINT,
        transaction_end BIGINT,
        transaction_amount DOUBLE
        )
    STORED AS PARQUET
    LOCATION '/data/credit_card/filtered_transactions'"""

hql_load_filtered_table = """INSERT OVERWRITE TABLE credit_card.filtered_transactions
     SELECT * FROM credit_card.transactions
     WHERE transaction_start <= transaction_end
     AND transaction_amount IS NOT null"""


"""
Ensure 'hive_cli_default' exists
"""

create_hive_database = HiveOperator(task_id='create_hive_database', 
    hql = hql_create_hive_database, 
    hive_cli_conn_id = "hive_cli_default", 
    dag=sample_hive_dag)

create_raw_table = HiveOperator(task_id='create_raw_table', 
    hql = hql_create_raw_table, 
    hive_cli_conn_id = "hive_cli_default", 
    dag=sample_hive_dag)

create_filtered_table = HiveOperator(task_id='create_filtered_table', 
    hql = hql_create_filtered_table, 
    hive_cli_conn_id = "hive_cli_default", 
    dag=sample_hive_dag)

load_filtered_table = HiveOperator(task_id='load_filtered_table', 
    hql = hql_load_filtered_table, 
    hive_cli_conn_id = "hive_cli_default", 
    dag=sample_hive_dag)


create_hive_database >> create_raw_table >> create_filtered_table >> load_filtered_table
