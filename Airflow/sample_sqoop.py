from airflow import DAG
from airflow.providers.apache.sqoop.operators.sqoop import SqoopOperator
from datetime import datetime, timedelta
import os

## Define the DAG object
DEFAULT_ARGS = {
    "owner": "admin",
    "depends_on_past": False
}
dag = DAG(
    dag_id="sample_sqoop_dag",
    default_args=DEFAULT_ARGS,
    default_view="graph",
    start_date=datetime(2022, 1, 29), ### Modify start date accordingly
    catchup=False, ### set it to True if backfill is required.
    tags=["example"],)

t1 = SqoopOperator(
    task_id='Sqoop_Job',
    conn_id='sqoop_default',
	cmd_type='import',
	table='transactions',
        driver='com.mysql.jdbc.Driver',
        num_mappers=1,
	target_dir='/data/credit_card/transactions/',
    dag=dag)

t1