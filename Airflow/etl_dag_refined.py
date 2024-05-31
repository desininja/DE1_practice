from datetime import datetime

from airflow import DAG

from airflow.contrib.operators.sqoop_operator import SqoopOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.subdag_operator import SubDagOperator

import os
 
os.environ.setdefault('HADOOP_CONF_DIR', '/etc/hadoop/conf')
os.environ.setdefault('SPARK_CONF_DIR','/etc/spark/conf')

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 1, 16),
    'end_date': datetime(2022, 9, 17),
    'catchup': False
}

etl_dag_refined = DAG(
    'etl_dag_refined',
    default_args=default_args,
    description='ETL DAG',
    schedule_interval="@once"
)

def generate_extract_subdag(parent_dag_name, child_dag_name, args):
    extract_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args,
        schedule_interval="@once"
    )

    """
    Extract the booking table from MySql to HDFS using Sqoop
    NOTE: create a sqoop connection named 'sqoop_default' from UI
    """
    extract_booking_table = SqoopOperator(task_id='extract_booking_table',
        conn_id='sqoop_default',
        table='booking',
        cmd_type='import',
        target_dir='hdfs:///data/raw/booking/{{ ds }}',
#        where="CAST(booking_ts AS DATE)='{{ ds }}'",
#        split_by="booking_id",
        num_mappers=1,
        dag=extract_subdag)


    """
    Extract the trip table from MySql to HDFS using Sqoop
    NOTE: create a sqoop connection named 'sqoop_default' from UI
    """
    extract_trip_table = SqoopOperator(task_id='extract_trip_table',
        conn_id='sqoop_default',
        table='trip',
        cmd_type='import',
        target_dir='hdfs:///data/raw/trip/{{ ds }}',
#        where="CAST(trip_start_ts AS DATE)='{{ ds }}'",
#        split_by="trip_id",
        num_mappers=1,
        dag=extract_subdag)
    
    return extract_subdag


extract_subdag = SubDagOperator(
    task_id='extract',
    subdag=generate_extract_subdag('etl_dag_refined','extract', default_args),
    dag=etl_dag_refined,
)

bash_commands = """
    sudo alternatives --config java <<< '1'
    """

switch_java_version = BashOperator(
    task_id='switch_java_version',
    bash_command=bash_commands,
    dag=etl_dag_refined)


"""
These can be done automatically inside hive or spark actions.
But having these created upfront reveals any possible permission or HDFS issue
"""
def generate_directory_subdag(parent_dag_name,child_dag_name):

    directory_subdag = DAG(
        dag_id = "%s.%s"%(parent_dag_name,child_dag_name),
        default_args=default_args,
        schedule_interval="@once"
        )

    create_raw_booking_location = BashOperator(
        task_id='create_raw_booking_location',
        bash_command='hdfs dfs -mkdir -p /data/raw/booking',
        dag=directory_subdag)

    create_filtered_booking_location = BashOperator(
        task_id='create_filtered_booking_location',
        bash_command='hdfs dfs -mkdir -p /data/refined/booking',
        dag=directory_subdag)

    create_raw_trip_location = BashOperator(
        task_id='create_raw_trip_location',
        bash_command='hdfs dfs -mkdir -p /data/raw/trip',
        dag=directory_subdag)

    create_filtered_trip_location = BashOperator(
        task_id='create_filtered_trip_location',
        bash_command='hdfs dfs -mkdir -p /data/refined/trip',
        dag=directory_subdag)

    create_result_trip_throughput_location = BashOperator(
        task_id='create_result_trip_throughput_location',
        bash_command='hdfs dfs -mkdir -p /data/output/trip_throughput',
        dag=directory_subdag)

    create_result_car_with_most_trips_location = BashOperator(
        task_id='create_result_car_with_most_trips_location',
        bash_command='hdfs dfs -mkdir -p /data/output/car_with_most_trips',
        dag=directory_subdag)

    create_raw_booking_location >> create_filtered_booking_location
    create_raw_trip_location >> create_filtered_trip_location

    return directory_subdag


hdfs_subdag = SubDagOperator(
    task_id='create_directories',
    subdag = generate_directory_subdag('etl_dag_refined','create_directories'),
    dag=etl_dag_refined
    )


"""
Subdag for filtering and validation
"""
 
def generate_load_subdag(parent_dag_name,child_dag_name):
    load_subdag = DAG(
        dag_id = "%s.%s"%(parent_dag_name,child_dag_name),
        default_args=default_args,
        schedule_interval="@once"
        )

    """
    NOTE: create connection to connect to hive server 2 with name 'hive_cli_default'
    """

    hql_create_hive_database = "CREATE DATABASE IF NOT EXISTS events"
    create_hive_database = HiveOperator(task_id='create_hive_database', hql = hql_create_hive_database, hive_cli_conn_id = "hive_cli_default", dag=load_subdag)

    hql_create_booking_raw_table = """CREATE TABLE IF NOT EXISTS events.booking_raw (
        booking_id INT,
        city STRING,
        booking_ts TIMESTAMP,
        car_type STRING)
    PARTITIONED BY (booking_dt DATE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','"""

    hql_create_trip_raw_table = """CREATE TABLE IF NOT EXISTS events.trip_raw (
        trip_id INT,
        booking_id INT,
        pickup_location_id INT,
        dropoff_location_id INT,
        trip_start_ts TIMESTAMP,
        trip_end_ts TIMESTAMP,
        trip_distance INT)
    PARTITIONED BY (trip_dt DATE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','"""
    
    create_booking_raw_table = HiveOperator(task_id='create_booking_raw_table', hql = hql_create_booking_raw_table, hive_cli_conn_id = "hive_cli_default", dag=load_subdag)
    create_trip_raw_table = HiveOperator(task_id='create_trip_raw_table', hql = hql_create_trip_raw_table, hive_cli_conn_id = "hive_cli_default", dag=load_subdag)
    
    
    hql_add_partitions_for_booking_table = """ALTER TABLE events.booking_raw
    ADD IF NOT EXISTS PARTITION (booking_dt = '{{ ds }}')
    LOCATION 'hdfs:///data/raw/booking/{{ ds }}'"""

    hql_add_partitions_for_trip_table = """ALTER TABLE events.trip_raw
    ADD IF NOT EXISTS PARTITION (trip_dt = '{{ ds }}')
    LOCATION 'hdfs:///data/raw/trip/{{ ds }}'"""

    add_partitions_for_booking_table = HiveOperator(task_id='add_partitions_for_booking_table', hql = hql_add_partitions_for_booking_table, hive_cli_conn_id = "hive_cli_default", dag=load_subdag)
    add_partitions_for_trip_table = HiveOperator(task_id='add_partitions_for_trip_table', hql = hql_add_partitions_for_trip_table, hive_cli_conn_id = "hive_cli_default", dag=load_subdag)

    hql_create_filter_booking_table = """CREATE TABLE IF NOT EXISTS events.booking (
        booking_id INT,
        city STRING,
        booking_ts TIMESTAMP,
        car_type STRING)
    PARTITIONED BY (booking_dt DATE)
    STORED AS PARQUET
    LOCATION 'hdfs:///data/refined/booking'"""

    hql_create_filter_trip_table = """CREATE TABLE IF NOT EXISTS events.trip (
        trip_id INT,
        booking_id INT,
        pickup_location_id INT,
        dropoff_location_id INT,
        trip_start_ts TIMESTAMP,
        trip_end_ts TIMESTAMP,
        trip_distance INT)
    PARTITIONED BY (trip_dt DATE)
    STORED AS PARQUET
    LOCATION 'hdfs:///data/refined/trip'"""

    create_filter_booking_table = HiveOperator(task_id='create_filter_booking_table', hql = hql_create_filter_booking_table, hive_cli_conn_id = "hive_cli_default", dag=load_subdag)
    create_filter_trip_table = HiveOperator(task_id='create_filter_trip_table', hql = hql_create_filter_trip_table, hive_cli_conn_id = "hive_cli_default", dag=load_subdag)

    """
    NOTE: create spark connection 'spark_default' to use run spark application locally
    """

    filter_booking_table = SparkSubmitOperator(task_id='filter_booking_table', 
        application='/home/hadoop/airflow_codes/uber/filter_booking.py', 
        conn_id='spark_default', 
        spark_binary='spark-submit',
        application_args=[ "{{ ds }}" ],
        dag=load_subdag)


    filter_trip_table = SparkSubmitOperator(task_id='filter_trip_table', 
        application='/home/hadoop/airflow_codes/uber/filter_trip.py', 
        conn_id='spark_default', 
        spark_binary='spark-submit',
        application_args=[ "{{ ds }}" ],
        dag=load_subdag)

    create_hive_database >> [create_booking_raw_table, create_trip_raw_table]
    create_booking_raw_table >> add_partitions_for_booking_table >> create_filter_booking_table >> filter_booking_table
    create_trip_raw_table >> add_partitions_for_trip_table >> create_filter_trip_table >> filter_trip_table

    return load_subdag


load_subdag = SubDagOperator(
    task_id='transform',
    subdag = generate_load_subdag('etl_dag_refined','transform'),
    dag=etl_dag_refined
    )


"""
Subdag to generate KPIs 
"""

def generate_kpi_subdag(parent_dag_name,child_dag_name):
    kpi_subdag = DAG(
        dag_id = "%s.%s"%(parent_dag_name,child_dag_name),
        default_args=default_args,
        schedule_interval="@once"
        )

    hql_create_car_with_most_trips_table = """CREATE TABLE IF NOT EXISTS events.trip_throughput (
            city STRING,
            trip_throughput DOUBLE
            )
        PARTITIONED BY (booking_dt DATE)
        STORED AS PARQUET
        LOCATION 'hdfs:///data/output/trip_throughput'"""

    hql_create_trip_throughput_table = """CREATE TABLE IF NOT EXISTS events.car_with_most_trips (
            city STRING,
            car_type STRING,
            num_trips INT
        )
        PARTITIONED BY (trip_dt DATE)
        STORED AS PARQUET
        LOCATION 'hdfs:///data/output/car_with_most_trips'"""

    create_car_with_most_trips_table = HiveOperator(task_id='create_car_with_most_trips_table', hql = hql_create_car_with_most_trips_table, hive_cli_conn_id = "hive_cli_default", dag=kpi_subdag)
    create_trip_throughput_table = HiveOperator(task_id='create_trip_throughput_table', hql = hql_create_trip_throughput_table, hive_cli_conn_id = "hive_cli_default", dag=kpi_subdag)

    generate_car_with_most_trips = SparkSubmitOperator(task_id='generate_car_with_most_trips', 
        application='/home/hadoop/airflow_codes/uber/generate_car_with_most_trips.py', 
        conn_id='spark_default', 
        spark_binary='spark-submit',
        application_args=[ "{{ ds }}" ],
        dag=kpi_subdag)

    generate_trip_throughput = SparkSubmitOperator(task_id='generate_trip_throughput', 
        application='/home/hadoop/airflow_codes/uber/generate_trip_throughput.py', 
        conn_id='spark_default', 
        spark_binary='spark-submit',
        application_args=[ "{{ ds }}" ],
        dag=kpi_subdag)

    create_car_with_most_trips_table >> generate_car_with_most_trips
    create_trip_throughput_table >> generate_trip_throughput

    return kpi_subdag

kpi_subdag = SubDagOperator(
    task_id = 'generate_kpi',
    subdag= generate_kpi_subdag('etl_dag_refined','generate_kpi'),
    dag=etl_dag_refined
    )
    


# Add task dependencies

extract_subdag >> switch_java_version >> hdfs_subdag >> load_subdag >> kpi_subdag

