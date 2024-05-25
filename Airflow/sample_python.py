from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 1, 4)
}

sample_python_dag = DAG(
    'sample_python_dag',
    default_args=default_args,
    catchup=False,
    description='Demo of Python Operator',
    schedule_interval="@daily"
)

"""
Check if the file /home/hadoop/transactions.csv exists
"""

bash_commands = """
    ls -l /home/hadoop/transactions.csv;
    if [ $? -eq 0 ]; 
      then
        echo "File Exists"; 
    else 
      echo "File does not exist"; 
      exit 1
    fi"""


def analyze_data():
  import json
  item_count = {}
  with open('/home/hadoop/transactions.csv', 'r') as data_file:
    for line in data_file.readlines():
      line = line.strip()
      if line == "":
        continue
      user_id, item_id, tr_start, tr_end, tr_amount = line.split(",")
      total_count = item_count.get(item_id, 0) + 1
      item_count[item_id] = total_count
  with open('/home/hadoop/output.json', 'w') as out_file:
    out_file.write(json.dumps(item_count, indent=2))


check_file = BashOperator(
    task_id='check_file',
    bash_command=bash_commands,
    dag=sample_python_dag)

analyze_file = PythonOperator(
    task_id='analyze_file',
    python_callable=analyze_data,
    dag=sample_python_dag)

check_file >> analyze_file
