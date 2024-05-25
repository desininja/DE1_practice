from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 1, 4)
}

sample_bash_dag = DAG(
    'sample_bash_dag',
    default_args=default_args,
    description='Demo of Bash Operator',
    max_active_runs=1,
    catchup=True,
    schedule_interval="0 11 * * *"
)

"""
Ensure user does not exist before
  sudo id -u admin
  sudo userdel admin
"""

bash_commands = """
    id -u admin;
    if [ $? -eq 0 ]; 
      then echo "User Exists"; 
    else sudo useradd -r admin; 
      echo "User Created"; 
    fi"""

create_user = BashOperator(
    task_id='create_user',
    bash_command=bash_commands,
    dag=sample_bash_dag)

