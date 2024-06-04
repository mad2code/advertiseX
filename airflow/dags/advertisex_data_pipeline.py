

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
airflow_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(parent_dir)
sys.path.append(airflow_dir)
print(f"sys.path: {sys.path}")

# from helper.utils import module_from_file
# dag_dir_path = os.path.dirname(os.path.abspath(__file__))
# airflow_tasks_path = os.path.join(parent_dir, 'airflow/airflow_tasks.py')
# AirflowTasks = module_from_file(airflow_tasks_path, module_name='airflow_tasks', object_name='AirflowTasks')

from tasks.data_workflow import AirflowTasks

# Airflow DAG setup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 31),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# schedule_interval = timedelta(days=1)
schedule_interval = '@once'

# Initialize Airflow tasks
tasks = AirflowTasks()

# Define the DAG
dag = DAG('advertisex_data_pipeline', default_args=default_args, schedule_interval=schedule_interval)

# Define the tasks
t1 = PythonOperator(task_id='kafka_setup', python_callable=tasks.kafka_setup, dag=dag)
t2 = PythonOperator(task_id='spark_processing', python_callable=tasks.spark_processing, dag=dag)
t3 = PythonOperator(task_id='read_and_log_iceberg_data', python_callable=tasks.read_and_log_iceberg_data, dag=dag)
t4 = PythonOperator(task_id='monitor_data_quality', python_callable=tasks.monitor_data_quality, dag=dag)

# Set the task dependencies
t1 >> t2 >> t3 >> t4

