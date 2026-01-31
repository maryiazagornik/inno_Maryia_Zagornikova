from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from modules import etl_logic

# When this file is updated, the second DAG will be triggered automatically.
data_trigger = Dataset(f"file://{etl_logic.OUTPUT_FILE}")

# Define default arguments for the DAG
default_args = {
    'owner': 'student',
    'start_date': days_ago(1),
}

# Dag definition
with DAG('1_etl_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    # Sensor: Waits for the file to appear
    sensor = PythonSensor(
        task_id='wait_for_file',
        python_callable=etl_logic.wait_for_file,
        poke_interval=10,
        timeout=600,
        mode='poke'
    )

    # Branching: Decides what to do next based on file content
    check = BranchPythonOperator(
        task_id='check_empty',
        python_callable=etl_logic.check_empty
    )

    # Alert: Runs if the file is empty
    alert = BashOperator(
        task_id='empty_alert',
        bash_command='echo "FILE PROBLEM: Empty or missing"'
    )

    # Transformation Group: Runs if the file has data
    with TaskGroup('transform') as group:
        # Clean null values
        t1 = PythonOperator(
            task_id='clean_nulls',
            python_callable=etl_logic.clean_nulls
        )

        # Sort data by date
        t2 = PythonOperator(
            task_id='sort_data',
            python_callable=etl_logic.sort_data
        )

        # Clean special characters and update the Dataset
        t3 = PythonOperator(
            task_id='clean_content',
            python_callable=etl_logic.clean_all_content,
            outlets=[data_trigger]  # This notifies the second DAG
        )

        # Define the order of execution inside the group
        t1 >> t2 >> t3

    # Main dag structure
    sensor >> check
    check >> alert
    check >> group