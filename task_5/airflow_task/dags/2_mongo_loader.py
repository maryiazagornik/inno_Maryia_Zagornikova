from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from modules import mongo_logic

# Define the same Dataset to listen for updates
data_trigger = Dataset(f"file://{mongo_logic.INPUT_FILE}")

# Dag definition
with DAG(
        '2_mongo_loader',
        start_date=days_ago(1),
        schedule=[data_trigger],
        catchup=False
) as dag:
    # Load data to MongoDB
    load_task = PythonOperator(
        task_id='push_to_mongo',
        python_callable=mongo_logic.load_to_mongo
    )