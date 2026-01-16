import pandas as pd
from airflow.providers.mongo.hooks.mongo import MongoHook

# Input file path (must match the output of the ETL process)
INPUT_FILE = '/opt/airflow/data/processed_data.csv'


def load_to_mongo():
    # Read the clean data
    df = pd.read_csv(INPUT_FILE)

    # Convert DataFrame to a list of dictionaries (JSON-like format)
    records = df.to_dict(orient='records')

    # Connect to MongoDB using the Airflow Connection ID
    hook = MongoHook(conn_id='mongo_default')

    # Insert data
    hook.insert_many(mongo_collection='comments', docs=records, mongo_db='analytics_db')

    print(f"Successfully inserted {len(records)} rows into MongoDB.")