import pandas as pd
import re
import os
import numpy as np

INPUT_FILE = '/opt/airflow/data/input_data.csv'
OUTPUT_FILE = '/opt/airflow/data/processed_data.csv'


def wait_for_file():
    return os.path.exists(INPUT_FILE)


def check_empty():
    if not os.path.exists(INPUT_FILE) or os.stat(INPUT_FILE).st_size == 0:
        return 'empty_alert'
    try:
        df = pd.read_csv(INPUT_FILE)
        if df.empty: return 'empty_alert'
        return 'transform.clean_nulls'
    except:
        return 'empty_alert'


def clean_nulls():
    df = pd.read_csv(INPUT_FILE)
    df.columns = df.columns.str.strip()

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace(['nan', 'NaN', 'null', 'None'], '-')

    df.fillna("-", inplace=True)

    if 'content' in df.columns:
        df = df[df['content'] != "-"]
        df = df[df['content'] != "nan"]

    df.to_csv(OUTPUT_FILE, index=False)


def sort_data():
    df = pd.read_csv(OUTPUT_FILE)

    date_col = None
    possible_names = ['at', 'created_date', 'date', 'reviewCreatedOn', 'reviewCreatedAt']

    for col in df.columns:
        if col in possible_names:
            date_col = col
            break

    if date_col:
        df.rename(columns={date_col: 'created_date'}, inplace=True)
        df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
        df = df.dropna(subset=['created_date'])
        df.sort_values(by='created_date', inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)


def clean_all_content():
    df = pd.read_csv(OUTPUT_FILE)

    def strict_clean(text):
        # FIX: Handle floats/doubles causing MongoDB crash
        if pd.isna(text) or text == "-":
            return ""

        # Force convert to string to avoid "found: double" error
        text = str(text)

        if text.lower() == 'nan':
            return ""

        # Clean regex
        cleaned = re.sub(r'[^a-zA-Z0-9\s\.,?!:;\(\)\'\-_]+', '', text).strip()
        return cleaned

    # Apply to ALL columns that might be text
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(strict_clean)

    # Final cleanup: remove empty rows created by cleaning
    if 'content' in df.columns:
        df = df[df['content'] != ""]

    df.to_csv(OUTPUT_FILE, index=False)