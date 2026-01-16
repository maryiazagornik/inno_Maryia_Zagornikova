from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple

import pandas as pd
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from snowflake.connector.pandas_tools import write_pandas

from utils.snowflake_runner import run_sql, run_sql_files

DATA_FILE_PATH = "/opt/airflow/data/airline_dataset.csv"

DB = "AIRLINE_DWH"
RAW_SCHEMA = "STAGE1_RAW"
DW_SCHEMA = "STAGE2_DW"
MART_SCHEMA = "STAGE3_MART"

RAW_TABLE = f"{DB}.{RAW_SCHEMA}.AIRLINE_RAW"
STREAM_NAME = "AIRLINE_RAW_STREAM"
STREAM_FQN = f"{DB}.{RAW_SCHEMA}.{STREAM_NAME}"

TARGET_COLUMNS = [
    "SRC_ROW_ID",
    "PASSENGER_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "GENDER",
    "AGE",
    "NATIONALITY",
    "AIRPORT_NAME",
    "AIRPORT_COUNTRY_CODE",
    "COUNTRY_NAME",
    "AIRPORT_CONTINENT",
    "CONTINENTS",
    "DEPARTURE_DATE",
    "ARRIVAL_AIRPORT",
    "PILOT_NAME",
    "FLIGHT_STATUS",
    "TICKET_TYPE",
    "PASSENGER_STATUS",
    "SRC_FILE_NAME",
]


def _normalize_col(name: str) -> str:
    s = str(name).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    return s.strip("_")


def _prepare_dataframe_for_airline_raw(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [_normalize_col(c) for c in df.columns]

    rename_map = {
        "UNNAMED_0": "SRC_ROW_ID",
        "UNNAMED_0_": "SRC_ROW_ID",
        "PASSENGERID": "PASSENGER_ID",
        "PASSENGER": "PASSENGER_ID",
        "FIRSTNAME": "FIRST_NAME",
        "LASTNAME": "LAST_NAME",
        "COUNTRY_CODE": "AIRPORT_COUNTRY_CODE",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    if "SRC_ROW_ID" not in df.columns:
        df.insert(0, "SRC_ROW_ID", range(len(df)))

    if "SRC_FILE_NAME" not in df.columns:
        df["SRC_FILE_NAME"] = Path(csv_path).name

    if "DEPARTURE_DATE" in df.columns:
        df["DEPARTURE_DATE"] = pd.to_datetime(df["DEPARTURE_DATE"], errors="coerce").dt.date

    missing = [c for c in TARGET_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(
            "CSV cannot be loaded into AIRLINE_RAW: missing columns: "
            + ", ".join(missing)
            + f". Current cols: {list(df.columns)}"
        )

    return df[TARGET_COLUMNS]


def _sf_connect(schema: str) -> snowflake.connector.SnowflakeConnection:
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PASSWORD"]
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    database = os.getenv("SNOWFLAKE_DATABASE", DB)

    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        role=role,
        database=database,
        schema=schema,
    )


def _fetch_scalar(sql: str, schema: str = "PUBLIC") -> Any:
    with _sf_connect(schema=schema) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()


def _stream_has_data_or_missing() -> Tuple[bool, bool]:
    """
    Returns (stream_exists, has_data).
    If the stream does not exist or is not authorized, returns (False, False).
    """
    try:
        val = _fetch_scalar(f"SELECT SYSTEM$STREAM_HAS_DATA('{STREAM_FQN}');", schema=RAW_SCHEMA)
        return True, bool(val)
    except Exception as e:
        msg = str(e).lower()
        if "does not exist" in msg or "not authorized" in msg or "not authorized" in msg:
            return False, False
        raise


def setup_objects() -> None:
    run_sql_files(
        [
            "/opt/airflow/snowflake/sql/00_setup.sql",
            "/opt/airflow/snowflake/sql/01_tables.sql",
            "/opt/airflow/snowflake/sql/02_procedures.sql",
        ]
    )


def load_via_write_pandas() -> None:
    df = _prepare_dataframe_for_airline_raw(DATA_FILE_PATH)
    with _sf_connect(schema=RAW_SCHEMA) as conn:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="AIRLINE_RAW",
            schema=RAW_SCHEMA,
            database=DB,
            auto_create_table=False,
            overwrite=False,
        )
        if not success:
            raise RuntimeError(f"write_pandas failed; chunks={nchunks}, rows={nrows}")


def bootstrap_stream_if_needed() -> None:
    raw_cnt = int(_fetch_scalar(f"SELECT COUNT(*) FROM {RAW_TABLE};", schema=RAW_SCHEMA) or 0)
    dw_cnt = int(_fetch_scalar(f"SELECT COUNT(*) FROM {DB}.{DW_SCHEMA}.DIM_PASSENGER;", schema=DW_SCHEMA) or 0)

    stream_exists, has_data = _stream_has_data_or_missing()

    if raw_cnt > 0 and dw_cnt == 0 and (not stream_exists or not has_data):
        run_sql(
            f"""
            CREATE OR REPLACE STREAM {STREAM_FQN}
              ON TABLE {RAW_TABLE}
              SHOW_INITIAL_ROWS = TRUE;
            """
        )


def transform_stage2(run_id: str) -> None:
    run_sql(f"CALL {DB}.{DW_SCHEMA}.SP_TRANSFORM_RAW_TO_DW('{run_id}');")


def build_mart(run_id: str) -> None:
    run_sql(f"CALL {DB}.{MART_SCHEMA}.SP_BUILD_MART('{run_id}');")


def apply_security() -> None:
    run_sql_files(["/opt/airflow/snowflake/sql/03_security.sql"])


def validate_loaded() -> None:
    fact_cnt = int(_fetch_scalar(f"SELECT COUNT(*) FROM {DB}.{DW_SCHEMA}.FACT_TRIP;", schema=DW_SCHEMA) or 0)
    mart_cnt = int(_fetch_scalar(f"SELECT COUNT(*) FROM {DB}.{MART_SCHEMA}.FACT_TRIP_ENRICHED;", schema=MART_SCHEMA) or 0)
    kpi_cnt = int(_fetch_scalar(f"SELECT COUNT(*) FROM {DB}.{MART_SCHEMA}.DAILY_KPI;", schema=MART_SCHEMA) or 0)

    if fact_cnt == 0 or mart_cnt == 0 or kpi_cnt == 0:
        raise RuntimeError(
            f"Validation failed: FACT_TRIP={fact_cnt}, FACT_TRIP_ENRICHED={mart_cnt}, DAILY_KPI={kpi_cnt}. "
            "RAW can be loaded, but DW/MART not populated."
        )


with DAG(
    dag_id="dwh_pipeline_write_pandas",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "dwh", "write_pandas"],
) as dag:
    setup = PythonOperator(task_id="setup", python_callable=setup_objects)
    load_raw = PythonOperator(task_id="load_raw_write_pandas", python_callable=load_via_write_pandas)
    bootstrap_stream = PythonOperator(task_id="bootstrap_stream", python_callable=bootstrap_stream_if_needed)

    transform = PythonOperator(
        task_id="transform_stage2",
        python_callable=transform_stage2,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    mart = PythonOperator(
        task_id="build_mart",
        python_callable=build_mart,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    security = PythonOperator(task_id="security_rls_secure_view", python_callable=apply_security)
    validate = PythonOperator(task_id="validate_counts", python_callable=validate_loaded)

    setup >> load_raw >> bootstrap_stream >> transform >> mart >> security >> validate
