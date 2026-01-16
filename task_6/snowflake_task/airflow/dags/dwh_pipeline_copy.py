from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.snowflake_runner import run_sql, run_sql_files

DATA_FILE_PATH = "/opt/airflow/data/airline_dataset.csv"

SQL_DIR = Path("/opt/airflow/snowflake/sql")


def _setup_objects() -> None:
    run_sql_files(
        [
            SQL_DIR / "00_setup.sql",
            SQL_DIR / "01_tables.sql",
            SQL_DIR / "02_procedures.sql",
        ]
    )


def _put_file_to_stage(file_path: str) -> None:
    run_sql(
        "PUT file://{file_path} "
        "@AIRLINE_DWH.STAGE1_RAW.AIRLINE_INT_STAGE "
        "AUTO_COMPRESS=TRUE OVERWRITE=TRUE;".format(file_path=file_path)
    )


def _call_proc(sql: str, run_id: str) -> None:
    run_sql(sql.format(run_id=run_id))


def _apply_security() -> None:
    run_sql_file = (SQL_DIR / "03_security.sql").read_text(encoding="utf-8")
    run_sql(run_sql_file)


with DAG(
    dag_id="dwh_pipeline_copy_into",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "dwh", "copy_into"],
) as dag:
    setup = PythonOperator(task_id="setup", python_callable=_setup_objects)

    put_file = PythonOperator(
        task_id="put_file_to_stage",
        python_callable=_put_file_to_stage,
        op_kwargs={"file_path": DATA_FILE_PATH},
    )

    load_stage1 = PythonOperator(
        task_id="load_stage1_raw",
        python_callable=_call_proc,
        op_kwargs={
            "sql": "CALL AIRLINE_DWH.STAGE1_RAW.SP_LOAD_RAW_FROM_STAGE('{run_id}');",
            "run_id": "{{ run_id }}",
        },
    )

    transform_stage2 = PythonOperator(
        task_id="transform_stage2",
        python_callable=_call_proc,
        op_kwargs={
            "sql": "CALL AIRLINE_DWH.STAGE2_DW.SP_TRANSFORM_RAW_TO_DW('{run_id}');",
            "run_id": "{{ run_id }}",
        },
    )

    build_mart = PythonOperator(
        task_id="build_mart",
        python_callable=_call_proc,
        op_kwargs={
            "sql": "CALL AIRLINE_DWH.STAGE3_MART.SP_BUILD_MART('{run_id}');",
            "run_id": "{{ run_id }}",
        },
    )

    security = PythonOperator(task_id="security_rls_secure_view", python_callable=_apply_security)

    setup >> put_file >> load_stage1 >> transform_stage2 >> build_mart >> security
