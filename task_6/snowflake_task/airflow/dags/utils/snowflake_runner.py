from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Iterable, Optional

import snowflake.connector
from snowflake.connector.util_text import split_statements


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    return v if v not in ("", None) else default


def get_conn_kwargs() -> dict:
    account = _env("SNOWFLAKE_ACCOUNT")
    user = _env("SNOWFLAKE_USER")
    password = _env("SNOWFLAKE_PASSWORD")
    host = _env("SNOWFLAKE_HOST")

    if account and user and password:
        kwargs = {
            "account": account,
            "user": user,
            "password": password,
            "role": _env("SNOWFLAKE_ROLE"),
            "warehouse": _env("SNOWFLAKE_WAREHOUSE"),
            "database": _env("SNOWFLAKE_DATABASE"),
            "schema": _env("SNOWFLAKE_SCHEMA"),
        }
        if host:
            kwargs["host"] = host
        return {k: v for k, v in kwargs.items() if v}

    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("snowflake_default")
        extra = conn.extra_dejson or {}

        account = conn.host or extra.get("account")
        if not account:
            raise RuntimeError("Airflow connection snowflake_default has no host/account.")

        kwargs = {
            "account": account,
            "user": conn.login,
            "password": conn.password,
            "role": extra.get("role"),
            "warehouse": extra.get("warehouse"),
            "database": extra.get("database"),
            "schema": extra.get("schema") or conn.schema,
        }
        if host:
            kwargs["host"] = host
        return {k: v for k, v in kwargs.items() if v}

    except Exception as e:
        raise RuntimeError(
            "Snowflake credentials not found. Set SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER/SNOWFLAKE_PASSWORD "
            "in .env (recommended), or configure Airflow connection 'snowflake_default' with host/account."
        ) from e


def connect():
    return snowflake.connector.connect(**get_conn_kwargs())


def run_sql(sql: str) -> None:
    sql = (sql or "").strip()
    if not sql:
        return

    with connect() as conn:
        with conn.cursor() as cur:
            for statement, _ in split_statements(io.StringIO(sql)):
                stmt = (statement or "").strip()
                if stmt:
                    cur.execute(stmt)


def run_sql_file(path: str | Path) -> None:
    p = Path(path)
    run_sql(p.read_text(encoding="utf-8"))


def run_sql_files(paths: Iterable[str | Path]) -> None:
    for p in paths:
        run_sql_file(p)
