import os

required = ["AIRFLOW_CONN_SNOWFLAKE_DEFAULT"]
missing = [k for k in required if not os.environ.get(k)]
if missing:
    raise SystemExit(f"Missing required env vars: {missing}")
print("OK")
