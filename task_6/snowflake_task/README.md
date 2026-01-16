# Snowflake + Airflow Mini-DWH (Airline Dataset)

This project meets the requirements of the assignment:

- Mini-DWH on Snowflake (≈5–10 tables) with multiple layers (Stage1 → Stage2 → Stage3)
- Data loading **in multiple ways through Airflow**
  - Option A: `PUT` → internal stage → `COPY INTO` (via stored procedure)
  - Option B: direct `write_pandas` (PythonOperator)
- Main pipeline **Stage1 → Stage2 → Stage3** using **procedures + streams**, orchestrated by Airflow
- Audit/logging of affected row counts (insert/update) in a separate table
- Examples of **2 DDL + 2 DML** operations using **Time Travel**
- **Secure View** + **Row Level Security (Row Access Policy)** for fact tables
- (Optional) notes on how to connect BI tools

---

## 1) Prerequisites

- Docker + Docker Compose
- Snowflake Trial account
- Snowflake parameters:
  - account identifier (e.g., `xy12345.eu-central-1`)
  - user / password
  - role (e.g., `ACCOUNTADMIN` during setup)
  - warehouse can be left as default or create `AIRFLOW_WH`

---

## 2) Quick Start

### 2.1 Secrets and Environment Variables

```bash
cp .env.example .env
```

Fill in `.env` with your Snowflake credentials and account.  
**Important:** `.env` is in `.gitignore`.

### 2.2 Run Airflow

```bash
make up
```

Airflow UI: http://localhost:8080  
Login/password: `airflow / airflow` (see `.env.example`).

---

## 3) Snowflake: Layers and Objects

SQL files in `snowflake/sql/`:

- `00_setup.sql` — DB/SCHEMA/WAREHOUSE, file format, internal stage, audit table
- `01_tables.sql` — Stage1/Stage2/Stage3 tables + stream
- `02_procedures.sql` — procedures:
  - `STAGE1_RAW.SP_LOAD_RAW_FROM_STAGE(run_id STRING)`
  - `STAGE2_DW.SP_TRANSFORM_RAW_TO_DW(run_id STRING)` (reads `STREAM`)
  - `STAGE3_MART.SP_BUILD_MART(run_id STRING)`
- `03_security.sql` — row access policy + secure view
- `04_time_travel_examples.sql` — 2 DDL + 2 DML examples for Time Travel

---

## 4) Airflow DAGs

### A) `dwh_pipeline_copy_into`
Loading **via internal stage**:
1. `PUT` CSV into `@STAGE1_RAW.AIRLINE_INT_STAGE`
2. Procedure `SP_LOAD_RAW_FROM_STAGE` performs `COPY INTO` to `AIRLINE_RAW`
3. Procedure `SP_TRANSFORM_RAW_TO_DW` loads Stage2 **incrementally** via stream
4. Procedure `SP_BUILD_MART` updates Stage3
5. `03_security.sql` creates Secure View + RLS

### B) `dwh_pipeline_write_pandas`
Loading **directly** via Python (`write_pandas`) into Stage1,
then Stage2/Stage3 similarly.

---

## 5) How to Run

1) Enable the DAG in Airflow (toggle)
2) Click **Trigger DAG**

Results:
- Tables: `AIRLINE_DWH.*`
- Audit: `AIRLINE_DWH.AUDIT.AUDIT_LOG`
- Secure view: `AIRLINE_DWH.SECURITY.VW_FACT_TRIP_SECURE`

---

## 6) BI (Optional)

Connect Power BI / Tableau / Metabase / Looker Studio:
- account + warehouse + database `AIRLINE_DWH`
- source: `AIRLINE_DWH.SECURITY.VW_FACT_TRIP_SECURE`

---

## 7) Make Commands

```bash
make up
make down
make logs
make fmt
make lint
```

---

## Security

- RLS via **Row Access Policy** (see `03_security.sql`)
- For production approach: key-pair auth and secret backend (Vault/SM), not `.env`
