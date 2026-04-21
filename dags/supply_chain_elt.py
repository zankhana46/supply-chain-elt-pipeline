"""
Airflow DAG: Supply Chain ELT Pipeline

This DAG orchestrates the full pipeline:
1. Extract: CSV → Parquet
2. Load: Parquet → DuckDB (raw schema)
3. Transform: dbt run (staging → marts star schema)
4. Test: dbt test (data quality checks)

Schedule: Daily at 6am (in production, timed after source system updates)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# Default args apply to every task in the DAG
default_args = {
    'owner': 'zankhana',
    'depends_on_past': False,
    # If a task fails, retry once after 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='supply_chain_elt',
    default_args=default_args,
    description='End-to-end supply chain ELT: extract, load, dbt transform & test',
    # Cron: minute hour day month weekday
    # '0 6 * * *' = every day at 6:00 AM
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    # Don't backfill all days since start_date
    catchup=False,
    tags=['supply-chain', 'elt'],
) as dag:

    # --- Task 1: Extract CSV to Parquet ---
    extract = BashOperator(
        task_id='extract_to_parquet',
        bash_command='cd /opt/airflow && python extract/extract_supply_chain.py',
    )

    # --- Task 2: Load Parquet into DuckDB ---
    load = BashOperator(
        task_id='load_to_duckdb',
        bash_command='cd /opt/airflow && python load/load_to_duckdb.py',
    )

    # --- Task 3: dbt run (build staging + marts) ---
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project/supply_chain && dbt run',
    )

    # --- Task 4: dbt test (data quality checks) ---
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project/supply_chain && dbt test',
    )

    # --- Define the execution order ---
    # This is the DAG (Directed Acyclic Graph)
    # Each >> means "run the next task only after the previous one succeeds"
    extract >> load >> dbt_run >> dbt_test