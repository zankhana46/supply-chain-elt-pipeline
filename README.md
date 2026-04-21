# Supply Chain ELT Pipeline

An end-to-end batch ELT pipeline that transforms raw supply chain data into a dimensional star schema for logistics analytics — built with dbt, DuckDB, Airflow, and Docker.

## Business Context

A global logistics company needs visibility into delivery performance, shipping delays, and order fulfillment across regions and carriers. This pipeline ingests raw order data (180K+ records across 50+ fields), transforms it into a clean star schema, and runs automated data quality checks — enabling operations teams to identify bottleneck shipping modes, underperforming routes, and late delivery patterns.

**Key insight from the data:** First Class shipping has a 95% late delivery rate — not because it's slow, but because the 1-day SLA is rarely achievable. Standard Class meets its 4-day SLA consistently at 62% on-time.

## Architecture

```
CSV (Source) → Python Extract → Parquet (Data Lake) → DuckDB (Warehouse)
                                                           │
                                                     dbt Transform
                                                           │
                                          ┌────────────────┼────────────────┐
                                          │                │                │
                                     raw schema      staging schema    marts schema
                                    (source data)   (cleaned/cast)   (star schema)
```

**Orchestration:** Apache Airflow (Dockerized) schedules and monitors the full pipeline daily.  
**CI/CD:** GitHub Actions runs `dbt run` + `dbt test` on every push to `main`.

## Star Schema

| Table | Type | Description | Rows |
|-------|------|-------------|------|
| `fact_order_items` | Fact | One row per order line item — quantities, sales, profit, shipping delays | 180,519 |
| `dim_customer` | Dimension | Customer profiles and segments | 20,652 |
| `dim_product` | Dimension | Products with categories and departments | 118 |
| `dim_geography` | Dimension | Shipping destinations with lat/long | 61,420 |
| `dim_shipping` | Dimension | Shipping modes and order statuses | 36 |
| `dim_date` | Dimension | Calendar table with year, quarter, month, day of week | 1,133 |

## Tech Stack

- **Extract/Load:** Python, Pandas, PyArrow
- **Storage:** Parquet (data lake), DuckDB (warehouse)
- **Transform:** dbt-core with dbt-duckdb adapter
- **Data Quality:** dbt tests (21 tests: uniqueness, not null, referential integrity)
- **Orchestration:** Apache Airflow 2.10 (Dockerized with PostgreSQL backend)
- **CI/CD:** GitHub Actions
- **Containerization:** Docker + Docker Compose

## Project Structure

```
supply-chain-elt-pipeline/
├── extract/                  # Python extract scripts (CSV → Parquet)
├── load/                     # Python load scripts (Parquet → DuckDB)
├── dbt_project/supply_chain/
│   ├── models/
│   │   ├── staging/          # Source definitions + cleaning layer
│   │   └── marts/            # Star schema (facts + dimensions)
│   └── macros/               # Custom schema name macro
├── dags/                     # Airflow DAG definition
├── docker/                   # Docker Compose for Airflow
├── tests/                    # Seed data for CI
├── data/
│   ├── raw/                  # Landing zone (Parquet files)
│   └── warehouse/            # DuckDB database
└── .github/workflows/        # CI/CD pipeline
```

## Setup

### Prerequisites
- Python 3.12+
- Docker Desktop

### Quick Start
```bash
# Clone and setup
git clone https://github.com/zankhana46/supply-chain-elt-pipeline.git
cd supply-chain-elt-pipeline
python3 -m venv .venv-supply-chain
source .venv-supply-chain/bin/activate
pip install -r requirements.txt

# Download DataCo dataset from Kaggle and place in data/raw/
# https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis

# Run the pipeline
python extract/extract_supply_chain.py
python load/load_to_duckdb.py
cd dbt_project/supply_chain
dbt run
dbt test

# Start Airflow (optional)
cd ../../docker
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
# Visit http://localhost:8081
```

## Data Quality

21 automated dbt tests run on every pipeline execution and every git push:

- **Uniqueness:** All dimension primary keys are unique
- **Not null:** No nulls in critical fact table columns or dimension keys  
- **Referential integrity:** Every foreign key in the fact table has a matching dimension record

## Key Design Decisions

- **ELT over ETL:** Raw data is loaded as-is, then transformed in dbt. This preserves the original data and makes transformations version-controlled and repeatable.
- **Parquet as landing format:** ~9x compression vs CSV, columnar storage, typed — industry standard for data lakes.
- **Surrogate keys for geography:** Raw data has no location ID, so MD5 hashes of city+state+country create stable dimension keys.
- **Views for staging, tables for marts:** Staging models are views (no data duplication), mart models are materialized tables (query performance).
- **Airflow in Docker:** Avoids local dependency conflicts and mirrors production deployment patterns.

## Dataset

[DataCo Smart Supply Chain for Big Data Analysis](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis) — 180K+ order records with customer, product, shipping, and financial data across multiple global markets.