"""
Load layer: Takes Parquet files from the data lake and loads them into DuckDB.

Why DuckDB?
- It's an embedded analytical database (like SQLite but for analytics)
- Reads Parquet files natively — no row-by-row inserts needed
- Same SQL dialect as Snowflake/BigQuery, so skills transfer directly
- In production, this layer would be loading into Snowflake, BigQuery, or Redshift

Why load raw data without transforming it?
- This is the "L" in ELT — we load first, transform later in dbt
- The raw table is our "single source of truth" 
- If a dbt model breaks, we don't need to re-extract from the source
- Auditors and analysts can always go back to the raw data
"""

import duckdb
import os
import glob


def get_latest_parquet(parquet_dir: str = "data/raw/parquet") -> str:
    """
    Finds the most recent Parquet file in the landing zone.
    
    Why not just hardcode the filename?
    Because each extract creates a timestamped file. This function
    always grabs the latest one — same pattern you'd use with 
    daily extracts landing in S3.
    """
    files = glob.glob(os.path.join(parquet_dir, "supply_chain_*.parquet"))
    
    if not files:
        raise FileNotFoundError(
            f"No Parquet files found in {parquet_dir}. "
            f"Run extract_supply_chain.py first."
        )
    
    # glob doesn't guarantee order, so we sort by filename
    # Our timestamp format (YYYYMMDD_HHMMSS) sorts chronologically
    latest = sorted(files)[-1]
    print(f"Latest parquet file: {latest}")
    return latest


def load_raw_table(
    db_path: str = "data/warehouse/supply_chain.duckdb",
    parquet_dir: str = "data/raw/parquet"
):
    """
    Loads the latest Parquet extract into DuckDB as a raw table.
    
    The raw table lives in a 'raw' schema — this keeps it separate
    from the transformed dbt models. Schema separation is standard 
    practice in any warehouse (raw → staging → marts).
    """
    
    # --- Step 1: Ensure the warehouse directory exists ---
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # --- Step 2: Find the latest extract ---
    parquet_path = get_latest_parquet(parquet_dir)
    
    # --- Step 3: Connect to DuckDB ---
    # If the .duckdb file doesn't exist, DuckDB creates it automatically
    # This is like connecting to a Snowflake warehouse, but local
    con = duckdb.connect(db_path)
    
    # --- Step 4: Create the raw schema ---
    # Schemas organize tables like folders organize files
    # raw = untouched source data
    # staging = cleaned/renamed (dbt will create this)
    # marts = business-ready star schema (dbt will create this)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    
    # --- Step 5: Load Parquet directly into a table ---
    # This is one of DuckDB's superpowers — it reads Parquet natively
    # No row-by-row inserts, no pandas intermediary needed
    # CREATE OR REPLACE means we can re-run this safely (idempotent)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.supply_chain AS 
        SELECT * FROM read_parquet('{parquet_path}')
    """)
    
    # --- Step 6: Verify the load ---
    row_count = con.execute(
        "SELECT COUNT(*) FROM raw.supply_chain"
    ).fetchone()[0]
    
    col_count = con.execute("""
        SELECT COUNT(*) FROM information_schema.columns 
        WHERE table_schema = 'raw' AND table_name = 'supply_chain'
    """).fetchone()[0]
    
    print(f"Loaded {row_count} rows and {col_count} columns into raw.supply_chain")
    
    # --- Step 7: Quick preview ---
    print("\nSample data:")
    print(con.execute("SELECT * FROM raw.supply_chain LIMIT 3").df())
    
    # --- Step 8: Always close your connection ---
    con.close()
    
    return row_count


if __name__ == "__main__":
    count = load_raw_table()
    print(f"\nLoad complete: {count} rows in warehouse")

""" In any real warehouse (Snowflake, BigQuery), you separate data by layers. raw is untouched source data, 
staging is cleaned, marts is business-ready. dbt will create the staging and marts schemas later. 
This is the layered architecture interviewers ask about.

CREATE OR REPLACE — This makes the script idempotent, meaning you can run it 10 times and get the same result. 
No duplicate data, no errors. Interviewers love hearing this word."""