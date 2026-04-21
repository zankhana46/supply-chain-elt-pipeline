"""
Extract layer: Reads raw CSV source data and converts to Parquet format.

Why Parquet?
- Columnar storage: queries that only need 3 columns don't read all 50
- Compressed: ~10x smaller than CSV
- Typed: preserves data types (CSV treats everything as strings)
- Industry standard: S3 data lakes, Spark, Snowflake all use Parquet

In production, this script would pull from an API, SFTP server, or S3 bucket.
We're simulating that by reading from a local CSV (the "source system").
"""

import pandas as pd
import os
from datetime import datetime


def extract_to_parquet(
    source_path: str = "data/raw/DataCoSupplyChainDataset.csv",
    output_dir: str = "data/raw/parquet"
):
    
    # --- Step 1: Check that source file exists ---
    # In production, this would be a health check on an API or S3 connection
    if not os.path.exists(source_path):
        raise FileNotFoundError(
            f"Source file not found: {source_path}. "
            f"Download it from Kaggle and place it in data/raw/"
        )
    
    # --- Step 2: Read the CSV ---
    # encoding='latin-1' because this dataset has special characters
    # (city names in Spanish/Portuguese). UTF-8 would crash.
    print(f"Reading source data from {source_path}...")
    df = pd.read_csv(source_path, encoding='latin-1')
    
    print(f"Extracted {len(df)} rows and {len(df.columns)} columns")
    
    # --- Step 3: Create output directory if it doesn't exist ---
    os.makedirs(output_dir, exist_ok=True)
    
    # --- Step 4: Write to Parquet with a timestamp ---
    # Why timestamp? In production, you'd have daily/hourly extracts.
    # The timestamp lets you track which extract produced which file.
    # This is called "landing with lineage." idempotency
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"supply_chain_{timestamp}.parquet")
    
    df.to_parquet(output_path, engine='pyarrow', index=False)
    
    # --- Step 5: Verify the output ---
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Parquet file written to: {output_path}")
    print(f"File size: {file_size_mb:.2f} MB")
    
    return output_path


# This block only runs when you execute this file directly
# (not when it's imported by Airflow or another script)
if __name__ == "__main__":
    output = extract_to_parquet()
    print(f"\nExtraction complete: {output}")