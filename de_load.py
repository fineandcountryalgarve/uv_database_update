import pandas as pd
from app.utils.db_engine import get_engine, kill_stale_sessions
from app.utils.get_base_path import get_base_path
from app.utils.bq_pandas_helper import upload_df_to_bq
from typing import Dict, List, Optional
from pathlib import Path
from sqlalchemy import text

base_path = get_base_path()

engine = get_engine ()

def save_selected_to_sql(all_files, selection=None):
    selection = selection or list(all_files.keys())

    for name in selection:
        xlsx_path = all_files[name]
        if not xlsx_path.exists():
            print(f"⚠️ Skipping {name}: file not found at {xlsx_path}")
            continue

        df = pd.read_excel(xlsx_path, engine="openpyxl")

        try:
            # First try to truncate + append
            with engine.begin() as conn:
                kill_stale_sessions(engine)
                conn.execute(text(f"TRUNCATE TABLE bronze.{name};"))
                df.to_sql(
                    name,
                    con=conn,
                    schema="bronze",
                    if_exists="append",
                    index=False
                )
                print(f"✅ {name}: {len(df):,} rows refreshed in bronze.{name}")

        except Exception as e:
            print(f"⚠️ {name}: structure mismatch or missing table, recreating it...")
            # Start a *new* transaction for the recreation
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS bronze.{name} CASCADE;"))
                df.to_sql(
                    name,
                    con=conn,
                    schema="bronze",
                    if_exists="replace",
                    index=False
                )
                print(f"✅ {name}: table recreated with {len(df):,} rows.")

        # If both fail, print error for debugging
        except Exception as e2:
            print(f"❌ Error loading {name}: {e2}")

def upload_selected_to_bigquery(all_files, selection=None, dataset="bronze", location="EU"):
    selection = selection or list(all_files.keys())
    
    for name in selection:
        xlsx_path = all_files[name]
        if not xlsx_path.exists():
            print(f"⚠️ Skipping {name}: file not found at {xlsx_path}")
            continue
        
        df = pd.read_excel(xlsx_path, engine="openpyxl")
        
        # Clean column names for BigQuery
        df.columns = (
            df.columns
            .str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)  # Replace invalid chars with _
            .str.strip('_')  # Remove leading/trailing underscores
        )
        
        # Clean object columns that might have mixed types
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).replace('nan', None)
        
        try:
            upload_df_to_bq(
                df, 
                table_name=name, 
                dataset=dataset,
                write_mode="WRITE_TRUNCATE",
                location=location
            )
            print(f"✅ {name}: {len(df):,} rows uploaded to {dataset}.{name} in BigQuery ({location})")
        
        except Exception as e:
            print(f"❌ Error uploading {name} to BigQuery: {e}")

def cleanup_tmp_folder():
    base = Path(base_path)
    for f in base.glob("*.xlsx"):
        try:
            f.unlink()
            print(f"🧹 Deleted {f}")
        except Exception as e:
            print(f"⚠️ Could not delete {f}: {e}")
