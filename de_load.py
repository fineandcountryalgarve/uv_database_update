import pandas as pd
from sqlalchemy import text
from app.utils.bq_pandas_helper import upload_df_to_bq
from app.utils.get_parquet_path import get_parquet_path
from app.utils.db_engine import get_engine
from de_transform import clean_column_names, prepare_dataframe
from de_extract import get_base_path
from typing import List, Optional
from pathlib import Path

engine = get_engine()

base_path = get_base_path()

all_files = {
    "rawleads": base_path / "all_leads.xlsx",
    "rawevents": base_path / "all_events.xlsx",
    "rawproperties": base_path / "all_properties.xlsx",
    "rawbuyers": base_path / "all_buyers.xlsx",
    "rawsellers": base_path / "all_sellers.xlsx",
    "rawbuyerssellers": base_path / "all_buyers_sellers.xlsx",
    "rawarchived": base_path / "all_longtermrentals.xlsx",
}

def save_selected_to_parquet(selection = None):
    selection = selection or list(all_files.keys())
    for name in selection:
        xlsx_path = all_files[name]
        if not xlsx_path.exists():
            print(f"⚠️ Skipping {name}: missing file at {xlsx_path}")
            continue
        df = pd.read_excel(xlsx_path)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df = clean_column_names(df, table_name=name)
        df = prepare_dataframe(df)

        pq_path = Path(get_parquet_path(name))        # ← ensure Path
        pq_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(pq_path)
        print(f"✅ {name} saved as Parquet → {pq_path}")

def upload_selected_to_postgresql(selection: Optional[List[str]] = None):
    selection = selection or list(all_files.keys())
    engine = get_engine()
    with engine.begin() as conn:
        for name in selection:
            pq_path = Path(get_parquet_path(name))    # ← ensure Path
            if not pq_path.exists():
                print(f"⚠️ Skipping PG upload for {name}: {pq_path} not found")
                continue
            df = pd.read_parquet(pq_path)
            conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
            df.to_sql(name, con=conn, if_exists='replace', index=False, chunksize=5000, method='multi')
            print(f"✅ {name} uploaded to PostgreSQL.")

def upload_selected_to_bigquery(selection: Optional[List[str]] = None, location: str = "EU"):
    selection = selection or list(all_files.keys())
    for name in selection:
        pq_path = Path(get_parquet_path(name))
        if not pq_path.exists():
            print(f"⚠️ Skipping BQ upload for {name}: {pq_path} not found")
            continue
        df = pd.read_parquet(pq_path)
        upload_df_to_bq(df, table_name=name, location=location)
        print(f"✅ {name} uploaded to BigQuery ({location}).")

from pathlib import Path

def clean_tmp_files():
    base_path = Path("/tmp")
    patterns = ["all_*.xlsx", "*.parquet"]

    removed = 0
    for pattern in patterns:
        for file_path in base_path.glob(pattern):
            try:
                file_path.unlink()
                removed += 1
            except Exception as e:
                print(f"⚠️ Could not delete {file_path.name}: {e}")
    print(f"🧹 Removed {removed} temporary ETL files from {base_path}")

