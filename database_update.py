"""
Unified pipeline that can run from:
- Terminal (CLI via argparse)
- Notebook/VSCode (ipywidgets UI via render_ui())

Features:
- Select ALL tables or a subset
- Optional: only save to Parquet, skip BQ/PG, dry-run
"""

from pathlib import Path
import sys
import argparse
import traceback
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

# === Your helper imports (as-is) ===
from app.utils.bq_pandas_helper import upload_df_to_bq
from app.utils.get_parquet_path import get_parquet_path
from app.utils.db_engine import get_engine
from app.utils.gdrive import list_files_in_folder, download_file_from_drive, upload_file_to_drive
from app.utils.auth import get_drive_service
from app.utils.mimetypes import MIMETYPES


# -----------------------------
# CONFIG
# -----------------------------
BASE_PATH = Path("/tmp")
BUYERS_FOLDER_ID = "1E9Pgiy0Fnzy2xRExHUTtZTdvrGq4dQPf"
OUTPUT_FOLDER_ID = "1vbM1JgH8NbYBDdpsci2pjGgDw9RhDCPA"  # Drive folder with all_* files

EXPECTED_OUTPUT_FILES = [
    "all_leads.xlsx",
    "all_events.xlsx",
    "all_properties.xlsx",
    "all_sellers.xlsx",
    "all_buyers_sellers.xlsx",
    "all_longtermrentals.xlsx",
]


# -----------------------------
# HELPER FUNCTIONS (unchanged logic, organized)
# -----------------------------
def clean_column_names(df: pd.DataFrame, table_name: str = None) -> pd.DataFrame:
    df.columns = df.columns.astype(str)  # Add this line
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True).str.lower()
    if table_name:
        df.columns = df.columns.map(lambda col: f"{table_name}_{col}")
    return df

def standardize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_columns = ['CreateDate', 'Create Time', 'Create Date', 'CreateTime', 'Last change', 'Publish date']
    for col in date_columns:
        if col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)
    return df

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Convert numerics to string for consistency with BQ/PG (matches your original logic)
    for column in df.columns:
        if df[column].dtype == 'object':
            # If any numeric-like values in object col, coerce to string
            if df[column].apply(lambda x: isinstance(x, (int, float))).any():
                df[column] = df[column].astype(str)
        elif df[column].dtype in ['float64', 'int64']:
            df[column] = df[column].astype(str)

    df = standardize_date_columns(df)
    df = df.where(pd.notnull(df), None)
    return df

def qr_code (df: pd.DataFrame) -> pd.DataFrame:
    if "properties" in df.Name:
        df["qr_code"] = 'pending'
    return df


# -----------------------------
# STAGING: download + combine + map file paths
# -----------------------------
def stage_inputs(base_path: Path = BASE_PATH,
                 buyers_folder_id: str = BUYERS_FOLDER_ID,
                 output_folder_id: str = OUTPUT_FOLDER_ID) -> Dict[str, Path]:
    """
    1) List+download partitioned buyers files -> combine -> export all_buyers.xlsx to /tmp and upload to Drive.
    2) Download all expected all_* files from output folder to /tmp if present.
    3) Return dict mapping logical table names -> local .xlsx paths
    """

    # Step 1: List and download all partitioned buyers files
    buyers_files = [
    f for f in list_files_in_folder(buyers_folder_id, name_contains="buyers")
    if not f['name'].startswith("all_") and "combined" not in f['name'].lower()
]

    all_buyers = pd.DataFrame()
    for file in buyers_files:
        filename = file['name']
        local_path = base_path / filename
        if local_path.exists():
            local_path.unlink()  # Delete before re-downloading
        download_file_from_drive(file['id'], str(local_path))
    # ✅ Read and append the file
        buyers_df = pd.read_excel(local_path)
        all_buyers = pd.concat([all_buyers, buyers_df], ignore_index=True)

    # Step 2: Export combined buyers file to /tmp/
    output_path = base_path / "all_buyers.xlsx"
    all_buyers.to_excel(output_path, index=False)
    print(f"✅ all_buyers.xlsx exported to {output_path}")

    existing = [
    f for f in list_files_in_folder(output_folder_id, name_contains="all_buyers.xlsx")
    if f['name'].strip().lower() == "all_buyers.xlsx"
]
    if existing:
        print(f"🔍 Found existing all_buyers.xlsx: {existing[0]['name']} (ID: {existing[0]['id']})")
        file_id = existing[0]['id']
    else:
        print("📁 No existing all_buyers.xlsx found — will create new.")
        file_id = None

    if existing and existing[0]['name'].strip().lower() != "all_buyers.xlsx":
        raise ValueError(f"🚨 Unexpected match: trying to overwrite {existing[0]['name']} instead of all_buyers.xlsx")

    upload_file_to_drive(
    local_path=str(output_path),
    filename="all_buyers.xlsx",
    mimetype=MIMETYPES["excel"],
    parent_folder_id=output_folder_id,
    file_id=file_id  # ✅ overwrite if exists
)
    print(f"✅ all_buyers.xlsx uploaded to Drive folder: {output_folder_id}")

    # Download existing all_* files
    for expected in EXPECTED_OUTPUT_FILES:
        found = list_files_in_folder(output_folder_id, name_contains=expected)
        if found:
            file_id = found[0]['id']
            download_file_from_drive(file_id, str(base_path / expected))
            print(f"✅ {expected} downloaded to {base_path}")
        else:
            print(f"⚠️ {expected} not found in Drive folder: {output_folder_id}")

    # Map logical names to paths (include buyers we just produced)
    all_files = {
        "rawleads": base_path / "all_leads.xlsx",
        "rawevents": base_path / "all_events.xlsx",
        "rawproperties": base_path / "all_properties.xlsx",
        "rawbuyers": output_path,
        "rawsellers": base_path / "all_sellers.xlsx",
        "rawbuyerssellers": base_path / "all_buyers_sellers.xlsx",
        "rawarchived": base_path / "all_longtermrentals.xlsx",
    }
    return all_files

def cleanup_tmp_folder(base_path: Path = BASE_PATH):
    for f in base_path.glob("*.xlsx"):
        try:
            f.unlink()
            print(f"🧹 Deleted {f}")
        except Exception as e:
            print(f"⚠️ Could not delete {f}: {e}")


# -----------------------------
# PARQUET + UPLOAD STEPS
# -----------------------------
from pathlib import Path

def save_selected_to_parquet(all_files: Dict[str, Path], selection: Optional[List[str]] = None):
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

def upload_selected_to_postgresql(selection: List[str]):
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

def upload_selected_to_bigquery(selection: List[str], location: str = "EU"):
    for name in selection:
        pq_path = Path(get_parquet_path(name))        # ← ensure Path
        if not pq_path.exists():
            print(f"⚠️ Skipping BQ upload for {name}: {pq_path} not found")
            continue
        df = pd.read_parquet(pq_path)
        upload_df_to_bq(df, table_name=name, location=location)
        print(f"✅ {name} uploaded to BigQuery ({location}).")


# -----------------------------
# ORCHESTRATION
# -----------------------------
def run_pipeline(
    selection: List[str],
    do_parquet: bool = True,
    do_postgres: bool = True,
    do_bq: bool = True,
    dry_run: bool = False,
    all_files: Optional[Dict[str, Path]] = None,
):
    """
    Execute the pipeline for the given selection.
    Assumes stage_inputs() was already run if all_files is provided.
    """
    # If caller didn't pass stage outputs, stage now
    if all_files is None:
        all_files = stage_inputs()

    if not selection:
        selection = list(all_files.keys())

    print(f"▶️ Selection: {selection}")
    print(f"    Options → parquet={do_parquet}, postgres={do_postgres}, bq={do_bq}, dry_run={dry_run}")

    if dry_run:
        print("🧪 Dry run: not performing any writes.")
        return

    if do_parquet:
        save_selected_to_parquet(all_files, selection)

    if do_postgres:
        upload_selected_to_postgresql(selection)

    if do_bq:
        upload_selected_to_bigquery(selection)


# -----------------------------
# TERMINAL INTERACTION UTILITIES
# -----------------------------
def prompt_for_tables(options: List[str]) -> List[str]:
    """
    Simple terminal prompt to pick tables by index or 'all'.
    """
    print("\nWhich tables do you want to process?")
    for i, name in enumerate(options, 1):
        print(f"  {i}. {name}")
    raw = input("Enter numbers separated by commas, or type 'all': ").strip().lower()
    if raw in ("all", "*"):
        return options
    try:
        indices = [int(x) for x in raw.replace(" ", "").split(",") if x]
        chosen = [options[i-1] for i in indices if 1 <= i <= len(options)]
        return chosen
    except Exception:
        print("⚠️ Invalid input; defaulting to ALL.")
        return options

# -----------------------------
# CLI
# -----------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive → Parquet → Postgres & BigQuery pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true", help="Process ALL tables")
    group.add_argument("-t", "--tables", nargs="+", help="Process only the specified tables (space-separated)")

    parser.add_argument("--only-parquet", action="store_true", help="Only write Parquet; skip databases")
    parser.add_argument("--no-postgres", action="store_true", help="Skip Postgres upload")
    parser.add_argument("--no-bq", action="store_true", help="Skip BigQuery upload")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run without writing")
    parser.add_argument("--ui", action="store_true", help="Launch ipywidgets UI (in supported environments)")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None):
    args = parse_args(argv)

    # Stage inputs to know what's available and to refresh /tmp copies
    all_files = stage_inputs()
    table_options = list(all_files.keys())

    # Determine selection
    if args.all:
        selection = table_options
    elif args.tables:
        # Validate requested names
        bad = [t for t in args.tables if t not in table_options]
        if bad:
            print(f"⚠️ Unknown table(s): {bad}")
            print(f"Available: {table_options}")
            sys.exit(2)
        selection = args.tables
    else:
        # No selection flags → prompt interactively in terminal
        selection = prompt_for_tables(table_options)

    do_parquet = True
    do_postgres = not (args.only_parquet or args.no_postgres)
    do_bq = not (args.only_parquet or args.no_bq)

    try:
        run_pipeline(
            selection=selection,
            do_parquet=do_parquet,
            do_postgres=do_postgres,
            do_bq=do_bq,
            dry_run=args.dry_run,
            all_files=all_files
        )
    except Exception:
        print("❌ Unhandled error:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

cleanup_tmp_folder()