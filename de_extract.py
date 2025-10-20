from pathlib import Path
from typing import Dict
import pandas as pd
from app.utils.gdrive import list_files_in_folder, download_file_from_drive, upload_file_to_drive
from app.utils.mimetypes import MIMETYPES
import os

def get_base_path() -> Path:
    """
    Returns a safe base temporary directory for file operations.
    Works on both local WSL and inside Airflow's Docker container.
    """
    # Inside Docker (Airflow)
    if os.getenv("AIRFLOW_HOME") or Path("/usr/local/airflow").exists():
        base_path = Path("/usr/local/airflow/tmp")
    else:
        base_path = Path("/tmp")


    base_path.mkdir(parents=True, exist_ok=True)
    return base_path

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

def stage_inputs(base_path: Path,
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