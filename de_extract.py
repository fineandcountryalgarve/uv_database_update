from pathlib import Path
from typing import Dict, Optional
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
    1) Combine partitioned buyers files -> export all_buyers.xlsx to /tmp and upload to Drive.
    2) Download all expected all_* files from output folder to /tmp if present.
    3) Return dict mapping logical table names -> local .xlsx paths
    """

    def exact_match(files, expected_name):
        return [
            f for f in files
            if f['name'].strip().lower() == expected_name.strip().lower()
        ]

    def download_exact_file(expected_filename: str, folder_id: str) -> Optional[Path]:
        files = list_files_in_folder(folder_id)
        matches = exact_match(files, expected_filename)

        if len(matches) > 1:
            raise ValueError(f"🚨 Multiple matches for {expected_filename}: {[f['name'] for f in matches]}")
        elif not matches:
            print(f"⚠️ {expected_filename} not found in Drive folder: {folder_id}")
            return None

        file_id = matches[0]['id']
        target_path = base_path / expected_filename
        if target_path.exists():
            target_path.unlink()
        download_file_from_drive(file_id, str(target_path))
        print(f"✅ {expected_filename} downloaded to {target_path}")
        return target_path

    # Step 1: Combine partitioned buyers files
    buyers_files = [
        f for f in list_files_in_folder(buyers_folder_id, name_contains="buyers")
        if not f['name'].startswith("all_") and "combined" not in f['name'].lower()
    ]

    all_buyers = pd.DataFrame()
    for file in buyers_files:
        filename = file['name']
        local_path = base_path / filename
        if local_path.exists():
            local_path.unlink()
        download_file_from_drive(file['id'], str(local_path))
        buyers_df = pd.read_excel(local_path)
        all_buyers = pd.concat([all_buyers, buyers_df], ignore_index=True)

    # Step 2: Export combined buyers file
    buyers_output = base_path / "all_buyers.xlsx"
    all_buyers.to_excel(buyers_output, index=False)
    print(f"✅ all_buyers.xlsx exported to {buyers_output}")

    # Step 3: Upload to Drive (overwrite if exists)
    existing = exact_match(list_files_in_folder(output_folder_id), "all_buyers.xlsx")
    file_id = existing[0]['id'] if existing else None

    if existing and existing[0]['name'].strip().lower() != "all_buyers.xlsx":
        raise ValueError(f"🚨 Unexpected match: trying to overwrite {existing[0]['name']} instead of all_buyers.xlsx")

    upload_file_to_drive(
        local_path=str(buyers_output),
        filename="all_buyers.xlsx",
        mimetype=MIMETYPES["excel"],
        parent_folder_id=output_folder_id,
        file_id=file_id
    )
    print(f"✅ all_buyers.xlsx uploaded to Drive folder: {output_folder_id}")

    # Step 4: Download all expected all_* files
    downloaded_paths = {}
    for expected in EXPECTED_OUTPUT_FILES:
        path = download_exact_file(expected, output_folder_id)
        if path:
            logical_name = expected.replace("all_", "").replace(".xlsx", "").lower()
            downloaded_paths[f"raw{logical_name}"] = path

    return downloaded_paths
