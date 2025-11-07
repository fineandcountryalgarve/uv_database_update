import pandas as pd
from app.utils.get_base_path import get_base_path
from app.utils.drive_folders import get_folder_id
from pathlib import Path
from typing import Dict
from app.utils.gdrive import list_files_in_folder, download_file_from_drive
base_path = get_base_path()

buyers_folder = get_folder_id("buyers")
output_folder = get_folder_id("all_tables")

EXPECTED_OUTPUT_FILES = [
    "all_leads.xlsx",
    "all_events.xlsx",
    "all_properties.xlsx",
    "all_sellers.xlsx",
    "all_buyers_sellers.xlsx",
    "all_longtermrentals.xlsx",
]

def stage_inputs(base_path: Path = base_path,
                 buyers_folder_id: str = buyers_folder,
                 output_folder_id: str = output_folder) -> Dict[str, Path]:
    """
    1) List+download partitioned buyers files -> combine -> export all_buyers.xlsx to /tmp and upload to Drive.
    2) Download all expected all_* files from output folder to /tmp if present.
    3) Return dict mapping logical table names -> local .xlsx paths
    """

    # Step 1: List and download all partitioned buyers files
    buyers_files = [
        f for f in list_files_in_folder(buyers_folder_id, name_contains="buyers")
        if not f["name"].startswith("all_") and "combined" not in f["name"].lower()
    ]

    for file in buyers_files:
        filename = file["name"].replace(" ", "_")
        local_path = base_path / filename
        print(f"📥 Downloading {filename} ...")
        download_file_from_drive(file["id"], str(local_path))
        print(f"✅ Downloaded {filename} ({local_path.stat().st_size/1024:.1f} KB)")

    # Step 2: Download existing all_* files (other tables)
    for expected in EXPECTED_OUTPUT_FILES:
        found = list_files_in_folder(output_folder_id, name_contains=expected)
        if found:
            file_id = found[0]["id"]
            download_file_from_drive(file_id, str(base_path / expected))
            print(f"✅ {expected} downloaded to {base_path}")
        else:
            print(f"⚠️ {expected} not found in Drive folder: {output_folder_id}")

    # Build mapping dict (buyers tables added dynamically)
    all_files = {
        "rawleads": base_path / "all_leads.xlsx",
        "rawevents": base_path / "all_events.xlsx",
        "rawproperties": base_path / "all_properties.xlsx",
        "rawsellers": base_path / "all_sellers.xlsx",
        "rawbuyerssellers": base_path / "all_buyers_sellers.xlsx",
        "rawarchived": base_path / "all_longtermrentals.xlsx",
        "rawbuyers_2425": base_path / "buyers_2024_2025.xlsx",
        "rawbuyers_2123": base_path / "buyers_2021_2023.xlsx",
        "rawbuyers_1720": base_path / "buyers_2017_2020.xlsx"
    }

    print("✅ All files staged locally.")
    return all_files
