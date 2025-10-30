import pandas as pd
from app.utils.get_base_path import get_base_path
from app.utils.drive_folders import get_folder_id
from pathlib import Path
from typing import Dict
from app.utils.gdrive import list_files_in_folder, download_file_from_drive, upload_file_to_drive, MIMETYPES

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