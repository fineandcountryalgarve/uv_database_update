"""
ELT Extract: Downloads year-partitioned Excel files from the CRM_EXTRACTION
Google Drive folder. Uses parse_drive_filename() to filter only current-year
contact files and all_properties.xlsx.
"""
from app.utils.get_base_path import get_base_path
from app.utils.drive_folders import get_folder_id
from pathlib import Path
from app.utils.gdrive import list_files_in_folder, download_file_from_drive
from database.elt_config import parse_drive_filename, get_current_year_suffix
import gc

base_path = get_base_path()
output_folder = get_folder_id("crm_extraction")


def stage_inputs(base_path: Path = base_path,
                 folder_id: str = output_folder) -> dict[str, str]:
    """
    Download current-year Excel files from the CRM_EXTRACTION Drive folder.

    File filtering via parse_drive_filename():
    - all_properties.xlsx -> always downloaded (-> crm_properties)
    - buyers_2026.xlsx -> downloaded (current year -> crm_buyers)
    - buyers_2025.xlsx -> skipped (old year)
    - all_buyers.xlsx -> skipped (old naming for partitioned tables)

    Returns:
        dict: File mapping {table_name: file_path} where table_name
              has 'raw' prefix for compatibility with elt_sources.py
    """
    current_yyyy = get_current_year_suffix()

    # List all files in the CRM extraction folder
    all_drive_files = list_files_in_folder(folder_id)
    excel_files = [f for f in all_drive_files if f["name"].endswith(".xlsx")]

    del all_drive_files
    gc.collect()

    print(f"Found {len(excel_files)} Excel files in CRM extraction folder")
    print(f"Filtering for current year suffix: _{current_yyyy}\n")

    # Filter files using parse_drive_filename
    files_to_download = []
    for file in excel_files:
        table_name = parse_drive_filename(file["name"])
        if table_name:
            files_to_download.append((file, table_name))
        else:
            print(f"  Skipping {file['name']}")

    if not files_to_download:
        print("No matching files found for current year. "
              "Contact tables will be skipped.")
        return {}

    print(f"\n{len(files_to_download)} files to download:\n")

    file_mapping = {}
    total_files = len(files_to_download)

    for i, (file, table_name) in enumerate(files_to_download, 1):
        filename = file["name"]
        file_id = file["id"]
        local_path = base_path / filename

        print(f"  [{i}/{total_files}] Downloading {filename}...")

        try:
            download_file_from_drive(file_id, str(local_path))

            # Add 'raw' prefix for compatibility with elt_sources.py
            raw_table_name = f"raw{table_name}"
            file_mapping[raw_table_name] = str(local_path)

            file_size_kb = local_path.stat().st_size / 1024
            print(f"  {filename} -> {table_name} ({file_size_kb:.1f} KB)\n")

        except Exception as e:
            print(f"  Failed to download {filename}: {e}")
            raise

        finally:
            gc.collect()

    print(f"Staged {len(file_mapping)} files locally.")
    return file_mapping
