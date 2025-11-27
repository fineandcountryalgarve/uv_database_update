from app.utils.get_base_path import get_base_path
from app.utils.drive_folders import get_folder_id
from pathlib import Path
from typing import Dict
from app.utils.gdrive import list_files_in_folder, download_file_from_drive

base_path = get_base_path()
output_folder = get_folder_id("all_tables")


def stage_inputs(base_path: Path = base_path,
                 folder_id: str = output_folder) -> Dict[str, Path]:
    """
    Download all .xlsx files from the specified Drive folder to local /tmp.
    Returns dict mapping table names (with 'raw' prefix) to local file paths.
    
    Bronze layer: no transformation, just download and stage for ingestion.
    """
    
    # List all files in the folder
    all_drive_files = list_files_in_folder(folder_id)
    excel_files = [f for f in all_drive_files if f["name"].endswith(".xlsx")]
    
    print(f"📋 Found {len(excel_files)} Excel files in Drive folder\n")
    
    file_mapping = {}
    
    for file in excel_files:
        filename = file["name"]
        local_path = base_path / filename
        
        print(f"📥 Downloading {filename}...")
        download_file_from_drive(file["id"], str(local_path))
        
        # Build table name: strip .xlsx and all_ prefix, then add raw prefix
        base_name = filename.replace(".xlsx", "").replace("all_", "")
        table_name = f"raw{base_name}"
        file_mapping[table_name] = local_path
        
        print(f"✅ {filename} → bronze.{table_name} ({local_path.stat().st_size/1024:.1f} KB)\n")
    
    print(f"✅ Staged {len(file_mapping)} files locally.")
    return file_mapping
