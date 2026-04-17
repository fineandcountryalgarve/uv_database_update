import io
import gc
from pathlib import Path
from typing import Dict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from app.utils.get_base_path import get_base_path
from app.utils.drive_folders import get_folder_id

_READER_KEY_DOCKER = Path("/keys/fc-pipeline-reader.json")
_READER_KEY_LOCAL = Path(__file__).parent.parent / "keys" / "fc-pipeline-reader.json"


def _get_drive_service():
    key_path = _READER_KEY_DOCKER if _READER_KEY_DOCKER.exists() else _READER_KEY_LOCAL
    creds = service_account.Credentials.from_service_account_file(
        str(key_path),
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds)


def _list_files(service, folder_id):
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        fields="files(id, name)",
        pageSize=100,
    ).execute()
    return results.get("files", [])


def _download_file(service, file_id, destination_path):
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(destination_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


base_path = get_base_path()
output_folder = get_folder_id("all_tables")


def stage_inputs(base_path: Path = base_path,
                 folder_id: str = output_folder) -> Dict[str, str]:
    """
    Download all .xlsx files from the specified Drive folder to local /tmp.
    Returns dict mapping table names (with 'raw' prefix) to local file paths.

    Bronze layer: no transformation, just download and stage for ingestion.
    """

    service = _get_drive_service()

    # List all files in the folder
    all_drive_files = _list_files(service, folder_id)
    excel_files = [f for f in all_drive_files if f["name"].endswith(".xlsx")]

    # Clear the list reference to free memory
    del all_drive_files
    gc.collect()

    print(f"📋 Found {len(excel_files)} Excel files in Drive folder\n")

    file_mapping = {}
    total_files = len(excel_files)

    for i, file in enumerate(excel_files, 1):
        filename = file["name"]
        file_id = file["id"]
        local_path = base_path / filename

        print(f"📥 [{i}/{total_files}] Downloading {filename}...")

        try:
            _download_file(service, file_id, str(local_path))

            # Build table name: strip .xlsx and all_ prefix, then add raw prefix
            base_name = filename.replace(".xlsx", "").replace("all_", "")
            table_name = f"raw{base_name}"
            file_mapping[table_name] = str(local_path)

            file_size_kb = local_path.stat().st_size / 1024
            print(f"✅ {filename} → bronze.{table_name} ({file_size_kb:.1f} KB)\n")

        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")
            raise

        finally:
            # Force garbage collection after each file to prevent memory buildup
            gc.collect()

    print(f"✅ Staged {len(file_mapping)} files locally.")
    return file_mapping
