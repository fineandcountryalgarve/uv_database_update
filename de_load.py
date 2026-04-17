import pandas as pd
import subprocess
import os
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from app.utils.db_engine import get_engine, kill_stale_sessions
from app.utils.get_base_path import get_base_path
from app.utils.bq_pandas_helper import upload_df_to_bq
from app.utils.drive_folders import get_folder_id
from app import config

_WRITER_KEY_DOCKER = Path("/keys/fc-pipeline-writer.json")
_WRITER_KEY_LOCAL = Path(__file__).parent.parent / "keys" / "fc-pipeline-writer.json"


def _get_writer_drive_service():
    key_path = _WRITER_KEY_DOCKER if _WRITER_KEY_DOCKER.exists() else _WRITER_KEY_LOCAL
    creds = service_account.Credentials.from_service_account_file(
        str(key_path),
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    return build("drive", "v3", credentials=creds)

base_path = get_base_path()

engine = get_engine()

def save_selected_to_sql(all_files, selection=None):
    selection = selection or list(all_files.keys())

    for name in selection:
        xlsx_path = Path(all_files[name])
        if not xlsx_path.exists():
            print(f"⚠️ Skipping {name}: file not found at {xlsx_path}")
            continue

        df = pd.read_excel(xlsx_path, engine="openpyxl")

        # Drop new unnamed columns that don't exist in DB (junk from Excel)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'bronze' AND table_name = '{name}'
                """))
                db_cols = set(row[0] for row in result)

            if db_cols:
                new_unnamed = [c for c in df.columns if str(c).startswith("Unnamed:") and str(c) not in db_cols]
                if new_unnamed:
                    df = df.drop(columns=new_unnamed)
                    print(f"🧹 {name}: dropped {len(new_unnamed)} new unnamed column(s) not in schema")

                new_named = [c for c in df.columns if not str(c).startswith("Unnamed:") and str(c) not in db_cols]
                if new_named:
                    print(f"⚠️ {name}: new named columns in Excel not in DB: {new_named}")
        except Exception:
            pass  # Table might not exist yet

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
            print(f"⚠️ {name}: structure mismatch or missing table: {e}")
            print(f"    Attempting recreation...")
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS bronze.{name};"))
                    df.to_sql(
                        name,
                        con=conn,
                        schema="bronze",
                        if_exists="replace",
                        index=False
                    )
                    print(f"✅ {name}: table recreated with {len(df):,} rows.")
            except Exception as e2:
                print(f"❌ Error loading {name}: {e2}")

def upload_selected_to_bigquery(all_files, selection=None, dataset="bronze", location="EU"):
    selection = selection or list(all_files.keys())
    
    for name in selection:
        xlsx_path = Path(all_files[name])
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


def backup_database_to_drive(folder_id=None):
    """
    Create a PostgreSQL dump and upload it to Google Drive.
    The dump is timestamped and uploaded to the same folder where source files are stored.
    """
    folder_id = folder_id or get_folder_id("all_tables")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_filename = f"db_backup_{timestamp}.sql"
    local_dump_path = Path(base_path) / dump_filename

    # Build pg_dump command
    pg_dump_cmd = [
        "pg_dump",
        "-h", config.PG_HOST,
        "-p", config.PG_PORT,
        "-U", config.PG_USER,
        "-d", config.PG_DB,
        "-f", str(local_dump_path),
        "--no-password"
    ]

    try:
        print(f"🔄 Creating database backup: {dump_filename}")

        # Set PGPASSWORD environment variable for authentication
        env = {"PGPASSWORD": config.PG_PASSWORD}

        result = subprocess.run(
            pg_dump_cmd,
            env={**os.environ, **env},
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode != 0:
            print(f"❌ pg_dump failed: {result.stderr}")
            return None

        # Check if dump file was created
        if not local_dump_path.exists():
            print(f"❌ Dump file not created at {local_dump_path}")
            return None

        dump_size_mb = local_dump_path.stat().st_size / (1024 * 1024)
        print(f"✅ Database dump created: {dump_size_mb:.2f} MB")

        # Upload to Google Drive
        print(f"📤 Uploading backup to Google Drive...")
        drive_service = _get_writer_drive_service()
        file_metadata = {"name": dump_filename, "parents": [folder_id]}
        media = MediaFileUpload(str(local_dump_path), mimetype="application/sql", resumable=True)
        uploaded = drive_service.files().create(
            body=file_metadata, media_body=media, fields="id, name"
        ).execute()

        print(f"✅ Backup uploaded to Google Drive: {dump_filename}")

        # Clean up local dump file
        local_dump_path.unlink()
        print(f"🧹 Local dump file deleted")

        return uploaded

    except subprocess.TimeoutExpired:
        print(f"❌ Database backup timed out after 5 minutes")
        return None
    except Exception as e:
        print(f"❌ Error creating database backup: {e}")
        # Clean up local file if it exists
        if local_dump_path.exists():
            local_dump_path.unlink()
        return None
