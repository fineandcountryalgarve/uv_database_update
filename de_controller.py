from os import path
from database.de_extract import stage_inputs
from database.de_load import save_selected_to_sql, upload_selected_to_bigquery, cleanup_tmp_folder, backup_database_to_drive
from app.utils.get_base_path import get_base_path

base_path = get_base_path

def de_extract():
    cleanup_tmp_folder()
    all_files = stage_inputs()
    print("✅ Extract stage completed.")
    return all_files


def de_load(all_files):
    save_selected_to_sql(all_files)
    upload_selected_to_bigquery(all_files)
    print("✅ Load stage completed.")
    cleanup_tmp_folder()

    # Create database backup after successful load
    print("🔄 Creating database backup...")
    backup_database_to_drive()
    print("✅ Backup stage completed.")


if __name__ == "__main__":
    all_files = de_extract()
    de_load(all_files)