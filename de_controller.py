from os import path
from de_extract import stage_inputs
from de_load import save_selected_to_sql, upload_selected_to_bigquery, cleanup_tmp_folder
from app.utils.get_base_path import get_base_path

base_path = get_base_path

def upload_bronze():
    cleanup_tmp_folder()
    all_files = stage_inputs()
    save_selected_to_sql(all_files)
    upload_selected_to_bigquery (all_files)
    print("✅ Bronze layer successfully updated.")
    cleanup_tmp_folder()
    return all_files

if __name__ == "__main__":
    upload_bronze()