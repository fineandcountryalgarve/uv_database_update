from de_extract import stage_inputs
from de_load import save_selected_to_sql

def upload_bronze():
    all_files = stage_inputs()
    save_selected_to_sql(all_files)
    print("✅ Bronze layer successfully updated.")
    return all_files

if __name__ == "__main__":
    upload_bronze()