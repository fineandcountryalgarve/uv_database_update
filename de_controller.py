from de_extract import stage_inputs, get_base_path
from de_load import save_selected_to_parquet, get_parquet_path, upload_selected_to_postgresql, upload_selected_to_bigquery, clean_tmp_files
import pandas as pd

def db_extract():
    base_path = get_base_path()
    stage_inputs(base_path)
    return "extracted"

def db_transform():
    save_selected_to_parquet()
    return "transformed"

def db_load():
    upload_selected_to_postgresql()
    upload_selected_to_bigquery()
    return "loaded"

if __name__ == "__main__":
    print(db_extract())
    print(db_transform())
    print(db_load())
    clean_tmp_files()
