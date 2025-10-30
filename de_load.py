import pandas as pd
from app.utils.db_engine import get_engine
from app.utils.get_base_path import get_base_path
from de_extract import stage_inputs

base_path = get_base_path()

engine = get_engine ()

def save_selected_to_sql(selection=None):
    all_files = stage_inputs()  # 👈 Reuse extract output
    selection = selection or list(all_files.keys())

    for name in selection:
        xlsx_path = all_files[name]
        if not xlsx_path.exists():
            print(f"⚠️ Skipping {name}: file not found at {xlsx_path}")
            continue

        df = pd.read_excel(xlsx_path)
        
        df.to_sql(
            name,
            con=engine,
            schema="bronze",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000
        )
        print(f"✅ {name}: {len(df):,} rows loaded into bronze.{name}")