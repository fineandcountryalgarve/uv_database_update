import pandas as pd
from app.utils.db_engine import get_engine, kill_stale_sessions
from app.utils.get_base_path import get_base_path
from pathlib import Path

base_path = get_base_path()

engine = get_engine ()

def save_selected_to_sql(all_files, selection=None):
    selection = selection or list(all_files.keys())

    for name in selection:
        xlsx_path = all_files[name]
        if not xlsx_path.exists():
            print(f"⚠️ Skipping {name}: file not found at {xlsx_path}")
            continue

        df = pd.read_excel ( 
        xlsx_path,
        engine="openpyxl",
    )
        
        with engine.begin() as conn:
            kill_stale_sessions(engine)
            df.to_sql(
                name,
                con=conn,
                schema="bronze",
                if_exists="replace",
                index=False
            )
            print(f"✅ {name}: {len(df):,} rows loaded into bronze.{name}")

def cleanup_tmp_folder():
    base = Path(base_path)
    for f in base.glob("*.xlsx"):
        try:
            f.unlink()
            print(f"🧹 Deleted {f}")
        except Exception as e:
            print(f"⚠️ Could not delete {f}: {e}")
