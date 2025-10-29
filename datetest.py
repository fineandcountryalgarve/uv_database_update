import os
import pandas as pd
from IPython import display

os.chdir('/mnt/c/users/rfont/Documents/Python/database_update')

customers = ["all_buyers.xlsx", 
             "all_buyers_sellers.xlsx", 
             "all_longtermrentals.xlsx",
             "all_sellers.xlsx"]

property_event = ["all_properties.xlsx",
                  "all_events.xlsx",
                  "all_leads.xlsx"]

def standardize_date_columns(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    date_candidates = [col for col in df.columns if any(key in col.lower() for key in ["date", "time", "change", "publish"])]
    for col in date_candidates:
        if filename in customers:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
        elif filename in property_event:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y %H:%M", errors="coerce")
        else:
            print(f"⚠️ Unknown file: {filename}. Defaulting to date-only format.")
    return df

for f in os.listdir():
    if f.endswith('.xlsx'):
        df = pd.read_excel(f)
        df = standardize_date_columns(df)

date_cols = df.select_dtypes(include=["datetime64[ns]"])
display(date_cols)