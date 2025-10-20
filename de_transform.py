import pandas as pd

def clean_column_names(df: pd.DataFrame, table_name: str = None) -> pd.DataFrame:
    df.columns = df.columns.astype(str)  # Add this line
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True).str.lower()
    if table_name:
        df.columns = df.columns.map(lambda col: f"{table_name}_{col}")
    return df

def standardize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_columns = ['CreateDate', 'Create Time', 'Create Date', 'CreateTime', 'Last change', 'Publish date']
    for col in date_columns:
        if col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)
    return df

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Convert numerics to string for consistency with BQ/PG (matches your original logic)
    for column in df.columns:
        if df[column].dtype == 'object':
            # If any numeric-like values in object col, coerce to string
            if df[column].apply(lambda x: isinstance(x, (int, float))).any():
                df[column] = df[column].astype(str)
        elif df[column].dtype in ['float64', 'int64']:
            df[column] = df[column].astype(str)

    df = standardize_date_columns(df)
    df = df.where(pd.notnull(df), None)
    return df