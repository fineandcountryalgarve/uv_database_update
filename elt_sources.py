"""
dlt source definition for Fine & Country CRM data.
Reads Excel files downloaded from Google Drive and yields incremental data.
Uses metadata.etl_run_log to seed the initial cursor for first runs.
"""
import re
import dlt
import pandas as pd
from typing import Iterator, Any
from sqlalchemy import text
from database.elt_config import TABLE_CONFIG
from app.utils.db_engine import get_engine

DEFAULT_INITIAL_VALUE = pd.Timestamp("1900-01-01")


def get_last_filter_end(table_name: str) -> pd.Timestamp:
    """
    Query metadata.etl_run_log for the last successful filter_end
    for a given table. This seeds dlt's initial_value so the first
    ELT run doesn't re-import rows already in bronze.

    Returns:
        pd.Timestamp, or DEFAULT_INITIAL_VALUE if no record exists.
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT filter_end
                    FROM metadata.etl_run_log
                    WHERE table_name = :table_name
                      AND status = 'success'
                      AND filter_end IS NOT NULL
                    ORDER BY run_completed_at DESC
                    LIMIT 1
                """),
                {"table_name": table_name},
            )
            row = result.fetchone()
            if row and row[0]:
                value = pd.Timestamp(row[0])
                print(f"  {table_name}: seeded from metadata (filter_end={value})")
                return value
    except Exception as e:
        print(f"  {table_name}: could not read metadata ({e}), using default")

    print(f"  {table_name}: no metadata found, using {DEFAULT_INITIAL_VALUE}")
    return DEFAULT_INITIAL_VALUE


@dlt.source(name="finecountry_crm")
def crm_source(file_mapping: dict[str, str]):
    """
    dlt source that yields resources for each CRM table.

    Args:
        file_mapping: Dict mapping table names to local Excel file paths
                     e.g., {"rawproperties": "/tmp/all_properties.xlsx"}
                     Note: Keys may have "raw" prefix from stage_inputs()
    """
    for raw_table_name, file_path in file_mapping.items():
        # Strip "raw" prefix if present (from stage_inputs)
        table_name = raw_table_name.removeprefix("raw")

        config = TABLE_CONFIG.get(table_name)
        if not config:
            print(f"  Skipping {table_name}: no config found")
            continue

        yield _create_resource(table_name, file_path, config)


def _create_resource(table_name: str, file_path: str, config: dict):
    """
    Create a dlt resource for a specific table.
    Uses incremental loading based on timestamp column.
    Seeds initial_value from metadata.etl_run_log if available.
    """
    incremental_col = config["incremental_column"]
    primary_key = config["primary_key"]
    write_disposition = config["write_disposition"]
    date_columns = set(config.get("date_columns", []))

    # Seed from metadata for first dlt run, fall back to 1900-01-01
    initial_value = get_last_filter_end(table_name)

    @dlt.resource(
        name=table_name,
        write_disposition=write_disposition,
        primary_key=primary_key,
    )
    def table_data(
        last_value=dlt.sources.incremental(
            incremental_col,
            initial_value=initial_value,
        )
    ) -> Iterator[dict[str, Any]]:
        """
        Read Excel file and yield rows newer than last processed value.
        """
        print(f"  Reading {file_path}...")
        df = pd.read_excel(file_path, engine="openpyxl")

        # Normalize column names: lowercase, spaces to underscores, strip special chars
        df.columns = [
            re.sub(r'[^a-z0-9_]', '', str(c).strip().lower().replace(" ", "_"))
            for c in df.columns
        ]

        total_rows = len(df)

        # Parse date columns as datetime, cast all others to text
        for col in df.columns:
            if col in date_columns:
                df[col] = pd.to_datetime(
                    df[col], errors="coerce", dayfirst=True,
                )
            else:
                df[col] = df[col].astype(str).replace({"nan": None, "None": None, "": None})

        # Filter to rows newer than last processed
        if incremental_col in df.columns:
            if last_value.last_value:
                cutoff = pd.to_datetime(last_value.last_value)
                df = df[df[incremental_col] > cutoff]
                print(f"  {table_name}: {len(df)}/{total_rows} rows after {cutoff}")
            else:
                print(f"  {table_name}: {total_rows} rows (first run)")
        else:
            print(f"  {table_name}: {total_rows} rows (no incremental column)")

        # Yield rows as dicts
        for _, row in df.iterrows():
            yield row.to_dict()

    return table_data
