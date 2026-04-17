"""
dlt source definition for Fine & Country CRM data.
Reads Excel files downloaded from Google Drive and yields incremental data.
dlt manages its own cursor state in BigQuery (_dlt_pipeline_state table).
"""
import re
import dlt
import pandas as pd
from typing import Iterator, Any
from database.elt_config import TABLE_CONFIG

DEFAULT_INITIAL_VALUE = pd.Timestamp("1900-01-01")


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
    Initial value defaults to 1900-01-01 on first run; dlt state takes over from there.
    """
    incremental_col = config["incremental_column"]
    primary_key = config["primary_key"]
    write_disposition = config["write_disposition"]
    date_columns = set(config.get("date_columns", []))

    @dlt.resource(
        name=table_name,
        write_disposition=write_disposition,
        primary_key=primary_key,
    )
    def table_data(
        last_value=dlt.sources.incremental(
            incremental_col,
            initial_value=DEFAULT_INITIAL_VALUE,
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

        # Yield rows as dicts, converting NaT to None for proper NULL storage
        for _, row in df.iterrows():
            record = row.to_dict()
            yield {k: (None if pd.isna(v) else v) for k, v in record.items()}

    return table_data
