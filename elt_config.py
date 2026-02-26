"""
dlt configuration for Fine & Country ELT pipeline.
Table configurations for incremental loading with year-partitioned extraction.
All CRM tables are prefixed with crm_ to distinguish from other data sources.
"""
import re
from datetime import datetime

# Table configurations for dlt pipeline
# Maps table names to their extraction/load settings
# Column names use normalized form (lowercase, spaces -> underscores)
# because elt_sources.py normalizes all Excel column names at ingestion.
# CRM originals: "Last change", "CreateTime", "EventDate", "Reference", "LastUpdate", "EventID"
TABLE_CONFIG = {
    "crm_properties": {
        "incremental_column": "last_change",
        "primary_key": ["reference", "last_change"],
        "write_disposition": "merge",
        "year_partitioned": False,
        "date_columns": ["create_date", "last_change", "publish_date"],
    },
    "crm_buyers": {
        "incremental_column": "createtime",
        "primary_key": ["entityid", "createtime"],
        "write_disposition": "append",
        "year_partitioned": True,
        "date_columns": ["createtime"],
    },
    "crm_sellers": {
        "incremental_column": "createtime",
        "primary_key": ["entityid", "createtime"],
        "write_disposition": "append",
        "year_partitioned": True,
        "date_columns": ["createtime"],
    },
    "crm_buyers_sellers": {
        "incremental_column": "createtime",
        "primary_key": ["entityid", "createtime"],
        "write_disposition": "append",
        "year_partitioned": True,
        "date_columns": ["createtime"],
    },
    "crm_leads": {
        "incremental_column": "lastupdate",
        "primary_key": ["eventid", "lastupdate"],
        "write_disposition": "merge",
        "year_partitioned": True,
        "date_columns": ["createdate", "startdate", "enddate", "lastupdate"],
    },
    "crm_events": {
        "incremental_column": "eventdate",
        "primary_key": ["eventid", "eventdate"],
        "write_disposition": "append",
        "year_partitioned": True,
        "date_columns": ["eventdate"],
    },
    "crm_archived": {
        "incremental_column": "createtime",
        "primary_key": ["entityid", "createtime"],
        "write_disposition": "append",
        "year_partitioned": True,
        "date_columns": ["createtime"],
    },
}


def get_current_year_suffix() -> str:
    """
    Return the current 4-digit year (e.g., '2026' for 2026).
    Used to match year-partitioned filenames like buyers_2026.xlsx.
    """
    return str(datetime.now().year)


def parse_drive_filename(filename: str) -> str | None:
    """
    Parse a Drive filename and return the target table name, or None if the
    file should be skipped.

    Rules:
    - 'all_properties.xlsx' -> 'crm_properties' (non-partitioned, always processed)
    - 'buyers_2026.xlsx' -> 'crm_buyers' (year-partitioned, current year only)
    - 'buyers_sellers_2026.xlsx' -> 'crm_buyers_sellers' (underscore in table name)
    - 'archived_2026.xlsx' -> 'crm_archived' (archived contacts)
    - 'buyers_2025.xlsx' -> None (old year, skip)
    - 'all_buyers.xlsx' -> None (old naming convention for partitioned table, skip)
    - 'notes.xlsx' -> None (not in TABLE_CONFIG, skip)

    Args:
        filename: The Excel filename from Google Drive (e.g., 'buyers_2026.xlsx')

    Returns:
        Table name string if file should be processed, None otherwise.
    """
    if not filename.endswith(".xlsx"):
        return None

    base = filename.removesuffix(".xlsx")
    current_yyyy = get_current_year_suffix()

    # Non-partitioned files: all_properties.xlsx -> crm_properties
    if base == "all_properties":
        return "crm_properties"

    # Year-partitioned files: try to match {table_base}_{yyyy}
    # The year suffix is always the last _XXXX (4 digits)
    match = re.match(r"^(.+)_(\d{4})$", base)
    if match:
        table_base = match.group(1)
        year_suffix = match.group(2)

        # Only process current year
        if year_suffix != current_yyyy:
            return None

        # Build crm_ prefixed table name and validate
        table_name = f"crm_{table_base}"
        config = TABLE_CONFIG.get(table_name)
        if config and config["year_partitioned"]:
            return table_name

    # Skip everything else (old naming like all_buyers.xlsx, unknown files)
    return None
