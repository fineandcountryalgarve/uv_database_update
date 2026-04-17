"""
ELT Load: Loads incremental data from Excel files to BigQuery bronze schema using dlt.
"""
import json
import dlt
from database.elt_sources import crm_source

_CREDENTIALS_PATH = "/keys/fc-airbyte-sa.json"


def load_to_bronze(file_mapping: dict[str, str]) -> dict:
    """
    Load Excel data to BigQuery bronze schema using dlt.

    Args:
        file_mapping: Dict mapping table names to Excel file paths
                     (from stage_inputs())

    Returns:
        dict: Pipeline execution metrics
    """
    print("=" * 50)
    print("Loading data to bronze schema (BigQuery)...")
    print("=" * 50)

    with open(_CREDENTIALS_PATH) as f:
        credentials = json.load(f)

    pipeline = dlt.pipeline(
        pipeline_name="finecountry_crm",
        destination=dlt.destinations.bigquery(credentials=credentials, location="EU"),
        dataset_name="bronze",
    )

    source = crm_source(file_mapping)
    load_info = pipeline.run(source)

    print("=" * 50)
    print(f"Load completed: {load_info}")
    print(f"Loaded packages: {len(load_info.load_packages)}")
    print("=" * 50)

    return {
        "status": "success",
        "load_info": str(load_info),
        "packages": len(load_info.load_packages),
    }
