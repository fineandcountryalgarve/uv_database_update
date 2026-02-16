"""
ELT Load: Loads incremental data from Excel files to raw schema using dlt.
"""
import dlt
from database.elt_sources import crm_source
from app import config


def load_to_raw(file_mapping: dict[str, str]) -> dict:
    """
    Load Excel data to raw schema using dlt.

    Args:
        file_mapping: Dict mapping table names to Excel file paths
                     (from stage_inputs())

    Returns:
        dict: Pipeline execution metrics
    """
    print("=" * 50)
    print("Loading data to raw schema...")
    print("=" * 50)

    # Create pipeline with postgres destination
    pipeline = dlt.pipeline(
        pipeline_name="finecountry_raw",
        destination=dlt.destinations.postgres(credentials=config.POSTGRES_URL),
        dataset_name="raw",  # Target schema (staging layer)
    )

    # Run the pipeline
    source = crm_source(file_mapping)
    load_info = pipeline.run(source)

    # Log results
    print("=" * 50)
    print(f"Load completed: {load_info}")
    print(f"Loaded packages: {len(load_info.load_packages)}")
    print("=" * 50)

    return {
        "status": "success",
        "load_info": str(load_info),
        "packages": len(load_info.load_packages),
    }
