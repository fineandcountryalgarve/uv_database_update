"""
ELT Controller: Orchestrates the extract, load, and transform pipeline.
"""
from database.elt_extract import stage_inputs
from database.de_load import cleanup_tmp_folder
from database.elt_load import load_to_raw
from database.elt_transform import transform_to_bronze, refresh_gold_views


def elt_extract() -> dict:
    """
    Extract: Download year-partitioned Excel files from Google Drive.
    Uses elt_extract.stage_inputs() with parse_drive_filename() filtering.

    Returns:
        dict: File mapping {table_name: file_path}
    """
    cleanup_tmp_folder()
    file_mapping = stage_inputs()
    return file_mapping


def elt_load(file_mapping: dict) -> dict:
    """
    Load: Use dlt to load Excel files to raw schema.

    Args:
        file_mapping: Dict from elt_extract()

    Returns:
        dict: Load results
    """
    result = load_to_raw(file_mapping)
    cleanup_tmp_folder()
    return result


def elt_transform() -> dict:
    """
    Transform: Move data from raw to bronze, refresh gold views.

    Returns:
        dict: Transform results
    """
    results = transform_to_bronze()
    refresh_gold_views()
    return results
