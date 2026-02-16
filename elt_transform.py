"""
ELT Transform: Transforms data from raw schema to bronze schema.
Handles merge (upsert) for properties, append for other tables.
"""
from sqlalchemy import text
from app.utils.db_engine import get_engine, kill_stale_sessions
from app.utils.refresh_view import refresh_mv
from database.elt_config import TABLE_CONFIG


def _get_raw_columns(conn, table_name: str) -> tuple[list[str], list[str]]:
    """
    Query raw table columns from information_schema and split into
    data columns and dlt internal columns.

    Returns:
        (data_columns, dlt_columns) — both sorted alphabetically within their group.
    """
    result = conn.execute(
        text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'raw' AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {"table_name": table_name},
    )
    all_cols = [row[0] for row in result]
    data_cols = [c for c in all_cols if not c.startswith("_dlt_") and not c.startswith("unnamed")]
    dlt_cols = [c for c in all_cols if c.startswith("_dlt_")]
    return data_cols, dlt_cols


def transform_to_bronze() -> dict:
    """
    Transform data from raw schema to bronze schema.

    - Properties/Leads: MERGE (delete existing keys, insert new)
    - Others: APPEND (insert all, silver handles deduplication)

    Selects explicit columns from raw, with _dlt_ columns at the end.

    Returns:
        dict: Results with row counts per table
    """
    engine = get_engine()
    results = {}

    print("=" * 50)
    print("Transforming raw -> bronze...")
    print("=" * 50)

    for table_name, config in TABLE_CONFIG.items():
        strategy = config["write_disposition"]
        primary_key = config["primary_key"]

        try:
            with engine.begin() as conn:
                # Check if raw table has data
                count_result = conn.execute(
                    text(f"SELECT COUNT(*) FROM raw.{table_name}")
                )
                raw_count = count_result.scalar()

                if raw_count == 0:
                    print(f"  {table_name}: no new data in raw")
                    results[table_name] = {"status": "skipped", "rows": 0}
                    continue

                kill_stale_sessions(engine)

                # Get columns: data first, _dlt_ at the end
                data_cols, dlt_cols = _get_raw_columns(conn, table_name)
                select_cols = ", ".join(data_cols + dlt_cols)

                if strategy == "merge" and primary_key:
                    # MERGE: Delete existing keys, then insert all from raw
                    conn.execute(text(f"""
                        DELETE FROM bronze.{table_name}
                        WHERE {primary_key} IN (
                            SELECT {primary_key} FROM raw.{table_name}
                        )
                    """))

                    conn.execute(text(f"""
                        INSERT INTO bronze.{table_name} ({select_cols})
                        SELECT {select_cols} FROM raw.{table_name}
                    """))

                    print(f"  {table_name}: merged {raw_count} rows (by {primary_key})")

                else:
                    # APPEND: Insert all rows from raw
                    conn.execute(text(f"""
                        INSERT INTO bronze.{table_name} ({select_cols})
                        SELECT {select_cols} FROM raw.{table_name}
                    """))

                    print(f"  {table_name}: appended {raw_count} rows")

                results[table_name] = {"status": "success", "rows": raw_count}

        except Exception as e:
            print(f"  {table_name}: error - {e}")
            results[table_name] = {"status": "error", "error": str(e)}

    print("=" * 50)
    return results


def refresh_gold_views() -> bool:
    """
    Refresh all gold materialized views after bronze updates.

    Returns:
        bool: True if all views refreshed successfully
    """
    engine = get_engine()

    gold_views = [
        "gold.customers_mv",
        # Add other gold views as needed:
        # "gold.properties_mv",
        # "gold.events_mv",
    ]

    print("Refreshing gold materialized views...")
    success = True

    try:
        with engine.begin() as conn:
            for view in gold_views:
                try:
                    refresh_mv(conn=conn, view_name=view)
                    print(f"  Refreshed {view}")
                except Exception as e:
                    print(f"  Could not refresh {view}: {e}")
                    success = False
        return success
    except Exception as e:
        print(f"Error refreshing gold views: {e}")
        return False
