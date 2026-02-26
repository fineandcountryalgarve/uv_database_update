"""
ELT Transform: Transforms data from raw schema to bronze schema.
Handles merge (upsert) for properties, append for other tables.
"""
from sqlalchemy import text
from app.utils.db_engine import get_engine, kill_stale_sessions
from app.utils.refresh_view import refresh_mv
from database.elt_config import TABLE_CONFIG


def _get_column_expressions(conn, table_name: str) -> tuple[str, str]:
    """
    Query raw and bronze column types and build INSERT/SELECT expressions.
    Quotes all column names to handle reserved words (e.g. "user").
    Adds CAST for columns where raw type differs from bronze type
    (e.g. dlt stores as varchar when all values are null, but bronze expects timestamp).

    Returns:
        (insert_cols, select_exprs) — quoted column list and SELECT expressions with casts.
    """
    # Get raw columns and types
    raw_result = conn.execute(
        text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'raw' AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {"table_name": table_name},
    )
    raw_types = {row[0]: row[1] for row in raw_result}

    # Get bronze column types for comparison
    bronze_result = conn.execute(
        text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {"table_name": table_name},
    )
    bronze_types = {row[0]: row[1] for row in bronze_result}

    # Split into data and dlt columns, skip unnamed
    all_cols = list(raw_types.keys())
    data_cols = [c for c in all_cols if not c.startswith("_dlt_") and not c.startswith("unnamed")]
    dlt_cols = [c for c in all_cols if c.startswith("_dlt_")]
    ordered_cols = data_cols + dlt_cols

    # Only include columns that exist in both raw and bronze
    ordered_cols = [c for c in ordered_cols if c in bronze_types]

    insert_cols = ", ".join(f'"{c}"' for c in ordered_cols)

    select_parts = []
    for col in ordered_cols:
        raw_type = raw_types[col]
        bronze_type = bronze_types.get(col, raw_type)
        if raw_type != bronze_type:
            select_parts.append(f'CAST("{col}" AS {bronze_type}) AS "{col}"')
        else:
            select_parts.append(f'"{col}"')
    select_exprs = ", ".join(select_parts)

    return insert_cols, select_exprs


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

    kill_stale_sessions(engine)

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

                # Get columns: data first, _dlt_ at the end, with quoting and casts
                insert_cols, select_exprs = _get_column_expressions(conn, table_name)

                if strategy == "merge" and primary_key:
                    # MERGE: Delete existing keys, then insert all from raw
                    pk_cols = primary_key if isinstance(primary_key, list) else [primary_key]
                    join_condition = " AND ".join(
                        f'bronze.{table_name}."{col}" = raw.{table_name}."{col}"'
                        for col in pk_cols
                    )
                    conn.execute(text(f"""
                        DELETE FROM bronze.{table_name}
                        USING raw.{table_name}
                        WHERE {join_condition}
                    """))

                    conn.execute(text(f"""
                        INSERT INTO bronze.{table_name} ({insert_cols})
                        SELECT {select_exprs} FROM raw.{table_name}
                    """))

                    print(f"  {table_name}: merged {raw_count} rows (by {primary_key})")

                else:
                    # APPEND: Insert all rows from raw
                    conn.execute(text(f"""
                        INSERT INTO bronze.{table_name} ({insert_cols})
                        SELECT {select_exprs} FROM raw.{table_name}
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
