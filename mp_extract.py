"""
Extract module for Mailchimp ETL pipeline.
Refreshes materialized view and extracts raw customer data from PostgreSQL.
"""
from sqlalchemy import text
import pandas as pd
from app.utils.refresh_view import refresh_mv
from app.utils.date_helper import get_dynamic_date_range
from app.utils.db_engine import get_engine


def refresh_materialized_view(view_name: str) -> bool:
    """
    Refresh a materialized view and commit the transaction.
    
    Args:
        view_name: Full name of the materialized view (e.g., 'gold.customers_mv')
        
    Returns:
        bool: True if successful, False otherwise
    """
    engine = get_engine()
    
    try:
        with engine.begin() as conn:  # Auto-commits on success
            refresh_mv(conn=conn, view_name=view_name)
            print(f"✅ Materialized view {view_name} refreshed and committed.")
        return True
        
    except Exception as e:
        print(f"❌ Error refreshing {view_name}: {e}")
        return False


def extract_mailchimp_data() -> pd.DataFrame | None:
    """
    Extracts raw customer data from gold.customers_mv.
    Refreshes the view first, then queries for customers within date range.
    
    Returns:
        pd.DataFrame | None: Raw customer data or None if error/no data
    """
    engine = get_engine()
    
    try:
        # Get date range for filtering
        start_date, end_date = get_dynamic_date_range()
        if not start_date or not end_date:
            print("⚠️ Could not determine date range.")
            return None
            
        print(f"📅 Date range: {start_date} to {end_date}")
        
        # Refresh materialized view first
        gold_customers_view = "gold.customers_mv"
        print("🔄 Refreshing materialized view...")
        
        if not refresh_materialized_view(gold_customers_view):
            print("⚠️ Failed to refresh view, continuing with existing data...")
        
        # Extract raw data from the view
        query = text("""
SELECT
    "Email",
    "Full Name",
    "Speaks",
    "Client nature",
    "CreateTime"
FROM gold.customers_mv
WHERE "CreateTime" >= :start_date
  AND "CreateTime" <= :end_date
  AND "Email" IS NOT NULL
  AND "Email" <> ''
  AND "Email" <> '-'
""")
        
        with engine.connect() as conn:
            customers_df = pd.read_sql(
                query, 
                conn, 
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
        
        # Validate results
        if customers_df.empty:
            print("⚠️ No rows found for the specified date range.")
            return None
        
        print(f"✅ Extracted {len(customers_df)} raw customer records.")
        
        # Show sample
        print("\n📋 Sample raw data:")
        print(customers_df.head(3).to_string())
        
        return customers_df
        
    except Exception as e:
        print(f"❌ Error in extract_mailchimp_data: {e}")
        import traceback
        traceback.print_exc()
        return None