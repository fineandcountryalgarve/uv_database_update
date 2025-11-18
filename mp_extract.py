"""
Extract module for Mailchimp ETL pipeline.
Refreshes materialized view and extracts raw customer data from PostgreSQL.
"""
from sqlalchemy import text
import pandas as pd
from app.utils.refresh_view import refresh_mv
from app.utils.date_helper import get_dynamic_date_range
from app.utils.db_engine import get_engine
from app.utils.gsheets import read_gsheet_to_df
from app.utils.gsheets_worksheets import get_gsheets_id

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

def extract_pre_enquiries(start_date, end_date) -> pd.DataFrame | None:
    """
    Extracts pre-enquiries from two Google Sheets and filters by date range.
    
    Args:
        start_date: Start date for filtering (date or datetime)
        end_date: End date for filtering (date or datetime)
        
    Returns:
        pd.DataFrame | None: Filtered pre-enquiries or None if error
    """
    try:
        print("\n📋 Extracting pre-enquiries from Google Sheets...")
        
        # Read both sheets
        carousel_id = get_gsheets_id("fb_carousel")
        properties_id = get_gsheets_id("fb_properties")
        print("\n📋 Extracting pre-enquiries from Google Sheets...")
        df_pre_enquiries = read_gsheet_to_df(carousel_id, "Sheet1")
        df_pre_enquiries_2 = read_gsheet_to_df(properties_id, "Sheet1")
        
        # Combine both dataframes
        fb_pre_enquiries = pd.concat([df_pre_enquiries, df_pre_enquiries_2], ignore_index=True)
        
        if fb_pre_enquiries.empty:
            print("⚠️ No pre-enquiries data found")
            return None
        
        # Convert created_time to datetime (UTC to handle mixed timezones)
        fb_pre_enquiries['created_time'] = pd.to_datetime(
            fb_pre_enquiries['created_time'], 
            utc=True
        )
        
        # Extract just the date part for comparison
        fb_pre_enquiries['created_date'] = fb_pre_enquiries['created_time'].dt.date
        
        # Ensure start_date and end_date are date objects
        if hasattr(start_date, 'date'):
            start_date = start_date.date()
        if hasattr(end_date, 'date'):
            end_date = end_date.date()
        
        print(f"   Filtering: {start_date} to {end_date}")
        
        # Filter by date range (comparing dates only)
        mask = (fb_pre_enquiries['created_date'] >= start_date) & \
               (fb_pre_enquiries['created_date'] <= end_date)
        fb_pre_enquiries = fb_pre_enquiries.loc[mask]
        
        if fb_pre_enquiries.empty:
            print(f"⚠️ No pre-enquiries found for date range {start_date} to {end_date}")
            return None
        
        print(f"✅ Extracted {len(fb_pre_enquiries)} pre-enquiries records")
        
        # Drop the temporary created_date column
        fb_pre_enquiries = fb_pre_enquiries.drop(columns=['created_date'])
        
        # Process to standard format
        processed_df = process_crm_data(fb_pre_enquiries)
        
        return processed_df
        
    except Exception as e:
        print(f"❌ Error extracting pre-enquiries: {e}")
        import traceback
        traceback.print_exc()
        return None


def process_crm_data(df):
    """
    Transforms pre-enquiries data to match Mailchimp format.
    
    Args:
        df: Raw pre-enquiries dataframe with 'full_name' and 'email' columns
        
    Returns:
        pd.DataFrame: Transformed dataframe matching Mailchimp schema
    """
    processed = df.copy()
    
    # Extract first name from full_name
    processed['First Name ENG'] = processed['full_name'].apply(
        lambda x: x.split()[0] if pd.notna(x) and str(x).strip() else ''
    )
    processed['First Name FRE'] = ''
    processed['First Name POR'] = ''
    processed['First Name GER'] = ''
    processed['Speaks'] = 'English'
    processed['Tags'] = 'ENG'
    processed['Client nature'] = 'Buyer'
    
    # Rename email column
    processed = processed.rename(columns={"email": "Email"})
    
    # Select only needed columns
    processed = processed[[
        'Email', 'Client nature', 'Speaks', 
        'First Name FRE', 'First Name POR', 'First Name GER', 
        'First Name ENG', 'Tags'
    ]]
    
    return processed


def extract_mailchimp_data() -> pd.DataFrame | None:
    """
    Extracts raw customer data from gold.customers_mv AND pre-enquiries from Google Sheets.
    Combines both sources into a single dataframe.
    
    Returns:
        pd.DataFrame | None: Combined customer data or None if error/no data
    """
    engine = get_engine()
    
    try:
        # Get date range for filtering (used by both sources)
        start_date, end_date = get_dynamic_date_range()
        if not start_date or not end_date:
            print("⚠️ Could not determine date range.")
            return None
            
        print(f"📅 Date range: {start_date} to {end_date}")
        
        # ==========================================
        # Extract from PostgreSQL (gold.customers_mv)
        # ==========================================
        print("\n🔄 Refreshing materialized view...")
        gold_customers_view = "gold.customers_mv"
        
        if not refresh_materialized_view(gold_customers_view):
            print("⚠️ Failed to refresh view, continuing with existing data...")
        
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
        
        print(f"✅ Extracted {len(customers_df)} customer records from PostgreSQL")
        
        # ==========================================
        # Extract from Google Sheets (pre-enquiries)
        # ==========================================
        pre_enquiries_df = extract_pre_enquiries(start_date, end_date)
        
        # ==========================================
        # Combine both sources
        # ==========================================
        if customers_df.empty and (pre_enquiries_df is None or pre_enquiries_df.empty):
            print("⚠️ No data found from any source for the specified date range.")
            return None
        
        # If we have both sources, combine them
        if pre_enquiries_df is not None and not pre_enquiries_df.empty:
            # Note: customers_df will be transformed in the transform step
            # pre_enquiries_df is already in final format from process_crm_data
            # For now, just store pre_enquiries separately in customers_df metadata
            print(f"\n📊 Data Summary:")
            print(f"   PostgreSQL customers: {len(customers_df)}")
            print(f"   Google Sheets pre-enquiries: {len(pre_enquiries_df)}")
            
            # Add a source column to track where data came from
            customers_df['_source'] = 'postgresql'
            pre_enquiries_df['_source'] = 'gsheets'
            
            # We'll combine after transform since formats differ
            # Store pre_enquiries in customers_df attrs for access in transform
            customers_df.attrs['pre_enquiries'] = pre_enquiries_df
        
        # Show sample
        print("\n📋 Sample PostgreSQL customer data:")
        print(customers_df.head(3).to_string())
        
        if pre_enquiries_df is not None:
            print("\n📋 Sample pre-enquiries data:")
            print(pre_enquiries_df.head(3).to_string())
        
        return customers_df
        
    except Exception as e:
        print(f"❌ Error in extract_mailchimp_data: {e}")
        import traceback
        traceback.print_exc()
        return None