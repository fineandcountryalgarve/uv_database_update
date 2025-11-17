"""
Controller for Mailchimp ETL pipeline.
Orchestrates extract → transform → load workflow.
"""
import pandas as pd
import argparse
from mp_extract import extract_mailchimp_data
from mp_transform import transform_mailchimp_data
from mp_load import (
    load_to_mailchimp,
    load_unsubscribed_to_google_sheets,
    fetch_and_tag_unsubscribed
)


def extract() -> pd.DataFrame | None:
    """
    EXTRACT STEP: Refresh materialized view and extract raw customer data.
    
    Returns:
        pd.DataFrame | None: Raw customer data or None if error
    """
    print("\n" + "="*60)
    print("📥 EXTRACT STEP: Fetching customer data from PostgreSQL")
    print("="*60)
    
    df = extract_mailchimp_data()
    
    if df is not None:
        print(f"\n✅ Extract complete: {len(df)} raw records")
    else:
        print("\n⚠️ Extract returned no data")
    
    return df


def transform(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """
    TRANSFORM STEP: Apply Python transformations to raw data.
    
    Args:
        df: Raw customer data from extract step
        
    Returns:
        pd.DataFrame | None: Transformed data ready for loading
    """
    print("\n" + "="*60)
    print("🔄 TRANSFORM STEP: Transforming customer data")
    print("="*60)
    
    if df is None or df.empty:
        print("⚠️ No data to transform")
        return None
    
    transformed_df = transform_mailchimp_data(df)
    
    if transformed_df is not None:
        print(f"\n✅ Transform complete: {len(transformed_df)} records ready")
    else:
        print("\n⚠️ Transform failed")
    
    return transformed_df


def preview_data(df: pd.DataFrame, num_rows: int = 10):
    """
    Display a preview of the transformed data for validation.
    
    Args:
        df: DataFrame to preview
        num_rows: Number of rows to display
    """
    print("\n" + "="*60)
    print("👀 DATA PREVIEW")
    print("="*60)
    
    if df is None or df.empty:
        print("⚠️ No data to preview")
        return
    
    # Basic statistics
    print(f"\n📊 Total records: {len(df)}")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Data quality checks
    print("\n🔍 Data Quality Checks:")
    print(f"   • Emails with null values: {df['Email'].isna().sum()}")
    print(f"   • Emails with empty strings: {(df['Email'] == '').sum()}")
    print(f"   • Duplicate emails: {df['Email'].duplicated().sum()}")
    
    # Client nature distribution
    print("\n📈 Client Nature Distribution:")
    print(df['Client nature'].value_counts().to_string())
    
    # Language distribution
    print("\n🌍 Language Distribution:")
    print(df['Speaks'].value_counts().to_string())
    
    # Tags distribution
    print("\n🏷️ Tags Distribution:")
    print(df['Tags'].value_counts().to_string())
    
    # Sample records
    print(f"\n📄 First {min(num_rows, len(df))} records:")
    print("="*60)
    
    # Display with better formatting
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 30)
    
    print(df.head(num_rows).to_string(index=False))
    
    print("\n" + "="*60)


def export_preview(df: pd.DataFrame, filename: str = None):
    """
    Export transformed data to CSV for manual review.
    
    Args:
        df: DataFrame to export
        filename: Optional custom filename
    """
    if df is None or df.empty:
        print("⚠️ No data to export")
        return
    
    from datetime import datetime
    
    if filename is None:
        filename = f"mailchimp_preview_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    try:
        df.to_csv(filename, index=False)
        print(f"\n✅ Preview exported to: {filename}")
        print(f"   Records exported: {len(df)}")
    except Exception as e:
        print(f"❌ Error exporting preview: {e}")


def load(df: pd.DataFrame | None, dry_run: bool = False) -> bool:
    """
    LOAD STEP: Upload transformed data to Mailchimp.
    Also processes unsubscribed contacts and exports them to Google Sheets.
    
    Args:
        df: Transformed customer data from transform step
        dry_run: If True, skip actual loading (preview mode)
        
    Returns:
        bool: True if all loads successful, False otherwise
    """
    print("\n" + "="*60)
    print("📤 LOAD STEP: Uploading data to destinations")
    print("="*60)
    
    if df is None or df.empty:
        print("⚠️ No data to load. Skipping load step.")
        return False
    
    if dry_run:
        print("\n🔒 DRY RUN MODE: Skipping actual data upload")
        print(f"   Would upload {len(df)} records to Mailchimp")
        print("   Would process unsubscribed contacts and export to Google Sheets")
        return True
    
    # Load to Mailchimp
    print("\n📧 Loading subscribed contacts to Mailchimp...")
    mailchimp_success = load_to_mailchimp(df)
    
    # Handle unsubscribed contacts
    print("\n🚫 Processing unsubscribed contacts...")
    unsubscribed_df = fetch_and_tag_unsubscribed()
    
    if unsubscribed_df is not None:
        print("\n📊 Loading unsubscribed contacts to Google Sheets...")
        unsubscribed_success = load_unsubscribed_to_google_sheets(unsubscribed_df)
    else:
        unsubscribed_success = True  # No unsubscribed = still success
    
    # Summary
    all_success = mailchimp_success and unsubscribed_success
    
    print("\n" + "="*60)
    print("📊 LOAD SUMMARY")
    print("="*60)
    print(f"   Mailchimp (subscribed): {'✅ Success' if mailchimp_success else '❌ Failed'}")
    print(f"   Google Sheets (unsubscribed): {'✅ Success' if unsubscribed_success else '❌ Failed'}")
    print("="*60)
    
    return all_success


def main():
    """
    Main entry point for Mailchimp ETL pipeline.
    Executes: Extract → Transform → Load
    
    Supports flags:
        --dry-run: Run extract and transform only, skip load
        --preview-rows N: Show N rows in preview (default: 10)
        --export-preview: Export transformed data to CSV
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Mailchimp ETL Pipeline')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Run extract and transform only, skip actual loading')
    parser.add_argument('--preview-rows', type=int, default=10,
                       help='Number of rows to show in preview (default: 10)')
    parser.add_argument('--export-preview', action='store_true',
                       help='Export transformed data to CSV file')
    parser.add_argument('--export-filename', type=str, default=None,
                       help='Custom filename for exported CSV')
    
    args = parser.parse_args()
    
    print("\n" + "🚀 " + "="*58)
    print("🚀 MAILCHIMP ETL PIPELINE STARTED")
    if args.dry_run:
        print("🔒 MODE: DRY RUN (No data will be uploaded)")
    print("🚀 " + "="*58 + "\n")
    
    # Step 1: Extract
    raw_df = extract()
    
    # Step 2: Transform
    transformed_df = transform(raw_df)
    
    # Step 3: Preview (always show in dry-run mode)
    if transformed_df is not None:
        if args.dry_run or args.export_preview:
            preview_data(transformed_df, num_rows=args.preview_rows)
        
        if args.export_preview:
            export_preview(transformed_df, filename=args.export_filename)
        
        # Step 4: Load
        success = load(transformed_df, dry_run=args.dry_run)
        
        if success:
            print("\n✅ Pipeline completed successfully!")
        else:
            print("\n⚠️ Pipeline completed with errors.")
    else:
        print("\n❌ Pipeline failed: No data to process.")
    
    print("\n" + "="*60)
    print("🏁 MAILCHIMP ETL PIPELINE FINISHED")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()