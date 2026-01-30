"""
Load module for Mailchimp ETL pipeline.
Handles uploading transformed data to Mailchimp and unsubscribed data to Google Sheets.
"""
import requests
import json
import hashlib
import time
import pandas as pd
from datetime import datetime
from app.utils.gsheets import append_df_to_gsheet
from app.utils.mailchimp_helper import (
    get_base_url,
    get_api_key,
    get_data_center,
    get_list_id,
    get_subscriber_hash,
    add_tags,
)
from app.utils.gsheets_worksheets import get_gsheets_id


def load_to_mailchimp(df: pd.DataFrame) -> bool:
    """
    Loads customer data to Mailchimp.
    
    Args:
        df: DataFrame with customer data (must have Email, First Name columns, Speaks, Tags, etc.)
        
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None or df.empty:
        print("⚠️ No data to load to Mailchimp.")
        return False
    
    try:
        base_url = get_base_url()
        api_key = get_api_key()
        
        print(f"📤 Loading {len(df)} contacts to Mailchimp...")
        
        success_count = 0
        skipped_count = 0
        error_count = 0

        for index, row in df.iterrows():
            # Prepare subscriber data
            subscriber_data = {
                "email_address": row['Email'],
                "status": "subscribed",
                "merge_fields": {
                    "FNAMEENG": row.get('First Name ENG', ''),
                    "FNAMEFRE": row.get('First Name FRE', ''),
                    "FNAMEPOR": row.get('First Name POR', ''),
                    "FNAMEGER": row.get('First Name GER', ''),
                    "SPEAKS": row.get('Speaks', ''),
                    "CNATURE": row.get('Client nature', 'buyer')
                }
            }

            # Make POST request to add subscriber
            response = requests.post(
                base_url,
                auth=("anystring", api_key),
                data=json.dumps(subscriber_data),
                headers={"Content-Type": "application/json"}
            )

            # Check response
            if response.status_code in [200, 204]:
                print(f"✅ Subscriber {row['Email']} added successfully.")
                success_count += 1

                # Add tags
                subscriber_hash = get_subscriber_hash(row['Email'])
                add_tags(subscriber_hash, row.get('Tags', ''))

            elif response.status_code == 400:
                # Check if it's a "Member Exists" error - skip these
                try:
                    error_data = response.json()
                    if error_data.get('title') == 'Member Exists':
                        print(f"⏭️ Skipped {row['Email']} (already in list)")
                        skipped_count += 1
                    else:
                        print(f"❌ Failed to add subscriber {row['Email']}: {response.status_code}")
                        print(f"   Error: {error_data}")
                        error_count += 1
                except ValueError:
                    print(f"❌ Failed to add subscriber {row['Email']}: {response.status_code}")
                    error_count += 1
            else:
                print(f"❌ Failed to add subscriber {row['Email']}: {response.status_code}")
                error_count += 1
                try:
                    print(f"   Error: {response.json()}")
                except ValueError:
                    print("   No response content available.")

        print(f"\n📊 Mailchimp load complete: {success_count} added, {skipped_count} skipped, {error_count} errors")
        return error_count == 0
        
    except Exception as e:
        print(f"❌ Error loading to Mailchimp: {e}")
        return False


def load_unsubscribed_to_google_sheets(df: pd.DataFrame) -> bool:
    """
    Loads unsubscribed contacts to Google Sheets for CRM export.
    
    Args:
        df: DataFrame with unsubscribed contact data
        
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None or df.empty:
        print("⚠️ No unsubscribed data to load to Google Sheets.")
        return False
    
    try:
        sheet_id = get_gsheets_id("unsubscribed")
        
        if not sheet_id:
            print("❌ No Google Sheets ID configured for 'unsubscribed'")
            return False
        
        # Create timestamped worksheet name
        worksheet_name = f"unsubscribed_{datetime.now().strftime('%Y%m%d_%H%M')}"
        
        print(f"📤 Loading {len(df)} unsubscribed records to Google Sheets...")
        print(f"   Tab name: {worksheet_name}")
        
        # Append to Google Sheets
        append_df_to_gsheet(
            df=df,
            sheet_id=sheet_id,
            worksheet_name=worksheet_name,
            include_headers=True,
            create_if_missing=True
        )
        
        print(f"✅ Unsubscribed data successfully written to Google Sheet tab: {worksheet_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error loading unsubscribed to Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return False


def fetch_and_tag_unsubscribed() -> pd.DataFrame | None:
    """
    Fetches all unsubscribed contacts from Mailchimp,
    tags them as INACTIVE, and returns DataFrame for Google Sheets.
    
    Returns:
        pd.DataFrame | None: DataFrame of unsubscribed contacts or None
    """
    try:
        api_key = get_api_key()
        data_center = get_data_center()
        list_id = get_list_id()
        
        headers = {"Authorization": f"apikey {api_key}"}
        
        # Fetch all unsubscribed contacts with pagination
        print("🔍 Fetching unsubscribed contacts from Mailchimp...")
        unsubscribed_members = []
        count = 1000
        offset = 0
        total_items = 1
        
        while offset < total_items:
            url = f"https://{data_center}.api.mailchimp.com/3.0/lists/{list_id}/members?status=unsubscribed&count={count}&offset={offset}"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                total_items = data.get('total_items', 0)
                unsubscribed_members.extend(data.get('members', []))
                offset += count
                print(f"   Fetched {len(unsubscribed_members)} unsubscribed members so far...")
            else:
                print(f"❌ Failed to fetch unsubscribed contacts: {response.status_code}")
                break
        
        if not unsubscribed_members:
            print("ℹ️ No unsubscribed contacts found.")
            return None
        
        # Tag all unsubscribed as INACTIVE (batch processing)
        print(f"\n🏷️ Tagging {len(unsubscribed_members)} contacts as INACTIVE...")
        batch_size = 10
        
        for i in range(0, len(unsubscribed_members), batch_size):
            batch = unsubscribed_members[i:i + batch_size]
            print(f"   Processing batch {i // batch_size + 1}...")
            
            for member in batch:
                email = member['email_address']
                email_hash = hashlib.md5(email.lower().encode('utf-8')).hexdigest()
                tags_url = f"https://{data_center}.api.mailchimp.com/3.0/lists/{list_id}/members/{email_hash}/tags"
                payload = {"tags": [{"name": "INACTIVE", "status": "active"}]}
                
                response = requests.post(tags_url, headers=headers, json=payload)
                
                if response.status_code == 204:
                    print(f"   ✅ Tagged {email} as INACTIVE")
                else:
                    print(f"   ❌ Failed to tag {email}: {response.status_code}")
                    if response.status_code == 429:
                        print("   ⏳ Rate limit reached, waiting 60 seconds...")
                        time.sleep(60)
            
            time.sleep(1)  # Wait between batches
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                "Email": member['email_address'],
                "First Name": member['merge_fields'].get('FNAME'),
                "Last Name": member['merge_fields'].get('LNAME'),
                "Status": member['status']
            }
            for member in unsubscribed_members
        ])
        
        print(f"✅ Processed {len(df)} unsubscribed contacts.")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching/tagging unsubscribed contacts: {e}")
        return None