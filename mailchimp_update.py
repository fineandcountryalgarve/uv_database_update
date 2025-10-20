# Buyers/Vendors CRM

import pandas as pd
from app.utils.bq_pandas_helper import get_bq_client
from app.utils.date_helper import get_dynamic_date_range
from app.utils.gsheets import append_df_to_gsheet
from IPython.display import display
import requests
import hashlib
import json
import time
from app.utils.mailchimp_helper import (
    get_base_url,
    get_api_key,
    get_data_center,
    get_list_id,
    get_subscriber_hash,
    add_tags,
)

client = get_bq_client()
base_url = get_base_url()
api_key = get_api_key()
list_id = get_list_id()
data_center = get_data_center()

start_date, end_date = get_dynamic_date_range()

try:
    if start_date and end_date:
        query = f"""
        WITH cleaned_name AS (
        SELECT 
            rawbuyers_name AS full_name,
            rawbuyers_email AS email,
            rawbuyers_language AS language,
            rawbuyers_createtime AS create_time,
            rawbuyers_buttons AS buttons, 
            'Buyer' AS `Client nature`,

          IFNULL(
          CASE
            WHEN LOWER(rawbuyers_name) LIKE '%&%' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawbuyers_name, '&')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawbuyers_name, '&')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            WHEN LOWER(rawbuyers_name) LIKE '% and %' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawbuyers_name, ' and ')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawbuyers_name, ' and ')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            WHEN LOWER(rawbuyers_name) LIKE '% e %' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawbuyers_name, ' e ')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawbuyers_name, ' e ')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            ELSE SPLIT(rawbuyers_name, ' ')[OFFSET(0)]
          END,
          ''
        ) AS name

        FROM finecountrydatabase.algarve.rawbuyers

        UNION ALL 

        SELECT 
            rawsellers_name AS full_name,
            rawsellers_email AS email,
            rawsellers_language AS language,
            rawsellers_createtime AS create_time,
            rawsellers_buttons AS buttons,  
        'Seller' AS `Client nature`,

        IFNULL(
          CASE
            WHEN LOWER(rawsellers_name) LIKE '%&%' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawsellers_name, '&')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawsellers_name, '&')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            WHEN LOWER(rawsellers_name) LIKE '% and %' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawsellers_name, ' and ')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawsellers_name, ' and ')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            WHEN LOWER(rawsellers_name) LIKE '% e %' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawsellers_name, ' e ')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawsellers_name, ' e ')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            ELSE SPLIT(rawsellers_name, ' ')[OFFSET(0)]
          END,
          ''
        ) AS name
        FROM finecountrydatabase.algarve.rawsellers

        UNION ALL

        SELECT rawbuyerssellers_name AS full_name, 
        rawbuyerssellers_email AS email, 
        rawbuyerssellers_language AS language,
        rawbuyerssellers_createtime AS create_time,
        rawbuyerssellers_buttons AS buttons, 
        'Buyer/seller' AS `Client nature`,

        IFNULL(
          CASE
            WHEN LOWER(rawbuyerssellers_name) LIKE '%&%' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawbuyerssellers_name, '&')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawbuyerssellers_name, '&')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            WHEN LOWER(rawbuyerssellers_name) LIKE '% and %' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawbuyerssellers_name, ' and ')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawbuyerssellers_name, ' and ')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            WHEN LOWER(rawbuyerssellers_name) LIKE '% e %' THEN
              CONCAT(
                SPLIT(TRIM(SPLIT(rawbuyerssellers_name, ' e ')[OFFSET(0)]), ' ')[OFFSET(0)],
                ' & ',
                SPLIT(TRIM(SPLIT(rawbuyerssellers_name, ' e ')[OFFSET(1)]), ' ')[OFFSET(0)]
              )
            ELSE SPLIT(rawbuyerssellers_name, ' ')[OFFSET(0)]
          END,
          ''
        ) AS name
        FROM finecountrydatabase.algarve.rawbuyerssellers
        )

        SELECT DISTINCT
          email AS Email,
          `Client nature`,
          language AS Speaks,

          CASE WHEN language = 'French' THEN IFNULL(name, '') ELSE '' END AS `First Name FRE`,
          CASE WHEN language = 'Portuguese' THEN IFNULL(name, '') ELSE '' END AS `First Name POR`,
          CASE WHEN language = 'German' THEN IFNULL(name, '') ELSE '' END AS `First Name GER`,
          CASE WHEN language NOT IN ('German', 'Portuguese', 'French') THEN IFNULL(name, '') ELSE '' END AS `First Name ENG`,

          CASE
            WHEN language = 'French' THEN 'FRE'
            WHEN language = 'Portuguese' THEN 'POR'
            WHEN language = 'German' THEN 'GER'
            ELSE 'ENG'
          END AS `Tags`

        FROM cleaned_name
          WHERE
          (
                SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(create_time, 1, 10)) >= DATE('{start_date}')
                AND SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(create_time, 1, 10)) <= DATE('{end_date}')
            )
            OR (
                SAFE.PARSE_DATE('%d/%m/%Y', SUBSTR(create_time, 1, 10)) >= DATE('{start_date}')
                AND SAFE.PARSE_DATE('%d/%m/%Y', SUBSTR(create_time, 1, 10)) <= DATE('{end_date}')
            )
        AND (LOWER(buttons) NOT IN ('unsubscribed', 'to_update') OR buttons IS NULL)
        AND email IS NOT NULL AND email <> '-' AND NOT REGEXP_CONTAINS(LOWER(email), r'@(x+\.com|placeholder\.com|fake\.com)$')
        """

      # Execute the query and load the results into a DataFrame
    customers_crm = client.query(query).to_dataframe(create_bqstorage_client=True)

    display (customers_crm) 
    print(len(customers_crm))

except Exception as e:
    print (f"Error {e}, could not complete the query.")

# Iterate over the rows of your DataFrame
for index, row in customers_crm.iterrows():
    # Prepare the subscriber data
    subscriber_data = {
        "email_address": row['Email'],         # Email address
        "status": "subscribed",                # Status in Mailchimp
        "merge_fields": {
            "FNAMEENG": row['First Name ENG'],  # Directly use 'First Name ENG'
            "FNAMEFRE": row['First Name FRE'],  # Directly use 'First Name FRE'
            "FNAMEPOR": row['First Name POR'],  # Directly use 'First Name POR'
            "FNAMEGER": row['First Name GER'],  # Directly use 'First Name GER'
            "SPEAKS":  row['Speaks'],           # Use 'Speaks' field
            "CNATURE": row['Client nature']     # Use 'Client nature' field directly
        }
    }

    # Make a POST request to add the subscriber
    response = requests.post(
        base_url,  # Use base_url defined above
        auth=("anystring", api_key),  # API key authentication
        data=json.dumps(subscriber_data),
        headers={
            "Content-Type": "application/json"
        }
    )

    # Check if the request was successful
    if response.status_code == 200 or response.status_code == 204:
        print(f"Subscriber {row['Email']} added successfully.")

        # After adding the subscriber, add the tags
        subscriber_hash = get_subscriber_hash(row['Email'])
        add_tags(subscriber_hash, row['Tags'])  # Use the Tags column to add tags
    else:
        print(f"Failed to add subscriber {row['Email']}: {response.status_code}")
        try:
            print("Error:", response.json())  # Try to parse the error
        except ValueError:
            print("No response content available.")

print ("All contacts updated.")

headers = {
    "Authorization": f"apikey {api_key}"
}

# Step 1: Fetch all unsubscribed contacts using pagination (count and offset)
def fetch_all_unsubscribed_contacts():
    unsubscribed_members = []
    count = 1000  # Fetch up to 1000 contacts per request (max allowed by Mailchimp)
    offset = 0  # Start from the first contact
    total_items = 1  # Initialize total_items to start the loop

    while offset < total_items:
        url = f"https://{data_center}.api.mailchimp.com/3.0/lists/{list_id}/members?status=unsubscribed&count={count}&offset={offset}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            total_items = data.get('total_items', 0)  # Get the total number of unsubscribed contacts
            unsubscribed_members.extend(data.get('members', []))
            offset += count  # Move to the next batch
            print(f"Fetched {len(unsubscribed_members)} unsubscribed members so far...")
        else:
            print(f"Failed to fetch unsubscribed contacts: {response.status_code}")
            break

    return unsubscribed_members

# Fetch unsubscribed contacts
unsubscribed_members = fetch_all_unsubscribed_contacts()

# Step 2: Tag all unsubscribed contacts as INACTIVE (Batch Processing)
def batch_tag_inactive(members):
    batch_size = 10  # Set the batch size to 10 contacts per batch

    for i in range(0, len(members), batch_size):
        batch = members[i:i + batch_size]  # Create batches of 10 contacts
        print(f"Processing batch {i // batch_size + 1} with {len(batch)} contacts...")  # Log batch processing start

        for member in batch:
            email = member['email_address']
            email_hash = hashlib.md5(email.lower().encode('utf-8')).hexdigest()
            tags_url = f"https://{data_center}.api.mailchimp.com/3.0/lists/{list_id}/members/{email_hash}/tags"
            payload = {
                "tags": [{"name": "INACTIVE", "status": "active"}]
            }

            # Send request to apply the tag
            response = requests.post(tags_url, headers=headers, json=payload)

            if response.status_code == 204:
                print(f"Tagged {email} as INACTIVE.")
            else:
                print(f"Failed to tag {email}. Status code: {response.status_code}")
                try:
                    print(f"Error response: {response.json()}")
                except ValueError:
                    print("No error message available.")
                if response.status_code == 429:  # Rate limit
                    print("Rate limit reached, waiting 60 seconds...")
                    time.sleep(60)  # Wait for 60 seconds before retrying

        # Wait 1 second between each batch to avoid rate limiting
        print(f"Processed batch {i // batch_size + 1}. Waiting before processing the next batch...")
        time.sleep(1)

    print("All batches processed.")  # Log when all batches are processed

# Step 3: Process unsubscribed members for tagging in batches
if len(unsubscribed_members) > 0:
    print(f"Tagging {len(unsubscribed_members)} unsubscribed contacts as INACTIVE in batches of 10.")
    batch_tag_inactive(unsubscribed_members)
else:
    print("No unsubscribed contacts found.")

if unsubscribed_members:
    df = pd.DataFrame([
        {
            "Email": member['email_address'],
            "First Name": member['merge_fields'].get('FNAME'),
            "Last Name": member['merge_fields'].get('LNAME'),
            "Status": member['status']
        }
        for member in unsubscribed_members
    ])

    from datetime import datetime
    sheet_id = "1qD-rvDUM1okGqHOAoiAQ7sNy2oW9HxzNVOufu35oW90"
    worksheet_name = f"unsubscribed_{datetime.now().strftime('%Y%m%d_%H%M')}"

    append_df_to_gsheet(
        df=df,
        sheet_id=sheet_id,
        worksheet_name=worksheet_name,
        include_headers=True,
        create_if_missing=True
    )

    print(f"Unsubscribed contacts successfully written to Google Sheet tab: {worksheet_name}")
else:
    print("No unsubscribed contacts added to Google Sheets.")