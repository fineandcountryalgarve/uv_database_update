import pandas as pd
from app.utils.gsheets import read_gsheet_to_df
from IPython.display import display
import requests
import json
from app.utils.mailchimp_helper import (
    get_subscriber_hash,
    add_tags,
    get_base_url,
    get_api_key
)

base_url = get_base_url()
api_key = get_api_key()

def process_crm_data(df):
    df['First Name ENG'] = df['full_name'].apply(lambda x: x.split()[0])
    df['First Name FRE'] = ''
    df['First Name POR'] = ''
    df['First Name GER'] = ''
    df['Speaks'] = 'English'
    df['Tags'] = 'ENG'
    df['Client nature'] = 'Buyer'
    df = df.rename(columns={"email": "Email"})
    df = df[['Email', 'Client nature', 'Speaks', 'First Name FRE', 'First Name POR', 'First Name GER', 
                               'First Name ENG', 'Tags',]]
    return df

def extract_pre_enquiries ():
    df_pre_enquiries = read_gsheet_to_df("1VY-q3fM5u1W9aPYavcHFt2xDjyO9ojKqcTm2jtiF7_8", "Sheet1")
    df_pre_enquiries_2 = read_gsheet_to_df("1tXoC7jdp1SkhJFExQxVFeqp1hv9D0pKyflzNRUZBIog", "Sheet1")
    fb_pre_enquiries = pd.concat([df_pre_enquiries, df_pre_enquiries_2])

# Iterate over the rows of your DataFrame
for index, row in fb_pre_enquiries.iterrows():
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
