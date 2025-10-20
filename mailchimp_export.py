# Buyers/Vendors CRM

import pandas as pd
from pathlib import Path
from app.utils.bq_pandas_helper import get_bq_client
from datetime import datetime
from dateutil.relativedelta import relativedelta
from IPython.display import display

client = get_bq_client()

today = datetime.today()
last_month = today - relativedelta(months=1)

start_date = last_month.date()
end_date = today.date()

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

customers_crm.to_csv('/mnt/c/Users/rfont/Documents/Python/database_update/customers_crm.csv', index=False)