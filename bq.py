import json
from google.cloud import bigquery


PROJECT_ID = "sec-edgar-filings-web-scraping"
BQ_DATASET = 'edgar-data'
BQ_TABLE = 'nport'
BQ = bigquery.Client()

with open("./output/NPORT-P_2020-07-17_ASA Gold & Precious Metals Ltd_ASA GOLD  PRECIOUS METALS LTD.json", 'r') as f:
    data = json.load(f)
    print(data)

