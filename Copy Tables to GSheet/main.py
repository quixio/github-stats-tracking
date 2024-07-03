import os
import duckdb
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import logging
import ast
import time
from dotenv import load_dotenv
load_dotenv()

mdtoken = os.environ['MOTHERDUCK_TOKEN']
mddatabase = os.environ['MOTHERDUCK_DATABASE']
targettablestr = os.environ['TARGET_TABLES']
sleeptime = int(os.environ["sleeptime"])
targettables = ast.literal_eval(targettablestr)

print(f"Connecting to {mddatabase}...")

# initiate the MotherDuck connection through a service token through
con = duckdb.connect(f'md:{mddatabase}?motherduck_token={mdtoken}')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load credentials from environment variable
creds_json = os.getenv('GDRIVE_API_CREDENTIALS')
creds_dict = json.loads(creds_json)

# Authenticate and initialize Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

while True:
    for table in targettables:
        print(f"Updating table: {table}...")

        # Connect to DuckDB and query data
        df = con.execute(f'SELECT * FROM {table}').df()
        # Convert Timestamp objects to strings
        df = df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)

        print(f"Dataframe preview {df.head()}")

        # Open Google Sheet by ID and sheet name
        sheet_id = os.getenv('GSHEET_ID')
        sheet = client.open_by_key(sheet_id).worksheet(f'{table}')

        # Clear existing data
        sheet.clear()

        # Update with new data
        sheet.update([df.columns.values.tolist()] + df.values.tolist())

        print(f"Updated Sheet: {table}")

    print("--ALL TABLES UPDATED--")
    print(f"Sleeping {sleeptime} secs ({sleeptime / 3600} hours)...")

    time.sleep(sleeptime)

