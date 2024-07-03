import requests
import os
import logging
import pandas as pd
import duckdb
import time
from dotenv import load_dotenv
load_dotenv()

# Replace with your GitHub personal access token and repository details
token = os.environ['GH_TOKEN']
owner = os.environ['GH_ORG']
repo = os.environ['GH_REPO']

mdtoken = os.environ['MOTHERDUCK_TOKEN']
mddatabase = os.environ['MOTHERDUCK_DATABASE']
sleeptime = int(os.environ["sleeptime"])
print(f"Connecting to {mddatabase}...")

# initiate the MotherDuck connection through a service token through
conn = duckdb.connect(f'md:{mddatabase}?motherduck_token={mdtoken}')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

while True:
    headers = {
        'Accept': 'application/vnd.github.v3.star+json',
        'Authorization': f'Bearer {token}'
    }

    url = f'https://api.github.com/repos/{owner}/{repo}/stargazers?per_page=1'
    params = {'per_page': 100}  # Adjust the number of results per page
    stargazers = []
    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            stargazers.extend(response.json())
            # Check if there's a next page
            if 'next' in response.links:
                url = response.links['next']['url']
                params = None  # No need to send params again for the next pages
            else:
                url = None
        else:
            print(f"Error: {response.status_code}")
            break

    data = {
        'user': [],
        'stardate': [],
    }

    for gazer in stargazers:
        data['user'].append(gazer['user']['login'])
        data['stardate'].append(gazer['starred_at'])

    # Convert dictionary to DataFrame
    df = pd.DataFrame(data)
    df['stardate'] = pd.to_datetime(df['stardate'])
    df_sorted = df.sort_values(by='stardate', ascending=False)
    df_sorted = df_sorted.reset_index().rename(columns={'index': 'number'})

    # Display the DataFrame
    print(df_sorted)

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ghstars (
        number INTEGER,
        user VARCHAR,
        stardate TIMESTAMP
    );
    """
    conn.execute(create_table_query)

    # Insert DataFrame into DuckDB table
    conn.execute("INSERT INTO ghstars SELECT * FROM df_sorted")

    # Verify insertion
    result = conn.execute("SELECT * FROM ghstars").fetchall()
    print(result)

    print(f"All rows published. Sleeping {sleeptime} secs ({sleeptime / 3600} hours)...")
    time.sleep(sleeptime) # sleep 1 hour
