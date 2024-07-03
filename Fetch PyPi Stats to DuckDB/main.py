import pypistats
import duckdb
import os
import logging
from dotenv import load_dotenv
load_dotenv()

mdtoken = os.environ['MOTHERDUCK_TOKEN']
mddatabase = os.environ['MOTHERDUCK_DATABASE']
targetable = os.environ['TABLE_NAME']

# More docs: https://github.com/hugovk/pypistats#numpy-and-pandas
df = pypistats.overall("quixstreams", total=True, format="pandas")

print(df.head())

print(f"Connecting to {mddatabase}...")

# initiate the MotherDuck connection through a service token through
conn = duckdb.connect(f'md:{mddatabase}?motherduck_token={mdtoken}')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a table from the DataFrame
conn.execute(f'''
CREATE TABLE IF NOT EXISTS {targetable} (
    category VARCHAR,
    date DATE,
    percent VARCHAR,
    downloads INTEGER
);
''')

# Insert data from the DataFrame into the table
conn.register('df_view', df)
conn.execute(f'INSERT INTO {targetable} SELECT * FROM df_view')

# Verify the data insertion
result = conn.execute(f'SELECT * FROM {targetable}').fetchall()
print(result)

# Close the connection
conn.close()