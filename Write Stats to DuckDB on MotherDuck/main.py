from quixstreams import Application
import duckdb
import os
import logging
import datetime
from dotenv import load_dotenv
load_dotenv()

mdtoken = os.environ['MOTHERDUCK_TOKEN']
mddatabase = os.environ['MOTHERDUCK_DATABASE']
# initiate the MotherDuck connection through a service token through
con = duckdb.connect(f'md:{mddatabase}?motherduck_token={mdtoken}')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

referralstable = "referral_stats_rolling14days"
pageviewstable = "pageview_stats_rolling14days"
dailyviewstable = "totalviews_daybreakdown"
totalviewstable = "totalviews_rolling14days"

def to_duckdb(conn, msg):
    try:
        sourcerepo = msg["repo"]
        reportedtime = msg["day_recorded"]
        logger.info(f"####### Collecting stats for repo {sourcerepo}")

        # Check if the referrals table exists and create it if not
        table_exists = conn.execute(
            f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{referralstable}')").fetchone()[0]
        if not table_exists:

            conn.execute(f'''
                CREATE TABLE {referralstable} (
                    repo VARCHAR,
                    referrer VARCHAR,
                    count INTEGER,
                    uniques INTEGER,
                    day TIMESTAMP,
                    UNIQUE(repo, referrer, day)
                );
            ''')

        for record in msg["referrals"]:
            conn.execute(f'''
                INSERT INTO {referralstable} (repo, referrer, count, uniques, day) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (repo, referrer, day) # Need to overwrite rather than insert if same combo of date-repo-referrer already exists
                DO UPDATE SET count = excluded.count, uniques = excluded.uniques;
                ''', (sourcerepo, record['referrer'], record['count'], record['uniques'], reportedtime))
            logger.info(f"Wrote referral record: {record}")

        # Check if the pageviews table exists and create it if not
        table_exists = conn.execute(
            f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{pageviewstable}')").fetchone()[
            0]
        if not table_exists:
            conn.execute(f'''
                CREATE TABLE {pageviewstable} (
                    day TIMESTAMP,
                    repo VARCHAR,
                    path VARCHAR,
                    title VARCHAR,
                    count INTEGER,
                    uniques INTEGER,
                    UNIQUE(repo, path, day)
                );
            ''')

        for record in msg["pageviews"]:
            conn.execute(f'''
                INSERT INTO {pageviewstable} (day, repo, path, title, count, uniques) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (repo, path, day)  # Need to overwrite rather than insert if same combo of date-repo-path already exists
                DO UPDATE SET title = excluded.title, count = excluded.count, uniques = excluded.uniques;
                ''', (reportedtime, sourcerepo, record['path'], record['title'], record['count'], record['uniques']))

        # Check if the daily views table exists and create it if not
        table_exists = conn.execute(
            f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{dailyviewstable}')").fetchone()[
            0]
        if not table_exists:
            conn.execute(f'''
            CREATE TABLE {dailyviewstable} (
                day TIMESTAMP,
                repo VARCHAR,
                count INTEGER,
                uniques INTEGER,
                UNIQUE(repo, day)
            );
            ''')

        for record in msg["views"]["views"]:
            conn.execute(f'''
                INSERT INTO {dailyviewstable} (day, repo, count, uniques) VALUES (?, ?, ?, ?)
                ON CONFLICT (repo, day)  # Need to overwrite rather than insert if same combo of date-repo already exists
                DO UPDATE SET count = excluded.count, uniques = excluded.uniques;
                ''', (record['timestamp'], sourcerepo, record['count'], record['uniques']))
            logger.info(f"Wrote views record: {record}")

        # Create the rolling views table to match the data structure
        table_exists = conn.execute(
            f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{totalviewstable}')").fetchone()[
            0]
        if not table_exists:
            conn.execute(f'''
            CREATE TABLE {totalviewstable} (
                repo VARCHAR,
                count INTEGER,
                uniques INTEGER,
                day TIMESTAMP,
                UNIQUE(repo, day)
            );
            ''')

        conn.execute(f'''
        INSERT INTO {totalviewstable} (repo, count, uniques, day) VALUES (?, ?, ?, ?)
        ON CONFLICT (repo, day)   # Need to overwrite rather than insert if same combo of date-repo already exists
        DO UPDATE SET count = excluded.count, uniques = excluded.uniques;
        ''', (sourcerepo, msg["views"]["count"], msg["views"]["uniques"], reportedtime))
        logger.info(f"Wrote agg-views record for: {sourcerepo}")

    except Exception as e:
        logger.info(f"{str(datetime.datetime.utcnow())}: Write failed")
        logger.info(e)

# Define your application and settings
app = Application(
    consumer_group=os.environ['CONSUMER_GROUP_NAME'],
    auto_offset_reset="earliest",
)

# Define an input topic with JSON deserializer
input_topic = app.topic(os.environ['input'], value_deserializer="json")

# Initialize a streaming dataframe based on the stream of messages from the input topic:
sdf = app.dataframe(topic=input_topic)
sdf = sdf.update(lambda val: print(f"Received update: {val}"))

# Trigger the write function for any new messages(rows) in the SDF
sdf = sdf.update(lambda val: to_duckdb(con, val), stateful=False)

app.run(sdf)
