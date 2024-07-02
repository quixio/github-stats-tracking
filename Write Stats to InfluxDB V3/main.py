# import Utility modules
import os
from datetime import datetime
import logging

# import vendor-specific modules
from quixstreams import Application
from influxdb_client_3 import Point, InfluxDBClient3, WritePrecision

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# read the consumer group from config
consumer_group_name = os.environ.get("CONSUMER_GROUP_NAME", "influxdb-data-writer")

# read the timestamp column from config
timestamp_column = os.environ.get("TIMESTAMP_COLUMN", "")

# Create a Quix platform-specific application instead
app = Application(
    consumer_group=consumer_group_name,
    auto_offset_reset="earliest",
    use_changelog_topics=False)

input_topic = app.topic(os.environ["input"])

# Read the environment variable and convert it to a dictionary
# tag_keys = ast.literal_eval(os.environ.get("INFLUXDB_TAG_KEYS", "[]"))
# ield_keys = ast.literal_eval(os.environ.get("INFLUXDB_FIELD_KEYS", "[]"))

# Read the environment variable for the field(s) to get.
# For multiple fields, use a list "["field1","field2"]"
                                           
influx3_client = InfluxDBClient3(
                         token=os.environ["INFLUXDB_TOKEN"],
                         host=os.environ["INFLUXDB_HOST"],
                         org=os.environ["INFLUXDB_ORG"],
                         database=os.environ["INFLUXDB_DATABASE"])

# Get the measurement name to write data to
# measurement_name = os.environ.get("INFLUXDB_MEASUREMENT_NAME", "measurement1")

# Initialize a buffer for batching points and a timestamp for the last write
# points_buffer = []
# service_start_state = True
# last_write_time_ns = int(time() * 1e9)  # Convert current time from seconds to nanoseconds

def to_influxdb(msg):
    try:
        sourcerepo = msg["repo"]
        reportedtime = msg["timestamp_iso"]
        logger.info(f"####### Collecting stats for repo {sourcerepo}")

        for item in msg["referrals"]:
            point = Point("referrals-rolling14days") \
                .tag("repo", sourcerepo) \
                .tag("referrer", item["referrer"]) \
                .field("count", item["count"]) \
                .field("uniques", item["uniques"]) \
                .time(reportedtime, WritePrecision.NS)
            influx3_client.write(record=point)
            logger.info(f"Wrote referrals record: {point}")

        for item in msg["pageviews"]:
            point = Point("pageviews-rolling14days") \
                .tag("repo", sourcerepo) \
                .tag("path", item["path"]) \
                .field("title", item["title"]) \
                .field("count", item["count"]) \
                .field("uniques", item["uniques"]) \
                .time(reportedtime, WritePrecision.NS)
            influx3_client.write(record=point)
            logger.info(f"Wrote referrer record: {point}")

        for view in msg["views"]["views"]:
            point = Point("views-daily") \
                .tag("repo", sourcerepo) \
                .field("count", view["count"]) \
                .field("uniques", view["uniques"]) \
                .time(view["timestamp"], WritePrecision.NS)
            influx3_client.write(record=point)
            logger.info(f"Wrote views record: {point}")

        point = Point("views-rolling14days") \
            .tag("repo", sourcerepo) \
            .field("count", msg["views"]["count"]) \
            .field("uniques", msg["views"]["uniques"]) \
            .time(reportedtime, WritePrecision.NS)
        influx3_client.write(record=point)
        logger.info(f"Wrote agg-views record: {point}")

    except Exception as e:
        logger.info(f"{str(datetime.utcnow())}: Write failed")
        logger.info(e)


sdf = app.dataframe(input_topic)
sdf = sdf.update(to_influxdb)

if __name__ == "__main__":
    logger.info("Starting application")
    app.run(sdf)