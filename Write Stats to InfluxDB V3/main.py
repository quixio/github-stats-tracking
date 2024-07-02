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

# Create a Quix platform-specific application instead
app = Application(
    consumer_group=consumer_group_name,
    auto_offset_reset="earliest",
    use_changelog_topics=False)

input_topic = app.topic(os.environ["input"])
                                           
influx3_client = InfluxDBClient3(
                         token=os.environ["INFLUXDB_TOKEN"],
                         host=os.environ["INFLUXDB_HOST"],
                         org=os.environ["INFLUXDB_ORG"],
                         database=os.environ["INFLUXDB_DATABASE"])

def to_influxdb(msg):
    try:
        sourcerepo = msg["repo"]
        reportedtime = msg["day_recorded"]
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
            point = Point("views-dailybreakdown") \
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