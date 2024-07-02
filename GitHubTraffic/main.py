from quixstreams import Application
import os
import json
import requests
import time
import datetime

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(auto_create_topics=True)  # create an Application

# define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(topic_name)

# Replace with your GitHub token and organization details
GITHUB_TOKEN = os.getenv('GH_TOKEN', '')
ORG = 'quixio'

def get_repos():
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    repos = []
    page = 1
    while True:
        repos_url = f'https://api.github.com/orgs/{ORG}/repos?page={page}&per_page=100'
        response = requests.get(repos_url, headers=headers)
        if response.status_code != 200:
            break
        page_repos = response.json()
        if not page_repos:
            break
        repos.extend(page_repos)
        page += 1
    return repos

def get_data(repo_name):
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }

    # Get traffic sources
    traffic_url = f'https://api.github.com/repos/{ORG}/{repo_name}/traffic/popular/referrers'
    response = requests.get(traffic_url, headers=headers)
    traffic_sources = response.json()

    # Get referring sites
    referring_sites_url = f'https://api.github.com/repos/{ORG}/{repo_name}/traffic/popular/paths'
    response = requests.get(referring_sites_url, headers=headers)
    referring_sites = response.json()

    # Get total and unique visitors
    views_url = f'https://api.github.com/repos/{ORG}/{repo_name}/traffic/views'
    response = requests.get(views_url, headers=headers)
    views = response.json()

    # debug
    traffic_sources_json = json.dumps(traffic_sources)
    referring_sites_json = json.dumps(referring_sites)
    views_json = json.dumps(views)
    print(f"Traffic Sources JSON for {repo_name}:", traffic_sources_json)
    print(f"Referring Sites JSON for {repo_name}:", referring_sites_json)
    print(f"Views JSON for {repo_name}:", views_json)

    current_time = datetime.datetime.utcnow()
    return {
        "repo": repo_name,
        "traffic": traffic_sources,
        "referrers": referring_sites,
        "views": views,
        "timestamp_iso": current_time.isoformat() + 'Z',  # ISO 8601 format
        "timestamp_unix": int(current_time.timestamp())  # Unix timestamp
    }

def main():
    """
    Read data from the hardcoded dataset and publish it to Kafka
    """
    while True:
        repos = get_repos()
        with app.get_producer() as producer:
            for repo in repos:
                repo_name = repo['name']
                json_data = json.dumps(get_data(repo_name))  # convert the row to JSON
                print(json_data)
                # publish the data to the topic
                producer.produce(
                    topic=topic.name,
                    key=f'github_stats_{ORG}_{repo_name}',
                    value=json_data,
                )

            print("All rows published")
        time.sleep(3600) # sleep 1 hour

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")