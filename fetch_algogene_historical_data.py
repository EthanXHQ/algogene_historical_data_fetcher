import os
import pandas as pd
import requests
import yaml

from datetime import datetime, timedelta

# Read config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

url = config["base_url"]
headers = {"Content-Type": "application/json"}
MAX_COUNT = 1000  # Single query limit

# Query time period
start_time = datetime.strptime(config["start_time"], "%Y-%m-%d")
end_time = datetime.strptime(config["end_time"], "%Y-%m-%d")

all_data = []  # Storing all query data
current_time = end_time  # from end_time to start_time

while current_time >= start_time:
    print(f"Fetching data before {current_time.strftime('%Y-%m-%d')}...")

    params = {
        "user": config["user_id"],
        "api_key": config["api_key"],
        "count": MAX_COUNT,
        "interval": config["interval"],
        "instrument": config["instrument"],
        "timestamp": current_time.strftime("%Y-%m-%d")
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        if "res" in data and data["res"]:
            all_data[:0] = data["res"]  # Add new query to the front
            print(f"Received {len(data['res'])} records")

            # Get the earliest timestamp
            oldest_timestamp = all_data[0]["t"]
            current_time = datetime.strptime(oldest_timestamp, "%Y-%m-%d %H:%M:%S") - timedelta(seconds=1)

        else:
            print("No data returned for this timestamp.")
            break
    else:
        print(f"API request failed: {response.status_code}, {response.text}.")
        break

# Write in CSV
data_dir = "algogene_dataset"
os.makedirs(data_dir, exist_ok=True)

if all_data:
    df = pd.DataFrame(all_data)
    csv_filename = os.path.join(data_dir, f"{config['instrument']}_{config["start_time"]}_{config["end_time"]}_{config["interval"]}.csv")
    df.to_csv(csv_filename, index=False)
    print(f"Data has been saved in {csv_filename}.")
else:
    print("No data returned.")
