from airflow.sdk import dag, task, asset
from pendulum import datetime
from asset_11 import fetch_data
import os

@asset(
    schedule=fetch_data,
    uri = "/opt/airflow/logs/data/data_process.txt",
    name="process_data"
)
def process_data(self):
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    # Write data to the file
    with open(self.uri, "w") as f:
        f.write("This is a sample data extract.")

    print(f"Data extract written to {self.uri}")