from airflow.sdk import dag, task, asset
from pendulum import datetime
import os

@asset(
    schedule="@daily",
    uri = "/opt/airflow/logs/data/data_extract.txt",
    name="fetch_data"
)
def fetch_data(self):
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    # Write data to the file
    with open(self.uri, "w") as f:
        f.write("This is a sample data extract.")

    print(f"Data extract written to {self.uri}")