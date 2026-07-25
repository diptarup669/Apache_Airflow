from airflow.sdk import dag, task , asset
from pendulum import datetime
import os

@asset(
    schedule="@daily",
    #uri is the location where the asset will be stored, in this case it is a text file named data_extract.txt located in the logs/data directory
    uri="/opt/airflow/logs/data/data_extract.txt",
    name="fetch_data",
 )

def fetch_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri, "w") as f:
        f.write(f"Data fetched successfully")
    print(f"Data writen to {self.uri}")