from airflow.sdk import dag, task , asset
from pendulum import datetime
import os
from assets13 import fetch_data

@asset(
    schedule=fetch_data,
    #uri is the location where the asset will be stored, in this case it is a text file named data_extract.txt located in the logs/data directory
    uri="/opt/airflow/logs/data/data_processed.txt",
    name="process_data",
 )

def process_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri, "w") as f:
        f.write(f"Data processed successfully")
    print(f"Data written to {self.uri}")