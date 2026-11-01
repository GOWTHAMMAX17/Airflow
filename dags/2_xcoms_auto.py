from airflow.sdk import dag, task
from datetime import datetime

from more_itertools import extract

@dag(
    dag_id="xcoms_auto"
)
def xcoms_auto():

    @task.python
    def extract():
        print("Extracting data...task_1")
        fetched_data = {"data":[1,2,3,4,5]}
        return fetched_data

    @task.python
    def transform(fetched_data: dict):
        print("Transforming data...task_2")
        transformed_data = [i*2 for i in fetched_data["data"]]
        return transformed_data

    @task.python
    def load(transformed_data: list):
        print("Loading data...task_3")
        loaded_data  = transformed_data
        return loaded_data

    # Define the task dependencies
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    # Set the order of execution
    extracted >> transformed >> loaded

#instantiate the DAG
dag = xcoms_auto()