from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id = "xcom_auto_p"
)
def xcom_auto_p():

    @task.python
    def extract():
        print("Extracting data...task 1")
        fetched_data = {"data":[1,2,3,4,5]}
        return fetched_data

    @task.python
    def transform(fetched_data: dict):
        print("transforming data...task 2")
        transformed_data= [i*2 for i in fetched_data["data"]]
        return transformed_data

    @task.python
    def load(transformed_data: list):
        print("Loading data...task 3")
        loaded_data = transformed_data*2
        return loaded_data

    # Define the task dependencies
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    # set the order of execution
    extracted >> transformed >> loaded

#instantiate the DAG
dag = xcom_auto_p()