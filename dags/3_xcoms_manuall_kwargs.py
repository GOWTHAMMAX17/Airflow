from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id = 'xcom_manual_kward'
)
def xcom_manual_kwargs():

    @task.python
    def extract(**kwargs):
        #extracting 'ti' from kwargs to push xcoms manually
        ti = kwargs["ti"]
        print("Extracting data...task_1")
        fetched_data = {"data":[1,2,3,4,5]}
        #pushing xcoms manually
        ti.xcom_push(key="working_data", value=fetched_data)

    @task.python
    def transform(**kwargs):
        #extracting 'ti' from kwargs to pull xcoms manually
        ti = kwargs["ti"]
        print("Transforming data...task_2")
        #pulling xcoms manually pushed by the first task
        fetched_data = ti.xcom_pull(key="working_data", task_ids="extract")
        transformed_data = [i*2 for i in fetched_data["data"]]
        #pushing xcoms manually
        ti.xcom_push(key="transformed_data", value=transformed_data)

    @task.python
    def load(**kwargs):
        #extracting 'ti' from kwargs to pull xcoms manually
        ti = kwargs["ti"]
        print("Loading data...task_3")
        #pulling xcoms manually pushed by the second task
        transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform")
        loaded_data = transformed_data*2
        return loaded_data

    # Define the task dependencies
    extracted = extract()
    transformed = transform()
    loaded = load()

    extracted >> transformed >> loaded
#instantiate the DAG
dag = xcom_manual_kwargs()