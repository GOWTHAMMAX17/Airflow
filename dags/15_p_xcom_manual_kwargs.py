from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id = "xcom_kwargs_p"
)
def xcom_kwargs_p():

    @task.python
    def extract(**kwargs):
        print("Extrating data...task_1")
        ti = kwargs["ti"]
        feteched_data = {"data" : [1,2,3,4,5]}
        ti.xcom_push(key="working_data", value=feteched_data)

    @task.python
    def transform(**kwargs):
        print("Transforming data...task_2")
        ti = kwargs["ti"]
        feteched_data = ti.xcom_pull(key="working_data", task_ids="extract")
        tarnsformed_data = [i*2 for i in feteched_data["data"]]
        ti.xcom_push(key="transformed_data", value = tarnsformed_data)

    @task.python
    def load(**kwargs):
        print("Loading data...task_3")
        ti = kwargs["ti"]
        transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform")
        loaded_data = transformed_data*2
        return loaded_data

    # Define the task dependencies
    extracted = extract()
    transformed = transform()
    loaded = load()


    # set the order of execution
    extracted >> transformed >> loaded

#instantiate the DAG
dag = xcom_kwargs_p()
