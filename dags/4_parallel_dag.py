from airflow.sdk import dag,task

@dag(
    dag_id = "parallel_dag"
)
def parallel_dag():

    @task.python
    def extract(**kwargs):
        ti = kwargs["ti"]
        print("Extracting data...task_1")
        extractdata_dict = {"api_data":[1,2,3],
                            "db_data":[4,5,6],
                            "GCS_data":[7,8,9]}
        ti.xcom_push(key="return_value", value=extractdata_dict)

    @task.python
    def api_transform(**kwargs):
        ti = kwargs["ti"]
        print("Transforming the api data...task_2")
        api_data = ti.xcom_pull(key="return_value", task_ids="extract") ["api_data"]
        # Perform transformation on api_data
        transformed_api_data = [x * 2 for x in api_data]
        ti.xcom_push(key="return_value", value=transformed_api_data)
        return transformed_api_data

    @task.python
    def db_transform(**kwargs):
        ti = kwargs["ti"]
        print("Transforming the db_data...task_3")
        db_data = ti.xcom_pull(key="return_value", task_ids="extract")["db_data"]
        db_transform = [x*10 for x in db_data]
        ti.xcom_push(key="return_value", value=db_transform)
        return db_transform

    @task.python
    def GCS_transform(**kwargs):
        ti = kwargs["ti"]
        print("Transforming the GCS data...task_4")
        GCS_data = ti.xcom_pull(key="return_value", task_ids="extract")["GCS_data"]
        GCS_transform = [x*100 for x in GCS_data]
        ti.xcom_push(key="return_value", value=GCS_transform)
        return GCS_transform

    @task.bash
    def load(**kwargs):
        ti = kwargs["ti"]
        print("Loading data to destination...task_5")
        api_data = ti.xcom_pull(key="return_value", task_ids="api_transform")
        db_data = ti.xcom_pull(key="return_value", task_ids="db_transform")
        GCS_data = ti.xcom_pull(key="return_value", task_ids="GCS_transform")
        print(f"Loading the transformed data: {api_data}, {db_data}, {GCS_data}")
        return f"echo 'Loading the transformed data: {api_data}, {db_data}, {GCS_data}'"

    # Define the task dependencies
    extracted = extract()
    api_transformed = api_transform()
    db_transformed = db_transform()
    GCS_transformed = GCS_transform()
    loaded = load()

    # Set the order of execution
    extracted >> [api_transformed, db_transformed, GCS_transformed] >> loaded

#instantiate the DAG
dag = parallel_dag()


