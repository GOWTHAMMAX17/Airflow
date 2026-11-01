from airflow.sdk import dag, task, CronDataIntervalTimetable
from pendulum import datetime

@dag(
    dag_id = "incremental_load_dag",
    start_date=datetime(year=2026, month=9, day=14, tz="UTC"),
    end_date=datetime(year=2026, month=9, day=17, tz="UTC"),
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    catchup=True
)
def incremental_load_dag():
    @task.python
    def incremental_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_process():
        return "echo 'Processing the fetched data from{{data_interval_start}} to {{data_interval_end}}'"

    # Define the task dependencies  
    fetch = incremental_fetch() 
    process = incremental_process()

    # Set the order of execution
    fetch >> process

#instantiate the DAG
dag = incremental_load_dag()