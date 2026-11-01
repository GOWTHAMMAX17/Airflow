from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="schedule_dag",
    start_date=datetime(year=2026, month=9, day=1, tz="UTC"),
    schedule="@daily",
    is_paused_upon_creation=False,
    catchup=True
)
def schedule_dag():

    @task.python
    def task_1():
        print("My first task")

    @task.python
    def task_2():
        print("My second task")

    @task.python
    def task_3():
        print("My third task. DAG is working fine!")

    # Define the task dependencies  
    first = task_1()
    second = task_2()
    third = task_3()

    # Set the order of execution
    first >> second >> third 

#instantiate the DAG
dag = schedule_dag()