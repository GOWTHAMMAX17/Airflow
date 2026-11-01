from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="second_practice_dag",
    start_date=datetime(2026,10,31),
    schedule="@daily"
)
def second_practice_dag():

    @task.python
    def task_1():
        print("My first task")

    @task.python
    def task_2():
        print("My second task")

    @task.python
    def task_3():
        print("My third task")

    # Define the task dependencies
    first = task_1()
    second = task_2()
    third = task_3()

    # set task order of execution
    first >> second >> third

# instantiate the DAG 
dag = second_practice_dag()