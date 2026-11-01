from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="first_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily"
)
def first_dag():

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
dag = first_dag()