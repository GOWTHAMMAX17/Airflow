from airflow.sdk import dag, task
from datetime import datetime
import os
@dag(
    dag_id="second_orchestration_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily"
)
def second_orchestration_dag():

    @task.python
    def task_1():
        print("My first task")

    @task.python
    def task_2():
        print("My second task")

    @task.python
    def task_3():
       def task_3():
               os.makedirs(os.path.dirname("/opt/airflow/logs/data/child_file_2.txt"), exist_ok=True)
               # Write data to the file
               with open("/opt/airflow/logs/data/child_file_2.txt", "w") as f:
                       f.write("This is a sample child 2 data.")
               print(f"Data extract written to /opt/airflow/logs/data/child_file_2.txt")

    # Define the task dependencies  
    first = task_1()
    second = task_2()
    third = task_3()

    # Set the order of execution
    first >> second >> third 

#instantiate the DAG
dag = second_orchestration_dag()