from airflow.sdk import CronTriggerTimetable, dag, task, DeltaTriggerTimetable, DeltaDataIntervalTimetable
from pendulum import datetime, Duration

@dag(
    dag_id="delta_schedule_dag",
    start_date=datetime(year=2026, month=9, day=1,hour=16, tz="UTC"),
    schedule=DeltaDataIntervalTimetable(Duration(days=3)),
    end_date=datetime(year=2026, month=9, day=30, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=True
)
def delta_schedule_dag():

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
dag = delta_schedule_dag()