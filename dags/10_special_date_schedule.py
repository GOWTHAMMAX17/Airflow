from airflow.sdk import dag, task
from airflow.timetables.events import EventsTimetable
from pendulum import datetime

special_dates = EventsTimetable(
    event_dates=[
        datetime(2026,9,3),
        datetime(2026,9,8),
        datetime(2026,9,13),
        datetime(2026,9,18)
    ])
@dag(
    dag_id="special_date_schedule_dag",
    start_date=datetime(year=2026, month=9, day=1, tz="UTC"),
    end_date=datetime(year=2026, month=9, day=30, tz="UTC"),
    schedule=special_dates,
    catchup=True
)
def special_date_schedule_dag():
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

# Instantiate the DAG
dag = special_date_schedule_dag()