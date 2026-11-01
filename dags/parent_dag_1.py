from airflow.sdk import dag, task
from child_dag_1 import first_orchestration_dag
from child_dag_2 import second_orchestration_dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="parent_dag",
)
def parent_dag():
    @task
    def trigger_child_dag_1():
        trigger = TriggerDagRunOperator(
            task_id="trigger_child_dag_1",
            trigger_dag_id="first_orchestration_dag",
            wait_for_completion=True,
        )
        trigger.execute(context={})

    @task
    def trigger_child_dag_2():
        trigger = TriggerDagRunOperator(
            task_id="trigger_child_dag_2",
            trigger_dag_id="second_orchestration_dag",
            wait_for_completion=True,
        )
        trigger.execute(context={})

    # Define the task dependencies
    trigger_child_dag_1() >> trigger_child_dag_2()

#instantiate the DAG
dag = parent_dag()