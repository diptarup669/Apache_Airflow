from dag_orchestrate_1 import first_orchestrator_dag
from dag_orchestrate_2 import second_orchestrator_dag
from airflow.sdk import dag, task

@dag
def parent_dag():

    @task
    def first_orchestrator_task():
            first_orchestrator_dag()
            print("This is the 1st Task of the first orchestrator DAG")
    @task
    def second_orchestrator_task():
            second_orchestrator_dag()
            print("This is the 2nd Task of the second orchestrator DAG")

    first_orchestrator_task() >> second_orchestrator_task()

#Initiating the DAG
parent_dag()    