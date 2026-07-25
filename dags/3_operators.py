from airflow.sdk import dag, task
import airflow.operators.bash as bash
from airflow.providers.standard.operators.bash import BashOperator

@dag
def operators_dag():
            @task
            def first_task():
                print("This is the 1st Task")
            @task
            def second_task():
                print("This is the 2nd Task")
            @task
            def third_task():
                print("This is the 3rd Task")
            @task
            def fourth_task():
                print("This is the DAG version 2.0") 

            @task.bash
            def bash_task_modern() -> str:
                return "echo https://airflow.apache.org/"

            bash_task_oldSchool = BashOperator(
            task_id="bash_task_oldSchool",
            bash_command="echo https://airflow.apache.org/",
)   
#Defining task dependency

            first=first_task()
            second=second_task()
            third=third_task()
            fourth=fourth_task()
            bash_modern=bash_task_modern()
            bash_old=bash_task_oldSchool

            first >>second >> third >> fourth >> bash_modern >> bash_old

#Initiating the DAG

operators_dag()
