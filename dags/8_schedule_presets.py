from airflow.sdk import dag, task
from pendulum import datetime
@dag(
        #start_date defines the start date of the DAG, schedule defines the frequency of the DAG execution, is_paused_upon_creation defines whether the DAG should be paused upon creation or not
        #Created by Diptarup
        dag_id="first_Schedular_dag",
        start_date=datetime(year=2026, month=1, day=1,tz="UTC"),
        schedule="@daily",
        is_paused_upon_creation=False    
)
def first_scheduler_dag():
            @task
            def first_task():
                print("This is the 1st Task")
            @task
            def second_task():
                print("This is the 2nd Task")
            @task
            def third_task():
                print("This is the 3rd Task")

#Defining task dependency

            first=first_task()
            second=second_task()
            third=third_task()

            first >>second >> third

#Initiating the DAG

first_scheduler_dag()
