from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable
@dag(
        #start_date defines the start date of the DAG, schedule defines the frequency of the DAG execution, is_paused_upon_creation defines whether the DAG should be paused upon creation or not
        #Created by Diptarup
        dag_id="cron_Schedular_dag",
        start_date=datetime(year=2026, month=7, day=18,tz="UTC"),
        schedule=CronTriggerTimetable("0 16 * * MON-FRI", timezone="UTC"), #This is a cron expression which means the DAG will run at 4 PM from Monday to Friday
        end_date=datetime(year=2026, month=7, day=22,tz="UTC"),
        is_paused_upon_creation=False,
        catchup=True
)
def cron_scheduler_dag():
            @task
            def first_task():
                print("This is the             1st Task")
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

cron_scheduler_dag()
