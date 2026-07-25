from airflow.sdk import dag, task
from pendulum import datetime,duration
from airflow.timetables.events import EventsTimetable


special_dates=EventsTimetable(
    event_dates=[
            datetime(2026,7,1),
            datetime(2026,7,4),
            datetime(2026,7,7),
            datetime(2026,7,10)   
                 ])

@dag(
        schedule=special_dates,
        start_date=datetime(year=2026, month=7, day=1,tz="UTC"),
        end_date=datetime(year=2026, month=7, day=22,tz="UTC"),
        catchup=True
   )

def special_dates_dag():
    
    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"This is a special event task for the date: {execution_date}")
        
    special_event = special_event_task()


#Initiating the DAG
special_dates_dag()
