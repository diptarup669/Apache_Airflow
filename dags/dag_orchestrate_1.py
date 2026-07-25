from airflow.sdk import dag, task

@dag
def first_orchestrator_dag():
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

first_orchestrator_dag()

