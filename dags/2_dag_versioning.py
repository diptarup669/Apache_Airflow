from airflow.sdk import dag, task

@dag
def versioned_dag():
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

#Defining task dependency

            first=first_task()
            second=second_task()
            third=third_task()
            fourth=fourth_task()

            first >>second >> third >> fourth

#Initiating the DAG

versioned_dag()
