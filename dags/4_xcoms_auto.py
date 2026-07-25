from airflow.sdk import dag, task

@dag
def xcoms_auto():
            @task
            def first_task():
                print("Extracting the data: This is the 1st Task")
                fetched_data = {"data": [1,2,3,4,5]}
                return fetched_data

            @task
            def second_task(data:dict):
                print("Transforming the data: This is the 2nd Task")
                fetched_data = data['data']
                transformed_data=fetched_data*2
                transformed_data_dict={"transf_data": transformed_data}
                return transformed_data_dict

            @task
            def third_task(data:dict):
                print("Loading the data: This is the 3rd Task")
                load_data = data
                return load_data


#Defining task dependency

            first=first_task()
            second=second_task(first)
            third=third_task(second)
            


#Initiating the DAG

xcoms_auto()
