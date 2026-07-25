from airflow.sdk import dag, task

@dag
def xcoms_mannual_kwargs():
            @task
            #defining the first task using KWARGS
            def first_task(**kwargs):
                ti = kwargs['ti']
                print("Extracting the data: This is the 1st Task")
                fetched_data = {"data": [1,2,3,4,5]}
                ti.xcom_push(key='return_result', value=fetched_data)
                #return fetched_data ----> You dont need to return the data as we are using XCOMS to pass the data to the next task

            @task
            def second_task(**kwargs):
                #print("Transforming the data: This is the 2nd Task")
                #fetched_data = data['data']
                #declaring the ti variable to use XCOMS to pass the data to the next task
                ti = kwargs['ti']
                fetched_data =ti.xcom_pull(task_ids='first_task', key='return_result')['data'] # Pulling the data from the first task using XCOMS
                transformed_data=fetched_data*2
                transformed_data_dict={"transf_data": transformed_data}
                #return transformed_data_dict ----> You dont need to return the data as we are using XCOMS to pass the data to the next task
                #return transformed_data_dict
                ti.xcom_push(key='return_result' , value=transformed_data_dict) # Again we are using XCOMS to pass the data to the next task

            @task
            def third_task(**kwargs):
                ti = kwargs['ti']
                print("Loading the data: This is the 3rd Task")
                load_data = ti.xcom_pull(task_ids='second_task', key='return_result') # Loading the data from the second task using XCOMS
                return load_data


#Defining task dependency

            first=first_task()
            second=second_task()
            third=third_task()
            
            first >> second >> third
#Initiating the DAG
xcoms_mannual_kwargs()