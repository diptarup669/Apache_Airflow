from airflow.sdk import dag, task

@dag
def parallel_tasks_dag_NEw_NEW():
            @task
            def extract_task(**kwargs):
                print("Extracting the data: This is the 1st Task")
                ti= kwargs['ti']
                extracted_data_dict = {"Api_Extracted_data": [1,2,3,4,5],
                                      "DB_Extracted_data": [6,7,8,9,10],
                                      "s3_Extracted_data": [11,12,13,14,15]}
                ti.xcom_push(key='return value', value=extracted_data_dict) 


            @task
            def transform_task_api(**kwargs):
                ti=kwargs['ti']
                api_extracted_data = ti.xcom_pull(task_ids='extract_task')['Api_Extracted_data']
                print(f"Transforming API data: {api_extracted_data}.....")                
                transformed_api_data = [i*10 for i in api_extracted_data]
                ti.xcom_push(key='return value', value=transformed_api_data) 

                
            @task
            def transform_task_db(**kwargs):
                ti=kwargs['ti']
                db_extracted_data = ti.xcom_pull(task_ids='extract_task')['DB_Extracted_data']
                print("Transforming the data: DB Extracted data: This is the 3rd Task")
                transformed_db_data = [i*100 for i in db_extracted_data]
                ti.xcom_push(key='return value',value=transformed_db_data)


            @task
            def transform_task_s3(**kwargs):
                ti=kwargs['ti']
                s3_extracted_data = ti.xcom_pull(task_ids='extract_task')['s3_Extracted_data']
                print("Transforming the data: S3 Extracted data: This is the 4th Task")
                transformed_s3_data = [i*1000 for i in s3_extracted_data]
                ti.xcom_push(key='return value', value=transformed_s3_data)


            @task.bash
            def load_task(**kwargs):
                ti=kwargs['ti']
                api_data = ti.xcom_pull(task_ids='transform_task_api')
                db_data = ti.xcom_pull(task_ids='transform_task_db')
                s3_data = ti.xcom_pull(task_ids='transform_task_s3')
                print("Loading the data: This is the 5th Task")

                return f" echo 'Loaded data: {api_data}, {db_data}, {s3_data}'"
                

#Defining task dependency

            extract=extract_task()
            transform_api=transform_task_api()
            transform_db=transform_task_db()
            transform_s3=transform_task_s3()
            load=load_task()

            extract >> [transform_api, transform_db, transform_s3] >> load


#Initiating the DAG

parallel_tasks_dag_NEw_NEW()
