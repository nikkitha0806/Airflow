# import libraries:

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd


def first_function_execution(**context):
    print("First function execute")
    context["ti"].xcom_push(key="mykey", value="first_function_execution says Hello")


def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [{"name": "Nikkitha", "title": "Senior Data Scientist"},
            {"name": "Rohit", "title": "Senior Product Manager"}]
    df = pd.DataFrame(data)
    print(df)
    print("Second function execution {}".format(instance))



# Creating a DAG:
with DAG(
    #Keep the dag name as the file name:
    dag_id  = "first_dag",
    schedule_interval = '@daily',
    default_args ={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2022, 11, 1),
    },
    # If catchup is True then it will re-run from the old date since we have specified start date as older date
    catchup =False) as f:

    first_function_execution = PythonOperator(
        #specify the task id as function name:
        task_id = "first_function_execution",
        python_callable = first_function_execution,
        provide_context=True,
        op_kwargs = {"name": "Nikkitha!"}
    )
    second_function_execute = PythonOperator(
        #specify the task id as function name:
        task_id = "second_function_execute",
        python_callable = second_function_execute,
        provide_context=True,
    )

first_function_execution >> second_function_execute