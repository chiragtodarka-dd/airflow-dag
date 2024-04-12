from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the function to print 'hello world' in the logs
def print_hello():
    print("Hello World")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 11),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple DAG that prints Hello World every minute',
    schedule_interval=timedelta(minutes=1)
)

# Define the task that prints 'hello world'
print_hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag
)

# Set task dependencies
print_hello_task
