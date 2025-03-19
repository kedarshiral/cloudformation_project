from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import time

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_dag',  # DAG ID
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,  # Disable catchup to avoid backfilling
)

# Task 1: Print a message
def print_message():
    print("Hello, this is Task 1!")

task1 = PythonOperator(
    task_id='print_message_task',
    python_callable=print_message,
    dag=dag,
)

# Task 2: Wait for 5 seconds and print another message
def wait_and_print():
    time.sleep(5)  # Wait for 5 seconds
    print("Task 2 is done!")

task2 = PythonOperator(
    task_id='wait_and_print_task',
    python_callable=wait_and_print,
    dag=dag,
)

# Task 3: Run a Bash command
task3 = BashOperator(
    task_id='bash_command_task',
    bash_command='echo "This is a Bash command!"',
    dag=dag,
)

# Define task dependencies
task2.set_upstream(task1)
task3.set_upstream(task2)