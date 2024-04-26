from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'raufh',
    'depends_on_past': False,
    'start_date': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'upwork_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for extracting, transforming, and loading Upwork data',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'upwork'],
) as dag:

    # Define the tasks

    # Task 1: Extract data from the source
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='python3 /home/raufhamidy/Documents/upwork_etl_pipeline/etl_scripts/extract_upwork_data.py',
        dag=dag,
    )

    # Task 2: Transform the data

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='python3 /home/raufhamidy/Documents/upwork_etl_pipeline/etl_scripts/transform_upwork_data.py',
        dag=dag,
    )

    # Task 3: Load the data to the destination

    load_data = BashOperator(
        task_id='load_data',
        bash_command='python3 /home/raufhamidy/Documents/upwork_etl_pipeline/etl_scripts/load_upwork_data.py',
        dag=dag,
    )

# Set the task dependencies
extract_data >> transform_data >> load_data