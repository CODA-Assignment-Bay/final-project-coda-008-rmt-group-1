# =================================================

# Final Project

# Grup : 001

# Batch : CODA-RMT-008

# Python script ini digunakan untuk membuat automation ETL Directed Acyclic Graph pada airflow dimana python script ini dijadwalkan mulai dari tanggal 15 September 2025 dengan schedule dijalankan setiap hari pada akhir bulan pada tanggal 28 - 31 setiap jam 02.00 AM.

# =================================================

# Import the datetime module to handle dates and times.
import datetime as dt

# Import timedelta class specifically to define time intervals.
from datetime import timedelta

# Import the DAG class from Airflow to define a workflow.
from airflow import DAG

# Import BashOperator to run Bash commands/scripts.
from airflow.operators.bash_operator import BashOperator

# Import PythonOperator.
from airflow.operators.python_operator import PythonOperator


# Define default arguments for the DAG and its tasks.
default_args = {
    'owner': 'CODA_RMT_008_GROUP_001', # Name of the owner of this DAG.
    'start_date': dt.datetime(2025, 9, 15), # When the DAG should start scheduling.
    'retries': 1, # Number of times to retry a failed task.
    'retry_delay': dt.timedelta(minutes=600), # Time to wait between retries (600 minutes = 10 hours).
}

# Define the DAG context using a "with" block.
with DAG(
    'CODA_RMT_008_GROUP_001', # DAG ID (name).
    default_args=default_args, # Apply the default arguments defined above.
    schedule_interval='0 2 28-31 * *', # CRON schedule: At 02:00 on every day-of-month from 28 through 31.
    catchup=False, # Don't run missed DAG runs for past dates.
) as dag:

    # Define the Extract task using BashOperator.
    sdg_extract = BashOperator(
        task_id='Extract', # Task ID.
        bash_command='sudo -u airflow python /opt/airflow/scripts/sdg_project_extract.py' # Command to run.
    )

    # Define the Transform and load task using BashOperator.
    sdg_transform = BashOperator(
        task_id='Transform', # Task ID.
        bash_command='sudo -u airflow python /opt/airflow/scripts/sdg_project_transform.py' # Command to run.
    )

    # Define the Load task using BashOperator.
    sdg_load = BashOperator(
        task_id='Load', # Task ID
        bash_command='sudo -u airflow python /opt/airflow/scripts/sdg_project_load.py' # Command to run.
    )

    # Define the Load task using BashOperator.
    sdg_gx = BashOperator(
        task_id='GX', # Task ID
        bash_command='sudo -u airflow python /opt/airflow/scripts/sdg_gx.py' # Command to run.
    )

# Set task dependencies.
# Extract runs first, then Transform, then Load.
sdg_extract >> sdg_transform >> sdg_gx >> sdg_load
