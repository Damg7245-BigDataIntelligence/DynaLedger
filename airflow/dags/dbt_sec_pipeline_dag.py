from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/sec_pipeline"
DBT_PROFILE_DIR = "/opt/airflow/sec_pipeline/profiles"

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 12),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='dbt_sec_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Only manual trigger
    catchup=False
) as dag:

    # Define the BashOperator task
    run_dbt_pipeline = BashOperator(
        task_id='run_dbt_pipeline',
        bash_command= f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILE_DIR} && dbt run --profiles-dir {DBT_PROFILE_DIR}",
        dag=dag,
    )

# Set task order
run_dbt_pipeline