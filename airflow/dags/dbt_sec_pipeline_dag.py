from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

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
        bash_command='export DBT_PROFILES_DIR=/opt/airflow/dags/DBT/.dbt && bash /opt/airflow/dags/DBT/sec_pipeline/run_dbt_pipeline.sh',
        dag=dag,
    )

# Set task order
run_dbt_pipeline
