from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sys
import os
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

sys.path.append(os.path.join(os.path.dirname(__file__), '../../backend'))
from backend.snowflake_denormalized_loader import DenormalizedProcessor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_denormalized_data(**context):
    """Task to process denormalized fact tables"""
    task_instance = context['task_instance']
    try:
        year = int(Variable.get("sec_data_year"))
        quarter = int(Variable.get("sec_data_quarter"))
        
        processor = DenormalizedProcessor(year, quarter)
        processor.ensure_data_availability()
        processor.create_analytics_schema()
        processor.process_denormalized_tables()
        processor.cleanup()
        
        return f"Processed denormalized data for {year} Q{quarter}"
        
    except Exception as e:
        error_msg = f"Denormalized processing failed: {str(e)}"
        logger.error(error_msg)
        task_instance.xcom_push(key='failure_reason', value=error_msg)
        raise

dag = DAG(
    'sec_financial_denormalized_pipeline',
    default_args=default_args,
    description='Pipeline to create denormalized fact tables from SEC data',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sec', 'financial', 'analytics'],
)

process_denormalized = PythonOperator(
    task_id='process_denormalized_data',
    python_callable=process_denormalized_data,
    provide_context=True,
    dag=dag,
)

process_denormalized