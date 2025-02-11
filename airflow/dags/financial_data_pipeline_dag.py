from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sys
import os
from airflow.exceptions import AirflowException
from airflow.utils.state import State
import logging

logger = logging.getLogger(__name__)

# Add the backend directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../../backend'))

from backend.web_scrapper import download_quarterly_data
from backend.zip_ext_and_parq_store import SECDataProcessor
from backend.snowflake_raw_data_loader import SnowflakeLoader

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_processing_period():
    """Get the year and quarter from Airflow variables"""
    year = Variable.get("sec_data_year", default_var=2023)
    quarter = Variable.get("sec_data_quarter", default_var=4)
    return int(year), int(quarter)

def web_scraping_task(**context):
    """Task to download data from SEC website"""
    task_instance = context['task_instance']
    try:
        logger.info("Starting web scraping task")
        year, quarter = get_processing_period(**context)
        
        # Store these values for tracking
        task_instance.xcom_push(key='processing_year', value=year)
        task_instance.xcom_push(key='processing_quarter', value=quarter)
        
        success = download_quarterly_data(year, quarter)
        if not success:
            error_msg = f"Failed to download data for {year} Q{quarter}"
            logger.error(error_msg)
            task_instance.xcom_push(key='failure_reason', value=error_msg)
            raise AirflowException(error_msg)
            
        return f"Downloaded data for {year} Q{quarter}"
        
    except Exception as e:
        error_msg = f"Web scraping failed: {str(e)}"
        logger.error(error_msg)
        task_instance.xcom_push(key='failure_reason', value=error_msg)
        raise

def parquet_processing_task(**context):
    """Task to process ZIP files and convert to Parquet"""
    task_instance = context['task_instance']
    try:
        logger.info("Starting parquet processing")
        year, quarter = get_processing_period(**context)
        
        # Log progress steps
        logger.info("Initializing processor")
        processor = SECDataProcessor()
        
        logger.info("Starting file extraction")
        processor.extract_zip_file(year, quarter)
        
        return f"Processed data for {year} Q{quarter}"
        
    except Exception as e:
        error_msg = f"Parquet processing failed: {str(e)}"
        logger.error(error_msg)
        task_instance.xcom_push(key='failure_reason', value=error_msg)
        raise

def snowflake_loading_task(**context):
    """Task to load data into Snowflake"""
    task_instance = context['task_instance']
    try:
        logger.info("Starting Snowflake loading")
        year, quarter = get_processing_period(**context)
        loader = SnowflakeLoader(year, quarter)
        
        steps = {
            'create_schema': 'Creating schema',
            'create_tables': 'Creating tables',
            'setup_s3': 'Setting up S3 integration',
            'load_data': 'Loading data'
        }
        
        current_step = 'initialization'
        task_instance.xcom_push(key='current_step', value=current_step)
        
        for step, message in steps.items():
            current_step = step
            task_instance.xcom_push(key='current_step', value=current_step)
            logger.info(f"Step: {message}")
            getattr(loader, step)()
        
        loader.cleanup()
        return f"Loaded data for {year} Q{quarter}"
        
    except Exception as e:
        error_msg = f"Snowflake loading failed at step '{current_step}': {str(e)}"
        logger.error(error_msg)
        task_instance.xcom_push(key='failure_reason', value=error_msg)
        loader.cleanup()
        raise

# Create the DAG
dag = DAG(
    'sec_financial_data_pipeline',
    default_args=default_args,
    description='Pipeline to process SEC financial statement data',
    schedule_interval=None,  # Set to None for manual triggers
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sec', 'financial', 'data'],
)

# Define tasks
web_scrape = PythonOperator(
    task_id='web_scraping',
    python_callable=web_scraping_task,
    provide_context=True,
    dag=dag,
)

process_parquet = PythonOperator(
    task_id='parquet_processing',
    python_callable=parquet_processing_task,
    provide_context=True,
    dag=dag,
)

load_snowflake = PythonOperator(
    task_id='snowflake_loading',
    python_callable=snowflake_loading_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
web_scrape >> process_parquet >> load_snowflake