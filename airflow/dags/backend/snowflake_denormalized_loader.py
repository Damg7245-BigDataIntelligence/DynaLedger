import boto3
import os
from dotenv import load_dotenv
import logging
from backend.web_scrapper import download_quarterly_data
from backend.zip_ext_and_parq_store import SECDataProcessor
import pandas as pd
import snowflake.connector

logger = logging.getLogger(__name__)
load_dotenv()

class DenormalizedProcessor:
    def __init__(self, year, quarter):
        self.year = year
        self.quarter = quarter
        self.source_id = f"{year}Q{quarter}"
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        self.extracted_prefix = 'extracted/'
        
        # Snowflake connection
        self.conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        self.cur = self.conn.cursor()

    def check_data_availability(self):
        """Check if required data exists in S3"""
        try:
            path = f"{self.extracted_prefix}{self.source_id}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=path
            )
            
            if 'Contents' in response:
                logger.info(f"Data already exists for {self.source_id}")
                return True
                
            logger.info(f"Data not found for {self.source_id}")
            return False
            
        except Exception as e:
            logger.error(f"Error checking data availability: {str(e)}")
            raise

    def ensure_data_availability(self):
        """Ensure data is available, download if necessary"""
        if not self.check_data_availability():
            logger.info(f"Downloading data for {self.source_id}")
            success = download_quarterly_data(self.year, self.quarter)
            if success:
                processor = SECDataProcessor()
                processor.extract_zip_file(self.year, self.quarter)
            else:
                raise Exception(f"Failed to download data for {self.source_id}")

    def create_analytics_schema(self):
        """Create schema for denormalized tables"""
        try:
            self.cur.execute("CREATE SCHEMA IF NOT EXISTS SEC_DATA_ANALYTICS")
            self.cur.execute("USE SCHEMA SEC_DATA_ANALYTICS")
            logger.info("Created/switched to SEC_DATA_ANALYTICS schema")
        except Exception as e:
            logger.error(f"Error creating analytics schema: {str(e)}")
            raise

    def process_denormalized_tables(self):
        """Process and create denormalized fact tables"""
        try:
            # Implementation for processing logic
            # This will involve:
            # 1. Reading parquet files from S3
            # 2. Joining and transforming data
            # 3. Creating denormalized fact tables
            # 4. Loading data into Snowflake
            pass
            
        except Exception as e:
            logger.error(f"Error processing denormalized tables: {str(e)}")
            raise

    def cleanup(self):
        """Close connections"""
        self.cur.close()
        self.conn.close()