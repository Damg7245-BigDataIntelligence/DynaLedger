import os
import snowflake.connector
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Snowflake connection details
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SCHEMA_NAME = os.getenv('SCHEMA_NAME')

# AWS S3 and IAM details
AWS_ROLE_ARN = os.getenv('AWS_ROLE_ARN')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')

# Ensure environment variables are loaded correctly
if not AWS_S3_BUCKET_NAME:
    raise ValueError("❌ ERROR: AWS_S3_BUCKET_NAME is not set. Check your .env file.")

# Construct the correct S3 path
S3_PATH = f"s3://{AWS_S3_BUCKET_NAME}/JSON_conversion/"

logger.debug(f"✅ Verified S3 Path: {S3_PATH}")

# Snowflake SQL Commands
USE_SCHEMA_SQL = f"USE SCHEMA {SCHEMA_NAME};"

CREATE_TABLE_SQL = f"""
CREATE OR REPLACE TABLE {SCHEMA_NAME}.sec_json_table (
    symbol STRING,                         -- Extracted from JSON
    company_name STRING,                   -- Extracted from JSON
    year INT,                               -- Extracted from JSON
    quarter STRING,                         -- Extracted from JSON
    start_date DATE,                        -- Extracted from JSON
    end_date DATE,                          -- Extracted from JSON
    raw_json VARIANT                        -- Full JSON storage
);
"""

# ✅ **Step 1: Remove Old Data Before Loading New Data**
TRUNCATE_TABLE_SQL = f"""
TRUNCATE TABLE {SCHEMA_NAME}.sec_json_table;
"""

# ✅ **Step 2: Load Data from S3 into Staging Table**
COPY_INTO_SQL = f"""
COPY INTO {SCHEMA_NAME}.sec_json_table (raw_json)
FROM @sec_json_stage
FILE_FORMAT = (FORMAT_NAME = 'json_file_format')
PATTERN = '.*\\.json';
"""

# ✅ **Step 3: Extract Fields from JSON into Structured Columns**
UPDATE_TABLE_SQL = f"""
UPDATE {SCHEMA_NAME}.sec_json_table
SET 
    symbol = raw_json:symbol::STRING,
    company_name = raw_json:name::STRING,
    year = raw_json:year::INT,
    quarter = raw_json:quarter::STRING,
    start_date = raw_json:startDate::DATE,
    end_date = raw_json:endDate::DATE;
"""

# ✅ **Step 4: Merge Data to Avoid Duplicates**
MERGE_SQL = f"""
MERGE INTO {SCHEMA_NAME}.sec_json_table AS target
USING (
    SELECT 
        raw_json, 
        raw_json:symbol::STRING AS symbol,
        raw_json:name::STRING AS company_name,
        raw_json:year::INT AS year,
        raw_json:quarter::STRING AS quarter,
        raw_json:startDate::DATE AS start_date,
        raw_json:endDate::DATE AS end_date
    FROM {SCHEMA_NAME}.sec_json_table
) AS source
ON target.symbol = source.symbol
WHEN MATCHED THEN 
    UPDATE SET 
        target.raw_json = source.raw_json,
        target.company_name = source.company_name,
        target.year = source.year,
        target.quarter = source.quarter,
        target.start_date = source.start_date,
        target.end_date = source.end_date
WHEN NOT MATCHED THEN 
    INSERT (symbol, company_name, year, quarter, start_date, end_date, raw_json)
    VALUES (source.symbol, source.company_name, source.year, source.quarter, source.start_date, source.end_date, source.raw_json);
"""

def load_json_data_to_snowflake():
    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE
        )
        cs = ctx.cursor()
        logger.info("✅ Successfully connected to Snowflake.")

        # ✅ Ensure schema is selected
        logger.info("🔹 Setting active schema...")
        cs.execute(USE_SCHEMA_SQL)

        # ✅ Create Table
        logger.info("🔹 Creating table with structured columns...")
        cs.execute(CREATE_TABLE_SQL)

        # ✅ Truncate Table to Remove Old Data
        logger.info("🔹 Truncating table to remove old records before loading new data...")
        cs.execute(TRUNCATE_TABLE_SQL)

        # ✅ Copy JSON Data from S3 to Snowflake
        logger.info("🔹 Copying JSON data into Snowflake table...")
        cs.execute(COPY_INTO_SQL)

        # ✅ Update Structured Columns from JSON
        logger.info("🔹 Extracting JSON fields into structured columns...")
        cs.execute(UPDATE_TABLE_SQL)

        # ✅ Merge Data to Avoid Duplicates
        logger.info("🔹 Merging new data with existing records to avoid duplicates...")
        cs.execute(MERGE_SQL)

        # ✅ Check number of rows loaded
        cs.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.sec_json_table;")
        row_count = cs.fetchone()[0]
        logger.info(f"✅ Data load completed. Total rows loaded: {row_count}")

    except Exception as e:
        logger.error(f"❌ Error loading JSON data into Snowflake: {e}", exc_info=True)
    finally:
        cs.close()
        ctx.close()

if __name__ == "__main__":
    load_json_data_to_snowflake()
