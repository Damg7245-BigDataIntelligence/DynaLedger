import os
import snowflake.connector
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SnowflakeLoader:
    def __init__(self, year, quarter):
        """Initialize Snowflake connection and configurations"""
        self.year = year
        self.quarter = quarter
        self.source_id = f"{year}Q{quarter}"
        
        self.conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        self.cur = self.conn.cursor()
        self.schema_name = "SEC_DATA_RAW"
        self.s3_bucket = os.getenv('AWS_S3_BUCKET_NAME')
        self.s3_path = f'extracted/{self.source_id}/'  
        
        # AWS credentials for Snowflake integration
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
    def create_schema(self):
        """Create schema for raw SEC data"""
        try:
            # Drop schema if exists and create new one
            self.cur.execute(f"DROP SCHEMA IF EXISTS {self.schema_name}")
            self.cur.execute(f"CREATE SCHEMA {self.schema_name}")
            self.cur.execute(f"USE SCHEMA {self.schema_name}")
            logger.info(f"Created and switched to schema: {self.schema_name}")
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise

    def create_tables(self):
        """Drop and then Create the required tables in Snowflake"""
        # First drop existing tables in correct order (reverse of creation order)
        drop_statements = [
            "DROP TABLE IF EXISTS sec_num_data_raw",
            "DROP TABLE IF EXISTS sec_pre_data_raw",
            "DROP TABLE IF EXISTS sec_sub_data_raw",
            "DROP TABLE IF EXISTS sec_tag_data_raw"
        ]

        for drop_stmt in drop_statements:
            logger.info(f"Executing: {drop_stmt}")
            self.cur.execute(drop_stmt)

        tables_phase1 = {
            "sec_tag_data_raw": """
                CREATE OR REPLACE TABLE sec_tag_data_raw (
                    tag VARCHAR(256) NOT NULL,
                    version VARCHAR(20) NOT NULL,
                    custom NUMBER(1,0),  -- Changed from BOOLEAN to NUMBER for Int64
                    abstract NUMBER(1,0),  -- Changed from BOOLEAN to NUMBER for Int64
                    datatype VARCHAR(20),
                    iord CHAR(1),
                    crdr CHAR(1),
                    tlabel VARCHAR(512),
                    doc TEXT,
                    source_file VARCHAR(20),
                    PRIMARY KEY (tag, version)
                );
            """,
            "sec_sub_data_raw": """
                CREATE OR REPLACE TABLE sec_sub_data_raw (
                    adsh VARCHAR(20) NOT NULL,
                    cik NUMBER(38,0),  -- Changed from VARCHAR to NUMBER for Int64
                    name VARCHAR(150),
                    sic NUMBER(38,0),  -- Changed from VARCHAR to NUMBER for Float64
                    countryba CHAR(2),
                    stprba CHAR(2),
                    cityba VARCHAR(30),
                    zipba VARCHAR(10),
                    bas1 VARCHAR(40),
                    bas2 VARCHAR(40),
                    baph VARCHAR(20),
                    countryma CHAR(2),
                    stprma CHAR(2),
                    cityma VARCHAR(30),
                    zipma VARCHAR(10),
                    mas1 VARCHAR(40),
                    mas2 VARCHAR(40),
                    countryinc CHAR(3),
                    stprinc CHAR(2),
                    ein NUMBER(38,0),  -- Changed from VARCHAR to NUMBER for Int64
                    former VARCHAR(150),
                    changed NUMBER(38,0),  -- Changed from DATE to NUMBER for Float64
                    afs VARCHAR(5),
                    wksi NUMBER(1,0),  -- Changed from BOOLEAN to NUMBER for Int64
                    fye NUMBER(38,0),  -- Changed from CHAR to NUMBER for Float64
                    form VARCHAR(10),
                    period NUMBER(38,0),  -- Changed from DATE to NUMBER for Int64
                    fy NUMBER(38,0),  -- Changed from INTEGER to NUMBER for Float64
                    fp VARCHAR(2),
                    filed NUMBER(38,0),  -- Changed from DATE to NUMBER for Int64
                    accepted VARCHAR(30),  -- Kept as VARCHAR for timestamp string
                    prevrpt NUMBER(1,0),  -- Changed from BOOLEAN to NUMBER for Int64
                    detail NUMBER(1,0),  -- Changed from BOOLEAN to NUMBER for Int64
                    instance VARCHAR(40),
                    nciks NUMBER(38,0),  -- Changed from INTEGER to NUMBER for Int64
                    aciks VARCHAR(120),
                    source_file VARCHAR(20),
                    PRIMARY KEY (adsh)
                );
            """
        }

        tables_phase2 = {
            "sec_num_data_raw": """
                CREATE OR REPLACE TABLE sec_num_data_raw (
                    adsh VARCHAR(20) NOT NULL,
                    tag VARCHAR(256) NOT NULL,
                    version VARCHAR(20),
                    ddate NUMBER(38,0),  -- Changed from VARCHAR to NUMBER for Int64
                    qtrs NUMBER(38,0),  -- Changed from VARCHAR to NUMBER for Int64
                    uom VARCHAR(20),
                    segments VARCHAR,  -- Added for the object column
                    coreg VARCHAR(256),
                    value NUMBER(38,10),  -- For Float64
                    footnote VARCHAR(512),
                    source_file VARCHAR(20),
                    FOREIGN KEY (adsh) REFERENCES sec_sub_data_raw(adsh),
                    FOREIGN KEY (tag, version) REFERENCES sec_tag_data_raw(tag, version)
                );
            """,
            "sec_pre_data_raw": """
                CREATE OR REPLACE TABLE sec_pre_data_raw (
                    adsh VARCHAR(20) NOT NULL,
                    report NUMBER(38,0),  -- Changed from INTEGER to NUMBER for Int64
                    line NUMBER(38,0),  -- Changed from INTEGER to NUMBER for Int64
                    stmt CHAR(2),
                    inpth NUMBER(1,0),  -- Changed from BOOLEAN to NUMBER for Int64
                    rfile CHAR(1),
                    tag VARCHAR(256) NOT NULL,
                    version VARCHAR(20) NOT NULL,
                    plabel VARCHAR(512),
                    negating NUMBER(1,0),  -- Changed from BOOLEAN to NUMBER for Int64
                    source_file VARCHAR(20),
                    FOREIGN KEY (adsh) REFERENCES sec_sub_data_raw(adsh),
                    FOREIGN KEY (tag, version) REFERENCES sec_tag_data_raw(tag, version)
                );
            """
        }

        # Create tables in correct order
        for table_name, ddl in tables_phase1.items():
            logger.info(f"Creating table: {table_name}")
            self.cur.execute(ddl)

        for table_name, ddl in tables_phase2.items():
            logger.info(f"Creating table: {table_name}")
            self.cur.execute(ddl)

    def setup_s3_integration(self):
        """Setup Snowflake integration with S3"""
        # Create storage integration
        integration_command = f"""
        CREATE OR REPLACE STORAGE INTEGRATION Snowflake_AWS_OBJ
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            ENABLED = TRUE
            STORAGE_AWS_ROLE_ARN = '{os.getenv('AWS_ROLE_ARN')}'
            STORAGE_ALLOWED_LOCATIONS = ('s3://{self.s3_bucket}')
        """
        self.cur.execute(integration_command)

        # Get the AWS IAM user for the integration
        desc_integration_command = "DESC INTEGRATION Snowflake_AWS_OBJ"
        self.cur.execute(desc_integration_command)
        integration_info = self.cur.fetchall()
        print("integration_info: :",integration_info)
        
        # Log the integration info for debugging
        logger.info("Integration Info:")
        for info in integration_info:
            logger.info(str(info))

        # Create file format for Parquet
        file_format_command = """
        CREATE OR REPLACE FILE FORMAT parquet_format
            TYPE = PARQUET
            COMPRESSION = 'SNAPPY'
        """
        self.cur.execute(file_format_command)

        # Create stage
        stage_command = f"""
        CREATE OR REPLACE STAGE sec_parquet_stage
            STORAGE_INTEGRATION = Snowflake_AWS_OBJ
            URL = 's3://{self.s3_bucket}/extracted/{self.source_id}/'
            FILE_FORMAT = parquet_format
        """
        self.cur.execute(stage_command)
        logger.info(f"Stage created for {self.source_id}")

    def load_data(self):
        """Load data from S3 parquet files into Snowflake tables"""
        file_table_mapping = {
            'sub': 'sec_sub_data_raw',
            'tag': 'sec_tag_data_raw',
            'num': 'sec_num_data_raw',
            'pre': 'sec_pre_data_raw'
        }

        for file_type, table_name in file_table_mapping.items():
            logger.info(f"\nProcessing {file_type}.parquet into {table_name}")
            
            file_check = f"SELECT COUNT(*) FROM @sec_parquet_stage/{file_type}.parquet"
            try:
                self.cur.execute(file_check)
                file_count = self.cur.fetchone()[0]
                logger.info(f"File check count for {file_type}.parquet: {file_count}")
            except Exception as e:
                logger.error(f"Error checking file {file_type}.parquet: {str(e)}")
                continue
            
            copy_command = f"""
            COPY INTO {table_name}
            FROM @sec_parquet_stage/{file_type}.parquet
            FILE_FORMAT = parquet_format
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE;
            """
            
            logger.info(f"Loading data into {table_name}")
            self.cur.execute(copy_command)
            
            # Verify row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            row_count = self.cur.execute(count_query).fetchone()[0]
            logger.info(f"Loaded {row_count} rows into {table_name}")

    def cleanup(self):
        """Close connections"""
        self.cur.close()
        self.conn.close()
        logger.info("Connections closed")

def main():
    """Main execution function"""
    year, quarter = 2023, 4
    try:
        loader = SnowflakeLoader(year, quarter)
        loader.create_schema()
        loader.create_tables()
        loader.setup_s3_integration()
        loader.load_data()
        loader.cleanup()
        logger.info("Data loading completed successfully")
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main()