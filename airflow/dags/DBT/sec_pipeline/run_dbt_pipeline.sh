#!/bin/bash

set -e  # Exit script immediately on error

# Set the working directory to where dbt_project.yml is located
cd /opt/airflow/dags/DBT/sec_pipeline/

# Explicitly set the DBT profiles directory
export DBT_PROFILES_DIR=/opt/airflow/dags/DBT/.dbt

STAGE_NAME="2023Q1"

echo -e "\n🚀 Step 0: Installing dependencies..."
dbt deps

echo -e "\n🚀 Step 1: Creating Snowflake Stage..."
dbt run-operation create_stage --args '{"stage_name": "'"$STAGE_NAME"'"}'

echo -e "\n🚀 Step 2: Running dbt models to create tables..."
dbt run --vars '{"stage_name": "'"$STAGE_NAME"'"}'

echo -e "\n🚀 Step 3: Copying data into tables..."
dbt run-operation copy_into_raw_num --args '{"stage_name": "'"$STAGE_NAME"'"}'
dbt run-operation copy_into_raw_pre --args '{"stage_name": "'"$STAGE_NAME"'"}'
dbt run-operation copy_into_raw_sub --args '{"stage_name": "'"$STAGE_NAME"'"}'
dbt run-operation copy_into_raw_tag --args '{"stage_name": "'"$STAGE_NAME"'"}'

echo -e "\n✅ Data load completed successfully!"

echo -e "\n🧪 Running dbt tests..."
dbt test --vars '{"stage_name": "'"$STAGE_NAME"'"}' --store-failures

echo -e "\n✅ All steps completed successfully!"
