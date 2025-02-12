#!/bin/bash

STAGE_NAME="2023Q1"

dbt deps

echo "🚀 Step 1: Creating Snowflake Stage"
dbt run-operation create_stage --args "{ \"stage_name\": \"$STAGE_NAME\" }"

echo "🚀 Step 2: Running dbt models to create tables"
dbt run --vars "{ \"stage_name\": \"$STAGE_NAME\" }"

echo "🚀 Step 3: Copying data into tables"
dbt run-operation copy_into_raw_num --args "{ \"stage_name\": \"$STAGE_NAME\" }"
dbt run-operation copy_into_raw_pre --args "{ \"stage_name\": \"$STAGE_NAME\" }"
dbt run-operation copy_into_raw_sub --args "{ \"stage_name\": \"$STAGE_NAME\" }"
dbt run-operation copy_into_raw_tag --args "{ \"stage_name\": \"$STAGE_NAME\" }"

echo "✅ Pipeline execution completed successfully!"

dbt test --vars "{ \"stage_name\": \"$STAGE_NAME\" }" --store-failures


