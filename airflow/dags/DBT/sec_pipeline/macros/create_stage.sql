{% macro create_stage(stage_name) %}
  {% set full_stage_name = 'SEC_STAGE_' ~ stage_name %}
  {% set s3_url = 's3://bigdata-team3-ass2-bucket/extracted/' ~ stage_name %}

  {% set sql %}
      CREATE STAGE IF NOT EXISTS {{ full_stage_name }}
      URL = '{{ s3_url }}'
      STORAGE_INTEGRATION = sec_s3_integration
      FILE_FORMAT = parquet_format;
  {% endset %}

  -- Log the query for debugging
  {% do log("Executing SQL: " ~ sql, info=True) %}

  -- Execute the query
  {% do run_query(sql) %}
{% endmacro %}
