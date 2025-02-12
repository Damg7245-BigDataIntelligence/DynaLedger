{% macro copy_into_raw_pre(stage_name) %}
  {% set table_name = 'RAW_PRE_' ~ stage_name %}
  {% set stage_location = '@SEC_STAGE_' ~ stage_name ~ '/pre.parquet' %}

  {% set sql %}
      COPY INTO {{ table_name }}
      FROM {{ stage_location }}
      FILE_FORMAT = parquet_format
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = CONTINUE;
  {% endset %}

  {% do log("Executing COPY INTO: " ~ sql, info=True) %}
  {% do run_query(sql) %}
{% endmacro %}
