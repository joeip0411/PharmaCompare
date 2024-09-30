{% macro create_dev_schema(source_schema) %}
    {%- set snowflake_user = env_var('SNOWFLAKE_USER') -%}

    {% if not snowflake_user %}
        {%- do exceptions.raise_compiler_error("Environment variable 'SNOWFLAKE_USER' is not set.") -%}
    {% endif %}

    {%- set target_schema = snowflake_user | upper -%}

    {% set clone_sql %}
        CREATE OR REPLACE SCHEMA {{ target_schema }} CLONE {{ source_schema }};
    {% endset %}

    -- Execute the SQL command to clone the schema
    {% do run_query(clone_sql) %}

    -- Return a message indicating success
    {{ return("Schema cloned to " ~ target_schema) }}
{% endmacro %}