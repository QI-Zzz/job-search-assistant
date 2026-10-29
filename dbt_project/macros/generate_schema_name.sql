-- By default dbt prefixes custom schemas with the target schema
-- (+schema: mart -> "staging_mart"). Use the custom schema name as-is
-- so models land in the staging / mart schemas created in db/02.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
