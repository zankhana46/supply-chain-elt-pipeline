/*
    By default, dbt creates schema names like: <default_schema>_<custom_schema>
    So we get "marts_staging" instead of just "staging".
    
    This macro overrides that behavior:
    - If a custom schema is set (like +schema: staging), use ONLY that name
    - If no custom schema is set, fall back to the default from profiles.yml
    
    This is one of the most common dbt customizations in production.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}