{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {# If no custom schema is defined in dbt_project.yml, use the default (DEV) #}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {# If a custom schema IS defined (like 'stg' or 'marts'), use ONLY that name and ignore the default #}
    {%- else -%}

        {{ custom_schema_name | trim | upper }}

    {%- endif -%}

{%- endmacro %}
