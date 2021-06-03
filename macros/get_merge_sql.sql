{% macro snowflake__get_merge_sql(target, source, unique_key, dest_columns,cluster_by,cluster_key_min_val,cluster_key_max_val) %}
    {%- set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set cluster_by = config.get('cluster_by') -%}
    {%- set cluster_key_min_val = config.get('cluster_key_min_val') -%}    
    {%- set cluster_key_max_val = config.get('cluster_key_max_val') -%}     
    {%- set insert_only = config.get('insert_only') -%}      

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE

    {% if unique_key %}
      on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% if cluster_by %} and DBT_INTERNAL_DEST.{{cluster_by}} between DBT_INTERNAL_SOURCE.{{cluster_key_min_val}} and DBT_INTERNAL_SOURCE.{{cluster_key_max_val}} {% endif %}
    {% else %}
        on false
    {% endif %}

    {% if unique_key or insert_only %}
    when matched then update set
        {% for column in dest_columns -%}
            {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{%- endmacro %}