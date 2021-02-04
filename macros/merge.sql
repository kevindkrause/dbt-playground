{#
  Fork of common_get_merge_sql from the dbt source code that adds "destination_predicate_filter" config support.
  This optimizes billing for incremental tables so we can limit the upsert searching logic to a few recent partitions.
  This is the difference of doing a fully billed table scan of the destination table, vs only being billed for reading
  a few days of the destination table.
#}
{% macro snowflake__get_merge_sql(target, source, unique_key, dest_columns,cluster_by,cluster_key_min_val,cluster_key_max_val) %}
    {%- set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set cluster_by = config.get('cluster_by') -%}
    {%- set cluster_key_min_val = config.get('cluster_key_min_val') -%}    
    {%- set cluster_key_max_val = config.get('cluster_key_max_val') -%}     

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE

    {% if unique_key %}
      on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% if cluster_by %} and DBT_INTERNAL_DEST.{{cluster_by}} between DBT_INTERNAL_SOURCE.{{cluster_key_min_val}} and DBT_INTERNAL_SOURCE.{{cluster_key_max_val}} {% endif %}
    {% else %}
        on FALSE
    {% endif %}

    {% if unique_key %}
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