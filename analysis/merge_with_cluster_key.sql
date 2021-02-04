{# START - Determine the value list of Partition Field. #}
{# If Partition filter is provided in config block #}
    {%- set partition_filter_field = config.get('partition_filter_field') -%}
    {%- set partition_filter_source = config.get('partition_filter_source') -%}
    {% if partition_filter_source %}
        {# 1. Get Partition by information #}
        {% if partition_filter_field %}
            {%- set partition_by_field = partition_filter_field -%}
        {% else %}   
            {%- set raw_partition_by = config.get('partition_by', none) -%}
            {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
            {%- set partition_by_field = partition_by.field -%}
        {% endif %}

        {# 2. Get the view/table name to uses for Partition selection #}
        {% set partition_filter_source_ref %}
            {{ref(partition_filter_source)}}
        {% endset%}

        {# 3. Get values list for Partition field #}
        {% set value_list_sql %}
            select distinct {{partition_by_field}} FROM {{partition_filter_source_ref}}
        {% endset %}
        {%- set values = run_query(value_list_sql) -%}
        {%- set value_list = values.columns[0].values() -%}

        {# 4. Build WHERE clause #}
        {%- set partition_clause = [] -%}
        {%- for value in value_list -%}
            {%- do partition_clause.append('DBT_INTERNAL_DEST.'~ partition_by_field ~ ' = "' ~ value ~ '"') -%}
        {%- endfor -%} 

        {# 5. Add WHERE clause to predicates #}
        {% do predicates.append('( ' ~ partition_clause |join(' or ') ~ ' )') %} {# BigQuery requires ORs for multiple values in a MERGE statement #}
    {% else %}  
    {% endif %}

{# END - Determine the value list of Partition Field. #}


{{config  (
        partition_filter_field  = # This isn't mandatory, it can be derived from the model's config
        partition_filter_source = # This is mandatory for the code to be 'activated'. This can point to another model or to the current model i.e. {this}
) }}


on ( DBT_INTERNAL_DEST.PARTITION_DATE = "2021-01-06" OR DBT_INTERNAL_DEST.PARTITION_DATE = "2021-01-07" ) and 
            DBT_INTERNAL_SOURCE.UNIQUE_KEY = DBT_INTERNAL_DEST.UNIQUE_KEY
			
			
9925873			

    and tgt.order_dt between '1992-01-01' and '1992-01-03'
	
	
{#
  Fork of common_get_merge_sql from the dbt source code that adds "destination_predicate_filter" config support.
  This optimizes billing for incremental tables so we can limit the upsert searching logic to a few recent partitions.
  This is the difference of doing a fully billed table scan of the destination table, vs only being billed for reading
  a few days of the destination table.
#}
{% macro snowflake__get_merge_sql(target, source, unique_key, dest_column,sorts) %}
    {%- set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set sort_field = config.get('sort_field') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE

    {% if unique_key %}
      on
        {% if sort_field %} DBT_INTERNAL_DEST.{{sort_field}} between min_val and max_val and {% endif %}
        DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
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



{{
  config(
    materialized = "incremental",
    partition_by = "date(created_at)",
    unique_key = "id",
    destination_predicate_filter = "created_at >= timestamp_sub(current_timestamp(), interval 3 day)"
  )
}}

merge {target}
using {source}
on {target}.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
  and {target}.id = {source}.id
when not matched then update ...
when not matched then insert ...	