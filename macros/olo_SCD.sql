{% macro build_snapshot_table_olo(strategy,sql) %}
    select *,
        to_date({{ strategy.updated_at }}) as start_dt,
        to_date({{ strategy.updated_at }}) as end_dt
    from (
        {{sql}}
    ) sbq

{% endmacro %}