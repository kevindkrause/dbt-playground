{{
  config(
    materialized= 'incremental',
    unique_key = 'id',
    pre_hook = after_commit( "insert into olo.dbt_audit_log_status( invocation_id, event_model, status_val ) values( '{{ invocation_id }}', '{{ model.name }}', 0 )" ),
    post_hook = "{{ insert_dbt_audit_log_status('invocation_id', 'model.name', 1 ) }}"
  )
}}

{% set p_id = 5 %}

select '{{ p_id }} ' as id, '{{ invocation_id }}' as err_nm, 'special err' as err_type, 10/0 as val