{% macro insert_dbt_audit_log_status( p_invocation_id, p_model_nm, p_status_val ) %}
  insert into olo.dbt_audit_log_status( invocation_id, event_model, status_val ) 
  values( '{{ invocation_id }}', '{{ model.name }}', {{ p_status_val }} )
{% endmacro %}