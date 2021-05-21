{{
  config(
    materialized= 'incremental',
    unique_key = 'id'
  )
}}

{% if execute %}
{% set p_id = 2 %}
{% else %}
{% set p_id = 0 %}
{% endif %}

select '{{ p_id }} ' as id, '{{ project_name }}' as proj, '{{ database }}' as db, '{{ schema }}' as schema, '{{ user }}' as usr, '{{this}}' as nm, '{{ model.name }}' as model_nm,
  '{{ run_started_at.astimezone(modules.pytz.timezone("America/New_York")) }}' as run_started_est
  