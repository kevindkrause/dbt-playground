{% snapshot x_dim %}

{{
    config(
      target_schema='kkrause',
      strategy='check',
      unique_key='id',
      check_cols="all"
    )

}}


with final as (
    select 2 as id, current_timestamp as ts
)
select * from final

{% endsnapshot %}