{% if var("crm_warehouse_contact_sources") %}

{{
    config(
        unique_key='contact_pk',
        alias='contacts_dim'
    )
}}


WITH contacts AS
  (
  SELECT *
  FROM
     {{ ref('int_contacts') }} c
)
select    {{ dbt_utils.surrogate_key(
          ['contact_name']
          ) }} as contact_pk,
          *,
          case
             when contact_non_sample_orders > 0 then 'Decorator'
             when contact_sample_orders>0 then 'Customer'
             when coalesce(contact_sample_orders+contact_non_sample_orders,0) = 0 then 'Visitor'
             end as contact_category,
          concat(case when contact_is_pro then 'Pro ' else '' end,
            case
               when contact_non_sample_orders > 0 then 'Decorator'
               when contact_sample_orders>0 then 'Customer'
               when coalesce(contact_sample_orders+contact_non_sample_orders,0) = 0 then 'Visitor'
               end) as contact_segment
          FROM
          contacts c

{% else %} {{config(enabled=false)}} {% endif %}
