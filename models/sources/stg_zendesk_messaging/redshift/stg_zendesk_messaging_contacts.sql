{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contacts_sources") %}
{% if 'zendesk_messaging' in var("crm_warehouse_contacts_sources") %}

with source as (
  {{ filter_segment_relation(relation=var('stg_zendesk_messaging_fivetran_contacts_table')) }}
),
renamed as (
  SELECT
    concat('{{ var('stg_zendesk_messaging_id-prefix') }}',id) as contact_id,
    {{ dbt_utils.split_part(name,' ',1) }} as contact_first_name,
    {{ dbt_utils.split_part(name,' ',2) }} as contact_last_name,
    cast(null as varchar) as job_title,
    email  as contact_email,
    phone as contact_phone,
    cast(null as varchar) as contact_address,
    cast(null as varchar) as contact_city,
    cast(null as varchar) as contact_state,
    cast(null as varchar) as contact_country,
    cast(null as varchar) contact_postcode_zip,
    cast(null as varchar) contact_company,
    cast(null as varchar) contact_website,
    cast(null as varchar) as contact_company_id,
    cast(null as varchar) as contact_owner_id,
    cast(null as varchar) as contact_lifecycle_stage,
    cast(null as boolean) as contact_is_staff,
    state='enabled'                          as contact_is_active,
    cast(null as string) as contact_is_pro,
     cast(null as boolean) as contact_is_marketing_opt_in,
     cast(null as int) as contact_lifetime_value,
     cast(null as int) as contact_purchase_count,
     cast(null as boolean) as contact_has_verified_email,
     cast(null as boolean) as contact_accepts_marketing,
     cast(null as int) as contact_non_sample_orders,
     cast(null as int) as contact_sample_orders,
  FROM
    source)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
