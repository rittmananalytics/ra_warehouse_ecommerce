{{config
  (enabled =
      (target.type == 'redshift' and var("stg_google_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_group_sources") %}

with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_ad_groups_table')) }}
),
renamed as (
  SELECT cast(id as varchar) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
         cast(campaign_id as varchar) ad_campaign_id,
         'Google Ads' as ad_network
  FROM source )
select
 *
from
 renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
