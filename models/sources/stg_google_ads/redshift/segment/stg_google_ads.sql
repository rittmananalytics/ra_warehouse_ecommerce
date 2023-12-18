{{config
  (enabled =
      (target.type == 'redshift' and var("stg_google_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}


with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_ad_table')) }}
),
renamed as (
SELECT
      cast(id as varchar)          as ad_id,
      status      as ad_status,
      type        as ad_type,
      final_urls  as ad_final_urls,
      cast(ad_group_id as varchar) as ad_group_id,
      cast(null as varchar) as ad_bid_type,
      cast(null as varchar)  as ad_utm_parameters,
      cast(null as varchar)  as ad_utm_campaign,
      cast(null as varchar)  as ad_utm_content,
      cast(null as varchar)  as ad_utm_medium,
      cast(null as varchar)  as ad_utm_source,
      'Google Ads' as ad_network
FROM
  source)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
