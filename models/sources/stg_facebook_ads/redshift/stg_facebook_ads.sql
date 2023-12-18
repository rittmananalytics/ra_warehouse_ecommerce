{{config(enabled = target.type == 'redshift')}}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

{% if var("stg_facebook_ads_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_ads_table'),unique_column='id') }}
),
renamed as (

    select
    cast(id as string)              as ad_id,
    status      as ad_status,
      cast(null as string)        as ad_type,
      cast(null as string)   as ad_final_urls,
      cast(adset_id as string) as ad_group_id,
      bid_type as ad_bid_type,
      url_parameters as ad_utm_parameters,
      utm_campaign as ad_utm_campaign,
      utm_content as ad_utm_content,
      utm_medium as ad_utm_medium,
      utm_source as ad_utm_source,
      'Facebook Ads' as ad_network

    from source
)
{% elif var("stg_facebook_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_relation(var('stg_facebook_ads_segment_ads_table')) }}
),
  campaigns as (
    select
      ad_campaign_id,
      ad_campaign_name
  from {{ref ('stg_facebook_ads_campaigns') }}
),
joined as (
  select s.id,
         s.status,
         cast(null as varchar) as ad_type,
         cast(null as varchar)   as ad_final_urls,
         cast(adset_id as varchar) as adset_id,
         s.bid_type,
         s.url_parameters,
         case when s.utm_campaign like '%campaign.name%' then c.ad_campaign_name else utm_campaign end as utm_campaign,
         case when s.utm_content like '%ad.name%' then s.name else s.utm_content end as utm_content,
         s.utm_medium,
         s.utm_source
  from source s
  left join campaigns c
  on cast(s.campaign_id as varchar) = c.ad_campaign_id
),
renamed as (
SELECT
      cast(id as varchar)           as ad_id,
      status      as ad_status,
      cast(null as varchar)        as ad_type,
      cast(null as varchar)   as ad_final_urls,
      cast(adset_id as varchar) as ad_group_id,
      bid_type as ad_bid_type,
      url_parameters as ad_utm_parameters,
      utm_campaign as ad_utm_campaign,
      utm_content as ad_utm_content,
      utm_medium as ad_utm_medium,
      utm_source as ad_utm_source,
      'Facebook Ads' as ad_network
FROM
  joined)
{% endif %}
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
