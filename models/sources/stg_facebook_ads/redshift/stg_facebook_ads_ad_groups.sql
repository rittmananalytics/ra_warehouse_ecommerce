{{config(enabled = target.type == 'redshift')}}
{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_group_sources") %}

{% if var("stg_facebook_ads_etl") == 'segment' %}

with source as (
  {{ filter_segment_relation(var('stg_facebook_ads_segment_ad_groups_table')) }}
),
renamed as (
  SELECT cast(id as varchar)  as ad_group_id,
         name as ad_group_name,
         effective_status as ad_group_status,
         cast(campaign_id as varchar) ad_campaign_id,
         'Facebook Ads' as ad_network
  FROM source )

{% elif var("stg_facebook_ads_etl") == 'fivetran' %}

WITH source AS (
  	SELECT
  		id AS ad_group_id,
  		name AS ad_group_name,
  		effective_status AS ad_group_status,
  		cast(campaign_id AS varchar) ad_campaign_id,
  		'Facebook Ads' AS ad_network,
  		updated_time,
  		max(updated_time) OVER (PARTITION BY id) AS last_updated_time
  	FROM
  		{{ var('stg_facebook_ads_fivetran_ad_groups_table') }}
    ORDER BY
  			ad_group_id
  )
renamed as (
  SELECT
  	ad_group_name,
  	ad_group_status,
  	ad_campaign_id,
  	ad_network
  FROM
  	ad_groups
  WHERE
  	updated_time = last_updated_time
)
{% endif %}
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
