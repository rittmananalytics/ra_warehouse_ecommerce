{{config
  (enabled =
      (target.type == 'redshift' and var("stg_google_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_campaign_sources") %}
{{
    config(
      alias='ad_performance_fact'
    )
}}

WITH
ad_performance AS
  (
  SELECT * from {{ ref('int_ad_performance') }}
),
 ads as (
   SELECT * from {{ ref('wh_ads_dim') }}
),
ad_groups as (
  SELECT * from {{ ref('wh_ad_groups_dim') }}
),
ad_performance_joined as (
select {{ dbt_utils.surrogate_key(['s.ad_pk','c.ad_serve_ts','g.ad_campaign_id']) }} as ad_performance_pk,
       s.ad_pk,
       s.ad_id,
       g.ad_group_id,
       s.ad_utm_source as utm_source,
       s.ad_utm_campaign as utm_campaign,
       s.ad_utm_content as utm_content,
       g.ad_campaign_id as campaign_id,
       c.*
from ad_performance c
left outer join ads s
on c.ad_id = s.ad_id
join ad_groups g
on s.ad_group_id = g.ad_group_id
)
,
web_events as (
  SELECT * from {{ ref('wh_web_events_fact') }}
),
segment_clicks AS (
SELECT
  utm_source,
  utm_campaign,
  campaign_date,
  SUM(total_clicks) AS total_clicks
FROM (
  SELECT
    LOWER(utm_source) as utm_source,
    LOWER(utm_campaign) AS utm_campaign,
    utm_term,
    {{ dbt_utils.date_trunc('DAY', 'event_ts') }} AS campaign_date,

    blended_user_id,

    COUNT(web_event_pk) AS total_clicks
  FROM
    web_events
  WHERE
    utm_source IN ('adwords',
      'facebook','instagram')
  {{ dbt_utils.group_by(n=5) }}
    )
GROUP BY
  1,2,3),
  ad_network_clicks AS (
  SELECT
    ad_pk,
    lower(utm_source) as utm_source,
    lower(utm_campaign) as utm_campaign,
    campaign_date,
    SUM(total_reported_cost) AS total_reported_cost,
    AVG(avg_reported_time_on_site) AS avg_reported_time_on_site,
    AVG(avg_reported_bounce_rate) AS avg_reported_bounce_rate,
    SUM(total_reported_clicks) AS total_reported_clicks,
    SUM(total_reported_impressions) AS total_reported_impressions
  FROM (
    SELECT
      {{ dbt_utils.date_trunc('DAY','ad_serve_ts') }} AS campaign_date,
      ad_pk,
      utm_source,
      utm_campaign,
      ad_total_cost AS total_reported_cost,
      ad_avg_time_on_site AS avg_reported_time_on_site,
      ad_bounce_rate AS avg_reported_bounce_rate,
      ad_total_clicks AS total_reported_clicks,
      ad_total_impressions AS total_reported_impressions
  FROM
      ad_performance_joined)
  GROUP BY
    1,2,3,4),
 joined as (
SELECT
  {{ dbt_utils.surrogate_key(['a.ad_pk','a.campaign_date']) }} as ad_performance_pk,
  a.*,
  coalesce(s.total_clicks,0) as total_clicks,
  {{ safe_divide('s.total_clicks','A.total_reported_clicks') }} AS actual_vs_reported_clicks_pct,
  {{ safe_divide('a.total_reported_cost','a.total_reported_clicks') }} as reported_cpc,
  {{ safe_divide('a.total_reported_cost','s.total_clicks') }}   AS actual_cpc,
  {{ safe_divide('a.total_reported_clicks','a.total_reported_impressions') }}   AS reported_ctr,
  {{ safe_divide('s.total_clicks','a.total_reported_impressions') }}   AS actual_ctr,
  {{ safe_divide('a.total_reported_cost*1000','a.total_reported_impressions') }}   AS reported_cpm
FROM
  ad_network_clicks a
LEFT JOIN
  segment_clicks s
ON
  s.utm_source = a.utm_source
  AND s.utm_campaign = a.utm_campaign
  AND s.campaign_date = a.campaign_date
)
select * from joined
where utm_campaign is not null
{% endif %}
