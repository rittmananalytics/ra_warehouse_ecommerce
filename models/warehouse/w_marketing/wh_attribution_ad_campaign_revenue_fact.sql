{{config
  (enabled =
      (target.type == 'redshift' and var("stg_google_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_campaign_sources") %}
{{
    config(
      alias='attribution_ad_campaign_revenue_fact'
    )
}}

SELECT date(f.session_start_ts) as session_date,
        f.utm_content as ad_id,
        g.ad_network,
        c.ad_campaign_pk,
        sum(f.first_click_attrib_conversions) as total_first_click_attrib_conversions,
        sum(f.last_click_attrib_conversions) as total_last_click_attrib_conversions,
        sum(f.even_click_attrib_conversions) as total_even_click_attrib_conversions,
        sum(f.time_decay_attrib_conversions) as total_time_decay_attrib_conversions
        FROM {{ ref('wh_attribution_fact') }} f
LEFT OUTER JOIN {{ ref('wh_ads_dim') }} a
ON f.utm_content = a.ad_id
left outer join {{ ref('wh_ad_groups_dim') }} g
on a.ad_group_id = g.ad_group_id
left outer join {{ ref('wh_ad_campaigns_dim') }} c
on g.ad_campaign_id = c.ad_campaign_id
        where g.ad_campaign_id is not null
group by 1,2,3,4
{% endif %}
