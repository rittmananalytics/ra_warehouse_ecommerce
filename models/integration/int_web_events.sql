{% if var('product_warehouse_event_sources') %}

with events_merge_list as
  (
    {% for source in var('product_warehouse_event_sources') %}

      {% set relation_source = 'stg_' + source + '_events' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )


select
  e.*


from events_merge_list e

{% if var("enable_event_type_mapping")   %}
left outer join
  {{ ref('event_mapping_list') }} m
on
  e.event_type = m.event_type_original
{% endif %}

{% else %}

{{config(enabled=false)}}

{% endif %}
