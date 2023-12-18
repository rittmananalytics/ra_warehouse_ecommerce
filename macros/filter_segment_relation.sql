{%- macro filter_segment_relation(relation) -%}

SELECT
  *
FROM
  (
    SELECT
      *,
      MAX(uuid_ts) OVER (PARTITION BY id) AS max_uuid_ts
    FROM
      {{ relation }}
  )
WHERE
  uuid_ts = max_uuid_ts
{%- endmacro -%}
