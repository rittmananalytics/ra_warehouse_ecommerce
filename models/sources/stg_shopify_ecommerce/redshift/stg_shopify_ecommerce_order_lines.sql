{{config(enabled = target.type == 'redshift')}}
{% if var("ecommerce_warehouse_order_lines_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_order_lines_sources") %}

with source as (

  select * from {{ ref('shopify__order_lines') }}


),
     order_lines_tax as (

       select
              order_line_id,
              title as tax_type,
              price as tax_amount,
              rate as tax_rate
        from shopify.tax_line t
       where index =
       (select max(index) from shopify.tax_line d where d.order_line_id = t.order_line_id)

     ),
     product as (

     select
            id,
            product_type
     from shopify.product
   ),
joined as (
  SELECT
    o.*,
    p.product_type,
    t.tax_type,
    t.tax_amount,
    t.tax_rate
  FROM
    source o
  LEFT JOIN
    product p
  ON o.product_id = p.id
  LEFT JOIN
    order_lines_tax t
  ON o.order_line_id = t.order_line_id
)
,
renamed as (
    select
      fulfillable_quantity ,
      fulfillment_service ,
      fulfillment_status ,
      is_gift_card ,
      grams ,
      order_line_id ,
      index ,
      name ,
      order_id ,
      pre_tax_price,
      price,
      product_id ,
      product_type,
      property_charge_interval_frequency ,
      property_for_shipping_jan_3_rd_2020 ,
      property_shipping_interval_frequency ,
      property_shipping_interval_unit_type ,
      property_subscription_id ,
      quantity ,
      is_requiring_shipping ,
      sku ,
      is_taxable ,
      tax_type,
      tax_amount,
      tax_rate,
      title ,
      total_discount,
      variant_id ,
      vendor ,
      source_relation ,
      refunded_quantity,
      refunded_subtotal,
      quantity_net_refunds,
      subtotal_net_refunds,
      variant_created_at ,
      variant_updated_at ,
      inventory_item_id ,
      image_id ,
      variant_title ,
      variant_price,
      variant_sku ,
      variant_position ,
      variant_inventory_policy ,
      variant_compare_at_price,
      variant_fulfillment_service ,
      variant_inventory_management ,
      variant_is_taxable ,
      variant_barcode ,
      variant_grams,
      variant_inventory_quantity ,
      variant_weight,
      variant_weight_unit ,
      variant_option_1 ,
      variant_option_2 ,
      variant_option_3 ,
      variant_tax_code ,
      variant_is_requiring_shipping,
      case when lower(variant_title) like '%sample%' or lower(product_type) like '%sample box%' then 'Sample'
           when lower(variant_title) not like '%sample%' and lower(title) not like '%paint brochure%'
            and lower(variant_title) not like '%default title%' and lower(product_type) not like '%supplies%'
            and lower(product_type) not like '%service%' and product_type is not null then 'Product' end as Product_or_Sample
    from joined
),
calculated as (
  select
    *,
    CASE WHEN Product_or_Sample = 'Sample' THEN
				quantity
			END AS sample_order_quantity,
		CASE WHEN Product_or_Sample = 'Product' THEN
				quantity
			END AS product_order_quantity
  from
    renamed
)
select * from calculated

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
