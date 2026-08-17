-- 상품 마스터. 원천에 updated_at 이 없어 변경 이력은 snapshot 이 만든다.
-- (snapshots/snap_products.sql, strategy='check')
with source as (

    select
        id,
        name,
        brand,
        category,
        department,
        sku,
        cost,
        retail_price,
        distribution_center_id,
        _ingested_at
    from {{ source('raw_thelook', 'products') }}

)

select
    id                                  as product_id,
    name                                as product_name,
    brand                               as brand_name,
    category                            as category_name,
    department                          as department_name,
    sku,

    cast(cost as numeric)               as unit_cost,
    cast(retail_price as numeric)       as retail_price,

    distribution_center_id,

    _ingested_at

from source
