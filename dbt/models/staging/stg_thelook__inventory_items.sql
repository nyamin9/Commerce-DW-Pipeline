-- 재고 단품. 원천에 상품 속성이 비정규화되어 함께 들어 있다.
--
-- 그 비정규화 컬럼(product_name, product_brand ...)은 **여기서 끊는다.**
-- 같은 사실이 products 와 두 곳에 존재하면 어느 쪽이 맞는지 물을 때 답이 없어진다.
-- 상품 속성의 단일 출처는 stg_thelook__products 다.
with source as (

    select
        id,
        product_id,
        created_at,
        sold_at,
        cost,
        product_distribution_center_id,
        _ingested_at
    from {{ source('raw_thelook', 'inventory_items') }}

)

select
    id                                  as inventory_item_id,
    product_id,

    cast(cost as numeric)               as unit_cost,

    created_at                          as stocked_at,
    sold_at,
    date(created_at)                    as stocked_date,

    product_distribution_center_id      as distribution_center_id,

    _ingested_at

from source
