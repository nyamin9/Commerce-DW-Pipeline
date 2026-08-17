-- 주문 상세. 매출 금액의 원천 입도(grain)가 여기다.
with source as (

    select
        id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status,
        sale_price,
        created_at,
        shipped_at,
        delivered_at,
        returned_at,
        _ingested_at
    from {{ source('raw_thelook', 'order_items') }}

)

select
    id                                  as order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,

    lower(status)                       as order_item_status,

    -- FLOAT64 로 들어온 금액을 NUMERIC 으로 고정한다.
    -- 부동소수 누적 오차로 합계가 어긋나는 것을 staging 에서 끊는다.
    cast(sale_price as numeric)         as sale_price,

    created_at                          as ordered_at,
    shipped_at,
    delivered_at,
    returned_at,
    date(created_at)                    as ordered_date,

    _ingested_at

from source
