-- 주문 상세. 매출 금액의 원천 입도(grain)가 여기다.
--
-- 대리키를 두는 이유는 stg_thelook__orders 와 같다. 원천이 재생성되면서 id 가
-- 재사용되어 단독으로는 유일하지 않다.
--
-- **여기서 orders 의 order_key 를 만들 수는 없다.** order_key 는 헤더의 created_at 을
-- 재료로 쓰는데, 상세의 created_at 은 헤더와 다르다. 같은 세대만 놓고 재도
-- 완전 일치는 0.0%(4,179건 중 2건), 날짜만 맞춰도 80.1% 다. 상품마다 시각이
-- 조금씩 어긋나는 원천의 성질이라 재적재해도 그대로다.
-- 그래서 헤더와의 연결은 int_order_items_enriched 의 조인으로만 맺는다.
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
    -- 주문 상세의 대리키.
    {{ dbt_utils.generate_surrogate_key(['id', 'order_id', 'unix_seconds(created_at)']) }}
                                        as order_item_key,

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

    -- 적재 세대. 헤더와 조인할 때 세대를 맞추는 데 쓴다.
    date(_ingested_at)                  as _load_generation,
    _ingested_at

from source
