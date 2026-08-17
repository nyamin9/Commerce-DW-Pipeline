-- 주문 헤더. 원천 1행 = 1행. 조인·집계 없음.
with source as (

    select
        order_id,
        user_id,
        status,
        gender,
        num_of_item,
        created_at,
        shipped_at,
        delivered_at,
        returned_at,
        _ingested_at
    from {{ source('raw_thelook', 'orders') }}

)

select
    order_id,
    user_id,

    -- 원천이 'Complete' / 'complete' 를 섞어 쓴다. downstream에서 매번 lower() 하지 않도록 여기서 고정.
    lower(status)                       as order_status,

    -- users.gender 가 주문 시점 값으로 비정규화되어 들어와 있다.
    -- dim_users 의 현재 성별과 다를 수 있어 이름으로 구분해 둔다.
    lower(gender)                       as user_gender_at_order,

    num_of_item                         as item_count,

    created_at                          as ordered_at,
    shipped_at,
    delivered_at,
    returned_at,

    -- 파티션 키·조인 키로 반복해서 쓰이므로 여기서 한 번만 만든다.
    date(created_at)                    as ordered_date,

    _ingested_at

from source
