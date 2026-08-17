-- 주문 헤더. 원천 1행 = 1행. 조인·집계 없음.
--
-- ── 대리키를 두는 이유 ──────────────────────────────────────────────────
-- 원천 PK 단독으로는 유일하지 않다. `bigquery-public-data.thelook_ecommerce` 는
-- 계속 재생성되는 합성 데이터셋이고, **재생성될 때 ID 가 재사용된다.**
-- 옛 세대에서 2023년 주문에 붙어 있던 order_id 가 새 세대에서는 다른 주문에 붙는다.
-- partition_overwrite 는 lookback 구간 밖을 건드리지 않으므로 두 세대가 공존하고
-- 그 순간 order_id 의 유일성이 깨진다. (docs/incidents/ 참조)
--
-- 그래서 사건을 실제로 특정하는 조합으로 키를 만든다.
--   order_id  주문 식별자
--   user_id   누구의 주문인가
--   created_at 언제 발생한 사건인가 — 세대가 갈리는 지점이 여기다
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
    -- 주문의 대리키(surrogate key). downstream 은 이것을 PK 로 쓴다.
    {{ dbt_utils.generate_surrogate_key(['order_id', 'user_id', 'unix_seconds(created_at)']) }}
                                        as order_key,

    -- 원천 식별자는 그대로 남긴다. 추적과 원천 대조에 필요하다.
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

    -- 적재 세대. 같은 order_id 라도 세대가 다르면 다른 사건이다.
    -- order_items 와의 조인이 세대를 넘지 않게 하는 데 쓴다.
    date(_ingested_at)                  as _load_generation,
    _ingested_at

from source
