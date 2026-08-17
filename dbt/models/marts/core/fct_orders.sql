{{
    config(
        materialized='table',
        partition_by={'field': 'ordered_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['user_id']
    )
}}

-- 주문 헤더 팩트. 1행 = 주문 1건.
--
-- 원천 orders 를 그대로 옮기지 않고 상세를 집계해 함께 싣는다.
-- "주문 금액"을 물었을 때 order_items 를 매번 합산하게 하면
-- 취소·반품 제외 기준이 사람마다 달라지고 숫자가 갈라진다.
--
-- **PK 는 order_key 다.** 원천 order_id 는 재생성 시 재사용되어 유일하지 않다.
-- order_id 도 함께 싣되 퇴화 차원으로 다룬다 — 원천 대조용이지 조인 키가 아니다.

with orders as (

    select * from {{ ref('stg_thelook__orders') }}

), item_rollup as (

    select
        order_key,
        count(*)                                            as line_item_count,
        count(distinct product_id)                          as distinct_product_count,
        sum(sale_price)                                     as gross_revenue,
        sum(case when is_revenue_recognized then sale_price else 0 end)
                                                            as net_revenue,
        sum(case when is_revenue_recognized then gross_profit else 0 end)
                                                            as net_gross_profit,
        countif(order_item_status = 'returned')             as returned_item_count,
        countif(order_item_status = 'cancelled')            as cancelled_item_count
    from {{ ref('int_order_items_enriched') }}
    -- 세대가 맞는 헤더를 못 찾은 상세는 어느 주문에도 귀속시키지 않는다.
    where order_key is not null
    group by order_key

)

select
    orders.order_key,

    -- 퇴화 차원. 원천 대조용이며 조인 키로 쓰지 않는다.
    orders.order_id,

    orders.user_id,

    orders.order_status,
    orders.user_gender_at_order,

    -- 원천이 준 값과 실제 상세 행 수를 **둘 다** 싣는다.
    -- 둘이 어긋나는 주문이 있는지는 singular test 가 감시한다
    -- (tests/assert_order_item_count_matches.sql).
    orders.item_count                                       as source_item_count,
    item_rollup.line_item_count,
    item_rollup.distinct_product_count,

    item_rollup.gross_revenue,
    item_rollup.net_revenue,
    item_rollup.net_gross_profit,
    item_rollup.returned_item_count,
    item_rollup.cancelled_item_count,

    -- 객단가. 분모가 0 이 될 수 없지만 방어해 둔다.
    case
        when item_rollup.line_item_count > 0
            then round(item_rollup.net_revenue / item_rollup.line_item_count, 4)
    end                                                     as net_revenue_per_item,

    orders.ordered_at,
    orders.ordered_date,
    orders.shipped_at,
    orders.delivered_at,
    orders.returned_at

from orders
left join item_rollup
    on orders.order_key = item_rollup.order_key
