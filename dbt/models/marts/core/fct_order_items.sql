{{
    config(
        materialized='table',
        partition_by={'field': 'ordered_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['product_id', 'user_id']
    )
}}

-- **매출의 기준 입도(grain)는 주문 상세 1행이다.**
-- fct_orders 와 둘 다 두는 이유는 입도가 다르기 때문이다.
-- 상품별·카테고리별 매출은 여기서만 답할 수 있고,
-- 주문 건수·객단가는 fct_orders 에서만 답할 수 있다.
--
-- 증분으로 만들지 않았다. 180K행이라 전체 재생성이 초 단위로 끝나고,
-- 증분은 복잡도를 얹는 대가로만 정당화된다. 커지면 그때 옮긴다.

select
    -- **PK 는 대리키다.** 원천 id 는 재생성 시 재사용되어 유일하지 않다
    -- (docs/incidents/ 참조). staging 에서 만든 것을 그대로 들고 온다.
    order_item_key,

    -- 팩트 간 외래키. fct_orders.order_key 를 가리킨다.
    -- 세대가 맞는 헤더를 못 찾은 상세는 null 이다(약 1%).
    order_key,

    -- 퇴화 차원. 원천 대조용이며 조인 키로 쓰지 않는다.
    order_item_id,
    order_id,

    -- 차원 외래키
    user_id,
    product_id,
    distribution_center_id,

    -- 퇴화 차원(degenerate dimension) — 별도 차원 테이블을 만들 만한 속성이 없다
    order_item_status,
    order_status,

    -- 측정값
    sale_price,
    unit_cost,
    gross_profit,
    discount_rate,
    is_revenue_recognized,

    -- 매출로 인정되지 않는 건은 0 으로 접어 둔 값.
    -- 소비자가 매번 case 문을 쓰지 않게 하고, 그 기준이 한 곳에만 있게 한다.
    case when is_revenue_recognized then sale_price else 0 end   as net_revenue,
    case when is_revenue_recognized then gross_profit else 0 end as net_gross_profit,

    ordered_at,
    ordered_date,
    shipped_at,
    delivered_at,
    returned_at,

    -- 배송 리드타임(일). 미배송이면 null 이다.
    date_diff(date(shipped_at), ordered_date, day)               as days_to_ship,
    date_diff(date(delivered_at), ordered_date, day)             as days_to_deliver

from {{ ref('int_order_items_enriched') }}
