-- 주문 상세에 주문 헤더와 상품 속성을 붙인다.
--
-- **이 계층이 존재하는 이유는 성능이 아니라 정합성이다.**
-- fct_order_items, rpt_daily_revenue 가 각자 같은 조인을 하면
-- 나중에 한쪽만 고쳐져 매출 숫자가 갈라진다. 조인을 여기 한 번만 둔다.
--
-- 마진(margin)도 여기서 계산한다. "매출 - 원가"의 정의가 두 곳에 생기면
-- 그 순간부터 어느 쪽이 맞는지 물었을 때 답이 없어진다.
--
-- ── 헤더 조인에 _load_generation 을 함께 거는 이유 ───────────────────────
-- 원천이 재생성되면서 order_id 가 재사용되어, raw 에 두 세대가 공존한다.
-- order_id 만으로 조인하면 세대를 넘어 매칭되어 행이 불어난다(실측 +8,535행).
-- 적재 세대를 조인 조건에 넣으면 팬아웃이 0 이 된다.
--
-- **여기가 order_items 에 order_key 를 붙이는 유일한 지점이다.** 상세 테이블은
-- 헤더의 created_at 을 모르기 때문에 자기 컬럼만으로는 order_key 를 만들 수 없다.
with order_items as (

    select * from {{ ref('stg_thelook__order_items') }}

), orders as (

    select
        order_key,
        order_id,
        order_status,
        item_count,
        _load_generation
    from {{ ref('stg_thelook__orders') }}

), products as (

    select
        product_id,
        product_name,
        brand_name,
        category_name,
        department_name,
        unit_cost,
        retail_price,
        distribution_center_id
    from {{ ref('stg_thelook__products') }}

)

select
    order_items.order_item_key,

    -- 헤더의 대리키. 세대가 맞는 헤더가 없으면 null 이다(전체의 약 1%).
    orders.order_key,

    order_items.order_item_id,
    order_items.order_id,
    order_items.user_id,
    order_items.product_id,
    order_items.inventory_item_id,

    -- 상세와 헤더의 상태가 다를 수 있다. 둘 다 남기고 이름으로 구분한다.
    order_items.order_item_status,
    orders.order_status,
    orders.item_count                                   as order_item_count,

    products.product_name,
    products.brand_name,
    products.category_name,
    products.department_name,
    products.distribution_center_id,

    order_items.sale_price,
    products.unit_cost,
    products.retail_price,

    -- 마진의 단일 정의.
    order_items.sale_price - products.unit_cost         as gross_profit,

    -- 정가 대비 얼마나 깎여 팔렸는가. 0 나눗셈을 방어한다.
    case
        when products.retail_price > 0
            then round((products.retail_price - order_items.sale_price) / products.retail_price, 4)
    end                                                 as discount_rate,

    -- 취소·반품을 매출에서 제외하는 기준을 여기서 한 번 정한다.
    order_items.order_item_status not in ('cancelled', 'returned')
                                                        as is_revenue_recognized,

    order_items.ordered_at,
    order_items.ordered_date,
    order_items._load_generation,
    order_items.shipped_at,
    order_items.delivered_at,
    order_items.returned_at

from order_items
left join orders
    on  order_items.order_id        = orders.order_id
    -- 세대를 넘는 매칭을 막는다. 이 조건이 없으면 행이 불어난다.
    and order_items._load_generation = orders._load_generation
left join products
    on order_items.product_id = products.product_id
