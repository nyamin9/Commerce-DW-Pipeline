{{ config(materialized='table', partition_by={'field': 'ordered_date', 'data_type': 'date', 'granularity': 'day'}) }}

-- 소비 영역 마트. 일자 × 부서 기준 매출 지표.
--
-- ── 표준 영역과 무엇이 다른가 ────────────────────────────────────────
-- fct_order_items(표준 영역)는 **사실을 입도 그대로** 들고 있다.
-- 이 모델은 **특정 목적에 맞춰 반정규화·집계**한 것이다.
-- "주간 기준으로도 보고 싶다"는 요청이 왔을 때 고칠 곳은 여기지 fct 가 아니다.
-- 사실은 그대로이고 보는 방식만 바뀐 것이기 때문이다.
--
-- ── daily → 누적(WTD/MTD/YTD) → 비교(YoY) 구조 ──────────────────────
-- 모든 지표가 같은 모양을 갖게 해서, 새 지표가 추가돼도 소비자가
-- 컬럼 이름 규칙을 다시 배우지 않아도 되게 한다.
--
-- ⚠️ 누적 컬럼은 **매 실행마다 전체를 다시 계산한다.**
--    현재 볼륨(약 7년 × 2부서)에서는 문제가 없지만,
--    분해 축이 늘거나 기간이 길어지면 이 설계는 그대로 통하지 않는다.
--    그때는 전일 누적값에 당일치를 더하는 증분 누적으로 바꿔야 한다.

-- 팩트는 외래키와 측정값만 들고 있다. 부서명 같은 서술 속성은 차원에 있다.
-- **그 조합을 소비 계층에서 하는 것이 별 스키마의 사용법이다.**
-- 팩트에 서술 속성을 미리 심어두면 상품 분류가 바뀔 때 과거 팩트까지 다시 써야 한다.
with order_items as (

    select * from {{ ref('fct_order_items') }}

), products as (

    select
        product_id,
        department_name
    from {{ ref('dim_products') }}

), daily as (

    select
        order_items.ordered_date,
        products.department_name,

        count(distinct order_items.order_id)                as order_count,
        count(*)                                            as order_item_count,
        count(distinct order_items.user_id)                 as buyer_count,

        sum(order_items.net_revenue)                        as net_revenue,
        sum(order_items.net_gross_profit)                   as net_gross_profit,
        countif(order_items.order_item_status = 'returned') as returned_item_count

    from order_items
    inner join products
        on order_items.product_id = products.product_id
    where products.department_name is not null
    group by order_items.ordered_date, products.department_name

), with_cumulative as (

    select
        *,

        sum(net_revenue) over (
            partition by department_name, date_trunc(ordered_date, week(monday))
            order by ordered_date
            rows between unbounded preceding and current row
        )                                                   as net_revenue_wtd,

        sum(net_revenue) over (
            partition by department_name, date_trunc(ordered_date, month)
            order by ordered_date
            rows between unbounded preceding and current row
        )                                                   as net_revenue_mtd,

        sum(net_revenue) over (
            partition by department_name, date_trunc(ordered_date, year)
            order by ordered_date
            rows between unbounded preceding and current row
        )                                                   as net_revenue_ytd

    from daily

)

select
    current_period.ordered_date,
    current_period.department_name,

    current_period.order_count,
    current_period.order_item_count,
    current_period.buyer_count,
    current_period.returned_item_count,

    current_period.net_revenue,
    current_period.net_gross_profit,

    current_period.net_revenue_wtd,
    current_period.net_revenue_mtd,
    current_period.net_revenue_ytd,

    -- 전년 동일자 대비. 조인 키를 날짜 연산으로 만들어 캘린더 테이블 없이 처리한다.
    prior_year.net_revenue                                  as net_revenue_yoy_base,
    case
        when prior_year.net_revenue > 0
            then round(
                (current_period.net_revenue - prior_year.net_revenue) / prior_year.net_revenue,
                4
            )
    end                                                     as net_revenue_yoy_rate

from with_cumulative as current_period
left join with_cumulative as prior_year
    on current_period.department_name = prior_year.department_name
   and prior_year.ordered_date = date_sub(current_period.ordered_date, interval 1 year)
