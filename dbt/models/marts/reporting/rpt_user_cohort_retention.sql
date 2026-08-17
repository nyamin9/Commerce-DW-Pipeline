{{ config(materialized='table') }}

-- 소비 영역 마트. 가입 코호트 × 경과 개월 기준 구매 리텐션.
--
-- AARRR 의 Retention 에 해당한다. Acquisition 은 dim_users.acquisition_channel,
-- Activation~Revenue 는 rpt_daily_funnel 이 담당한다.
--
-- 코호트 정의를 여기 한 곳에만 둔다. 대시보드마다 코호트를 다시 정의하면
-- 같은 이름의 리텐션이 서로 다른 값을 갖게 된다.

with users as (

    select
        user_id,
        signup_cohort_month
    from {{ ref('dim_users') }}

), cohort_size as (

    select
        signup_cohort_month,
        count(*)                                        as cohort_user_count
    from users
    group by signup_cohort_month

), purchases as (

    select distinct
        orders.user_id,
        users.signup_cohort_month,
        date_trunc(orders.ordered_date, month)          as activity_month
    from {{ ref('fct_orders') }} as orders
    inner join users
        on orders.user_id = users.user_id
    -- 취소만 있는 주문은 구매로 보지 않는다
    where orders.net_revenue > 0

), retention as (

    select
        signup_cohort_month,
        activity_month,
        date_diff(activity_month, signup_cohort_month, month)
                                                        as months_since_signup,
        count(distinct user_id)                         as retained_user_count
    from purchases
    -- 가입 이전 달에 찍힌 활동은 데이터 이상이므로 제외하고, 별도로 감시한다
    where activity_month >= signup_cohort_month
    group by signup_cohort_month, activity_month

)

select
    retention.signup_cohort_month,
    retention.activity_month,
    retention.months_since_signup,

    cohort_size.cohort_user_count,
    retention.retained_user_count,

    round(
        safe_divide(retention.retained_user_count, cohort_size.cohort_user_count),
        4
    )                                                   as retention_rate

from retention
inner join cohort_size
    on retention.signup_cohort_month = cohort_size.signup_cohort_month
