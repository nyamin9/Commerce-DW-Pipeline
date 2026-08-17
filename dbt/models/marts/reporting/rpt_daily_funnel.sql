{{ config(materialized='table', partition_by={'field': 'session_date', 'data_type': 'date', 'granularity': 'day'}) }}

-- 소비 영역 마트. 일자 × 유입채널 기준 세션 퍼널.
--
-- AARRR 의 Activation → Revenue 구간을 세션 단위로 본다.
-- 퍼널 단계 정의(무엇을 'cart 도달'로 볼 것인가)를 여기 한 곳에 고정한다.
--
-- 전환율의 분모를 무엇으로 둘지는 매번 논쟁이 되는 지점이라
-- **단계별 전환율(직전 단계 대비)과 전체 전환율(세션 대비)을 둘 다 낸다.**
-- 하나만 내면 반드시 다른 하나를 각자 계산하기 시작한다.

with sessions as (

    select * from {{ ref('fct_sessions') }}

), daily as (

    select
        session_date,
        coalesce(entry_traffic_source, '(unknown)')     as entry_traffic_source,

        count(*)                                        as session_count,
        count(distinct user_id)                         as user_count,

        countif(viewed_product)                         as product_view_sessions,
        countif(added_to_cart)                          as cart_sessions,
        countif(purchased)                              as purchase_sessions,

        countif(is_bounce)                              as bounce_sessions,
        round(avg(session_duration_seconds), 1)         as avg_session_seconds,
        round(avg(event_count), 2)                      as avg_events_per_session

    from sessions
    group by session_date, entry_traffic_source

)

select
    session_date,
    entry_traffic_source,

    session_count,
    user_count,

    product_view_sessions,
    cart_sessions,
    purchase_sessions,
    bounce_sessions,

    avg_session_seconds,
    avg_events_per_session,

    -- 전체 전환율 — 분모는 세션 수
    round(safe_divide(product_view_sessions, session_count), 4)      as cvr_session_to_product,
    round(safe_divide(cart_sessions, session_count), 4)              as cvr_session_to_cart,
    round(safe_divide(purchase_sessions, session_count), 4)          as cvr_session_to_purchase,

    -- 단계별 전환율 — 분모는 직전 단계
    round(safe_divide(cart_sessions, product_view_sessions), 4)      as cvr_product_to_cart,
    round(safe_divide(purchase_sessions, cart_sessions), 4)          as cvr_cart_to_purchase,

    round(safe_divide(bounce_sessions, session_count), 4)            as bounce_rate

from daily
