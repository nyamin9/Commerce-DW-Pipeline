{{
    config(
        materialized='table',
        partition_by={'field': 'session_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['user_id']
    )
}}

-- 세션 팩트. 1행 = 세션 1건. fct_user_events 와 **입도가 다른** 팩트다.
--
-- ── 증분으로 만들지 않은 판단 ────────────────────────────────────────
-- 세션은 자정을 넘길 수 있어 파티션 경계에 걸친 세션을 다시 계산하려면
-- 시작일과 종료일 중 어느 쪽으로 파티션을 잡을지부터 갈린다.
-- 현재 볼륨에서는 전체 재생성이 더 싸고 틀릴 여지가 적다.
-- 커지면 session_started_at 기준 파티션 + lookback 으로 옮긴다.
--
-- 이 판단 자체를 코드에 남기는 이유는, 나중에 누가 "왜 증분이 아닌가"를
-- 물었을 때 답이 사람 기억에만 있으면 안 되기 때문이다.

with events as (

    select * from {{ ref('int_events_sessionized') }}

), session_rollup as (

    select
        session_id,
        user_id,

        min(event_at)                                       as session_started_at,
        max(event_at)                                       as session_ended_at,
        count(*)                                            as event_count,
        count(distinct event_type)                          as distinct_event_type_count,

        -- 세션 진입 채널. 세션 첫 이벤트의 값을 쓴다.
        array_agg(traffic_source order by event_at, event_key limit 1)[safe_offset(0)]
                                                            as entry_traffic_source,
        array_agg(uri order by event_at, event_key limit 1)[safe_offset(0)]
                                                            as entry_uri,
        array_agg(browser order by event_at, event_key limit 1)[safe_offset(0)]
                                                            as browser,

        -- 퍼널 도달 여부. AARRR 의 Activation~Revenue 구간에 대응한다.
        countif(event_type = 'product') > 0                 as viewed_product,
        countif(event_type = 'cart') > 0                    as added_to_cart,
        countif(event_type = 'purchase') > 0                as purchased,
        countif(event_type = 'cancel') > 0                  as cancelled,

        -- 원천 세션 ID 가 이 세션 안에서 몇 개로 쪼개져 있는가.
        -- 자체 세션화와 원천의 차이를 수치로 보여주는 값이다.
        count(distinct source_session_id)                   as source_session_id_count

    from events
    group by session_id, user_id

)

select
    session_id,
    user_id,

    session_started_at,
    session_ended_at,
    date(session_started_at)                                as session_date,

    timestamp_diff(session_ended_at, session_started_at, second)
                                                            as session_duration_seconds,

    event_count,
    distinct_event_type_count,

    entry_traffic_source,
    entry_uri,
    browser,

    viewed_product,
    added_to_cart,
    purchased,
    cancelled,

    -- 이벤트가 하나뿐인 세션. 이탈률 계산의 기준이 된다.
    event_count = 1                                         as is_bounce,

    source_session_id_count

from session_rollup
