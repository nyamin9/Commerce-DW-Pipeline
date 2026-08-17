{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['user_id', 'event_type'],
        on_schema_change='sync_all_columns'
    )
}}

-- 유저 행동 팩트. 1행 = 이벤트 1건. 이 프로젝트에서 가장 큰 테이블이다.
--
-- ── insert_overwrite 를 쓰는 이유 ────────────────────────────────────
-- merge 전략은 unique_key 로 전체 테이블을 대조해야 해서 스캔이 크다.
-- insert_overwrite 는 **결과에 등장한 파티션만 통째로 교체**한다.
-- 같은 run_date 로 몇 번을 돌려도 결과가 같고(멱등), 스캔은 대상 구간에 한정된다.
--
-- ── ip_address 를 내리지 않는다 ─────────────────────────────────────
-- staging 까지는 있지만 여기서 끊는다. 이 테이블이 가장 널리 조회되는 자산이라
-- 노출 범위가 가장 넓고, 분석에 필요한 지역 정보는 city/state 로 충분하다.

with sessionized as (

    select * from {{ ref('int_events_sessionized') }}

    {% if is_incremental() %}
        where event_date >= {{ get_lookback_start_date() }}
          and event_date <= {{ get_run_date() }}
    {% endif %}

)

select
    event_id,

    -- 차원 외래키
    user_id,

    -- 세션은 별도 차원 테이블이 아니라 fct_sessions 라는 다른 입도의 팩트다.
    -- 여기서는 조인 키로만 들고 간다.
    session_id,

    event_type,
    traffic_source,
    browser,
    uri,

    -- 원천 세션 ID 와 자체 세션 ID 의 차이를 사후에 측정할 수 있게 남긴다.
    source_session_id,

    is_session_start,
    minutes_since_prev_event,

    event_at,
    event_date

from sessionized
