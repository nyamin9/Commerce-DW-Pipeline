{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['user_id'],
        on_schema_change='sync_all_columns'
    )
}}

-- 행동 로그에 **자체 세션 ID** 를 부여한다.
--
-- ── 이 모델만 intermediate 의 view 규칙에서 예외를 둔다 ──────────────────
-- 나머지 intermediate 는 view 다. 이 모델은 2.4M행 위에서 윈도우 함수를 돌리므로
-- view 로 두면 downstream(fct_user_events, fct_sessions)가 참조할 때마다 전부 다시 계산된다.
-- 계층 규칙보다 재계산 비용이 더 큰 경우라 증분 테이블로 내렸다.
--
-- ── 세션 ID 를 누적 카운터로 만들지 않은 이유 ──────────────────────────
-- `sum(is_session_start) over (unbounded preceding)` 같은 일련번호는
-- **유저의 전체 이력이 있어야 계산된다.** 최근 3일치만 읽는 증분에서는 성립하지 않는다.
-- 그래서 ID 를 "세션의 첫 이벤트 시각"으로 잡았다.
-- 이러면 버퍼 구간만 읽어도 같은 값이 나오고, 몇 번을 다시 돌려도 변하지 않는다.
--
-- ── 버퍼가 필요한 이유 ───────────────────────────────────────────────
-- 대상 구간의 첫 이벤트는 직전 이벤트가 있어야 "새 세션인지"를 판정할 수 있다.
-- 대상 구간보다 하루 더 읽어서 판정에만 쓰고, 출력은 대상 구간만 한다.

with events as (

    select
        event_id,
        user_id,
        source_session_id,
        event_type,
        traffic_source,
        browser,
        uri,
        event_at,
        event_date
    from {{ ref('stg_thelook__events') }}

    -- 비로그인 트래픽(전체의 약 46%)은 세션을 이어붙일 키가 없다.
    -- 익명 세션을 다루려면 별도 식별자가 필요하고 이 프로젝트 범위 밖이다.
    where user_id is not null

    {% if is_incremental() %}
        -- 판정용 버퍼. 대상 구간보다 하루 더 읽는다.
        and event_date >= {{ get_lookback_start_date(extra_days=1) }}
        and event_date <= {{ get_run_date() }}
    {% endif %}

), with_gap as (

    select
        *,
        timestamp_diff(
            event_at,
            lag(event_at) over (partition by user_id order by event_at, event_id),
            minute
        ) as minutes_since_prev_event
    from events

), with_session_start as (

    select
        *,
        -- 직전 이벤트가 없거나(첫 이벤트) 무활동이 기준을 넘으면 새 세션이 시작된다.
        case
            when minutes_since_prev_event is null then 1
            when minutes_since_prev_event > {{ var('session_timeout_minutes') }} then 1
            else 0
        end as is_session_start
    from with_gap

), with_session_start_at as (

    select
        *,
        -- 가장 최근의 "세션 시작" 이벤트 시각을 끌고 내려온다.
        -- 이 값이 같은 이벤트들이 한 세션이다.
        last_value(
            case when is_session_start = 1 then event_at end
            ignore nulls
        ) over (
            partition by user_id
            order by event_at, event_id
            rows between unbounded preceding and current row
        ) as session_started_at
    from with_session_start

)

select
    event_id,
    user_id,

    -- **결정적(deterministic) 세션 ID.**
    -- 난수도 생성 시각도 섞지 않는다. 같은 입력이면 언제 다시 돌려도 같은 값이 나와야
    -- 백필과 재처리가 성립한다.
    format(
        '%d-%s',
        user_id,
        format_timestamp('%Y%m%d%H%M%S', session_started_at)
    )                                           as session_id,
    session_started_at,

    -- 원천이 준 값. 자체 세션화와의 차이를 측정하려고 함께 들고 간다.
    source_session_id,

    is_session_start,
    minutes_since_prev_event,

    event_type,
    traffic_source,
    browser,
    uri,

    event_at,
    event_date

from with_session_start_at

{% if is_incremental() %}
    -- 버퍼로 읽은 하루는 판정에만 쓰고 버린다.
    -- insert_overwrite 는 결과에 등장한 파티션만 교체하므로,
    -- 여기서 걸러내지 않으면 버퍼 날짜 파티션까지 불완전한 내용으로 덮인다.
    where event_date >= {{ get_lookback_start_date() }}
      and event_date <= {{ get_run_date() }}
{% endif %}
