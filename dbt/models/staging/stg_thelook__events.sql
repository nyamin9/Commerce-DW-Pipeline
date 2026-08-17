-- 웹 행동 로그. 2.4M행으로 이 프로젝트에서 가장 큰 테이블이다.
--
-- 원천이 session_id 를 이미 주지만 **그대로 쓰지 않고 이름을 구분해 둔다.**
-- int_user_sessions 가 무활동 기준으로 세션을 다시 만들고,
-- 두 값을 대조할 수 있게 하기 위해서다. (README 「세션 ID 를 다시 만드는 이유」)
-- id 단독으로는 유일하지 않다(중복 42,787건). 원천 재생성 시 id 가 재사용되기
-- 때문이며, 원인과 대응은 stg_thelook__orders 의 주석과 같다.
with source as (

    select
        id,
        user_id,
        session_id,
        sequence_number,
        event_type,
        traffic_source,
        browser,
        uri,
        city,
        state,
        postal_code,
        ip_address,
        created_at,
        _ingested_at
    from {{ source('raw_thelook', 'events') }}

)

select
    -- 이벤트의 대리키. 세션과 발생 시각까지 넣어야 사건이 특정된다.
    {{ dbt_utils.generate_surrogate_key(['id', 'session_id', 'unix_seconds(created_at)']) }}
                                        as event_key,

    id                                  as event_id,
    user_id,

    -- 원천 제공 세션 ID. 자체 세션화 결과와 구분하기 위해 source_ 접두어를 붙인다.
    session_id                          as source_session_id,
    sequence_number,

    lower(event_type)                   as event_type,
    lower(traffic_source)               as traffic_source,
    browser,
    uri,

    city,
    state,
    postal_code,

    -- PII. mart 로 내리지 않는다 (fct_user_events 에서 제외).
    ip_address,

    created_at                          as event_at,
    date(created_at)                    as event_date,

    date(_ingested_at)                  as _load_generation,
    _ingested_at

from source
