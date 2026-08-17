-- 회원 마스터.
--
-- ⚠️ PII 판단을 여기서 한 번 한다.
--    street_address 와 user_geom(좌표)은 **staging 에서 끊는다.**
--    분석 용도가 없는데 downstream로 내려보내면 마트마다 마스킹 판단을 반복해야 하고,
--    한 곳이라도 빠지면 그게 유출 경로가 된다.
--    반대로 city/state/country 는 지역 분석에 필요해 남긴다.
--    email 은 남기되 마트에서 해시로 대체한다 (dim_users 참조).
with source as (

    select
        id,
        first_name,
        last_name,
        email,
        age,
        gender,
        city,
        state,
        postal_code,
        country,
        traffic_source,
        created_at,
        _ingested_at
    from {{ source('raw_thelook', 'users') }}

)

select
    id                                  as user_id,

    -- PII. 마트로 그대로 내리지 않는다.
    first_name,
    last_name,
    email,

    age,
    lower(gender)                       as gender,

    city,
    state,
    postal_code,
    country,

    lower(traffic_source)               as acquisition_channel,

    created_at                          as signed_up_at,
    date(created_at)                    as signed_up_date,

    _ingested_at

from source
