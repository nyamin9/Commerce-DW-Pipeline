{{ config(materialized='table', cluster_by=['user_id']) }}

-- 회원 차원. SCD Type 1 — 현재 상태만 유지한다.
--
-- ── 마트 시점의 PII 판단 ────────────────────────────────────────────────
-- staging 에서 이미 street_address 와 좌표를 끊었다. 여기서 한 단계 더 간다.
--
--   이름       → 아예 내리지 않는다. 분석 용도가 없다.
--   email      → 원문 대신 해시와 도메인만 남긴다.
--                도메인은 남겨야 "회사 메일 가입자 비중" 같은 분석이 된다.
--   user_id    → **해싱하지 않는다.**
--                내부 대리키라 그 자체로는 개인을 식별하지 못하고,
--                해싱하면 fct_* 와의 조인이 전부 깨진다.
--                (해싱된 값끼리는 조인되지만, 이미 적재된 사실 테이블과는 안 맞는다.
--                 바꾸려면 전 계층을 동시에 바꿔야 한다)
--
-- 요지는 "PII 를 지운다"가 아니라 **분석 가능성과 노출 범위를 어디서 맞바꿀지**를
-- 계층마다 명시적으로 정하는 것이다.

with users as (

    select * from {{ ref('stg_thelook__users') }}

)

select
    user_id,

    -- 원문 email 은 여기서 끝난다.
    to_hex(sha256(lower(trim(email))))          as email_hash,
    lower(split(email, '@')[safe_offset(1)])    as email_domain,

    age,
    case
        when age < 20 then '10s'
        when age < 30 then '20s'
        when age < 40 then '30s'
        when age < 50 then '40s'
        when age < 60 then '50s'
        else '60s+'
    end                                         as age_group,
    gender,

    city,
    state,
    country,
    postal_code,

    acquisition_channel,

    signed_up_at,
    signed_up_date,

    -- 가입 코호트. 리텐션 분석이 매번 date_trunc 를 다시 쓰지 않도록 여기 둔다.
    date_trunc(signed_up_date, month)           as signup_cohort_month

from users
