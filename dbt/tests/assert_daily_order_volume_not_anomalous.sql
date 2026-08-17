{{ config(severity='warn', store_failures=true) }}

-- 일별 주문 건수가 **평소 범위**를 벗어났는가.
--
-- ── 이 테스트가 여기 있는 것 자체가 경계 사례다 ──────────────────────
-- dbt test 는 원래 **사람이 미리 선언한 규칙**을 검사하는 도구다
-- (unique, not_null, 값 도메인). 규칙은 데이터를 보지 않고도 쓸 수 있다.
--
-- 그런데 "오늘 주문이 평소보다 적다"는 미리 쓸 수 없다.
-- **정상 범위가 데이터에서 나오기 때문이다.** 이건 기준선(baseline) 기반 감시이고,
-- 원래는 파이프라인 밖에서 자기 주기로 도는 독립 감시의 영역이다.
--
-- 여기 넣은 이유는 두 가지다.
--   1. 규칙 기반과 기준선 기반이 실제로 어떻게 다른지 코드로 남기려고
--   2. 이 규모에서는 별도 감시 체계를 세우는 비용이 이득을 넘어서므로
--
-- **한계를 알고 쓴다:** 이 테스트는 빌드에 붙어 있어서 빌드가 실패하면 돌지 않는다.
-- 파이프라인이 아예 멈춘 날은 아무것도 감지하지 못한다. 그게 독립 감시가 필요한 이유다.
--
-- ── 판정 기준 ───────────────────────────────────────────────────────
-- 같은 요일의 직전 12주와 비교한다. 요일을 맞추는 이유는 주중/주말 패턴이
-- 요일 무시 평균을 그대로 왜곡하기 때문이다.
--
-- ── 이 기준의 한계 (실제로 관측됨) ──────────────────────────────────
-- z-score 기준선은 **추세를 반영하지 못한다.** 서비스가 꾸준히 성장하면
-- 최근값이 항상 과거 평균보다 크므로 매일 울린다.
-- 실제로 이 데이터셋에서 z = 21 ~ 49 로 3일 연속 걸렸는데,
-- 장애가 아니라 주문량이 우상향한 것이다.
--
-- 운영에서는 셋 중 하나를 택한다.
--   1. 추세를 제거하고(전주 대비 증감률 등) 그 위에서 이탈을 본다
--   2. 기준선 창을 짧게 잡아 최근 수준을 따라가게 한다
--   3. 표준편차 대신 변동폭(min~max 범위) 이탈로 판정한다 — 추세에 덜 민감하다
--
-- 여기서는 기준을 그대로 두고 한계를 드러내는 쪽을 택했다.
-- **"경보가 울린다"와 "문제가 있다"는 다르고, 그 간극이 이상탐지 설계의 핵심이다.**

with daily_orders as (

    select
        ordered_date,
        count(*) as order_count
    from {{ ref('fct_orders') }}
    group by ordered_date

), with_baseline as (

    select
        ordered_date,
        order_count,
        -- 같은 요일의 직전 12주. 당일은 제외한다(자기 자신이 기준선에 섞이면 둔감해진다).
        avg(order_count) over same_weekday_window  as baseline_avg,
        stddev(order_count) over same_weekday_window as baseline_stddev,
        count(*) over same_weekday_window          as baseline_weeks
    from daily_orders
    window same_weekday_window as (
        partition by extract(dayofweek from ordered_date)
        order by ordered_date
        rows between 12 preceding and 1 preceding
    )

)

select
    ordered_date,
    order_count,
    round(baseline_avg, 1)                                          as baseline_avg,
    round(safe_divide(order_count - baseline_avg, baseline_stddev), 2) as z_score
from with_baseline
where
    -- 기준선이 충분히 쌓이지 않은 초기 구간은 판정하지 않는다
    baseline_weeks >= 8
    and baseline_stddev > 0
    -- 가장 최근 구간만 본다. 과거 이상치까지 매번 다시 알릴 이유가 없다
    and ordered_date >= date_sub({{ get_run_date() }}, interval {{ var('lookback_days') }} day)
    and ordered_date < {{ get_run_date() }}
    and abs(safe_divide(order_count - baseline_avg, baseline_stddev)) > 3
