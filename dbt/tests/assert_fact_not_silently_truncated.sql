{{ config(severity='error') }}

-- fact 가 staging 보다 적은 행을 갖고 있지 않은가.
--
-- ── 이 테스트가 존재하는 이유 ────────────────────────────────────────
-- 웨어하우스가 **우리가 쓴 행을 나중에 지우는** 경우가 있다.
-- BigQuery 의 파티션 만료(default_partition_expiration_ms)가 대표적이다.
-- 데이터셋에 만료가 걸려 있으면 파티션 테이블에 과거 데이터를 써도
-- 기준일보다 오래된 파티션이 삭제된다.
--
-- 문제는 이게 **조용하다**는 것이다.
--   dbt 가 모델을 씀      → fct_orders 에 125,158 행 기록
--   웨어하우스가 만료 적용 → 60일 이전 파티션 삭제, 16,108 행만 남음
--   dbt 가 테스트를 돌림   → 잘린 테이블을 봄
-- dbt 는 자기가 쓴 행 수를 다시 세지 않으므로 모델 생성은 성공으로 보고된다.
-- 실제로 이 프로젝트에서 한 번 밟았고, 증상이 참조 무결성 위반과
-- 중복 키로 나타나 데이터 문제처럼 보였다.
--
-- ── staging 을 기준선으로 쓰는 이유 ─────────────────────────────────
-- staging 은 view 라 저장하지 않는다. 그래서 만료의 영향을 받지 않고
-- 항상 원천의 전량을 보여준다. 잘리지 않는 기준선이 된다.
--
-- ── severity 를 error 로 둔 이유 ────────────────────────────────────
-- 원천의 정합성 문제(assert_order_item_count_matches)와 성격이 다르다.
-- 이건 **마트에 있어야 할 행이 없다**는 뜻이라 그 위의 모든 숫자가 틀린다.
-- downstream를 진행시키면 안 된다.
--
-- fct_orders 와 stg_thelook__orders 는 1:1 이므로 행 수가 같아야 한다.
-- 파티션 만료뿐 아니라 조인이 행을 떨어뜨리는 경우도 함께 잡힌다.

with counts as (

    select
        (select count(*) from {{ ref('stg_thelook__orders') }}) as staging_rows,
        (select count(*) from {{ ref('fct_orders') }})          as fact_rows

)

select
    staging_rows,
    fact_rows,
    staging_rows - fact_rows as missing_rows
from counts
where staging_rows != fact_rows
