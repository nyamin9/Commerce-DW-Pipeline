{{ config(materialized='table', cluster_by=['product_id']) }}

-- 상품 차원의 **SCD Type 2** 표현. snapshot 이 만든 이력을 소비 가능한 형태로 정리한다.
--
-- snapshot 테이블을 사용자에게 직접 노출하지 않는 이유:
--   1. dbt_valid_from / dbt_valid_to / dbt_scd_id 는 **도구의 구현 세부사항**이다.
--      도구를 바꾸면 컬럼명이 바뀌는데, 그 이름에 대시보드가 붙어 있으면 같이 깨진다.
--   2. 현재 유효 행을 고르는 조건(dbt_valid_to is null)을 매번 사용자가 쓰게 하면
--      언젠가 빠뜨리고 중복 집계가 난다. is_current 플래그로 드러낸다.
--
-- 특정 시점의 가격으로 매출을 다시 보고 싶을 때 이 테이블을 쓴다:
--   join ... on f.ordered_at >= h.valid_from and (h.valid_to is null or f.ordered_at < h.valid_to)

with snapshot_rows as (

    select * from {{ ref('snap_products') }}

)

select
    -- 이력 행의 대리키. 같은 product_id 가 여러 행으로 존재하므로
    -- 이 테이블의 PK 는 product_id 가 아니다.
    dbt_scd_id                                  as product_history_key,

    id                                          as product_id,

    name                                        as product_name,
    brand                                       as brand_name,
    category                                    as category_name,
    department                                  as department_name,
    sku,

    cast(cost as numeric)                       as unit_cost,
    cast(retail_price as numeric)               as retail_price,

    distribution_center_id,

    dbt_valid_from                              as valid_from,
    dbt_valid_to                                as valid_to,

    -- 현재 유효한 행. 소비자가 유효구간 조건을 직접 쓰지 않아도 되게 한다.
    dbt_valid_to is null                        as is_current

from snapshot_rows
