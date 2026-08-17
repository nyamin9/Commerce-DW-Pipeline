{{ config(materialized='table', cluster_by=['product_id']) }}

-- 상품 차원. **SCD Type 1 — 현재 상태만 유지한다.**
-- 변경 이력이 필요한 질문은 dim_products_history 가 답한다.
--
-- 두 개로 나눈 이유: 대부분의 조회는 "지금 이 상품이 무엇인가"를 묻는다.
-- 그 질문에 유효구간 조건(valid_from <= x < valid_to)을 매번 붙이게 하면
-- 쿼리가 복잡해지고 실수로 이력 행까지 세어 중복 집계가 난다.

with products as (

    select * from {{ ref('stg_thelook__products') }}

), distribution_centers as (

    select
        distribution_center_id,
        distribution_center_name
    from {{ ref('stg_thelook__distribution_centers') }}

)

select
    products.product_id,
    products.product_name,
    products.brand_name,
    products.category_name,
    products.department_name,
    products.sku,

    products.unit_cost,
    products.retail_price,

    -- 정가 기준 마진율. 실제 판매 마진은 int_order_items_enriched 에 있다.
    -- 이름을 다르게 둬서 둘을 혼동하지 않게 한다.
    case
        when products.retail_price > 0
            then round((products.retail_price - products.unit_cost) / products.retail_price, 4)
    end                                             as list_margin_rate,

    products.distribution_center_id,
    distribution_centers.distribution_center_name

from products
left join distribution_centers
    on products.distribution_center_id = distribution_centers.distribution_center_id
