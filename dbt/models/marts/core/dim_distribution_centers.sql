{{ config(materialized='table') }}

-- 물류센터 차원. 10행이라 물리 설정을 따로 걸지 않는다.
-- 파티셔닝·클러스터링은 공짜가 아니고, 이 크기에서는 메타데이터 오버헤드만 는다.

select
    distribution_center_id,
    distribution_center_name,
    latitude,
    longitude
from {{ ref('stg_thelook__distribution_centers') }}
