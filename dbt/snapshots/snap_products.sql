{% snapshot snap_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='check',
        check_cols=[
            'retail_price',
            'cost',
            'name',
            'brand',
            'category',
            'department',
            'distribution_center_id'
        ],
        invalidate_hard_deletes=True
    )
}}

-- 상품 마스터의 변경 이력. dbt 가 SCD Type 2 를 만들어 준다.
--
-- ── 왜 timestamp 가 아니라 check 전략인가 ──────────────────────────────
-- timestamp 전략은 원천에 신뢰할 만한 갱신 시각이 있어야 쓸 수 있는데
-- **thelook 의 products 에는 updated_at 이 없다.**
-- check 는 지정한 컬럼 값을 이전 스냅샷과 직접 비교한다.
-- 비교 비용이 들고, **지정하지 않은 컬럼의 변경은 놓친다.**
-- 그래서 sku 처럼 바뀔 일이 없는 컬럼은 빼고 비즈니스적으로 의미 있는 것만 넣었다.
--
-- ── 한계를 알고 쓴다 ─────────────────────────────────────────────────
-- snapshot 은 **실행 시점의 상태만 본다.**
-- 하루 한 번 돌리는데 그 사이 가격이 두 번 바뀌었다면 중간 값은 영원히 남지 않는다.
-- 실행 주기가 곧 이력의 해상도다.
--
-- ── SELECT * 를 쓰지 않는다 ──────────────────────────────────────────
-- 원천에 컬럼이 추가되면 스냅샷 스키마가 조용히 바뀐다. 명시해서 그 순간 깨지게 둔다.

select
    id,
    name,
    brand,
    category,
    department,
    sku,
    cost,
    retail_price,
    distribution_center_id
from {{ source('raw_thelook', 'products') }}

{% endsnapshot %}
