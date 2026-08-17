# 이력 적재

> dbt  ·  [← 테스트와 데이터 품질](03-testing.md) | [목차](README.md) | [매크로와 메타데이터 →](05-macros-metadata.md)

값이 바뀌는 원천의 과거 상태를 남기는 두 방식 — SCD Type 2(`snapshot`)와 일자별 스냅샷 — 의 차이, 선택 기준, 둘을 잇는 방법을 다룸.

## 4-1. 이력 적재 — SCD Type 2와 일자별 스냅샷

> **용어가 겹쳐서 가장 많이 헷갈리는 지점임.** 이력을 남기는 방식은 크게 둘인데,
> dbt의 `snapshot` **기능은 그중 하나(SCD Type 2)만 구현함.**
> 나머지 하나는 일반 증분 모델로 만듦.

### 4-1-1. 두 방식의 구분

| | **① SCD Type 2** | **② 일자별 스냅샷** (periodic snapshot) |
|---|---|---|
| 적재 방식 | **바뀐 것만** 새 행으로 | **매일 전체 상태를** 그날 파티션에 통째로 |
| 행 수 | 변경 횟수 | 엔티티 수 × 일수 |
| 시점 조회 | `valid_from <= x and (valid_to is null or x < valid_to)` | `where snapshot_date = '2026-08-15'` |
| 조인 형태 | **범위 조인** | **동등 조인** |
| "언제 바뀌었나" | `valid_from`이 직접 알려줌 | 날짜끼리 비교해 역산해야 함 |
| 삭제 처리 | `invalidate_hard_deletes` 필요 | 다음 파티션에 안 나타나면 끝 |
| **dbt 구현** | **`snapshot` 리소스** | **`incremental` + `insert_overwrite`** |

- **Kimball 용어로는** ①이 SCD Type 2(dimension 이력 관리 기법), ②가 periodic snapshot fact(매일의 상태 자체가 사실인 팩트 테이블)임
- **dbt가 ①에 "snapshot"이라는 이름을 붙이면서 용어가 꼬였음**

---

### 4-1-2. ① SCD Type 2 — dbt의 `snapshot`

- `snapshots/`에 정의하고 `dbt snapshot`으로 실행함
- **원천의 현재 상태를 주기적으로 찍어서 변경 이력을 만듦**

```sql
{% snapshot products_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
) }}
select * from {{ source('raw','products') }}
{% endsnapshot %}
```

**변경 감지 전략 두 가지**

- **`timestamp`** — `updated_at` 컬럼을 보고 바뀌었는지 판단
  - 가볍고 정확함
  - 원천에 신뢰할 만한 갱신 시각이 있어야 함
- **`check`** — 지정한 컬럼들의 값을 이전 스냅샷과 비교
  - `updated_at`이 없을 때 씀
  - 비교 비용이 들고, **지정 안 한 컬럼의 변경은 놓침**

> **전략은 SCD 유형이 아님.** `timestamp`와 `check`는 "무엇이 바뀌었는지 어떻게 알아내는가"의
> 차이일 뿐, 둘 다 결과물은 SCD Type 2임.

- **dbt가 자동으로 붙이는 컬럼**: `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`
  - 현재 행은 `dbt_valid_to`가 null임
  - 이게 SCD Type 2의 유효기간 구조 그대로임

- **`invalidate_hard_deletes`** — 원천에서 행이 사라졌을 때의 처리
  - 기본값 `False`에서는 삭제된 행이 **영원히 "현재 유효"로 남음**
  - `True`면 사라진 시점에 `dbt_valid_to`를 찍어 구간을 닫음
- **원천이 hard delete를 하는 시스템이면 반드시 켬**

- **snapshot 테이블을 그대로 소비하지 않음**
  - `dbt_valid_from` / `dbt_scd_id`는 도구의 구현 세부사항이라, 도구를 바꾸면 컬럼명이 바뀌고 거기 붙은 대시보드가 같이 깨짐
  - `dim_*_history` 같은 모델로 한 번 감싸서 `valid_from` / `is_current`로 노출함

- **가장 중요한 한계**: snapshot은 **실행 시점의 상태만 봄**
  - 하루에 한 번 돌리는데 그 사이에 값이 두 번 바뀌었으면 중간 값은 영원히 못 봄
- **실행 주기가 곧 이력의 해상도임**
  - 더 촘촘한 이력이 필요하면 주기를 올릴 게 아니라 원천에서 CDC를 받아야 한다([7-1](07-ingestion.md) 참조)
  - 도구를 바꿀 문제임

---

### 4-1-3. ② 일자별 스냅샷 — 증분 모델로 만듦

- **`snapshot` 기능을 쓰지 않음**
  - 파티션을 날짜로 잡은 평범한 증분 모델임

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'snapshot_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    {{ get_run_date() }} as snapshot_date,
    product_id,
    retail_price,
    category
from {{ ref('stg_products') }}
```

- 같은 날짜로 다시 돌리면 **그 파티션만 통째로 교체**되므로 멱등함
- **백필은 되지 않음**
  - 과거 시점의 원천 상태를 우리가 갖고 있지 않기 때문임
  - 이건 ①도 마찬가지임 — 두 방식 모두 "찍기 시작한 시점"부터만 이력이 있음
  - 삭제는 별도 처리가 필요 없음
  - 사라진 엔티티는 다음 파티션에 그냥 안 나타남

---

### 4-1-4. 어느 쪽을 고르나

| 상황 | 선택 |
|---|---|
| 엔티티가 많고(수백만) 변경은 드묾 | **① SCD Type 2.** 일자별이면 저장이 폭발함 |
| "특정 시점의 전체 상태"를 자주 봄 | **② 일자별.** 동등 조인이라 쿼리가 단순함 |
| **변경 시점·변경 전후 값**이 분석 대상 | **① SCD Type 2** |
| 잔액·재고처럼 **매일의 상태 자체가 사실** | **② 일자별.** periodic snapshot fact가 이것임 |
| **배치 시점 정합**이 중요 — 여러 테이블을 같은 기준일로 맞춰 조인해야 함 | **② 일자별.** `snapshot_date` 축을 공유하므로 `where snapshot_date = '{{ ds }}'` 한 줄로 맞음 |

- **범위 조인의 비용이 실무에서 갈림길이 됨**
  - BigQuery 같은 스캔 과금 웨어하우스에서 `between` 조인은 파티션 프루닝이 잘 듣지 않음
  - 일자별 스냅샷은 `snapshot_date = ordered_date` 한 줄이라 스캔이 정확히 잘림
- **대규모 DW가 저장을 더 쓰면서도 일자별을 유지하는 이유가 여기 있음**

> **일자별의 비용을 과소평가하지 않음.** 엔티티 100만 개를 1년 쌓으면 3.65억 행임.
> 파티션 만료를 걸거나, 최근 N일만 일자별로 두고 그 이전은 **월말 시점만 남기는** 식으로 줄임.

### 4-1-5. 둘을 잇는 방법

- SCD Type 2로 저장하고, `dbt_utils.date_spine`으로 날짜를 펼쳐 일자별로 소비함

```sql
with dates as (
    {{ dbt_utils.date_spine(datepart="day", start_date="'2024-01-01'", end_date="current_date()") }}
)
select d.date_day as snapshot_date, h.*
from dates d
join {{ ref('dim_products_history') }} h
  on d.date_day >= date(h.valid_from)
 and (h.valid_to is null or d.date_day < date(h.valid_to))
```

- **저장은 ①로 아끼고 소비는 ②로 단순하게** 가져가는 절충임
  - 다만 이 뷰를 table로 물리화하면 결국 일자별 스냅샷과 같은 크기가 됨

---

[← 테스트와 데이터 품질](03-testing.md) | [목차](README.md) | [매크로와 메타데이터 →](05-macros-metadata.md)
