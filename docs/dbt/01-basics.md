# 기초

> dbt  ·  [목차](README.md) | [프로젝트 구조와 명령어 →](02-project-setup.md)

`ref()` / `source()` 로 의존성 그래프가 만들어지는 원리, staging / intermediate / marts 계층 구조, materialization 4종과 증분 모델에서 부딪히는 것들을 다룸.

## 1-1. dbt가 하는 일 — ref() / source()와 의존성 그래프

### 1-1-1. dbt 가 담당하는 범위

- dbt(data build tool)는 **DW 안에서의 변환(T)만 담당하는 도구**임
- 데이터를 옮기지 않음
- 이미 DW에 들어온 데이터를 SQL로 변환하되, 그 SQL에 소프트웨어 엔지니어링 관행(버전 관리, 의존성 관리, 테스트, 문서화)을 입힘

### 1-1-2. ref() 와 source()

- **`ref()`가 dbt의 핵심임**
  - 모델 안에서 다른 모델을 참조할 때 테이블명을 직접 쓰지 않고 `{{ ref('stg_orders') }}`로 씀
  - 이것이 두 가지를 동시에 함

- **① 컴파일 시 실제 경로로 치환됨**
  - dev 환경이면 `dev_schema.stg_orders`, prod면 `prod_schema.stg_orders`
  - **코드는 그대로 두고 환경만 바뀜**
- **② 의존성 그래프가 자동으로 만들어짐**
  - 사람이 실행 순서를 적지 않음
  - dbt가 그래프를 위상 정렬해서 순서를 정하고, 독립적인 모델은 병렬로 돌림

- **`source()`는 dbt가 만들지 않은 원천 테이블**을 가리킴
  - `sources.yml`에 선언하고 `{{ source('raw', 'orders') }}`로 씀
  - 원천과 모델의 경계를 명시하는 것이 목적임

- **freshness**는 원천이 얼마나 최신인지 검사함

```yaml
sources:
  - name: raw
    tables:
      - name: orders
        loaded_at_field: _ingested_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
```

- `dbt source freshness`로 실행함
- **파이프라인이 성공했는데 데이터가 안 들어온 경우**를 잡는 장치임
  - 변환 로직은 정상인데 원천 쪽에서 데이터가 안 온 경우가 여기 해당함


## 1-2. 계층 구조 — staging / intermediate / marts

> 이 섹션이 dbt 프로젝트 디렉토리 설계의 기준임.

### 1-2-1. 정의

**아키텍처 계층은 3개임. dbt 디렉토리 이름을 계층으로 세지 않음.**

| 아키텍처 계층 | Medallion | dbt에서 |
|---|---|---|
| 원천 랜딩(landing) | Bronze | `source()` — **모델이 아님** |
| 정제·통합 | Silver | `staging` + `intermediate` |
| 소비 | Gold | `marts` |

- **`staging`과 `intermediate`는 둘 다 Silver임**
  - 별개 계층이 아니라 **Silver 내부를 코드 조직 관점에서 나눈 것**임

> 근거: dbt에서 staging 모델은 대개 **view나 ephemeral**로 만듦.
> 즉 **저장되지도 않음.** 저장 계층이 아니라 이름 붙인 CTE에 가까움.
> dbt가 이 구조를 강제하지도 않음. 관례일 뿐임.

- **용어 주의**: `Lake → staging → DW`에서 말하는 staging은 **물리적 랜딩 구역**이고, 이건 dbt의 `staging`이 아니라 **`source`**임
  - 랜딩 구역은 `raw_`나 `bronze_`로 부르고, `staging`은 dbt 계층 이름으로만 쓰는 편이 안전함

### 1-2-2. 그럼 왜 굳이 나누나 — 이유는 각각 하나씩

- **staging의 존재 이유: 원천 이름과 표준 이름의 경계를 한 곳으로 못 박음**
  - staging 위쪽은 표준화된 이름만 씀
  - 원천 컬럼명이 바뀌면 고칠 곳이 그 모델 하나로 제한됨
  - `1:1 유지, 조인 금지`가 규칙인 이유가 이것 하나임
  - 조인하는 순간 경계가 흐려짐

- **intermediate의 존재 이유: 같은 조인을 두 번 하지 않음**
  - mart A와 mart B가 각자 같은 조인을 하면, 나중에 한쪽만 고쳐져 숫자가 갈라짐
- **성능이 아니라 정합성을 위한 계층임**

### 1-2-3. 언제 나누고 언제 안 나누나

| 상황 | 판단 |
|---|---|
| 원천 소수, mart 소수 | **나누지 않음.** source에서 mart로 직행해도 됨 |
| 원천이 여럿이고 자주 바뀜 | staging 도입. 변경 영향을 한곳에 가두는 효과가 큼 |
| 같은 조인이 **두 번째** mart에서 또 필요 | 그 시점에 intermediate로 분리함 |

- **미리 만들지 않음**
  - 필요해진 시점에 만듦
  - 안 그러면 통과만 하는 빈 계층이 쌓임

## 여기에 DW / DM 구분을 겹침

```
models/
├── staging/
│   ├── stg_orders.sql
│   ├── stg_products.sql
│   └── stg_users.sql
├── intermediate/
│   ├── int_order_items_joined.sql
│   └── int_user_sessions.sql
└── marts/
    ├── core/              ← DW
    │   ├── fct_orders.sql
    │   ├── dim_products.sql
    │   └── dim_users.sql
    └── reporting/         ← DM
        ├── rpt_daily_revenue.sql
        └── rpt_user_retention.sql
```

| 영역 | 디렉토리 | 성격 |
|---|---|---|
| **DW** | `marts/core/` | 전사 표준 사실을 **재사용 가능한 형태**로. Fact / Dimension. grain 유지 |
| **DM** | `marts/reporting/` | 특정 목적에 맞춰 **반정규화·집계**. 대시보드용, 지표 테이블 |

> **왜 나누는가:** "이 지표를 주간 기준으로도 보고 싶다"는 요청이 왔을 때
> `core`를 건드리면 안 됨. 사실은 그대로이고 **보는 방식만 바뀐 것**이기 때문임.
> 나누지 않으면 요청이 올 때마다 Fact 테이블을 고치게 됨.


## 1-3. Materialization 4종

### 1-3-1. 정의

- 같은 SQL을 **어떤 물리 형태로 만들 것인가**를 정함

| 방식 | 동작 | 쓰는 상황 | 비용 |
|---|---|---|---|
| **view** | 뷰만 생성. 데이터 저장 없음 | 가볍고 자주 안 읽히는 모델. staging 기본값 | 저장 0, **조회할 때마다 연산** |
| **table** | 매번 전체 재생성 (CREATE OR REPLACE) | 로직이 복잡하고 자주 읽히는 모델 | 저장 O, **매 실행마다 전체 재계산** |
| **incremental** | 새/변경 데이터만 처리해 기존 테이블에 반영 | 큰 이벤트성 테이블 | 저장 O, **증분만 연산** |
| **ephemeral** | 테이블을 안 만들고 CTE로 인라인 | 여러 모델이 쓰는 짧은 중간 로직 | 저장 0, 참조하는 쿼리에 인라인 |

**incremental의 핵심은 `is_incremental()` 블록임.**

```sql
{{ config(materialized='incremental', unique_key='order_id') }}

select * from {{ ref('stg_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

- 최초 실행에는 전체를 만들고, 이후에는 조건절이 붙어 증분만 처리함
- `incremental_strategy`로 반영 방식을 고름 — `merge`, `insert_overwrite`, `append`, `delete+insert`
- **BigQuery에서는 파티션 단위 `insert_overwrite`가 특히 잘 맞음**

- **선택 기준**: 데이터 크기 × 갱신 빈도 × 조회 빈도
  - 작으면 view나 table로 단순하게 가고, 커지면 incremental로 감
- **처음부터 incremental로 시작하지 않음**
  - 복잡도가 붙기 때문임

### 1-3-2. 증분 모델에서 반드시 부딪히는 것들

**`on_schema_change` — 모델에 컬럼을 추가하면 어떻게 되는가**

- 증분 모델은 이미 만들어진 테이블에 덧붙이므로, SQL에 컬럼을 추가해도 기존 테이블 스키마는 그대로임
- 그 상황을 어떻게 처리할지 정하는 옵션임

| 값 | 동작 |
|---|---|
| `ignore` (기본) | **새 컬럼을 무시함.** 조용히 반영되지 않음 |
| `fail` | 스키마가 다르면 즉시 실패 |
| `append_new_columns` | 새 컬럼을 추가. **기존 행은 NULL** |
| `sync_all_columns` | 추가 + 삭제까지 반영 |

- **기본값이 `ignore`라는 게 함정임**
  - 컬럼을 추가하고 배포했는데 값이 안 들어오고, 에러도 안 남
  - `--full-refresh`를 돌려야 반영됨
  - 운영에서는 `fail`이나 `append_new_columns`로 바꿔 두는 편이 안전함

**`incremental_predicates` — merge 대상 범위를 좁힘**

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_predicates=["DBT_INTERNAL_DEST.ordered_date >= date_sub(current_date(), interval 7 day)"]
) }}
```

- `merge` 전략은 대상 테이블 전체를 스캔해 매칭함
- 과거 데이터가 바뀌지 않는다는 것을 알고 있다면 조건을 걸어 **스캔 범위를 줄임**
- 큰 테이블에서 비용 차이가 큼

**materialized view (dbt 1.6+)**

```sql
{{ config(materialized='materialized_view') }}
```

- 웨어하우스가 **자동으로 갱신하는 뷰**임
- dbt가 만드는 게 아니라 웨어하우스에 위임함
- `dbt run`을 돌리지 않아도 최신 상태가 유지되므로 배치 주기보다 신선해야 하는 집계에 씀
- 다만 웨어하우스마다 제약이 다르고(BigQuery는 지원 SQL이 제한적) 디버깅이 어려움

---

[목차](README.md) | [프로젝트 구조와 명령어 →](02-project-setup.md)
