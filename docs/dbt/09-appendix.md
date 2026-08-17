# 부록

> dbt  ·  [← 실전 운영](08-operations.md) | [목차](README.md)

Dataform 을 쓰던 경우의 개념 대응표와 구현 체크리스트.

## 9-1. Dataform 을 쓰던 경우 — 개념 대응표

> 이미 Dataform 으로 같은 문제를 풀어 본 경우 개념을 옮겨오기 위한 대조표.
> dbt 를 처음 접한다면 건너뛰어도 됨.

- **Dataform은 dbt와 같은 문제를 푸는 도구임**
  - Google이 인수해 GCP에 통합했음

| 개념 | Dataform | dbt |
|---|---|---|
| 모델 파일 | `.sqlx` (config 블록 + SQL) | `.sql` + 별도 `.yml` |
| 모델 참조 | `${ref("model_name")}` | `{{ ref('model_name') }}` |
| 원천 선언 | `declaration` 타입 | `source()` + `sources.yml` |
| 품질 검증 | `assertions` (uniqueKey, nonNull, rowConditions) | `tests` (generic + singular) |
| 재사용 로직 | `includes/`의 JavaScript 함수 | `macros/`의 Jinja 매크로 |
| 템플릿 언어 | JavaScript | Jinja2 |
| 증분 처리 | `type: "incremental"` + `${when(incremental(), ...)}` | `materialized='incremental'` + `is_incremental()` |
| 선택 실행 | `tags` | `tags` + `--select` 문법 |
| 의존성 그래프 | ref로 자동 구성 | 동일 |
| SCD Type 2 | **기능 없음** (쓰려면 유효구간 로직을 직접 작성해야 함) | `snapshot` 내장 |
| 패키지 생태계 | 사실상 없음 | dbt Hub (dbt_utils 등) |
| 문서/리니지 | 콘솔의 의존성 그래프 | `dbt docs` (manifest 기반) |

**같은 모델을 양쪽으로 쓰면**

```javascript
-- Dataform: orders.sqlx — config 와 SQL 이 한 파일에
config {
  type: "table",
  assertions: { uniqueKey: ["order_id"], nonNull: ["order_id", "user_id"] }
}
select order_id, user_id, created_at as ordered_at
from ${ref("stg_orders")}
```

```sql
-- dbt: models/marts/fct_orders.sql — SQL 만
{{ config(materialized='table') }}
select order_id, user_id, created_at as ordered_at
from {{ ref('stg_orders') }}
```

```yaml
# dbt: models/marts/_marts__models.yml — 선언은 별도 파일
models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: user_id
        data_tests: [not_null]
```

- **dbt 가 SQL 과 선언을 분리한 것이 가장 눈에 띄는 차이임**
  - 파일이 둘로 늘지만 YAML 쪽이 그대로 문서·카탈로그가 된다([5-2](05-macros-metadata.md) 참조)

- **같은 것이 훨씬 많음**
  - 다른 것은 snapshot, 패키지 생태계, 템플릿 언어 정도임


## 9-2. 구현 체크리스트

이 문서의 개념을 실제 프로젝트에 옮길 때 확인할 항목. 괄호는 해당 절 번호.

**모델링과 계층**

- [ ] staging / intermediate / marts 3계층 분리, **staging에서 조인·집계 없음** ([1-2](01-basics.md))
- [ ] `marts/core`(DW)와 `marts/reporting`(DM) 폴더 분리 ([1-2](01-basics.md))
- [ ] staging에서 `SELECT *` 금지 — 컬럼을 명시해 스키마 드리프트를 조기에 감지 ([7-5](07-ingestion.md))
- [ ] 네이밍 컨벤션 — `stg_` / `int_` / `fct_` / `dim_` / `rpt_` ([2-1](02-project-setup.md))
- [ ] `dbt_project.yml`에 계층별 기본 materialization 선언 ([2-1](02-project-setup.md))

**테스트**

- [ ] 게이트로 쓸 generic test — PK 유일성, not null, 값 도메인, 참조 무결성 ([3-1](03-testing.md))
- [ ] 여러 모델에 걸친 주장은 singular test로 ([3-1](03-testing.md))
- [ ] unit test 최소 1개 — 로직이 가장 복잡한 모델에 ([3-1](03-testing.md))
  - [ ] 증분 모델이면 `overrides`로 `is_incremental`을 고정
- [ ] 볼륨 이상 감지는 dbt test의 영역이 아님을 인지 ([3-2](03-testing.md))
  - [ ] 넣는다면 singular test로 전일·전주 대비를 비교하거나 elementary 같은 패키지를 얹음

**원천과 이력**

- [ ] `source()` 선언 + freshness 임계값 설정 ([1-2](01-basics.md))
- [ ] 변경 이력이 필요한 dimension에 snapshot(SCD Type 2) 또는 일자별 스냅샷 ([4-1](04-history.md))
  - [ ] 원천이 hard delete를 한다면 `invalidate_hard_deletes` 확인
- [ ] incremental 모델 + lookback 윈도우로 지연 도착 대응 (1-3, [7-2](07-ingestion.md))
- [ ] 증분 모델의 `on_schema_change`를 기본값 `ignore`에서 변경 ([1-3](01-basics.md))

**물리 설정과 비용**

- [ ] `partition_by` + `cluster_by`를 config로 선언 ([8-1](08-operations.md))
- [ ] `insert_overwrite` 전략으로 파티션 단위 멱등 재처리 ([8-1](08-operations.md))
- [ ] `maximum_bytes_billed` 같은 비용 상한 장치 ([8-1](08-operations.md))

**메타데이터와 협업**

- [ ] `dbt docs generate`로 리니지 그래프 확인 ([5-2](05-macros-metadata.md))
- [ ] exposure 선언 — 대시보드를 리니지에 연결하고 `dbt ls`로 영향도 확인 ([5-3](05-macros-metadata.md))
- [ ] contract를 downstream 소비가 넓은 모델 1~2개에. 전부에 걸지 않음 ([5-3](05-macros-metadata.md))
- [ ] `persist_docs`로 description을 웨어하우스까지 반영 ([5-3](05-macros-metadata.md))
- [ ] `profiles.yml`을 git에 올리지 않음 — `.gitignore` 확인 ([2-1](02-project-setup.md))
- [ ] CI에서 `dbt build --select state:modified+`로 변경분만 빌드 ([8-2](08-operations.md))
- [ ] README에 설계 판단 근거와 재처리 절차를 기록 ([2-1](02-project-setup.md), [2-2](02-project-setup.md))

## 관련 문서

- **[Airflow](../airflow/README.md)** — 이 변환을 언제 어떤 순서로 실행할 것인가

---

[← 실전 운영](08-operations.md) | [목차](README.md)
