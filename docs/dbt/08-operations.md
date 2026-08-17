# 실전 운영

> dbt  ·  [← dbt 앞단 — 원천에서 DW로](07-ingestion.md) | [목차](README.md) | [부록 →](09-appendix.md)

BigQuery 물리 설정으로 스캔 비용을 줄이는 방법, Slim CI, Semantic Layer, 그리고 장애 유형별 대응과 재처리 절차를 다룸.

## 8-1. dbt에서 BigQuery 물리 설정 — partition_by / cluster_by

### 8-1-1. 정의

- **BigQuery의 파티셔닝·클러스터링을 dbt config로 선언하는 방법**임
  - BigQuery는 스캔한 바이트로 과금하므로 이 설정이 비용에 직접 영향을 줌

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['user_id', 'event_name'],
    require_partition_filter=true
) }}
```

| 설정 | 역할 |
|---|---|
| `partition_by` | 스캔할 블록 자체를 잘라냄. **비용에 직접 영향** |
| `cluster_by` | 파티션 안에서 정렬. 필터·조인 효율 향상 (최대 4개) |
| `require_partition_filter` | 파티션 필터 없는 쿼리를 **거부**. 실수로 전체 스캔하는 것을 방지 |
| `insert_overwrite` | 해당 파티션만 통째로 교체 → **멱등** |

- **`insert_overwrite` + `partition_by` 조합이 핵심임**
  - 최근 N일을 통째로 다시 만드는 lookback 재처리가 정확히 이 형태가 됨


## 8-2. dbt CI/CD — Slim CI와 state 비교

### 8-2-1. 정의

- **문제**: 모델이 300개인데 PR에서 한 개만 고쳤음
  - 전부 빌드하면 CI가 30분 걸림

- **해결**: 이전 실행의 `manifest.json`과 비교해 **바뀐 것과 그 downstream만** 빌드함

```bash
dbt build --select state:modified+ --defer --state ./prod-manifest
```

| 옵션 | 역할 |
|---|---|
| `state:modified+` | 변경된 모델 **+ 그 downstream 전부** (`+`가 downstream 포함) |
| `--defer` | 안 바뀐 upstream는 **prod 것을 그대로 참조** (다시 안 만듦) |
| `--state` | 비교 기준이 되는 manifest 경로 |

- **선택 문법(selector)** 도 함께 알아둠
  - `--select tag:daily`, `--select stg_orders+`(downstream), `--select +fct_orders`(upstream), `--select path:models/marts`, `--exclude`

**전형적인 dbt CI 파이프라인**
- PR 생성 → 2. `dbt deps` → 3. `dbt build --select state:modified+ --defer` → 4. 실패하면 머지 차단 → 5. 머지 후 prod 실행 및 manifest 갱신


## 8-3. dbt Semantic Layer (MetricFlow)

### 8-3-1. 정의

- dbt의 Semantic Layer는 **지표를 테이블이 아니라 정의로 관리**함
- MetricFlow가 그 정의를 받아 쿼리 시점에 SQL을 생성함

```yaml
semantic_models:
  - name: orders
    model: ref('fct_orders')
    entities:
      - name: order_id
        type: primary
    dimensions:
      - name: ordered_at
        type: time
    measures:
      - name: revenue
        agg: sum

metrics:
  - name: total_revenue
    type: simple
    type_params: {measure: revenue}
```

- **핵심은 "미리 만들지 않는다"는 것임**
  - 일별·주별·월별 집계 테이블을 각각 만드는 대신 **지표 정의 하나를 두고 요청 시점에 조합**함
  - 집계 조합의 폭발을 막고, 정의가 한 곳에만 존재하게 함

| | **사전 집계 테이블 방식** | **dbt Semantic Layer** |
|---|---|---|
| 지표 정의 위치 | 각 테이블의 SQL | YAML 정의 한 곳 |
| 새 분해 축 추가 | 테이블 추가 | 정의에 dimension 추가 |
| 조회 속도 | 빠름 (미리 계산) | 쿼리 시점 계산 |
| 일관성 | 테이블 간 어긋날 수 있음 | 구조적으로 하나 |


## 8-4. 장애 대응과 재처리 — 운영의 실체

> **"파이프라인 운영"의 대부분은 만드는 게 아니라 깨졌을 때 복구하는 일임.**

### 8-4-1. 정의

**장애 유형별 대응**

| 유형 | 증상 | 대응 |
|---|---|---|
| **원천 미도착** | 배치는 성공, 데이터가 없음 | `source freshness` 경고 → 대기 또는 스킵 |
| **원천 오염** | 값이 들어왔는데 틀림 | 테스트 실패 → **downstream 차단**(`dbt build`) → 원천 수정 후 재처리 |
| **변환 버그** | 로직이 잘못됨 | 수정 후 **영향 구간 백필** |
| **인프라 실패** | 타임아웃, OOM | 재시도. 반복되면 모델 분할이나 리소스 조정 |

**재처리 절차 — 순서가 중요함**

- **범위 확정** — 어느 구간, 어느 모델부터인가
- **downstream 파악** — `dbt ls --select model+`로 영향 목록 확인
- **재처리** — `dbt build --select model+ --vars '{start: ..., end: ...}'`
- **검증** — 테스트 통과 + 재처리 전후 값 대조
- **기록** — 원인과 조치를 남김

**명령으로 보면**

```bash
# 1. 범위 확정 — 무엇이 틀렸고 어느 구간인가
dbt build --select fct_order_items --vars '{"run_date": "2026-07-15"}'

# 2. downstream 파악 — 이 모델을 고치면 무엇이 영향받나
dbt ls --select fct_order_items+
dbt ls --select +exposure:executive_revenue_dashboard   # 대시보드까지

# 3. 재처리 — 모델과 downstream 전부
dbt build --select fct_order_items+ --vars '{"run_date": "2026-07-15"}'

# Airflow 쪽에서 구간을 다시 돌릴 때
airflow tasks clear thelook_dw_daily \
    --task-regex "dbt_build_marts.*" --downstream \
    --start-date 2026-07-01 --end-date 2026-07-31

# 4. 검증 — 테스트 통과 + 재처리 전후 대조
dbt test --select fct_order_items+
```

**증분 모델을 통째로 다시 만들 때**

```bash
# 전체 재생성. 큰 테이블에서는 비용과 시간이 크다
dbt build --select fct_user_events --full-refresh

# 범위를 좁힐 수 있으면 좁힌다 — lookback 을 늘려 해당 구간만
dbt build --select fct_user_events --vars '{"run_date": "2026-07-15", "lookback_days": 14}'
```

- **실패 행을 눈으로 확인할 때** (`store_failures` 를 켠 테스트)

```sql
select * from `<project>.<target>_dbt_test__audit.assert_order_item_count_matches`
order by ordered_date desc
```

- **멱등성이 전제조건임**([Airflow 1-3](../airflow/README.md) 참조)
  - 재처리가 안전하지 않으면 위 절차 자체가 성립하지 않음

- **`--full-refresh` 주의**: incremental 모델을 통째로 다시 만듦
  - 큰 테이블에서는 비용과 시간이 크므로 **범위를 좁힐 수 있으면 좁힘**

---

[← dbt 앞단 — 원천에서 DW로](07-ingestion.md) | [목차](README.md) | [부록 →](09-appendix.md)
