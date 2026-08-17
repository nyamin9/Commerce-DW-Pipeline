# staging/ — 원천 컬럼명을 표준 이름으로

## 1. 개요

- 원천 테이블 하나당 모델 하나. 정제만 함
- 조인 금지, 집계 금지, `SELECT *` 금지. 전부 view
- 규칙이 셋이지만 목적은 하나 — 원천 컬럼명이 바뀌었을 때 손댈 곳을 한 파일로 몰아두는 것
- staging 위쪽은 표준화된 이름만 쓰므로, 원천이 바뀌어도 영향이 여기서 멈춤

여기서 실제로 하는 일은 이 정도.

```
lower(status)               원천이 'Complete'와 'complete'를 섞어 씀. downstream에서 매번 lower() 하지 않도록
cast(sale_price as numeric) FLOAT64 누적 오차로 합계가 어긋나는 것을 여기서 끊음
created_at → ordered_at     "언제 만들어졌나"가 아니라 "언제 주문됐나". 의미를 이름에 담음
date(created_at)            파티션 키·조인 키로 반복 사용되므로 한 번만 만듦
```

## 2. 구성

| 모델 | 원천 | 행 수 | 비고 |
|---|---|---:|---|
| `stg_thelook__orders` | orders | 124,500 | 주문 헤더 |
| `stg_thelook__order_items` | order_items | 180,790 | 매출의 원천 grain |
| `stg_thelook__products` | products | 29,120 | `updated_at` 없음 |
| `stg_thelook__users` | users | 100,000 | PII 다수 |
| `stg_thelook__events` | events | 2,422,900 | 가장 큼. `user_id` 46%가 NULL |
| `stg_thelook__inventory_items` | inventory_items | 488,306 | 상품 속성이 비정규화되어 함께 들어 있음 |
| `stg_thelook__distribution_centers` | distribution_centers | 10 | |

- `_thelook__sources.yml` — 원천 선언과 freshness 기준
- `_stg_thelook__models.yml` — 모델 설명과 테스트 42개

**이 계층에서 쓰이는 dbt 기능** — 개념은 [`docs/dbt/`](../../../docs/dbt/README.md)의 해당 절

| 기능 | 이 계층에서 | 개념 |
|---|---|---|
| `source()` | `_thelook__sources.yml`에 `raw_thelook` 7테이블을 선언. 모델은 전부 `source()`로 시작하고 `ref()`는 쓰지 않음 | [1-2](../../../docs/dbt/01-basics.md) |
| source freshness | `_ingested_at` 기준. 기본 warn 12h / error 36h, `events`만 6h / 24h로 조임. **`products`는 제외**(3번 참조) — 대상 6개 | [1-2](../../../docs/dbt/01-basics.md) |
| materialization — `view` | `dbt_project.yml` 기본값을 그대로 씀. 모델에 `config()` 재정의가 한 건도 없는 유일한 계층 | [1-3](../../../docs/dbt/01-basics.md) |
| generic test | `not_null` · `unique` · `accepted_values` · `relationships` 42개 | [3-1](../../../docs/dbt/03-testing.md) |
| YAML 문서화 | `_stg_thelook__models.yml`의 description이 `dbt docs`의 카탈로그가 됨 | [5-2](../../../docs/dbt/05-macros-metadata.md) |

## 3. 고려사항

- **raw 레이어를 한 단계 둔 이유**
  - 처음엔 공개 데이터셋을 `source()`로 바로 가리켰음. 그래도 동작은 함
  - 막힌 건 freshness를 걸려던 순간. 원천에 적재 시각 컬럼이 없어 "배치는 성공했는데 데이터가 안 들어왔다"를 판별할 근거가 없었음
  - Airflow가 `raw_thelook`으로 한 번 받아 적으면서 `_ingested_at`을 찍도록 함
  - 부수 효과 둘 — EL과 T의 경계가 코드에 드러남, 원천을 바꿀 수 있게 되어 snapshot이 이력을 쌓는지 확인 가능해짐
  - → 다음 프로젝트: 원천에 적재 시각이 있는지 먼저 확인. 없으면 랜딩 레이어를 두는 비용이 정당화됨

- **staging을 언제 table로 내리는가**
  - 지금은 전부 view. 저장하지 않고 컬럼명만 바꾸므로 그게 기본값
  - 다만 BigQuery에서 view는 조회 시점에 전개됨. mart 다섯 개가 같은 staging view를 참조하면 원천 스캔도 다섯 번
  - "staging은 view"는 규칙이 아니라 출발점. 아래가 겹치면 table로 내림
    - downstream 참조가 3개를 넘고
    - 원천이 크고 (수억 행대)
    - staging의 변환이 가벼워 저장 비용보다 재스캔 비용이 클 때
  - 이 프로젝트는 원천이 최대 2.4M행이라 아직 그 지점이 아님
  - → 다음 프로젝트: 스캔 과금 웨어하우스에서는 downstream 참조 수가 곧 비용 배수. "staging은 view"를 규칙으로 외우지 말 것

- **PII를 여기서 한 번 끊음**
  - `stg_thelook__users`에서 `street_address`와 좌표를 아예 읽지 않음
  - 분석에 쓸 일이 없는데 downstream로 내려보내면 마트마다 마스킹 판단을 반복해야 하고, 한 곳이라도 빠뜨리면 그게 유출 경로가 됨
  - 반면 `city` / `state` / `country`는 지역 분석에 필요하니 남김
  - `email`과 `ip_address`는 여기까지가 마지막 원문. 실제 처리는 마트에서 함
  - 계층별 판단은 [../README.md](../README.md) 참조

- **`products`만 freshness 검사에서 뺐음**
  - 원천 7개 중 이 테이블만 EL 전략이 `merge_insert_only`임 → [../../../airflow/README.md](../../../airflow/README.md)
  - 신규 상품이 없는 날은 기존 행을 건드리지 않으므로 `_ingested_at`이 갱신되지 않음. **정상 동작인데 시간이 갈수록 stale로 잡힘**
  - 실제로 42시간까지 벌어져 `dbt source freshness`가 실패했고, 그 태스크가 리프라서 DAG 전체가 빨갛게 떴음
  - `loaded_at_field`가 재는 건 "마지막으로 **신규 상품을 본** 시각"이지 "마지막으로 **확인한** 시각"이 아님. 적재 전략과 신선도의 정의가 어긋나 있음
  - `error_after`를 늘리는 건 답이 아님 — 늦게 틀릴 뿐 언젠가 반드시 걸림
  - → 다음 프로젝트: 신선도를 걸기 전에 **그 테이블의 적재 전략이 그 값을 갱신하긴 하는가**를 확인할 것. 재지 못하는 걸 재면 경보가 아니라 소음이 됨
  - 남는 공백: "언제 확인했는가"는 여전히 측정하지 않음. 그게 필요하면 EL이 별도 컬럼이나 감사 테이블로 남겨야 함

- **채널 값이 원천마다 다름 (발견 사항)**
  - `users.traffic_source` — Display / Email / Facebook / Organic / Search
  - `events.traffic_source` — Adwords / Email / Facebook / Organic / YouTube
  - 이름은 같은데 값 집합이 겹치지 않는 부분이 있음. 두 테이블을 채널로 조인하면 조용히 어긋남
  - `accepted_values` 테스트를 각각 따로 걸어 이 사실을 코드에 박아 둠
  - 표준화는 필요하지만 마트가 아니라 이 계층 위에서 해야 함. 마트에서 각자 매핑하면 매핑 규칙이 여러 벌 생김

## 4. 실행

```bash
dbt build --select staging                # 모델 7 + 테스트 42
dbt source freshness                      # 원천 지연 확인 (빌드와 분리)
dbt build --select stg_thelook__orders+   # 이 모델과 downstream 전부
```
