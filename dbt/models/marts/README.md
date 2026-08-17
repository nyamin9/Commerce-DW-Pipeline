# marts/ — 소비 계층

## 1. 개요

- 두 영역으로 나뉨. 이 프로젝트에서 가장 중요한 결정
  - `core/` — **DW**. 전사 표준 fact / dimension 을 재사용 가능한 형태로, grain 을 유지
  - `reporting/` — **DM**. 특정 목적에 맞춰 반정규화·집계
- 판단 기준은 한 문장 — "이 지표를 주간 기준으로도 보고 싶다"는 요청이 오면 어디를 고치나
  - 답은 `reporting`. 사실은 그대로이고 보는 방식만 바뀐 것이므로
  - 나누지 않으면 요청이 올 때마다 fact 테이블을 고치게 되고, fact가 바뀌면 그 위 숫자가 전부 바뀜

## 2. 구성

**core/ — DW (8 모델)**

| 모델 | 1행이 뜻하는 것 | materialization |
|---|---|---|
| `fct_order_items` | 주문 상세 1건 | table (파티션 + 클러스터) |
| `fct_orders` | 주문 1건 | table |
| `fct_user_events` | 이벤트 1건 | 증분 (`insert_overwrite`) |
| `fct_sessions` | 세션 1건 | table |
| `dim_users` | 회원 1명 | table · contract 강제 |
| `dim_products` | 상품 1개 (현재 상태) | table |
| `dim_products_history` | 상품 1개 × 유효 구간 | table (SCD Type 2) |
| `dim_distribution_centers` | 물류센터 1개 | table |

**reporting/ — DM (3 모델)**

| 모델 | 1행이 뜻하는 것 | 대응 |
|---|---|---|
| `rpt_daily_revenue` | 일자 × 부서 | 매출 · 누적 · 전년비 |
| `rpt_daily_funnel` | 일자 × 유입채널 | AARRR의 Activation → Revenue |
| `rpt_user_cohort_retention` | 가입 코호트 × 경과월 | AARRR의 Retention |

**이 계층에서 쓰이는 dbt 기능** — 개념은 [`docs/dbt/`](../../../docs/dbt/README.md)의 해당 절

| 기능 | 이 계층에서 | 개념 |
|---|---|---|
| `ref()` | staging · intermediate · snapshot을 참조. `source()`는 쓰지 않음 | [1-2](../../../docs/dbt/01-basics.md) |
| materialization — `table` | 계층 기본값은 table. `fct_user_events`만 `incremental` + `insert_overwrite` | [1-3](../../../docs/dbt/01-basics.md) |
| `partition_by` / `cluster_by` | fact 4개는 날짜 파티션 + `user_id` 클러스터, dimension은 클러스터만 (`dim_distribution_centers`는 10행이라 없음) | 7-1 |
| contract + `constraints` | `dim_users` · `fct_order_items` 둘만. 컬럼이 빠지거나 타입이 바뀌면 빌드가 실패 | [5-3](../../../docs/dbt/05-macros-metadata.md) · [3-1](../../../docs/dbt/03-testing.md) |
| generic test | reporting 세 모델에 `unique_combination_of_columns` — grain을 코드에 고정 | [3-1](../../../docs/dbt/03-testing.md) |
| snapshot 소비 | `dim_products_history`가 `snap_products`를 감싸 도구 컬럼명을 가림 | [4-1](../../../docs/dbt/04-history.md) |
| `exposure` | 대시보드 2 · 분석 1을 lineage에 연결. `maturity`로 사전 공지 대상을 구분 | [5-3](../../../docs/dbt/05-macros-metadata.md) |

## 3. 고려사항

- **fact가 왜 네 개인가 — grain이 다름**
  - 카테고리별 매출은? → `fct_order_items` (매출의 기준 grain)
  - 주문 건수와 객단가는? → `fct_orders`
  - 어떤 이벤트가 몇 번 일어났나? → `fct_user_events`
  - 세션당 전환율과 이탈률은? → `fct_sessions`
  - `fct_orders`에는 상세를 집계해 함께 실었음. "주문 금액"을 물을 때마다 `order_items`를 합산하게 하면 취소·반품을 뺄지가 사람마다 달라짐
  - 그 기준은 `int_order_items_enriched` 한 곳에만 둠
  - 원천이 준 `num_of_item`과 실제 상세 행 수를 둘 다 실었고, 어긋나는 주문은 singular test가 감시

- **fact에 서술 속성을 심지 않음**
  - `rpt_daily_revenue`는 부서명이 필요하면 `fct_order_items`에 `dim_products`를 조인
  - fact에 부서명을 미리 넣어 두면 상품 분류가 바뀔 때 과거 fact까지 다시 써야 함
  - → 다음 프로젝트: 마트를 하나로 두고 요청마다 컬럼을 붙이면 곧 되돌리기 어려워짐. 사실을 담는 층과 목적에 맞춘 층을 처음부터 폴더로 갈라둘 것

- **dim_products와 dim_products_history를 나눈 이유**
  - 조회의 대부분은 "지금 이 상품이 무엇인가"를 물음
  - 그 질문에 매번 유효 구간 조건(`valid_from <= x < valid_to`)을 붙이게 하면 쿼리가 길어지고, 언젠가 조건을 빠뜨려 이력 행까지 세면서 중복 집계가 남
  - snapshot 테이블을 그대로 노출하지도 않음. `dbt_valid_from`이나 `dbt_scd_id`는 도구의 구현 세부사항이라, 도구를 바꾸면 컬럼명이 바뀌고 거기 붙은 대시보드가 같이 깨짐
  - `dim_products_history`가 `valid_from` / `is_current`로 감쌈
  - 특정 시점 가격으로 매출을 다시 보려면

    ```sql
    join dim_products_history h
      on f.product_id = h.product_id
     and f.ordered_at >= h.valid_from
     and (h.valid_to is null or f.ordered_at < h.valid_to)
    ```

  - → 다음 프로젝트: 도구가 만든 컬럼명을 소비자에게 그대로 노출하지 말 것. 도구를 바꿀 때 발목을 잡음

- **contract를 두 모델에만 건 이유**
  - `dim_users`와 `fct_order_items`에만 `contract: {enforced: true}`. 컬럼이 빠지거나 타입이 바뀌면 빌드 실패
  - 전부에 걸지 않은 건 contract가 공짜가 아니어서. 모든 컬럼의 `data_type`을 선언해야 하고, 모델을 고칠 때마다 YAML도 같이 고쳐야 함
  - 그 비용은 downstream 소비가 넓은 모델에서만 비용을 들일 만함. 이 둘은 각각 PII 경계와 매출 기준 grain이라 조용한 변경의 피해가 가장 큼

- **constraints는 테스트가 아님**
  - contract를 건 모델에는 `data_tests:`와 `constraints:`가 함께 있는데 성격이 다름

    ```yaml
    - name: user_id
      data_type: int64
      constraints: [{type: not_null}]   # DDL로 내려감. 테스트 노드가 아님
      data_tests: [unique]              # 테스트 노드가 됨
    ```

  - `constraints`는 `CREATE TABLE ... (user_id int64 not null)`로 렌더링되어 테이블을 만드는 중에 웨어하우스가 강제
  - 위반하면 테스트가 실패하는 게 아니라 테이블 생성 자체가 실패함
  - BigQuery가 강제할 수 있는 constraint는 `not_null`뿐. `unique`는 지원하지 않고 `primary_key` / `foreign_key`는 메타데이터로만 기록됨
  - 유일성과 참조 무결성을 테스트로 할 수밖에 없는 이유가 여기 있음. 자세한 비교는 [../../tests/README.md](../../tests/README.md)

- **dim_users의 PII 처리**
  - 이름 — 내리지 않음. 분석에 쓸 일이 없음
  - `email` — SHA-256 해시와 도메인만. 원문은 이 계층에 없음
  - `user_id` — 해싱하지 않음. 내부 대리키라 그 자체로 개인을 식별하지 못하고, 해싱하면 이미 적재된 fact와의 조인이 전부 깨짐. 바꾸려면 전 계층을 동시에 바꿔야 함
  - `email_domain`을 남긴 건 원문 없이도 "회사 메일 가입자 비중" 같은 분석이 살아 있게 하려는 것
  - 핵심은 지우느냐가 아니라 분석 가능성과 노출 범위를 어디서 맞바꿀지

- **reporting의 daily → 누적 → 비교 구조**
  - `rpt_daily_revenue`는 모든 지표가 같은 모양 — `net_revenue` / `_wtd` / `_mtd` / `_ytd` / `_yoy_rate`
  - 새 지표가 붙어도 소비자가 컬럼 이름 규칙을 다시 배우지 않아도 됨
  - 다만 누적 컬럼은 매 실행마다 전체를 다시 계산. 지금 규모(약 7년 × 2부서)에서는 문제없지만, 분해 축이 늘거나 기간이 길어지면 전일 누적값에 당일치를 더하는 방식으로 바꿔야 함

- **전환율 분모를 둘 다 내는 이유**
  - `rpt_daily_funnel`은 단계별 전환율(직전 단계 대비)과 전체 전환율(세션 대비)을 둘 다 냄
  - 하나만 내면 반드시 다른 하나를 각자 계산하기 시작하고, 그 순간부터 "전환율"이라는 같은 이름의 서로 다른 숫자가 돌아다님
  - → 다음 프로젝트: 분모가 갈릴 여지가 있는 지표는 두 버전을 다 내고 이름으로 구분할 것

- **grain을 코드에 고정**
  - reporting 세 모델 모두 `unique_combination_of_columns`를 걸었음
  - grain이 깨지면 대시보드에서 매출이 두 배로 보이는데, 그건 눈으로 잘 안 잡힘

- **exposure로 lineage를 대시보드까지 연장**
  - dbt docs의 기본 lineage는 모델에서 끝남. 그 아래에 무엇이 붙어 있는지 모르니 "이 컬럼 지워도 되나요"에 답할 수 없음
  - exposure를 선언하면 그 답이 명령 한 줄이 됨

    ```bash
    dbt ls --select +exposure:executive_revenue_dashboard   # 이 대시보드가 의존하는 전부
    dbt ls --select rpt_daily_revenue+                      # 이 모델을 바꾸면 영향받는 것
    ```

  - 스키마 변경 전 downstream 확인이 사람의 기억이 아니라 절차가 되는 게 요점
  - `maturity`(high / medium / low)로 사전 공지가 필요한 대상을 구분함

## 4. 실행

```bash
dbt build --select marts.core          # DW (모델 8 + 테스트 36)
dbt build --select marts.reporting     # DM (모델 3 + 테스트 11)
dbt build --select fct_order_items+    # 이 fact와 downstream 전부
dbt ls --select +exposure:executive_revenue_dashboard
```
