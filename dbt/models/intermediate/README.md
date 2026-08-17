# intermediate/ — 공통 로직을 한 번만

## 1. 개요

- 여러 mart가 공통으로 쓰는 조인과 전처리를 여기 한 번만 둠
- 최종 소비자에게는 노출하지 않음
- 성능이 아니라 정합성을 위한 계층. `fct_order_items`와 `rpt_daily_revenue`가 각자 같은 조인을 하면 나중에 한쪽만 고쳐져 매출 숫자가 갈라짐
- `is_revenue_recognized`(취소·반품을 매출에서 뺄 기준)와 `gross_profit`(매출 − 원가)의 정의가 여기 한 곳에만 있음

## 2. 구성

| 모델 | materialization | 하는 일 |
|---|---|---|
| `int_order_items_enriched` | view | 주문 상세 + 헤더 + 상품 속성 조인, 마진·매출인정 기준 계산 |
| `int_events_sessionized` | 증분 테이블 | 행동 로그에 자체 session ID 부여 |

**이 계층에서 쓰이는 dbt 기능** — 개념은 [`docs/dbt/`](../../../docs/dbt/README.md)의 해당 절

| 기능 | 이 계층에서 | 개념 |
|---|---|---|
| `ref()` | staging 모델만 참조. `source()`를 직접 읽지 않음 — 원천 접점을 한 계층에 묶어 두려는 것 | [1-2](../../../docs/dbt/01-basics.md) |
| materialization 재정의 | 계층 기본값은 view, `int_events_sessionized`만 `config()`로 `incremental` | [1-3](../../../docs/dbt/01-basics.md) |
| `is_incremental()` | 최초 실행은 전체 CTAS, 이후 실행만 lookback 구간으로 좁힘 | [1-3](../../../docs/dbt/01-basics.md) |
| `insert_overwrite` + `partition_by` | `event_date` 파티션을 통째로 교체. `cluster_by=['user_id']` | 7-1 |
| macro · `var()` | `get_lookback_start_date(extra_days=1)`로 버퍼 구간을 계산, `session_timeout_minutes`로 세션 기준을 뺌 | [5-1](../../../docs/dbt/05-macros-metadata.md) |
| unit test | `test_session_splits_on_inactivity_gap`. 고정 입력으로 세션 경계 판정 로직만 검증 | [3-1](../../../docs/dbt/03-testing.md) |

## 3. 고려사항

- **계층을 미리 만들지 않음**
  - `int_order_items_enriched`는 두 번째 mart가 같은 조인을 필요로 했을 때 올렸음
  - 처음부터 만들었다면 통과만 하는 빈 계층이 됐을 것
  - → 다음 프로젝트: 계층이 있다는 사실 자체가 도움이 되는 게 아니라, 중복을 실제로 제거할 때만 의미가 있음

- **ephemeral을 쓰지 않은 이유**
  - dbt 관례는 intermediate를 ephemeral이나 view로 두는 것
  - ephemeral은 테이블을 만들지 않고 참조하는 쿼리에 CTE로 인라인됨
  - 중첩되면 컴파일된 쿼리가 부풀고, 직접 조회가 안 되니 디버깅할 방법이 사라짐
  - 세션화처럼 로직이 복잡한 모델이 ephemeral이면 문제가 생겼을 때 들여다볼 수가 없음

- **왜 하나만 증분 테이블인가**
  - `int_events_sessionized`만 view 규칙에서 예외
  - 2.4M행 위에서 윈도우 함수를 두 번 돌림. view면 downstream인 `fct_user_events`와 `fct_sessions`가 참조할 때마다 그 계산이 통째로 다시 일어남
  - 계층 규칙을 지켜서 얻는 것보다 재계산 비용이 컸음
  - 참고로 이 모델이 파이프라인에서 가장 비싼 연산. 분산환경이라면 `user_id` 기준 셔플이 여기서 발생하고, 헤비 유저에게 데이터가 쏠리면 그 파티션이 병목이 됨. BigQuery는 이 리소스 관리를 추상화해 겉으로 드러내지 않을 뿐

- **session ID를 다시 만든 이유**
  - 원천 `events`에 `session_id`가 이미 있는데도 자체 세션화를 함
  - 세션 기준이 분석 목적에 따라 달라져야 함. 무활동 30분이 유일한 정답은 아니라 `session_timeout_minutes` 변수로 뺌
  - 원천 session ID의 정확성은 우리가 통제할 수 없음
  - 원천 값을 버리지는 않음. `source_session_id`로 함께 들고 가고, `fct_sessions.source_session_id_count`가 자체 세션 하나가 원천에서 몇 개로 쪼개져 있는지를 숫자로 남김
  - 둘의 차이를 주장이 아니라 측정으로 말하기 위함

- **증분이 키 설계를 바꾼 과정** — 이 프로젝트에서 가장 배울 게 많았던 지점
  - 처음에는 일련번호로 session ID를 만들었음

    ```sql
    sum(is_session_start) over (partition by user_id order by event_at
                                rows between unbounded preceding and current row)
    ```

  - 증분으로 옮기려는 순간 깨짐. 일련번호는 유저의 전체 이력이 있어야 계산됨
  - 최근 3일치만 읽으면 번호가 1부터 다시 시작하고, 어제 만든 session ID와 값이 달라짐. 같은 세션인데 날마다 다른 ID가 붙는 것
  - 그래서 ID를 "세션의 첫 이벤트 시각"으로 바꿈 — `{user_id}-{yyyymmddhhmmss}` (예: `12345-20260815103000`)
  - 버퍼 구간만 읽어도 같은 값이 나오고, 몇 번을 다시 돌려도 변하지 않음
  - → 다음 프로젝트: 증분으로 갈 가능성이 있으면 키 설계를 먼저 검토할 것. "전체 이력이 있어야 계산되는 값"은 증분과 함께 갈 수 없음

- **버퍼가 필요한 이유**
  - 대상 구간의 첫 이벤트는 직전 이벤트가 있어야 "새 세션인지"를 판정할 수 있음
  - 그래서 대상보다 하루 더 읽음

    ```
    읽기:  [run_date - 4, run_date]     판정용 버퍼 포함
    출력:  [run_date - 3, run_date]     이 구간만
    ```

  - 버퍼 날짜를 출력에서 걸러내지 않으면 `insert_overwrite`가 그 파티션까지 불완전한 내용으로 덮어씀. 실제로 한 번 밟은 함정

## 4. 실행

```bash
dbt build --select intermediate
dbt test --select test_type:unit                 # 세션화 로직 검증
dbt build --select int_events_sessionized --vars '{"run_date": "2026-08-01"}'
dbt compile --select int_events_sessionized      # 생성될 SQL 확인
```
