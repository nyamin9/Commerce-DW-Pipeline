# tests/ — 데이터 품질 검증

## 1. 개요

- 이 폴더에 있는 건 singular test 4개뿐. 그런데 프로젝트 전체 테스트는 102개
- dbt는 `unique`나 `not_null` 같은 선언을 컬럼 하나당 테스트 노드 하나로 셈. YAML 한 줄이 곧 테스트 1개

| 종류 | 개수 | 선언 위치 |
|---|---:|---|
| `not_null` | 43 | 각 계층의 `_*__models.yml` |
| `unique` | 17 | 〃 |
| `expression_is_true` | 12 | 〃 |
| `relationships` | 12 | 〃 |
| `accepted_values` | 10 | 〃 |
| `unique_combination_of_columns` | 3 | 〃 |
| singular | 4 | 이 폴더의 `.sql` 파일 |
| unit | 1 | `models/intermediate/_int__models.yml` |

- 직접 작성한 테스트는 5개(singular 4 + unit 1). 나머지 97개는 YAML 선언이고 그중 60개(not_null 43 + unique 17)가 PK 유일성과 필수값
- 모델 20개에 컬럼별 기본 검증을 걸면 자연히 이 숫자가 됨

계층별 분포.

```
staging          42     원천이 깨지면 여기서 먼저 걸려야 함
marts/core       36     downstream 소비가 가장 넓음
marts/reporting  11     grain 고정 위주
intermediate      8
직접 작성          4
```

- staging이 가장 많은 건 오염이 downstream로 퍼지기 전에 끊으려는 것
- `dbt build`는 모델을 만든 직후 그 모델의 테스트를 돌리고, 실패하면 downstream로 내려가지 않음. 그래서 upstream의 테스트가 효과가 가장 큼

## 2. 구성

**generic / singular / unit — 검증 대상이 다름**

| | generic | singular | unit |
|---|---|---|---|
| 검증 대상 | 데이터 | 데이터 | 변환 로직 |
| 형태 | YAML 선언 | 실패 행을 반환하는 SQL | 고정 입력 → 기대 출력 |
| 원천 데이터 | 필요 | 필요 | 불필요 |
| 재사용 | 여러 모델에 | 그 상황 하나 | 그 모델 하나 |

**이 폴더의 singular test 4개**

- `assert_order_item_count_matches.sql` — 원천이 준 `orders.num_of_item`과 실제 `order_items` 행 수가 일치하는지. `fct_orders`가 두 값을 다 싣고 있어 비교 가능
- `assert_no_order_before_signup.sql` — 가입 시각보다 이른 주문이 있는지. `rpt_user_cohort_retention`이 이미 걸러내고 있지만, 걸러낸 양이 늘고 있다면 원천 쪽 문제
- `assert_daily_order_volume_not_anomalous.sql` — 일별 주문 건수가 같은 요일 직전 12주 대비 정상 범위인지. 요일을 맞추는 건 주중/주말 패턴이 요일 무시 평균을 왜곡해서
- `assert_fact_not_silently_truncated.sql` — `fct_orders`의 행 수가 `stg_thelook__orders`와 같은지. 웨어하우스가 우리가 쓴 행을 나중에 지우는 상황을 잡음

**unit test 1개** (`models/intermediate/_int__models.yml`)

- 로직이 가장 복잡한 세션화에 걸었음. 검증하는 건 셋
  - 무활동 30분을 넘기면 세션이 갈리고, 넘지 않으면 묶임
  - 다른 유저의 이벤트가 세션 경계에 영향을 주지 않음
  - 비로그인 트래픽(`user_id is null`)이 제외됨

## 3. 고려사항

- **constraints는 102개에 포함되지 않음**
  - `models/marts/core/_core__models.yml`의 `dim_users`와 `fct_order_items`에는 `data_tests:` 말고 `constraints:` 선언도 있는데, 이건 테스트가 아님

  | | `data_tests:` | `constraints:` |
  |---|---|---|
  | 만들어지는 것 | 테스트 노드 | DDL |
  | 검사 시점 | 모델을 만든 뒤 SELECT로 | 모델을 만드는 중 웨어하우스가 |
  | 실패하면 | 테스트 FAIL → downstream 차단 | 테이블 생성 자체가 실패 |
  | `dbt test` 개수 | 포함 | 미포함 |
  | 위반 행 확인 | 가능 (`store_failures`) | 불가 — 잡 에러만 남음 |
  | 전제 조건 | 없음 | `contract: {enforced: true}` |

  - `fct_order_items`는 `not_null` constraint를 6개 컬럼에 걸었는데 not_null 테스트는 0개. 대신 생성 DDL에 박힘

    ```sql
    create or replace table ... (
        order_item_id int64 not null,     -- constraint가 만든 것
        order_id      int64 not null,
        ...
    )
    ```

  - BigQuery `INFORMATION_SCHEMA.COLUMNS`에서 `is_nullable = NO`로 확인됨

- **BigQuery는 not_null만 강제함**

  | constraint | BigQuery |
  |---|---|
  | `not_null` | ENFORCED — DDL로 실제 강제 |
  | `primary_key` / `foreign_key` | NOT_ENFORCED — 메타데이터로만 기록 |
  | `unique` / `check` | NOT_SUPPORTED |

  - 그래서 `dim_users.user_id`에 둘이 함께 붙어 있음 — `not_null`은 constraint로 DDL에, `unique`는 웨어하우스가 강제 못 하니 테스트로
  - 유일성과 참조 무결성은 반드시 `data_tests`여야 함
  - 반대로 contract를 건 두 모델에는 not_null 테스트를 일부러 걸지 않음. DDL이 이미 막으므로
  - 43개 not_null 테스트는 contract를 안 건 나머지 18개 모델에서 나옴
  - → 다음 프로젝트: 웨어하우스가 어떤 constraint를 실제로 강제하는지 먼저 확인할 것. 선언은 받아주면서 강제는 안 하는 경우가 있고, 그러면 "걸어놨으니 괜찮다"가 착각이 됨

- **웨어하우스가 우리가 쓴 행을 지울 수 있음**
  - `assert_fact_not_silently_truncated`는 데이터를 의심하는 테스트가 아니라 **웨어하우스를 의심하는 테스트**임
  - BigQuery 샌드박스는 데이터셋마다 `default_partition_expiration_ms = 60일`을 자동으로 검. 결제를 연결해도 이미 걸린 데이터셋에는 그대로 남음
  - 파티션 테이블에 과거 데이터를 써도 기준일보다 오래된 파티션이 삭제됨

    ```
    dbt 가 모델을 씀       →  fct_orders 에 125,158 행 기록
    웨어하우스가 만료 적용  →  60일 이전 파티션 삭제, 16,108 행만 남음
    dbt 가 테스트를 돌림    →  잘린 테이블을 봄
    ```

  - **dbt는 성공으로 보고함.** 자기가 쓴 행 수를 다시 세지 않으므로
  - 실제로 밟았고, 증상이 엉뚱하게 나타남 — `relationships` 위반 230건과 `unique` 위반 4,101건. 원천을 아무리 봐도 고아 0·중복 0이라 데이터 문제로 보이지 않음
  - 고아 230건의 기전은 확정됨. `fct_orders`는 주문일, `fct_order_items`는 상세일로 파티셔닝되는데 둘이 45% 다름. 같은 날짜로 잘라도 **부모만 사라진 자식**이 생김
  - staging을 기준선으로 쓴 건 view라 저장하지 않아 만료의 영향을 받지 않기 때문. **잘리지 않는 유일한 계층**
  - → 다음 프로젝트: "모델이 성공했다"와 "데이터가 거기 있다"는 다른 명제임. 스토리지 정책(만료·보존·수명주기)이 있는 환경에서는 둘을 잇는 테스트를 따로 둘 것

- **unit test가 나머지 둘과 근본적으로 다름**
  - generic과 singular는 "지금 들어온 데이터가 규칙을 지키는가"를 물음
  - unit test는 "이 SQL이 내가 의도한 대로 계산하는가"를 물음
  - 원천 데이터 없이 돌기 때문에 로직을 고칠 때마다 즉시 실행 가능

- **severity를 나눈 기준 — 우리 변환의 버그인가, 원천의 문제인가**

  | 테스트 | severity | 근거 |
  |---|---|---|
  | PK 유일성 · not null · 참조 무결성 · 값 도메인 | `error` | 깨지면 downstream 숫자가 틀림. 막아야 함 |
  | `assert_no_order_before_signup` | `error` | 시간 순서가 뒤집힌 값. 코호트 분석을 조용히 망가뜨림 |
  | `assert_order_item_count_matches` | `warn` | 원천 정합성 문제. 매출은 `order_items` 기준이라 값이 틀리진 않음 |
  | `assert_daily_order_volume_not_anomalous` | `warn` | 이상 신호일 뿐 오류가 아님. 사람이 판단할 몫 |
  | `assert_fact_not_silently_truncated` | `error` | 마트에 있어야 할 행이 없다는 뜻. 그 위 숫자가 전부 틀림 |

  - 전부 `error`면 원천이 어긋난 날 마트 전체가 멈춤. 전부 `warn`이면 게이트가 사라짐
  - 막을 것과 알릴 것을 가르는 게 핵심
  - `warn`인 둘은 `store_failures: true`를 켬. 한 번의 실패보다 누적 추세가 중요한 종류라 언제부터 몇 건씩 어긋났는지 볼 수 있어야 함
  - 실패 행은 `<target>_dbt_test__audit` 데이터셋에 쌓임

- **이상탐지를 dbt test에 넣은 것의 한계**
  - `assert_daily_order_volume_not_anomalous`는 사실 여기 있으면 안 되는 종류
  - dbt test는 사람이 미리 선언한 규칙을 검사하는 도구. 규칙은 데이터를 보지 않고도 쓸 수 있음
  - 그런데 "오늘 주문이 평소보다 적다"는 미리 쓸 수 없음. 정상 범위가 데이터에서 나오기 때문
  - 기준선 기반 감시이고, 원래는 파이프라인 밖에서 자기 주기로 도는 영역
  - 그래도 넣은 이유 둘 — 규칙 기반과 기준선 기반의 차이를 코드로 남기려고, 이 규모에서는 별도 감시 체계 비용이 이득을 넘어서므로
  - 한계 두 가지가 실제로 관측됨
    - 빌드에 붙어 있음. 빌드가 실패한 날은 아무것도 감지하지 못함. 파이프라인이 멈춘 날이 가장 위험한 날인데 하필 그때 감시가 멈춤
    - z-score 기준선은 추세를 반영하지 못함. 이 데이터셋에서 z가 21~49로 3일 연속 걸렸는데 장애가 아니라 주문량이 우상향한 것
  - 기준을 고치지 않고 그대로 둠. "경보가 울린다"와 "문제가 있다"는 다르고, 그 간극이 이상탐지 설계의 핵심이라 드러내는 편이 낫다고 봤음
  - 운영에서는 셋 중 하나를 택함
    - 추세를 제거하고(전주 대비 증감률 등) 그 위에서 이탈을 봄
    - 기준선 창을 짧게 잡아 최근 수준을 따라가게 함
    - 표준편차 대신 변동폭(min~max) 이탈로 판정 — 추세에 덜 민감함
  - → 다음 프로젝트: 이상탐지를 붙이기 전에 대상 지표가 성장 추세인지부터 확인할 것. 우상향하는 지표에 고정 기준선을 걸면 매일 울리고, 매일 울리는 알림은 곧 무시됨

## 4. 실행

```bash
dbt test                                   # 전체 102개
dbt test --select staging                  # 계층 단위
dbt test --select fct_order_items          # 모델 단위
dbt test --select test_type:unit           # unit test만
dbt test --select test_type:singular       # 이 폴더의 4개만
dbt build                                  # 모델 + 테스트를 계층 순서대로 (게이트 동작)

# 실패 행 확인 (store_failures를 켠 테스트)
bq query 'select * from `<project>.<target>_dbt_test__audit.assert_order_item_count_matches`'
```

- `dbt test`는 이미 만들어진 테이블을 검사만 함
- `dbt build`는 모델을 만든 직후 그 모델의 테스트를 돌려 실패하면 downstream를 막음
