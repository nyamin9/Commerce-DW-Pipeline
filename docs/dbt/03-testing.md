# 테스트와 데이터 품질

> dbt  ·  [← 프로젝트 구조와 명령어](02-project-setup.md) | [목차](README.md) | [이력 적재 →](04-history.md)

generic / singular / unit 세 종류의 테스트가 각각 무엇을 검증하는지, 그리고 dbt test 로는 잡히지 않는 영역(기준선 기반 감시)이 무엇인지 다룸.

## 3-1. Test — generic / singular / unit

### 3-1-1. 정의

- **generic test**는 재사용 가능한 테스트임
  - YAML에 선언함
  - 기본 제공은 네 개이고, `dbt_utils` 패키지로 더 붙일 수 있음

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: status
        data_tests:
          - accepted_values: {values: ['placed','shipped','completed']}
      - name: customer_id
        data_tests:
          - relationships: {to: ref('dim_customers'), field: customer_id}
```

> **키 이름 주의.** dbt 1.8부터 정식 키는 `data_tests:` 임.
> `tests:` 도 아직 동작하지만 deprecated 경고가 뜸.

### 3-1-2. 무엇을 검증하는 테스트인가

- 읽을 때 이름만으로 목적이 잡히도록 정리함
- **놓치는 것** 칸이 실무에서 더 중요함
  - 걸어놨다고 안심하면 안 되는 지점임

| 테스트 | 목적 | 놓치는 것 |
|---|---|---|
| `unique` | **키가 중복되지 않는가.** PK·대리키(surrogate key)의 유일성. 깨지면 조인에서 행이 불어나 집계가 부풀려짐 | **NULL.** 유일성만 보고 결측은 안 봄 |
| `not_null` | **필수값이 비어 있지 않은가.** 키·금액·시각처럼 없으면 downstream 계산이 NULL로 전파되는 컬럼에 걺 | 값의 타당성(음수·이상치)은 안 봄 |
| `accepted_values` | **값이 정해진 집합 안에 있는가.** 상태·구분 코드에 걺. 원천에 새 코드가 생겼을 때 집계에서 조용히 누락되는 것을 잡음 | **NULL은 통과함.** 결측까지 막으려면 `not_null`을 같이 걺 |
| `relationships` | **참조 무결성.** 외래키 값이 부모 테이블에 실재하는가. 고아 행은 조인 시 사라져 합계가 원인 없이 줄어듦 | **NULL은 검사 대상에서 제외됨.** 그리고 방향이 한쪽이라 "부모에는 있는데 아무도 참조 안 하는 행"은 안 잡음 |
| `dbt_utils.expression_is_true` | **값의 범위·관계 조건.** `>= 0`, `between 0 and 1`, 컬럼 간 비교 등 위 넷으로 표현 못 하는 규칙 | 조건에 쓴 것만 봄 |
| `dbt_utils.unique_combination_of_columns` | **복합키의 유일성 = 테이블의 grain 고정.** 집계 마트가 의도한 grain 을 유지하는지 검사함 | 〃 |

- `unique`와 `not_null`을 보통 같이 거는 이유가 위 표에 있음
- **둘 다 상대가 안 보는 것을 봄**

- `dbt_utils`에는 `recency`(최신성), `equal_rowcount`(두 테이블 행 수 일치), `not_null_proportion`(NULL 비율 상한) 등도 있음
- 직접 만들 수도 있음 (`macros/`에 test 매크로로 정의)

> **`constraints`는 테스트가 아님.** 모델 YAML에 `constraints:`가 보이면
> 그건 generic test가 아니라 **계약(contract) 기능**이고, 테스트 노드를 만들지 않고
> `CREATE TABLE`의 DDL로 내려감. 위반하면 테스트가 실패하는 게 아니라
> **테이블 생성 자체가 실패함.** BigQuery가 강제하는 것은 `not_null`뿐이고
> `unique`는 지원하지 않음 — **유일성과 참조 무결성을 테스트로 할 수밖에 없는 이유임.**

- **singular test**는 특정 상황 하나를 위한 SQL 파일임
  - `tests/` 디렉토리에 두고, **실패한 행을 반환하는 쿼리**를 씀
  - 반환 행이 0이면 통과임

```sql
-- tests/assert_revenue_is_positive.sql
select order_id, revenue
from {{ ref('fct_orders') }}
where revenue < 0
```

- **severity**로 `warn`과 `error`를 나눔
  - `store_failures: true`면 실패한 행을 테이블로 남겨 원인 추적이 쉬워짐

### 3-1-3. unit test — 검증 대상이 다른 세 번째 축 (dbt 1.8+)

**앞의 둘과 묻는 질문이 다름.**

| | generic | singular | **unit** |
|---|---|---|---|
| 검증 대상 | **데이터** | **데이터** | **변환 로직** |
| 묻는 질문 | "지금 들어온 값이 규칙을 지키는가" | 〃 | "이 SQL이 내가 의도한 대로 계산하는가" |
| 형태 | YAML 선언 | 실패 행을 반환하는 SQL | **고정 입력 → 기대 출력** |
| 원천 데이터 | 필요 | 필요 | **불필요** |
| 실행 시점 | 모델을 만든 뒤 | 모델을 만든 뒤 | **모델을 만들지 않고도** |
| 재사용 | 여러 모델에 | 그 상황 하나 | 그 모델 하나 |

- **원천 데이터가 필요 없다는 점이 핵심임**
  - 로직을 고칠 때마다 즉시 돌릴 수 있고, 원천이 비어 있는 날에도 돎
  - 소프트웨어 개발의 단위 테스트와 같은 발상임

```yaml
unit_tests:
  - name: test_discount_applies_only_above_threshold
    model: int_orders_priced
    given:
      - input: ref('stg_orders')
        rows:
          - {order_id: 1, amount: 50000}
          - {order_id: 2, amount: 30000}
    expect:
      rows:
        - {order_id: 1, final_amount: 45000}   # 4만원 이상 10% 할인
        - {order_id: 2, final_amount: 30000}
```

- `given` — upstream 모델(`ref`)이나 원천(`source`)을 **가짜 행으로 대체**함
- `expect` — 그 입력에서 나와야 하는 결과
- 행이 정확히 일치해야 통과함
- `given`에 적지 않은 컬럼은 NULL로 채워짐
- **의도한 컬럼만 적으면 됨**

**증분 모델에 걸 때 주의할 점이 있음.**

```yaml
    overrides:
      macros:
        is_incremental: false
```

- `is_incremental()`은 실행 상황에 따라 값이 달라지는 함수임
- 그대로 두면 유닛 테스트가 증분 분기를 타서 `{{ this }}`(아직 없는 테이블)를 참조하려 함
- **`overrides`로 고정해야 함**
  - `macros` 외에 `vars`, `env_vars`도 같은 방식으로 덮어씀

**어디에 거는가**

| 상황 | 판단 |
|---|---|
| 윈도우 함수, 세션화, 복잡한 조건 분기 | **걺.** 눈으로 검증하기 어렵고 회귀가 잘 남 |
| 금액·비율 계산 규칙 | **걺.** 경계값(0, 임계값 근처)을 명시할 수 있음 |
| 단순 컬럼 rename·캐스팅 | 안 걺. 테스트가 SQL을 그대로 옮겨 적는 꼴이 됨 |

- **모든 모델에 걸지 않음**
  - 기대 출력을 손으로 적어야 해서 유지보수 비용이 붙음
  - 로직이 복잡해서 **"이게 맞게 도는지 자신이 없는 곳"**에만 걺

```bash
dbt test --select test_type:unit          # unit test만
dbt test --select test_type:singular      # singular만
dbt test --select test_type:generic       # generic만
```

> **버전 주의.** unit test는 **dbt 1.8**에서 정식 도입됨. 그 이전 버전에는 없고,
> `dbt_unit_testing` 같은 서드파티 패키지로 흉내 내야 했음.

- **dbt test의 실행 시점이 중요함**
  - `dbt build`는 모델 하나를 만든 직후 그 모델의 테스트를 돌리고, 실패하면 그 아래 계층으로 전파를 멈춤
  - `dbt run` 전부 → `dbt test` 전부와는 다름
- **오염된 데이터가 downstream로 퍼지는 것을 막는 게 `build`임**


## 3-2. 데이터 품질의 두 축 — 내장 검증 vs 독립 감시

> 1-4가 "dbt test를 어떻게 쓰는가"였다면, 여기는 **"dbt test로 안 되는 것은 무엇인가"**임.
> 도구가 아니라 **검증이 파이프라인 안에 있느냐 밖에 있느냐**의 문제임.

### 3-2-1. 정의

- **흔한 오해**: "dbt test = 데이터 품질 관리"라고 묶는 것
- **정확한 구분**: 검증이 **파이프라인 안에 있느냐 밖에 있느냐**임
- **dbt냐 Dataform이냐의 문제가 아님**

| | **파이프라인 내장 검증** | **독립 감시 (Observability)** |
|---|---|---|
| 해당 도구 | **dbt test, Dataform assertion** | 별도 잡, Monte Carlo·Soda 등 |
| 묻는 질문 | "이 모델이 **내가 선언한 계약**을 지키는가" | "**평소와 다른 일**이 벌어지고 있는가" |
| 기준 | **사람이 미리 선언한 규칙** (unique, not_null) | **과거 이력에서 나온 기준선** (예: 최근 N주 변동폭) |
| 실행 시점 | **빌드에 붙어서** — 빌드가 없으면 안 돎 | **자기 스케줄로** — 파이프라인과 무관 |
| 실패의 의미 | **게이트.** downstream로 못 내려감 | **알림.** 사람이 판단 |
| 예측 못 한 문제 | **못 잡음.** 규칙을 안 써놨으면 안 걸림 | **잡을 수 있음.** 기준선에서 벗어나면 걸림 |
| 대상 범위 | 그 도구가 만드는 모델만 | 파이프라인 밖 자산 포함 |

> **dbt test의 "빌드에 붙어 있다"는 성질은 dbt 고유의 한계가 아님.**
> **Dataform assertion도 완전히 동일함.** 워크플로가 실행될 때 함께 돌고,
> 실행이 없으면 돌지 않음. 이걸 dbt 특유의 약점처럼 말하면 틀린 설명이 됨.

**차이는 두 가지로 요약됨.**

- **규칙 기반 vs 기준선 기반** — `revenue >= 0`은 미리 쓸 수 있지만 "오늘 주문이 평소보다 40% 적다"는 미리 쓸 수 없음
  - 정상 범위가 데이터에서 나오기 때문임
- **붙어서 도느냐 따로 도느냐** — 내장 검증은 빌드가 있어야 돎
  - 독립 감시는 파이프라인이 돌든 안 돌든 자기 주기로 확인함
  - (배치와 빌드의 차이는 **3번 '용어'** 참조)

### 3-2-2. dbt가 독립 감시 쪽으로 확장한 부분

| dbt 기능 | 성격 |
|---|---|
| `source freshness` | **별도 명령**(`dbt source freshness`). 빌드와 분리해 원천 지연만 확인 |
| `store_failures` | 실패 행을 테이블로 적재 → **누적하면 추세를 볼 수 있음** |
| `run_results.json` | 실행 이력 아티팩트 → 외부 모니터링으로 보낼 수 있음 |
| **elementary / re_data** | dbt 위에 **이상탐지를 얹는 패키지.** 볼륨·스키마 변화 감지 |

**`store_failures` 를 켜면 추세를 볼 수 있음**

```yaml
- name: fct_orders
  columns:
    - name: net_revenue
      data_tests:
        - dbt_utils.expression_is_true:
            expression: ">= 0"
            config: {severity: warn, store_failures: true}
```

```sql
-- 실패 행이 테이블로 쌓이므로 "언제부터 몇 건씩" 을 볼 수 있다
select date(_dbt_run_started_at) as run_date, count(*) as failures
from `<project>.<target>_dbt_test__audit.dbt_utils_expression_is_true_fct_orders_net_revenue___0`
group by run_date
order by run_date
```

**기준선 기반 감시를 얹으려면 패키지를 씀**

```yaml
# packages.yml
packages:
  - package: elementary-data/elementary
    version: [">=0.16.0", "<0.17.0"]
```

- **dbt 기본은 게이트 도구이고, 독립 감시는 패키지나 별도 체계로 붙임**
  - Dataform은 이 부분이 더 비어 있어서 **직접 만들어야 했음**

---

[← 프로젝트 구조와 명령어](02-project-setup.md) | [목차](README.md) | [이력 적재 →](04-history.md)
