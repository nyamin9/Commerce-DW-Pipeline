# 매크로와 메타데이터

> dbt  ·  [← 이력 적재](04-history.md) | [목차](README.md) | [모델을 추가하는 절차 →](06-workflow.md)

Jinja 와 macro, `dbt docs` 가 만드는 lineage, 그리고 contract · model version · exposure · `persist_docs` · `grants` 로 스키마와 권한을 코드에 두는 방법을 다룸.

## 5-1. Macro와 Jinja

### 5-1-1. 정의

- dbt의 SQL 파일은 그냥 SQL이 아니라 **Jinja2 템플릿**임
- `dbt run`을 하면 먼저 Jinja가 렌더링되어 순수 SQL이 만들어지고, 그게 DW로 감

- `{{ }}` — 값 치환 (`{{ ref('x') }}`, `{{ var('start_date') }}`)
- `{% %}` — 제어문 (`{% if %}`, `{% for %}`, `{% macro %}`)

- **macro는 재사용 가능한 SQL 조각**임
  - 함수와 비슷함

```sql
{% macro cents_to_dollars(column_name, precision=2) %}
    round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
```

- `{{ cents_to_dollars('amount') }}`로 호출하면 컴파일 시 치환됨

**중요한 차이 — macro는 UDF가 아님.**

| | **macro (dbt)** | **UDF (DB)** |
|---|---|---|
| 실행 시점 | **컴파일 타임 텍스트 치환** | **런타임 DB에서 실행** |
| 위치 | dbt 프로젝트 안 | DB에 배포된 객체 |
| dbt 밖에서 사용 | 불가 | 가능 |
| 디버깅 | 컴파일된 SQL을 봐야 함 | DB에서 직접 호출 |

### 5-1-2. 컴파일 시점에 쓸 수 있는 내장 변수

- 매크로 안에서 자주 쓰는 것들임
- **모두 컴파일 시점에 값이 정해짐**

| 변수 | 값 | 쓰임 |
|---|---|---|
| `{{ this }}` | 지금 만들고 있는 모델의 실제 경로 | 증분 모델에서 기존 테이블 참조 |
| `{{ target }}` | 현재 target 정보 (`.name`, `.schema`, `.database`) | 환경별 분기 |
| `{{ var('x') }}` | `dbt_project.yml`의 vars 또는 `--vars` | 실행 파라미터 주입 |
| `{{ env_var('X') }}` | 환경변수 | 자격증명처럼 코드에 못 두는 값 |

```sql
-- dev 에서는 최근 7일만 만들어 반복 실행을 빠르게 한다
{% if target.name == 'dev' %}
  where event_date >= date_sub(current_date(), interval 7 day)
{% endif %}
```

> **`env_var()`는 컴파일 시점에 읽힘.** 값이 없으면 그 자리에서 컴파일이 실패함.
> 기본값이 필요하면 `env_var('X', 'default')`로 줌.

### 5-1-3. 커스텀 generic test도 매크로다

- 기본 제공 네 개(`unique`·`not_null`·`accepted_values`·`relationships`)로 표현되지 않는 규칙이 **여러 모델에 반복되면** 직접 만듦
- `macros/`에 `test_` 접두어로 정의함

```sql
-- macros/test_positive_amount.sql
{% test positive_amount(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} <= 0
{% endtest %}
```

```yaml
- name: net_revenue
  data_tests: [positive_amount]
```

- `model`과 `column_name`은 dbt가 넘겨줌
- 추가 인자를 받을 수도 있음
- **실패 행을 반환하는 쿼리**를 쓴다는 규칙은 singular test와 같음
- **판단 기준은 반복 횟수임**
  - 한 곳에서만 쓰면 singular test로 두는 게 읽기 쉽고, 세 번째 모델에서 또 필요해지면 그때 generic으로 승격시킴

- `dbt_utils`의 테스트들이 정확히 이 방식으로 만들어져 있음

### 5-1-4. hook — 모델 전후에 SQL을 끼워 넣기

| hook | 실행 시점 |
|---|---|
| `pre_hook` | 모델을 만들기 **직전** |
| `post_hook` | 모델을 만든 **직후** |
| `on_run_start` / `on_run_end` | `dbt run` 전체의 시작·끝 (`dbt_project.yml`) |

```sql
{{ config(
    post_hook="grant select on {{ this }} to 'group:analysts@example.com'"
) }}
```

- **주로 쓰는 곳**: 권한 부여, 테이블 메타데이터·라벨 설정, 감사 로그 적재

> **hook에 변환 로직을 넣지 않음.** hook의 SQL은 `ref()` 그래프에 잡히지 않아
> 리니지에서 보이지 않음. 데이터를 바꾸는 일은 모델로 표현해야 함.

### 5-1-5. run_query — 컴파일 중에 DW에 질의하기

- 매크로 안에서 실제로 쿼리를 던져 그 결과로 SQL을 만듦

```sql
{% set results = run_query("select distinct department from " ~ ref('dim_products')) %}
{% if execute %}
  {% set departments = results.columns[0].values() %}
{% endif %}
```

- 동적 피벗(값의 개수를 미리 모르는 경우)에 씀

> **`{% if execute %}` 가 필요한 이유.** dbt는 SQL을 두 번 훑음.
> 첫 번째(parse)에서는 의존성만 수집하고 쿼리를 실행하지 않아 `results`가 비어 있음.
> 이 가드가 없으면 파싱 단계에서 에러가 남.

- **단, 남용하지 않음**
  - `run_query`는 컴파일마다 DW에 질의하므로 느려지고, 생성되는 SQL이 데이터 상태에 따라 달라져 **같은 코드가 매번 다른 결과를 만듦**
  - 파티션이 데이터에 따라 바뀌면 재현이 어려워짐

### 5-1-6. 매크로를 얼마나 쓸 것인가

- **SQL을 읽을 수 없게 만드는 순간 손해임**
  - dbt 프로젝트의 자산은 SQL의 가독성이고, Jinja가 겹겹이 쌓이면 컴파일된 SQL을 봐야만 이해할 수 있는 코드가 됨

| 쓸 만한 경우 | 피할 경우 |
|---|---|
| 같은 식이 여러 모델에 반복 | 한 곳에서만 쓰는 로직 |
| 기준일·구간처럼 **밖에서 주입받아야 하는 값** | 단순 별칭 |
| 환경별로 달라져야 하는 설정 | Jinja `for` 문으로 만드는 긴 SQL |

- `dbt compile`로 생성된 SQL을 확인하는 습관이 전제임


## 5-2. dbt docs와 리니지(lineage)

### 5-2-1. 정의

- `dbt docs generate`를 실행하면 두 개의 산출물이 나옴

- **`manifest.json`** — 모든 모델, 의존성, 테스트, 설명이 담긴 프로젝트의 전체 상태
- **`catalog.json`** — DW에서 조회한 실제 컬럼, 타입, 통계

- `dbt docs serve`로 브라우저에서 열면 **리니지 그래프**가 나옴

- **리니지가 만들어지는 원리가 중요함**
  - 별도로 그린 게 아니라 **`ref()`와 `source()` 호출을 파싱해서 자동 생성**함
  - 코드가 곧 리니지임
  - 그래서 문서와 실제가 어긋날 수 없음

```bash
dbt docs generate          # manifest.json + catalog.json 생성
dbt docs serve             # 브라우저에서 리니지 확인
```

- **`manifest.json` 은 사람이 읽으라고만 있는 게 아님**
  - 프로젝트의 전체 상태가 JSON 으로 들어 있어 **메타데이터 파이프라인의 입력**이 됨

```bash
# 테스트가 하나도 없는 모델 찾기
jq -r '
  [.nodes[] | select(.resource_type=="test") | .depends_on.nodes[]] as $tested
  | .nodes[] | select(.resource_type=="model")
  | select((.unique_id | IN($tested[])) | not) | .name
' target/manifest.json

# description 이 비어 있는 컬럼 찾기 — 카탈로그 품질 점검
jq -r '.nodes[] | select(.resource_type=="model") as $m
  | $m.columns[] | select(.description == "") | "\($m.name).\(.name)"' target/manifest.json
```

- **한계**: 기본 제공은 **모델 레벨 리니지**임
  - "이 컬럼이 어디서 왔는가"인 컬럼 레벨 리니지는 dbt Core 기본에는 없음
  - dbt Cloud나 별도 도구(DataHub, OpenMetadata 등)가 필요함


## 5-3. 계약과 메타데이터 — contract · version · exposure · 문서화

> 앞 항목들이 "데이터를 어떻게 만드는가"였다면, 여기는
> **"만든 것을 어떻게 약속하고 알리는가"**임. 데이터 거버넌스와 맞닿는 부분임.

### 5-3-1. contract — 스키마를 약속으로 만듦

- 모델의 컬럼과 타입을 선언하고, 어기면 **빌드를 실패시킴**

```yaml
models:
  - name: dim_users
    config:
      contract: {enforced: true}
    columns:
      - name: user_id
        data_type: int64
        constraints: [{type: not_null}]
      - name: email_domain
        data_type: string
```

- `enforced: true`면 **모든 컬럼의 `data_type`을 선언해야 함**
- 컬럼이 빠지거나 타입이 바뀌면 모델 생성 자체가 실패함
- `constraints`는 테스트가 아니라 **DDL로 내려감**([3-1](03-testing.md) 참조)

- **전부에 걸지 않음**
  - 공짜가 아님 — 모델을 고칠 때마다 YAML도 같이 고쳐야 함
- **downstream 소비가 넓은 모델**(다른 팀이 쓰는 것, BI가 직접 붙은 것)에서만 비용을 들일 만함

### 5-3-2. model version — 깨는 변경을 유예 기간과 함께 냄

- 컬럼을 지워야 하는데 소비자가 여럿이면, 바로 지울 수 없음

```yaml
models:
  - name: dim_users
    latest_version: 2
    versions:
      - v: 1
        deprecation_date: 2026-12-31
      - v: 2
        columns: [{include: all, exclude: [legacy_grade]}]
```

```sql
{{ ref('dim_users', version=1) }}   -- 아직 옮기지 못한 소비자
{{ ref('dim_users') }}              -- latest_version 을 가리킨다
```

- **두 버전을 동시에 서비스하면서 소비자를 옮김**
  - `deprecation_date`가 지나면 경고가 뜸
  - contract와 짝으로 씀 — 계약이 있어야 "깨는 변경"이 무엇인지 정의됨

### 5-3-3. exposure — 리니지를 대시보드까지 연장하기

- dbt의 리니지는 기본적으로 **모델에서 끝남**
- 그 아래에 무엇이 붙어 있는지 모르니 **"이 컬럼 지워도 되나요"에 답할 수 없음**

```yaml
exposures:
  - name: executive_revenue_dashboard
    type: dashboard          # dashboard / analysis / ml / application
    maturity: high           # high / medium / low
    url: https://bi.example.com/d/123
    description: 주간 전략회의용. 부서별 매출과 누적 추이
    depends_on:
      - ref('rpt_daily_revenue')
    owner: {name: Analytics Engineering, email: ae@example.com}
```

```bash
dbt ls --select +exposure:executive_revenue_dashboard   # 이 대시보드가 의존하는 전부
dbt ls --select rpt_daily_revenue+                      # 이 모델을 바꾸면 영향받는 것
```

- **요점은 스키마 변경 전 영향도 확인이 사람의 기억이 아니라 절차가 된다는 것임**
  - `maturity`로 사전 공지가 필요한 대상을 구분함

> **한계**: exposure는 **손으로 선언함.** BI 도구를 파싱해 자동으로 채우지 않으므로,
> 선언하지 않은 대시보드는 여전히 보이지 않음. 이 부분을 자동화하려면
> 메타데이터 플랫폼(DataHub, OpenMetadata) 영역으로 넘어감.

### 5-3-4. 문서화 — description · docs 블록 · meta

- **description**은 `dbt docs`의 카탈로그가 되는 것이지 주석이 아님

```yaml
- name: net_revenue
  description: "취소·반품을 제외한 매출. 기준은 int_order_items_enriched 에 있다"
  meta:
    owner: analytics-engineering
    pii: false
    metric_tier: gold
```

**같은 설명이 여러 모델에 반복되면 docs 블록으로 뺌.**

```sql
-- models/docs.md
{% docs net_revenue %}
취소·반품을 제외한 매출. 판단 기준은 `is_revenue_recognized` 한 곳에만 있다.
{% enddocs %}
```

```yaml
description: "{{ doc('net_revenue') }}"
```

**`persist_docs` — 설명을 웨어하우스까지 밀어 넣음**

```yaml
models:
  my_project:
    +persist_docs: {relation: true, columns: true}
```

- dbt에 쓴 description이 **BigQuery 테이블·컬럼 설명으로 반영됨**
- dbt docs를 열지 않는 사람(콘솔에서 테이블을 뒤지는 분석가)에게도 설명이 닿음
- **메타데이터가 두 곳에서 갈라지지 않게 하는 장치**이기도 함

### 5-3-5. grants — 권한을 코드로 관리하기

```yaml
models:
  my_project:
    marts:
      +grants:
        select: ['group:analysts@example.com']
```

- dbt 1.2+의 **네이티브 기능**임
- 예전에는 `post_hook`으로 `grant` 문을 직접 썼음
- 모델을 다시 만들 때마다 권한이 재적용됨
- `CREATE OR REPLACE`로 권한이 날아가는 문제가 사라짐
- 기본은 **덮어쓰기**임
- 기존 권한에 더하려면 `+grants: {+select: [...]}`처럼 `+`를 붙임

- **권한을 코드에 두면 리뷰 대상이 됨**
  - 누가 무엇을 볼 수 있는지가 콘솔의 현재 상태가 아니라 git 히스토리에 남음

---

[← 이력 적재](04-history.md) | [목차](README.md) | [모델을 추가하는 절차 →](06-workflow.md)
