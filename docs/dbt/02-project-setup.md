# 프로젝트 구조와 명령어

> dbt  ·  [← 기초](01-basics.md) | [목차](README.md) | [테스트와 데이터 품질 →](03-testing.md)

`dbt_project.yml` · `profiles.yml` 골격, 환경 분리와 네이밍, seed, 스키마 이름이 정해지는 규칙, 자주 쓰는 패키지, 그리고 명령어와 선택 문법을 다룸.

## 2-1. 프로젝트 구조 · 환경 분리 · 네이밍

> 앞 항목들이 "무엇을 만드는가"였다면, 이건 **"어디에 어떻게 두는가"**임.
> 프로젝트를 처음 세울 때 가장 먼저 정해야 하는 것들임.

### 2-1-1. 정의

**프로젝트 골격**

| 파일 | 역할 |
|---|---|
| `dbt_project.yml` | 프로젝트 설정. 모델 경로, **디렉토리별 기본 materialization**, 태그 |
| `profiles.yml` | **접속 정보와 환경(target).** 보통 `~/.dbt/`에 두고 **git에 올리지 않음** |
| `packages.yml` | 외부 패키지 선언. `dbt deps`로 설치 |

```yaml
# dbt_project.yml — 계층별 기본값을 여기서 정한다
models:
  my_project:
    staging:
      +materialized: view          # staging 은 저장하지 않는다 (1-2 참조)
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
```

- **환경 분리(target)** — 같은 코드가 dev와 prod에서 다른 스키마를 씀

```yaml
# profiles.yml
my_project:
  target: dev
  outputs:
    dev:  {dataset: dbt_alice, ...}    # 개발자별로 분리
    prod: {dataset: analytics, ...}
```

- `ref()`가 이걸 가능하게 한다([1-2](01-basics.md) 참조)
- 코드는 그대로 두고 `--target prod`만 바꿈
- **dev 환경은 개발자별로 데이터셋을 나누는 것이 표준 관행**임
  - 서로의 테이블을 안 덮어씀

- **네이밍 컨벤션** — 이름만 보고 계층과 성격을 알 수 있어야 함

| 접두어 | 계층 | 예시 |
|---|---|---|
| `stg_` | staging | `stg_shopify__orders` (원천명 이중 언더스코어로 구분) |
| `int_` | intermediate | `int_orders_joined` |
| `fct_` | marts/core — 사건 | `fct_orders` |
| `dim_` | marts/core — 개체 | `dim_users` |
| `rpt_` | marts/reporting — 소비 | `rpt_daily_revenue` |

### 2-1-2. seed — 참조 데이터를 git에 두기

- `seeds/`에 CSV를 두고 `dbt seed`로 적재함
- 테이블이 되고 `ref()`로 참조함

```
seeds/country_code_mapping.csv
seeds/campaign_channel_mapping.csv
```

- **쓰는 곳**: 코드 매핑표, 부서·조직 마스터, 지역 분류처럼 **원천에 없고 사람이 관리하며 잘 안 바뀌는** 소량 데이터

> **원천 데이터를 seed로 넣지 않음.** git이 데이터 저장소가 되고,
> 수만 행이 넘어가면 `dbt seed`가 느려짐. **수백 행 규모까지가 상식적인 선임.**
> 값이 바뀌면 커밋이 남는다는 게 seed의 진짜 값임 — 매핑이 언제 왜 바뀌었는지 추적됨.

### 2-1-3. 스키마 이름은 어떻게 정해지는가 — generate_schema_name

- `dbt_project.yml`에 `+schema: marts_core`라고 쓰면 실제 데이터셋은 `marts_core`가 아니라 **`<profile의 dataset>_marts_core`**가 됨

```
profiles.yml 의 dataset: dbt_dev
+schema: marts_core
→ 실제 데이터셋: dbt_dev_marts_core
```

- 기본 매크로 `generate_schema_name`이 **둘을 이어붙이기** 때문임
- dev 환경을 개발자별로 나누면서도 계층 구분을 유지하려는 설계임

- prod에서는 접두어 없이 `marts_core`로 두고 싶다면 이 매크로를 덮어씀

```sql
{% macro generate_schema_name(custom_schema_name, node) %}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}{% if custom_schema_name %}_{{ custom_schema_name | trim }}{% endif %}
    {%- endif -%}
{% endmacro %}
```

> **snapshot은 이 규칙을 따르지 않음.** snapshot의 `target_schema`는
> **절대 스키마**라 접두어가 붙지 않음. 그래서 설정을 그대로 두면
> **dev와 prod가 같은 snapshot 테이블을 씀.** 이력 테이블은 하나여야 한다는
> 관점도 있지만, 의도한 것인지 확인하고 넘어가야 하는 지점임.

### 2-1-4. 자주 쓰는 패키지

- `packages.yml`에 선언하고 `dbt deps`로 설치함

| 패키지 | 쓰임 |
|---|---|
| **dbt_utils** | 가장 기본. `expression_is_true`, `unique_combination_of_columns`, `date_spine`, `star`, `surrogate_key` |
| **dbt_expectations** | Great Expectations 스타일의 풍부한 테스트. 분포·타입·정규식 검증 |
| **codegen** | 원천에서 `sources.yml`·staging 모델 **초안을 자동 생성**. 원천이 많을 때 시간을 크게 아낌 |
| **audit_helper** | 두 테이블의 **행·컬럼 단위 비교**. 마이그레이션이나 리팩터링 후 "결과가 같은가"를 검증 |
| **elementary / re_data** | 이상탐지·관측성([3-2](03-testing.md) 참조) |

- **`audit_helper`가 실무에서 특히 유용함**
  - 레거시 쿼리를 dbt로 옮기거나 모델을 다시 쓸 때, 눈으로 몇 행 비교하는 대신 전수 대조 결과를 냄


## 2-2. 명령어와 선택 문법

| 명령 | 하는 일 |
|---|---|
| `dbt deps` | `packages.yml`의 패키지 설치 |
| `dbt debug` | 접속·설정 확인 |
| `dbt parse` | 프로젝트를 파싱해 `manifest.json` 생성 (실행 없음) |
| `dbt compile` | Jinja를 렌더링해 **실제로 나갈 SQL** 생성 |
| `dbt seed` | `seeds/`의 CSV 적재 |
| `dbt run` | 모델 생성 |
| `dbt test` | 테스트만 실행 (이미 만들어진 테이블 대상) |
| **`dbt build`** | **seed → run → test → snapshot을 의존성 순서대로.** 실패하면 downstream 차단 |
| `dbt snapshot` | snapshot 적재 |
| `dbt source freshness` | 원천 지연 확인 (빌드와 분리) |
| `dbt docs generate` / `serve` | 카탈로그·리니지 |
| `dbt ls` | 선택 결과 목록 확인. **영향도 파악에 씀** |
| `dbt clean` | `target/`·`dbt_packages/` 삭제 |
| `dbt retry` (1.6+) | **직전 실행에서 실패한 것부터** 재시작 |
| `dbt clone` (1.6+) | 다른 환경의 테이블을 복제 (CI에서 prod 참조용) |
| `dbt run-operation` | 매크로를 모델 없이 직접 실행 (일회성 DDL·백필 등) |

**자주 쓰는 플래그**

| 플래그 | 역할 |
|---|---|
| `--select` / `-s` | 대상 지정. 아래 선택 문법 참조 |
| `--exclude` | 제외 |
| `--target` | `profiles.yml`의 환경 전환 (`dev` / `prod`) |
| `--vars '{"key": "value"}'` | 실행 파라미터 주입 |
| `--full-refresh` | 증분 모델을 통째로 재생성 |
| `--threads` | 병렬 실행 수. `profiles.yml` 값을 일시적으로 덮어씀 |
| `--fail-fast` | 첫 실패에서 즉시 중단 |
| `--empty` | 0행으로 빌드해 SQL 유효성만 확인 (1.8+) |

**선택 문법(selector)** — `--select` 에 들어가는 표현

| 표현 | 뜻 |
|---|---|
| `stg_orders` | 그 모델 하나 |
| `stg_orders+` | 그 모델과 **downstream 전부** |
| `+fct_orders` | 그 모델과 **upstream 전부** |
| `2+fct_orders` | upstream **2단계까지만** |
| `tag:daily` | 태그로 |
| `path:models/marts` | 경로로 |
| `marts.core` | 디렉토리 계층으로 |
| `test_type:unit` | 테스트 종류로 (`unit` / `singular` / `generic`) |
| `state:modified+` | 변경분과 downstream (CI 용, [8-2](08-operations.md) 참조) |
| `source:raw+` | 그 원천을 쓰는 모델 전부 |
| `source_status:fresher+` | **직전 실행 이후 실제로 갱신된 원천**의 downstream 만. `--state` 와 함께 씀 |

- 공백으로 나열하면 합집합, 쉼표로 이으면 교집합임
  - `--select tag:daily tag:hourly` — 둘 중 하나라도 해당
  - `--select tag:daily,marts` — 둘 다 해당

- **`dbt build`가 기본임**
  - `run` 따로 `test` 따로는 오염된 데이터가 이미 마트까지 내려간 뒤에 알게 된다([3-1](03-testing.md) 참조)

- **`dbt compile`을 습관화함**
  - Jinja가 들어간 모델은 컴파일된 SQL을 봐야 무엇이 나가는지 알 수 있음

---

[← 기초](01-basics.md) | [목차](README.md) | [테스트와 데이터 품질 →](03-testing.md)
