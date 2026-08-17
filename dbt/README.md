# dbt/ — 변환 계층

## 1. 개요

- dbt는 DW 안에서의 변환(T)만 담당함. 데이터를 옮기지 않음
- `source()`가 가리키는 `raw_thelook` 테이블은 Airflow가 이미 채워 놓은 상태여야 함
- 그 앞 구간은 [../airflow/](../airflow/README.md)

## 2. 구성

| | |
|---|---|
| [`models/`](models/README.md) | 3계층 변환 모델 20개 |
| [`tests/`](tests/README.md) | singular test 4개 + 테스트 전략 |
| [`snapshots/`](snapshots/README.md) | 상품 가격 이력 (SCD Type 2) |
| [`macros/`](macros/README.md) | `run_date` · lookback 해석 |
| `dbt_project.yml` | 계층별 기본 materialization, 프로젝트 변수 |
| `packages.yml` | `dbt_utils` |
| `profiles.yml.example` | 접속 정보 예시 (실제 파일은 `~/.dbt/`에 두고 git에 올리지 않음) |

**materialization 정책** — `dbt_project.yml`에 계층별 기본값, 모델에서 `config()`로 개별 재정의

| 계층 | 기본값 | 예외 |
|---|---|---|
| `staging` | view | 없음 |
| `intermediate` | view | `int_events_sessionized` → 증분 테이블 |
| `marts` | table | `fct_user_events` → 증분 테이블 |

**네이밍** — 이름만 보고 계층과 성격을 알 수 있게 함

| 접두어 | 계층 | 예시 |
|---|---|---|
| `stg_<원천>__<테이블>` | staging | `stg_thelook__orders` |
| `int_` | intermediate | `int_order_items_enriched` |
| `dim_` | marts/core — 개체 | `dim_users` |
| `fct_` | marts/core — 사건 | `fct_order_items` |
| `rpt_` | marts/reporting — 소비 | `rpt_daily_revenue` |
| `snap_` | snapshots | `snap_products` |

**출력 데이터셋** — `profiles.yml`의 `dataset`에 계층 이름이 붙어 만들어짐

```
dbt_dev_staging          dbt_dev_intermediate
dbt_dev_marts_core       dbt_dev_marts_reporting
snapshots                dbt_dev_dbt_test__audit   (store_failures 결과)
(snapshot만 접두어 없음 — target_schema는 절대 스키마)
```

## 3. 고려사항

- **`location`은 US여야 함**
  - 원천이 `bigquery-public-data`(US 멀티리전)이고 BigQuery는 리전 간 조인을 막음
  - `asia-northeast3`로 두면 첫 모델부터 실패함
  - `profiles.yml`에 `maximum_bytes_billed`도 걸어 뒀음. 실수로 전체 스캔하는 쿼리를 비용 발생 전에 끊는 장치

- **`dbt run`이 아니라 `dbt build`를 씀**
  - `build`는 모델을 만든 직후 그 모델의 테스트를 돌리고 실패하면 downstream를 막음
  - `run` 전부 → `test` 전부 순서로는 오염된 데이터가 이미 마트까지 내려간 뒤에 알게 됨

- **증분은 둘 다 `insert_overwrite` 전략**
  - `merge`는 `unique_key`로 전체 테이블을 대조해야 해서 스캔이 큼
  - `insert_overwrite`는 결과에 등장한 파티션만 통째로 교체함. 멱등하고 스캔이 좁음
  - 처음부터 증분으로 시작하지 않았음. 복잡도가 붙으므로 재계산 비용이 실제로 문제가 된 두 모델만 옮겼음
  - → 다음 프로젝트: 증분은 기본값이 아니라 예외로 둘 것. 비용 문제가 관측된 뒤에 옮겨도 늦지 않음

- **원천명을 이중 언더스코어로 구분**
  - 원천이 늘었을 때 `stg_shopify__orders`와 `stg_thelook__orders`가 충돌 없이 공존해야 하므로

- **dev 환경을 개발자별로 분리**
  - `--target prod`로 바꾸면 같은 코드가 다른 데이터셋에 만들어짐. `ref()`가 이걸 가능하게 함
  - 코드는 그대로 두고 접속 대상만 바뀜. 그래야 서로의 테이블을 덮어쓰지 않음

## 4. 실행

**설정**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
dbt deps
dbt debug     # 접속 확인
```

`~/.dbt/profiles.yml`에 `thelook_dw` 항목을 만듦. `profiles.yml.example` 참고.

**빌드**

```bash
dbt build                                   # 모델 + 테스트, 계층 순서대로
dbt build --select staging                  # 계층 단위
dbt build --select marts.core
dbt build --select fct_orders               # 모델 하나
dbt build --select fct_orders+              # 그 모델과 downstream 전부
dbt build --select +fct_orders              # 그 모델과 upstream 전부
```

**특정 일자로 재처리**

```bash
dbt build --select fct_user_events --vars '{"run_date": "2026-08-01"}'
```

- 증분 모델은 `run_date` 기준 `[run_date - lookback_days, run_date]` 구간을 다시 만듦
- 같은 값으로 몇 번을 돌려도 결과가 같음 → [macros/](macros/README.md)

**그 밖**

```bash
dbt source freshness                        # 원천 지연만 별도 확인 (빌드와 분리)
dbt snapshot                                # 상품 가격 이력 적재
dbt test --select test_type:unit            # 변환 로직 검증
dbt docs generate && dbt docs serve         # lineage 그래프
dbt ls --select rpt_daily_revenue+          # 영향도 확인 (exposure까지 나옴)
dbt compile --select int_events_sessionized # 생성될 SQL 확인
```
