# thelook 커머스 DW 파이프라인

BigQuery 공개 데이터셋(`bigquery-public-data.thelook_ecommerce`)의 주문·상품·행동 로그를
정리하는 dbt + Airflow 웨어하우스.

| | |
|---|---|
| 변환 | dbt-core 1.8 + dbt-bigquery 1.8 |
| 오케스트레이션 | Apache Airflow 2.10 |
| 웨어하우스 | BigQuery (US 멀티리전) |
| 원천 | `bigquery-public-data.thelook_ecommerce` — 7 테이블, 최대 2.4M행 |
| 규모 | 모델 20 · 테스트 102 · snapshot 1 · exposure 3 · Airflow task 18 |

## 1. 아키텍처

```
bigquery-public-data.thelook_ecommerce     원천 · 우리가 통제하지 않음
                │
                │   Airflow EL — 테이블마다 다른 적재 전략        → airflow/
                ▼
        raw_thelook                        랜딩 · 불변 · _ingested_at 부착
                │
                │   dbt
                ▼
        staging/           원천 컬럼명을 표준 이름으로. 조인·집계 없음    [view]
                ▼
        intermediate/      공통 조인을 한 번만. 세션화           [view / 증분]
                ▼
        marts/core/        DW — fact / dimension, grain 유지     [table / 증분]
                ▼
        marts/reporting/   DM — 목적에 맞춘 집계                 [table]
                ▼
        exposure           대시보드 · 분석 — lineage가 여기까지 이어짐
```

- Medallion 대응 — `raw_thelook`이 Bronze, `staging` + `intermediate`가 Silver, `marts`가 Gold
- staging과 intermediate는 별개 계층이 아님. 둘 다 Silver이고 안쪽을 코드 조직 관점에서 나눈 것

## 2. 폴더 구조

```
.
├── README.md
├── .gitignore
│
├── airflow/                  언제, 어떤 순서로
│   ├── README.md                 DAG 구조 · EL 적재 전략 3종 · 실행법
│   └── dags/
│       ├── thelook_dw_daily.py   일 배치 DAG (18 task)
│       └── thelook/              EL SQL 생성 모듈 (DAG과 스크립트가 공유)
│
├── dbt/                      무엇을 어떻게 변환할지
│   ├── README.md                 프로젝트 설정 · 명령어 · 네이밍 · materialization 정책
│   ├── models/
│   │   ├── README.md             3계층 규칙과 계층 간 금지 사항
│   │   ├── staging/              원천 컬럼명을 표준 이름으로        (7 모델)
│   │   ├── intermediate/         공통 조인 · 세션화             (2 모델)
│   │   └── marts/
│   │       ├── core/             DW — fact / dimension       (8 모델)
│   │       └── reporting/        DM — 목적별 집계             (3 모델)
│   ├── snapshots/                상품 가격 이력 (SCD Type 2)
│   ├── tests/                    singular test 4개 + 테스트 전략
│   └── macros/                   run_date · lookback 해석
│
├── scripts/                  손으로 실행하는 것들
│   ├── airflow_env.sh            Airflow 실행 환경을 이 레포에 고정 (source)
│   ├── run_el.py                 최초 전체 적재 / EL SQL 확인
│   └── simulate_price_change.py  SCD Type 2 실증용 가격 변경
│
└── docs/                     참고 문서
    ├── dbt/                      dbt 9편
    ├── airflow/                  Airflow 7편
    └── incidents/                장애 기록 — 현상·원인·조치사항
```

| 폴더 | 담당 | 들어 있는 것 |
|---|---|---|
| [airflow/](airflow/README.md) | 오케스트레이션 — 언제·어떤 순서로 | 일 배치 DAG 1개(18 task), EL SQL 생성 모듈 |
| [dbt/](dbt/README.md) | 변환(T) 전체 | 프로젝트 설정, materialization 정책, 네이밍 규칙 |
| [dbt/models/](dbt/models/README.md) | 3계층 변환 | 모델 20개와 계층 간 규칙 |
| [dbt/models/staging/](dbt/models/staging/README.md) | 원천과 1:1 정제 — 조인·집계 없음 | 7 모델 (view) |
| [dbt/models/intermediate/](dbt/models/intermediate/README.md) | 여러 mart가 공유하는 중간 로직 | 2 모델 (view / 증분) |
| [dbt/models/marts/](dbt/models/marts/README.md) | 소비 계층 — **DW**(`core`) · **DM**(`reporting`) | core 8 모델 (fact/dim) · reporting 3 모델 (집계) |
| [dbt/snapshots/](dbt/snapshots/README.md) | 상품 가격 변경 이력 (SCD Type 2) | snapshot 1개 |
| [dbt/tests/](dbt/tests/README.md) | 여러 모델에 걸친 검증 | singular test 4개 + 테스트 전략 |
| [dbt/macros/](dbt/macros/README.md) | 재사용 SQL 조각 | `run_date` · lookback 해석 매크로 |
| [scripts/](scripts/README.md) | 배치에 없는 수동 실행 | Airflow 환경 고정, 최초 전체 적재, 가격 변경 시뮬레이션 |
| [docs/dbt/](docs/dbt/README.md) · [docs/airflow/](docs/airflow/README.md) | 도구 자체의 개념 | dbt 9편 · Airflow 7편 |
| [docs/incidents/](docs/incidents/README.md) | 실제로 깨진 것 | 장애별 현상 · 원인 · 조치사항 |

- 각 폴더 README는 `개요 → 구성 → 고려사항 → 실행` 구성임
- **`고려사항`에 그 폴더에서 내린 결정과 이유가 모여 있음.** 코드만 봐서 "왜 여기만 다르지" 싶은 지점의 답이 거기 있음

## 3. 개념 문서

파이프라인을 만들면서 정리한 dbt · Airflow 참고 문서. 코드에서 "왜 이렇게 했나"가 궁금하면
각 폴더 README를 보고, 개념 자체가 궁금하면 여기를 봄.

| 문서 | 내용 |
|---|---|
| [docs/dbt/](docs/dbt/README.md) | `ref()`/`source()` · 계층 구조 · materialization · 테스트 3종 · 이력 적재 · macro · contract·exposure · 모델 추가 절차 · 명령어 · 장애 대응 |
| [docs/airflow/](docs/airflow/README.md) | 시스템 구성 요소 · DAG/스케줄링 · 태스크 상태와 `trigger_rule` · 분기 · Sensor · Dynamic Mapping · 실행 환경 격리 · dbt 연동 · 명령어 |
| [docs/incidents/](docs/incidents/README.md) | 이 파이프라인이 실제로 깨진 기록. 개념이 아니라 사고 |

- 폴더 README의 기능 매핑표가 해당 절로 바로 연결됨

## 4. 빠른 시작

**준비**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export THELOOK_GCP_PROJECT=<your-gcp-project>

# ~/.dbt/profiles.yml 에 thelook_dw 항목 추가 (dbt/profiles.yml.example 참고)
# location은 US. 원천이 US 멀티리전이고 BigQuery는 리전 간 조인을 막음

cd dbt && dbt deps && cd ..
```

**최초 적재** — 일 배치는 최근 며칠만 다루므로 과거는 한 번 채워야 함

```bash
python scripts/run_el.py --project $THELOOK_GCP_PROJECT --start 2019-01-01 --end 2026-08-31
```

**전체 빌드**

```bash
cd dbt
dbt build                              # 모델 20개 + 테스트 102개를 계층 순서대로
dbt docs generate && dbt docs serve    # lineage 그래프
```

**Airflow**

```bash
export THELOOK_GCP_PROJECT=<your-project>
export THELOOK_DBT_BIN=/path/to/dbt-core-1.8/bin/dbt   # `which dbt` 로 잡지 말 것
export THELOOK_AIRFLOW_VENV=/path/to/.venv-airflow     # 레포 안에 있으면 생략 가능

# AIRFLOW_HOME · DAGS_FOLDER · PATH 를 이 레포로 고정한다. **매 셸마다**
source scripts/airflow_env.sh

airflow db migrate
airflow connections add google_cloud_default \
    --conn-type google_cloud_platform \
    --conn-extra "{\"key_path\": \"$GOOGLE_APPLICATION_CREDENTIALS\", \"project\": \"$THELOOK_GCP_PROJECT\"}"

airflow scheduler                  # 터미널 1
python scripts/airflow_ui.py       # 터미널 2 (source 부터 다시)
# UI(localhost:8080)에서 thelook_dw_daily 활성화
```

> **`airflow standalone` · `airflow webserver` 는 이 환경에서 쓸 수 없음** — gunicorn 워커가
> 기동 직후 전부 SIGSEGV로 죽음. `scripts/airflow_ui.py` 가 fork 없는 단일 프로세스로 대신 띄움 →
> [장애 기록](docs/incidents/2026-08-17-airflow-webserver-fork-crash.md)

- 설치와 커넥션 등록은 [airflow/README.md](airflow/README.md) 참조
  - `db migrate` 와 `connections add` 를 빠뜨리면 EL 태스크 7개가 전부 실패함
  - **`source scripts/airflow_env.sh` 를 빠뜨리면 태스크가 로그도 없이 전부 실패함.**
    `.venv-airflow/bin/airflow` 를 절대경로로 부르는 것으로 대신할 수 없음 →
    [장애 기록](docs/incidents/2026-08-17-airflow-task-never-launched.md)

- 계층 단위 실행, 특정 일자 재처리는 [dbt/README.md](dbt/README.md) 참조

## 5. 테스트가 102개인 이유

- 테스트 파일을 102개 쓴 게 아님. dbt는 `unique`, `not_null` 같은 선언을 컬럼 하나당 테스트 노드 하나로 셈
- YAML 한 줄이 곧 테스트 1개

| 종류 | 개수 | 어디서 왔나 |
|---|---:|---|
| `not_null` | 43 | YAML 한 줄씩 |
| `unique` | 17 | 〃 |
| `expression_is_true` | 12 | 〃 (금액 ≥ 0, 전환율 0~1 등) |
| `relationships` | 12 | 〃 (외래키 무결성) |
| `accepted_values` | 10 | 〃 (상태값·성별·부서 등) |
| `unique_combination_of_columns` | 3 | 〃 (마트의 grain 고정) |
| singular test | 4 | 직접 쓴 SQL 파일 |
| unit test | 1 | 직접 쓴 YAML |

- 직접 작성한 테스트는 5개뿐(singular 4 + unit 1). 나머지 97개는 YAML 선언이고 그중 60개가 PK 유일성과 필수값
- 선언 위치는 staging 42 / marts.core 36 / marts.reporting 11 / intermediate 8 / 직접 작성 4
- staging이 가장 많은 이유는 원천이 깨졌을 때 거기서 먼저 걸려야 하기 때문
- 자세한 내용은 [dbt/tests/](dbt/tests/README.md)
