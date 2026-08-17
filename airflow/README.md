# airflow/ — 오케스트레이션

## 1. 개요

- Airflow는 "언제, 어떤 순서로, 실패하면 어떻게"를 담당함
- "무엇을 어떻게 변환할지"는 dbt의 몫 → [../dbt/](../dbt/README.md)

```
dags/
├── thelook_dw_daily.py    일 배치 DAG (17 task)
└── thelook/
    ├── config.py          테이블별 적재 전략 정의
    └── el.py              적재 SQL 생성
```

| 파일 | 역할 |
|---|---|
| `thelook_dw_daily.py` | DAG 하나. 태스크 정의와 의존성, 재시도·알림 정책 |
| `thelook/config.py` | 원천·대상 데이터셋, `LOOKBACK_DAYS`, 테이블별 `TableSpec`(전략과 근거) |
| `thelook/el.py` | `TableSpec`을 받아 적재 SQL을 만드는 `build_el_sql()` |

- `thelook/`은 DAG과 [`scripts/run_el.py`](../scripts/README.md)가 **같은 함수를 씀**
  - SQL이 두 곳에 복사되면 언젠가 한쪽만 고쳐져 조용히 갈라지므로
- 전략과 근거를 `config.py`에 함께 둔 건 **"이 테이블은 왜 이렇게 적재하나"가 가장 자주 받는 질문**이라서. 코드가 곧 근거 문서가 되게 함

## 2. 구성

**DAG 구조**

```
start
 ├─ extract_load/            (7 task, 병렬)  원천 → raw_thelook
 │    ├─ load_orders                partition_overwrite
 │    ├─ load_order_items           partition_overwrite
 │    ├─ load_events                partition_overwrite
 │    ├─ load_inventory_items       full_replace
 │    ├─ load_users                 full_replace
 │    ├─ load_distribution_centers  full_replace
 │    └─ load_products              merge_insert_only
 └─ dbt_deps                 EL과 무관하므로 병렬
      │
      ├─ dbt_source_freshness       ← downstream 없음. 실패해도 변환은 진행됨
      │
      └─ dbt_snapshot               상품 이력. staging보다 먼저 돎
           └─ dbt_build_staging
                └─ dbt_build_intermediate
                     └─ dbt_build_marts_core
                          └─ dbt_build_marts_reporting
                               └─ dbt_docs_generate
                                    └─ end
```

- 모델 빌드 task는 4개뿐. 나머지는 EL 7 · 부수 작업 4 · 경계 2
- `dbt_source_freshness`만 **downstream가 없음.** 신선도는 게이트가 아니라 관측이라서

| 설정 | 값 | 이유 |
|---|---|---|
| `schedule` | `0 3 * * *` | |
| `catchup` | `False` | 켜는 순간 수백 개 run이 한꺼번에 뜨는 것을 막음 |
| `max_active_runs` | `1` | 같은 파티션을 두 run이 동시에 덮으면 결과가 갈림 |
| `retries` | 2, 지수 백오프 | 3번 참조 |

**EL 적재 전략 3종** — `thelook/config.py`에 전략과 근거를 함께 둠

| 전략 | 대상 | 동작 | 근거 |
|---|---|---|---|
| `partition_overwrite` | orders, order_items, events | `[run_date-3, run_date]`를 지우고 다시 넣음 | 생성일이 곧 사건 발생일이라 파티션 경계가 명확 |
| `full_replace` | users, inventory_items, distribution_centers | 통째로 교체 | 과거 행이 나중에 갱신됨 |
| `merge_insert_only` | products | 신규 키만 삽입 | 변경 감지는 snapshot이 담당 |

**이 DAG에서 쓰이는 Airflow 기능** — 개념은 [`docs/airflow/`](../docs/airflow/README.md)의 해당 절

| 기능 | 이 DAG에서 | 개념 |
|---|---|---|
| `TaskGroup` | `extract_load` 7개를 하나로 접음. UI에서 EL 구간이 한 덩어리로 보임 | [1-1](../docs/airflow/01-architecture.md) |
| `{{ ds }}` · `macros.ds_add` | `RUN_DATE` · `LOOKBACK_START`. `datetime.now()`를 쓰지 않음 | [2-1](../docs/airflow/02-dag-scheduling.md) |
| `catchup=False` | 켜는 순간 밀린 run이 한꺼번에 뜨는 것을 막음 | [2-2](../docs/airflow/02-dag-scheduling.md) |
| `max_active_runs=1` | 같은 파티션을 두 run이 동시에 덮는 것을 막음 | [6-2](../docs/airflow/06-operations.md) |
| `retries` + 지수 백오프 | 기본 2회. `source_freshness`·`docs_generate`만 `retries=0` | [2-3](../docs/airflow/02-dag-scheduling.md) |
| `on_failure_callback` | `notify_failure` — 웹훅이 있으면 Slack, 없으면 로그 | [2-3](../docs/airflow/02-dag-scheduling.md) |
| `BashOperator` | dbt가 다른 venv에 있어 프로세스 경계를 넘음 | [3-1](../docs/airflow/03-execution-failure.md) |
| `BigQueryInsertJobOperator` | EL 7개. `gcp_conn_id`로 자격증명을 코드에서 분리 | [6-1](../docs/airflow/06-operations.md) |
| `doc_md` | DAG·task 설명이 UI에 그대로 뜸. dbt의 `description`과 같은 역할 | [dbt 5-2](../docs/dbt/05-macros-metadata.md) |

## 3. 고려사항

- **dbt 모델을 task로 쪼개지 않음**
  - 모델은 20개인데 모델을 빌드하는 task는 4개 (`staging` / `intermediate` / `marts.core` / `marts.reporting`)
  - 쪼개면 의존성이 dbt의 `ref()`와 DAG 양쪽에 정의됨. 한쪽만 고치면 순서가 틀어지고 그 사고는 에러 없이 조용히 일어남
  - 계층 단위로는 나눴음. 계층이 실패 시 재시작 지점이자 "어디까지 신뢰할 수 있는가"의 경계라서
  - 판단 근거 전체는 [`docs/airflow/`](../docs/airflow/05-dbt-integration.md) 5-1 참조
  - → 다음 프로젝트: 그래프의 주인을 한쪽으로 정하고 나머지는 호출만 할 것

- **inventory_items가 full_replace인 이유** — 적재 전략 중 가장 설명이 필요한 케이스
  - `created_at`(입고일)이 있으니 증분으로 뜨고 싶어짐
  - 그런데 `sold_at`은 나중에 채워짐. 입고일로 파티션을 잘라 증분을 뜨면 **과거 파티션에서 일어난 판매 갱신을 영원히 놓침**
  - 488K행이라 전체 교체가 더 싸고 정확함
  - → 다음 프로젝트: 적재 전략은 테이블 크기가 아니라 갱신 패턴이 정함. "생성일 컬럼이 있으니 증분"이 아니라 **"이 행이 나중에 바뀌는가"**를 먼저 물을 것

- **products가 merge_insert_only인 이유**
  - 원천이 공개 데이터셋이라 값이 변하지 않음
  - EL이 매번 전체를 덮으면 [시뮬레이션한 가격 변경](../dbt/snapshots/README.md)이 다음 배치에 되돌아가고, SCD Type 2 이력이 만들어질 여지가 사라짐

- **`trigger_rule`로 "내 실패를 무시해달라"를 표현할 수 없음** — 처음에 틀렸던 부분
  - `dbt_source_freshness`를 변환 체인 중간에 두고 `trigger_rule=all_done`을 걸었음. 의도는 "신선도 경고로 배치를 멈추지 말자"였음
  - **그런데 `trigger_rule`은 "내가 언제 시작할지"를 정하지, "내 실패가 downstream를 막을지"를 정하지 않음**
  - 부작용이 있었음. EL이 실패하면 upstream가 `upstream_failed`가 되는데 `all_done`은 그것도 "끝난 상태"로 보고 통과시킴. **원천이 안 들어온 날 dbt가 그대로 도는 경로**가 열려 있었음
  - 고친 방법은 트리거 규칙이 아니라 **그래프 모양**이었음. freshness에 downstream 태스크를 두지 않으면, 실패해도 뒤에서 막힐 태스크가 없음
  - 같은 이유로 `dbt_deps`도 EL과 병렬로 뒀음. 패키지 설치일 뿐이라 원천 적재를 기다릴 이유가 없음
  - → 다음 프로젝트: "이 실패는 무시하고 싶다"는 `trigger_rule`이 아니라 **의존성 위치**로 표현할 것

- **venv를 절대경로로 기동하는 건 격리의 반쪽이었음** — 2026-08-17 장애
  - `.venv-airflow/bin/airflow scheduler` 로 띄웠음. venv를 썼으니 격리됐다고 생각했음
  - **그런데 Airflow는 태스크를 `["airflow", "tasks", "run", ...]` 라는 맨 이름 명령으로 띄우고 `PATH`로 찾음**
  - 절대경로 기동은 `venv/bin` 을 `PATH`에 넣지 않음. 그래서 그 서브프로세스가 pyenv shim을 잡고 기동 실패했고, 태스크 16개가 로그 한 줄 없이 죽음
  - 증상이 원인을 가렸음. 태스크가 시작조차 못 해 `hostname` 이 비었고, UI에는 로그 조회 실패(`No host supplied`)만 떴음
  - 고친 방법은 [`scripts/airflow_env.sh`](../scripts/README.md)로 `PATH`까지 기동 절차에 포함시킨 것 → [장애 기록](../docs/incidents/2026-08-17-airflow-task-never-launched.md)
  - → 다음 프로젝트: 도구를 venv에 격리했으면 **기동 스크립트가 `PATH`까지 책임질 것.** 실행 파일 경로만 맞추는 건 절반임

- **lookback 값이 두 도구에 걸쳐 있음**
  - `config.py`의 `LOOKBACK_DAYS`와 dbt `vars.lookback_days`가 **반드시 같아야 함**
  - 달라지면 EL이 채운 구간과 dbt가 다시 만드는 구간이 어긋남. 에러 없이 결과만 틀림
  - Airflow Variable로 빼지 않은 것도 그래서임. 설정 지점을 늘리면 갈라질 자리가 늘어남
  - → 다음 프로젝트: 두 도구가 같은 숫자를 알아야 하면 **어느 쪽이 원본인지 먼저 정할 것**

## 4. 실행

**설치** — Airflow는 dbt와 의존성이 충돌하므로 별도 venv에 깖

```bash
python3 -m venv .venv-airflow
.venv-airflow/bin/pip install "apache-airflow==2.10.5" "apache-airflow-providers-google" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
```

- constraint 파일 없이 깔면 의존성이 어긋나 기동 자체가 안 됨. Airflow는 이 파일로 버전을 고정하는 것이 공식 방식임

**환경변수**

경로와 `PATH`는 [`scripts/airflow_env.sh`](../scripts/README.md)가 잡음. 직접 export 하는 건 자격증명과 도구 경로뿐임.

```bash
export THELOOK_GCP_PROJECT=<your-project>
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
export THELOOK_DBT_BIN=/path/to/dbt-core-1.8/bin/dbt   # `which dbt` 로 잡지 말 것
export THELOOK_SLACK_WEBHOOK=<optional>

# venv가 레포 밖에 있으면 위치를 알려줌 (기본값은 <repo>/.venv-airflow)
export THELOOK_AIRFLOW_VENV=/path/to/.venv-airflow

cd <repo>
source scripts/airflow_env.sh
```

| 변수 | 기본값 | 용도 |
|---|---|---|
| `THELOOK_DBT_BIN` | `dbt` | dbt 실행 파일 경로 |
| `THELOOK_DBT_TARGET` | `dev` | dbt target |
| `THELOOK_GCP_PROJECT` | — | 적재 대상 프로젝트 |
| `THELOOK_GCP_CONN_ID` | `google_cloud_default` | Airflow BigQuery 커넥션 |
| `THELOOK_SLACK_WEBHOOK` | 없음 | 있으면 실패 알림 전송 |
| `THELOOK_AIRFLOW_VENV` | `<repo>/.venv-airflow` | Airflow venv 위치. `airflow_env.sh`가 이 경로를 `PATH` 앞에 붙임 |

`airflow_env.sh`가 대신 잡아 주는 것 — 직접 export 할 필요 없음.

| 변수 | 값 |
|---|---|
| `AIRFLOW_HOME` | `<repo>/airflow_home` |
| `AIRFLOW__CORE__DAGS_FOLDER` | `<repo>/airflow/dags` |
| `THELOOK_DBT_PROJECT_DIR` | `<repo>/dbt` |
| `PATH` | `$THELOOK_AIRFLOW_VENV/bin` 을 맨 앞에 |

> **`$(which dbt)` 를 쓰지 않는 이유** — PATH에 dbt-fusion(2.x preview)이 깔려 있으면
> 그쪽이 잡힘. Fusion은 이 프로젝트의 YAML 형식을 거부해서
> `Deprecated test arguments` 로 52개 에러가 나고 한 모델도 못 만듦.
> dbt-core 1.8 실행 파일 경로를 직접 지정할 것.

**초기화와 커넥션 등록**

```bash
airflow db migrate

airflow connections add google_cloud_default \
    --conn-type google_cloud_platform \
    --conn-extra "{\"key_path\": \"$GOOGLE_APPLICATION_CREDENTIALS\", \"project\": \"$THELOOK_GCP_PROJECT\"}"
```

> **커넥션 등록은 건너뛰면 안 됨** — Airflow 2.10의 `db migrate` 는 기본 커넥션을
> 만들지 않음(예전 `db init` 과 달라진 부분). `google_cloud_default` 가 없으면
> `BigQueryInsertJobOperator` 를 쓰는 EL 태스크 7개가 전부
> `AirflowNotFoundException: The conn_id 'google_cloud_default' isn't defined` 로 실패함.
> 실패 원인이 GCP 권한처럼 보이지만 인증 문제가 아님.

**기동**

```bash
source scripts/airflow_env.sh     # ← 셸을 새로 열 때마다. 건너뛰면 태스크가 전부 실패함
airflow scheduler &
airflow webserver --port 8080 &
# UI(localhost:8080)에서 thelook_dw_daily 활성화
```

> **`.venv-airflow/bin/airflow` 를 절대경로로 실행하지 말 것** — Airflow는 태스크를
> `["airflow", "tasks", "run", ...]` 라는 **맨 이름 명령**으로 띄우고 그걸 `PATH`로 찾음.
> 절대경로로 기동하면 `venv/bin` 이 `PATH`에 없어서 그 서브프로세스가 기동조차 못 하고,
> 태스크가 로그 한 줄 없이 즉시 실패함. UI에는 엉뚱하게
> `Could not read served logs: ... No host supplied` 가 뜸.
> `airflow_env.sh` 가 `PATH`를 잡고 `airflow` 가 venv 안에서 해석되는지까지 검증함.
> → [`docs/incidents/2026-08-17-airflow-task-never-launched.md`](../docs/incidents/2026-08-17-airflow-task-never-launched.md)

**스케줄러 없이 한 번만 돌려보기**

```bash
airflow dags test thelook_dw_daily 2026-08-15
```

- 메타DB에 run을 만들되 스케줄러·웹서버 없이 태스크를 순차 실행함. DAG 검증에 가장 빠름

**백필**

```bash
airflow dags backfill thelook_dw_daily --start-date 2026-07-01 --end-date 2026-07-31
```

- `max_active_runs=1`이라 순차 실행됨. 동시에 돌면 같은 파티션을 두 run이 덮어 결과가 갈림

**그 밖**

```bash
# DAG 파싱만 확인
python -c "import sys; sys.path.insert(0,'dags'); import thelook_dw_daily as m; print(len(m.dag.tasks))"

# 생성될 EL SQL 확인 (Airflow 없이)
python ../scripts/run_el.py --project <PROJECT> --dry-run
```
