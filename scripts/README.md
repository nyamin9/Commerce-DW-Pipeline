# scripts/ — 손으로 실행하는 것들

## 1. 개요

- 일 배치에 들어 있지 않은 것만 여기 둠
- 자동으로 도는 것과 사람이 판단해서 돌리는 것을 폴더로 구분함

| 스크립트 | 언제 쓰나 |
|---|---|
| `airflow_env.sh` | Airflow를 다루는 셸을 열 때마다. **가장 먼저 부름** |
| `airflow_ui.py` | Airflow 웹 UI 기동. `airflow webserver` 대신 씀 |
| `run_el.py` | 최초 전체 적재. EL SQL 확인 |
| `simulate_price_change.py` | SCD Type 2가 이력을 쌓는지 실증할 때 |

- `.py` 둘은 `GOOGLE_APPLICATION_CREDENTIALS`가 필요함
- `airflow_env.sh`만 성격이 다름. 뭔가를 실행하는 게 아니라 **실행 환경을 고정함**

## 2. 구성

**airflow_env.sh**

- `source` 로 부름. 실행하면 환경변수가 현재 셸에 남지 않으므로 그 경우 에러로 막음
- 잡아 주는 것 — `AIRFLOW_HOME` · `AIRFLOW__CORE__DAGS_FOLDER` · `THELOOK_DBT_PROJECT_DIR` · `PATH`
- 스크립트 위치에서 레포 루트를 역산하므로 **어디서 부르든 이 레포를 가리킴**
- 기동 전에 검증하는 것

  | 검사 | 통과 못 하면 |
  |---|---|
  | `airflow` 가 지정한 venv 안에서 해석되는가 | 태스크가 로그 없이 전부 실패함 |
  | `AIRFLOW__CORE__DAGS_FOLDER` 가 실재하는가 | DAG이 하나도 안 뜸 |
  | `THELOOK_GCP_PROJECT` 가 설정됐는가 | DAG **임포트**가 깨짐 |
  | `THELOOK_DBT_BIN` 이 설정됐는가 | 경고만. dbt 태스크가 `PATH`의 dbt를 씀 |

**airflow_ui.py**

- `airflow webserver` · `airflow standalone` 이 이 환경에서 동작하지 않아 대신 씀
  - gunicorn 마스터가 fork 전에 provider 를 전부 로드하고, 그 상태가 fork 된 워커에서 깨짐
  - werkzeug 단일 프로세스라 fork 자체를 안 함 → [장애 기록](../docs/incidents/2026-08-17-macos-fork-unsafe-os-log.md)
- `AIRFLOW_HOME` 이 이 레포를 안 가리키면 경고함. **UI는 뜨는데 다른 메타DB를 보여주는 사고**를 막는 장치임
- 잃는 것은 gunicorn 멀티워커뿐. 로컬에서 워커 4개가 필요할 일은 없음

**run_el.py**

- 일 배치는 `[run_date - 3, run_date]` 구간만 다루므로 과거는 한 번 채워야 함
- `raw_thelook` 데이터셋이 없으면 US 리전으로 만들고 시작함
  - DAG 쪽은 `create_raw_dataset` 태스크가 같은 일을 함. 두 경로 모두 자기 힘으로 설 수 있어야 함
- 실행이 끝나면 테이블별 과금 대상 바이트를 출력함

**simulate_price_change.py**

- 시뮬레이션 스크립트. 일 배치에 들어 있지 않고, 위치로 그 사실을 드러냄
- `--show` 출력 형태

  ```
  product_id  retail_price  valid_from                  valid_to                    name
       12345         49.00  2026-08-16 03:00:00+00:00   2026-08-16 05:12:00+00:00   ...
       12345         56.35  2026-08-16 05:12:00+00:00   (현재)                       ...
  ```

## 3. 고려사항

- **SQL을 이 폴더에 복사하지 않음**
  - `run_el.py`는 SQL을 `airflow/dags/thelook/el.py`에서 가져옴
  - 두 벌이 되면 언젠가 한쪽만 고쳐지므로. Airflow DAG과 완전히 같은 코드 경로를 돎

- **`airflow_env.sh` 가 `PATH`까지 건드리는 이유**
  - Airflow는 태스크를 `["airflow", "tasks", "run", ...]` 라는 맨 이름 명령으로 띄우고 `PATH`로 찾음
  - venv를 절대경로로 기동하면 `venv/bin` 이 `PATH`에 없어서 그 서브프로세스가 기동 실패함. 태스크는 로그 한 줄 없이 죽고, UI에는 로그 조회 실패만 뜸
  - 실제로 밟았음 → [장애 기록](../docs/incidents/2026-08-17-airflow-task-never-launched.md)
  - 환경변수만 잡고 끝내지 않고 **`airflow` 가 정말 venv 안에서 해석되는지 확인**까지 하는 건 그래서임

- **`--dry-run`을 먼저 쓰는 편이 안전함**
  - 적재 전략이 테이블마다 다름
  - 무엇이 지워지고 무엇이 덮이는지 SQL로 확인하고 실행할 것 → [../airflow/README.md](../airflow/README.md)

- **가격 변경 대상을 결정적으로 고름**
  - `order by id limit N`으로 뽑고 `mod(id, 2)`로 증감 방향을 정함
  - 매번 다른 상품이 무작위로 바뀌면 무엇이 왜 바뀌었는지 추적할 수 없음

- **왜 시뮬레이션이 필요한가**
  - `bigquery-public-data.thelook_ecommerce`는 우리가 바꿀 수 없는 공개 데이터셋이라 상품 가격이 변하지 않음
  - snapshot을 아무리 돌려도 버전이 하나뿐이고 유효 구간 전이가 만들어지지 않음
  - `raw_thelook.products`의 적재 전략이 `merge_insert_only`(신규 상품만 삽입)인 이유도 여기 있음. `full_replace`였다면 여기서 바꾼 값이 다음 배치에 되돌아감

- **샌드박스에서는 둘 다 제한됨**
  - `run_el.py`의 `partition_overwrite`·`merge_insert_only`는 DML(DELETE/MERGE)을 씀
  - `simulate_price_change.py`는 UPDATE를 씀
  - BigQuery 샌드박스는 DML을 차단함 → [../README.md](../README.md)

## 4. 실행

**airflow_env.sh** — Airflow를 만지는 셸에서 가장 먼저

```bash
export THELOOK_GCP_PROJECT=<PROJECT>
export THELOOK_DBT_BIN=/path/to/dbt-core-1.8/bin/dbt

# venv가 레포 밖에 있으면
export THELOOK_AIRFLOW_VENV=/path/to/.venv-airflow

source scripts/airflow_env.sh
```

- `source` 임. `./scripts/airflow_env.sh` 로 실행하면 막힘
- 셸을 새로 열 때마다 다시 불러야 함 → [../airflow/README.md](../airflow/README.md) 4번

**airflow_ui.py**

```bash
source scripts/airflow_env.sh     # 반드시 먼저
python scripts/airflow_ui.py                  # localhost:8080
python scripts/airflow_ui.py --port 8081      # 포트 변경
```

**run_el.py**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

# 최초 전체 적재
python scripts/run_el.py --project <PROJECT> --start 2019-01-01 --end 2026-08-31

# 일 배치와 같은 구간 (오늘 기준 3일)
python scripts/run_el.py --project <PROJECT> --lookback 3

# 특정 테이블만
python scripts/run_el.py --project <PROJECT> --tables orders,order_items

# 실행하지 않고 SQL만 출력
python scripts/run_el.py --project <PROJECT> --dry-run
```

**simulate_price_change.py**

```bash
# 1. 가격 변경 (기본 20개 상품, ±15%)
python scripts/simulate_price_change.py --project <PROJECT>

# 2. 스냅샷을 다시 돌려 새 버전을 쌓음
cd dbt && dbt snapshot && cd ..

# 3. 이력 확인 — 버전이 2개 이상인 상품만 나옴
python scripts/simulate_price_change.py --project <PROJECT> --show
```
