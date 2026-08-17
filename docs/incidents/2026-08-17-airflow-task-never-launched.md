# 태스크가 실행되지 않고 즉시 실패

> 2026-08-17  ·  [← 장애 기록 목록](README.md)

`thelook_dw_daily` 의 EL 태스크 7개와 `dbt_deps` 가 재시도를 모두 소진하고 실패했고,
하위 태스크 9개가 `upstream_failed` 로 연쇄 실패함.

| | |
|---|---|
| 대상 | `thelook_dw_daily` · `scheduled__2026-08-16T03:00:00+00:00` |
| 발생 | 2026-08-17 06:07 UTC (첫 배치) ~ 06:35 UTC (재시도 소진) |
| 범위 | 워커 프로세스가 필요한 **모든 태스크**. 16/17개 실패 |
| 원인 분류 | 실행 환경 (코드·데이터 무관) |

## 1. 현상

**UI 에서 로그를 열면 로그 대신 이 메시지가 뜸**

```
*** Could not read served logs: Invalid URL
'http://:8793/log/dag_id=thelook_dw_daily/run_id=scheduled__2026-08-16T03:00:00+00:00/task_id=extract_load.load_orders/attempt=3.log':
No host supplied
```

**실패 범위**

```
start                        success      ← EmptyOperator
extract_load.load_orders     failed  (3)
extract_load.load_order_items          failed  (3)
extract_load.load_events               failed  (3)
extract_load.load_inventory_items      failed  (3)
extract_load.load_users                failed  (3)
extract_load.load_distribution_centers failed  (3)
extract_load.load_products             failed  (3)
dbt_deps                     failed  (3)   ← EL 과 무관한 태스크도 같이 실패
dbt_snapshot ~ end           upstream_failed
```

- `start` 만 성공함. `EmptyOperator` 는 스케줄러가 직접 success 로 처리하고 워커를 쓰지 않음
- **`dbt_deps` 도 같이 실패함.** `dbt deps` 만 돌리는 태스크로 BigQuery·GCP 와 무관함
- 태스크 로그 파일이 디스크에 **하나도 생성되지 않음** (`$AIRFLOW_HOME/logs/` 에 `dag_id=thelook_dw_daily` 디렉토리 자체가 없음)
- 같은 DAG 의 2026-08-15 **수동 실행은 17개 태스크 전부 성공**했음

**메타DB 의 태스크 상태**

```sql
SELECT task_id, state, try_number, hostname, start_date, duration
FROM task_instance WHERE dag_id='thelook_dw_daily';
```

| | 08-15 manual (성공) | 08-16 scheduled (실패) |
|---|---|---|
| `hostname` | `mac-mini.local` | **빈 문자열** |
| `start_date` | 있음 | **NULL** |
| `duration` | 있음 | **NULL** |
| `try_number` | 1 | 3 (소진) |

## 2. 원인

### 2-1. 처음에 틀렸던 추정

- **EL SQL 이나 BigQuery 문제** → 아님. `dbt_deps` 가 똑같이 실패했음. 그 태스크는 BigQuery 를 건드리지 않음
- **`google_cloud_default` 커넥션 미등록** → 아님. 8/15 수동 실행이 전 구간 성공했음
- **`raw_thelook` 데이터셋 없음** → 아님. 같은 이유
- **`trigger_rule` 이 `all_success` 라서** → 아님. 애초에 이 DAG 은 `trigger_rule` 을 설정하지 않음.
  그리고 `upstream_failed` 는 EL 실패의 **결과**지 원인이 아님

**"태스크가 하는 일과 무관하게 전부 같은 방식으로 죽었다"** 가 핵심 단서였음.
그러면 원인은 태스크 로직 밖에 있음.

### 2-2. 근본 원인

스케줄러의 `PATH` 에 Airflow venv 가 없어서, 태스크를 띄우는 서브프로세스가 기동에 실패함.

**연결 고리**

1. Airflow 는 태스크를 **별도 프로세스**로 실행함. 명령어를 이렇게 만듦

   ```python
   # airflow/models/taskinstance.py
   cmd = ["airflow", "tasks", "run", dag_id, task_id, run_id]
   ```

   `airflow` 가 절대경로가 아니라 **맨 이름**임. `PATH` 로 찾음

2. `SequentialExecutor` 가 그걸 그대로 실행함. 스케줄러의 `PATH` 를 물려받음

   ```python
   # airflow/executors/sequential_executor.py
   subprocess.check_call(command, close_fds=True)
   ```

3. 그런데 스케줄러가 **venv 활성화 없이 절대경로로** 떠 있었음

   ```
   /…/pipeline/.venv-airflow/bin/python3 /…/pipeline/.venv-airflow/bin/airflow scheduler
   ```

   이러면 `.venv-airflow/bin` 이 `PATH` 에 들어가지 않음

4. `PATH` 가 `airflow` 를 pyenv shim 으로 해석하고, 그건 깨져 있었음

   ```
   $ /Users/…/.pyenv/shims/airflow version
   pyenv: airflow: command not found
   ```

5. 서브프로세스가 1초 안에 죽음 → `CalledProcessError` → executor 가 "실패" 를 보고.
   그런데 태스크는 아직 `queued` 상태임. 메타DB `log` 테이블에 남은 원문:

   > Executor SequentialExecutor(parallelism=32) reported that the task instance
   > `<TaskInstance: thelook_dw_daily.dbt_deps … [queued]>` finished with state failed,
   > **but the task instance's state attribute is queued.**

6. 태스크가 실행된 적이 없으니 로그 파일도 없고 `hostname` 도 비어 있음.
   웹서버는 로컬 로그를 못 찾자 워커에서 가져오려고 `http://{hostname}:8793/log/…` 를 만드는데,
   `hostname` 이 빈 문자열이라 → **`Invalid URL 'http://:8793/…': No host supplied`**

**UI 에 뜬 메시지는 원인이 아니라 체인의 마지막 증상이었음.**

### 2-3. 뒷받침하는 관측

- 실패 8건이 **9밀리초 안에** 한꺼번에 보고됨 (`06:07:58.926` → `.935`).
  `SequentialExecutor` 는 한 번에 하나씩 돌리므로, 8개 프로세스를 실제로 띄웠다면 불가능한 간격임
- 8/15 수동 실행은 venv 를 활성화한 셸에서 돌렸음. 그래서 `hostname` 이 찍혀 있고 성공했음
- DAG 파싱은 정상이었음. `import_error` 테이블이 비어 있고, DagFileProcessor 로그에 파싱 성공이 계속 찍힘.
  **파싱과 실행은 다른 프로세스**라서 한쪽만 깨질 수 있음

**확인에 쓴 쿼리**

```sql
-- 태스크가 running 에 진입했는가 (start_date 가 NULL 이면 아님)
SELECT task_id, state, try_number, hostname, start_date, duration
FROM task_instance WHERE dag_id='thelook_dw_daily';

-- executor 가 무엇을 보고했는가
SELECT dttm, event, task_id, extra FROM log
WHERE dag_id='thelook_dw_daily' AND event='state mismatch' ORDER BY dttm;

-- DAG 파싱 자체는 정상이었는가
SELECT * FROM import_error;
```

## 3. 조치사항

### 3-1. 즉시 복구

`PATH` 에 venv 를 넣고 스케줄러를 다시 띄움. 이 레포는 [`scripts/airflow_env.sh`](../../scripts/airflow_env.sh) 가 그걸 포함해 처리함.

```bash
source scripts/airflow_env.sh     # PATH · AIRFLOW_HOME · DAGS_FOLDER 를 한 번에
airflow scheduler
```

- 실패한 run 은 UI 에서 Clear 하면 재실행됨

### 3-2. 재발 방지

| 반영한 것 | 위치 |
|---|---|
| `PATH` 에 venv 를 넣고, `airflow` 가 venv 안에서 해석되는지 **기동 전에 검증** | [`scripts/airflow_env.sh`](../../scripts/airflow_env.sh) |
| 절대경로 기동을 금지하는 경고와 올바른 실행법 | [`airflow/README.md`](../../airflow/README.md) 4번 |
| 체크리스트 항목 추가 | [`docs/airflow/07-appendix.md`](../airflow/07-appendix.md) 7-1 |

`airflow_env.sh` 가 검증하는 것:

- `airflow` 실행 파일이 지정한 venv 안에 있는가 — **이번 사고의 직접 원인**
- `AIRFLOW_HOME` · `AIRFLOW__CORE__DAGS_FOLDER` 가 이 레포를 가리키는가
- `THELOOK_GCP_PROJECT` 가 설정돼 있는가 — 없으면 DAG **임포트**가 깨짐 (증상이 또 다름)

### 3-3. 배운 것

- **`which` 로 잡히는 실행 파일을 신뢰하지 말 것.**
  이 레포는 [`airflow/README.md`](../../airflow/README.md) 에서 `dbt` 에 대해 같은 경고를 이미 하고 있었음
  (`$(which dbt)` 로 잡으면 pyenv 의 dbt-fusion 이 잡힘). 같은 함정이 이번엔 **`airflow` 자기 자신에게** 일어났음
  - → 다음 프로젝트: 도구를 venv 에 격리했으면 **기동 스크립트가 `PATH` 까지 책임질 것.**
    절대경로로 실행하는 건 격리가 아니라 격리의 반쪽임

- **"로그가 안 보인다" 와 "태스크가 실패했다" 는 다른 사건일 수 있음.**
  이번엔 같은 원인의 두 증상이었지만, 로그 조회 실패 메시지를 실패 원인으로 읽으면 엉뚱한 곳을 팜
  - → 다음 프로젝트: 로그가 비어 있으면 먼저 **`task_instance.start_date` 와 `hostname`** 을 봄.
    둘 다 비어 있으면 태스크는 시작조차 못 한 것이고, 원인은 태스크 밖에 있음

- **태스크가 하는 일과 무관하게 전부 같은 방식으로 죽으면 실행 환경을 의심할 것.**
  이번 건은 `dbt_deps`(로컬 bash) 와 `load_orders`(BigQuery) 가 동일하게 죽은 것이 결정적 단서였음

## 관련 문서

- **[airflow/README.md](../../airflow/README.md)** — 설치·환경변수·기동
- **[docs/airflow/01-architecture.md](../airflow/01-architecture.md)** — 스케줄러와 워커가 별개 프로세스인 이유
- **[docs/airflow/03-execution-failure.md](../airflow/03-execution-failure.md)** — 태스크 상태와 `trigger_rule`

---

[← 장애 기록 목록](README.md)
