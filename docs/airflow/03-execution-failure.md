# 실행 순서와 실패 처리

> Airflow  ·  [← DAG 작성과 스케줄링](02-dag-scheduling.md) | [목차](README.md) | [Dynamic Mapping · 환경 격리 · 테스트 · Airflow 3 →](04-advanced.md)

태스크 상태 11종과 `trigger_rule` 의 관계, 재시도와 SLA, 분기와 조건 실행, Sensor 로 외부 의존을 기다리는 방법을 다룸.

## 3-1. 태스크 상태와 생애주기

> **`trigger_rule`을 이해하려면 상태를 먼저 알아야 함.** "실패"와 "건너뜀"과
> "upstream 실패"는 서로 다른 상태이고, 규칙마다 무엇을 통과시키는지가 다름.

### 3-1-1. 정의

| 상태 | 의미 |
|---|---|
| `scheduled` | 실행 조건을 만족해 스케줄러가 잡음 |
| `queued` | Executor 큐에 들어감 |
| `running` | 실행 중 |
| `success` / `failed` | 종료 |
| `up_for_retry` | 실패했고 재시도가 남음 |
| `up_for_reschedule` | Sensor 가 `reschedule` 모드로 슬롯을 놓고 대기 |
| **`upstream_failed`** | **upstream가 실패해서 실행되지 못함.** 자기가 실패한 게 아님 |
| **`skipped`** | **분기에서 선택되지 않음**, 또는 `ShortCircuit` 에 걸림 |
| `deferred` | Triggerer 에 넘겨 비동기 대기 중 |
| `removed` | DAG 에서 사라진 태스크의 과거 인스턴스 |

**`trigger_rule`이 무엇을 "끝난 것"으로 보는지가 핵심임.**

| trigger_rule | 통과 조건 |
|---|---|
| `all_success` (기본) | upstream 전부 `success` |
| `all_failed` | upstream 전부 실패 |
| **`all_done`** | upstream 전부 **종료 상태.** `failed`·`skipped`·`upstream_failed` 포함 |
| `one_success` / `one_failed` | 하나라도 |
| `none_failed` | 실패 없음. `skipped` 는 허용 |
| **`none_failed_min_one_success`** | **분기 뒤 합류 지점의 표준값** |

> **`all_done`을 "실패해도 넘어가게" 용도로 쓰면 안 됨.** 이건 **"내가 언제 시작할지"**를
> 정하지 **"내 실패가 downstream를 막을지"**를 정하지 않음.
> upstream가 실패해도 통과시키므로, 데이터가 없는 날 변환이 그대로 도는 경로가 열림.
>
> **"이 실패는 무시하고 싶다"는 의존성 위치로 표현함.**
> 게이트가 아닌 태스크는 downstream 태스크를 두지 않음.

**함정을 코드로 보면**

```python
# 잘못된 방식 — 의도: "신선도 경고로 배치를 멈추지 말자"
#    실제: EL 이 실패해 upstream가 upstream_failed 가 되어도 통과한다.
#          원천이 안 들어온 날 dbt 가 그대로 돈다.
extract_load >> dbt_deps >> freshness >> dbt_build
freshness.trigger_rule = "all_done"

# 올바른 방식 — 게이트가 아닌 것은 downstream를 갖지 않게 둔다. downstream 이 없으면 막힐 것도 없다.
start >> [extract_load, dbt_deps]
[extract_load, dbt_deps] >> freshness          # downstream 없음 — 관측 전용
[extract_load, dbt_deps] >> dbt_build >> end   # 변환 체인
```

```python
# 분기 뒤 합류 — 갈래 하나가 skipped 라도 진행되어야 한다
join = EmptyOperator(
    task_id="join",
    trigger_rule="none_failed_min_one_success",
)
[full_rebuild, incremental_load] >> join
```

**`depends_on_past` / `wait_for_downstream`**

| 설정 | 동작 | 주의 |
|---|---|---|
| `depends_on_past=True` | **직전 run 의 같은 태스크가 성공해야** 실행 | 한 번 실패하면 이후 run 이 전부 멈춤 |
| `wait_for_downstream=True` | 직전 run 의 downstream까지 성공해야 실행 | 더 강한 제약 |

- 누적 계산처럼 순서가 의미를 갖는 경우에만 켬
- **기본은 끄는 것**이고, 켜두면 오래된 실패 하나가 파이프라인 전체를 조용히 정지시킴

## 3-2. 의존성 · 재시도 · SLA

### 3-2-1. 정의

- **의존성**은 `>>` `<<`로 정의함
  - `trigger_rule`로 조건을 바꿀 수 있음

| trigger_rule | 동작 |
|---|---|
| `all_success` (기본) | 모든 upstream가 성공해야 실행 |
| `all_done` | 성공·실패 무관, 끝나기만 하면 실행 (정리 작업용) |
| `one_failed` | 하나라도 실패하면 실행 (알림용) |
| `none_failed_min_one_success` | 실패 없고 최소 하나 성공 |

- **재시도**는 `retries`, `retry_delay`, `retry_exponential_backoff`로 설정함

- **여기서 중요한 판단**: 재시도로 해결되는 실패와 아닌 실패를 구분해야 함
  - 네트워크 타임아웃이나 일시적 리소스 부족은 재시도가 맞지만, **데이터가 틀린 경우는 100번 재시도해도 똑같이 틀림**
  - 오히려 실패를 숨겨서 발견을 늦춤

- **SLA**는 "이 작업이 언제까지 끝나야 하는가"임
  - Airflow 2.x에서는 task의 `sla` 파라미터와 `sla_miss_callback`으로 다룸

주의할 점 두 가지.

- **SLA를 넘겨도 태스크를 강제로 끝내지는 않음**
  - 콜백만 발생함. 실제로 중단시키려면 `execution_timeout` 을 써야 함
- **버전에 따라 동작과 지원 범위가 달라져 왔음**
  - 실제 환경의 버전을 확인해야 함

### 3-2-2. 자주 쓰는 파라미터 — DAG 레벨과 Task 레벨

- **어디에 쓰는지가 갈림**
  - `DAG(...)` 인자는 **워크플로 자체**의 성질이고, `default_args` 는 **모든 태스크에 공통 적용**될 값임

**DAG 레벨**

| 파라미터 | 역할 |
|---|---|
| `dag_id` | 고유 식별자 |
| `schedule` | 실행 주기 ([2-2](02-dag-scheduling.md)) |
| `start_date` / `end_date` | 스케줄 계산의 시작·종료 |
| `catchup` | 밀린 구간 자동 백필 여부 ([2-4](02-dag-scheduling.md)) |
| `max_active_runs` | 동시 run 상한 ([6-2](06-operations.md)) |
| `max_active_tasks` | DAG 내 동시 태스크 상한 |
| `tags` | UI 필터링·분류 |
| `params` | 실행 파라미터 선언 ([2-3](02-dag-scheduling.md)) |
| `default_args` | 아래 값들을 태스크 전체에 전달 |
| `doc_md` | UI 에 뜨는 설명 ([2-1](02-dag-scheduling.md)) |

**Task 레벨 (`default_args` 또는 개별 태스크)**

| 파라미터 | 역할 |
|---|---|
| `owner` | 담당자 |
| `retries` / `retry_delay` / `retry_exponential_backoff` | 재시도 |
| **`execution_timeout`** | **태스크 최대 실행 시간. 넘으면 강제 종료함** |
| `on_failure_callback` / `on_success_callback` / `on_retry_callback` | 이벤트 콜백 |
| `sla` / `sla_miss_callback` | 기한 (3-2) |
| `depends_on_past` / `wait_for_downstream` | 이전 run 과의 관계 (3-1) |
| `trigger_rule` | 실행 조건 (3-1) |
| `pool` / `priority_weight` | 자원 제한·우선순위 ([6-2](06-operations.md)) |
| `queue` | 특정 워커 큐로 보냄 |

> **`execution_timeout` 은 반드시 걸어야 함.** 없으면 쿼리 하나가 걸렸을 때
> 태스크가 **영원히 running 으로 남아** 슬롯을 점유한 채 다음 run 까지 막음.
> SLA 는 알림만 주고 강제로 끝내지는 않는다는 점과 대비됨.

**코드**

```python
# 의존성 표현
start >> [load_a, load_b] >> transform >> end     # 리스트로 팬아웃·팬인
start >> load_a >> transform
start >> load_b >> transform                       # 위와 같은 그래프

# 태스크 단위 재시도 — default_args 를 개별 태스크가 덮어쓴다
BashOperator(
    task_id="dbt_docs_generate",
    bash_command="dbt docs generate",
    retries=0,                       # 문서 생성 실패로 배치를 실패시키지 않는다
)

# SLA — 넘겨도 죽이지 않는다. 콜백만 발생한다
BashOperator(
    task_id="dbt_build_marts_core",
    bash_command="dbt build --select marts.core",
    sla=timedelta(minutes=30),
    execution_timeout=timedelta(hours=1),   # 이쪽이 실제로 태스크를 끝냄
)

# 정리 태스크 — 앞이 실패해도 돈다
EmptyOperator(task_id="cleanup", trigger_rule="all_done")
```

> **`sla` 와 `execution_timeout` 은 구분해야 함.**
> `sla` 는 **"늦었다"를 알리고**, `execution_timeout` 은 **"그만해라"로 강제 종료함.**

## 3-3. 분기와 조건 실행

### 3-3-1. 정의

- **`BranchPythonOperator`** — 실행할 태스크 ID를 반환하면 나머지 갈래는 `skipped` 가 됨

```python
@task.branch
def choose_path(**context):
    if context["ds"] == context["ds"]:   # 예: 월말이면 전체 재계산
        return "full_rebuild"
    return "incremental_load"
```

- **`ShortCircuitOperator`** — falsy 를 반환하면 **downstream 전부**를 건너뜀

```python
ShortCircuitOperator(
    task_id="skip_if_no_data",
    python_callable=lambda: row_count > 0,
)
```

| | 쓰는 곳 |
|---|---|
| `branch` | **여러 경로 중 하나**를 고름 |
| `ShortCircuit` | 조건이 아니면 **아예 안 함** |

> **분기 뒤 합류 지점에서 자주 틀림.** 갈래 하나가 `skipped` 인데
> 합류 태스크가 기본값 `all_success` 면 그 태스크도 건너뛰어짐.
> **`trigger_rule="none_failed_min_one_success"`** 를 걸어야 함.

## 3-4. Sensor와 외부 의존 대기

### 3-4-1. 정의

- **Sensor**는 조건이 만족될 때까지 기다리는 Operator임
  - `ExternalTaskSensor`(다른 DAG의 태스크 완료), `FileSensor`, `S3KeySensor`, `SqlSensor`(쿼리 결과가 조건을 만족할 때까지) 등이 있음

**모드가 성능을 좌우함.**

| 모드 | 동작 | 문제 |
|---|---|---|
| `poke` (기본) | 워커 슬롯을 잡고 주기적으로 확인 | **긴 대기 시 슬롯 고갈.** 센서만 잔뜩 떠서 실제 작업이 못 돔 |
| `reschedule` | 확인 후 슬롯을 놓고, 다음 확인 때 다시 잡음 | 긴 대기에 적합 |
| **deferrable** | Triggerer가 비동기로 감시. 슬롯 점유 없음 | 권장. 별도 triggerer 프로세스 필요 |

- **`timeout`을 반드시 설정함**
  - 안 그러면 영원히 기다림

```python
from airflow.providers.common.sql.sensors.sql import SqlSensor

wait_for_upstream = SqlSensor(
    task_id="wait_for_raw_orders",
    conn_id="bigquery_default",
    sql="select count(*) from raw_thelook.orders where date(_ingested_at) = '{{ ds }}'",
    mode="reschedule",              # 슬롯을 놓고 대기 — 긴 대기의 기본값
    poke_interval=300,              # 5분마다 확인
    timeout=60 * 60 * 4,            # 4시간이면 포기. **반드시 건다**
)

# 다른 DAG 의 태스크 완료를 기다린다
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_el = ExternalTaskSensor(
    task_id="wait_for_el_dag",
    external_dag_id="raw_ingest_daily",
    external_task_id="end",
    execution_delta=timedelta(hours=1),   # upstream DAG 과 logical_date 차이
    mode="reschedule",
    timeout=60 * 60 * 2,
)
```

> **`execution_delta` 를 빼먹으면 영원히 기다림.** `ExternalTaskSensor` 는
> **같은 `logical_date`** 의 태스크를 찾음. upstream DAG 의 스케줄이 다르면
> 그 차이를 알려줘야 함.

---

[← DAG 작성과 스케줄링](02-dag-scheduling.md) | [목차](README.md) | [Dynamic Mapping · 환경 격리 · 테스트 · Airflow 3 →](04-advanced.md)
