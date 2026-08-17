# DAG 작성과 스케줄링

> Airflow  ·  [← Airflow 시스템 구성 요소](01-architecture.md) | [목차](README.md) | [실행 순서와 실패 처리 →](03-execution-failure.md)

DAG / Task / Operator 의 기본 구조와 XCom · TaskFlow · TaskGroup · `doc_md`, `logical_date` 의 함정, 템플릿과 파라미터, 백필과 멱등성을 다룸.

## 2-1. DAG / Task / Operator

### 2-1-1. 정의

- Airflow는 **워크플로 오케스트레이터**임
- "무엇을 언제 어떤 순서로 실행할지"를 관리함
- 데이터를 직접 처리하는 도구가 아님

| 개념 | 의미 |
|---|---|
| **DAG** | 방향성 비순환 그래프. 작업들과 그 순서의 집합. 스케줄의 단위 |
| **Task** | DAG의 노드. Operator의 인스턴스 |
| **Operator** | "무엇을 할지"의 템플릿. `BashOperator`, `PythonOperator`, `SQLExecuteQueryOperator` 등 |
| **Sensor** | 조건이 만족될 때까지 기다리는 특수한 Operator |
| **XCom** | Task 간 소량의 값 전달. **대용량 데이터 전달용이 아님** |

- 의존성은 `>>`로 표현함

```python
extract >> transform >> load
```

- Airflow 2.0의 **TaskFlow API**를 쓰면 `@task` 데코레이터로 Python 함수를 태스크로 만들고 반환값이 자동으로 XCom을 통해 전달됨

- **중요한 원칙 — DAG 파일은 스케줄러가 주기적으로 파싱함**
  - 따라서 DAG 파일 최상단에 무거운 연산이나 DB 조회를 두면 안 됨
  - 스케줄러 전체가 느려짐

### 2-1-2. XCom — 태스크 사이로 값 넘기기

- 태스크는 **서로 다른 프로세스(또는 다른 머신)에서 돎**
- 그래서 변수를 공유할 수 없고, 값을 넘기려면 메타DB를 거쳐야 함
- 그 통로가 XCom(cross-communication)임

```python
def extract(**context):
    context["ti"].xcom_push(key="row_count", value=1234)

def report(**context):
    n = context["ti"].xcom_pull(task_ids="extract", key="row_count")
```

- **가장 중요한 제약은 크기임**
  - XCom 값은 메타DB의 한 컬럼에 직렬화되어 들어감

| DB | 상한 |
|---|---|
| SQLite | 약 2GB (이론상) |
| PostgreSQL | 1GB |
| MySQL | 64KB |

- **숫자와 무관하게 대용량 데이터 전달용이 아님**
  - 배치마다 수 MB씩 쌓이면 메타DB가 부풀고 스케줄러 전체가 느려짐
- **데이터는 스토리지로 넘기고 XCom으로는 경로만 넘김**

```python
# 안티패턴 — 데이터프레임 자체를 넘김
return df

# 권장 — 경로만 넘김
return "gs://bucket/staging/2026-08-15/orders.parquet"
```

> **Custom XCom Backend**로 GCS·S3에 저장하고 메타DB에는 참조만 두는 구성도 있음.
> 큰 값을 넘겨야 하는 게 확실하면 그쪽으로 감.

### 2-1-3. TaskFlow API — 함수를 태스크로

- Airflow 2.0에서 들어온 데코레이터 방식임
- **XCom을 명시적으로 다루지 않아도 됨**

```python
from airflow.decorators import dag, task

@dag(schedule="0 3 * * *", start_date=datetime(2026, 1, 1), catchup=False)
def thelook_pipeline():

    @task
    def extract() -> str:
        return "gs://bucket/orders.parquet"

    @task
    def transform(path: str) -> int:
        ...
        return row_count

    transform(extract())      # 반환값이 자동으로 XCom 을 타고 넘어간다

thelook_pipeline()
```

| | 기존 Operator 방식 | TaskFlow |
|---|---|---|
| 의존성 | `a >> b` 로 직접 | **함수 호출 관계에서 자동 추론** |
| 값 전달 | `xcom_push` / `xcom_pull` | 반환값과 인자 |
| 적합한 곳 | Bash·SQL·외부 시스템 호출 | **Python 로직이 주인 경우** |

- **둘은 섞어 씀**
  - `BashOperator`로 dbt를 부르면서 Python 전처리는 `@task`로 쓰는 식임
  - TaskFlow가 기존 방식을 대체하는 게 아니라 Python 태스크의 문법을 줄인 것임

### 2-1-4. TaskGroup — 태스크를 논리적으로 묶기

- 태스크가 수십 개가 되면 Graph 화면이 읽히지 않음
- `TaskGroup`은 **UI에서 접을 수 있는 묶음**을 만들고, 의존성도 그룹 단위로 걸 수 있게 함

```python
with TaskGroup(group_id="extract_load") as extract_load:
    for spec in TABLES:
        BigQueryInsertJobOperator(task_id=f"load_{spec.name}", ...)

start >> extract_load >> transform      # 그룹 전체가 끝나야 다음으로
```

- 태스크 ID가 `extract_load.load_orders`처럼 **그룹명으로 네임스페이스가 붙음**
- 실행 단위가 아니라 **표현 단위**임
- 그룹 자체는 태스크가 아니고 실행되지 않음
- SubDAG(구버전)을 대체함
- SubDAG은 별도 스케줄러 부하를 만들어 폐기됨

### 2-1-5. doc_md — 설명을 코드와 함께 두기

- DAG과 태스크에 마크다운 설명을 붙이면 **UI에 그대로 렌더링됨**

```python
with DAG(dag_id="...", doc_md=__doc__) as dag:      # 모듈 docstring 을 그대로
    BashOperator(
        task_id="dbt_snapshot",
        doc_md="products 의 SCD Type 2 이력. **staging 보다 먼저 돈다** — ...",
    )
```

- **dbt의 `description`이 `dbt docs`가 되는 것과 같은 구조임**
  - 별도 위키에 적으면 코드와 갈라지지만, `doc_md`는 코드와 함께 움직임

> **설계 근거는 `doc_md`에, 실행 방법은 README에 둠.** 이렇게 나누면
> "왜 이렇게 만들었나"를 운영자가 UI에서 바로 볼 수 있고, 문서가 중복되지 않음.

## 2-2. 스케줄링과 logical_date의 함정

### 2-2-1. 정의

**Airflow에서 가장 많이 틀리는 지점임.**

- `logical_date`(구 `execution_date`)는 **DAG이 실제로 실행된 시각이 아님**
- **처리 대상 데이터 구간의 시작 시점**임

일 배치를 예로 들면:

```
스케줄: 매일 00:00
2026-08-15 00:00에 시작된 run
  → logical_date = 2026-08-14
  → 처리 대상 = 2026-08-14 하루치
```

- **왜 이런가**
  - 배치는 **구간이 끝나야 그 구간을 처리할 수 있기 때문**임
  - 8월 14일 데이터는 8월 15일 0시가 되어야 완결됨
  - 그래서 8월 15일에 도는 작업이 8월 14일 것을 처리하고, 이름표는 8월 14일이 붙음

- Airflow 2.2부터 `data_interval_start` / `data_interval_end`가 도입되어 이 의미가 명시적으로 드러남
- `{{ ds }}`는 `logical_date`의 날짜 부분임

### 2-2-2. schedule 을 표현하는 방법

| 방식 | 예 | 비고 |
|---|---|---|
| **cron 표현식** | `"0 3 * * *"` | 가장 흔함. 절대 시각 기준 |
| **프리셋** | `"@daily"`, `"@hourly"`, `"@once"` | cron 의 별칭 |
| `timedelta` | `timedelta(hours=6)` | **직전 run 기준 상대 간격** |
| `None` | `schedule=None` | 자동 실행 없음. 수동·외부 트리거 전용 |
| **Asset / Dataset** | `schedule=[orders]` | 데이터 갱신 기반([6-3](06-operations.md) 참조) |

- **cron 표현식** — 다섯 자리가 각각 분·시·일·월·요일임

```
┌─ 분 (0-59)
│ ┌─ 시 (0-23)
│ │ ┌─ 일 (1-31)
│ │ │ ┌─ 월 (1-12)
│ │ │ │ ┌─ 요일 (0-6, 0=일요일)
* * * * *
```

| 표현식 | 의미 |
|---|---|
| `0 3 * * *` | 매일 03:00 |
| `30 * * * *` | 매시 30분 |
| `0 */2 * * *` | 2시간마다 정각 |
| `0 6-9 * * *` | 매일 06·07·08·09시 정각 |
| `0 22 * * 1-5` | 월~금 22:00 |
| `0 0 1 * *` | 매월 1일 00:00 |

> **`cron` 과 `timedelta` 는 기준이 다름.** cron 은 **벽시계 시각**에 맞추고,
> `timedelta` 는 **직전 run 으로부터의 간격**을 셈. 실행이 밀리면 뒤가 다르게 움직임.

**여기서 나오는 최악의 실수:**

```python
# 절대 하면 안 되는 것
today = datetime.now().strftime('%Y-%m-%d')
```

- `now()`를 쓰면 **백필이 완전히 망가짐**
- 3개월 전 데이터를 다시 돌려도 오늘 날짜로 처리해버림
- 반드시 `{{ ds }}`나 `{{ data_interval_start }}`를 써야 함

## 2-3. 템플릿과 파라미터

### 2-3-1. 정의

- **모든 필드에 Jinja 가 적용되지 않음**
  - Operator 마다 `template_fields` 로 어떤 인자가 렌더링되는지 정해져 있음

| Operator | 템플릿 되는 필드 |
|---|---|
| `BashOperator` | `bash_command`, `env`, `cwd` |
| `BigQueryInsertJobOperator` | `configuration`, `job_id`, `impersonation_chain` |
| `PythonOperator` | `op_args`, `op_kwargs`, `templates_dict` |

```python
# 동작한다 — bash_command 는 template_fields 에 있다
BashOperator(bash_command="dbt build --vars '{\"run_date\": \"{{ ds }}\"}'")

# 동작하지 않는다 — 이 인자는 템플릿 대상이 아니다
BashOperator(task_id="load_{{ ds }}")
```

- **`template_ext`** 로 파일도 렌더링됨
  - `.sql` / `.sh` 파일 경로를 넘기면 파일 내용을 읽어 Jinja 를 적용함
- **SQL 을 코드 문자열이 아니라 파일로 관리**할 수 있음

**자주 쓰는 템플릿 변수**

| 변수 | 값 |
|---|---|
| `{{ ds }}` / `{{ ds_nodash }}` | `2026-08-15` / `20260815` |
| `{{ data_interval_start }}` / `{{ data_interval_end }}` | 처리 구간 |
| `{{ macros.ds_add(ds, -3) }}` | 날짜 연산 |
| `{{ dag_run.conf }}` | 수동 트리거 시 넘긴 값 |
| `{{ params.x }}` | DAG `params` 기본값 + `conf` 로 덮어쓴 값 |
| `{{ ti }}` / `{{ task_instance }}` | XCom 접근 |

**`params` — DAG 에 파라미터를 선언함**

```python
with DAG(
    params={"backfill_days": Param(3, type="integer", minimum=1, maximum=30)},
    ...
):
    BashOperator(bash_command="run.sh --days {{ params.backfill_days }}")
```

- UI 의 **Trigger DAG w/ config** 화면에 입력 폼이 만들어지고, 타입·범위가 검증됨
- 운영자가 코드를 고치지 않고 재처리 범위를 조정할 수 있게 하는 장치임

## 2-4. 백필과 멱등성

### 2-4-1. 정의

- **백필(backfill)**은 과거 구간을 다시 실행하는 것임
  - 새 파이프라인을 만들었을 때 과거 데이터를 채우거나, 로직 버그를 고친 뒤 영향받은 구간을 다시 계산할 때 씀

- `catchup=True`면 `start_date`부터 현재까지 밀린 run을 자동으로 채움
- (대부분 `catchup=False`로 두고 필요할 때 수동 백필함
- 안 그러면 DAG을 켜는 순간 수백 개 run이 한꺼번에 뜸.)

- **멱등성(idempotency)이 백필의 전제조건임**
  - 같은 `logical_date`로 몇 번을 실행해도 결과가 같아야 함

| | 안티패턴 | 멱등한 방식 |
|---|---|---|
| 적재 | `INSERT INTO ...` (재실행 시 중복) | `DELETE WHERE date = {{ ds }}` 후 `INSERT` |
| 적재 | | 파티션 덮어쓰기 (`insert_overwrite`) |
| 적재 | | `MERGE` (upsert) |
| 기준 시각 | `now()` | `{{ ds }}` |
| 순번 | auto increment에 의존 | 결정적 키 사용 |

**코드로 보면 차이가 분명함.**

```python
# 안티패턴 — 실행할 때마다 다른 구간을 처리하고, 재실행하면 중복된다
def load():
    today = datetime.now().strftime("%Y-%m-%d")          # 백필이 망가진다
    cursor.execute(f"INSERT INTO fct_orders SELECT * FROM raw WHERE d = '{today}'")

# 멱등한 방식 — 기준일을 밖에서 받고, 그 구간을 통째로 교체한다
BashOperator(
    task_id="load_orders",
    bash_command=(
        "bq query --use_legacy_sql=false "
        "\\"DELETE FROM fct_orders WHERE ordered_date = '{{ ds }}'; "
        " INSERT INTO fct_orders SELECT * FROM raw WHERE d = '{{ ds }}'\\""
    ),
)
```

**백필 실행**

```bash
# 구간을 지정해 다시 돌린다. max_active_runs 설정에 따라 순차/병렬이 갈린다
airflow dags backfill thelook_dw_daily \
    --start-date 2026-07-01 --end-date 2026-07-31

# 특정 태스크만
airflow tasks clear thelook_dw_daily \
    --task-regex "dbt_build_marts.*" \
    --start-date 2026-07-01 --end-date 2026-07-31
```

> **`clear` 가 재처리의 기본 수단임.** 태스크 상태를 지우면 스케줄러가
> 다시 잡아 실행함. downstream까지 함께 지우려면 `--downstream` 을 붙임.

---

[← Airflow 시스템 구성 요소](01-architecture.md) | [목차](README.md) | [실행 순서와 실패 처리 →](03-execution-failure.md)
