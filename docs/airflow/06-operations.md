# 실전 운영

> Airflow  ·  [← dbt 와의 역할 분담](05-dbt-integration.md) | [목차](README.md) | [부록 →](07-appendix.md)

Connection · Variable · Hook 로 자격증명을 코드에서 분리하는 방법, Executor 와 동시성 제어, 데이터 기반 스케줄링, 명령어 지도를 다룸.

## 6-1. Airflow Connection · Variable · Hook

### 6-1-1. 정의

| 개념 | 용도 |
|---|---|
| **Connection** | 외부 시스템 접속 정보(호스트, 계정, 키). **코드에 자격증명을 두지 않기 위한 장치** |
| **Variable** | 환경별 설정값. `Variable.get('target_dataset')` |
| **Hook** | Connection을 사용해 외부 시스템과 통신하는 인터페이스. Operator 내부에서 쓰임 |

```
Connection            접속 정보 (호스트·계정·키)  — UI 나 env 로 등록
     ↓ 사용
Hook                  그 정보로 외부 시스템과 통신하는 인터페이스
     ↓ 내부에서 사용
Operator              태스크로 노출되는 단위
```

- **Operator 는 대개 Hook 을 감싼 것임**
  - `BigQueryInsertJobOperator` 안에 `BigQueryHook` 이 있고, 그 Hook 이 `gcp_conn_id` 로 Connection 을 찾아 붙음

- **Hook 을 직접 쓰는 경우**는 전용 Operator 로 표현되지 않는 일을 할 때임

```python
@task
def check_row_count():
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    df = hook.get_pandas_df("select count(*) as n from raw.orders")
    return int(df.iloc[0]["n"])
```

- 자격증명은 여전히 Connection 에서 오므로 **코드에 키가 남지 않음**
- `BaseHook` 을 상속해 커스텀 Hook 을 만들 수도 있음

- **보안 관점이 중요함** (→ `docs/tech/data-security.md` *(작성 예정)*)
  - 기본 저장소는 메타DB이고, 운영에서는 **Secrets Backend** (GCP Secret Manager, AWS Secrets Manager, Vault)로 외부화함

- **주의**: `Variable.get()`을 DAG 파일 최상단에 두면 스케줄러가 파싱할 때마다 DB를 조회해 전체가 느려짐
  - 태스크 안에서 호출함

## 6-2. Executor와 동시성 제어

### 6-2-1. 정의

**Executor는 태스크를 어디서 실행할지를 정함.**

| Executor | 실행 위치 | 용도 |
|---|---|---|
| **LocalExecutor** | 스케줄러와 같은 머신의 프로세스 | 소규모, 단일 노드 |
| **CeleryExecutor** | 워커 풀에 분산 | 중대규모. 워커 수로 확장 |
| **KubernetesExecutor** | 태스크마다 파드 생성 | 격리·탄력적 확장. 기동 오버헤드 |

**동시성 제어 계층**

| 설정 | 범위 |
|---|---|
| `max_active_runs` | **DAG당 동시 run 수.** 백필 폭주 방지 |
| `max_active_tasks` | DAG당 동시 태스크 수 |
| **`pool`** | **여러 DAG에 걸친 공유 자원 제한.** DW 커넥션 보호에 필수 |

```bash
# pool 생성 — DW 동시 쿼리에 상한을 건다
airflow pools set dw_pool 5 "BigQuery 동시 실행 제한"
```

```python
BashOperator(
    task_id="dbt_build_marts_core",
    bash_command="dbt build --select marts.core",
    pool="dw_pool",
    priority_weight=10,        # 같은 pool 안에서 먼저 슬롯을 잡는다
)
```

- **pool이 실무에서 가장 중요함**
  - DAG 10개가 각자 알아서 돌면 DW에 동시 쿼리가 몰려 전체가 느려짐
  - `dw_pool`을 만들고 슬롯 수를 제한하면 **DW 부하에 상한을 걺**

## 6-3. 데이터 기반 스케줄링 — Dataset / Asset (Airflow 2.4+)

### 6-3-1. 정의

- 기존 Airflow는 **시간 기반**이었음
- "매일 3시에 실행." `Dataset`은 **데이터 기반** 스케줄링을 가능하게 함
- "이 데이터가 갱신되면 실행."

```python
orders = Dataset("bigquery://project/raw/orders")

# 생산자 — 이 태스크가 끝나면 orders가 갱신된 것으로 표시
BashOperator(task_id="load_orders", outlets=[orders], ...)

# 소비자 — orders가 갱신되면 자동 실행
with DAG(dag_id="transform_orders", schedule=[orders]):
    ...
```

- **이게 해결하는 문제**: 기존에는 downstream DAG이 "upstream가 3시에 끝나겠지"라고 **추측해서** 4시로 잡았음
  - upstream가 늦으면 downstream는 빈 데이터를 처리함
  - `ExternalTaskSensor`로 기다릴 수도 있지만 슬롯을 쓰고 설정이 번거로움

## 6-4. 명령어 지도

> Airflow 는 UI 로 대부분을 할 수 있지만, **초기 구성과 장애 대응은 CLI 가 빠름.**
> 특히 커넥션·변수 등록과 재처리는 CLI 로 하는 편이 재현 가능함.

### 6-4-1. 기동과 상태 확인

| 명령 | 하는 일 |
|---|---|
| `airflow version` / `airflow info` | 버전과 설정 경로. **문제가 생기면 여기부터** |
| `airflow db migrate` | 메타DB 스키마 생성·업그레이드 |
| `airflow standalone` | 학습용. 스케줄러·웹서버·관리자 계정을 한 번에 |
| `airflow scheduler` | 스케줄러 기동 |
| `airflow api-server` (3.x) / `webserver` (2.x) | UI·API 기동 |
| `airflow triggerer` | deferrable 태스크 대기 처리 ([3-4](03-execution-failure.md) 참조) |
| `airflow dag-processor` | DAG 파싱 전담 프로세스 |
| `airflow config list` | 현재 적용된 설정 전체 |
| `airflow providers list` | 설치된 provider 확인 |

> **스케줄러가 없으면 자동 실행이 없음.** UI 만 떠 있으면 DAG 이 보이기만 하고 돌지 않음([1-1](01-architecture.md) 참조).

### 6-4-2. DAG 다루기

```bash
airflow dags list                      # 등록된 DAG
airflow dags list-import-errors        # 파싱 실패 원인. 새 DAG 이 안 보이면 여기부터
airflow dags unpause <dag_id>          # 활성화 (기본은 일시정지)
airflow dags trigger <dag_id>          # 지금 즉시 1회 실행
airflow dags trigger <dag_id> --conf '{"backfill_days": 7}'   # 파라미터와 함께 (1-4 참조)
airflow dags test <dag_id> 2026-08-15  # 스케줄러 없이 인프로세스로 전 구간 실행
airflow dags backfill <dag_id> --start-date 2026-07-01 --end-date 2026-07-31
```

| 헷갈리는 셋 | 차이 |
|---|---|
| `dags trigger` | **스케줄러를 통해** run 을 만듦. 실제 운영 경로와 같음 |
| `dags test` | **스케줄러 없이** 현재 프로세스에서 순차 실행. 개발 중 검증용 |
| `dags backfill` | 지정 구간의 여러 run 을 채움. `max_active_runs` 에 따라 순차/병렬 |

### 6-4-3. 태스크 다루기

```bash
airflow tasks list <dag_id>                  # 태스크 목록
airflow tasks test <dag_id> <task_id> 2026-08-15   # 태스크 하나만, 상태 기록 없이
airflow tasks states-for-dag-run <dag_id> <run_id> # 그 run 의 태스크별 상태
airflow tasks clear <dag_id> --task-regex "dbt_.*" --downstream \
    --start-date 2026-07-01 --end-date 2026-07-31
```

- **`tasks clear` 가 재처리의 기본 수단임**
  - 상태를 지우면 스케줄러가 다시 잡아 실행함
  - `--downstream` 으로 downstream까지, `--only-failed` 로 실패한 것만
- `tasks test` 는 **메타DB에 상태를 남기지 않음**. 로직만 확인할 때 씀

### 6-4-4. 커넥션·변수·풀

```bash
airflow connections add google_cloud_default \
    --conn-type google_cloud_platform \
    --conn-extra '{"key_path": "/path/sa.json", "project": "my-project"}'
airflow connections list
airflow variables set target_dataset analytics
airflow variables export vars.json        # 환경 이관용
airflow pools set dw_pool 5 "웨어하우스 동시 실행 제한"
```

> **`db migrate` 는 기본 커넥션을 만들지 않음.** 예전 `db init` 과 달라진 부분이라,
> 새 환경을 세울 때 `connections add` 를 빠뜨려 태스크가 전부 실패하는 일이 흔함.

---

[← dbt 와의 역할 분담](05-dbt-integration.md) | [목차](README.md) | [부록 →](07-appendix.md)
