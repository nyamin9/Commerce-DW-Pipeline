# Dynamic Mapping · 환경 격리 · 테스트 · Airflow 3

> Airflow  ·  [← 실행 순서와 실패 처리](03-execution-failure.md) | [목차](README.md) | [dbt 와의 역할 분담 →](05-dbt-integration.md)

실행 시점에 태스크 개수를 정하는 Dynamic Task Mapping, Python 실행 환경을 격리하는 다섯 가지 방법, DAG 테스트 3층, Airflow 3 에서 달라진 것을 다룸.

## 4-1. Dynamic Task Mapping (Airflow 2.3+)

### 4-1-1. 정의

- **태스크 개수를 실행 시점에 정함**
  - 그전에는 DAG 파싱 시점에 개수가 고정돼야 해서, "파일 개수만큼 태스크를 만든다" 같은 것이 불가능했음

```python
@task
def list_files() -> list[str]:
    return ["a.csv", "b.csv", "c.csv"]      # 개수를 미리 모른다

@task
def process(path: str) -> int:
    ...

process.expand(path=list_files())            # 파일 수만큼 태스크 인스턴스 생성
```

- `.partial(fixed=...)` 로 **고정 인자**를 주고 `.expand()` 로 변하는 인자를 폄
- 각 인스턴스는 `map_index` 로 구분되고 **개별 재시도가 됨**
- 기본 상한은 1024개(`max_map_length`)

```python
process.partial(bucket="my-bucket").expand(path=list_files())
```

- **언제 쓰나** — 대상이 데이터에 따라 달라질 때임
  - 파티션 목록, 파일 목록, 테이블 목록
  - 반대로 **대상이 코드에 고정돼 있으면 `for` 문으로 태스크를 만드는 게 나음**
  - 적재할 테이블 목록을 설정 파일에 적어 두는 경우가 그렇다 — 파싱 시점에 개수를 이미 알고 있음

> **UI 부담을 생각함.** 매핑이 수백 개로 벌어지면 Grid 가 무거워지고
> 메타DB 행도 그만큼 늘어남. 자연스러운 배치 단위로 묶는 편이 나음.

## 4-2. Python 실행 환경 격리

### 4-2-1. 정의

- **DAG 파일은 스케줄러 프로세스가 파싱함**
  - 그래서 DAG 최상단에 무거운 라이브러리를 import 하면 스케줄러가 그 의존성을 지게 되고, 버전 충돌이 곧 스케줄러 장애가 됨

| 방법 | 동작 | 비용 |
|---|---|---|
| `PythonVirtualenvOperator` | **실행할 때마다 venv 를 새로 만듦** | 매번 설치 시간 |
| `ExternalPythonOperator` | **이미 만들어 둔 venv 의 파이썬**으로 실행 | 사전 준비 필요 |
| `@task.virtualenv` / `@task.external_python` | 위 둘의 TaskFlow 문법 | 〃 |
| `BashOperator` | 다른 venv 의 실행 파일을 그냥 호출 | 값 전달이 문자열뿐 |
| `KubernetesPodOperator` | **별도 이미지의 파드**에서 실행 | 인프라 복잡도, 기동 시간 |
| `DockerOperator` | 로컬 도커 컨테이너 | 〃 |

```python
# ① 실행할 때마다 venv 를 새로 만든다 — 느리지만 준비가 필요 없다
@task.virtualenv(requirements=["pandas==2.2.0"], system_site_packages=False)
def transform(path: str) -> int:
    import pandas as pd
    return len(pd.read_parquet(path))

# ② 미리 만들어 둔 venv 의 파이썬으로 실행한다 — 빠르다
@task.external_python(python="/opt/venvs/analytics/bin/python")
def score(path: str) -> float:
    import lightgbm
    ...

# ③ 별도 이미지의 파드에서 실행한다 — 격리가 가장 완전하다
KubernetesPodOperator(
    task_id="heavy_transform",
    image="asia-northeast3-docker.pkg.dev/proj/repo/transform:1.4.2",
    cmds=["python", "main.py", "--date", "{{ ds }}"],
    get_logs=True,
)

# ④ 그냥 다른 venv 의 실행 파일을 부른다 — CLI 도구에 가장 단순하다
BashOperator(
    task_id="dbt_build",
    bash_command="cd /opt/dbt && /opt/venvs/dbt/bin/dbt build --target prod",
)
```

- **dbt 를 부를 때 `BashOperator` 를 쓰는 이유가 여기 있음**
  - dbt 와 Airflow 는 의존성이 충돌해서 같은 venv 에 두기 어렵고, dbt 는 CLI 도구라 값을 돌려받을 필요도 없음
- **프로세스 경계를 넘기만 하면 되므로 가장 단순한 수단이 맞음**

> **규모가 커지면 `KubernetesPodOperator` 로 감.** 태스크마다 이미지를 고정할 수 있어
> 의존성 격리가 완전해지고, 워커 자원도 태스크 단위로 잡힘.
> 대신 이미지 빌드·레지스트리·기동 지연이 붙음.

## 4-3. DAG 테스트

### 4-3-1. 정의

- **DAG 도 코드라서 테스트함**
  - 세 층으로 나뉨

- **① 파싱 검증** — import 에러와 순환 의존을 잡음
  - CI 의 최소선임

```python
def test_dag_loads():
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert bag.import_errors == {}
```

- **② 구조 검증** — 태스크 수, 의존 관계, 설정값이 의도대로인지 봄

```python
def test_freshness_is_not_a_gate():
    dag = DagBag("dags/").get_dag("thelook_dw_daily")
    assert dag.get_task("dbt_source_freshness").downstream_task_ids == set()
```

- **③ 실제 실행** — `dag.test()` 또는 `airflow dags test <dag_id> <date>`
  - 스케줄러·웹서버 없이 **인프로세스로 태스크를 순차 실행**함
  - 로컬에서 DAG 을 검증하는 가장 빠른 방법임

```bash
airflow dags test thelook_dw_daily 2026-08-15
```

> **DAG 파일에 무거운 최상단 코드를 두지 않아야 하는 이유가 여기서도 나옴.**
> 스케줄러는 DAG 파일을 **주기적으로 반복 파싱**함. 최상단에서 DB 를 조회하거나
> API 를 부르면 그 비용이 파싱 주기마다 발생해 스케줄러 전체가 느려짐.
> `Variable.get()` 을 최상단에 두지 말라는 것도 같은 이야기다([6-1](06-operations.md) 참조).

## 4-4. Airflow 3에서 달라진 것

> **2.x 와 3.x 는 운영 모델이 다름.** 어느 쪽을 쓰는지에 따라 전제가 달라지므로
> 기존 환경에 합류하거나 버전을 올릴 때 차이를 먼저 확인해야 함.

### 4-4-1. 정의

| 영역 | Airflow 2.x | Airflow 3.x |
|---|---|---|
| **태스크와 메타DB** | 워커가 **메타DB에 직접 접속** | **Task Execution API** 를 거침. 워커가 DB 자격증명을 갖지 않음 |
| **DAG 버전** | 코드를 바꾸면 **과거 run 의 그래프도 바뀐 것으로 보임** | **DAG 버전 관리.** run 이 실행 당시 버전을 유지 |
| **데이터 기반 스케줄링** | `Dataset` | **`Asset`** 으로 이름 변경, 개념 확장 |
| **`execution_date`** | deprecated 이지만 동작 | **제거됨.** `logical_date` 로 통일 |
| **SubDAG** | deprecated | **제거됨.** `TaskGroup` 사용 |
| **UI** | Flask 기반 | React 로 재작성 |
| **백필** | CLI 중심 | 스케줄러가 다루는 1급 기능 |

**코드로 보이는 차이**

```python
# Airflow 2.x
from airflow.datasets import Dataset
orders = Dataset("bigquery://project/raw/orders")

with DAG(schedule=[orders], ...):
    ...

# Airflow 3.x — Asset 으로 이름이 바뀌었다
from airflow.sdk import Asset
orders = Asset("bigquery://project/raw/orders")

with DAG(schedule=[orders], ...):
    ...
```

```python
# 2.x 에서 아직 쓰이던 것 — 3.x 에서 제거됨
context["execution_date"]        # 3.x 에서 제거
context["logical_date"]          # 이쪽으로 통일
```

**가장 실질적인 변화는 두 가짐.**

- **워커가 메타DB에 붙지 않음**
  - 보안 경계가 명확해지고, 원격·다언어 워커가 가능해짐
  - 2.x 에서는 태스크 코드가 메타DB에 접근할 수 있다는 것 자체가 위험 요소였음
- **DAG 버전 관리**
  - 2.x 에서는 DAG 을 고치면 **과거 run 의 Grid 화면도 새 그래프로 그려져** "그때 실제로 무엇이 돌았는가"를 확인할 수 없었음

> **"Airflow 를 쓴다"는 말만으로는 정보가 부족함.** 2.x 인지 3.x 인지에 따라
> 워커의 DB 접근 여부, 과거 run 의 추적 가능성, 스케줄링 방식이 달라짐.

---

[← 실행 순서와 실패 처리](03-execution-failure.md) | [목차](README.md) | [dbt 와의 역할 분담 →](05-dbt-integration.md)
