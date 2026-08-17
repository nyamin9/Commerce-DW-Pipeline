# dbt 와의 역할 분담

> Airflow  ·  [← Dynamic Mapping · 환경 격리 · 테스트 · Airflow 3](04-advanced.md) | [목차](README.md) | [실전 운영 →](06-operations.md)

오케스트레이터와 변환 도구가 무엇을 나눠 갖는지, dbt 모델을 태스크로 쪼개면 안 되는 이유, 실제 호출 패턴을 다룸.

## 5-1. 오케스트레이터와 변환 도구의 경계

### 5-1-1. 정의

| | **Airflow** | **dbt** |
|---|---|---|
| 담당 | **언제 · 어떤 순서로 · 실패하면 어떻게** | **무엇을 어떻게 변환할지** |
| 관리하는 의존성 | 시스템 간 의존 (추출 → 변환 → 알림) | 모델 간 의존 (`ref`) |
| 실패 처리 | 재시도, 알림, SLA | 테스트 실패 시 downstream 차단 |
| 다루는 것 | 프로세스 | SQL |

- **경계는 명확함**
  - "원천 적재가 끝났는가"는 Airflow의 문제이고, "stg_orders 다음에 int_order_items가 와야 한다"는 dbt의 문제임

### 5-1-2. 용어 — 배치와 빌드

- 같은 일을 가리키는 것 같지만 **출신이 다른 단어**이고, 범위도 다름

| | **배치(batch)** | **빌드(build)** |
|---|---|---|
| 용어의 출처 | 데이터 처리 — 실시간의 반대말 | 소프트웨어 개발 — **dbt가 빌려온 말** |
| 뜻 | 정해진 시각에 **한 번에 몰아서 처리** | 코드로부터 **결과물을 만드는 행위** |
| 범위 | 추출 → 적재 → 변환 → 알림 **전체** | **변환만** |
| 주체 | 스케줄러(Airflow) | dbt |

```
[일 배치]  새벽 3시 실행
  1. 원천 추출
  2. DW 적재
  3. dbt 빌드   ← 배치 안의 한 단계
  4. 알림
```

- **빌드는 배치가 아닐 때도 일어남**
  - 개발자가 로컬에서 `dbt build`를 돌릴 때, PR을 올려 CI가 검증할 때도 빌드임
  - 이때는 스케줄과 무관함
- **그래서 "dbt test는 배치마다 돈다"가 아니라 "빌드마다 돈다"가 정확함**

## 왜 dbt 내부 의존성을 Airflow로 쪼개면 안 되는가

- **실무에서 자주 갈리는 판단임**
  - 쪼개면 안 되는 이유가 네 가지 있음

- **의존성이 두 곳에 중복 정의됨**
  - 모델을 하나 추가할 때마다 dbt의 `ref`와 Airflow DAG을 **둘 다** 고쳐야 함
  - 한쪽만 고치면 순서가 틀어지고, 그 사고는 조용히 일어남

- **dbt가 이미 정확한 그래프를 갖고 있음**
  - `ref()`에서 자동으로 나온 것을 사람이 손으로 다시 그리는 것은 정보를 늘리는 게 아니라 **틀릴 기회를 늘리는 것**임

- **dbt의 실행 최적화를 잃음**
  - `dbt run`은 그래프를 보고 독립적인 모델을 스레드로 병렬 실행함
  - 태스크로 쪼개면 그 판단을 Airflow가 하게 되는데, Airflow는 모델 간 관계를 dbt만큼 알지 못함

- **오버헤드가 붙음**
  - 태스크마다 dbt 프로세스가 기동되고 프로젝트를 파싱함
  - 모델 300개면 300번임

**단, 쪼개는 게 맞는 경우도 있음.**
- **Cosmos처럼 자동 생성**이면 1번(중복 정의) 문제가 없음
  - 도메인별로 **SLA나 스케줄이 다를 때**는 나누는 게 맞음
  - 실패 시 **모델 단위 재실행**이 운영상 꼭 필요할 때


## 5-2. Airflow에서 dbt 실행하기

### 5-2-1. 정의

| 패턴 | 방식 | 장점 | 단점 |
|---|---|---|---|
| **BashOperator** | `dbt run --select tag:daily` 한 방 | 단순. dbt가 병렬 실행 최적화 | Airflow UI에서 태스크 1개로만 보임. 실패 지점 파악이 어려움 |
| **astronomer-cosmos** | dbt 프로젝트를 파싱해 **모델마다 Airflow 태스크 생성** | 모델 단위 관측성·재시도 | 태스크 수 증가, 오버헤드 |
| **KubernetesPodOperator** | 격리된 파드에서 실행 | 의존성 격리, 확장성 | 인프라 복잡도 |
| **dbt Cloud API** | API로 job 트리거 | 운영 부담 적음 | 유료, 종속성 |

### 5-2-2. PythonOperator 로 다 만들지 않기

- **처음 만들 때 흔한 패턴** — 파이썬으로 다 짜고 `PythonOperator` 로 감쌈
  - 동작은 함
  - 그런데 같은 일을 하는 **전용 Operator 가 대개 이미 있음**

| 하려는 일 | 전용 Operator |
|---|---|
| API 호출 | `HttpOperator` |
| SQL 실행 | `SQLExecuteQueryOperator` |
| GCS → BigQuery 적재 | `GCSToBigQueryOperator` |
| 로컬 → GCS 업로드 | `LocalFilesystemToGCSOperator` |
| S3 → MySQL 이관 | `S3ToMySqlOperator` |
| BigQuery 잡 실행 | `BigQueryInsertJobOperator` |
| Slack 알림 | `SlackAPIOperator` |
| 이메일 발송 | `EmailOperator` |

**전용 Operator 를 쓰면 얻는 것**

- **Connection 을 통해 자격증명이 코드에서 분리됨**
  - 직접 짠 파이썬은 키를 어딘가에 두게 됨
  - 재시도·타임아웃·로깅이 Operator 안에 이미 구현돼 있음
  - `template_fields` 가 정의돼 있어 Jinja 가 적용됨
  - UI 에서 무슨 일을 하는 태스크인지 이름만으로 드러남

- **`XToYOperator` 라는 이름 규칙**이 있음 — `GCSToBigQueryOperator`, `MySQLToGCSOperator` 처럼 **A에서 B로 옮기는 것**은 대개 만들어져 있음
  - 직접 짜기 전에 provider 목록을 먼저 뒤짐

> **그래도 `PythonOperator` / `@task` 가 맞는 경우가 있음.**
> 비즈니스 로직이 들어가거나, 여러 시스템을 조합하거나, 전용 Operator 가 없는 경우임.
> 기준은 **"이게 이 파이프라인 고유의 로직인가, 아니면 흔한 이동·실행인가"** 임.

- **로직이 길어지면 DAG 파일에서 빼냄**
  - `plugins/` 나 별도 패키지에 함수·클래스로 두고 DAG 은 호출만 함
  - DAG 파일은 스케줄러가 반복 파싱하므로 가벼울수록 좋음([4-3](04-advanced.md) 참조)

**계층 단위로 나눈 형태 — 가장 흔한 구성**

```python
DBT_BIN = os.environ["DBT_BIN"]              # dbt 는 별도 venv 에 있다
DBT_DIR = os.environ["DBT_PROJECT_DIR"]
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

def dbt_command(subcommand: str, extra: str = "") -> str:
    # run_date 를 vars 로 넘기는 것이 핵심이다.
    # 모델 안에서 current_date() 를 부르면 백필이 조용히 망가진다.
    return (
        f"cd {DBT_DIR} && {DBT_BIN} {subcommand} "
        f"--target {DBT_TARGET} "
        f"--vars '{{\"run_date\": \"{{{{ ds }}}}\"}}' {extra}"
    ).strip()

dbt_staging = BashOperator(
    task_id="dbt_build_staging",
    bash_command=dbt_command("build", "--select staging"),
)
dbt_marts = BashOperator(
    task_id="dbt_build_marts_core",
    bash_command=dbt_command("build", "--select marts.core"),
)

dbt_staging >> dbt_marts
```

- **`dbt run` 이 아니라 `dbt build`** 를 부름
  - 모델을 만든 직후 테스트를 돌려 실패하면 downstream를 막는다([dbt 1-4](../dbt/README.md) 참조)
  - `--select` 로 계층을 지정함
  - 모델 하나하나로 쪼개지 않는 이유는 3번에 있음
- **Jinja 중괄호가 겹침**
  - 파이썬 f-string 안에서 `{{ ds }}` 를 그대로 남기려면 `{{{{ ds }}}}` 로 써야 함
  - 자주 틀리는 지점임

**Cosmos 를 쓰면 모델 단위로 자동 생성됨**

```python
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

dbt_tg = DbtTaskGroup(
    group_id="dbt",
    project_config=ProjectConfig(DBT_DIR),
    profile_config=ProfileConfig(profile_name="thelook_dw", target_name="prod", ...),
)
```

- dbt 프로젝트를 파싱해 **모델마다 Airflow 태스크를 만듦**
- 의존성을 손으로 다시 그리지 않으므로 중복 정의 문제가 없음
- 대신 태스크 수와 파싱 오버헤드를 받음

- **실무에서 가장 흔한 선택은 BashOperator + 태그 기반 분할**임
  - 도메인이나 주기별로 몇 개 태스크로 나누되, 모델 하나하나로는 쪼개지 않음

---

[← Dynamic Mapping · 환경 격리 · 테스트 · Airflow 3](04-advanced.md) | [목차](README.md) | [실전 운영 →](06-operations.md)
