# Airflow 시스템 구성 요소

> Airflow  ·  [목차](README.md) | [DAG 작성과 스케줄링 →](02-dag-scheduling.md)

Scheduler · DAG Processor · Worker · Triggerer · 메타DB 가 각각 무슨 일을 하는지, 배포 형태는 어떤 선택지가 있는지, 장애가 나면 어디부터 보는지를 다룸.

## 1-1. Airflow 시스템 구성 요소

> **"Airflow 를 쓴다"는 것은 여러 프로세스가 함께 도는 시스템을 운영한다는 뜻임.**
> DAG 을 쓰기 전에 무엇이 그 DAG 을 읽고 실행하는지부터 알아야 하고,
> 장애가 나면 어느 컴포넌트가 문제인지 가릴 수 있어야 함.

### 1-1-1. 정의

| 컴포넌트 | 하는 일 | 없으면 |
|---|---|---|
| **Metadata DB** | DAG·run·태스크 상태·Connection·Variable·XCom 저장. **시스템의 유일한 진실** | 아무것도 안 됨 |
| **Scheduler** | DAG 을 보고 **실행할 태스크를 판단해 큐에 넣음** | 자동 실행이 안 됨 |
| **DAG Processor** | `dags/` 를 주기적으로 스캔·파싱해 메타DB에 반영 | 새 DAG·수정이 반영 안 됨 |
| **Worker** | 태스크를 **실제로 실행** | 큐에 쌓이기만 함 |
| **Triggerer** | `deferrable` 태스크의 비동기 대기를 전담 | deferred 태스크가 안 깨어남 |
| **Webserver / API Server** | UI 와 REST API | 실행은 되지만 볼 수 없음 |
| **Broker** (Redis 등) | Celery 사용 시 큐 저장소 | Celery 구성에서만 필요 |

```
        dags/ ──파싱──▶ DAG Processor ──▶ ┌──────────────┐
                                          │  Metadata DB │◀── Webserver (UI/API)
        Scheduler ──실행 판단──▶ 큐 ──────▶ └──────────────┘
                                               ▲
        Worker ──태스크 실행·상태 기록──────────┘
        Triggerer ──비동기 대기 완료 신호───────┘
```

- **DAG Processor 가 별도 프로세스로 분리된 것이 비교적 최근 변화임**
  - 예전에는 스케줄러가 파싱까지 했고, 그래서 무거운 DAG 파일이 스케줄링 전체를 느리게 만들었음
- **Webserver 가 죽어도 파이프라인은 돎**
  - UI 가 안 보일 뿐임
  - 반대로 스케줄러가 죽으면 아무것도 시작되지 않음

**장애를 가릴 때 쓰는 구분**

| 증상 | 의심할 곳 |
|---|---|
| 새로 넣은 DAG 이 UI 에 안 보임 | DAG Processor · 파싱 에러 |
| DAG 은 보이는데 run 이 안 생김 | Scheduler · `catchup`·`start_date`·일시정지 상태 |
| 태스크가 `queued` 에서 안 넘어감 | Worker 부족 · `pool` 소진 · 동시성 상한 |
| Sensor 만 잔뜩 떠서 다른 게 안 돎 | `poke` 모드 슬롯 고갈 ([3-4](03-execution-failure.md)) |
| UI 만 안 뜸 | Webserver |

### 1-1-2. 배포 형태

| 형태 | 특징 |
|---|---|
| **로컬 (pip + LocalExecutor)** | 학습·단일 개발자용. SQLite 메타DB는 동시성이 없어 실습에만 |
| **Docker Compose** | 공식 `docker-compose.yaml` 로 전체 스택을 띄움. **로컬 실습의 표준** |
| **Kubernetes (Helm)** | 운영 표준. 태스크마다 파드 격리 |
| **관리형** | **AWS MWAA**, **GCP Cloud Composer**, Astronomer |

- **관리형을 고려하는 기준**은 규모가 아니라 **운영 인력**임
  - 스케줄러·워커·메타DB를 직접 굴리면 업그레이드·백업·확장이 전부 우리 일이 됨
  - 데이터 처리 자체가 클라우드에서 일어나고 있다면 오케스트레이터만 자체 운영할 이유가 크지 않음

> **Docker 로 띄우면 실행 환경이 컨테이너 안임.** 코드를 편집하는 곳은 내 컴퓨터지만
> **DAG 이 실제로 도는 파이썬 환경은 컨테이너 내부**임. 로컬에 설치한 패키지는
> 컨테이너에 없음. `dags/` · `logs/` · `plugins/` · `config/` 가 볼륨으로 연결돼
> 파일만 공유될 뿐임. **"로컬에서 되는데 Airflow 에서 안 된다"의 대부분이 여기서 나옴.**

**Docker Compose 로 띄울 때 핵심 부분**

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:3.0.4
  environment:
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags        # 작업 컴퓨터 : 컨테이너 내부
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config

services:
  postgres: {image: postgres:15, ...}      # 메타DB
  redis:    {image: redis:7, ...}          # Celery 브로커
  airflow-scheduler:  {<<: *airflow-common, command: scheduler}
  airflow-dag-processor: {<<: *airflow-common, command: dag-processor}
  airflow-worker:     {<<: *airflow-common, command: celery worker}
  airflow-triggerer:  {<<: *airflow-common, command: triggerer}
  airflow-apiserver:  {<<: *airflow-common, command: api-server, ports: ["8080:8080"]}
```

```bash
docker compose up airflow-init      # 메타DB 초기화 · 관리자 계정 생성
docker compose up -d
docker ps                           # 전부 healthy 가 될 때까지 확인
```

### 1-1-3. 관측성 — UI 밖에서 보는 법

- **Airflow UI 는 커스터마이징이 거의 안 됨**
  - 필요한 지표가 UI 에 없으면 두 방향임

- **메타DB를 직접 조회함**
  - `dag_run` · `task_instance` 테이블에 실행 시각·상태·소요 시간이 전부 있음
  - "요즘 어느 태스크가 느려지고 있나" 같은 건 SQL 로 뽑는 게 빠름
- **로그를 중앙화함**
  - 워커가 늘어나면 로그가 흩어지므로 S3·GCS 같은 객체 저장소나 Elasticsearch 로 보내 한 곳에서 검색되게 함
  - `remote_logging` 설정으로 처리함

```sql
-- 최근 2주, 태스크별 평균 소요 시간과 실패율
select
    ti.task_id,
    count(*)                                                    as runs,
    countif(ti.state = 'failed')                                as failures,
    round(avg(timestamp_diff(ti.end_date, ti.start_date, second)), 1) as avg_sec
from task_instance ti
where ti.dag_id = 'thelook_dw_daily'
  and ti.start_date >= current_date - 14
group by ti.task_id
order by avg_sec desc
```

- **알림은 콜백과 Operator 양쪽으로 낼 수 있음**
  - `on_failure_callback` 으로 직접 보내거나, `SlackAPIOperator` 같은 전용 Operator 를 실패 경로에 붙인다(3-3 의 분기 참조)

---

[목차](README.md) | [DAG 작성과 스케줄링 →](02-dag-scheduling.md)
