# 웹서버 워커가 기동 직후 SIGSEGV

> 2026-08-17  ·  [← 장애 기록 목록](README.md)

`airflow webserver` 와 `airflow standalone` 을 이 환경에서 쓸 수 없다.

| | |
|---|---|
| 환경 | macOS (Darwin 25.5) · Python 3.11.8 (pyenv) · Airflow 2.10.5 |
| 범위 | 웹 UI 전용. **스케줄러와 태스크 실행은 정상** |
| 상태 | 근본 해결 불가. 대체 기동 수단으로 우회 |

## 1. 현상

`airflow webserver` 를 띄우면 gunicorn 워커가 전부 즉시 죽고, 120초 뒤 마스터가 스스로 종료한다.

```
[ERROR] Worker (pid:98127) was sent SIGSEGV!
[ERROR] Worker (pid:98128) was sent SIGSEGV!
...
{webserver_command.py:223} ERROR - No response from gunicorn master within 120 seconds
{webserver_command.py:224} ERROR - Shutting down webserver
```

- `airflow standalone` 도 내부에서 이 웹서버를 띄우므로 같이 실패한다
- **스케줄러와 EL 태스크는 정상 동작한다.** 같은 머신에서 DAG 전 구간이 성공한 적이 있다

macOS 크래시 리포트(`~/Library/Logs/DiagnosticReports/python3.11-*.ips`)의 스택:

```
EXC_BAD_ACCESS (SIGSEGV)
  libsystem_trace.dylib     _os_log_preferences_refresh
  CoreFoundation            _CFBundleLoadExecutableAndReturnError
  _setproctitle...darwin.so darwin_set_process_title
```

## 2. 원인

google provider 는 EL 에 필수다. 그런데 웹서버(gunicorn)는 fork 전에 provider 를 전부 로드해두고,
그렇게 초기화된 네이티브 상태가 fork 된 워커에서 깨진다. standalone 은 그 웹서버를 띄우므로 같이 불가능하다.
EL 실행 자체는 fork+exec 경로라 멀쩡하다.

**로드 순서는 Airflow 가 의도한 것이다.** `airflow/www/gunicorn_config.py`:

```python
def on_starting(server):
    providers_manager.initialize_providers_configuration()
    providers_manager.connection_form_widgets   # "Load providers before forking workers"
```

워커마다 중복 로드를 피하려는 최적화인데, macOS 에서는 역효과다.

**같은 머신에서 스케줄러가 멀쩡한 이유는 프로세스 생성 방식이 다르기 때문이다.**

| | 방식 | 결과 |
|---|---|---|
| gunicorn 워커 | fork 만 함 | 부모의 초기화된 메모리를 그대로 물려받음 → 깨짐 |
| 태스크 (`SequentialExecutor`) | fork + **exec** | 새 프로세스 이미지로 갈아탐 → 부모 상태와 무관 → 정상 |

- 크래시가 `setproctitle` 에서 났지만 그건 착지 지점이지 원인이 아니다 (3-1 참조)
- 어느 provider 가 CoreFoundation 을 초기화하는지는 격리하지 않았다. google 은 추론이다

## 3. 조치사항

### 3-1. 시도했고 실패한 것 — 다시 하지 말 것

| 시도 | 결과 |
|---|---|
| `no_proxy="*"` + `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` | 워커 전원 SIGSEGV |
| 위 + `GRPC_ENABLE_FORK_SUPPORT=1` + `GRPC_POLL_STRATEGY=poll` + `--workers 1` | SIGSEGV 3,069회 |
| `setproctitle` 을 no-op 스텁으로 교체 | SIGSEGV 30회 |

세 번째가 **크래시 스택에 보이는 `setproctitle` 이 원인이 아님**을 보여준다.
provider 를 빼는 것도 답이 아니다. google 을 빼면 EL 이 사라지고, 빼더라도 나머지 9개가 그대로 로드된다.

### 3-2. 대체 기동 수단

[`scripts/airflow_ui.py`](../../scripts/airflow_ui.py) — werkzeug 단일 프로세스로 띄운다. fork 를 하지 않으므로 문제가 발생하지 않는다.

```bash
source scripts/airflow_env.sh
airflow scheduler              # 터미널 1
python scripts/airflow_ui.py   # 터미널 2 (source 먼저)
```

- 잃는 것은 gunicorn 멀티워커뿐이다. 로컬 환경에서 워커 4개가 필요할 일은 없다
- 파이프라인 기능에는 영향이 없다

### 3-3. 배운 것

- **크래시 스택의 최상단 프레임이 원인이라는 보장은 없다.** `setproctitle` 을 없애도 그대로 죽었다
  - → 다음 프로젝트: 스택에서 범인을 지목했으면 **그것을 제거해서 증상이 사라지는지**로 검증할 것

- **fork 와 fork+exec 의 차이가 실무에서 갈린다.** 같은 머신, 같은 venv 인데 웹서버만 죽고 스케줄러는 멀쩡했다
  - → 다음 프로젝트: "왜 A 는 되는데 B 는 안 되지"는 프로세스 생성 방식부터 볼 것

## 관련 문서

- **[태스크가 실행되지 않고 즉시 실패](2026-08-17-airflow-task-never-launched.md)** — 같은 날 별개 원인의 장애
- **[airflow/README.md](../../airflow/README.md)** — 기동 절차

---

[← 장애 기록 목록](README.md)
