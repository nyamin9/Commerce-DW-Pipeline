# fork 된 자식이 os_log 에서 멈추거나 죽는다

> 2026-08-17  ·  [← 장애 기록 목록](README.md)

macOS 의 `os_log`(libsystem_trace)는 fork 안전하지 않다. Airflow 는 웹서버와 태스크 실행
양쪽에서 fork 를 쓰기 때문에 **증상이 두 가지로 나뉘어 나타났고, 한동안 별개 문제로 보였다.**

| | |
|---|---|
| 환경 | macOS (Darwin 25.5) · Python 3.11.8 (pyenv) · Airflow 2.10.5 |
| 증상 A | gunicorn 워커 전원 SIGSEGV → `airflow webserver` · `standalone` 사용 불가 |
| 증상 B | 태스크가 `running` 인 채로 무한 스핀 → **일 배치가 영원히 안 끝남** |
| 상태 | B 는 `OS_ACTIVITY_MODE=disable` 로 해결. A 는 대체 기동 수단으로 우회 |

## 1. 현상

### 증상 A — 웹서버 워커가 죽는다

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
  libsystem_trace.dylib     _os_log_preferences_refresh     ← 주목
  CoreFoundation            _CFBundleLoadExecutableAndReturnError
  _setproctitle...darwin.so darwin_set_process_title
```

### 증상 B — 태스크가 끝나지 않는다

스케줄러가 태스크를 시작하지만 영원히 `running` 에 머문다. 26분을 기다려도 그대로였다.

- 로그가 `Job N: Subtask <task_id>` 에서 멈추고 더 안 찍힌다
- 프로세스 상태가 `S`(대기)가 아니라 **`R`(CPU 소모 중)** — 네트워크 대기가 아니라 스핀이다
- `airflow tasks test` 로는 **잘 된다.** 그건 fork 없이 같은 프로세스에서 실행하기 때문이다

`sample <pid>` 로 뜬 스택 (표본 100%):

```
pysqlite_connection_init → openDatabase (libsqlite3)
  → os_signpost_id_make_with_pointer → os_signpost_enabled
    → _os_log_preferences_refresh                          ← 증상 A 와 같은 함수
```

## 2. 원인

**두 증상의 끝점이 같은 함수(`_os_log_preferences_refresh`)다.** 호출 경로만 다르다.

```
setproctitle → CoreFoundation ─┐
sqlite3 openDatabase          ─┼→ os_signpost / os_log_type_enabled
google provider 로드          ─┘   └→ _os_log_preferences_refresh   ← 여기서 멈추거나 죽음
```

즉 범인은 setproctitle 도 sqlite 도 CoreFoundation 도 아니고, **fork 된 자식에서 os_log 를
건드리는 것 자체**다. 무엇을 통해 들어가느냐만 달랐다.

### 2-1. 증상 A — 웹서버

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

### 2-2. 증상 B — 태스크 실행

Airflow 는 태스크를 **두 번** 프로세스로 분리한다.

| 단계 | 방식 | 안전한가 |
|---|---|---|
| Executor → `airflow tasks run` | `subprocess.check_call` = fork + **exec** | 안전. 새 이미지로 갈아탐 |
| `StandardTaskRunner._start_by_fork()` | **fork 만** | **여기서 멈춘다** |

두 번째 fork 의 자식이 sqlite 커넥션을 열거나 프로세스 이름을 바꾸면 os_log 로 들어가 스핀한다.

**설정으로 이 fork 를 끌 수 없다.** `standard_task_runner.py` 는 `CAN_FORK` 를 직접 보는데,

```python
CAN_FORK = hasattr(os, "fork")          # airflow/settings.py — macOS 에서 항상 True
if CAN_FORK and not self.run_as_user:
    self.process = self._start_by_fork()
```

`core.execute_tasks_new_python_interpreter` 설정은 이름과 달리 **`LocalExecutor` 에서만** 쓰이고
`StandardTaskRunner` 에는 영향을 주지 않는다.

## 3. 조치사항

### 3-1. 해결 — `OS_ACTIVITY_MODE=disable`

fork 를 못 끄므로 **로깅 쪽을 끈다.** [`scripts/airflow_env.sh`](../../scripts/airflow_env.sh) 가 export 한다.

```bash
export OS_ACTIVITY_MODE=disable
```

이 한 줄로 증상 B 가 사라졌다. 적용 전후:

| | 적용 전 | 적용 후 |
|---|---|---|
| `create_raw_dataset` | 26분+ 무한 스핀 | **1.6초** |
| EL 7 태스크 | 도달 못 함 | 전부 성공 (각 5~10초) |
| `dbt_snapshot` | 도달 못 함 | 성공 (27초) |

**증상 A(웹서버)에는 효과가 없다.** 그쪽은 여전히 `scripts/airflow_ui.py` 로 우회한다.

### 3-2. 시도했고 실패한 것 — 다시 하지 말 것

| 시도 | 결과 |
|---|---|
| `no_proxy="*"` + `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` | 워커 전원 SIGSEGV |
| 위 + `GRPC_ENABLE_FORK_SUPPORT=1` + `GRPC_POLL_STRATEGY=poll` + `--workers 1` | SIGSEGV 3,069회 |
| `setproctitle` 을 no-op 스텁으로 교체 | SIGSEGV 30회 |

세 번째가 **크래시 스택에 보이는 `setproctitle` 이 원인이 아님**을 보여준다.
provider 를 빼는 것도 답이 아니다. google 을 빼면 EL 이 사라지고, 빼더라도 나머지 9개가 그대로 로드된다.

### 3-3. 웹 UI 대체 기동 수단

[`scripts/airflow_ui.py`](../../scripts/airflow_ui.py) — werkzeug 단일 프로세스로 띄운다. fork 를 하지 않으므로 문제가 발생하지 않는다.

```bash
source scripts/airflow_env.sh
airflow scheduler              # 터미널 1
python scripts/airflow_ui.py   # 터미널 2 (source 먼저)
```

- 잃는 것은 gunicorn 멀티워커뿐이다. 로컬 환경에서 워커 4개가 필요할 일은 없다
- 파이프라인 기능에는 영향이 없다

### 3-4. 배운 것

- **크래시 스택의 최상단 프레임이 원인이라는 보장은 없다.** `setproctitle` 을 없애도 그대로 죽었다
  - → 다음 프로젝트: 스택에서 범인을 지목했으면 **그것을 제거해서 증상이 사라지는지**로 검증할 것

- **fork 와 fork+exec 의 차이가 실무에서 갈린다.** 같은 머신, 같은 venv 인데 웹서버만 죽고 스케줄러는 멀쩡했다
  - → 다음 프로젝트: "왜 A 는 되는데 B 는 안 되지"는 프로세스 생성 방식부터 볼 것

- **증상이 다르다고 원인이 다른 것은 아니다.** SIGSEGV 와 무한 스핀은 정반대로 보이는 증상인데
  스택의 끝점이 같은 함수였다. 두 개를 따로 쫓느라 시간을 썼다
  - → 다음 프로젝트: 스택을 끝까지 볼 것. 최상단 프레임(setproctitle, sqlite)은 통로였고
    바닥(`_os_log_preferences_refresh`)이 범인이었다

- **`running` 인데 안 끝나면 프로세스 상태부터 본다.** `S`(대기)면 I/O 를 기다리는 것이고,
  `R`(실행 중)이면 스핀이다. 후자는 `sample <pid>` 로 어디서 도는지 바로 나온다

## 관련 문서

- **[태스크가 실행되지 않고 즉시 실패](2026-08-17-airflow-task-never-launched.md)** — 같은 날 별개 원인의 장애
- **[airflow/README.md](../../airflow/README.md)** — 기동 절차

---

[← 장애 기록 목록](README.md)
