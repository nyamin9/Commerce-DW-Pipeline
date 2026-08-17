#!/usr/bin/env python3
"""Airflow 웹 UI 를 gunicorn 없이 단일 프로세스로 띄운다.

**이 환경에서는 `airflow webserver` 와 `airflow standalone` 을 쓸 수 없다.**
gunicorn 마스터가 fork 전에 provider 를 전부 로드해두는데(`airflow/www/gunicorn_config.py`
의 `on_starting`), 그렇게 초기화된 네이티브 상태가 fork 된 워커에서 깨져
전 워커가 기동 직후 SIGSEGV 로 죽는다.

werkzeug 개발 서버는 fork 를 하지 않으므로 그 경로를 통째로 피한다.
잃는 것은 gunicorn 멀티워커뿐이고, 로컬에서는 필요하지 않다.

**우회법을 다시 시도하지 말 것.** no_proxy / OBJC_DISABLE_INITIALIZE_FORK_SAFETY /
GRPC_ENABLE_FORK_SUPPORT / setproctitle 스텁 모두 실패했다.
    → docs/incidents/2026-08-17-macos-fork-unsafe-os-log.md

사용:
    source scripts/airflow_env.sh     # 먼저. AIRFLOW_HOME 이 이 레포를 가리켜야 한다
    python scripts/airflow_ui.py
    python scripts/airflow_ui.py --port 8081
"""

from __future__ import annotations

import argparse
import os
import sys


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Airflow 웹 UI (단일 프로세스)")
    p.add_argument("--host", default="127.0.0.1",
                   help="바인드 주소. 기본은 로컬 전용")
    p.add_argument("--port", type=int, default=8080, help="포트 (기본 8080)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # airflow_env.sh 를 안 부르면 .zshrc 나 기본값의 AIRFLOW_HOME 을 보게 된다.
    # 그러면 UI 는 뜨지만 **다른 메타DB** 를 보여준다. 에러가 안 나서 더 위험하다.
    airflow_home = os.environ.get("AIRFLOW_HOME")
    if not airflow_home:
        print("AIRFLOW_HOME 이 설정되지 않았다. 먼저:", file=sys.stderr)
        print("    source scripts/airflow_env.sh", file=sys.stderr)
        return 1

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    expected = os.path.join(repo_root, "airflow_home")
    if os.path.abspath(airflow_home) != expected:
        print(f"[경고] AIRFLOW_HOME 이 이 레포를 가리키지 않는다", file=sys.stderr)
        print(f"  현재  : {airflow_home}", file=sys.stderr)
        print(f"  기대  : {expected}", file=sys.stderr)
        print(f"  다른 메타DB 를 보게 된다. source scripts/airflow_env.sh 확인할 것", file=sys.stderr)

    from airflow.www.app import create_app
    from werkzeug.serving import run_simple

    print(f"AIRFLOW_HOME  {airflow_home}")
    print(f"UI            http://{args.host}:{args.port}")

    app = create_app(testing=False)
    run_simple(
        args.host,
        args.port,
        app,
        threaded=True,
        # 리로더는 프로세스를 하나 더 띄운다. 여기서는 얻을 게 없고 혼란만 만든다.
        use_reloader=False,
        use_debugger=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
