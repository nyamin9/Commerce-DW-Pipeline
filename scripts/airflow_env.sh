#!/usr/bin/env bash
# Airflow 실행 환경을 이 레포에 고정한다.
#
#   source scripts/airflow_env.sh
#   airflow scheduler
#
# **반드시 source 로 부른다.** 실행하면 환경변수가 현재 셸에 남지 않는다.
#
# 이 스크립트가 존재하는 이유는 PATH 때문이다. Airflow 는 태스크를
#   ["airflow", "tasks", "run", ...]
# 라는 **맨 이름 명령**으로 띄우고, 그걸 PATH 로 찾는다. venv 를 절대경로로
# 기동하면 venv/bin 이 PATH 에 없어서 태스크가 기동조차 못 하고 즉시 실패한다.
# 로그 파일도 안 남아서 원인 파악이 어렵다.
#   → docs/incidents/2026-08-17-airflow-task-never-launched.md

# ── source 로 불렸는지 확인 ──────────────────────────────────────────────
if [ -n "${ZSH_VERSION:-}" ]; then
    _thelook_src="$(eval 'echo ${(%):-%x}')"
    case "${ZSH_EVAL_CONTEXT:-}" in
        *:file*) _thelook_sourced=1 ;;
        *)       _thelook_sourced=0 ;;
    esac
else
    _thelook_src="${BASH_SOURCE[0]:-$0}"
    if [ "${BASH_SOURCE[0]:-}" != "${0}" ]; then
        _thelook_sourced=1
    else
        _thelook_sourced=0
    fi
fi

if [ "${_thelook_sourced}" -ne 1 ]; then
    echo "이 스크립트는 source 로 불러야 한다:" >&2
    echo "    source scripts/airflow_env.sh" >&2
    exit 1
fi

# ── 경로 ────────────────────────────────────────────────────────────────
THELOOK_REPO="$(cd "$(dirname "${_thelook_src}")/.." && pwd)"
unset _thelook_src _thelook_sourced

export AIRFLOW_HOME="${THELOOK_REPO}/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="${THELOOK_REPO}/airflow/dags"
export THELOOK_DBT_PROJECT_DIR="${THELOOK_REPO}/dbt"

# venv 위치. 레포 안에 없으면 THELOOK_AIRFLOW_VENV 로 지정한다.
THELOOK_AIRFLOW_VENV="${THELOOK_AIRFLOW_VENV:-${THELOOK_REPO}/.venv-airflow}"

# ── 검증 ────────────────────────────────────────────────────────────────
_thelook_fail() { echo "airflow_env: $*" >&2; return 1; }

if [ ! -x "${THELOOK_AIRFLOW_VENV}/bin/airflow" ]; then
    echo "airflow_env: Airflow venv 를 찾지 못했다: ${THELOOK_AIRFLOW_VENV}" >&2
    echo "  기존 venv 를 쓰려면:" >&2
    echo "    export THELOOK_AIRFLOW_VENV=/path/to/.venv-airflow" >&2
    echo "  새로 만들려면 airflow/README.md 4번 「설치」 참조" >&2
    return 1
fi

# **PATH 앞에 붙인다.** 이 한 줄이 이 스크립트의 존재 이유다.
case ":${PATH}:" in
    *":${THELOOK_AIRFLOW_VENV}/bin:"*) ;;
    *) PATH="${THELOOK_AIRFLOW_VENV}/bin:${PATH}" ;;
esac
export PATH

# airflow 가 정말 venv 안에서 해석되는지 확인한다.
# pyenv shim 처럼 PATH 앞자리를 차지하는 것이 있으면 여기서 걸린다.
_thelook_airflow="$(command -v airflow 2>/dev/null)"
if [ -z "${_thelook_airflow}" ]; then
    unset _thelook_airflow
    _thelook_fail "PATH 에서 airflow 를 찾지 못했다"
    return 1
fi
if [ "${_thelook_airflow}" != "${THELOOK_AIRFLOW_VENV}/bin/airflow" ]; then
    echo "airflow_env: airflow 가 venv 밖에서 잡힌다" >&2
    echo "  잡힌 것 : ${_thelook_airflow}" >&2
    echo "  기대한 것: ${THELOOK_AIRFLOW_VENV}/bin/airflow" >&2
    echo "  이 상태로 스케줄러를 띄우면 태스크가 기동조차 못 하고 실패한다." >&2
    echo "  → docs/incidents/2026-08-17-airflow-task-never-launched.md" >&2
    unset _thelook_airflow
    return 1
fi
unset _thelook_airflow

if [ ! -d "${AIRFLOW__CORE__DAGS_FOLDER}" ]; then
    _thelook_fail "DAG 폴더가 없다: ${AIRFLOW__CORE__DAGS_FOLDER}"
    return 1
fi

# DAG 이 모듈 최상단에서 읽는다. 없으면 파싱 단계에서 깨진다.
if [ -z "${THELOOK_GCP_PROJECT:-}" ]; then
    echo "airflow_env: THELOOK_GCP_PROJECT 가 설정되지 않았다" >&2
    echo "  DAG 이 임포트 시점에 읽으므로 없으면 DAG 자체가 뜨지 않는다." >&2
    echo "    export THELOOK_GCP_PROJECT=<your-gcp-project>" >&2
    return 1
fi

# 없으면 DAG 이 'dbt' 로 폴백하고, PATH 에 dbt-fusion 이 있으면 그쪽이 잡힌다.
if [ -z "${THELOOK_DBT_BIN:-}" ]; then
    echo "airflow_env: [경고] THELOOK_DBT_BIN 미설정 — dbt 태스크가 PATH 의 dbt 를 쓴다" >&2
    echo "  dbt-core 1.8 이 아닌 것이 잡히면 dbt 태스크가 전부 실패한다." >&2
    echo "    export THELOOK_DBT_BIN=/path/to/dbt-core-1.8/bin/dbt" >&2
fi

unset -f _thelook_fail

echo "airflow_env: 이 레포를 서빙하도록 설정됨"
echo "  AIRFLOW_HOME  ${AIRFLOW_HOME}"
echo "  DAGS_FOLDER   ${AIRFLOW__CORE__DAGS_FOLDER}"
echo "  airflow       $(command -v airflow)"
echo "  GCP_PROJECT   ${THELOOK_GCP_PROJECT}"
