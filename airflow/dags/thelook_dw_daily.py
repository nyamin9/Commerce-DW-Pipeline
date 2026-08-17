"""thelook DW 일 배치.

EL(Airflow) → 변환(dbt) 순으로 돈다.

역할 분담:
    Airflow — 언제 · 어떤 순서로 · 실패하면 어떻게
    dbt     — 무엇을 어떻게 변환할지

**dbt 모델 하나하나를 Airflow 태스크로 쪼개지 않는다.** 이유는 네 가지다.
    1. 의존성이 dbt 의 ref() 와 DAG 양쪽에 중복 정의된다. 한쪽만 고치면 조용히 어긋난다
    2. dbt 가 이미 정확한 그래프를 갖고 있다. 손으로 다시 그리는 건 틀릴 기회를 늘리는 것
    3. dbt 의 병렬 실행 최적화를 잃는다
    4. 태스크마다 dbt 프로세스가 기동되고 프로젝트를 파싱한다

대신 **계층 단위로만** 나눈다. 계층은 실패 시 재시작 지점이자
"어디까지 신뢰할 수 있는가"의 경계라서, 그 입도에서는 나누는 값이 있다.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup

from thelook.config import LOOKBACK_DAYS, RAW_DATASET, TABLES
from thelook.el import build_el_sql

# ── 환경 ────────────────────────────────────────────────────────────────
# dbt 와 Airflow 는 **서로 다른 가상환경**에 있다. 의존성이 충돌하기 때문이고
# 실무에서도 권장되는 분리다. BashOperator 로 그 경계를 넘는다.
DBT_BIN = os.environ.get("THELOOK_DBT_BIN", "dbt")
DBT_PROJECT_DIR = os.environ.get(
    "THELOOK_DBT_PROJECT_DIR",
    str(Path(__file__).resolve().parents[2] / "dbt"),
)
DBT_TARGET = os.environ.get("THELOOK_DBT_TARGET", "dev")
GCP_PROJECT = os.environ["THELOOK_GCP_PROJECT"]
GCP_CONN_ID = os.environ.get("THELOOK_GCP_CONN_ID", "google_cloud_default")
BQ_LOCATION = "US"

# 처리 기준일. **`{{ ds }}` 를 쓰고 `datetime.now()` 는 쓰지 않는다.**
# now() 를 쓰면 3개월 전 구간을 백필해도 오늘 날짜로 계산해 백필이 조용히 망가진다.
#
# lookback 값은 thelook.config 에서 온다. Airflow Variable 로 빼지 않는 이유는
# **dbt 의 vars.lookback_days 와 반드시 같은 값이어야 하기 때문이다.**
# 두 곳에서 따로 설정하게 두면 언젠가 한쪽만 바뀌고, EL 이 채운 구간과
# dbt 가 다시 만드는 구간이 어긋난다. 그 사고는 조용히 일어난다.
RUN_DATE = "{{ ds }}"
LOOKBACK_START = f"{{{{ macros.ds_add(ds, -{LOOKBACK_DAYS}) }}}}"


def notify_failure(context) -> None:
    """실패 알림.

    Slack 웹훅이 설정돼 있으면 보내고, 없으면 로그만 남긴다.
    운영에서는 여기서 심각도를 나눠 채널을 달리 가져간다 —
    파이프라인 중단과 테스트 경고는 대응 속도가 다르기 때문이다.
    """
    task_instance = context["task_instance"]
    message = (
        f"[thelook_dw_daily] 실패\n"
        f"  task: {task_instance.task_id}\n"
        f"  logical_date: {context['logical_date']}\n"
        f"  try: {task_instance.try_number}"
    )

    webhook = os.environ.get("THELOOK_SLACK_WEBHOOK")
    if not webhook:
        print(message)
        return

    import json
    import urllib.request

    request = urllib.request.Request(
        webhook,
        data=json.dumps({"text": message}).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request, timeout=10)


default_args = {
    "owner": "analytics-engineering",
    # 재시도로 풀리는 실패와 아닌 실패는 다르다.
    # 네트워크·일시적 리소스 부족은 재시도가 맞지만
    # **데이터가 틀린 경우는 100번 돌려도 똑같이 틀린다.** 오히려 발견을 늦춘다.
    # 그래서 횟수를 짧게 두고, 테스트 실패는 재시도 대상이 아니게 설계했다.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
    "depends_on_past": False,
}


def dbt_command(subcommand: str, extra: str = "") -> str:
    """dbt 명령 문자열. run_date 를 vars 로 넘기는 것이 핵심이다."""
    return (
        f"cd {DBT_PROJECT_DIR} && "
        f"{DBT_BIN} {subcommand} "
        f"--target {DBT_TARGET} "
        f"--vars '{{\"run_date\": \"{RUN_DATE}\"}}' "
        f"{extra}"
    ).strip()


with DAG(
    dag_id="thelook_dw_daily",
    description="thelook 커머스 DW — EL(Airflow) + 변환(dbt)",
    start_date=datetime(2026, 6, 16),
    schedule="0 3 * * *",
    # 켜는 순간 수백 개 run 이 한꺼번에 뜨는 것을 막는다. 백필은 필요할 때 수동으로.
    catchup=False,
    # 백필 폭주 방지. 같은 파티션을 두 run 이 동시에 덮으면 결과가 갈린다.
    max_active_runs=1,
    default_args=default_args,
    tags=["thelook", "dw", "dbt"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 랜딩 데이터셋 부트스트랩 ────────────────────────────────────────
    # EL 의 SQL 은 CREATE TABLE 로 **테이블**을 만들지만 **데이터셋은 만들지 않는다.**
    # 없으면 EL 7개가 전부 `404 Not found: Dataset` 으로 죽는다.
    #
    # scripts/run_el.py 는 이걸 직접 만들고 있었는데 DAG 에만 없었다.
    # 두 경로가 build_el_sql() 은 공유하면서 부트스트랩만 갈라져 있던 것이다.
    # 멱등이라 매 run 돌아도 무해하다.
    create_raw_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_raw_dataset",
        gcp_conn_id=GCP_CONN_ID,
        project_id=GCP_PROJECT,
        dataset_id=RAW_DATASET,
        location=BQ_LOCATION,
        if_exists="ignore",
        dataset_reference={
            "description": (
                "thelook EL 랜딩 영역. 불변으로 다루고 재처리는 파티션 교체로 한다"
            )
        },
        doc_md=(
            "랜딩 데이터셋을 만든다. **EL 보다 먼저 돈다** — "
            "데이터셋이 없으면 적재 SQL 이 테이블을 만들 곳이 없다."
        ),
    )

    # ── EL: 원천 → raw 랜딩 ────────────────────────────────────────────
    # dbt 는 추출도 적재도 하지 않는다. source() 가 가리키는 테이블이
    # 이미 DW 에 있어야 시작된다. 그 앞을 채우는 구간이다.
    with TaskGroup(group_id="extract_load") as extract_load:
        for spec in TABLES:
            BigQueryInsertJobOperator(
                task_id=f"load_{spec.name}",
                gcp_conn_id=GCP_CONN_ID,
                location=BQ_LOCATION,
                configuration={
                    "query": {
                        "query": build_el_sql(
                            spec,
                            project=GCP_PROJECT,
                            start_date=LOOKBACK_START,
                            end_date=RUN_DATE,
                        ),
                        "useLegacySql": False,
                    }
                },
                doc_md=f"**{spec.strategy}** — {spec.rationale}",
            )

    # ── 원천 신선도 ────────────────────────────────────────────────────
    # 빌드와 분리해 따로 실행한다. **파이프라인은 성공했는데 데이터가 안 들어온 경우**를
    # 잡는 장치라서, 변환 성공 여부와 독립적으로 판단해야 한다.
    # ── dbt 패키지 ─────────────────────────────────────────────────────
    # **매 run 마다 받지 않는다.** `dbt deps` 는 hub 를 거쳐 GitHub 에서 타르볼을
    # 내려받는데, 배치마다 부르면 두 가지가 문제가 된다.
    #   1. 네트워크가 배치의 임계 경로에 들어온다. 우리가 통제할 수 없는 실패 지점이다
    #   2. 호출이 쌓이면 GitHub 이 429 로 막는다. 실제로 겪었다
    # 게다가 `dbt deps` 는 **먼저 dbt_packages 를 비우고 받는다.** 받기에 실패하면
    # 있던 패키지까지 사라져서, 다음 태스크가 매크로를 못 찾고 전부 깨진다.
    #
    # 패키지는 packages.yml 과 package-lock.yml 로 버전이 고정돼 있어 자주 바뀌지
    # 않는다. 없을 때만 받고, 갱신이 필요하면 THELOOK_DBT_DEPS_ALWAYS=1 로 강제한다.
    #   → docs/incidents/2026-08-17-dbt-deps-github-rate-limit.md
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            'if [ -z "${THELOOK_DBT_DEPS_ALWAYS:-}" ] '
            '&& [ -f dbt_packages/dbt_utils/dbt_project.yml ]; then '
            'echo "dbt_packages 가 이미 있다 — deps 를 건너뛴다"; '
            f'else {DBT_BIN} deps; fi'
        ),
        doc_md=(
            "dbt 패키지 설치. **없을 때만 받는다** — 매 run 마다 받으면 GitHub 이 "
            "429 로 막고, 실패하면 있던 패키지까지 지워진다. "
            "강제하려면 `THELOOK_DBT_DEPS_ALWAYS=1`."
        ),
    )

    # **downstream가 없는 태스크다.** 신선도는 게이트가 아니라 관측이다.
    # 원천 지연은 우리가 고칠 수 없고, 있는 데이터로 만들어 두는 편이
    # 아무것도 없는 것보다 낫다. 그래서 이 태스크가 실패해도 변환은 진행된다.
    #
    # 예전에는 이걸 체인 중간에 두고 trigger_rule="all_done" 으로 표현했는데
    # **그건 의미가 달랐다.** trigger_rule 은 "언제 시작할지"를 정하지
    # "내 실패가 downstream를 막을지"를 정하지 않는다. 오히려 EL 이 실패해 upstream가
    # upstream_failed 가 되어도 all_done 은 통과시키므로, **원천이 안 들어온 날
    # dbt 가 그대로 도는 경로**가 열려 있었다.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=dbt_command("source freshness"),
        retries=0,
    )

    # ── 변환: 계층 단위로만 나눈다 ─────────────────────────────────────
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt_command("snapshot"),
        doc_md=(
            "products 의 SCD Type 2 이력. **staging 보다 먼저 돈다** — "
            "원천을 직접 읽고, 변환이 실패해도 그날의 상태는 남아야 하기 때문이다."
        ),
    )

    dbt_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=dbt_command("build", "--select staging"),
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_build_intermediate",
        bash_command=dbt_command("build", "--select intermediate"),
    )

    dbt_marts_core = BashOperator(
        task_id="dbt_build_marts_core",
        bash_command=dbt_command("build", "--select marts.core"),
        doc_md="표준 영역 — 도메인 사실을 재사용 가능한 형태로. 입도를 유지한다.",
    )

    dbt_marts_reporting = BashOperator(
        task_id="dbt_build_marts_reporting",
        bash_command=dbt_command("build", "--select marts.reporting"),
        doc_md="소비 영역 — 특정 목적에 맞춰 반정규화·집계한다.",
    )

    # ── 문서·리니지 ────────────────────────────────────────────────────
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=dbt_command("docs generate"),
        # 문서 생성 실패로 데이터 파이프라인을 실패로 만들지 않는다.
        retries=0,
    )

    end = EmptyOperator(task_id="end")

    # dbt_deps 는 EL 과 무관하다. 패키지 설치일 뿐이라 원천 적재를 기다릴 이유가 없어
    # 병렬로 둔다. dbt 를 부르는 첫 태스크들만 둘 다를 기다린다.
    start >> [create_raw_dataset, dbt_deps]
    create_raw_dataset >> extract_load

    # 관측 전용 — downstream가 없다
    [extract_load, dbt_deps] >> dbt_source_freshness

    # 변환 체인 — 계층 단위로만 나눈다
    (
        [extract_load, dbt_deps]
        >> dbt_snapshot
        >> dbt_staging
        >> dbt_intermediate
        >> dbt_marts_core
        >> dbt_marts_reporting
        >> dbt_docs
        >> end
    )
