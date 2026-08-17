#!/usr/bin/env python3
"""EL 단계를 Airflow 없이 실행한다.

용도는 두 가지다.
  1) 최초 전체 적재 — 일 배치는 lookback 구간만 다루므로 과거는 한 번 채워야 한다
  2) 로컬에서 SQL 을 확인 — Airflow 를 띄우지 않고 같은 코드 경로를 돌린다

SQL 은 `airflow/dags/thelook/el.py` 에서 가져온다. **여기에 SQL 을 복사하지 않는다.**

사용 예:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
    python scripts/run_el.py --project my-proj --start 2019-01-01 --end 2026-08-16
    python scripts/run_el.py --project my-proj --lookback 3          # 일 배치와 동일 구간
    python scripts/run_el.py --project my-proj --dry-run             # SQL 만 출력
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

# Airflow 의 dags 폴더가 sys.path 에 올라가는 것과 같은 상태를 만든다.
DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
sys.path.insert(0, str(DAGS_DIR))

from thelook.config import LOOKBACK_DAYS, RAW_DATASET, TABLES  # noqa: E402
from thelook.el import build_el_sql  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="thelook EL 실행")
    p.add_argument("--project", required=True, help="적재 대상 GCP 프로젝트")
    p.add_argument("--start", help="구간 시작일 (YYYY-MM-DD)")
    p.add_argument("--end", help="구간 종료일 (YYYY-MM-DD)")
    p.add_argument(
        "--lookback",
        type=int,
        help=f"오늘 기준 N일 전부터 오늘까지. 미지정 시 {LOOKBACK_DAYS}일",
    )
    p.add_argument("--tables", help="쉼표로 구분한 대상 테이블. 기본은 전체")
    p.add_argument("--dry-run", action="store_true", help="실행하지 않고 SQL 만 출력")
    p.add_argument(
        "--location", default="US",
        help="BigQuery 리전. 원천이 US 멀티리전이라 기본값을 바꾸면 조인이 깨진다",
    )
    return p.parse_args()


def resolve_window(args: argparse.Namespace) -> tuple[str, str]:
    if args.start and args.end:
        return args.start, args.end
    days = args.lookback if args.lookback is not None else LOOKBACK_DAYS
    end = date.today()
    return str(end - timedelta(days=days)), str(end)


def main() -> int:
    args = parse_args()
    start, end = resolve_window(args)

    specs = list(TABLES)
    if args.tables:
        wanted = {t.strip() for t in args.tables.split(",")}
        specs = [s for s in specs if s.name in wanted]
        missing = wanted - {s.name for s in specs}
        if missing:
            print(f"알 수 없는 테이블: {', '.join(sorted(missing))}", file=sys.stderr)
            return 2

    print(f"구간 [{start}, {end}] · 대상 {len(specs)}개 테이블")

    if args.dry_run:
        for spec in specs:
            print(f"\n{'=' * 70}\n-- {spec.name} ({spec.strategy})\n{'=' * 70}")
            print(build_el_sql(spec, args.project, start, end))
        return 0

    from google.cloud import bigquery

    client = bigquery.Client(project=args.project)

    dataset_id = f"{args.project}.{RAW_DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = args.location
    dataset.description = "thelook EL 랜딩 영역. 불변으로 다루고 재처리는 파티션 교체로 한다"
    client.create_dataset(dataset, exists_ok=True)

    total_bytes = 0
    for spec in specs:
        sql = build_el_sql(spec, args.project, start, end)
        job = client.query(sql, location=args.location)
        job.result()
        billed = job.total_bytes_billed or 0
        total_bytes += billed
        print(f"  {spec.name:22s} {spec.strategy:20s} billed {billed / 1e6:>9.1f} MB")

    print(f"\n합계 {total_bytes / 1e9:.3f} GB 과금 대상")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
