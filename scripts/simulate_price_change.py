#!/usr/bin/env python3
"""SCD Type 2 를 실증하기 위해 원천 상품 가격을 인위적으로 바꾼다.

── 왜 이런 스크립트가 필요한가 ────────────────────────────────────────
`bigquery-public-data.thelook_ecommerce` 는 우리가 바꿀 수 없는 공개 데이터셋이고,
상품 가격이 실제로 변하지 않는다. 그래서 snapshot 을 아무리 돌려도
버전이 하나뿐이고 dbt_valid_from / dbt_valid_to 의 전이가 만들어지지 않는다.

**이건 시뮬레이션이다.** 일 배치(`thelook_dw_daily`)에는 들어 있지 않고,
SCD Type 2 가 실제로 이력을 쌓는지 확인할 때만 손으로 실행한다.
`raw_thelook.products` 의 EL 전략을 merge_insert_only 로 둔 것도
여기서 바꾼 값이 다음 배치에 되돌아가지 않게 하기 위해서다.

── 사용법 ────────────────────────────────────────────────────────────
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

    # 1. 가격 변경 (기본 20개 상품, ±15%)
    python scripts/simulate_price_change.py --project my-proj

    # 2. 스냅샷을 다시 돌려 새 버전을 쌓는다
    cd dbt && dbt snapshot

    # 3. 이력 확인
    python scripts/simulate_price_change.py --project my-proj --show

⚠️ BigQuery 샌드박스(결제 미연결)에서는 UPDATE 가 차단되어 동작하지 않는다.
"""

from __future__ import annotations

import argparse
import sys


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="상품 가격 변경 시뮬레이션")
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", default="raw_thelook")
    p.add_argument("--count", type=int, default=20, help="변경할 상품 수")
    p.add_argument("--pct", type=float, default=0.15, help="변동 폭 (0.15 = ±15%%)")
    p.add_argument("--show", action="store_true", help="변경하지 않고 이력만 조회")
    p.add_argument("--snapshot-dataset", default="snapshots",
                   help="snapshot 의 target_schema. 모델과 달리 profile 의 dataset "
                        "접두어가 붙지 않아 절대 스키마다")
    return p.parse_args()


def show_history(client, args) -> int:
    table = f"`{args.project}.{args.snapshot_dataset}.snap_products`"
    try:
        rows = list(client.query(f"""
            select id, name, retail_price, dbt_valid_from, dbt_valid_to
            from {table}
            where id in (
                select id from {table} group by id having count(*) > 1
            )
            order by id, dbt_valid_from
            limit 40
        """))
    except Exception as exc:
        print(f"스냅샷 조회 실패: {exc}", file=sys.stderr)
        print("먼저 `dbt snapshot` 을 한 번 이상 실행했는지 확인한다.", file=sys.stderr)
        return 1

    if not rows:
        print("버전이 2개 이상인 상품이 없다.")
        print("--show 없이 한 번 실행해 가격을 바꾼 뒤 `dbt snapshot` 을 다시 돌린다.")
        return 0

    print(f"{'product_id':>10} {'retail_price':>13}  {'valid_from':<26} {'valid_to':<26} name")
    for r in rows:
        valid_to = str(r.dbt_valid_to) if r.dbt_valid_to else "(현재)"
        print(f"{r.id:>10} {float(r.retail_price):>13.2f}  {str(r.dbt_valid_from):<26} {valid_to:<26} {r.name[:34]}")
    return 0


def main() -> int:
    args = parse_args()
    from google.cloud import bigquery

    client = bigquery.Client(project=args.project)

    if args.show:
        return show_history(client, args)

    target = f"`{args.project}.{args.dataset}.products`"

    # 결정적으로 대상을 고른다. 매번 다른 상품이 바뀌면 무엇이 왜 바뀌었는지 추적이 안 된다.
    sql = f"""
    update {target} as p
    set retail_price = round(
            p.retail_price * (1 + {args.pct} * if(mod(p.id, 2) = 0, 1, -1)),
            2
        )
    from (
        select id from {target} order by id limit {args.count}
    ) as picked
    where p.id = picked.id
    """

    try:
        job = client.query(sql)
        job.result()
    except Exception as exc:
        message = str(exc)
        if "Billing has not been enabled" in message:
            print("BigQuery 샌드박스에서는 UPDATE 가 차단된다. 결제를 연결해야 한다.", file=sys.stderr)
            return 3
        raise

    print(f"상품 {job.num_dml_affected_rows}개의 retail_price 를 ±{args.pct:.0%} 조정했다.")
    print("이제 `cd dbt && dbt snapshot` 을 실행하면 새 버전이 쌓인다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
