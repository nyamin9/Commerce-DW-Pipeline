# snapshots/ — SCD Type 2

## 1. 개요

- `snap_products` 하나뿐. 상품 마스터의 변경 이력을 만듦
- dbt가 `dbt_valid_from` / `dbt_valid_to` / `dbt_scd_id` / `dbt_updated_at`을 자동으로 붙임
- 현재 유효한 행은 `dbt_valid_to`가 NULL. SCD Type 2의 유효 구간 구조 그대로
- 소비는 이 테이블을 직접 하지 않고 [`dim_products_history`](../models/marts/README.md)가 감쌈

## 2. 구성

| 항목 | 값 |
|---|---|
| unique_key | `id` |
| strategy | `check` |
| check_cols | `retail_price`, `cost`, `name`, `brand`, `category`, `department`, `distribution_center_id` |
| invalidate_hard_deletes | `true` |
| target_schema | `snapshots` |

## 3. 고려사항

- **왜 check 전략인가**
  - `timestamp` 전략이 가볍고 정확하지만 원천에 믿을 만한 갱신 시각이 있어야 씀
  - `thelook`의 `products`에는 `updated_at`이 없음

  | 전략 | 동작 | 조건 |
  |---|---|---|
  | `timestamp` | `updated_at`을 보고 변경 판단 | 신뢰할 만한 갱신 시각 필요 |
  | `check` | 지정 컬럼 값을 이전 스냅샷과 직접 비교 | 없어도 됨. 대신 비교 비용 |

  - `check`는 지정하지 않은 컬럼의 변경을 놓침
  - 그래서 `sku`처럼 바뀔 일 없는 컬럼은 빼고 비즈니스적으로 의미 있는 것만 넣었음

- **한계를 알고 씀**
  - snapshot은 실행 시점의 상태만 봄
  - 하루 한 번 돌리는데 그 사이 가격이 두 번 바뀌었다면 중간 값은 영원히 남지 않음
  - 실행 주기가 곧 이력의 해상도
  - 더 촘촘한 이력이 필요하면 snapshot 주기를 올릴 게 아니라 원천에서 CDC를 받아야 함. 도구를 바꿀 문제
  - → 다음 프로젝트: SCD Type 2를 붙이기 전에 "얼마나 촘촘한 이력이 필요한가"를 먼저 물을 것. snapshot은 배치 주기보다 세밀한 변경을 절대 잡지 못함

- **staging보다 먼저 도는 이유**
  - snapshot은 `source()`를 직접 읽으니 staging이 필요 없음
  - 변환이 실패해도 그날의 원천 상태는 남아야 함. 놓친 날의 상태는 되돌릴 방법이 없음

- **`SELECT *`를 쓰지 않음**
  - 원천에 컬럼이 추가되면 스냅샷 스키마가 조용히 바뀜. 명시해서 그 순간 깨지게 둠

- **이력이 실제로 쌓이는지 확인하려면**
  - 공개 데이터셋은 상품 가격이 변하지 않음. snapshot을 아무리 돌려도 버전이 하나뿐이고 유효 구간 전이가 만들어지지 않음
  - 원천을 인위적으로 바꿔야 함

    ```bash
    # 1. 가격 변경 (시뮬레이션 — 일 배치에는 없음)
    python ../../scripts/simulate_price_change.py --project $THELOOK_GCP_PROJECT

    # 2. 스냅샷을 다시 돌려 새 버전을 쌓음
    dbt snapshot

    # 3. 이력 확인
    python ../../scripts/simulate_price_change.py --project $THELOOK_GCP_PROJECT --show
    ```

  - `raw_thelook.products`의 EL 전략을 `merge_insert_only`로 둔 것도 여기서 바꾼 값이 다음 배치에 되돌아가지 않게 하려는 것 → [../../airflow/README.md](../../airflow/README.md)
  - BigQuery 샌드박스(결제 미연결)에서는 snapshot 2회차가 MERGE라 막힘. 최초 실행은 CTAS라서 통과함

## 4. 실행

```bash
dbt snapshot                             # 이력 적재
dbt build --select snap_products+        # 스냅샷과 downstream(dim_products_history)
```
