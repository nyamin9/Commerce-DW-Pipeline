# scripts/ — 손으로 실행하는 것들

## 1. 개요

- 일 배치에 들어 있지 않은 것만 여기 둠
- 자동으로 도는 것과 사람이 판단해서 돌리는 것을 폴더로 구분함

| 스크립트 | 언제 쓰나 |
|---|---|
| `run_el.py` | 최초 전체 적재. EL SQL 확인 |
| `simulate_price_change.py` | SCD Type 2가 이력을 쌓는지 실증할 때 |

- 둘 다 `GOOGLE_APPLICATION_CREDENTIALS`가 필요함

## 2. 구성

**run_el.py**

- 일 배치는 `[run_date - 3, run_date]` 구간만 다루므로 과거는 한 번 채워야 함
- `raw_thelook` 데이터셋이 없으면 US 리전으로 만들고 시작함
- 실행이 끝나면 테이블별 과금 대상 바이트를 출력함

**simulate_price_change.py**

- 시뮬레이션 스크립트. 일 배치에 들어 있지 않고, 위치로 그 사실을 드러냄
- `--show` 출력 형태

  ```
  product_id  retail_price  valid_from                  valid_to                    name
       12345         49.00  2026-08-16 03:00:00+00:00   2026-08-16 05:12:00+00:00   ...
       12345         56.35  2026-08-16 05:12:00+00:00   (현재)                       ...
  ```

## 3. 고려사항

- **SQL을 이 폴더에 복사하지 않음**
  - `run_el.py`는 SQL을 `airflow/dags/thelook/el.py`에서 가져옴
  - 두 벌이 되면 언젠가 한쪽만 고쳐지므로. Airflow DAG과 완전히 같은 코드 경로를 돎

- **`--dry-run`을 먼저 쓰는 편이 안전함**
  - 적재 전략이 테이블마다 다름
  - 무엇이 지워지고 무엇이 덮이는지 SQL로 확인하고 실행할 것 → [../airflow/README.md](../airflow/README.md)

- **가격 변경 대상을 결정적으로 고름**
  - `order by id limit N`으로 뽑고 `mod(id, 2)`로 증감 방향을 정함
  - 매번 다른 상품이 무작위로 바뀌면 무엇이 왜 바뀌었는지 추적할 수 없음

- **왜 시뮬레이션이 필요한가**
  - `bigquery-public-data.thelook_ecommerce`는 우리가 바꿀 수 없는 공개 데이터셋이라 상품 가격이 변하지 않음
  - snapshot을 아무리 돌려도 버전이 하나뿐이고 유효 구간 전이가 만들어지지 않음
  - `raw_thelook.products`의 적재 전략이 `merge_insert_only`(신규 상품만 삽입)인 이유도 여기 있음. `full_replace`였다면 여기서 바꾼 값이 다음 배치에 되돌아감

- **샌드박스에서는 둘 다 제한됨**
  - `run_el.py`의 `partition_overwrite`·`merge_insert_only`는 DML(DELETE/MERGE)을 씀
  - `simulate_price_change.py`는 UPDATE를 씀
  - BigQuery 샌드박스는 DML을 차단함 → [../README.md](../README.md)

## 4. 실행

**run_el.py**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

# 최초 전체 적재
python scripts/run_el.py --project <PROJECT> --start 2019-01-01 --end 2026-08-31

# 일 배치와 같은 구간 (오늘 기준 3일)
python scripts/run_el.py --project <PROJECT> --lookback 3

# 특정 테이블만
python scripts/run_el.py --project <PROJECT> --tables orders,order_items

# 실행하지 않고 SQL만 출력
python scripts/run_el.py --project <PROJECT> --dry-run
```

**simulate_price_change.py**

```bash
# 1. 가격 변경 (기본 20개 상품, ±15%)
python scripts/simulate_price_change.py --project <PROJECT>

# 2. 스냅샷을 다시 돌려 새 버전을 쌓음
cd dbt && dbt snapshot && cd ..

# 3. 이력 확인 — 버전이 2개 이상인 상품만 나옴
python scripts/simulate_price_change.py --project <PROJECT> --show
```
