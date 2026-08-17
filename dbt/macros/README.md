# macros/ — 재사용 SQL 조각

## 1. 개요

- `get_run_date.sql` 하나에 매크로 둘

| 매크로 | 돌려주는 것 |
|---|---|
| `get_run_date()` | 처리 기준일. Airflow가 넘긴 `run_date` var, 없으면 `current_date()` |
| `get_lookback_start_date(extra_days=0)` | `run_date - lookback_days - extra_days` |

## 2. 구성

값이 전달되는 경로.

```
Airflow  --vars '{"run_date": "{{ ds }}"}'
   ↓
dbt      var('run_date')
   ↓
모델      {{ get_run_date() }}  →  date('2026-08-15')
```

## 3. 고려사항

- **모델 안에서 `current_date()`를 직접 부르지 않음**

  ```sql
  -- 하면 안 되는 것
  where event_date >= date_sub(current_date(), interval 3 day)

  -- 대신
  where event_date >= {{ get_lookback_start_date() }}
  ```

  - `current_date()`를 쓰면 3개월 전 구간을 백필해도 오늘 날짜로 계산해 버림
  - 백필이 실패하는 게 아니라 조용히 잘못된 결과를 만듦. 실패는 알아채지만 조용한 오답은 몇 달 뒤에 발견됨
  - → 다음 프로젝트: 배치 로직에 "지금"을 뜻하는 함수가 들어가면 백필이 성립하지 않음. 기준 시각은 항상 밖에서 주입받을 것

- **lookback 값이 한 곳에서만 오는 이유**
  - `lookback_days`는 `dbt_project.yml`의 vars에 있고, Airflow 쪽은 `airflow/dags/thelook/config.py`의 `LOOKBACK_DAYS`를 씀
  - Airflow Variable로 빼지 않은 건 두 값이 반드시 같아야 하기 때문
  - 따로 설정하게 두면 언젠가 한쪽만 바뀌고, EL이 채운 구간과 dbt가 다시 만드는 구간이 어긋남. 조용히 일어나는 사고
  - 지금은 두 파일에 같은 숫자(3)가 적혀 있음. 완전한 단일 출처는 아니고, 값이 갈리면 깨지는 구조라는 것을 양쪽 주석에 남겨 뒀음
  - 더 엄격하게 가려면 Airflow가 dbt vars로 lookback도 함께 넘기면 됨

- **macro는 UDF가 아님**

  | | macro | UDF |
  |---|---|---|
  | 실행 시점 | 컴파일 타임 텍스트 치환 | 런타임 DB 실행 |
  | 위치 | dbt 프로젝트 안 | DB에 배포된 객체 |
  | dbt 밖에서 사용 | 불가 | 가능 |

  - 매크로는 SQL을 만들어내는 도구지 DB에 존재하는 함수가 아님

## 4. 실행

```bash
# 치환 결과 확인
dbt compile --select int_events_sessionized
cat target/compiled/thelook_dw/models/intermediate/int_events_sessionized.sql
```
