# models/ — 3계층 변환

## 1. 개요

```
staging/         원천 컬럼명을 표준 이름으로       7 모델   [view]
    ▼
intermediate/    공통 조인 · 세션화           2 모델   [view / 증분]
    ▼
marts/core/      DW — fact / dimension, grain 유지  8 모델   [table / 증분]
    ▼
marts/reporting/ DM — 목적별 집계                3 모델   [table]
```

| 폴더 | 설명 |
|---|---|
| [`staging/`](staging/README.md) | 원천과 1:1. 정제만 함 |
| [`intermediate/`](intermediate/README.md) | 여러 mart가 공통으로 쓰는 중간 로직 |
| [`marts/`](marts/README.md) | 소비 계층. **DW**(`core`)와 **DM**(`reporting`)으로 다시 나뉨 |

## 2. 구성

각 계층은 금지 사항이 곧 존재 이유임.

| 계층 | 해도 되는 것 | 하면 안 되는 것 |
|---|---|---|
| `staging` | 컬럼명 표준화, 타입 캐스팅, 불필요 컬럼 제거 | 조인, 집계, 비즈니스 로직 |
| `intermediate` | 조인, 전처리, 파생 계산 | 최종 소비자에게 노출 |
| `marts/core` (DW) | fact / dimension 구성 | grain을 바꾸는 집계, 목적별 반정규화 |
| `marts/reporting` (DM) | 집계, 반정규화, 지표 계산 | 여기서만 존재하는 사실 정의 |

**Medallion 대응**

| 아키텍처 계층 | Medallion | 여기서는 |
|---|---|---|
| 원천 착륙 | Bronze | `raw_thelook` — 모델이 아니라 `source()` |
| 정제·통합 | Silver | `staging` + `intermediate` |
| 소비 | Gold | `marts` |

- `staging`과 `intermediate`는 별개 계층이 아님. 둘 다 Silver이고 안쪽을 코드 조직 관점에서 나눈 것
- 근거는 staging이 대부분 view라 저장조차 되지 않는다는 점. 저장 계층이라기보다 이름 붙인 CTE에 가까움

## 3. 고려사항

- **staging에서 조인을 금지한 것이 이 구조 전체의 출발점**
  - staging 위쪽은 표준화된 이름만 씀
  - 원천 컬럼명이 바뀌어도 손댈 곳이 그 모델 하나
  - 조인하는 순간 그 경계가 흐려짐

- **intermediate는 성능이 아니라 정합성을 위한 계층**
  - mart A와 mart B가 각자 같은 조인을 하면 나중에 한쪽만 고쳐져 숫자가 갈라짐

- **계층을 미리 만들지 않음**
  - 원천이 적고 mart가 적으면 source에서 mart로 직행해도 됨
  - 계층은 필요해지는 시점에 올림. 안 그러면 통과만 하는 빈 계층이 쌓임
  - 이 프로젝트에 intermediate가 2개뿐인 것도 그 때문. `int_order_items_enriched`는 두 번째 mart가 같은 조인을 필요로 했을 때 올렸음

- **`SELECT *`를 쓰지 않음**
  - staging에서 컬럼을 명시하는 건 스키마 드리프트를 일찍 감지하려는 것

  | 원천 변경 | 위험도 | 이 구조에서 |
  |---|---|---|
  | 컬럼 추가 | 낮음 | staging이 무시함. 필요해지면 명시적으로 추가 |
  | 컬럼 삭제 | 높음 | staging이 즉시 깨짐 — 원하는 동작 |
  | 타입 변경 | 가장 위험 | 캐스팅이 실패하거나 테스트가 잡음 |

  - `SELECT *`였다면 컬럼이 사라져도 조용히 지나가고, 마트에서 NULL이 늘어난 뒤에야 알게 됨
  - → 다음 프로젝트: 원천 스키마를 통제할 수 없는 구간에서는 관대하게 받고 엄격하게 읽을 것. 랜딩은 스키마를 강제하지 않고, staging은 컬럼을 명시해 그 자리에서 깨지게 둠

- **PII를 어느 계층에서 끊는가** — 한 곳에서 다 처리하지 않고 계층마다 나눠 판단했음

  | 계층 | 처리 |
  |---|---|
  | `staging` | `street_address`와 좌표를 아예 읽지 않음. 분석에 쓸 일이 없음 |
  | `staging` | `email`, `ip_address`는 남김. 여기까지가 마지막 원문 |
  | `marts` | 이름을 내리지 않음. `email`은 해시와 도메인으로. `ip_address`는 내리지 않음 |

  - `user_id`는 해싱하지 않음. 내부 대리키라 그 자체로는 개인을 식별하지 못하고, 해싱하면 이미 적재된 fact와의 조인이 전부 깨짐. 바꾸려면 전 계층을 동시에 바꿔야 함
  - 핵심은 PII를 지우느냐가 아니라 분석 가능성과 노출 범위를 어디서 맞바꿀지
  - `email_domain`을 남긴 것도 그 때문. 원문 없이도 "회사 메일 가입자 비중" 같은 분석은 살아 있음
