# 모델을 추가하는 절차

> dbt  ·  [← 매크로와 메타데이터](05-macros-metadata.md) | [목차](README.md) | [dbt 앞단 — 원천에서 DW로 →](07-ingestion.md)

새 모델 하나를 만들 때 dev 환경에서 무슨 순서로 손을 움직이고, 테스트를 어디에 끼워 넣고, CI 를 거쳐 운영에 배포하기까지의 흐름을 다룸.

## 6-1. 모델을 추가하는 절차 — 개발부터 배포까지

> 앞 절들이 "무엇이 있는가"였다면, 여기는 **"실제로 무슨 순서로 손을 움직이는가"**임.

### 6-1-1. 전체 흐름

```
dev 환경                        내 작업 결과만 만들어짐. 남의 것을 건드리지 않음
      ↓  통과하면
PR → CI                        격리된 환경에서 변경분만 다시 빌드·테스트
      ↓  통과하면
머지 → 운영 스케줄               prod target 으로 같은 코드가 다시 돎
```

- **배포는 코드를 옮기는 것이지 테이블을 옮기는 것이 아님**
  - dev 에서 만든 테이블을 prod 로 복사하지 않음
  - 머지된 코드를 운영 스케줄이 **prod 데이터셋에 다시 만듦**
  - `ref()` 가 이걸 가능하게 함([1-1](01-basics.md) 참조). 코드는 그대로 두고 `--target` 만 바뀜

### 6-1-2. 계층 순서대로 올라감

```
source 선언  →  staging  →  (필요하면) intermediate  →  marts
```

- 한 계층을 완성하고 **테스트를 통과시킨 뒤** 다음 계층으로 감
- 한 번에 mart 까지 만들면 틀렸을 때 어디서 틀렸는지 찾기 어려움
- intermediate 는 **두 번째 mart 가 같은 조인을 필요로 할 때** 올림([1-2](01-basics.md) 참조). 미리 만들지 않음

### 6-1-3. 모델 하나를 만드는 루프

**① 만들기 전에 — SQL 유효성부터**

```bash
dbt compile --select my_new_model            # Jinja 가 렌더링된 실제 SQL 확인
dbt build   --select my_new_model --empty    # 0행으로 빌드. 문법·컬럼 참조 오류만 걸러냄 (1.8+)
```

- `--empty` 는 큰 테이블을 스캔하지 않으므로 **비용 없이 빠르게** 반복할 수 있음

**② 만들어보기 — dev 환경에서**

```bash
dbt run --select my_new_model
```

- `profiles.yml` 의 `dev` 가 개발자별 데이터셋을 가리키므로 다른 사람 테이블에 영향이 없음([2-1](02-project-setup.md) 참조)
- **materialization 은 여기서 정하지 않음.** view 나 table 로 단순하게 시작하고,
  재계산 비용이 실제로 문제가 된 뒤에 incremental 로 옮김([1-3](01-basics.md) 참조)

**③ 눈으로 확인한 것을 테스트로 고정**

- 방금 확인한 것(키가 겹치지 않는가, 금액이 음수가 아닌가)을 그대로 YAML 에 옮겨 적음

```yaml
models:
  - name: my_new_model
    description: "이 모델의 1행이 무엇을 뜻하는가"
    columns:
      - name: order_id
        data_tests: [unique, not_null]
```

- **description 을 이때 씀.** 나중에 몰아서 쓰면 안 씀([5-3](05-macros-metadata.md) 참조)

**④ 만들면서 테스트 — 여기서부터 `build`**

```bash
dbt build --select my_new_model
```

- `run` 과 달리 **모델을 만든 직후 그 모델의 테스트를 돌림**([3-1](03-testing.md) 참조)

**⑤ downstream 회귀 확인**

```bash
dbt ls    --select my_new_model+     # 무엇이 영향받는지 먼저 봄
dbt build --select my_new_model+     # 실제로 돌려 확인
```

### 6-1-4. 로직이 복잡하면 만들기 전에 unit test

- unit test 는 **원천 데이터가 필요 없음**([3-1](03-testing.md) 참조)
- 그래서 기대 출력을 먼저 적어 두고 로직을 맞춰 갈 수 있음

```bash
dbt test --select test_type:unit     # 몇 초면 끝남
```

- 모든 모델에 걸지 않음. 단순 rename 에 걸면 SQL 을 그대로 옮겨 적는 꼴이 됨
- **"이게 맞게 도는지 자신이 없는 곳"** 에만 걺

### 6-1-5. 계층마다 거는 테스트가 다름

| 계층 | 무엇을 지키나 |
|---|---|
| `staging` | 원천과의 계약 — `not_null` · `unique` · `accepted_values` |
| `intermediate` | grain 유지 — `unique_combination_of_columns` |
| `marts/core` | 참조 무결성 `relationships`, 비즈니스 규칙 `expression_is_true` |
| `marts/reporting` | 집계 후에도 grain 이 깨지지 않았는지 |

- **staging 에 테스트가 가장 많아야 함.** 오염이 downstream로 퍼지기 전에 끊는 자리라서

### 6-1-6. 테스트가 실패했을 때 — 코드부터 고치지 않음

**실패는 세 가지 중 하나임. 어느 쪽인지 먼저 가려야 함.**

| 원인 | 신호 | 조치 |
|---|---|---|
| **내 변환이 틀림** | 방금 고친 모델에서만 실패 | 코드를 고침 |
| **원천이 오염됨** | 코드를 안 건드렸는데 실패 | 원천을 고치고 재처리([8-4](08-operations.md) 참조). 코드를 고치면 문제를 덮는 것 |
| **테스트가 틀림** | 실패한 행을 보니 정상 데이터 | 테스트를 고침. 규칙을 잘못 선언한 경우임 |

> **실패를 없애려고 테스트를 지우거나 `severity` 를 낮추는 것이 가장 흔한 실수임.**
> 그건 게이트를 없애는 것이지 문제를 푸는 것이 아님.
> `warn` 으로 내리는 판단은 **"이건 우리 변환의 버그가 아니라 원천의 문제이고,
> downstream 숫자가 틀리지는 않는다"** 가 성립할 때만 함([3-1](03-testing.md) 참조).

### 6-1-7. dev 에서 통과했는데 운영에서 깨지는 경우

- **dev 와 prod 는 보는 데이터가 다를 수 있음**
  - dev 에서 최근 며칠만 빌드했다면 과거 데이터의 이상값을 못 봄
  - 원천 전량에서만 나타나는 중복·고아 행이 있음
- **그래서 CI 를 둠**([8-2](08-operations.md) 참조)

```bash
dbt build --select state:modified+ --defer --state ./prod-manifest
```

- 변경된 모델과 **그 downstream만** 빌드하되, 안 바뀐 upstream는 prod 것을 그대로 참조함
- 모델이 300개여도 몇 분에 끝나고, **prod 와 같은 데이터**로 검증됨

### 6-1-8. 정리

```
1. dbt compile / --empty        SQL 이 말이 되는가
2. dbt run --select 모델         내 데이터셋에 만들어봄
3. YAML 에 description + test    확인한 것을 코드에 고정
4. dbt build --select 모델       만들고 테스트
5. dbt build --select 모델+      downstream 회귀 확인
6. PR → CI (state:modified+)    prod 데이터로 검증
7. 머지 → 운영 스케줄            같은 코드가 prod 에 다시 만듦
```

---

[← 매크로와 메타데이터](05-macros-metadata.md) | [목차](README.md) | [dbt 앞단 — 원천에서 DW로 →](07-ingestion.md)
