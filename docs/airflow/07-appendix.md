# 부록

> Airflow  ·  [← 실전 운영](06-operations.md) | [목차](README.md)

구현 체크리스트.

## 7-1. 구현 체크리스트

이 문서의 개념을 실제 DAG에 옮길 때 확인할 항목. 괄호는 해당 절 번호.

**멱등성과 시간**

- [ ] `{{ ds }}` 사용, `datetime.now()` 금지 ([2-4](02-dag-scheduling.md))
- [ ] 같은 `logical_date`로 재실행해도 결과가 같은지 확인 ([2-4](02-dag-scheduling.md))
- [ ] 적재는 파티션 교체나 MERGE로. 단순 INSERT는 재실행 시 중복 ([2-4](02-dag-scheduling.md))
- [ ] `catchup` 값을 의식적으로 결정 — 켜면 밀린 run이 한꺼번에 뜸 ([2-4](02-dag-scheduling.md))

**의존성과 실패 처리**

- [ ] 게이트가 아닌 태스크는 downstream 태스크를 두지 않음 ([3-1](03-execution-failure.md))
  - [ ] `trigger_rule=all_done`으로 "내 실패를 무시해달라"를 표현하지 않음. upstream 실패까지 통과시킴
- [ ] 분기 뒤 합류 지점에 `none_failed_min_one_success` ([3-3](03-execution-failure.md))
- [ ] 태스크마다 `execution_timeout` 설정. SLA는 알리기만 하고 끝내지 않음 ([3-1](03-execution-failure.md))
- [ ] 재시도가 답인 실패와 아닌 실패를 구분해 `retries` 조정 ([3-1](03-execution-failure.md))
- [ ] `on_failure_callback`으로 실패 알림 경로 확보 ([3-1](03-execution-failure.md))

**구조와 가독성**

- [ ] 태스크가 많아지면 `TaskGroup`으로 묶음 ([2-2](02-dag-scheduling.md))
- [ ] `doc_md`로 설계 근거를 UI에 노출. 별도 위키에 적으면 코드와 갈라짐 ([2-2](02-dag-scheduling.md))
- [ ] 변환 도구의 내부 의존성을 DAG으로 다시 그리지 않음 ([5-1](05-dbt-integration.md))
- [ ] 로직이 길어지면 DAG 파일 밖으로 분리. DAG 파일은 반복 파싱됨 (2-2, [4-3](04-advanced.md))
- [ ] 대상 개수가 실행 시점에 정해지면 Dynamic Task Mapping ([4-1](04-advanced.md))

**연결과 자원**

- [ ] 자격증명을 코드에 두지 않고 Connection으로 분리 ([6-1](06-operations.md))
- [ ] `Variable.get()`을 DAG 최상단에 두지 않음 ([6-1](06-operations.md))
- [ ] `max_active_runs`로 동시 run 제한 ([6-2](06-operations.md))
- [ ] 웨어하우스 동시 쿼리에 `pool`로 상한 ([6-2](06-operations.md))

**검증**

- [ ] CI에 DAG 파싱 테스트. `DagBag.import_errors == {}`가 최소선 ([4-3](04-advanced.md))
- [ ] `airflow dags test`로 스케줄러 없이 전 구간 실행 확인 ([4-3](04-advanced.md))
- [ ] EL 구간을 별도 태스크로 두고 변환 도구가 그 결과를 가리키게 함 ([5-1](05-dbt-integration.md))

## 관련 문서

- **[dbt](../dbt/README.md)** — 이 DAG이 실행하는 변환 계층의 설계

---

[← 실전 운영](06-operations.md) | [목차](README.md)
