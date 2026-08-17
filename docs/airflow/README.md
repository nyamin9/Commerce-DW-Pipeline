# Airflow

> Airflow로 데이터 파이프라인을 설계·구축·운영하면서 정리한 문서.
> 정의와 코드, 선택 기준, 자주 밟는 함정을 다룸.
> 변환 계층(dbt)은 [dbt](../dbt/README.md)에 있음.

## 목차

| | 문서 | 다루는 것 |
|---|---|---|
| 1 | [시스템 구성](01-architecture.md) | Scheduler · Worker · Triggerer · 메타DB · 배포 형태 · 장애 시 볼 곳 |
| 2 | [DAG과 스케줄링](02-dag-scheduling.md) | DAG/Task/Operator · XCom · TaskFlow · `logical_date` · 템플릿 · 백필 |
| 3 | [실행 순서와 실패 처리](03-execution-failure.md) | 태스크 상태 11종 · `trigger_rule` · 재시도 · 분기 · Sensor |
| 4 | [심화](04-advanced.md) | Dynamic Task Mapping · 실행 환경 격리 · DAG 테스트 · Airflow 3 |
| 5 | [dbt 연동](05-dbt-integration.md) | 오케스트레이터와 변환 도구의 경계 · dbt 실행 패턴 |
| 6 | [운영](06-operations.md) | Connection·Variable·Hook · Executor와 동시성 · 데이터 기반 스케줄링 · 명령어 |
| 7 | [부록](07-appendix.md) | 구현 체크리스트 |

- 절 번호는 문서 번호를 따름. `3-1`은 3번 문서의 첫 절임

## 읽는 순서

- **1을 먼저 봄.** 무엇이 DAG을 읽고 실행하는지 모르면 나머지가 공중에 뜸
- 2 → 3이 핵심. 특히 3의 태스크 상태는 `trigger_rule`을 이해하는 전제임
- 4는 필요해질 때, 5는 dbt를 붙일 때, 6은 레퍼런스
