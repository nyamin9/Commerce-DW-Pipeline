# dbt

> dbt로 데이터 웨어하우스 변환 계층을 설계·구축·운영하면서 정리한 문서.
> 정의와 코드, 선택 기준, 자주 밟는 함정을 다룸.
> 오케스트레이션은 [Airflow](../airflow/README.md)에 있음.

## 목차

| | 문서 | 다루는 것 |
|---|---|---|
| 1 | [기초](01-basics.md) | `ref()` / `source()` · 계층 구조 · materialization 4종 |
| 2 | [프로젝트 구조와 명령어](02-project-setup.md) | `dbt_project.yml` · `profiles.yml` · 환경 분리 · 네이밍 · seed · 패키지 · 명령어와 선택 문법 |
| 3 | [테스트와 데이터 품질](03-testing.md) | generic / singular / unit test · 내장 검증과 독립 감시 |
| 4 | [이력 적재](04-history.md) | SCD Type 2 (`snapshot`) · 일자별 스냅샷 · 선택 기준 |
| 5 | [매크로와 메타데이터](05-macros-metadata.md) | Jinja와 macro · lineage · contract · exposure · `persist_docs` |
| 6 | [모델을 추가하는 절차](06-workflow.md) | dev에서 만들고 CI로 검증해 운영에 배포하기까지 |
| 7 | [dbt 앞단](07-ingestion.md) | 추출 전략 · 워터마크 함정 · 랜딩 · 스키마 드리프트 |
| 8 | [실전 운영](08-operations.md) | BigQuery 물리 설정 · Slim CI · Semantic Layer · 장애 대응과 재처리 |
| 9 | [부록](09-appendix.md) | Dataform 개념 대응표 · 구현 체크리스트 |

- 절 번호는 문서 번호를 따름. `3-1`은 3번 문서의 첫 절임

## 읽는 순서

- **1 → 2** 로 시작함. 2에 프로젝트 골격과 명령어가 있어 손을 움직이려면 먼저 필요함
- **3 → 6** 이 핵심. 검증을 배우고 나서 "모델 하나를 추가하는 한 바퀴"를 봄
- 4·5는 필요해질 때 찾아보는 편이 나음
- 7은 dbt 밖의 이야기라 EL을 직접 만들 때, 8은 굴리기 시작한 뒤에 봄
