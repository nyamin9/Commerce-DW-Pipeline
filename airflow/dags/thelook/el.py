"""EL 단계의 SQL 생성.

Airflow 태스크와 초기 적재 스크립트가 **같은 함수를 쓴다.**
SQL 이 두 곳에 복사되면 언젠가 한쪽만 고쳐져 조용히 갈라지기 때문이다.
"""

from __future__ import annotations

from .config import RAW_DATASET, SOURCE_DATASET, SOURCE_PROJECT, TableSpec


def _source_ref(spec: TableSpec) -> str:
    return f"`{SOURCE_PROJECT}.{SOURCE_DATASET}.{spec.name}`"


def _target_ref(spec: TableSpec, project: str) -> str:
    return f"`{project}.{RAW_DATASET}.{spec.name}`"


def _ddl_options(spec: TableSpec) -> str:
    """랜딩 테이블의 물리 설계.

    파티션은 스캔 블록 자체를 잘라내므로 BigQuery 에서 비용에 직접 영향을 준다.
    클러스터링은 파티션 안의 정렬이라 필터·조인 효율에 기여한다.
    """
    parts = []
    if spec.event_time_column:
        parts.append(f"PARTITION BY DATE({spec.event_time_column})")
    if spec.cluster_by:
        parts.append(f"CLUSTER BY {', '.join(spec.cluster_by)}")
    return ("\n" + "\n".join(parts)) if parts else ""


def build_el_sql(spec: TableSpec, project: str, start_date: str, end_date: str) -> str:
    """`spec.strategy` 에 맞는 적재 SQL 을 만든다.

    어느 전략이든 **같은 인자로 몇 번을 실행해도 결과가 같아야 한다.**
    그 성질이 깨지면 백필이 성립하지 않는다.
    """
    src = _source_ref(spec)
    tgt = _target_ref(spec, project)

    if spec.strategy == "partition_overwrite":
        col = spec.event_time_column
        window = (
            f"DATE({col}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')"
        )
        return f"""
-- {spec.name}: {spec.rationale}
-- 대상 구간 [{start_date}, {end_date}] 을 지우고 다시 넣는다 → 재실행해도 결과 동일
CREATE TABLE IF NOT EXISTS {tgt}{_ddl_options(spec)}
AS SELECT *, CURRENT_TIMESTAMP() AS _ingested_at FROM {src} WHERE FALSE;

DELETE FROM {tgt} WHERE {window};

INSERT INTO {tgt}
SELECT *, CURRENT_TIMESTAMP() AS _ingested_at
FROM {src}
WHERE {window};
""".strip()

    if spec.strategy == "full_replace":
        return f"""
-- {spec.name}: {spec.rationale}
CREATE OR REPLACE TABLE {tgt}{_ddl_options(spec)}
AS SELECT *, CURRENT_TIMESTAMP() AS _ingested_at FROM {src};
""".strip()

    if spec.strategy == "merge_insert_only":
        pk = spec.primary_key
        return f"""
-- {spec.name}: {spec.rationale}
CREATE TABLE IF NOT EXISTS {tgt}{_ddl_options(spec)}
AS SELECT *, CURRENT_TIMESTAMP() AS _ingested_at FROM {src} WHERE FALSE;

MERGE {tgt} AS target
USING (
    SELECT *, CURRENT_TIMESTAMP() AS _ingested_at FROM {src}
) AS source
ON target.{pk} = source.{pk}
WHEN NOT MATCHED THEN
    INSERT ROW;
""".strip()

    raise ValueError(f"알 수 없는 적재 전략: {spec.strategy}")
