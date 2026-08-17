"""EL 단계의 테이블별 적재 전략 정의.

전략을 한 곳에 모아 두는 이유는, 테이블마다 왜 다른 방식을 택했는지가
파이프라인에서 가장 자주 받는 질문이기 때문이다. 코드가 곧 근거 문서가 되게 한다.
"""

from dataclasses import dataclass, field

SOURCE_PROJECT = "bigquery-public-data"
SOURCE_DATASET = "thelook_ecommerce"
RAW_DATASET = "raw_thelook"

# 3rd party 수집 지연·트랜잭션 지연을 흡수하는 재처리 윈도우(일).
# dbt vars 의 lookback_days 와 같은 값을 유지한다.
LOOKBACK_DAYS = 3


@dataclass(frozen=True)
class TableSpec:
    name: str
    strategy: str
    rationale: str
    event_time_column: str | None = None
    cluster_by: tuple[str, ...] = field(default_factory=tuple)
    primary_key: str = "id"


# 전략 3종
#
#   partition_overwrite : 이벤트성 테이블. [ds-lookback, ds] 구간을 지우고 다시 넣는다.
#                         같은 구간을 몇 번 돌려도 결과가 같다 → 백필 가능.
#   full_replace        : 작고, 과거 행이 나중에 갱신되는 테이블.
#                         생성일 기준 증분으로는 그 갱신을 놓치므로 통째로 다시 만든다.
#   merge_insert_only   : 마스터. 기존 행을 덮지 않고 신규 키만 넣는다.
#                         (이 프로젝트에서 이 전략을 택한 추가 사정은 README 참조)

TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        name="orders",
        strategy="partition_overwrite",
        rationale="주문 헤더. 생성일이 곧 사건 발생일이라 파티션 경계가 명확하다",
        event_time_column="created_at",
        cluster_by=("user_id",),
        primary_key="order_id",
    ),
    TableSpec(
        name="order_items",
        strategy="partition_overwrite",
        rationale="주문 상세. orders 와 같은 구간으로 잘라야 두 테이블의 정합이 유지된다",
        event_time_column="created_at",
        cluster_by=("order_id", "product_id"),
    ),
    TableSpec(
        name="events",
        strategy="partition_overwrite",
        rationale="행동 로그. 2.4M행으로 가장 크고, 전체 재적재 비용이 유일하게 유의미한 테이블",
        event_time_column="created_at",
        cluster_by=("user_id", "event_type"),
    ),
    TableSpec(
        name="inventory_items",
        strategy="full_replace",
        rationale=(
            "재고 단품. created_at 은 입고일이지만 sold_at 은 나중에 채워진다. "
            "입고일 기준으로 증분을 뜨면 과거 파티션에서 일어난 판매 갱신을 영원히 놓친다. "
            "488K행이라 전체 교체가 더 싸고 정확하다"
        ),
    ),
    TableSpec(
        name="users",
        strategy="full_replace",
        rationale="회원 마스터. 100K행. 프로필은 언제든 갱신되므로 생성일 증분이 성립하지 않는다",
    ),
    TableSpec(
        name="distribution_centers",
        strategy="full_replace",
        rationale="물류센터 마스터. 10행",
    ),
    TableSpec(
        name="products",
        strategy="merge_insert_only",
        rationale=(
            "상품 마스터. 기존 행을 덮지 않고 신규 상품만 적재한다. "
            "원천에 updated_at 이 없어 변경 감지는 dbt snapshot(check 전략)이 담당하며, "
            "EL 이 매번 전체를 덮으면 그 이력이 만들어질 여지가 사라진다"
        ),
    ),
)

TABLES_BY_NAME = {t.name: t for t in TABLES}
