{{ config(severity='warn', store_failures=true) }}

-- 원천이 준 주문 상품 수(num_of_item)와 실제 주문 상세 행 수가 일치하는가.
--
-- ── severity 를 warn 으로 둔 이유 ────────────────────────────────────
-- 이건 **우리 변환의 버그가 아니라 원천의 정합성 문제**다.
-- error 로 두면 원천이 어긋난 날 downstream 마트 전체가 멈춘다.
-- 매출 숫자 자체는 order_items 기준으로 계산되므로 값이 틀리지는 않는다.
-- 그래서 downstream를 막지 않고 기록만 남기고, 추세를 본다.
--
-- ── store_failures 를 켠 이유 ───────────────────────────────────────
-- 실패한 행을 테이블로 남긴다. 한 번의 실패보다 **누적된 추세**가 중요한 종류의
-- 문제라서, 언제부터 몇 건씩 어긋나기 시작했는지를 볼 수 있어야 한다.

select
    order_key,
    order_id,
    source_item_count,
    line_item_count,
    line_item_count - source_item_count as diff,
    ordered_date
from {{ ref('fct_orders') }}
where line_item_count is not null
  and source_item_count != line_item_count
