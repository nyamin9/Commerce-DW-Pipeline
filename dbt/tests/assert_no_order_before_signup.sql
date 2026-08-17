-- 가입 시각보다 이른 주문이 있는가.
--
-- 시간 순서가 뒤집힌 데이터는 리텐션·코호트 분석을 조용히 망가뜨린다.
-- months_since_signup 이 음수가 되면서 코호트 표에 존재할 수 없는 칸이 생긴다.
-- rpt_user_cohort_retention 이 그 행을 걸러내고 있지만,
-- **걸러내는 것과 없는 것은 다르다.** 걸러낸 양이 늘고 있다면 원천에 문제가 있는 것이다.
--
-- severity 는 기본값(error)이다. 이건 값이 틀렸다는 신호라 downstream로 내려보내면 안 된다.

select
    orders.order_id,
    orders.user_id,
    orders.ordered_at,
    users.signed_up_at
from {{ ref('fct_orders') }} as orders
inner join {{ ref('dim_users') }} as users
    on orders.user_id = users.user_id
where orders.ordered_at < users.signed_up_at
