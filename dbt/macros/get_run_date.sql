{#
    처리 기준일을 돌려준다.

    Airflow 가 `--vars '{run_date: "{{ ds }}"}'` 로 넘긴 값을 쓰고,
    없으면 오늘로 떨어진다.

    **모델 안에서 current_date() 를 직접 부르지 않는 이유가 이것이다.**
    직접 부르면 3개월 전 구간을 백필해도 오늘 날짜로 계산해버려
    백필이 조용히 잘못된 결과를 만든다.
#}

{% macro get_run_date() %}
    {%- if var('run_date', '') != '' -%}
        date('{{ var("run_date") }}')
    {%- else -%}
        current_date()
    {%- endif -%}
{% endmacro %}


{#
    재처리 구간의 시작일. [run_date - lookback_days, run_date] 를 매번 다시 만든다.
    3rd party 수집 지연과 트랜잭션 지연을 흡수하는 장치다.
#}

{% macro get_lookback_start_date(extra_days=0) %}
    date_sub({{ get_run_date() }}, interval {{ var('lookback_days') + extra_days }} day)
{% endmacro %}
