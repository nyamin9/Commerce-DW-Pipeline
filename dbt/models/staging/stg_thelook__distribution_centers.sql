-- 물류센터 마스터. 10행.
with source as (

    select
        id,
        name,
        latitude,
        longitude,
        _ingested_at
    from {{ source('raw_thelook', 'distribution_centers') }}

)

select
    id                                  as distribution_center_id,
    name                                as distribution_center_name,
    latitude,
    longitude,
    _ingested_at

from source
