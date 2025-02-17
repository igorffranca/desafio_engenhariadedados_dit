{{ config(materialized='incremental') }}

SELECT
    vehicle_id,
    timestamp,
    ST_GeomFromText(geometry) as position,
    speed,
    updated_at
FROM {{ source('raw', 'raw_brt_data') }}

{% if is_incremental() %}
    WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}

