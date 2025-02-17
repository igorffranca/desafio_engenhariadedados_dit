{{ config(materialized='table') }}

SELECT
    vehicle_id,
    position,
    speed,
    timestamp,
    ST_X(position::geometry) as longitude,
    ST_Y(position::geometry) as latitude
FROM {{ ref('stg_brt_data') }}