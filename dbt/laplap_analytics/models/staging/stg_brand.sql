{{ config(materialized='view') }}

SELECT 
    id AS brand_id,
    name AS brand_name,
    is_chip_brand,
    created_on,
    changed_on,
    elton_created_at,
    COALESCE(changed_on, created_on, elton_created_at) AS record_updated_at
FROM {{ source('laplap', 'bronze_brand_raw') }}