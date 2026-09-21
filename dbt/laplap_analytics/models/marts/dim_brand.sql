{{ config(materialized='table') }}

WITH ranked AS (

    SELECT
        brand_id,
        brand_name,
        is_chip_brand,
        record_updated_at,
        elton_created_at,

        ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY record_updated_at DESC, elton_created_at DESC) AS rn

    FROM {{ ref('stg_brand') }}

)

SELECT
    brand_id,
    brand_name,
    is_chip_brand

FROM ranked

WHERE rn = 1