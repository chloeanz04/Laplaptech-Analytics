

WITH ranked AS (

    SELECT
        gpu_model_id,
        gpu_model_name,
        brand_id,
        is_active,
        record_updated_at,
        elton_created_at,

        ROW_NUMBER() OVER (
            PARTITION BY gpu_model_id
            ORDER BY
                record_updated_at DESC,
                elton_created_at DESC
        ) AS rn

    FROM `laplap-analytics`.`laplap_analytics`.`stg_gpu_model`

)

SELECT
    gpu_model_id,
    gpu_model_name,
    brand_id,
    is_active

FROM ranked

WHERE rn = 1