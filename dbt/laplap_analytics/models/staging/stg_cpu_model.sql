{{ config(materialized='view') }}

SELECT 
    id AS cpu_model_id, 
    name AS cpu_model_name, 
    brand_id, 
    is_active, 
    created_on, 
    changed_on, 
    elton_created_at, 
    COALESCE(changed_on, created_on, elton_created_at) AS record_updated_at
FROM {{ source('laplap', 'bronze_cpu_model_raw') }}
