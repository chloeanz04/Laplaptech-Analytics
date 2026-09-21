

SELECT 
    id AS gpu_model_id, 
    name AS gpu_model_name, 
    brand_id, 
    is_active, 
    created_on, 
    changed_on, 
    elton_created_at,
    COALESCE(changed_on, created_on, elton_created_at) AS record_updated_at
FROM `laplap-analytics`.`laplap_analytics`.`bronze_gpu_model_raw`