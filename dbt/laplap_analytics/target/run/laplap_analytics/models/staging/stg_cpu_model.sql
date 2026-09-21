

  create or replace view `laplap-analytics`.`laplap_analytics`.`stg_cpu_model`
  OPTIONS()
  as 

SELECT 
    id AS cpu_model_id, 
    name AS cpu_model_name, 
    brand_id, 
    is_active, 
    created_on, 
    changed_on, 
    elton_created_at, 
    COALESCE(changed_on, created_on, elton_created_at) AS record_updated_at
FROM `laplap-analytics`.`laplap_analytics`.`bronze_cpu_model_raw`;

