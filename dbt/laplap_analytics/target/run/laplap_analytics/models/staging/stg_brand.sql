

  create or replace view `laplap-analytics`.`laplap_analytics`.`stg_brand`
  OPTIONS()
  as 

SELECT 
    id AS brand_id,
    name AS brand_name,
    is_chip_brand,
    created_on,
    changed_on,
    elton_created_at,
    COALESCE(changed_on, created_on, elton_created_at) AS record_updated_at
FROM `laplap-analytics`.`laplap_analytics`.`bronze_brand_raw`;

