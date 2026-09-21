

  create or replace view `laplap-analytics`.`laplap_analytics`.`int_laptop_model_latest`
  OPTIONS()
  as 

WITH ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY laptop_id ORDER BY record_updated_at DESC, elton_created_at DESC) AS rn,
    FROM `laplap-analytics`.`laplap_analytics`.`stg_laptop_model`
)

SELECT 
    * EXCEPT(rn)
FROM ranked
WHERE rn = 1;

