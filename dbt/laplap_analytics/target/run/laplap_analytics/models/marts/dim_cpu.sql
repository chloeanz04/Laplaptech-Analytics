
  
    

    create or replace table `laplap-analytics`.`laplap_analytics`.`dim_cpu`
      
    
    

    
    OPTIONS()
    as (
      

WITH ranked AS (
    SELECT
        cpu_model_id,
        cpu_model_name,
        brand_id,
        is_active,
        record_updated_at,
        elton_created_at,

        ROW_NUMBER() OVER (
            PARTITION BY cpu_model_id
            ORDER BY
                record_updated_at DESC,
                elton_created_at DESC
        ) AS rn

    FROM `laplap-analytics`.`laplap_analytics`.`stg_cpu_model`
)

SELECT
    cpu_model_id,
    cpu_model_name,
    brand_id,
    is_active

FROM ranked

WHERE rn = 1
    );
  