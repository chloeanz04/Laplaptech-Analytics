
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select gpu_model_id
from `laplap-analytics`.`laplap_analytics`.`dim_gpu`
where gpu_model_id is null



  
  
      
    ) dbt_internal_test