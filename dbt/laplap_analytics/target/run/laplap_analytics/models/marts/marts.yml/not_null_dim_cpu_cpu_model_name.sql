
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cpu_model_name
from `laplap-analytics`.`laplap_analytics`.`dim_cpu`
where cpu_model_name is null



  
  
      
    ) dbt_internal_test