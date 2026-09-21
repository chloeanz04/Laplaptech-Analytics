
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select benchmark_result_id
from `laplap-analytics`.`laplap_analytics`.`dim_laptop_benchmark`
where benchmark_result_id is null



  
  
      
    ) dbt_internal_test