
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select comparison_device_count
from `laplap-analytics`.`laplap_analytics`.`fct_comparison_behavior`
where comparison_device_count is null



  
  
      
    ) dbt_internal_test