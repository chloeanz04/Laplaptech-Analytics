
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select comparison_type
from `laplap-analytics`.`laplap_analytics`.`fct_comparison_behavior`
where comparison_type is null



  
  
      
    ) dbt_internal_test