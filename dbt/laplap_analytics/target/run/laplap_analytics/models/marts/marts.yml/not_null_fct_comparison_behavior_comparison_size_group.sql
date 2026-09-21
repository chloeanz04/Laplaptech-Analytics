
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select comparison_size_group
from `laplap-analytics`.`laplap_analytics`.`fct_comparison_behavior`
where comparison_size_group is null



  
  
      
    ) dbt_internal_test