
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select laptop_name
from `laplap-analytics`.`laplap_analytics`.`dim_laptop`
where laptop_name is null



  
  
      
    ) dbt_internal_test