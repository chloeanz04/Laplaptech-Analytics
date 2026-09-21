
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select laptop_id
from `laplap-analytics`.`laplap_analytics`.`stg_laptop_model`
where laptop_id is null



  
  
      
    ) dbt_internal_test