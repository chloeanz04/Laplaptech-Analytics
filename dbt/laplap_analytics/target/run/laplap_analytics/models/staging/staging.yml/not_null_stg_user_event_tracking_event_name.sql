
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_name
from `laplap-analytics`.`laplap_analytics`.`stg_user_event_tracking`
where event_name is null



  
  
      
    ) dbt_internal_test