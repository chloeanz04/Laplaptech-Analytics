
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from `laplap-analytics`.`laplap_analytics`.`int_events_parsed`
where id is null



  
  
      
    ) dbt_internal_test