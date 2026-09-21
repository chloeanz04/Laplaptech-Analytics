
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select intent_rate
from `laplap-analytics`.`laplap_analytics`.`fct_product_engagement`
where intent_rate is null



  
  
      
    ) dbt_internal_test