
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select view_count
from `laplap-analytics`.`laplap_analytics`.`fct_product_engagement`
where view_count is null



  
  
      
    ) dbt_internal_test