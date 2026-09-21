
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select add_compare_count
from `laplap-analytics`.`laplap_analytics`.`fct_product_engagement`
where add_compare_count is null



  
  
      
    ) dbt_internal_test