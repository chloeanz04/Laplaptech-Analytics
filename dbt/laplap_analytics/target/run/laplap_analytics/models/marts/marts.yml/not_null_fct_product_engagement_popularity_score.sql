
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select popularity_score
from `laplap-analytics`.`laplap_analytics`.`fct_product_engagement`
where popularity_score is null



  
  
      
    ) dbt_internal_test