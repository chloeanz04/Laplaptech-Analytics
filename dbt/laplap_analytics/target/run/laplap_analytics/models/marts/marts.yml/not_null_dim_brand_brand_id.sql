
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select brand_id
from `laplap-analytics`.`laplap_analytics`.`dim_brand`
where brand_id is null



  
  
      
    ) dbt_internal_test