
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select brand_name
from `laplap-analytics`.`laplap_analytics`.`dim_brand`
where brand_name is null



  
  
      
    ) dbt_internal_test