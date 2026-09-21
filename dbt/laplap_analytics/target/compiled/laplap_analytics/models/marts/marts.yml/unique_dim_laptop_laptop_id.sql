
    
    

with dbt_test__target as (

  select laptop_id as unique_field
  from `laplap-analytics`.`laplap_analytics`.`dim_laptop`
  where laptop_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


