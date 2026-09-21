
    
    

with dbt_test__target as (

  select cpu_model_id as unique_field
  from `laplap-analytics`.`laplap_analytics`.`dim_cpu`
  where cpu_model_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


