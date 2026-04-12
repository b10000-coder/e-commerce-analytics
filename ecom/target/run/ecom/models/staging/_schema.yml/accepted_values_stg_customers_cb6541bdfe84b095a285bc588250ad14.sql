
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        customer_tier as value_field,
        count(*) as n_records

    from "ecom"."main_staging"."stg_customers"
    group by customer_tier

)

select *
from all_values
where value_field not in (
    'bronze','silver','gold'
)



  
  
      
    ) dbt_internal_test