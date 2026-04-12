
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rating
from "ecom"."main_staging"."stg_sellers"
where rating is null



  
  
      
    ) dbt_internal_test