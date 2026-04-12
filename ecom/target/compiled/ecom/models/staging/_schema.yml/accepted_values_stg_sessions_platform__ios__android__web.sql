
    
    

with all_values as (

    select
        platform as value_field,
        count(*) as n_records

    from "ecom"."main_staging"."stg_sessions"
    group by platform

)

select *
from all_values
where value_field not in (
    'ios','android','web'
)


