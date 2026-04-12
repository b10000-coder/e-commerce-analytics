
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from "ecom"."main_staging"."stg_orders"
    group by payment_method

)

select *
from all_values
where value_field not in (
    'credit_card','debit_card','bank_transfer','cash_on_delivery','wallet'
)


