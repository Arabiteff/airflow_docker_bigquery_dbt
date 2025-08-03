select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select product_id
from `proud-life-467704-k6`.`retail`.`fct_invoices`
where product_id is null



      
    ) dbt_internal_test