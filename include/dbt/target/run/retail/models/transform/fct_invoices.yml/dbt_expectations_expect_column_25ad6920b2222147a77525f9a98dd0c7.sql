select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with relation_columns as (

        
        select
            cast('INVOICE_ID' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('DATETIME_ID' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('PRODUCT_ID' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('CUSTOMER_ID' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('QUANTITY' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('TOTAL' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'DATETIME_ID'
            and
            relation_column_type not in ('STRING')

    )
    select *
    from test_data
      
    ) dbt_internal_test