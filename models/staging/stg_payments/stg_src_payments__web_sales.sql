{{
    config(materialized = 'table')
}}

with 

source as (

    select * from {{ source('src_payments', 'web_sales') }} limit 500000

),

date_cte as (
    select * from {{ source("src_payments","date_dim")}} limit 500000
),

final as (

    select distinct
        YEAR(d_date) || '-'|| MONTH(d_date) as FY,
        SUBSTRING(d_quarter_name,5,2) as quarter,
        --ws_quantity as quantity,
        sum(ws_net_profit) over(partition by FY order by FY) as Monthly_Profit,
        sum(ws_net_profit) over(partition by quarter order by quarter) as Quarter_profit

    from source as sales 
    inner join date_cte as dy
    on sales.ws_sold_date_sk = dy.d_date_sk
    

)

select * from final
