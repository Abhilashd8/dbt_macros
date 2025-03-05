with 

customers_cte as (

    select * from {{ source('src_payments', 'customer') }}

),

customer_address_cte as (

    select * from {{ source('src_payments', 'customer_address') }}

),

customer_demographics_cte as (

    select * from {{ source('src_payments', 'customer_demographics') }}

),

customer_details as (

    select
    
        c_customer_id as customer_id,
        c_current_cdemo_sk ,
        cd_demo_sk,
        c_current_addr_sk,
        ca_address_sk,
        c_salutation || ' '|| c_first_name || ' '|| c_last_name as full_name,
        c_preferred_cust_flag,
        c_birth_day||'-'|| c_birth_month ||'-'||c_birth_year as Date_of_birth,
        c_birth_country as Country,
        c_email_address as email,
        ca_street_number as street_number,
        ca_street_name as street_name,
        ca_street_type as street_type,
        ca_suite_number as suite_number,
        ca_city as city,
        ca_state as state,
        ca_zip as zip_pin_code,
        ca_country as country,
        ca_location_type,
        cd_gender as gender,
        cd_marital_status,
        cd_education_status,
        cd_purchase_estimate,
        cd_credit_rating
    from customers_cte as customers
    join customer_address_cte as address
    on customers.c_current_addr_sk = address.ca_address_sk
    left join customer_demographics_cte demographics
    on customers.c_current_cdemo_sk = demographics.cd_demo_sk
)

select * from customer_details
