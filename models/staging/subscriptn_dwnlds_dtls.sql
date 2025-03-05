/*Find the total number of downloads for paying and non-paying users by date. 
Include only records where non-paying customers have more downloads than paying customers. 
The output should be sorted by earliest date first and contain 3 columns date, non-paying downloads, paying downloads. 
By solving this, you'll learn how to use join, groupby and having.
*/

with  user_details as (
    select 
        user_info.user_id as user,
        user_downloads.date as date,
        account_info.paying_customer as customers_type,
        user_downloads.downloads  as number_of_downloads
    from {{ ref('ms_user_dimension') }} as user_info
    inner join {{ref('ms_account_dimension')}} as account_info 
    on user_info.acc_id = account_info.acc_id
    inner  join {{ref('ms_download_facts')}} as user_downloads
    on user_info.user_id = user_downloads.user_id
    --group by user,account_info.paying_customer,user_downloads.date
)

select date,
    sum(case 
            when customers_type = 'Yes' THEN number_of_downloads else 0 end) as paying_downloads,
    sum(case    
            When customers_type = 'No' THEN number_of_downloads else 0 end) as non_paying_downloads
 from user_details
 group by date 
 having sum(case    
            When customers_type = 'No' THEN number_of_downloads else 0 end) > sum(case 
            when customers_type = 'Yes' THEN number_of_downloads else 0 end) 
order by date