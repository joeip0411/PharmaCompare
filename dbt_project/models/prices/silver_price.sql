{{ config(
    materialized='view'
) }}

with source_data as (

    select
        data_id,
        price::decimal(12, 2) as price,
        created_at_utc::timestamp_ntz::date as created_at_utc
    from {{ source("chemistwarehouse", "bronze_price") }}

)

select * from source_data
