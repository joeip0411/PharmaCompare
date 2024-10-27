{{ config(
    materialized='incremental',
    unique_key=['data_id', 'created_at_utc']
) }}

with source as (
    select
        data_id,
        array_construct(object_construct(created_at_utc::varchar, price))
            as date_price
    from {{ ref('silver_price') }}
    where
        created_at_utc >= '{{ var("start_date") }}'
        and created_at_utc < '{{ var("end_date") }}'
),

history as (
    select
        data_id,
        date_prices,
        created_at_utc
    from {{ this }}
    where
        created_at_utc >= dateadd(day, -1, '{{ var("start_date") }}')
        and created_at_utc < dateadd(day, -1, '{{ var("end_date") }}')
),

combine as (
    select
        coalesce(s.data_id, h.data_id) as data_id,
        case
            when
                s.data_id is not null and h.data_id is not null
                then array_cat(s.date_price, h.date_prices)
            when s.data_id is not null and h.data_id is null then s.date_price
            else
                array_cat(
                    array_construct(
                        object_construct_keep_null(
                            dateadd(day, 1, h.created_at_utc)::date::varchar,
                            null
                        )
                    ),
                    h.date_prices
                )
        end as date_prices,
        date('{{ var("start_date") }}') as created_at_utc
    from source as s full outer join history as h on s.data_id = h.data_id

),

truncated as (
    select
        data_id,
        created_at_utc,
        case
            when array_size(date_prices) <= 365 then date_prices
            else array_slice(date_prices, 0, 365)
        end as date_prices
    from combine

)

select * from truncated
