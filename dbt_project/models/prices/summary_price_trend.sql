with price as (
    select *
    from
        chemist_warehouse.public.imputed_price
),

lagged as (
    select
        data_id,
        created_at_utc,
        imputed_price,
        lag(imputed_price) over (
            partition by data_id
            order by
                created_at_utc asc
        ) as prev_price,
        case
            when prev_price != imputed_price then 1
            else 0
        end as is_different
    from
        price
),

segmented as (
    select
        data_id,
        created_at_utc,
        imputed_price,
        sum(is_different) over (
            partition by data_id
            order by
                created_at_utc asc
        ) as segment
    from
        lagged
),

grouped as (
    select
        data_id,
        imputed_price as price,
        min(created_at_utc) as start_date_utc,
        max(created_at_utc) as end_date_utc,
        datediff(day, start_date_utc, end_date_utc) + 1 as duration_days
    from
        segmented
    group by
        data_id,
        imputed_price,
        segment
)

select *
from
    grouped
