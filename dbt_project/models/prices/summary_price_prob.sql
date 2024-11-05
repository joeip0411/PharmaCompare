with price_trend as (
    select *
    from
        {{ ref('summary_price_trend') }}
),

price_prob as (
    select
        data_id,
        price,
        sum(duration_days) as total_days,
        ratio_to_report(total_days) over (partition by data_id)
        * 100 as price_prob
    from
        price_trend
    group by
        data_id,
        price
)

select *
from
    price_prob
