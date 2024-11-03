with latest_partition as (
    select *
    from
        {{ ref('cumulative_price') }}
    qualify row_number() over (
        partition by data_id
        order by
            created_at_utc desc
    ) = 1
),

unnested as (
    select
        data_id,
        keys.value::date as created_at_utc,
        case
            when is_null_value(flattened_prices.value[keys.value]) then null
            else flattened_prices.value[keys.value]
        end as price
    from
        latest_partition,
        lateral flatten(input => date_prices) as flattened_prices,
        lateral flatten(input => object_keys(flattened_prices.value)) as keys
),

imputed as (
    select
        data_id,
        created_at_utc,
        price,
        coalesce(
            price,
            lag(price) ignore nulls over (
                partition by data_id
                order by
                    created_at_utc
            )
        ) as imputed_price
    from
        unnested
)

select * from imputed
