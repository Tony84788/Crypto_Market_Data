{{ config(
    materialized='table'
) }}

with ranked_prices as (

    select
        *,
        row_number() over (
            partition by crypto_id
            order by last_updated desc
        ) as row_num

    from {{ ref('stg_crypto_prices') }}

)

select
    crypto_id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    circulating_supply,
    total_supply,
    max_supply,
    ath,
    atl,
    last_updated

from ranked_prices

where row_num = 1
