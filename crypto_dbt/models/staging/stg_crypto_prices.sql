{{ config(
    materialized='view'
) }}

with source_data as (

    select
        event_id,
        event_type,
        source,
        received_at,
        id as crypto_id,
        lower(symbol) as symbol,
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

    from {{ source("crypto_market", "crypto_prices") }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by crypto_id, last_updated
            order by received_at desc
        ) as row_num

    from source_data

)

select
    event_id,
    event_type,
    source,
    received_at,
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

from deduplicated

where row_num = 1