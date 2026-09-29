{{ config(
    materialized='table'
) }}

SELECT
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
    last_updated
FROM {{ ref('stg_crypto_prices') }}
WHERE current_price IS NOT NULL
  AND current_price > 0
  AND last_updated IS NOT NULL
