SELECT
    crypto_id,
    current_price,
    market_cap,
    total_volume,
    market_cap_rank
FROM {{ ref('stg_crypto_prices') }}
WHERE current_price < 0
   OR market_cap < 0
   OR total_volume < 0
   OR market_cap_rank <= 0
