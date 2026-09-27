SELECT
    crypto_id,
    last_updated,
    COUNT(*) AS duplicate_count
FROM {{ ref('stg_crypto_prices') }}
GROUP BY
    crypto_id,
    last_updated
HAVING COUNT(*) > 1
