-- Este modelo toma los datos de silver y calcula
-- el precio promedio, mínimo y máximo por día

{{ config(materialized='table', schema='crypto') }}

SELECT
    DATE(fetched_at)                    AS price_date,
    coin_id,
    ROUND(AVG(price_usd)::NUMERIC, 2)   AS avg_price_usd,
    ROUND(MIN(price_usd)::NUMERIC, 2)   AS min_price_usd,
    ROUND(MAX(price_usd)::NUMERIC, 2)   AS max_price_usd,
    COUNT(*)                            AS data_points
FROM crypto.silver_prices
GROUP BY DATE(fetched_at), coin_id
ORDER BY price_date DESC