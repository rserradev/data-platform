-- Schema para el pipeline de criptomonedas
CREATE SCHEMA IF NOT EXISTS crypto;

-- Tabla bronze: datos crudos de la API
CREATE TABLE IF NOT EXISTS crypto.bronze_prices (
    id          SERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    coin_id     TEXT NOT NULL,
    raw_payload JSONB NOT NULL
);

-- Tabla silver: datos limpios y tipados
CREATE TABLE IF NOT EXISTS crypto.silver_prices (
    id               SERIAL PRIMARY KEY,
    coin_id          TEXT NOT NULL,
    symbol           TEXT NOT NULL,
    name             TEXT NOT NULL,
    price_usd        NUMERIC(18, 6) NOT NULL,
    market_cap_usd   NUMERIC(24, 2),
    volume_24h_usd   NUMERIC(24, 2),
    price_change_24h NUMERIC(10, 4),
    fetched_at       TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);