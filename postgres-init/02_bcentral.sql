-- Schema y tablas para el pipeline del Banco Central de Chile
CREATE SCHEMA IF NOT EXISTS bcentral;

CREATE TABLE IF NOT EXISTS bcentral.silver_indicators (
    id          SERIAL PRIMARY KEY,
    indicador   TEXT NOT NULL,
    serie_id    TEXT NOT NULL,
    fecha       DATE NOT NULL,
    valor       NUMERIC(18, 6) NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);