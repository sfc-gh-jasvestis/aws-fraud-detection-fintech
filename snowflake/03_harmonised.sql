-- =============================================================================
-- Phase 3: Harmonised Layer — Dynamic Tables + Synthetic Reference Views
-- File: 03_harmonised.sql
-- Pattern: Dynamic Tables on RAW VARIANT sources → typed, normalised schema
--          Lag = '1 minute' for near-real-time refresh.
-- Includes: VW_SYNTHETIC_PRICES and VW_SYNTHETIC_WALLET_LABELS (folded from
--           10_marketplace_sources.sql for demo simplicity)
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_SURVEILLANCE;

-- ─── HARMONISED.TRADES ───────────────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_SURVEILLANCE
    COMMENT    = 'Normalised CEX trade executions; deduplicated by trade_id'
AS
SELECT DISTINCT
    PAYLOAD:trade_id::STRING                                  AS trade_id,
    PAYLOAD:order_id::STRING                                  AS order_id,
    PAYLOAD:account_id::STRING                                AS account_id,
    PAYLOAD:wallet_id::STRING                                 AS wallet_id,
    PAYLOAD:venue::STRING                                     AS venue,
    PAYLOAD:trading_pair::STRING                              AS trading_pair,
    PAYLOAD:base_asset::STRING                                AS base_asset,
    PAYLOAD:quote_asset::STRING                               AS quote_asset,
    PAYLOAD:side::STRING                                      AS side,
    PAYLOAD:price::FLOAT                                      AS price,
    PAYLOAD:quantity::FLOAT                                   AS quantity,
    PAYLOAD:quote_qty::FLOAT                                  AS quote_qty,
    PAYLOAD:fee::FLOAT                                        AS fee,
    PAYLOAD:fee_currency::STRING                              AS fee_currency,
    PAYLOAD:is_maker::BOOLEAN                                 AS is_maker,
    TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)           AS trade_ts,
    DATE_TRUNC('DAY', TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)) AS trade_date,
    _LOAD_TS                                                  AS ingest_ts,
    _SOURCE                                                   AS source_system,
    _RECORD_ID                                                AS raw_record_id
FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW
WHERE PAYLOAD:trade_id IS NOT NULL
  AND PAYLOAD:account_id IS NOT NULL;

-- ─── HARMONISED.ORDERS ───────────────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.ORDERS
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_SURVEILLANCE
    COMMENT    = 'Normalised CEX order lifecycle events; latest state per order_id'
AS
WITH ranked AS (
    SELECT
        PAYLOAD:order_id::STRING                                AS order_id,
        PAYLOAD:account_id::STRING                              AS account_id,
        PAYLOAD:wallet_id::STRING                               AS wallet_id,
        PAYLOAD:venue::STRING                                   AS venue,
        PAYLOAD:trading_pair::STRING                            AS trading_pair,
        PAYLOAD:side::STRING                                    AS side,
        PAYLOAD:order_type::STRING                              AS order_type,
        PAYLOAD:state::STRING                                   AS state,
        PAYLOAD:price::FLOAT                                    AS price,
        PAYLOAD:orig_qty::FLOAT                                 AS orig_qty,
        PAYLOAD:filled_qty::FLOAT                               AS filled_qty,
        COALESCE(PAYLOAD:filled_qty::FLOAT, 0) /
            NULLIF(PAYLOAD:orig_qty::FLOAT, 0)                  AS fill_ratio,
        TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)         AS event_ts,
        _LOAD_TS                                                AS ingest_ts,
        _RECORD_ID                                              AS raw_record_id,
        ROW_NUMBER() OVER (
            PARTITION BY PAYLOAD:order_id::STRING
            ORDER BY TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING) DESC
        )                                                       AS rn
    FROM CRYPTO_SURVEILLANCE.RAW.CEX_ORDERS_RAW
    WHERE PAYLOAD:order_id IS NOT NULL
)
SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1;

-- ─── HARMONISED.BALANCES_SNAPSHOT ────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.BALANCES_SNAPSHOT
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = WH_SURVEILLANCE
    COMMENT    = 'Latest balance per account + asset (point-in-time snapshot)'
AS
WITH ranked AS (
    SELECT
        PAYLOAD:account_id::STRING                             AS account_id,
        PAYLOAD:asset::STRING                                  AS asset,
        PAYLOAD:available::FLOAT                               AS available,
        PAYLOAD:locked::FLOAT                                  AS locked,
        COALESCE(PAYLOAD:available::FLOAT, 0) +
            COALESCE(PAYLOAD:locked::FLOAT, 0)                 AS total,
        PAYLOAD:change_type::STRING                            AS change_type,
        TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)        AS snapshot_ts,
        _LOAD_TS                                               AS ingest_ts,
        _RECORD_ID                                             AS raw_record_id,
        ROW_NUMBER() OVER (
            PARTITION BY PAYLOAD:account_id::STRING, PAYLOAD:asset::STRING
            ORDER BY TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING) DESC
        )                                                      AS rn
    FROM CRYPTO_SURVEILLANCE.RAW.CEX_BALANCES_RAW
    WHERE PAYLOAD:account_id IS NOT NULL
      AND PAYLOAD:asset IS NOT NULL
)
SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1;

-- ─── Forward-declare WALLET table (full DDL in 04_entity_graph.sql) ─────────
-- Required here because ONCHAIN_TRANSFERS DT LEFT JOINs to WALLET for risk tags.
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.WALLET (
    wallet_id           STRING        NOT NULL PRIMARY KEY,
    wallet_address      STRING,
    chain               STRING,
    owner_entity_id     STRING,
    wallet_type         STRING        DEFAULT 'UNKNOWN',
    is_exchange_wallet  BOOLEAN       DEFAULT FALSE,
    is_mixer            BOOLEAN       DEFAULT FALSE,
    is_sanctioned       BOOLEAN       DEFAULT FALSE,
    is_high_risk        BOOLEAN       DEFAULT FALSE,
    risk_score          FLOAT,
    label               STRING,
    cluster_id          STRING,
    first_seen_ts       TIMESTAMP_LTZ,
    last_seen_ts        TIMESTAMP_LTZ,
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system       STRING        DEFAULT 'ONCHAIN_INDEXER'
);

-- ─── HARMONISED.ONCHAIN_TRANSFERS ────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS
    TARGET_LAG = '2 minutes'
    WAREHOUSE  = WH_SURVEILLANCE
    COMMENT    = 'Normalised on-chain transfer events across supported chains'
AS
SELECT DISTINCT
    PAYLOAD:event_id::STRING                                   AS event_id,
    PAYLOAD:chain::STRING                                      AS chain,
    PAYLOAD:block_number::NUMBER                               AS block_number,
    TRY_TO_TIMESTAMP_LTZ(PAYLOAD:block_timestamp::STRING)      AS block_ts,
    PAYLOAD:tx_hash::STRING                                    AS tx_hash,
    LOWER(PAYLOAD:from_address::STRING)                        AS from_address,
    LOWER(PAYLOAD:to_address::STRING)                          AS to_address,
    PAYLOAD:value_raw::STRING                                  AS value_raw,
    PAYLOAD:value_decimal::FLOAT                               AS value_decimal,
    LOWER(PAYLOAD:token_address::STRING)                       AS token_address,
    PAYLOAD:token_symbol::STRING                               AS token_symbol,
    PAYLOAD:decimals::NUMBER                                   AS decimals,
    PAYLOAD:event_type::STRING                                 AS event_type,
    COALESCE(wf.risk_score, wt.risk_score)                     AS risk_score,
    CASE
        WHEN wf.is_sanctioned OR wt.is_sanctioned THEN 'SANCTIONS'
        WHEN wf.is_mixer OR wt.is_mixer           THEN 'MIXER'
        ELSE NULL
    END                                                        AS risk_category,
    COALESCE(wf.is_sanctioned, FALSE)
        OR COALESCE(wt.is_sanctioned, FALSE)                   AS is_sanctioned_counterparty,
    TRY_TO_TIMESTAMP_LTZ(PAYLOAD:ingested_at::STRING)          AS ingested_at,
    r._RECORD_ID                                               AS raw_record_id
FROM CRYPTO_SURVEILLANCE.RAW.ONCHAIN_EVENTS_RAW r
LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET wf
    ON LOWER(r.PAYLOAD:from_address::STRING) = wf.wallet_address
    AND r.PAYLOAD:chain::STRING = wf.chain
    AND (wf.is_sanctioned = TRUE OR wf.is_mixer = TRUE)
LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET wt
    ON LOWER(r.PAYLOAD:to_address::STRING) = wt.wallet_address
    AND r.PAYLOAD:chain::STRING = wt.chain
    AND (wt.is_sanctioned = TRUE OR wt.is_mixer = TRUE)
WHERE r.PAYLOAD:tx_hash IS NOT NULL
  AND r.PAYLOAD:chain   IS NOT NULL;

-- ─── Synthetic Reference Views (folded from 10_marketplace_sources.sql) ──────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_SYNTHETIC_PRICES AS
WITH daily_agg AS (
    SELECT
        base_asset                              AS asset_ticker,
        trading_pair,
        trade_date                              AS price_date,
        MIN(price)                              AS low_price,
        MAX(price)                              AS high_price,
        AVG(price)                              AS avg_price,
        SUM(quantity)                           AS volume_base,
        SUM(quote_qty)                          AS volume_quote_usd,
        COUNT(*)                                AS trade_count
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    WHERE trade_ts >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY base_asset, trading_pair, trade_date
)
SELECT
    asset_ticker,
    trading_pair,
    price_date,
    avg_price                                   AS open_price,
    high_price,
    low_price,
    avg_price                                   AS close_price,
    volume_base,
    volume_quote_usd,
    trade_count,
    'SYNTHETIC'                                 AS data_source
FROM daily_agg;

CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_SYNTHETIC_WALLET_LABELS AS
SELECT
    wallet_address,
    chain,
    CASE
        WHEN is_sanctioned THEN 'SANCTIONS'
        WHEN is_mixer      THEN 'MIXER'
        WHEN is_exchange_wallet THEN 'EXCHANGE'
        WHEN wallet_type = 'CONTRACT' THEN 'SMART_CONTRACT'
        ELSE 'UNKNOWN'
    END                                     AS label_type,
    COALESCE(label, wallet_type)            AS label_name,
    CASE WHEN is_sanctioned OR is_mixer THEN 80.0 ELSE 0.0 END AS risk_score,
    'SYNTHETIC'                             AS data_source
FROM CRYPTO_SURVEILLANCE.HARMONISED.WALLET;

-- ─── Marketplace Reference Prices (SNOWFLAKE_PUBLIC_DATA_FREE) ──────────────
-- Uses free Snowflake Marketplace data: BTC/ETH ETFs + crypto-related equities
-- Pivots EAV stock-price timeseries into OHLCV format matching VW_SYNTHETIC_PRICES.
-- Provides REAL reference prices for Pump & Dump / Cross-Exchange Arb detection.
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_PRICES AS
WITH pivoted AS (
    SELECT
        TICKER                                                      AS asset_ticker,
        TICKER || '/USD'                                            AS trading_pair,
        DATE                                                        AS price_date,
        MAX(CASE WHEN VARIABLE = 'pre-market_open' THEN VALUE END)  AS open_price,
        MAX(CASE WHEN VARIABLE = 'all-day_high'    THEN VALUE END)  AS high_price,
        MAX(CASE WHEN VARIABLE = 'all-day_low'     THEN VALUE END)  AS low_price,
        MAX(CASE WHEN VARIABLE = 'post-market_close' THEN VALUE END) AS close_price,
        MAX(CASE WHEN VARIABLE = 'nasdaq_volume'   THEN VALUE END)  AS volume_base,
        NULL::FLOAT                                                 AS volume_quote_usd,
        COUNT(*)                                                    AS trade_count,
        'SNOWFLAKE_PUBLIC_DATA_FREE'                                AS data_source
    FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
    WHERE TICKER IN ('BTC', 'ETH', 'GBTC', 'ETHE', 'COIN', 'BITO', 'MARA', 'RIOT', 'MSTR')
      AND VARIABLE IN ('pre-market_open', 'all-day_high', 'all-day_low',
                        'post-market_close', 'nasdaq_volume')
    GROUP BY TICKER, DATE
)
SELECT * FROM pivoted
WHERE open_price IS NOT NULL AND close_price IS NOT NULL;

-- ─── Marketplace FX Rates (SNOWFLAKE_PUBLIC_DATA_FREE) ──────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_FX_RATES AS
SELECT
    BASE_CURRENCY_ID,
    QUOTE_CURRENCY_ID,
    BASE_CURRENCY_NAME,
    QUOTE_CURRENCY_NAME,
    DATE                                     AS rate_date,
    VALUE                                    AS exchange_rate,
    'SNOWFLAKE_PUBLIC_DATA_FREE'             AS data_source
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
WHERE BASE_CURRENCY_ID = 'USD';

-- ─── Unified Price View (marketplace + synthetic fallback) ──────────────────
-- When marketplace data exists for a date, it takes priority over synthetic.
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKET_PRICES AS
SELECT * FROM CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_PRICES
UNION ALL
SELECT
    asset_ticker, trading_pair, price_date, open_price, high_price,
    low_price, close_price, volume_base, volume_quote_usd, trade_count,
    data_source
FROM CRYPTO_SURVEILLANCE.HARMONISED.VW_SYNTHETIC_PRICES sp
WHERE NOT EXISTS (
    SELECT 1 FROM CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_PRICES mp
    WHERE mp.asset_ticker = sp.asset_ticker AND mp.price_date = sp.price_date
);

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_SYNTHETIC_PRICES TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_SYNTHETIC_WALLET_LABELS TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_PRICES TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_FX_RATES TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKET_PRICES TO ROLE SURVEILLANCE_ANALYST;
