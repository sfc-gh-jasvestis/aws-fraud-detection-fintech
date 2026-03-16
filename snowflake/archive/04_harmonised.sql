-- =============================================================================
-- Phase 4: Harmonised Layer — Dynamic Tables
-- File: 04_harmonised.sql
-- Pattern: Dynamic Tables on RAW VARIANT sources → typed, normalised schema
--          Lag = '1 minute' for near-real-time refresh.
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_TRANSFORM;

-- ─── HARMONISED.TRADES ───────────────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    TARGET_LAG = '1 minute'
    WAREHOUSE  = WH_TRANSFORM
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
    WAREHOUSE  = WH_TRANSFORM
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
    WAREHOUSE  = WH_TRANSFORM
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

-- ─── HARMONISED.ONCHAIN_TRANSFERS ────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS
    TARGET_LAG = '2 minutes'
    WAREHOUSE  = WH_TRANSFORM
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

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.HARMONISED
    TO ROLE SURVEILLANCE_ML;
