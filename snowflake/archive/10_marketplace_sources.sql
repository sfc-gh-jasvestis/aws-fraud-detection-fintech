-- =============================================================================
-- Marketplace Sources Setup
-- File: sql/setup/10_marketplace_sources.sql
-- Pre-requisites:
--   - 00_setup.sql already executed (DB + roles exist)
--   - Marketplace listings are OPTIONAL — this script works without them
--
-- This script creates the EXTERNAL schema and a set of normalised views that
-- abstract over provider schemas. The rest of the platform always references
-- these views — never the provider tables directly. This means:
--   a) Switching providers = update one view definition.
--   b) Falling back to synthetic data = the view exists regardless.
--
-- DEFAULT MODE (demo): All views point to synthetic data only.
-- PRODUCTION MODE: Uncomment the Marketplace UNION ALL blocks in Section B
--                  after subscribing to the relevant listings.
--
-- Feature flag: USE_MARKETPLACE_DATA (config/settings.py)
--   false → All views return synthetic data (safe for demo)
--   true  → Uncomment Marketplace blocks below after subscribing
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_ANALYTICS;

-- ─── EXTERNAL schema ──────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS CRYPTO_SURVEILLANCE.EXTERNAL
    COMMENT = 'Normalised views over Snowflake Marketplace shared databases + synthetic fallbacks';

-- Grant Marketplace imported privileges to the surveillance role
-- Run once per shared database after subscribing:
-- GRANT IMPORTED PRIVILEGES ON DATABASE CYBERSYN__FINANCIAL_MARKET_DATA TO ROLE SURVEILLANCE_ADMIN;
-- GRANT IMPORTED PRIVILEGES ON DATABASE ALLIUM__ONCHAIN_ANALYTICS        TO ROLE SURVEILLANCE_ADMIN;
-- GRANT IMPORTED PRIVILEGES ON DATABASE TRM__CRYPTO_RISK_INTELLIGENCE     TO ROLE SURVEILLANCE_ADMIN;
-- GRANT IMPORTED PRIVILEGES ON DATABASE DOW_JONES__RISK_COMPLIANCE        TO ROLE SURVEILLANCE_ADMIN;


-- =============================================================================
-- SECTION A: Synthetic Fallback Views (always present; no Marketplace needed)
-- =============================================================================

-- ─── VW_SYNTHETIC_PRICES ─────────────────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_SYNTHETIC_PRICES AS
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

-- ─── VW_SYNTHETIC_WALLET_LABELS ───────────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_SYNTHETIC_WALLET_LABELS AS
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


-- =============================================================================
-- SECTION B: Unified Views (synthetic by default; Marketplace blocks commented)
-- To enable Marketplace data: uncomment the relevant UNION ALL block in each
-- view AFTER subscribing to the listing and granting IMPORTED PRIVILEGES.
-- =============================================================================

-- ─── VW_MARKET_PRICES ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_MARKET_PRICES AS
SELECT
    asset_ticker,
    trading_pair,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume_base,
    volume_quote_usd,
    (high_price - low_price) / NULLIF(close_price, 0) AS daily_range_pct,
    NULL::FLOAT                             AS daily_return,
    NULL::FLOAT                             AS rolling_30d_volatility,
    data_source
FROM CRYPTO_SURVEILLANCE.EXTERNAL.VW_SYNTHETIC_PRICES

-- ── To enable Cybersyn Marketplace data, uncomment the block below: ──────
-- UNION ALL
-- SELECT
--     a.TICKER                                AS asset_ticker,
--     CASE
--         WHEN a.TICKER IN ('BTC','WBTC')     THEN 'BTC-USDT'
--         WHEN a.TICKER IN ('ETH','WETH')     THEN 'ETH-USDT'
--         WHEN a.TICKER = 'SOL'              THEN 'SOL-USDT'
--         WHEN a.TICKER = 'BNB'              THEN 'BNB-USDT'
--         WHEN a.TICKER = 'XRP'              THEN 'XRP-USDT'
--         WHEN a.TICKER = 'MATIC'            THEN 'MATIC-USDT'
--         WHEN a.TICKER = 'AVAX'             THEN 'AVAX-USDT'
--         WHEN a.TICKER = 'LINK'             THEN 'LINK-USDT'
--         ELSE a.TICKER || '-USDT'
--     END                                     AS trading_pair,
--     a.DATE                                  AS price_date,
--     a.OPEN                                  AS open_price,
--     a.HIGH                                  AS high_price,
--     a.LOW                                   AS low_price,
--     a.CLOSE                                 AS close_price,
--     a.VOLUME                                AS volume_base,
--     a.CLOSE * a.VOLUME                      AS volume_quote_usd,
--     (a.HIGH - a.LOW) / NULLIF(a.CLOSE, 0)  AS daily_range_pct,
--     (a.CLOSE - LAG(a.CLOSE) OVER (PARTITION BY a.TICKER ORDER BY a.DATE))
--         / NULLIF(LAG(a.CLOSE) OVER (PARTITION BY a.TICKER ORDER BY a.DATE), 0)
--                                             AS daily_return,
--     STDDEV(a.CLOSE) OVER (
--         PARTITION BY a.TICKER
--         ORDER BY a.DATE
--         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
--     )                                       AS rolling_30d_volatility,
--     'CYBERSYN'                              AS data_source
-- FROM CYBERSYN__FINANCIAL_MARKET_DATA.CYBERSYN.ASSET_OHLCV_TIMESERIES a
-- WHERE a.DATE >= DATEADD('day', -365, CURRENT_DATE())
--   AND a.TICKER IN ('BTC','ETH','SOL','BNB','XRP','MATIC','AVAX','LINK','DOT','ADA',
--                    'DOGE','LTC','UNI','AAVE','CRV','WBTC','WETH','USDT','USDC')
;

-- ─── VW_ONCHAIN_LABELS ────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_ONCHAIN_LABELS AS
SELECT
    wallet_address,
    chain,
    label_type,
    label_type                              AS label_subtype,
    label_name,
    risk_score,
    1.0                                     AS confidence,
    CASE WHEN data_source = 'SYNTHETIC' THEN 'SYNTHETIC_FALLBACK' ELSE data_source END AS data_source
FROM CRYPTO_SURVEILLANCE.EXTERNAL.VW_SYNTHETIC_WALLET_LABELS

-- ── To enable Allium Marketplace data, uncomment the block below: ────────
-- UNION ALL
-- SELECT
--     LOWER(l.ADDRESS)                        AS wallet_address,
--     LOWER(l.CHAIN)                          AS chain,
--     l.LABEL_TYPE                            AS label_type,
--     COALESCE(l.LABEL_SUBTYPE, l.LABEL_TYPE) AS label_subtype,
--     l.LABEL_NAME                            AS label_name,
--     CASE l.LABEL_TYPE
--         WHEN 'sanctions'    THEN 95.0
--         WHEN 'mixer'        THEN 85.0
--         WHEN 'scam'         THEN 75.0
--         WHEN 'hack'         THEN 80.0
--         WHEN 'exchange'     THEN 5.0
--         WHEN 'defi'         THEN 10.0
--         ELSE 20.0
--     END                                     AS risk_score,
--     COALESCE(l.CONFIDENCE, 1.0)             AS confidence,
--     'ALLIUM'                                AS data_source
-- FROM ALLIUM__ONCHAIN_ANALYTICS.ETHEREUM.ADDRESS_LABELS l
-- WHERE l.CHAIN IN ('ethereum','polygon','arbitrum','optimism','solana','bitcoin')
;

-- ─── VW_WALLET_RISK_SCORES ────────────────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_WALLET_RISK_SCORES AS
SELECT
    wallet_address,
    chain,
    CASE WHEN is_sanctioned THEN 95.0 WHEN is_mixer THEN 80.0 ELSE 0.0 END AS risk_score,
    CASE WHEN is_sanctioned THEN 'SANCTIONS'
         WHEN is_mixer      THEN 'MIXER' ELSE 'UNKNOWN' END AS risk_category,
    CURRENT_TIMESTAMP()                     AS last_updated_ts,
    is_sanctioned,
    'SYNTHETIC_FALLBACK'                    AS data_source
FROM CRYPTO_SURVEILLANCE.HARMONISED.WALLET
WHERE is_sanctioned = TRUE OR is_mixer = TRUE

-- ── To enable TRM Labs Marketplace data, uncomment the block below: ──────
-- UNION ALL
-- SELECT
--     LOWER(r.ADDRESS)                        AS wallet_address,
--     LOWER(r.CHAIN)                          AS chain,
--     r.RISK_SCORE                            AS risk_score,
--     r.RISK_CATEGORY                         AS risk_category,
--     r.LAST_UPDATED                          AS last_updated_ts,
--     TRUE                                    AS is_sanctioned,
--     'TRM_LABS'                              AS data_source
-- FROM TRM__CRYPTO_RISK_INTELLIGENCE.PUBLIC.WALLET_RISK_SCORES r
-- WHERE r.RISK_SCORE > 50
;

-- ─── VW_SANCTIONS_LIST ────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_SANCTIONS_LIST AS
SELECT
    'Synthetic Sanctioned Entity - Demo'    AS sanctioned_entity_name,
    'OFAC_SDN_SYNTHETIC'                    AS list_name,
    'US'                                    AS jurisdiction,
    LOWER(wallet_address)                   AS wallet_address,
    CURRENT_TIMESTAMP()                     AS list_updated_at,
    'SYNTHETIC_FALLBACK'                    AS data_source
FROM CRYPTO_SURVEILLANCE.HARMONISED.WALLET
WHERE is_sanctioned = TRUE
  AND source_system = 'SYNTHETIC'

-- ── To enable Dow Jones Marketplace data, uncomment the block below: ─────
-- UNION ALL
-- SELECT
--     s.ENTITY_NAME                           AS sanctioned_entity_name,
--     s.LIST_NAME                             AS list_name,
--     s.JURISDICTION                          AS jurisdiction,
--     LOWER(s.WALLET_ADDRESS)                 AS wallet_address,
--     s.UPDATED_AT                            AS list_updated_at,
--     'DOW_JONES'                             AS data_source
-- FROM DOW_JONES__RISK_COMPLIANCE.PUBLIC.SANCTIONS_ENTITIES s
-- WHERE s.WALLET_ADDRESS IS NOT NULL
;


-- =============================================================================
-- SECTION C: Enrichment Views
-- =============================================================================

CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_ONCHAIN_TRANSFERS_ENRICHED AS
SELECT
    ot.event_id,
    ot.chain,
    ot.block_number,
    ot.block_ts,
    ot.tx_hash,
    ot.from_address,
    ot.to_address,
    ot.value_decimal,
    ot.token_symbol,
    ot.event_type,
    fl.label_type                           AS from_label_type,
    fl.label_name                           AS from_label_name,
    fl.risk_score                           AS from_risk_score,
    tl.label_type                           AS to_label_type,
    tl.label_name                           AS to_label_name,
    tl.risk_score                           AS to_risk_score,
    fr.risk_score                           AS from_trm_risk,
    tr_.risk_score                          AS to_trm_risk,
    (sl_from.wallet_address IS NOT NULL
     OR sl_to.wallet_address IS NOT NULL)   AS is_sanctioned_counterparty,
    COALESCE(sl_from.list_name, sl_to.list_name)
                                            AS sanctions_list_hit,
    GREATEST(
        COALESCE(fl.risk_score, 0),
        COALESCE(tl.risk_score, 0),
        COALESCE(fr.risk_score, 0),
        COALESCE(tr_.risk_score, 0)
    )                                       AS max_risk_score,
    ot.raw_record_id
FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS ot
LEFT JOIN CRYPTO_SURVEILLANCE.EXTERNAL.VW_ONCHAIN_LABELS fl
    ON ot.from_address = fl.wallet_address AND ot.chain = fl.chain
LEFT JOIN CRYPTO_SURVEILLANCE.EXTERNAL.VW_ONCHAIN_LABELS tl
    ON ot.to_address = tl.wallet_address AND ot.chain = tl.chain
LEFT JOIN CRYPTO_SURVEILLANCE.EXTERNAL.VW_WALLET_RISK_SCORES fr
    ON ot.from_address = fr.wallet_address AND ot.chain = fr.chain
LEFT JOIN CRYPTO_SURVEILLANCE.EXTERNAL.VW_WALLET_RISK_SCORES tr_
    ON ot.to_address   = tr_.wallet_address AND ot.chain = tr_.chain
LEFT JOIN CRYPTO_SURVEILLANCE.EXTERNAL.VW_SANCTIONS_LIST sl_from
    ON ot.from_address = sl_from.wallet_address
LEFT JOIN CRYPTO_SURVEILLANCE.EXTERNAL.VW_SANCTIONS_LIST sl_to
    ON ot.to_address   = sl_to.wallet_address;

CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.EXTERNAL.VW_TRADES_WITH_REFERENCE_PRICE AS
SELECT
    t.trade_id,
    t.account_id,
    t.wallet_id,
    t.venue,
    t.trading_pair,
    t.side,
    t.price             AS executed_price,
    t.quantity,
    t.quote_qty,
    t.trade_ts,
    t.trade_date,
    p.close_price                                   AS reference_close_price,
    p.daily_range_pct                               AS market_daily_range_pct,
    p.rolling_30d_volatility                        AS market_30d_volatility,
    (t.price - p.close_price) / NULLIF(p.close_price, 0)
                                                    AS price_deviation_pct,
    ABS((t.price - p.close_price) / NULLIF(p.close_price, 0))
                                                    AS abs_price_deviation_pct,
    CASE WHEN p.rolling_30d_volatility > 0
              AND ABS(t.price - p.close_price) > 2 * p.rolling_30d_volatility
         THEN TRUE ELSE FALSE END                   AS is_price_outlier,
    COALESCE(p.data_source, 'NO_PRICE_DATA')        AS price_data_source
FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES t
LEFT JOIN CRYPTO_SURVEILLANCE.EXTERNAL.VW_MARKET_PRICES p
    ON t.trading_pair = p.trading_pair
    AND t.trade_date  = p.price_date;

-- ─── Enrichment procedure ───────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE CRYPTO_SURVEILLANCE.EXTERNAL.SP_ENRICH_ONCHAIN_RISK()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    UPDATE CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS AS ot
    SET
        risk_score                 = e.max_risk_score,
        is_sanctioned_counterparty = e.is_sanctioned_counterparty
    FROM CRYPTO_SURVEILLANCE.EXTERNAL.VW_ONCHAIN_TRANSFERS_ENRICHED e
    WHERE ot.event_id = e.event_id
      AND (ot.risk_score IS NULL OR ot.is_sanctioned_counterparty IS NULL);

    RETURN 'Enrichment complete: ' || SQLROWCOUNT || ' rows updated';
END;
$$;

CREATE TASK IF NOT EXISTS CRYPTO_SURVEILLANCE.EXTERNAL.TASK_ENRICH_ONCHAIN
    WAREHOUSE = WH_ANALYTICS
    SCHEDULE  = '1 HOUR'
    COMMENT   = 'Enriches ONCHAIN_TRANSFERS with Marketplace risk labels hourly'
AS
    CALL CRYPTO_SURVEILLANCE.EXTERNAL.SP_ENRICH_ONCHAIN_RISK();

-- Task starts SUSPENDED; resume only after subscribing to Marketplace listings:
-- ALTER TASK CRYPTO_SURVEILLANCE.EXTERNAL.TASK_ENRICH_ONCHAIN RESUME;

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT USAGE  ON SCHEMA CRYPTO_SURVEILLANCE.EXTERNAL TO ROLE SURVEILLANCE_ANALYST;
GRANT USAGE  ON SCHEMA CRYPTO_SURVEILLANCE.EXTERNAL TO ROLE SURVEILLANCE_ML;
GRANT USAGE  ON SCHEMA CRYPTO_SURVEILLANCE.EXTERNAL TO ROLE SURVEILLANCE_ADMIN;

GRANT SELECT ON ALL VIEWS IN SCHEMA CRYPTO_SURVEILLANCE.EXTERNAL TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA CRYPTO_SURVEILLANCE.EXTERNAL TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CRYPTO_SURVEILLANCE.EXTERNAL TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CRYPTO_SURVEILLANCE.EXTERNAL TO ROLE SURVEILLANCE_ML;

-- ─── Activation checklist (for Marketplace enablement) ─────────────────────
--
--  1. Subscribe to Marketplace listings (docs/marketplace_sources.md)
--  2. Grant IMPORTED PRIVILEGES on each shared DB:
--       GRANT IMPORTED PRIVILEGES ON DATABASE CYBERSYN__FINANCIAL_MARKET_DATA TO ROLE SURVEILLANCE_ADMIN;
--       GRANT IMPORTED PRIVILEGES ON DATABASE ALLIUM__ONCHAIN_ANALYTICS        TO ROLE SURVEILLANCE_ADMIN;
--       GRANT IMPORTED PRIVILEGES ON DATABASE TRM__CRYPTO_RISK_INTELLIGENCE     TO ROLE SURVEILLANCE_ADMIN;
--       GRANT IMPORTED PRIVILEGES ON DATABASE DOW_JONES__RISK_COMPLIANCE        TO ROLE SURVEILLANCE_ADMIN;
--  3. Uncomment the UNION ALL blocks in Section B above for each subscribed DB
--  4. Re-run this script
--  5. Update .env: SURV_USE_MARKETPLACE_DATA=true
--  6. Run enrichment backfill: CALL CRYPTO_SURVEILLANCE.EXTERNAL.SP_ENRICH_ONCHAIN_RISK();
--  7. Resume enrichment task: ALTER TASK CRYPTO_SURVEILLANCE.EXTERNAL.TASK_ENRICH_ONCHAIN RESUME;
--  8. Refresh Dynamic Tables: ALTER DYNAMIC TABLE CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES REFRESH;
