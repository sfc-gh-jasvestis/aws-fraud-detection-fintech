-- ============================================================================
-- 03b_marketplace_stub.sql
-- ----------------------------------------------------------------------------
-- DEPLOYED 2026-05-06: VW_MARKETPLACE_PRICES decoupled from the Marketplace
-- shared database (SNOWFLAKE_PUBLIC_DATA_FREE). The shared listing periodically
-- becomes "no longer available" which broke the VW_PUMP_AND_DUMP_CANDIDATES
-- chain.
--
-- This stub returns zero rows with the correct column shape, so VW_MARKET_PRICES
-- (which is MARKETPLACE_PRICES UNION ALL SYNTHETIC_PRICES) falls through to the
-- synthetic prices alone — preserving the demo narrative without the dependency.
-- ============================================================================

CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_PRICES (
    ASSET_TICKER, TRADING_PAIR, PRICE_DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
    CLOSE_PRICE, VOLUME_BASE, VOLUME_QUOTE_USD, TRADE_COUNT, DATA_SOURCE
) AS
SELECT
    NULL::STRING, NULL::STRING, NULL::DATE,
    NULL::FLOAT,  NULL::FLOAT,  NULL::FLOAT,  NULL::FLOAT,
    NULL::FLOAT,  NULL::FLOAT,  NULL::NUMBER, 'STUB'::STRING
WHERE FALSE;
