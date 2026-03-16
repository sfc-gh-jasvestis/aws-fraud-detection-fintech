-- =============================================================================
-- Phase 5: Feature Engineering (Dynamic Tables, SQL + Snowpark)
-- File: 06_features.sql
-- Schema: CRYPTO_SURVEILLANCE.FEATURES
-- All features computed as Dynamic Tables so they refresh automatically
-- as HARMONISED layer updates.
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_TRANSFORM;

-- ─── FEATURES.TRADE_FEATURES ──────────────────────────────────────────────────
-- Rolling window features per account for fraud/wash-trade detection.
-- Lookback windows: 1h, 4h, 24h, 7d
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.FEATURES.TRADE_FEATURES
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = WH_TRANSFORM
    COMMENT    = 'Per-account rolling trade features for ML fraud/AML scoring'
AS
WITH base AS (
    SELECT
        account_id,
        wallet_id,
        venue,
        trading_pair,
        base_asset,
        side,
        price,
        quantity,
        quote_qty,
        fee,
        is_maker,
        trade_ts,
        trade_date
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    WHERE trade_ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
),
windows AS (
    SELECT
        account_id,
        wallet_id,
        venue,
        trade_ts,
        trading_pair,

        -- ── 1-hour window ────────────────────────────────────────────────────
        COUNT(*)            OVER w1h AS trade_count_1h,
        SUM(quote_qty)      OVER w1h AS volume_usd_1h,
        AVG(price)          OVER w1h AS avg_price_1h,
        STDDEV(price)       OVER w1h AS price_stddev_1h,
        SUM(CASE WHEN side = 'BUY'  THEN quote_qty ELSE 0 END) OVER w1h AS buy_vol_1h,
        SUM(CASE WHEN side = 'SELL' THEN quote_qty ELSE 0 END) OVER w1h AS sell_vol_1h,

        -- ── 4-hour window ────────────────────────────────────────────────────
        COUNT(*)            OVER w4h AS trade_count_4h,
        SUM(quote_qty)      OVER w4h AS volume_usd_4h,
        AVG(price)          OVER w4h AS avg_price_4h,
        STDDEV(price)       OVER w4h AS price_stddev_4h,

        -- ── 24-hour window ───────────────────────────────────────────────────
        COUNT(*)            OVER w24h AS trade_count_24h,
        SUM(quote_qty)      OVER w24h AS volume_usd_24h,
        AVG(price)          OVER w24h AS avg_price_24h,
        MAX(price)          OVER w24h AS price_max_24h,
        MIN(price)          OVER w24h AS price_min_24h,
        SUM(CASE WHEN side = 'BUY'  THEN quote_qty ELSE 0 END) OVER w24h AS buy_vol_24h,
        SUM(CASE WHEN side = 'SELL' THEN quote_qty ELSE 0 END) OVER w24h AS sell_vol_24h,
        COUNT(DISTINCT trading_pair) OVER w24h AS unique_pairs_24h,
        COUNT(DISTINCT venue)        OVER w24h AS unique_venues_24h,
        SUM(CASE WHEN is_maker THEN 1 ELSE 0 END) OVER w24h AS maker_count_24h,

        -- ── 7-day window ─────────────────────────────────────────────────────
        COUNT(*)            OVER w7d AS trade_count_7d,
        SUM(quote_qty)      OVER w7d AS volume_usd_7d,
        AVG(quote_qty)      OVER w7d AS avg_trade_size_7d,
        STDDEV(quote_qty)   OVER w7d AS trade_size_stddev_7d

    FROM base
    WINDOW
        w1h  AS (PARTITION BY account_id, trading_pair
                 ORDER BY trade_ts
                 RANGE BETWEEN INTERVAL '1 hour'  PRECEDING AND CURRENT ROW),
        w4h  AS (PARTITION BY account_id, trading_pair
                 ORDER BY trade_ts
                 RANGE BETWEEN INTERVAL '4 hours' PRECEDING AND CURRENT ROW),
        w24h AS (PARTITION BY account_id, trading_pair
                 ORDER BY trade_ts
                 RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW),
        w7d  AS (PARTITION BY account_id
                 ORDER BY trade_ts
                 RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW)
),
-- Wash trade: self-join trades to find rapid opposing pairs within 10 min
-- Fixed: (a) no FILTER+OVER (unsupported in Snowflake), (b) no EXTRACT(EPOCH),
--        (c) self-join on TRADES only — not a TRADES×ORDERS cross-join.
wash_pairs AS (
    SELECT DISTINCT
        t1.account_id,
        t1.trade_ts                         AS t1_ts,
        t1.trading_pair
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES t1
    JOIN CRYPTO_SURVEILLANCE.HARMONISED.TRADES t2
        ON  t1.account_id                       = t2.account_id
        AND t1.trading_pair                     = t2.trading_pair
        AND t1.trade_id                        <> t2.trade_id
        AND t1.side                            <> t2.side
        AND ABS(DATEDIFF('second', t1.trade_ts, t2.trade_ts)) < 600
        AND ABS(t2.price - t1.price) / NULLIF(t1.price, 0)    < 0.005
    WHERE t1.trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
-- Cancelled-order rate: use ORDERS table directly, no join to TRADES
cancelled_orders AS (
    SELECT
        account_id,
        event_ts                             AS order_ts,
        SUM(CASE WHEN state IN ('CANCELLED','EXPIRED') THEN 1 ELSE 0 END)
            OVER (PARTITION BY account_id
                  ORDER BY event_ts
                  RANGE BETWEEN INTERVAL '1 HOUR' PRECEDING AND CURRENT ROW)
            AS cancelled_orders_1h
    FROM CRYPTO_SURVEILLANCE.HARMONISED.ORDERS
    WHERE event_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
wash_indicators AS (
    SELECT
        t.account_id,
        t.wallet_id,
        t.trade_ts,
        t.trading_pair,
        -- Count wash pairs touching this trade's account+pair in the prior 10 rows
        COUNT(wp.t1_ts)
            OVER (PARTITION BY t.account_id, t.trading_pair
                  ORDER BY t.trade_ts
                  ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS wash_trade_signal,
        -- Latest cancelled-order count for this account at this trade timestamp
        COALESCE(
            LAST_VALUE(co.cancelled_orders_1h IGNORE NULLS)
                OVER (PARTITION BY t.account_id ORDER BY t.trade_ts
                      ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),
            0
        )                                    AS cancelled_orders_1h
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES t
    LEFT JOIN wash_pairs wp
        ON  t.account_id    = wp.account_id
        AND t.trading_pair  = wp.trading_pair
        AND t.trade_ts      = wp.t1_ts
    LEFT JOIN cancelled_orders co
        ON  t.account_id    = co.account_id
        AND t.trade_ts      >= co.order_ts
        AND t.trade_ts      <= DATEADD('hour', 1, co.order_ts)
    WHERE t.trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT
    w.account_id,
    w.wallet_id,
    w.venue,
    w.trade_ts,
    w.trading_pair,
    -- Rolling volumes
    w.trade_count_1h,    w.volume_usd_1h,   w.avg_price_1h,   w.price_stddev_1h,
    w.buy_vol_1h,        w.sell_vol_1h,
    w.trade_count_4h,    w.volume_usd_4h,   w.avg_price_4h,   w.price_stddev_4h,
    w.trade_count_24h,   w.volume_usd_24h,  w.avg_price_24h,
    w.price_max_24h,     w.price_min_24h,
    w.buy_vol_24h,       w.sell_vol_24h,
    w.unique_pairs_24h,  w.unique_venues_24h, w.maker_count_24h,
    w.trade_count_7d,    w.volume_usd_7d,   w.avg_trade_size_7d, w.trade_size_stddev_7d,
    -- Derived features
    COALESCE(w.buy_vol_1h  / NULLIF(w.sell_vol_1h, 0), 0)  AS buy_sell_ratio_1h,
    COALESCE(w.buy_vol_24h / NULLIF(w.sell_vol_24h, 0), 0) AS buy_sell_ratio_24h,
    COALESCE(w.price_max_24h - w.price_min_24h, 0) /
        NULLIF(w.avg_price_24h, 0)                          AS price_range_pct_24h,
    COALESCE(w.volume_usd_1h / NULLIF(w.volume_usd_7d / 168, 0), 0)
                                                             AS volume_spike_ratio_1h,
    COALESCE(w.maker_count_24h / NULLIF(w.trade_count_24h, 0), 0)
                                                             AS maker_ratio_24h,
    -- Wash trade signals
    COALESCE(wi.wash_trade_signal, 0)    AS wash_trade_signal,
    COALESCE(wi.cancelled_orders_1h, 0) AS cancelled_orders_1h,
    CURRENT_TIMESTAMP()                  AS feature_computed_at
FROM windows w
LEFT JOIN wash_indicators wi
    ON w.account_id   = wi.account_id
    AND w.trade_ts    = wi.trade_ts
    AND w.trading_pair = wi.trading_pair;

-- ─── FEATURES.ENTITY_FEATURES ────────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES
    TARGET_LAG = '10 minutes'
    WAREHOUSE  = WH_TRANSFORM
    COMMENT    = 'Per-entity aggregated risk features combining on-chain + CEX activity'
AS
WITH onchain_stats AS (
    SELECT
        w.owner_entity_id                             AS entity_id,
        COUNT(DISTINCT ot.tx_hash)                    AS onchain_tx_count_30d,
        SUM(ot.value_decimal)                         AS onchain_volume_30d,
        COUNT(DISTINCT ot.from_address)               AS unique_counterparties_30d,
        SUM(CASE WHEN ot.event_type = 'transfer' AND
                      ot.to_address IN (
                          SELECT wallet_address FROM CRYPTO_SURVEILLANCE.HARMONISED.WALLET
                          WHERE is_mixer = TRUE
                      ) THEN 1 ELSE 0 END)            AS mixer_interactions_30d,
        SUM(CASE WHEN ot.is_sanctioned_counterparty   THEN ot.value_decimal ELSE 0 END)
                                                      AS sanctioned_volume_30d,
        MAX(ot.risk_score)                            AS max_counterparty_risk_30d
    FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS ot
    JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET w
        ON (ot.from_address = w.wallet_address OR ot.to_address = w.wallet_address)
        AND ot.chain = w.chain
    WHERE ot.block_ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND w.owner_entity_id IS NOT NULL
    GROUP BY 1
),
cex_stats AS (
    SELECT
        account_id                                   AS entity_id,
        COUNT(*)                                     AS cex_trade_count_30d,
        SUM(quote_qty)                               AS cex_volume_usd_30d,
        COUNT(DISTINCT trading_pair)                 AS unique_pairs_30d,
        STDDEV(quote_qty)                            AS trade_size_volatility_30d,
        MAX(quote_qty)                               AS max_single_trade_30d,
        SUM(CASE WHEN DATEDIFF('hour',
                    LAG(trade_ts) OVER (PARTITION BY account_id ORDER BY trade_ts),
                    trade_ts) < 1 THEN 1 ELSE 0 END) AS burst_trade_count_30d
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    WHERE trade_ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY 1
)
SELECT
    e.entity_id,
    e.kyc_tier,
    e.aml_risk_rating,
    e.pep_flag,
    e.sanctions_flag,
    e.account_type,
    -- CEX features
    COALESCE(cs.cex_trade_count_30d, 0)      AS cex_trade_count_30d,
    COALESCE(cs.cex_volume_usd_30d, 0)       AS cex_volume_usd_30d,
    COALESCE(cs.unique_pairs_30d, 0)         AS unique_pairs_30d,
    COALESCE(cs.trade_size_volatility_30d,0) AS trade_size_volatility_30d,
    COALESCE(cs.max_single_trade_30d, 0)     AS max_single_trade_30d,
    COALESCE(cs.burst_trade_count_30d, 0)    AS burst_trade_count_30d,
    -- On-chain features
    COALESCE(oc.onchain_tx_count_30d, 0)     AS onchain_tx_count_30d,
    COALESCE(oc.onchain_volume_30d, 0)       AS onchain_volume_30d,
    COALESCE(oc.unique_counterparties_30d,0) AS unique_counterparties_30d,
    COALESCE(oc.mixer_interactions_30d, 0)   AS mixer_interactions_30d,
    COALESCE(oc.sanctioned_volume_30d, 0)    AS sanctioned_volume_30d,
    COALESCE(oc.max_counterparty_risk_30d,0) AS max_counterparty_risk_30d,
    -- Derived risk signals
    CASE WHEN COALESCE(oc.mixer_interactions_30d, 0)  > 0  THEN TRUE ELSE FALSE END
                                             AS has_mixer_exposure,
    CASE WHEN COALESCE(oc.sanctioned_volume_30d, 0)   > 0  THEN TRUE ELSE FALSE END
                                             AS has_sanctioned_exposure,
    CASE WHEN COALESCE(cs.burst_trade_count_30d, 0)   > 20 THEN TRUE ELSE FALSE END
                                             AS has_burst_behaviour,
    -- Entity count of wallets owned (graph depth)
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.WALLET
     WHERE owner_entity_id = e.entity_id)   AS wallet_count,
    CURRENT_TIMESTAMP()                      AS feature_computed_at
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY e
LEFT JOIN cex_stats    cs ON e.entity_id = cs.entity_id
LEFT JOIN onchain_stats oc ON e.entity_id = oc.entity_id;

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.FEATURES TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.FEATURES TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.FEATURES TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA CRYPTO_SURVEILLANCE.FEATURES TO ROLE SURVEILLANCE_ANALYST;
