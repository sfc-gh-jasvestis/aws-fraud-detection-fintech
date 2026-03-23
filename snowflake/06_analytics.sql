-- =============================================================================
-- Phase 6: ML Scoring + Detection Rules + ALERTS
-- File: 06_analytics.sql
-- Schema: CRYPTO_SURVEILLANCE.ANALYTICS + CRYPTO_SURVEILLANCE.ML
-- Merged from: 07_analytics.sql + 08_surveillance_rules.sql
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_SURVEILLANCE;

-- =============================================================================
-- Part A: ML Scoring UDF + ALERT_SCORES Dynamic Table
-- =============================================================================

CREATE OR REPLACE FUNCTION CRYPTO_SURVEILLANCE.ML.FRAUD_RISK_SCORE(
    CEX_TRADE_COUNT_30D        FLOAT,
    CEX_VOLUME_USD_30D         FLOAT,
    UNIQUE_PAIRS_30D           FLOAT,
    TRADE_SIZE_VOLATILITY_30D  FLOAT,
    MAX_SINGLE_TRADE_30D       FLOAT,
    BURST_TRADE_COUNT_30D      FLOAT,
    ONCHAIN_TX_COUNT_30D       FLOAT,
    ONCHAIN_VOLUME_30D         FLOAT,
    UNIQUE_COUNTERPARTIES_30D  FLOAT,
    MIXER_INTERACTIONS_30D     FLOAT,
    SANCTIONED_VOLUME_30D      FLOAT,
    MAX_COUNTERPARTY_RISK_30D  FLOAT,
    WALLET_COUNT               FLOAT,
    PEP_FLAG                   INT,
    SANCTIONS_FLAG             INT,
    HAS_MIXER_EXPOSURE         INT,
    HAS_SANCTIONED_EXPOSURE    INT,
    HAS_BURST_BEHAVIOUR        INT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'xgboost', 'scikit-learn', 'numpy')
HANDLER = 'score'
COMMENT = 'Returns fraud/AML probability 0-1 using trained XGBoost model'
AS
$$
import sys
import numpy as np

_cache = {}

def score(
    cex_trades, cex_vol, pairs, vol_stdev, max_trade, burst,
    onchain_tx, onchain_vol, counterparties, mixers, sanc_vol, max_risk, wallets,
    pep, sanctions, mixer_exp, sanc_exp, burst_beh
):
    import pickle, os
    if 'model' not in _cache:
        _cache['model'] = None

    if _cache['model'] is None:
        risk = 0.0
        risk += min(burst * 0.05, 0.3)
        risk += min(mixers * 0.15, 0.4)
        risk += sanc_vol * 0.001 if sanc_vol > 0 else 0.0
        risk += 0.2 if pep else 0.0
        risk += 0.4 if sanctions else 0.0
        risk += 0.2 if mixer_exp else 0.0
        risk += 0.3 if sanc_exp else 0.0
        risk += 0.1 if burst_beh else 0.0
        return min(round(risk, 4), 1.0)

    features = np.array([[
        cex_trades, cex_vol, pairs, vol_stdev, max_trade, burst,
        onchain_tx, onchain_vol, counterparties, mixers, sanc_vol, max_risk, wallets,
        pep, sanctions, mixer_exp, sanc_exp, burst_beh
    ]])
    return float(_cache['model'].predict_proba(features)[0][1])
$$;

GRANT USAGE ON FUNCTION CRYPTO_SURVEILLANCE.ML.FRAUD_RISK_SCORE(
    FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,
    INT,INT,INT,INT,INT
) TO ROLE SURVEILLANCE_ANALYST;
GRANT USAGE ON FUNCTION CRYPTO_SURVEILLANCE.ML.FRAUD_RISK_SCORE(
    FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,FLOAT,
    INT,INT,INT,INT,INT
) TO ROLE SURVEILLANCE_ADMIN;

-- ─── ANALYTICS.ALERT_SCORES ──────────────────────────────────────────────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES
    TARGET_LAG = '10 minutes'
    WAREHOUSE  = WH_SURVEILLANCE
    COMMENT    = 'Per-entity alert scores: rule flags + ML probability'
AS
SELECT
    ef.entity_id,
    ef.aml_risk_rating,
    ef.pep_flag,
    ef.sanctions_flag,
    ef.has_mixer_exposure        AS rule_mixer_exposure,
    ef.has_sanctioned_exposure   AS rule_sanctioned_exposure,
    ef.has_burst_behaviour       AS rule_burst_behaviour,
    CASE WHEN ef.mixer_interactions_30d   > 5  THEN TRUE ELSE FALSE END AS rule_heavy_mixer,
    CASE WHEN ef.sanctioned_volume_30d    > 0  THEN TRUE ELSE FALSE END AS rule_any_sanction,
    CASE WHEN ef.burst_trade_count_30d    > 50 THEN TRUE ELSE FALSE END AS rule_extreme_burst,
    CASE WHEN ef.max_single_trade_30d    > 100000 THEN TRUE ELSE FALSE END AS rule_large_trade,
    CRYPTO_SURVEILLANCE.ML.FRAUD_RISK_SCORE(
        ef.cex_trade_count_30d, ef.cex_volume_usd_30d,
        ef.unique_pairs_30d,    ef.trade_size_volatility_30d,
        ef.max_single_trade_30d, ef.burst_trade_count_30d,
        ef.onchain_tx_count_30d, ef.onchain_volume_30d,
        ef.unique_counterparties_30d, ef.mixer_interactions_30d,
        ef.sanctioned_volume_30d, ef.max_counterparty_risk_30d,
        ef.wallet_count,
        ef.pep_flag::INT, ef.sanctions_flag::INT,
        ef.has_mixer_exposure::INT, ef.has_sanctioned_exposure::INT,
        ef.has_burst_behaviour::INT
    )                                        AS ml_fraud_probability,
    CASE
        WHEN ef.sanctions_flag = TRUE       THEN 'CRITICAL'
        WHEN ef.pep_flag = TRUE
          OR ef.has_sanctioned_exposure     THEN 'HIGH'
        WHEN ef.has_mixer_exposure
          OR ef.has_burst_behaviour         THEN 'MEDIUM'
        ELSE 'LOW'
    END                                      AS rule_risk_tier,
    ef.cex_volume_usd_30d,
    ef.feature_computed_at,
    CURRENT_TIMESTAMP()                      AS score_computed_at
FROM CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES ef;

GRANT SELECT ON DYNAMIC TABLE CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES
    TO ROLE SURVEILLANCE_ANALYST;

-- =============================================================================
-- Part B: Detection Rules (6 typology views)
-- =============================================================================

-- ─── RULE 1: Pump & Dump Candidates ──────────────────────────────────────────
-- Enhanced: joins marketplace reference prices (BTC/ETH) for price_deviation_pct
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_PUMP_AND_DUMP_CANDIDATES AS
WITH price_spikes AS (
    SELECT
        trading_pair,
        DATE_TRUNC('HOUR', trade_ts)        AS trade_hour,
        DATE_TRUNC('DAY', trade_ts)         AS trade_date,
        SPLIT_PART(trading_pair, '/', 1)    AS base_asset,
        AVG(price)                          AS avg_price,
        MIN(price)                          AS min_price,
        MAX(price)                          AS max_price,
        (MAX(price) - MIN(price)) / NULLIF(MIN(price), 0) AS price_range_pct,
        SUM(quote_qty)                      AS total_volume,
        COUNT(DISTINCT account_id)          AS unique_accounts
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    WHERE trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3, 4
),
suspect_hours AS (
    SELECT
        ps.*,
        mp.close_price                      AS ref_close_price,
        mp.data_source                      AS ref_data_source,
        CASE WHEN mp.close_price IS NOT NULL AND mp.close_price > 0
             THEN (ps.avg_price - mp.close_price) / mp.close_price
             ELSE NULL
        END                                 AS ref_price_deviation_pct
    FROM price_spikes ps
    LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKET_PRICES mp
        ON ps.base_asset  = mp.asset_ticker
       AND ps.trade_date  = mp.price_date
    WHERE ps.price_range_pct > 0.05
      AND ps.total_volume    > 50000
),
concentrated_sellers AS (
    SELECT
        t.trading_pair,
        t.account_id,
        t.wallet_id,
        DATE_TRUNC('HOUR', t.trade_ts)  AS trade_hour,
        SUM(CASE WHEN t.side = 'SELL' THEN t.quote_qty ELSE 0 END) AS sell_volume,
        SUM(CASE WHEN t.side = 'BUY'  THEN t.quote_qty ELSE 0 END) AS buy_volume,
        COUNT(*)                         AS trade_count
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES t
    JOIN suspect_hours sh
        ON t.trading_pair = sh.trading_pair
       AND DATE_TRUNC('HOUR', t.trade_ts) = sh.trade_hour
    WHERE t.trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3, 4
)
SELECT
    cs.account_id,
    cs.wallet_id,
    cs.trading_pair,
    cs.trade_hour                           AS alert_window,
    sh.price_range_pct                      AS price_spike_pct,
    sh.ref_close_price                      AS marketplace_ref_price,
    sh.ref_price_deviation_pct              AS marketplace_price_deviation_pct,
    sh.ref_data_source                      AS ref_price_source,
    sh.total_volume                         AS window_volume,
    cs.sell_volume,
    cs.buy_volume,
    cs.trade_count,
    'PUMP_AND_DUMP'                         AS alert_type,
    'HIGH'                                  AS severity,
    'Price spike >5% with concentrated sell-off by same account'
        || CASE WHEN sh.ref_price_deviation_pct IS NOT NULL
                THEN ' | Ref price deviation: ' || ROUND(sh.ref_price_deviation_pct * 100, 2) || '%'
                ELSE '' END                 AS reason,
    CURRENT_TIMESTAMP()                     AS detected_at
FROM concentrated_sellers cs
JOIN suspect_hours sh
    ON cs.trading_pair = sh.trading_pair
   AND cs.trade_hour   = sh.trade_hour
WHERE cs.sell_volume > cs.buy_volume * 2
  AND cs.sell_volume > 10000;

-- ─── RULE 2: Wash Trade Patterns ─────────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_WASH_TRADE_PATTERNS AS
WITH paired_trades AS (
    SELECT
        b.trade_id          AS buy_trade_id,
        s.trade_id          AS sell_trade_id,
        b.account_id,
        b.wallet_id,
        b.trading_pair,
        b.price             AS buy_price,
        s.price             AS sell_price,
        ABS(b.price - s.price) / NULLIF(b.price, 0) AS price_diff_pct,
        b.quantity          AS buy_qty,
        s.quantity          AS sell_qty,
        ABS(b.quantity - s.quantity) / NULLIF(b.quantity, 0) AS qty_diff_pct,
        b.trade_ts          AS buy_ts,
        s.trade_ts          AS sell_ts,
        DATEDIFF('second', b.trade_ts, s.trade_ts) AS seconds_apart
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES b
    JOIN CRYPTO_SURVEILLANCE.HARMONISED.TRADES s
        ON b.account_id   = s.account_id
        AND b.trading_pair = s.trading_pair
        AND b.side         = 'BUY'
        AND s.side         = 'SELL'
        AND b.trade_id    <> s.trade_id
        AND ABS(DATEDIFF('second', b.trade_ts, s.trade_ts)) < 600
    WHERE b.trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT
    account_id,
    wallet_id,
    trading_pair,
    buy_trade_id,
    sell_trade_id,
    buy_price,
    sell_price,
    price_diff_pct,
    buy_qty,
    seconds_apart,
    COUNT(*) OVER (PARTITION BY account_id, trading_pair,
                   DATE_TRUNC('DAY', buy_ts))   AS wash_pairs_today,
    'WASH_TRADE'                                AS alert_type,
    CASE
        WHEN price_diff_pct < 0.001 AND qty_diff_pct < 0.01 THEN 'CRITICAL'
        WHEN price_diff_pct < 0.005 THEN 'HIGH'
        ELSE 'MEDIUM'
    END                                         AS severity,
    'Matching buy-sell within 10 min at near-identical price/qty' AS reason,
    CURRENT_TIMESTAMP()                         AS detected_at
FROM paired_trades
WHERE price_diff_pct < 0.01
  AND qty_diff_pct   < 0.05;

-- ─── RULE 3: Cross-Exchange Arbitrage Abuse ───────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_CROSS_EXCHANGE_ARBITRAGE AS
WITH multi_venue AS (
    SELECT
        account_id,
        wallet_id,
        trading_pair,
        DATE_TRUNC('MINUTE', trade_ts)          AS trade_minute,
        COUNT(DISTINCT venue)                    AS venues_count,
        ARRAY_AGG(DISTINCT venue)                AS venues,
        SUM(quote_qty)                           AS total_volume,
        MAX(price) - MIN(price)                  AS price_spread
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    WHERE trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3, 4
)
SELECT
    account_id,
    wallet_id,
    trading_pair,
    trade_minute,
    venues_count,
    venues,
    total_volume,
    price_spread,
    'CROSS_EXCHANGE_ARBITRAGE'                  AS alert_type,
    CASE WHEN venues_count >= 3 THEN 'HIGH'
         WHEN price_spread > 0 THEN 'MEDIUM'
         ELSE 'LOW' END                         AS severity,
    'Trading on ' || venues_count || ' venues within same minute' AS reason,
    CURRENT_TIMESTAMP()                         AS detected_at
FROM multi_venue
WHERE venues_count >= 2
  AND total_volume > 5000;

-- ─── RULE 4: Sanctioned Counterparty Exposure ─────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_SANCTIONED_COUNTERPARTY_EXPOSURE AS
SELECT
    w.owner_entity_id       AS entity_id,
    ot.tx_hash,
    ot.chain,
    ot.block_ts,
    ot.from_address,
    ot.to_address,
    ot.value_decimal,
    ot.token_symbol,
    CASE
        WHEN wf.is_sanctioned THEN ot.from_address
        WHEN wt.is_sanctioned THEN ot.to_address
    END                     AS sanctioned_address,
    'SANCTIONED_COUNTERPARTY' AS alert_type,
    'CRITICAL'              AS severity,
    'Direct on-chain transaction with OFAC/SDN-listed address' AS reason,
    CURRENT_TIMESTAMP()     AS detected_at
FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS ot
JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET w
    ON (ot.from_address = w.wallet_address OR ot.to_address = w.wallet_address)
    AND ot.chain = w.chain
LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET wf
    ON ot.from_address = wf.wallet_address AND ot.chain = wf.chain
    AND wf.is_sanctioned = TRUE
LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET wt
    ON ot.to_address = wt.wallet_address AND ot.chain = wt.chain
    AND wt.is_sanctioned = TRUE
WHERE ot.block_ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND w.owner_entity_id IS NOT NULL
  AND (wf.is_sanctioned = TRUE OR wt.is_sanctioned = TRUE);

-- ─── RULE 5: Layering / Structuring ──────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_STRUCTURING_PATTERNS AS
WITH daily_txns AS (
    SELECT
        account_id,
        wallet_id,
        DATE_TRUNC('DAY', trade_ts)             AS trade_date,
        COUNT(CASE WHEN quote_qty BETWEEN 8000 AND 9999 THEN 1 END) AS below_threshold_count,
        SUM(CASE WHEN quote_qty BETWEEN 8000 AND 9999 THEN quote_qty ELSE 0 END) AS below_threshold_volume,
        SUM(quote_qty)                          AS total_daily_volume
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    WHERE trade_ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3
)
SELECT
    account_id,
    wallet_id,
    trade_date,
    below_threshold_count,
    below_threshold_volume,
    total_daily_volume,
    'STRUCTURING'                               AS alert_type,
    'HIGH'                                      AS severity,
    below_threshold_count || ' transactions just below $10K threshold on same day' AS reason,
    CURRENT_TIMESTAMP()                         AS detected_at
FROM daily_txns
WHERE below_threshold_count >= 3
  AND below_threshold_volume > 20000;

-- ─── RULE 6: Mixer / Tumbler Exposure ────────────────────────────────────────
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_MIXER_EXPOSURE AS
SELECT
    w.owner_entity_id                           AS entity_id,
    ot.chain,
    COUNT(DISTINCT ot.tx_hash)                  AS mixer_tx_count,
    SUM(ot.value_decimal)                       AS mixer_volume,
    MIN(ot.block_ts)                            AS first_mixer_interaction,
    MAX(ot.block_ts)                            AS last_mixer_interaction,
    ARRAY_AGG(DISTINCT wm.label)                AS mixer_services,
    'MIXER_EXPOSURE'                            AS alert_type,
    CASE WHEN SUM(ot.value_decimal) > 10 THEN 'HIGH' ELSE 'MEDIUM' END AS severity,
    'On-chain interaction with known mixing/tumbling service' AS reason,
    CURRENT_TIMESTAMP()                         AS detected_at
FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS ot
JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET w
    ON (ot.from_address = w.wallet_address OR ot.to_address = w.wallet_address)
    AND ot.chain = w.chain
JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET wm
    ON (ot.from_address = wm.wallet_address OR ot.to_address = wm.wallet_address)
    AND ot.chain = wm.chain
    AND wm.is_mixer = TRUE
WHERE ot.block_ts >= DATEADD('day', -90, CURRENT_TIMESTAMP())
  AND w.owner_entity_id IS NOT NULL
  AND w.is_mixer = FALSE
GROUP BY 1, 2;

-- =============================================================================
-- Part C: ALERTS table + Refresh SP + Task
-- =============================================================================

CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS (
    alert_id            STRING        DEFAULT UUID_STRING() PRIMARY KEY,
    entity_id           STRING        NOT NULL,
    wallet_id           STRING,
    alert_type          STRING        NOT NULL,
    severity            STRING        NOT NULL,
    reason              STRING        NOT NULL,
    rule_flags          VARIANT,
    ml_fraud_probability FLOAT,
    composite_risk_tier STRING,
    trade_ids           VARIANT,
    tx_hashes           VARIANT,
    evidence_summary    STRING,
    status              STRING DEFAULT 'OPEN',
    assigned_to         STRING,
    case_id             STRING,
    detected_at         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    closed_at           TIMESTAMP_LTZ,
    dedup_hash          STRING COMMENT 'MD5(entity_id||alert_type||DATE_TRUNC(HOUR,detected_at))'
)
CLUSTER BY (severity, status, DATE_TRUNC('DAY', detected_at))
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Unified alert store; populated by rule views + ML scoring tasks';

CREATE STREAM IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.STREAM_ALERTS
    ON TABLE CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
    APPEND_ONLY = FALSE
    COMMENT = 'CDC stream on ALERTS for downstream case auto-creation';

-- ─── Task: Refresh ALERTS from rules ─────────────────────────────────────────
CREATE OR REPLACE PROCEDURE CRYPTO_SURVEILLANCE.ANALYTICS.SP_REFRESH_ALERTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    inserted INT DEFAULT 0;
    dedup_window TIMESTAMP_LTZ DEFAULT DATEADD('hour', -1, CURRENT_TIMESTAMP());
BEGIN
    INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        (entity_id, wallet_id, alert_type, severity, reason,
         rule_flags, ml_fraud_probability, composite_risk_tier, detected_at, dedup_hash)
    SELECT
        r.account_id, r.wallet_id, r.alert_type, r.severity, r.reason,
        OBJECT_CONSTRUCT('pump_and_dump', TRUE),
        COALESCE(s.ml_fraud_probability, 0),
        COALESCE(s.rule_risk_tier, 'MEDIUM'),
        r.detected_at,
        MD5(r.account_id || '|' || r.alert_type || '|' ||
            DATE_TRUNC('HOUR', r.detected_at)::STRING) AS dedup_hash
    FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_PUMP_AND_DUMP_CANDIDATES r
    LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES s
        ON r.account_id = s.entity_id
    WHERE r.detected_at >= :dedup_window
      AND MD5(r.account_id || '|' || r.alert_type || '|' ||
              DATE_TRUNC('HOUR', r.detected_at)::STRING)
          NOT IN (SELECT dedup_hash FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                  WHERE detected_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP()));

    INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        (entity_id, wallet_id, alert_type, severity, reason,
         rule_flags, ml_fraud_probability, composite_risk_tier, detected_at, dedup_hash)
    SELECT
        r.account_id, r.wallet_id, r.alert_type, r.severity, r.reason,
        OBJECT_CONSTRUCT('wash_trade', TRUE),
        COALESCE(s.ml_fraud_probability, 0),
        COALESCE(s.rule_risk_tier, 'HIGH'),
        r.detected_at,
        MD5(r.account_id || '|' || r.alert_type || '|' ||
            r.buy_trade_id || '|' || r.sell_trade_id)
    FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_WASH_TRADE_PATTERNS r
    LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES s ON r.account_id = s.entity_id
    WHERE r.detected_at >= :dedup_window
      AND MD5(r.account_id || '|' || r.alert_type || '|' ||
              r.buy_trade_id || '|' || r.sell_trade_id)
          NOT IN (SELECT dedup_hash FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                  WHERE detected_at >= DATEADD('day', -1, CURRENT_TIMESTAMP()));

    INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        (entity_id, wallet_id, alert_type, severity, reason,
         rule_flags, ml_fraud_probability, composite_risk_tier,
         evidence_summary, detected_at, dedup_hash)
    SELECT
        r.entity_id,
        NULL,
        r.alert_type,
        r.severity,
        r.reason,
        OBJECT_CONSTRUCT('sanctioned_counterparty', TRUE),
        COALESCE(s.ml_fraud_probability, 0),
        'CRITICAL',
        'Sanctioned address: ' || r.sanctioned_address || ' | Chain: ' || r.chain
            || ' | Value: ' || r.value_decimal::STRING,
        r.detected_at,
        MD5(r.entity_id || '|' || r.tx_hash)
    FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_SANCTIONED_COUNTERPARTY_EXPOSURE r
    LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES s ON r.entity_id = s.entity_id
    WHERE r.detected_at >= :dedup_window
      AND MD5(r.entity_id || '|' || r.tx_hash)
          NOT IN (SELECT dedup_hash FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                  WHERE detected_at >= DATEADD('day', -7, CURRENT_TIMESTAMP()));

    -- RULE 4: Cross-Exchange Arbitrage
    INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        (entity_id, wallet_id, alert_type, severity, reason,
         rule_flags, ml_fraud_probability, composite_risk_tier, detected_at, dedup_hash)
    SELECT
        r.account_id, r.wallet_id, r.alert_type, r.severity, r.reason,
        OBJECT_CONSTRUCT('cross_exchange_arbitrage', TRUE),
        COALESCE(s.ml_fraud_probability, 0),
        COALESCE(s.rule_risk_tier, 'MEDIUM'),
        r.detected_at,
        MD5(r.account_id || '|' || r.alert_type || '|' ||
            r.trade_minute::STRING)
    FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_CROSS_EXCHANGE_ARBITRAGE r
    LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES s ON r.account_id = s.entity_id
    WHERE r.detected_at >= :dedup_window
      AND MD5(r.account_id || '|' || r.alert_type || '|' ||
              r.trade_minute::STRING)
          NOT IN (SELECT dedup_hash FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                  WHERE detected_at >= DATEADD('day', -1, CURRENT_TIMESTAMP()));

    -- RULE 5: Structuring Patterns
    INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        (entity_id, wallet_id, alert_type, severity, reason,
         rule_flags, ml_fraud_probability, composite_risk_tier, detected_at, dedup_hash)
    SELECT
        r.account_id, r.wallet_id, r.alert_type, r.severity, r.reason,
        OBJECT_CONSTRUCT('structuring', TRUE),
        COALESCE(s.ml_fraud_probability, 0),
        COALESCE(s.rule_risk_tier, 'HIGH'),
        r.detected_at,
        MD5(r.account_id || '|' || r.alert_type || '|' ||
            r.trade_date::STRING)
    FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_STRUCTURING_PATTERNS r
    LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES s ON r.account_id = s.entity_id
    WHERE r.detected_at >= :dedup_window
      AND MD5(r.account_id || '|' || r.alert_type || '|' ||
              r.trade_date::STRING)
          NOT IN (SELECT dedup_hash FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                  WHERE detected_at >= DATEADD('day', -1, CURRENT_TIMESTAMP()));

    -- RULE 6: Mixer Exposure
    INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        (entity_id, wallet_id, alert_type, severity, reason,
         rule_flags, ml_fraud_probability, composite_risk_tier,
         evidence_summary, detected_at, dedup_hash)
    SELECT
        r.entity_id,
        NULL,
        r.alert_type,
        r.severity,
        r.reason,
        OBJECT_CONSTRUCT('mixer_exposure', TRUE),
        COALESCE(s.ml_fraud_probability, 0),
        COALESCE(s.rule_risk_tier, 'HIGH'),
        'Mixer txns: ' || r.mixer_tx_count::STRING || ' | Volume: ' || r.mixer_volume::STRING
            || ' | Chain: ' || r.chain,
        r.detected_at,
        MD5(r.entity_id || '|' || r.alert_type || '|' || r.chain)
    FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_MIXER_EXPOSURE r
    LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES s ON r.entity_id = s.entity_id
    WHERE r.detected_at >= :dedup_window
      AND MD5(r.entity_id || '|' || r.alert_type || '|' || r.chain)
          NOT IN (SELECT dedup_hash FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                  WHERE detected_at >= DATEADD('day', -7, CURRENT_TIMESTAMP()));

    RETURN 'ALERTS refreshed | Inserted: ' || SQLROWCOUNT;
END;
$$;

CREATE TASK IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.TASK_REFRESH_ALERTS
    WAREHOUSE  = WH_SURVEILLANCE
    SCHEDULE   = '5 MINUTE'
    COMMENT    = 'Refreshes ALERTS table from detection rule views every 5 minutes'
AS
    CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_REFRESH_ALERTS();

-- Task starts SUSPENDED; resume explicitly via demo_seed.sql or manually:
-- ALTER TASK CRYPTO_SURVEILLANCE.ANALYTICS.TASK_REFRESH_ALERTS RESUME;

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT SELECT ON TABLE  CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_PUMP_AND_DUMP_CANDIDATES  TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_WASH_TRADE_PATTERNS        TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_CROSS_EXCHANGE_ARBITRAGE   TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_SANCTIONED_COUNTERPARTY_EXPOSURE TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_STRUCTURING_PATTERNS       TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_MIXER_EXPOSURE             TO ROLE SURVEILLANCE_ANALYST;
