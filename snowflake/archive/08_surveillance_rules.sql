-- =============================================================================
-- Phase 6: Surveillance Detection Rules + ALERTS table
-- File: 08_surveillance_rules.sql
-- Schema: CRYPTO_SURVEILLANCE.ANALYTICS
-- Detection rule views: SQL-based typology detection per FSI surveillance RAs.
-- Each view returns candidate events with reason codes and severity.
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_ANALYTICS;

-- ─── RULE 1: Pump & Dump Candidates ──────────────────────────────────────────
-- Pattern: abnormal price spike (>5% in 1h) with high buy volume followed
-- by concentrated sell-off within 4h; same accounts driving both legs.
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_PUMP_AND_DUMP_CANDIDATES AS
WITH price_spikes AS (
    SELECT
        trading_pair,
        DATE_TRUNC('HOUR', trade_ts)        AS trade_hour,
        AVG(price)                          AS avg_price,
        MIN(price)                          AS min_price,
        MAX(price)                          AS max_price,
        (MAX(price) - MIN(price)) / NULLIF(MIN(price), 0) AS price_range_pct,
        SUM(quote_qty)                      AS total_volume,
        COUNT(DISTINCT account_id)          AS unique_accounts
    FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
    WHERE trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
suspect_hours AS (
    SELECT * FROM price_spikes
    WHERE price_range_pct > 0.05        -- >5% price range in 1 hour
      AND total_volume    > 50000        -- meaningful volume threshold
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
    sh.total_volume                         AS window_volume,
    cs.sell_volume,
    cs.buy_volume,
    cs.trade_count,
    'PUMP_AND_DUMP'                         AS alert_type,
    'HIGH'                                  AS severity,
    'Price spike >5% with concentrated sell-off by same account' AS reason,
    CURRENT_TIMESTAMP()                     AS detected_at
FROM concentrated_sellers cs
JOIN suspect_hours sh
    ON cs.trading_pair = sh.trading_pair
   AND cs.trade_hour   = sh.trade_hour
WHERE cs.sell_volume > cs.buy_volume * 2   -- sell >> buy in spike window
  AND cs.sell_volume > 10000;

-- ─── RULE 2: Wash Trade Patterns ─────────────────────────────────────────────
-- Pattern: same account buys and sells same pair at nearly identical price
-- and quantity within a tight time window (10 min); self-trades or
-- coordinated counterparties.
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
        AND ABS(DATEDIFF('second', b.trade_ts, s.trade_ts)) < 600   -- within 10 min
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
WHERE price_diff_pct < 0.01    -- price within 1%
  AND qty_diff_pct   < 0.05;   -- qty within 5%

-- ─── RULE 3: Cross-Exchange Arbitrage Abuse ───────────────────────────────────
-- Pattern: entity trades heavily on multiple venues within same minute;
-- potential front-running or coordinated manipulation.
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
-- Pattern: entity's on-chain wallet transacts with sanctioned/flagged address.
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
    -- Determine which direction is the sanctioned address
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
-- Join sanctioned from-address
LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET wf
    ON ot.from_address = wf.wallet_address AND ot.chain = wf.chain
    AND wf.is_sanctioned = TRUE
-- Join sanctioned to-address
LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET wt
    ON ot.to_address = wt.wallet_address AND ot.chain = wt.chain
    AND wt.is_sanctioned = TRUE
WHERE ot.block_ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND w.owner_entity_id IS NOT NULL
  AND (wf.is_sanctioned = TRUE OR wt.is_sanctioned = TRUE);

-- ─── RULE 5: Layering / Structuring ──────────────────────────────────────────
-- Pattern: multiple just-below-threshold transactions (structuring / smurfing).
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_STRUCTURING_PATTERNS AS
WITH daily_txns AS (
    SELECT
        account_id,
        wallet_id,
        DATE_TRUNC('DAY', trade_ts)             AS trade_date,
        COUNT(*) FILTER (
            WHERE quote_qty BETWEEN 8000 AND 9999
        )                                       AS below_threshold_count,
        SUM(quote_qty) FILTER (
            WHERE quote_qty BETWEEN 8000 AND 9999
        )                                       AS below_threshold_volume,
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
WHERE below_threshold_count >= 3               -- 3+ just-below-threshold trades in one day
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

-- ─── Unified ALERTS table ─────────────────────────────────────────────────────
-- Materialized from all rule views + ML scores; deduped by hash.
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS (
    alert_id            STRING        DEFAULT UUID_STRING() PRIMARY KEY,
    entity_id           STRING        NOT NULL,
    wallet_id           STRING,
    alert_type          STRING        NOT NULL,  -- PUMP_AND_DUMP|WASH_TRADE|etc.
    severity            STRING        NOT NULL,  -- CRITICAL|HIGH|MEDIUM|LOW
    reason              STRING        NOT NULL,
    -- Score components
    rule_flags          VARIANT,                 -- JSON: {rule_name: bool}
    ml_fraud_probability FLOAT,
    composite_risk_tier STRING,
    -- Evidence references
    trade_ids           VARIANT,                 -- ARRAY of relevant trade_ids
    tx_hashes           VARIANT,                 -- ARRAY of relevant tx_hashes
    evidence_summary    STRING,
    -- Lifecycle
    status              STRING DEFAULT 'OPEN',   -- OPEN|IN_REVIEW|ESCALATED|CLOSED|FALSE_POSITIVE
    assigned_to         STRING,
    case_id             STRING,
    -- Timestamps
    detected_at         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    closed_at           TIMESTAMP_LTZ,
    -- Dedup key
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
    -- Pump & dump
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

    -- Wash trades
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

    -- Sanctioned counterparty (always insert)
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

    RETURN 'ALERTS refreshed | Inserted: ' || SQLROWCOUNT;
END;
$$;

CREATE TASK IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.TASK_REFRESH_ALERTS
    WAREHOUSE  = WH_ANALYTICS
    SCHEDULE   = '5 MINUTE'
    COMMENT    = 'Refreshes ALERTS table from detection rule views every 5 minutes'
AS
    CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_REFRESH_ALERTS();

-- Task starts SUSPENDED; resume explicitly via demo_seed.sql or manually:
-- ALTER TASK CRYPTO_SURVEILLANCE.ANALYTICS.TASK_REFRESH_ALERTS RESUME;

-- Grants
GRANT SELECT ON TABLE  CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_PUMP_AND_DUMP_CANDIDATES  TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_WASH_TRADE_PATTERNS        TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_CROSS_EXCHANGE_ARBITRAGE   TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_SANCTIONED_COUNTERPARTY_EXPOSURE TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_STRUCTURING_PATTERNS       TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW   CRYPTO_SURVEILLANCE.ANALYTICS.VW_MIXER_EXPOSURE             TO ROLE SURVEILLANCE_ANALYST;
