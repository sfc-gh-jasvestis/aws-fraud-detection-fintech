-- =============================================================================
-- Phase 8 + 9: Monitoring, Ops, QuickSight Views, Governance
-- File: 12_monitoring.sql
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_ANALYTICS;

-- ─── QuickSight-ready views ───────────────────────────────────────────────────

-- Alert Volume Dashboard
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ALERT_VOLUME AS
SELECT
    DATE_TRUNC('HOUR', detected_at)::TIMESTAMP_NTZ AS alert_hour,
    alert_type,
    severity,
    COUNT(*)                                        AS alert_count,
    COUNT(DISTINCT entity_id)                       AS unique_entities,
    AVG(ml_fraud_probability)                       AS avg_ml_score,
    COUNT_IF(status = 'OPEN')                       AS open_count,
    COUNT_IF(status = 'CLOSED')                     AS closed_count
FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
WHERE detected_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3;

-- Entity Risk Heatmap
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ENTITY_HEATMAP AS
SELECT
    e.entity_id,
    e.aml_risk_rating,
    e.kyc_tier,
    e.account_type,
    e.pep_flag,
    e.sanctions_flag,
    s.ml_fraud_probability,
    s.rule_risk_tier,
    ef.cex_volume_usd_30d,
    ef.mixer_interactions_30d,
    ef.sanctioned_volume_30d,
    ef.wallet_count,
    COUNT(a.alert_id)           AS total_alerts,
    COUNT_IF(a.severity = 'CRITICAL') AS critical_alerts,
    COUNT_IF(a.severity = 'HIGH')     AS high_alerts,
    MAX(a.detected_at)          AS last_alert_ts
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY e
LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES s    ON e.entity_id = s.entity_id
LEFT JOIN CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES ef ON e.entity_id = ef.entity_id
LEFT JOIN CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS a          ON e.entity_id = a.entity_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;

-- Case SLA Dashboard
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_CASE_SLA AS
SELECT
    DATE_TRUNC('DAY', created_at)::DATE     AS case_date,
    priority,
    state,
    COUNT(*)                                AS case_count,
    COUNT_IF(sla_breached = TRUE)           AS breached_count,
    ROUND(AVG(DATEDIFF('hour', created_at,
        COALESCE(closed_at, CURRENT_TIMESTAMP()))), 1) AS avg_resolution_hours,
    ROUND(AVG(peak_ml_probability), 4)      AS avg_ml_score
FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES
WHERE created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3;

-- Asset Trading Surveillance Summary
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_TRADING_SUMMARY AS
SELECT
    DATE_TRUNC('DAY', trade_date)           AS trade_day,
    trading_pair,
    venue,
    COUNT(*)                                AS trade_count,
    SUM(quote_qty)                          AS volume_usd,
    AVG(price)                              AS avg_price,
    MAX(price)                              AS high_price,
    MIN(price)                              AS low_price,
    COUNT(DISTINCT account_id)              AS unique_traders
FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES
WHERE trade_ts >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3;

-- On-Chain Flow Summary (for flow-of-funds view in QuickSight)
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ONCHAIN_FLOWS AS
SELECT
    DATE_TRUNC('DAY', block_ts)::DATE       AS flow_date,
    chain,
    token_symbol,
    event_type,
    COUNT(*)                                AS tx_count,
    SUM(value_decimal)                      AS total_value,
    COUNT(DISTINCT from_address)            AS unique_senders,
    COUNT(DISTINCT to_address)              AS unique_receivers,
    COUNT_IF(is_sanctioned_counterparty)    AS sanctioned_tx_count
FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS
WHERE block_ts >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3, 4;

-- Grant all QuickSight views to analyst role
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ALERT_VOLUME     TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ENTITY_HEATMAP   TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_CASE_SLA         TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_TRADING_SUMMARY  TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ONCHAIN_FLOWS    TO ROLE SURVEILLANCE_ANALYST;

-- ─── SRE Monitoring Views ─────────────────────────────────────────────────────

-- Ingestion lag monitor (checks if RAW tables are receiving data)
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.RAW.VW_INGEST_LAG AS
SELECT
    'CEX_TRADES_RAW'     AS table_name,
    MAX(_LOAD_TS)        AS last_record_ts,
    DATEDIFF('minute', MAX(_LOAD_TS), CURRENT_TIMESTAMP()) AS lag_minutes,
    COUNT_IF(_LOAD_TS >= DATEADD('hour', -1, CURRENT_TIMESTAMP())) AS records_last_1h
FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW
UNION ALL
SELECT 'CEX_ORDERS_RAW', MAX(_LOAD_TS),
    DATEDIFF('minute', MAX(_LOAD_TS), CURRENT_TIMESTAMP()),
    COUNT_IF(_LOAD_TS >= DATEADD('hour', -1, CURRENT_TIMESTAMP()))
FROM CRYPTO_SURVEILLANCE.RAW.CEX_ORDERS_RAW
UNION ALL
SELECT 'CEX_BALANCES_RAW', MAX(_LOAD_TS),
    DATEDIFF('minute', MAX(_LOAD_TS), CURRENT_TIMESTAMP()),
    COUNT_IF(_LOAD_TS >= DATEADD('hour', -1, CURRENT_TIMESTAMP()))
FROM CRYPTO_SURVEILLANCE.RAW.CEX_BALANCES_RAW
UNION ALL
SELECT 'ONCHAIN_EVENTS_RAW', MAX(_LOAD_TS),
    DATEDIFF('minute', MAX(_LOAD_TS), CURRENT_TIMESTAMP()),
    COUNT_IF(_LOAD_TS >= DATEADD('hour', -1, CURRENT_TIMESTAMP()))
FROM CRYPTO_SURVEILLANCE.RAW.ONCHAIN_EVENTS_RAW;

-- DLQ summary
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.RAW.VW_DLQ_SUMMARY AS
SELECT
    source_table,
    COUNT(*)                    AS error_count,
    COUNT_IF(resolved = FALSE)  AS unresolved_count,
    MAX(_LOAD_TS)               AS latest_error_ts,
    ARRAY_AGG(DISTINCT SPLIT_PART(error_message, ':', 1)) AS error_types
FROM CRYPTO_SURVEILLANCE.RAW.DLQ_ERRORS
WHERE _LOAD_TS >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1;

-- Dynamic Table refresh health
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_DYNAMIC_TABLE_HEALTH AS
SELECT
    table_name,
    scheduling_state,
    last_completed_dependency_time,
    target_lag_sec,
    DATEDIFF('second',
        last_completed_dependency_time,
        CURRENT_TIMESTAMP()) AS actual_lag_sec,
    CASE
        WHEN DATEDIFF('second', last_completed_dependency_time, CURRENT_TIMESTAMP())
             > target_lag_sec * 2 THEN 'LAGGING'
        ELSE 'OK'
    END AS health_status
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
WHERE table_schema IN ('HARMONISED', 'FEATURES', 'ANALYTICS')
  AND table_catalog = 'CRYPTO_SURVEILLANCE';

GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.RAW.VW_INGEST_LAG               TO ROLE SURVEILLANCE_ADMIN;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.RAW.VW_DLQ_SUMMARY              TO ROLE SURVEILLANCE_ADMIN;
GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_DYNAMIC_TABLE_HEALTH TO ROLE SURVEILLANCE_ADMIN;
