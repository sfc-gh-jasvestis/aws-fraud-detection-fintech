-- ============================================================================
-- 06_INTELLIGENCE.SQL - search, anomaly detection, semantic view, agent,
-- live-trade alert and on-demand refresh DAG.
-- Run with snowflake/run_intelligence.py (substitutes validated __DEMO_DB__ /
-- __DEMO_WH__ / __ALERT_EMAIL__). Requires 00-05, plus 08 (Snowflake only) or
-- aws/setup_aws.py (AWS build) for RAW.LIVE_TRADES.
-- Alerts and tasks are created SUSPENDED; run them with EXECUTE ALERT / EXECUTE TASK.
-- ============================================================================
USE DATABASE __DEMO_DB__;
CREATE SCHEMA IF NOT EXISTS SEARCH;
CREATE SCHEMA IF NOT EXISTS APP;

-- ---------- Synthetic investigation knowledge base (clearly synthetic SOPs) ----------
CREATE OR REPLACE TABLE SEARCH.INVESTIGATION_DOCS AS
WITH rules AS (
  SELECT DISTINCT r.DETECTION_RULE, a.CATEGORY
  FROM RAW.ACCOUNT_DAILY r JOIN RAW.ACCOUNTS a ON a.ID = r.ENTITY_ID
  WHERE r.CONFIRMED_COUNT > 0
)
SELECT
  'SOP-' || LPAD(ROW_NUMBER() OVER (ORDER BY CATEGORY, DETECTION_RULE)::VARCHAR, 3, '0') AS DOC_ID,
  'SOP' AS DOC_TYPE,
  CATEGORY,
  DETECTION_RULE,
  CATEGORY || ' - ' || DETECTION_RULE || ' alert investigation' AS TITLE,
  'Synthetic demo SOP. Account type: ' || CATEGORY || '. Detection rule: ' || DETECTION_RULE || '. '
  || 'Step 1: open a case, link the alert and freeze further withdrawals above the account limit pending review. '
  || 'Step 2: ' || CASE
       WHEN DETECTION_RULE = 'Wash trading' THEN 'pull matched trades where both sides trace to the same beneficial owner or linked accounts, and compute self-matched volume as a share of the account''s daily volume.'
       WHEN DETECTION_RULE = 'Spoofing' THEN 'review order-book snapshots for large orders cancelled within seconds of placement, and compare the cancel ratio with the account''s 30-day baseline.'
       WHEN DETECTION_RULE = 'Pump and dump' THEN 'check for coordinated buying in a low-liquidity token followed by a sell-off, and identify linked accounts that traded the same token in the window.'
       WHEN DETECTION_RULE = 'Structuring' THEN 'list deposits and withdrawals just below reporting thresholds over 30 days and confirm whether they aggregate above the threshold.'
       WHEN DETECTION_RULE = 'Mixer exposure' THEN 'trace on-chain counterparties of recent deposits and withdrawals for direct or one-hop exposure to mixing services.'
       WHEN DETECTION_RULE = 'Sanctions screening hit' THEN 'compare the matched name, wallet and jurisdiction against the screening list entry, and escalate to the sanctions officer before any further transfers.'
       ELSE 'review the alerted activity against the account''s profile and escalate if unexplained.'
     END
  || ' Step 3: if self-matched volume exceeds 5% or the cancel ratio exceeds 40% after review, keep the case open and request enhanced due diligence. '
  || 'Step 4: record the disposition; if suspicion is confirmed, draft a suspicious activity report for the compliance officer to approve.' AS CONTENT
FROM rules;

CREATE OR REPLACE CORTEX SEARCH SERVICE SEARCH.INVESTIGATION_SOP_SEARCH
  ON CONTENT
  ATTRIBUTES CATEGORY, DETECTION_RULE
  WAREHOUSE = __DEMO_WH__
  TARGET_LAG = '7 days'
AS (SELECT DOC_ID, TITLE, CATEGORY, DETECTION_RULE, CONTENT FROM SEARCH.INVESTIGATION_DOCS);

-- ---------- Self-match ratio anomaly detection (train first 75 days, detect last 15) ----------
CREATE OR REPLACE VIEW ML.SELF_MATCH_SERIES AS
SELECT ENTITY_ID, EVENT_DATE::TIMESTAMP_NTZ AS TS, SELF_MATCH_PCT::FLOAT AS SELF_MATCH
FROM RAW.ACCOUNT_DAILY;
CREATE OR REPLACE VIEW ML.SELF_MATCH_TRAIN AS
SELECT * FROM ML.SELF_MATCH_SERIES WHERE TS < (SELECT DATEADD(day, -15, MAX(TS)) FROM ML.SELF_MATCH_SERIES);
CREATE OR REPLACE VIEW ML.SELF_MATCH_DETECT AS
SELECT * FROM ML.SELF_MATCH_SERIES WHERE TS >= (SELECT DATEADD(day, -15, MAX(TS)) FROM ML.SELF_MATCH_SERIES);

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ML.SELF_MATCH_ANOMALY_MODEL(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ML.SELF_MATCH_TRAIN'),
  SERIES_COLNAME => 'ENTITY_ID', TIMESTAMP_COLNAME => 'TS', TARGET_COLNAME => 'SELF_MATCH',
  LABEL_COLNAME => '');

CREATE OR REPLACE TABLE ML.SELF_MATCH_ANOMALIES AS
SELECT SERIES::VARCHAR AS ENTITY_ID, TS::DATE AS EVENT_DATE, Y AS SELF_MATCH, FORECAST AS EXPECTED,
       LOWER_BOUND, UPPER_BOUND, IS_ANOMALY, PERCENTILE
FROM TABLE(ML.SELF_MATCH_ANOMALY_MODEL!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ML.SELF_MATCH_DETECT'),
  SERIES_COLNAME => 'ENTITY_ID', TIMESTAMP_COLNAME => 'TS', TARGET_COLNAME => 'SELF_MATCH'));

-- ---------- Semantic view ----------
CREATE OR REPLACE SEMANTIC VIEW APP.SURVEILLANCE_ANALYTICS
  TABLES (
    accounts AS CURATED.PERFORMANCE_SUMMARY PRIMARY KEY (ENTITY_ID)
      COMMENT = 'One row per exchange account, 90-day totals',
    risk AS ML.SUSPICION_RISK_SCORES PRIMARY KEY (ENTITY_ID)
      COMMENT = 'Latest next-7-day suspicious-activity probability per account',
    rules AS CURATED.RULE_SUMMARY PRIMARY KEY (DETECTION_RULE)
      COMMENT = 'Alerts, confirmed alerts and SARs by detection rule, 90 days',
    daily AS CURATED.TREND_ANALYSIS PRIMARY KEY (METRIC_DATE)
      COMMENT = 'Exchange-wide totals per day'
  )
  RELATIONSHIPS (risk_account AS risk (ENTITY_ID) REFERENCES accounts)
  FACTS (
    accounts.alerts_f AS ALERT_COUNT,
    accounts.confirmed_f AS CONFIRMED_COUNT,
    accounts.sars_f AS SAR_COUNT,
    accounts.trades_f AS TRADE_COUNT,
    accounts.notional_f AS NOTIONAL_USD,
    accounts.review_due_f AS REVIEW_DUE,
    accounts.review_done_f AS REVIEW_COMPLETED,
    risk.suspicion_prob_f AS SUSPICION_PROB_7D,
    rules.rule_alerts_f AS ALERT_COUNT,
    rules.rule_confirmed_f AS CONFIRMED_COUNT,
    rules.rule_sars_f AS SAR_COUNT,
    rules.rule_notional_f AS FLAGGED_NOTIONAL_USD,
    daily.day_alerts_f AS ALERT_COUNT,
    daily.day_confirmed_f AS CONFIRMED_COUNT,
    daily.day_notional_f AS NOTIONAL_USD
  )
  DIMENSIONS (
    accounts.account_id AS ENTITY_ID WITH SYNONYMS = ('account', 'customer id'),
    accounts.account_name AS ENTITY_NAME,
    accounts.market AS REGION WITH SYNONYMS = ('market', 'jurisdiction', 'region') COMMENT = 'APJ market where the account is onboarded',
    accounts.account_type AS CATEGORY WITH SYNONYMS = ('segment', 'customer type', 'account type'),
    accounts.kyc_tier AS KYC_TIER COMMENT = 'KYC tier 1 (basic) to 3 (enhanced)',
    risk.risk_band AS RISK_BAND COMMENT = 'High >= 0.5, Medium >= 0.25, else Low',
    risk.scored_as_of AS SCORED_AS_OF,
    rules.detection_rule AS DETECTION_RULE WITH SYNONYMS = ('rule', 'typology', 'scenario'),
    daily.metric_date AS METRIC_DATE
  )
  METRICS (
    accounts.alert_precision_pct AS 100 * SUM(accounts.confirmed_f) / NULLIF(SUM(accounts.alerts_f), 0)
      COMMENT = 'Confirmed suspicious alerts / alerts raised',
    accounts.alerts_raised AS SUM(accounts.alerts_f) WITH SYNONYMS = ('alerts', 'alert volume'),
    accounts.confirmed_suspicious AS SUM(accounts.confirmed_f) WITH SYNONYMS = ('true positives', 'confirmed alerts'),
    accounts.sars_filed AS SUM(accounts.sars_f) WITH SYNONYMS = ('SARs', 'suspicious activity reports'),
    accounts.trades AS SUM(accounts.trades_f),
    accounts.total_notional_usd AS SUM(accounts.notional_f) WITH SYNONYMS = ('volume', 'traded value'),
    accounts.review_compliance_pct AS 100 * SUM(accounts.review_done_f) / NULLIF(SUM(accounts.review_due_f), 0)
      COMMENT = 'Periodic account reviews completed / reviews due',
    risk.avg_suspicion_prob AS AVG(risk.suspicion_prob_f),
    rules.rule_alerts AS SUM(rules.rule_alerts_f),
    rules.rule_confirmed AS SUM(rules.rule_confirmed_f),
    rules.rule_sars AS SUM(rules.rule_sars_f),
    rules.rule_precision_pct AS 100 * SUM(rules.rule_confirmed_f) / NULLIF(SUM(rules.rule_alerts_f), 0),
    daily.daily_alerts AS SUM(daily.day_alerts_f),
    daily.daily_confirmed AS SUM(daily.day_confirmed_f),
    daily.daily_notional_usd AS SUM(daily.day_notional_f)
  )
  COMMENT = 'Synthetic digital-asset exchange surveillance analytics (demo)';

-- ---------- Cortex Agent ----------
CREATE OR REPLACE AGENT APP.SURVEILLANCE_AGENT
  COMMENT = 'Surveillance assistant over a synthetic digital-asset exchange'
  FROM SPECIFICATION
$$
models:
  orchestration: claude-sonnet-4-5
instructions:
  response: "Answer only from tool results. State that data is synthetic. Give account IDs and numbers with units."
  orchestration: "Use surveillance_analyst for alerts, confirmed suspicious activity, SARs, alert precision, accounts, markets, detection rules and risk. Use sop_search for investigation procedures."
tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: surveillance_analyst
      description: "Alerts, confirmed suspicious alerts, SARs, alert precision, notional, review compliance, detection rules and suspicious-activity risk scores"
  - tool_spec:
      type: cortex_search
      name: sop_search
      description: "Synthetic alert-investigation SOPs by account type and detection rule"
tool_resources:
  surveillance_analyst:
    semantic_view: __DEMO_DB__.APP.SURVEILLANCE_ANALYTICS
    execution_environment:
      type: warehouse
      warehouse: __DEMO_WH__
  sop_search:
    name: __DEMO_DB__.SEARCH.INVESTIGATION_SOP_SEARCH
    max_results: 3
    id_column: DOC_ID
    title_column: TITLE
$$;

-- ---------- Live-trade alert ----------
CREATE TABLE IF NOT EXISTS APP.ALERT_LOG (
  ALERTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), ACCOUNT_ID VARCHAR,
  EVENT_TS TIMESTAMP_NTZ, NOTIONAL_USD FLOAT, SELF_MATCH_PCT FLOAT, SOP_HINT VARCHAR);

CREATE OR REPLACE NOTIFICATION INTEGRATION FRAUD_FINTECH_EMAIL_INT
  TYPE = EMAIL ENABLED = TRUE ALLOWED_RECIPIENTS = ('__ALERT_EMAIL__');

CREATE OR REPLACE PROCEDURE APP.LOG_LIVE_ALERTS()
RETURNS NUMBER
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  n NUMBER;
BEGIN
  INSERT INTO APP.ALERT_LOG (ACCOUNT_ID, EVENT_TS, NOTIONAL_USD, SELF_MATCH_PCT, SOP_HINT)
    SELECT t.ACCOUNT_ID, t.EVENT_TS, t.NOTIONAL_USD, t.SELF_MATCH_PCT,
           'Check ' || a.CATEGORY || ' investigation SOPs; current risk band ' || COALESCE(r.RISK_BAND, 'n/a')
    FROM RAW.LIVE_TRADES t
    JOIN RAW.ACCOUNTS a ON a.ID = t.ACCOUNT_ID
    LEFT JOIN ML.SUSPICION_RISK_SCORES r ON r.ENTITY_ID = t.ACCOUNT_ID
    WHERE t.STATUS = 'ALERT'
      AND NOT EXISTS (SELECT 1 FROM APP.ALERT_LOG l WHERE l.ACCOUNT_ID = t.ACCOUNT_ID AND l.EVENT_TS = t.EVENT_TS);
  n := SQLROWCOUNT;
  IF (n > 0) THEN
    CALL SYSTEM$SEND_EMAIL('FRAUD_FINTECH_EMAIL_INT', '__ALERT_EMAIL__',
      '[Demo] Exchange surveillance alert',
      'New live-trade alerts logged in APP.ALERT_LOG: ' || :n || '. Data is synthetic.');
  END IF;
  RETURN n;
END;
$$;

CREATE OR REPLACE ALERT APP.LIVE_TRADE_ALERT
  WAREHOUSE = __DEMO_WH__
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM RAW.LIVE_TRADES t
    WHERE t.STATUS = 'ALERT'
      AND NOT EXISTS (SELECT 1 FROM APP.ALERT_LOG l WHERE l.ACCOUNT_ID = t.ACCOUNT_ID AND l.EVENT_TS = t.EVENT_TS)))
  THEN CALL APP.LOG_LIVE_ALERTS();

-- ---------- On-demand refresh DAG (suspended; run with EXECUTE TASK APP.TASK_REFRESH_CURATED) ----------
CREATE OR REPLACE PROCEDURE APP.REFRESH_CURATED()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  ALTER DYNAMIC TABLE CURATED.PERFORMANCE_SUMMARY REFRESH;
  ALTER DYNAMIC TABLE CURATED.TREND_ANALYSIS REFRESH;
  ALTER DYNAMIC TABLE CURATED.RULE_SUMMARY REFRESH;
  ALTER DYNAMIC TABLE CURATED.KPI_SUMMARY REFRESH;
  RETURN 'refreshed';
END;
$$;

CREATE OR REPLACE TASK APP.TASK_REFRESH_CURATED
  WAREHOUSE = __DEMO_WH__
AS
  CALL APP.REFRESH_CURATED();

CREATE OR REPLACE TASK APP.TASK_RESCORE_RISK
  WAREHOUSE = __DEMO_WH__
  AFTER APP.TASK_REFRESH_CURATED
AS
  CREATE OR REPLACE TABLE ML.SUSPICION_RISK_SCORES COPY GRANTS AS
  WITH latest AS (
    SELECT * FROM ML.SUSPICION_FEATURES QUALIFY ROW_NUMBER() OVER (PARTITION BY ENTITY_ID ORDER BY EVENT_DATE DESC) = 1
  ), p AS (
    SELECT ENTITY_ID, EVENT_DATE,
           ML.SUSPICION_RISK_MODEL!PREDICT(INPUT_DATA => OBJECT_CONSTRUCT(
             'CATEGORY', CATEGORY, 'KYC_TIER', KYC_TIER, 'ACCOUNT_AGE_YEARS', ACCOUNT_AGE_YEARS,
             'SELF_MATCH_PCT', SELF_MATCH_PCT, 'CANCEL_RATIO_PCT', CANCEL_RATIO_PCT,
             'SELF_MATCH_7D', SELF_MATCH_7D, 'CONFIRMED_30D', CONFIRMED_30D)) AS PRED
    FROM latest
  )
  SELECT ENTITY_ID, EVENT_DATE AS SCORED_AS_OF, ROUND(PRED:probability:SUSPICIOUS::FLOAT, 4) AS SUSPICION_PROB_7D,
         CASE WHEN PRED:probability:SUSPICIOUS::FLOAT >= 0.5 THEN 'High'
              WHEN PRED:probability:SUSPICIOUS::FLOAT >= 0.25 THEN 'Medium' ELSE 'Low' END AS RISK_BAND,
         CURRENT_TIMESTAMP() AS SCORED_AT
  FROM p;
