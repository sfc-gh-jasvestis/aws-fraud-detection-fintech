-- Synthetic account-day observations for a fictional digital-asset exchange.
-- Nothing is seeded as a prediction. Randomness is HASH-seeded, so every rebuild
-- is reproducible: per-account suspicion propensity, review drift between
-- periodic account reviews, missed reviews, account-type-weighted detection
-- rules, false-positive alerts, and two market-wide volatility spikes.
USE DATABASE IDENTIFIER($DEMO_DB);
USE SCHEMA RAW;
USE WAREHOUSE IDENTIFIER($DEMO_WH);

CREATE TABLE RAW.ACCOUNTS AS
WITH accounts AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS ACCOUNT_INDEX
  FROM TABLE(GENERATOR(ROWCOUNT => 40))
), draws AS (
  SELECT ACCOUNT_INDEX,
         MOD(ABS(HASH(ACCOUNT_INDEX, 'age')), 1000000) / 1e6 AS U_AGE,
         MOD(ABS(HASH(ACCOUNT_INDEX, 'rate')), 1000000) / 1e6 AS U_RATE,
         MOD(ABS(HASH(ACCOUNT_INDEX, 'review')), 1000000) / 1e6 AS U_REVIEW,
         MOD(ABS(HASH(ACCOUNT_INDEX, 'discipline')), 1000000) / 1e6 AS U_DISCIPLINE,
         MOD(ABS(HASH(ACCOUNT_INDEX, 'tier')), 1000000) / 1e6 AS U_TIER
  FROM accounts
)
SELECT 'ACC-' || LPAD(ACCOUNT_INDEX::VARCHAR, 4, '0') AS ID,
       'Synthetic account ' || LPAD(ACCOUNT_INDEX::VARCHAR, 4, '0') AS NAME,
       -- Deterministic spread (5 and 8 are coprime): every market and account
       -- type is present.
       CASE MOD(ACCOUNT_INDEX, 5) WHEN 0 THEN 'Singapore' WHEN 1 THEN 'Hong Kong'
            WHEN 2 THEN 'Sydney' WHEN 3 THEN 'Tokyo' ELSE 'Seoul' END AS REGION,
       CASE MOD(ACCOUNT_INDEX, 8) WHEN 0 THEN 'Retail' WHEN 1 THEN 'Retail' WHEN 2 THEN 'Retail'
            WHEN 3 THEN 'VIP' WHEN 4 THEN 'VIP' WHEN 5 THEN 'Market Maker'
            WHEN 6 THEN 'Institutional' ELSE 'OTC Desk' END AS CATEGORY,
       ACCOUNT_INDEX,
       1 + FLOOR(U_TIER * 3) AS KYC_TIER,
       ROUND(0.2 + U_AGE * 5.8, 1) AS ACCOUNT_AGE_YEARS,
       -- Base daily probability of suspicious activity 0.4%-3%; ~15% of accounts
       -- are repeat offenders (x3).
       (0.004 + U_RATE * 0.026) * IFF(U_RATE > 0.85, 3, 1) AS BASE_SUSPICION_RATE,
       7 * (1 + FLOOR(U_REVIEW * 3)) AS REVIEW_INTERVAL_DAYS,
       0.55 + U_DISCIPLINE * 0.45 AS REVIEW_COMPLETION_PROB,
       'Active' AS STATUS
FROM draws;

CREATE TABLE RAW.ACCOUNT_DAILY AS
WITH days AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS DAY_INDEX
  FROM TABLE(GENERATOR(ROWCOUNT => 90))
), market_events AS (
  -- Two market-wide volatility spikes; every account in the market is alerted.
  SELECT * FROM VALUES (27, 'Hong Kong'), (64, 'Singapore') AS o(DAY_INDEX, REGION)
), base AS (
  SELECT a.ID AS ENTITY_ID, a.ACCOUNT_INDEX, a.CATEGORY, a.REGION, a.ACCOUNT_AGE_YEARS,
         a.BASE_SUSPICION_RATE, a.REVIEW_INTERVAL_DAYS, a.REVIEW_COMPLETION_PROB,
         d.DAY_INDEX,
         DATEADD('day', d.DAY_INDEX - 89, CURRENT_DATE()) AS EVENT_DATE,
         MOD(d.DAY_INDEX + a.ACCOUNT_INDEX * 5, a.REVIEW_INTERVAL_DAYS) AS DAYS_SINCE_REVIEW,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'sus')), 1000000) / 1e6 AS U_SUS,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'detect')), 1000000) / 1e6 AS U_DETECT,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'fp')), 1000000) / 1e6 AS U_FP,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'rule')), 1000000) / 1e6 AS U_RULE,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'done')), 1000000) / 1e6 AS U_DONE,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'trades')), 1000000) / 1e6 AS U_TRADES,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'noise')), 1000000) / 1e6 AS U_NOISE,
         MOD(ABS(HASH(a.ID, d.DAY_INDEX, 'sar')), 1000000) / 1e6 AS U_SAR,
         m.REGION IS NOT NULL AS MARKET_EVENT
  FROM RAW.ACCOUNTS a CROSS JOIN days d
  LEFT JOIN market_events m ON m.DAY_INDEX = d.DAY_INDEX AND m.REGION = a.REGION
), review AS (
  SELECT *,
         IFF(DAYS_SINCE_REVIEW = 0, 1, 0) AS REVIEW_DUE,
         IFF(DAYS_SINCE_REVIEW = 0 AND U_DONE < REVIEW_COMPLETION_PROB, 1, 0) AS REVIEW_COMPLETED,
         -- Risk drift rises between periodic reviews; weak review discipline carries it over.
         DAYS_SINCE_REVIEW / REVIEW_INTERVAL_DAYS + (1 - REVIEW_COMPLETION_PROB) AS DRIFT
  FROM base
), activity AS (
  SELECT *,
         LEAST(0.5, BASE_SUSPICION_RATE * (0.4 + 1.6 * DRIFT) * (1 + 1 / (1 + ACCOUNT_AGE_YEARS))) AS P_SUS,
         CASE WHEN U_SUS < LEAST(0.5, BASE_SUSPICION_RATE * (0.4 + 1.6 * DRIFT) * (1 + 1 / (1 + ACCOUNT_AGE_YEARS))) / 4 THEN 2
              WHEN U_SUS < LEAST(0.5, BASE_SUSPICION_RATE * (0.4 + 1.6 * DRIFT) * (1 + 1 / (1 + ACCOUNT_AGE_YEARS))) THEN 1
              ELSE 0 END AS SUSPICIOUS_COUNT
  FROM review
), alerts AS (
  SELECT *,
         -- Rules catch about 85% of suspicious activity; the rest goes undetected.
         IFF(MARKET_EVENT, 0, IFF(U_DETECT < 0.85, SUSPICIOUS_COUNT, 0)) AS CONFIRMED_COUNT,
         -- False positives: higher for high-volume account types.
         IFF(MARKET_EVENT, 1, IFF(U_FP < CASE CATEGORY WHEN 'Market Maker' THEN 0.20
                                                       WHEN 'Institutional' THEN 0.12
                                                       WHEN 'OTC Desk' THEN 0.14 ELSE 0.08 END, 1, 0)) AS FALSE_POSITIVE_COUNT
  FROM activity
), measured AS (
  SELECT *,
         CONFIRMED_COUNT + FALSE_POSITIVE_COUNT AS ALERT_COUNT,
         ROUND(CASE CATEGORY WHEN 'Market Maker' THEN 1800 WHEN 'Institutional' THEN 260
                             WHEN 'OTC Desk' THEN 40 WHEN 'VIP' THEN 120 ELSE 35 END
               * (0.7 + 0.6 * U_TRADES) * (1 + 0.8 * SUSPICIOUS_COUNT)) AS TRADE_COUNT,
         CASE CATEGORY WHEN 'Market Maker' THEN 2500 WHEN 'Institutional' THEN 42000
                       WHEN 'OTC Desk' THEN 310000 WHEN 'VIP' THEN 9500 ELSE 650 END
           * (0.8 + 0.4 * U_NOISE) AS AVG_TICKET_USD
  FROM alerts
)
SELECT ENTITY_ID || '-' || TO_CHAR(EVENT_DATE, 'YYYYMMDD') AS EVENT_ID,
       ENTITY_ID, EVENT_DATE,
       TRADE_COUNT,
       ROUND(TRADE_COUNT * AVG_TICKET_USD, 2) AS NOTIONAL_USD,
       ALERT_COUNT, CONFIRMED_COUNT,
       IFF(CONFIRMED_COUNT > 0 AND U_SAR < 0.6, 1, 0) AS SAR_FILED,
       CASE WHEN ALERT_COUNT = 0 THEN 'None'
            WHEN MARKET_EVENT THEN 'Market volatility spike'
            WHEN CATEGORY = 'Retail' THEN IFF(U_RULE < 0.5, 'Pump and dump', IFF(U_RULE < 0.8, 'Structuring', 'Mixer exposure'))
            WHEN CATEGORY = 'VIP' THEN IFF(U_RULE < 0.45, 'Structuring', IFF(U_RULE < 0.8, 'Mixer exposure', 'Sanctions screening hit'))
            WHEN CATEGORY = 'Market Maker' THEN IFF(U_RULE < 0.55, 'Wash trading', 'Spoofing')
            WHEN CATEGORY = 'Institutional' THEN IFF(U_RULE < 0.45, 'Wash trading', IFF(U_RULE < 0.8, 'Spoofing', 'Sanctions screening hit'))
            ELSE IFF(U_RULE < 0.5, 'Structuring', IFF(U_RULE < 0.75, 'Sanctions screening hit', 'Mixer exposure')) END AS DETECTION_RULE,
       REVIEW_DUE, REVIEW_COMPLETED,
       ROUND(0.5 + 2.0 * DRIFT + 3.0 * SUSPICIOUS_COUNT + U_NOISE * 0.8, 2) AS SELF_MATCH_PCT,
       ROUND(18 + 12 * DRIFT + 14 * SUSPICIOUS_COUNT + U_NOISE * 6, 1) AS CANCEL_RATIO_PCT,
       CURRENT_TIMESTAMP() AS LOADED_AT
FROM measured;

-- KYC document coverage per account (snapshot).
CREATE TABLE RAW.KYC_DOCUMENTS AS
SELECT ID AS ENTITY_ID,
       CASE CATEGORY WHEN 'Retail' THEN 'Proof of address' WHEN 'VIP' THEN 'Source of wealth'
                     WHEN 'Market Maker' THEN 'Market-making agreement'
                     WHEN 'Institutional' THEN 'Beneficial ownership' ELSE 'Source of funds' END AS DOC_TYPE,
       1 + MOD(ABS(HASH(ID, 'req')), 4) AS REQUIRED_QTY,
       MOD(ABS(HASH(ID, 'file')), 5) AS ON_FILE_QTY,
       IFF(MOD(ABS(HASH(ID, 'file')), 5) < 1 + MOD(ABS(HASH(ID, 'req')), 4),
           MOD(ABS(HASH(ID, 'pending')), 3), 0) AS PENDING_QTY,
       CURRENT_DATE() AS SNAPSHOT_DATE
FROM RAW.ACCOUNTS;
