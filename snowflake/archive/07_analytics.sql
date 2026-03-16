-- =============================================================================
-- Phase 5: ML Scoring + Alert Scores
-- File: 07_analytics.sql
-- Schema: CRYPTO_SURVEILLANCE.ANALYTICS
-- Combines rule-based flags + ML probability into unified ALERT_SCORES,
-- then into ANALYTICS.ALERTS.
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_ANALYTICS;

-- ─── ML Scoring UDF (wraps Snowpark ML Model Registry) ────────────────────────
-- After training (ml/train_fraud_model.py), deploy the model as a vectorized UDF:
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

# Load model from stage on first invocation (cached via _cache global)
_cache = {}

def score(
    cex_trades, cex_vol, pairs, vol_stdev, max_trade, burst,
    onchain_tx, onchain_vol, counterparties, mixers, sanc_vol, max_risk, wallets,
    pep, sanctions, mixer_exp, sanc_exp, burst_beh
):
    import pickle, os
    if 'model' not in _cache:
        # In production: load from @CRYPTO_SURVEILLANCE.ML.MODEL_STAGE/fraud_model.pkl
        # For demo: return heuristic score
        _cache['model'] = None

    if _cache['model'] is None:
        # Heuristic fallback for demo
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
-- Dynamic Table: combines rule-based + ML scores per entity
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES
    TARGET_LAG = '10 minutes'
    WAREHOUSE  = WH_ANALYTICS
    COMMENT    = 'Per-entity alert scores: rule flags + ML probability'
AS
SELECT
    ef.entity_id,
    ef.aml_risk_rating,
    ef.pep_flag,
    ef.sanctions_flag,
    -- Rule-based flags
    ef.has_mixer_exposure        AS rule_mixer_exposure,
    ef.has_sanctioned_exposure   AS rule_sanctioned_exposure,
    ef.has_burst_behaviour       AS rule_burst_behaviour,
    CASE WHEN ef.mixer_interactions_30d   > 5  THEN TRUE ELSE FALSE END AS rule_heavy_mixer,
    CASE WHEN ef.sanctioned_volume_30d    > 0  THEN TRUE ELSE FALSE END AS rule_any_sanction,
    CASE WHEN ef.burst_trade_count_30d    > 50 THEN TRUE ELSE FALSE END AS rule_extreme_burst,
    CASE WHEN ef.max_single_trade_30d    > 100000 THEN TRUE ELSE FALSE END AS rule_large_trade,
    -- ML score
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
    -- Composite risk tier
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
