"""
Snowpark ML: Fraud / AML Model Training
─────────────────────────────────────────────────────────────────────────────
Trains an XGBoost classifier on FEATURES.ENTITY_FEATURES to predict
high-risk entities (AML / financial crime risk).

Run via:
  snowpark-python train_fraud_model.py  (in a Snowflake notebook or local)
  or schedule as a Snowflake Task calling a stored procedure.

Output:
  - Model saved to Snowflake Model Registry (CRYPTO_SURVEILLANCE.ML)
  - UDF registered as CRYPTO_SURVEILLANCE.ML.FRAUD_RISK_SCORE(entity_id)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Snowpark + Snowpark ML
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, lit
from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.modeling.preprocessing import StandardScaler, LabelEncoder
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.ml.modeling.model_selection import cross_val_score
from snowflake.ml.registry import Registry

# ─── Configuration ────────────────────────────────────────────────────────────

SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "<SF_CONNECTION>")
SNOWFLAKE_USER    = os.environ.get("SNOWFLAKE_USER",    "SURVEILLANCE_ML_SVC")
SNOWFLAKE_ROLE    = "SURVEILLANCE_ML"
SNOWFLAKE_WH      = "WH_ML"
DATABASE          = "CRYPTO_SURVEILLANCE"
SCHEMA            = "ML"

FEATURE_TABLE     = "CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES"
MODEL_NAME        = "ENTITY_FRAUD_RISK_CLASSIFIER"
MODEL_VERSION     = datetime.now().strftime("%Y%m%d_%H%M")

# Feature columns used for training
NUMERIC_FEATURES = [
    "CEX_TRADE_COUNT_30D",
    "CEX_VOLUME_USD_30D",
    "UNIQUE_PAIRS_30D",
    "TRADE_SIZE_VOLATILITY_30D",
    "MAX_SINGLE_TRADE_30D",
    "BURST_TRADE_COUNT_30D",
    "ONCHAIN_TX_COUNT_30D",
    "ONCHAIN_VOLUME_30D",
    "UNIQUE_COUNTERPARTIES_30D",
    "MIXER_INTERACTIONS_30D",
    "SANCTIONED_VOLUME_30D",
    "MAX_COUNTERPARTY_RISK_30D",
    "WALLET_COUNT",
]

BOOLEAN_FEATURES = [
    "PEP_FLAG",
    "SANCTIONS_FLAG",
    "HAS_MIXER_EXPOSURE",
    "HAS_SANCTIONED_EXPOSURE",
    "HAS_BURST_BEHAVIOUR",
]

CATEGORICAL_FEATURES = [
    "KYC_TIER",
    "ACCOUNT_TYPE",
]

ALL_FEATURES = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES
LABEL_COLUMN = "RISK_LABEL"  # Derived: HIGH_RISK=1 if aml_risk_rating in (HIGH,CRITICAL) else 0


def get_session() -> Session:
    connection_params = {
        "account":   SNOWFLAKE_ACCOUNT,
        "user":      SNOWFLAKE_USER,
        "role":      SNOWFLAKE_ROLE,
        "warehouse": SNOWFLAKE_WH,
        "database":  DATABASE,
        "schema":    SCHEMA,
    }

    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "~/.snowflake/rsa_key.p8")
    if os.path.exists(os.path.expanduser(private_key_path)):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend
        with open(os.path.expanduser(private_key_path), "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        connection_params["private_key"] = private_key
    else:
        connection_params["authenticator"] = "externalbrowser"

    return Session.builder.configs(connection_params).create()


def load_training_data(session: Session):
    """Load features and create binary risk label from AML rating."""
    logger.info("Loading training data from %s", FEATURE_TABLE)

    df = session.table(FEATURE_TABLE)

    # Create binary label: 1 = high/critical risk, 0 = low/medium/unknown
    df = df.with_column(
        LABEL_COLUMN,
        when(col("AML_RISK_RATING").isin(["HIGH", "CRITICAL"]), lit(1))
        .otherwise(lit(0))
    )

    # Cast booleans to int for XGBoost
    for feat in BOOLEAN_FEATURES:
        df = df.with_column(feat, col(feat).cast("int"))

    logger.info("Training dataset: %d rows", df.count())
    return df


def build_pipeline() -> Pipeline:
    """Build Snowpark ML preprocessing + XGBoost pipeline."""
    return Pipeline(
        steps=[
            ("scaler",     StandardScaler(
                input_cols  = NUMERIC_FEATURES,
                output_cols = [f"{c}_SCALED" for c in NUMERIC_FEATURES],
            )),
            ("classifier", XGBClassifier(
                input_cols       = [f"{c}_SCALED" for c in NUMERIC_FEATURES] + BOOLEAN_FEATURES,
                label_cols       = [LABEL_COLUMN],
                output_cols      = ["PREDICTED_RISK", "FRAUD_PROBABILITY"],
                n_estimators     = 300,
                max_depth        = 6,
                learning_rate    = 0.05,
                subsample        = 0.8,
                colsample_bytree = 0.8,
                scale_pos_weight = 10,    # class imbalance weight
                eval_metric      = "aucpr",
                random_state     = 42,
            )),
        ]
    )


def train_and_register(session: Session) -> str:
    df       = load_training_data(session)
    pipeline = build_pipeline()

    logger.info("Training XGBoost pipeline on Snowpark ML...")
    pipeline.fit(df)

    # Cross-validation AUC
    cv_scores = cross_val_score(
        estimator = pipeline,
        X         = df,
        n_folds   = 5,
        scoring   = "roc_auc",
    )
    logger.info("CV AUC scores: %s | Mean: %.4f", cv_scores, sum(cv_scores) / len(cv_scores))

    # Register in Snowflake Model Registry
    registry = Registry(session=session, database_name=DATABASE, schema_name=SCHEMA)
    model_ref = registry.log_model(
        model          = pipeline,
        model_name     = MODEL_NAME,
        version_name   = MODEL_VERSION,
        comment        = (
            f"XGBoost fraud/AML risk classifier | "
            f"CV AUC={sum(cv_scores)/len(cv_scores):.4f} | "
            f"trained on {df.count()} entities"
        ),
        metrics        = {"cv_auc_mean": sum(cv_scores) / len(cv_scores)},
        sample_input_data = df.limit(100),
    )

    logger.info("Model registered: %s v%s", MODEL_NAME, MODEL_VERSION)
    return f"{DATABASE}.{SCHEMA}.{MODEL_NAME}/{MODEL_VERSION}"


def main() -> None:
    session = get_session()
    try:
        model_path = train_and_register(session)
        logger.info("Training complete. Model: %s", model_path)
    finally:
        session.close()


if __name__ == "__main__":
    main()
