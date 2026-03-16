"""
Global configuration for the Digital Asset Market Surveillance platform.
─────────────────────────────────────────────────────────────────────────────
All runtime behaviour is controlled here — no magic strings scattered across
the codebase. Import from any script:

    from config.settings import cfg

The USE_MARKETPLACE_DATA flag is the main feature gate:
  - False  → pure synthetic data, no external dependencies (default for demo)
  - True   → Marketplace views are joined into HARMONISED and FEATURES layers
              for richer, more realistic signals (POC / production mode)

Precedence (highest → lowest):
  1. Environment variables (SURV_*)
  2. .env file in project root (if python-dotenv installed)
  3. Defaults below
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env", override=False)
except ImportError:
    pass


def _env(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key, default)


def _env_bool(key: str, default: bool = False) -> bool:
    val = os.environ.get(key, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")


def _env_int(key: str, default: int = 0) -> int:
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default


# ─── Master feature flag ──────────────────────────────────────────────────────

@dataclass
class DataSourceConfig:
    """
    Controls which data sources are active.

    USE_MARKETPLACE_DATA:
        False (default) → all analytics use synthetic data only.
                          Safe for booth demo with zero external dependencies.
        True            → Marketplace views (VW_MARKET_PRICES, VW_ONCHAIN_LABELS, etc.)
                          are joined into HARMONISED and FEATURES for richer signals.
                          Requires at least one Marketplace listing subscribed in
                          target Snowflake account (see docs/marketplace_sources.md).
    """
    use_marketplace_data: bool = field(
        default_factory=lambda: _env_bool("SURV_USE_MARKETPLACE_DATA", False)
    )

    # Which Marketplace providers are available (comma-separated DB names)
    marketplace_price_db:   str = field(
        default_factory=lambda: _env("SURV_MARKETPLACE_PRICE_DB",   "SNOWFLAKE_PUBLIC_DATA_FREE")
    )
    marketplace_onchain_db: str = field(
        default_factory=lambda: _env("SURV_MARKETPLACE_ONCHAIN_DB", "ALLIUM__ONCHAIN_ANALYTICS")
    )
    marketplace_kyc_db:     str = field(
        default_factory=lambda: _env("SURV_MARKETPLACE_KYC_DB",     "TRM__CRYPTO_RISK_INTELLIGENCE")
    )

    # Synthetic data defaults
    synthetic_trade_count:    int  = field(default_factory=lambda: _env_int("SURV_SYNTHETIC_TRADES",     50_000))
    synthetic_onchain_count:  int  = field(default_factory=lambda: _env_int("SURV_SYNTHETIC_ONCHAIN",    10_000))
    synthetic_entity_count:   int  = field(default_factory=lambda: _env_int("SURV_SYNTHETIC_ENTITIES",      200))
    synthetic_wallet_count:   int  = field(default_factory=lambda: _env_int("SURV_SYNTHETIC_WALLETS",      1000))
    synthetic_scenario:       str  = field(
        default_factory=lambda: _env("SURV_SYNTHETIC_SCENARIO", "all")
    )
    # Seed for reproducibility (set to fixed value for deterministic demo datasets)
    random_seed: int | None = field(
        default_factory=lambda: _env_int("SURV_RANDOM_SEED", 42) or None
    )


# ─── Snowflake connection config ──────────────────────────────────────────────

@dataclass
class SnowflakeConfig:
    account:   str = field(default_factory=lambda: _env("SURV_SF_ACCOUNT",   "demo43"))
    user:      str = field(default_factory=lambda: _env("SURV_SF_USER",      "SURVEILLANCE_ADMIN_SVC"))
    role:      str = field(default_factory=lambda: _env("SURV_SF_ROLE",      "SURVEILLANCE_ADMIN"))
    warehouse: str = field(default_factory=lambda: _env("SURV_SF_WAREHOUSE", "WH_SURVEILLANCE"))
    database:  str = field(default_factory=lambda: _env("SURV_SF_DATABASE",  "CRYPTO_SURVEILLANCE"))
    schema:    str = field(default_factory=lambda: _env("SURV_SF_SCHEMA",    "RAW"))
    private_key_path: str = field(
        default_factory=lambda: _env("SURV_SF_PRIVATE_KEY", "~/.snowflake/rsa_key.p8")
    )
    authenticator: str = field(
        default_factory=lambda: _env("SURV_SF_AUTH", "externalbrowser")
    )

    # Ingest method: 'direct' = INSERT into RAW tables,
    #                'kinesis' = push to Kinesis → Snowpipe Streaming
    ingest_method: str = field(
        default_factory=lambda: _env("SURV_INGEST_METHOD", "direct")
    )


# ─── AWS config ───────────────────────────────────────────────────────────────

@dataclass
class AWSConfig:
    region:      str = field(default_factory=lambda: _env("AWS_DEFAULT_REGION", "us-west-2"))
    account_id:  str = field(default_factory=lambda: _env("AWS_ACCOUNT_ID",     "018437500440"))
    stream_prefix: str = field(
        default_factory=lambda: _env("SURV_KINESIS_STREAM_PREFIX", "crypto-surveillance")
    )


# ─── Marketplace schema/view names ────────────────────────────────────────────

@dataclass
class MarketplaceViewConfig:
    """
    Fully-qualified names of Marketplace-backed views created by
    sql/setup/10_marketplace_sources.sql.

    Used by HARMONISED Dynamic Tables and FEATURES Dynamic Tables
    when use_marketplace_data=True.
    """
    price_view:         str = "CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKET_PRICES"
    onchain_label_view: str = "CRYPTO_SURVEILLANCE.HARMONISED.VW_ONCHAIN_LABELS"
    wallet_risk_view:   str = "CRYPTO_SURVEILLANCE.HARMONISED.VW_WALLET_RISK_SCORES"
    sanctions_view:     str = "CRYPTO_SURVEILLANCE.HARMONISED.VW_SANCTIONS_LIST"

    # Synthetic fallback view names (always present regardless of flag)
    synthetic_price_view:   str = "CRYPTO_SURVEILLANCE.HARMONISED.VW_SYNTHETIC_PRICES"
    synthetic_label_view:   str = "CRYPTO_SURVEILLANCE.HARMONISED.VW_SYNTHETIC_WALLET_LABELS"

    def price_source(self, use_marketplace: bool) -> str:
        """Return the correct price view based on the feature flag."""
        return self.price_view if use_marketplace else self.synthetic_price_view

    def label_source(self, use_marketplace: bool) -> str:
        return self.onchain_label_view if use_marketplace else self.synthetic_label_view


# ─── Top-level config object ──────────────────────────────────────────────────

@dataclass
class Config:
    data:        DataSourceConfig     = field(default_factory=DataSourceConfig)
    snowflake:   SnowflakeConfig      = field(default_factory=SnowflakeConfig)
    aws:         AWSConfig            = field(default_factory=AWSConfig)
    marketplace: MarketplaceViewConfig = field(default_factory=MarketplaceViewConfig)

    # Convenience passthrough
    @property
    def use_marketplace(self) -> bool:
        return self.data.use_marketplace_data

    def describe(self) -> str:
        mode = "MARKETPLACE + SYNTHETIC" if self.use_marketplace else "SYNTHETIC ONLY"
        return (
            f"Data mode       : {mode}\n"
            f"SF account      : {self.snowflake.account}\n"
            f"SF ingest method: {self.snowflake.ingest_method}\n"
            f"Synthetic trades: {self.data.synthetic_trade_count:,}\n"
            f"Scenario        : {self.data.synthetic_scenario}\n"
            f"Random seed     : {self.data.random_seed}\n"
        )


# Singleton — import as `from config.settings import cfg`
cfg = Config()


# ─── Quick sanity print when run directly ─────────────────────────────────────
if __name__ == "__main__":
    print("=== Surveillance Platform Config ===")
    print(cfg.describe())
    print(f"\nMarketplace price source : {cfg.marketplace.price_source(cfg.use_marketplace)}")
    print(f"Marketplace label source : {cfg.marketplace.label_source(cfg.use_marketplace)}")
