"""Config for synthetic data generator — Snowflake connection via connection_name."""
import os
from dataclasses import dataclass, field


@dataclass
class SnowflakeConfig:
    connection_name: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_CONNECTION_NAME", "<YOUR_CONNECTION>"))
    account: str = ""
    user: str = ""
    role: str = "ACCOUNTADMIN"
    warehouse: str = "WH_SURVEILLANCE"
    database: str = "CRYPTO_SURVEILLANCE"
    schema: str = "RAW"
    private_key_path: str = "~/.snowflake/rsa_key.p8"
    authenticator: str = "externalbrowser"
    ingest_method: str = "direct"


@dataclass
class DataConfig:
    synthetic_trade_count: int = 5000
    synthetic_onchain_count: int = 1000
    synthetic_entity_count: int = 50
    synthetic_scenario: str = "all"
    random_seed: int = 42


@dataclass
class AWSConfig:
    region: str = "us-west-2"
    stream_prefix: str = "crypto-surveillance"


@dataclass
class Config:
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)
    data: DataConfig = field(default_factory=DataConfig)
    aws: AWSConfig = field(default_factory=AWSConfig)
    marketplace_price_db: str = "SNOWFLAKE_PUBLIC_DATA_FREE"
    use_marketplace_data: bool = os.getenv("SURV_USE_MARKETPLACE_DATA", "false").lower() == "true"
    use_marketplace: bool = os.getenv("SURV_USE_MARKETPLACE_DATA", "false").lower() == "true"


cfg = Config()
