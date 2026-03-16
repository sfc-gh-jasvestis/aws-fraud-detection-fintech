"""
Synthetic Data Generator — Digital Asset Market Surveillance
─────────────────────────────────────────────────────────────────────────────
Generates realistic synthetic CEX and on-chain data and writes it into
CRYPTO_SURVEILLANCE.RAW.* tables (or pushes to Kinesis for full-pipeline mode).

Generates four correlated datasets that reference shared entity/wallet/account IDs:

  1. CEX Trades        → RAW.CEX_TRADES_RAW
  2. CEX Orders        → RAW.CEX_ORDERS_RAW   (each trade traces to an order)
  3. CEX Balances      → RAW.CEX_BALANCES_RAW  (updated after each trade)
  4. On-Chain Transfers→ RAW.ONCHAIN_EVENTS_RAW (same wallets as CEX accounts)

Entity/wallet registry is seeded first so all tables reference consistent IDs.

Usage:
  python scripts/generate_synthetic_data.py
  python scripts/generate_synthetic_data.py --scenario pump_and_dump --trades 5000
  python scripts/generate_synthetic_data.py --ingest-method kinesis
  SURV_USE_MARKETPLACE_DATA=false python scripts/generate_synthetic_data.py

Config controlled via config/settings.py or environment variables (SURV_*).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import cfg

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Snowflake connector ──────────────────────────────────────────────────────

def get_sf_connection():
    import snowflake.connector
    conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME")
    if conn_name:
        return snowflake.connector.connect(
            connection_name=conn_name,
            database=cfg.snowflake.database,
            schema=cfg.snowflake.schema,
        )
    params: dict = {
        "account":   cfg.snowflake.account,
        "user":      cfg.snowflake.user,
        "role":      cfg.snowflake.role,
        "warehouse": cfg.snowflake.warehouse,
        "database":  cfg.snowflake.database,
        "schema":    cfg.snowflake.schema,
    }
    key_path = Path(cfg.snowflake.private_key_path).expanduser()
    if key_path.exists():
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend
        private_key = serialization.load_pem_private_key(
            key_path.read_bytes(), password=None, backend=default_backend()
        )
        params["private_key"] = private_key
    else:
        params["authenticator"] = cfg.snowflake.authenticator
    return snowflake.connector.connect(**params)


# ─── Reference universe ───────────────────────────────────────────────────────
# All four datasets reference the same shared registry so joins work cleanly.

TRADING_PAIRS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
    "MATIC-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT", "ADA-USDT",
    "DOGE-USDT", "LTC-USDT", "UNI-USDT", "AAVE-USDT", "CRV-USDT",
]

BASE_PRICES: dict[str, float] = {
    "BTC-USDT": 65_000, "ETH-USDT": 3_200, "SOL-USDT": 160,
    "BNB-USDT": 580,    "XRP-USDT": 0.55,  "MATIC-USDT": 0.85,
    "AVAX-USDT": 35,    "LINK-USDT": 18,   "DOT-USDT": 8,
    "ADA-USDT": 0.45,   "DOGE-USDT": 0.12, "LTC-USDT": 80,
    "UNI-USDT": 10,     "AAVE-USDT": 95,   "CRV-USDT": 0.50,
}

VENUES = ["CEX_MAIN", "CEX_PRO", "OTC_DESK", "DERIVATIVES"]
CHAINS = ["ethereum", "polygon", "arbitrum", "optimism", "solana", "bitcoin"]
ORDER_TYPES   = ["LIMIT", "MARKET", "STOP_LIMIT", "IOC", "FOK"]
ORDER_STATES  = ["FILLED", "CANCELLED", "PARTIAL_FILL", "EXPIRED"]
CHANGE_TYPES  = ["TRADE", "DEPOSIT", "WITHDRAWAL", "FEE", "LIQUIDATION"]

CHAIN_TOKENS: dict[str, list[tuple]] = {
    "ethereum": [
        ("0xdac17f958d2ee523a2206206994597c13d831ec7", "USDT",  6),
        ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USDC",  6),
        (None,                                          "ETH",  18),
        ("0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", "UNI",  18),
        ("0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9", "AAVE", 18),
    ],
    "polygon": [
        (None,                                          "MATIC", 18),
        ("0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "USDC",   6),
        ("0xc2132d05d31c914a87c6611c10748aeb04b58e8f", "USDT",   6),
    ],
    "arbitrum": [
        (None,                                          "ETH",   18),
        ("0xff970a61a04b1ca14834a43f5de4533ebddb5cc8", "USDC",   6),
        ("0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "USDT",   6),
    ],
    "optimism": [
        (None,                                          "ETH",   18),
        ("0x7f5c764cbc14f9669b88837ca1490cca17c31607", "USDC",   6),
    ],
    "solana":   [(None, "SOL", 9)],
    "bitcoin":  [(None, "BTC", 8)],
}

# ─── Scenario parameters ──────────────────────────────────────────────────────

SCENARIOS = {
    "all":           {"trades": 1.0, "pump": 0.12, "wash": 0.08, "structure": 0.03, "clean": 0.77},
    "pump_and_dump": {"trades": 1.0, "pump": 0.70, "wash": 0.05, "structure": 0.00, "clean": 0.25},
    "wash_trades":   {"trades": 1.0, "pump": 0.00, "wash": 0.80, "structure": 0.05, "clean": 0.15},
    "structuring":   {"trades": 1.0, "pump": 0.00, "wash": 0.00, "structure": 0.80, "clean": 0.20},
    "sanctioned":    {"trades": 0.3, "pump": 0.00, "wash": 0.00, "structure": 0.00, "clean": 1.00},
    "mixer_exposure":{"trades": 0.3, "pump": 0.00, "wash": 0.00, "structure": 0.00, "clean": 1.00},
    "clean":         {"trades": 1.0, "pump": 0.00, "wash": 0.00, "structure": 0.00, "clean": 1.00},
}


# ─── Entity / Wallet Registry ─────────────────────────────────────────────────

@dataclass
class Entity:
    entity_id:    str
    account_ids:  list[str]   # 1-3 CEX accounts per entity
    wallet_addrs: list[str]   # 2-5 on-chain wallets per entity
    kyc_tier:     str
    aml_risk:     str
    pep:          bool
    sanctioned:   bool
    account_type: str

    def primary_account(self) -> str:
        return self.account_ids[0]


# Known-bad wallet addresses injected into the universe
SANCTIONED_ADDRS = [
    "0xd882cfc20f52f2599d84b8e8d58c7fb62cfe344b",
    "0x7f367cc41522ce07553e823bf3be79a889debe1b",
    "0x1da5821544e25c636c1417ba96ade4cf6d2f9b5a",
    "0x098b716b8aaf21512996dc57eb0615e2383e2f96",
]
MIXER_ADDRS = [
    "0xba214c1c1928a32bffe790263e38b4af9bfcd659",
    "0x722122df12d4e14e13ac3b6895a86e84145b6967",
    "0xdd4c48c0b24039969fc16d1cdf626eab821d3384",
]


def _wallet() -> str:
    return "0x" + hashlib.md5(os.urandom(16)).hexdigest()[:40]


def _account() -> str:
    return f"ACC{random.randint(100_000, 999_999)}"


def build_entity_registry(n: int, rng: random.Random) -> list[Entity]:
    """Create n synthetic entities with correlated accounts and wallets."""
    tiers     = ["UNVERIFIED","BASIC","ENHANCED","INSTITUTIONAL"]
    aml_risks = ["LOW","MEDIUM","HIGH","CRITICAL"]
    tier_w    = [0.15, 0.40, 0.35, 0.10]
    risk_w    = [0.55, 0.25, 0.15, 0.05]

    entities: list[Entity] = []
    for i in range(n):
        pep       = rng.random() < 0.02
        sanctioned= rng.random() < 0.01
        aml_risk  = (
            "CRITICAL" if sanctioned else
            "HIGH"     if pep        else
            rng.choices(aml_risks, weights=risk_w, k=1)[0]
        )
        entities.append(Entity(
            entity_id    = f"ENTITY_{i+1:06d}",
            account_ids  = [_account() for _ in range(rng.randint(1, 3))],
            wallet_addrs = [_wallet()  for _ in range(rng.randint(2, 5))],
            kyc_tier     = rng.choices(tiers, weights=tier_w, k=1)[0],
            aml_risk     = aml_risk,
            pep          = pep,
            sanctioned   = sanctioned,
            account_type = rng.choice(["INDIVIDUAL","INDIVIDUAL","INDIVIDUAL","CORPORATE","INSTITUTIONAL"]),
        ))
    return entities


# ─── Timestamp helpers ────────────────────────────────────────────────────────

def _ts(minutes_ago: float = 0, jitter_s: int = 0, rng: random.Random = random) -> str:
    t = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    if jitter_s:
        t += timedelta(seconds=rng.randint(-jitter_s, jitter_s))
    return t.isoformat()


def _price(pair: str, rng: random.Random, spread_pct: float = 0.01) -> float:
    base = BASE_PRICES.get(pair, 1.0)
    return round(base * rng.uniform(1 - spread_pct, 1 + spread_pct), 6)


# ─── CEX Trade generator ─────────────────────────────────────────────────────

def gen_trades(
    entities: list[Entity],
    n: int,
    rng: random.Random,
    scenario_weights: dict,
    minutes_window: int = 10_080,  # 7 days
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns (trades, orders, balance_updates) — correlated across all three.
    Every trade links to an order; every trade triggers a balance update.
    """
    trades:   list[dict] = []
    orders:   list[dict] = []
    balances: list[dict] = []

    # Account balances tracker (for realistic balance deltas)
    bal: dict[tuple, float] = defaultdict(lambda: round(rng.uniform(100, 50_000), 2))

    pump_n     = int(n * scenario_weights.get("pump", 0))
    wash_n     = int(n * scenario_weights.get("wash", 0))
    struct_n   = int(n * scenario_weights.get("structure", 0))
    clean_n    = n - pump_n - wash_n - struct_n

    # ── Clean trades ──────────────────────────────────────────────────────────
    for _ in range(clean_n):
        entity = rng.choice(entities)
        acc_id = rng.choice(entity.account_ids)
        pair   = rng.choice(TRADING_PAIRS)
        side   = rng.choice(["BUY", "SELL"])
        price  = _price(pair, rng, spread_pct=0.015)
        qty    = round(rng.uniform(0.001, 8.0), 6)
        qval   = round(price * qty, 4)
        order_id, trade_id = str(uuid.uuid4()), str(uuid.uuid4())
        ts_m   = rng.uniform(0, minutes_window)

        order = _make_order(order_id, acc_id, entity.wallet_addrs[0], pair, side,
                            "FILLED", price, qty, qty, rng, ts_m)
        trade = _make_trade(trade_id, order_id, acc_id, entity.wallet_addrs[0],
                            pair, side, price, qty, qval, rng, ts_m)
        bal_update = _make_balance(acc_id, pair.split("-")[1] if side == "BUY" else pair.split("-")[0],
                                   qval if side == "BUY" else qty, side, bal, rng, ts_m)

        orders.append(order)
        trades.append(trade)
        balances.append(bal_update)

    # ── Pump & dump trades ────────────────────────────────────────────────────
    if pump_n > 0:
        conspirators = rng.sample(entities, k=min(5, len(entities)))
        pair         = rng.choice(["BTC-USDT", "ETH-USDT", "SOL-USDT"])
        base_p       = BASE_PRICES[pair]
        pump_share   = 2 / 3
        for i in range(pump_n):
            entity  = rng.choice(conspirators)
            acc_id  = entity.primary_account()
            is_pump = i < pump_n * pump_share
            price   = (
                base_p * (1 + (i / max(pump_n * pump_share, 1)) * 0.15)
                if is_pump
                else base_p * 1.15 * (1 - (i - pump_n * pump_share) / max(pump_n * (1 - pump_share), 1) * 0.12)
            )
            price  = round(max(price, 0.0001), 4)
            qty    = round(rng.uniform(2, 30), 4)
            side   = "BUY" if is_pump else "SELL"
            qval   = round(price * qty, 4)
            ts_m   = (pump_n - i) * 0.5 if is_pump else rng.uniform(0, 60)
            order_id, trade_id = str(uuid.uuid4()), str(uuid.uuid4())

            orders.append(_make_order(order_id, acc_id, entity.wallet_addrs[0], pair, side,
                                      "FILLED", price, qty, qty, rng, ts_m))
            trades.append(_make_trade(trade_id, order_id, acc_id, entity.wallet_addrs[0],
                                      pair, side, price, qty, qval, rng, ts_m))
            balances.append(_make_balance(acc_id, pair.split("-")[1] if side == "BUY" else pair.split("-")[0],
                                          qval, side, bal, rng, ts_m))

    # ── Wash trades ───────────────────────────────────────────────────────────
    if wash_n > 0:
        for _ in range(wash_n):
            entity = rng.choice(entities)
            acc_id = entity.primary_account()
            pair   = rng.choice(["ETH-USDT", "BTC-USDT", "SOL-USDT"])
            price  = _price(pair, rng, spread_pct=0.0005)  # tight spread
            qty    = round(rng.uniform(0.5, 10.0), 4)
            qval   = round(price * qty, 4)
            ts_m   = rng.uniform(0, minutes_window)

            for side in ["BUY", "SELL"]:
                oid, tid = str(uuid.uuid4()), str(uuid.uuid4())
                adj_price = price if side == "BUY" else price * rng.uniform(1.00005, 1.0005)
                orders.append(_make_order(oid, acc_id, entity.wallet_addrs[0], pair, side,
                                          "FILLED", adj_price, qty, qty, rng, ts_m))
                trades.append(_make_trade(tid, oid, acc_id, entity.wallet_addrs[0],
                                          pair, side, adj_price, qty, qval, rng,
                                          ts_m + rng.uniform(0, 1/60)))  # within 1 min
                balances.append(_make_balance(acc_id, "USDT", qval, side, bal, rng, ts_m))

    # ── Structuring (just-below-$10K) ─────────────────────────────────────────
    if struct_n > 0:
        for _ in range(struct_n):
            entity  = rng.choice(entities)
            acc_id  = entity.primary_account()
            pair    = rng.choice(["BTC-USDT", "ETH-USDT"])
            target  = rng.uniform(8_500, 9_999)           # just below $10K
            price   = BASE_PRICES[pair]
            qty     = round(target / price, 8)
            ts_m    = rng.uniform(0, 480)                 # same day window

            oid, tid = str(uuid.uuid4()), str(uuid.uuid4())
            orders.append(_make_order(oid, acc_id, entity.wallet_addrs[0], pair, "BUY",
                                      "FILLED", price, qty, qty, rng, ts_m))
            trades.append(_make_trade(tid, oid, acc_id, entity.wallet_addrs[0],
                                      pair, "BUY", price, qty, round(target, 4), rng, ts_m))
            balances.append(_make_balance(acc_id, "USDT", target, "BUY", bal, rng, ts_m))

    return trades, orders, balances


def _make_order(order_id, account_id, wallet_id, pair, side, state,
                price, orig_qty, filled_qty, rng, minutes_ago) -> dict:
    return {
        "event_type": "ORDER",
        "order_id":   order_id,
        "account_id": account_id,
        "wallet_id":  wallet_id,
        "venue":      rng.choice(VENUES),
        "trading_pair": pair,
        "side":       side,
        "order_type": rng.choice(ORDER_TYPES),
        "state":      state,
        "price":      round(price, 6),
        "orig_qty":   round(orig_qty, 6),
        "filled_qty": round(filled_qty, 6),
        "timestamp":  _ts(minutes_ago, jitter_s=30, rng=rng),
    }


def _make_trade(trade_id, order_id, account_id, wallet_id, pair, side,
                price, qty, quote_qty, rng, minutes_ago) -> dict:
    return {
        "event_type":   "TRADE",
        "trade_id":     trade_id,
        "order_id":     order_id,
        "account_id":   account_id,
        "wallet_id":    wallet_id,
        "venue":        rng.choice(VENUES),
        "trading_pair": pair,
        "base_asset":   pair.split("-")[0],
        "quote_asset":  pair.split("-")[1],
        "side":         side,
        "price":        round(price, 6),
        "quantity":     round(qty, 6),
        "quote_qty":    round(quote_qty, 4),
        "fee":          round(quote_qty * 0.001, 6),
        "fee_currency": "USDT",
        "is_maker":     rng.choice([True, False]),
        "timestamp":    _ts(minutes_ago, jitter_s=5, rng=rng),
    }


def _make_balance(account_id, asset, delta, side, bal_state, rng, minutes_ago) -> dict:
    key = (account_id, asset)
    if side == "BUY":
        bal_state[key] = max(0, bal_state[key] - delta)
    else:
        bal_state[key] += delta
    available = round(bal_state[key], 8)
    locked    = round(rng.uniform(0, available * 0.05), 8)
    return {
        "event_type":  "BALANCE",
        "account_id":  account_id,
        "asset":       asset,
        "available":   available,
        "locked":      locked,
        "total":       round(available + locked, 8),
        "change_type": "TRADE",
        "timestamp":   _ts(minutes_ago, jitter_s=2, rng=rng),
    }


# ─── On-Chain Transfer generator ──────────────────────────────────────────────

def gen_onchain(
    entities: list[Entity],
    n: int,
    rng: random.Random,
    scenario: str,
    minutes_window: int = 43_200,  # 30 days
) -> list[dict]:
    events: list[dict] = []

    # Weighted chain selection
    chain_weights = [0.50, 0.20, 0.15, 0.05, 0.05, 0.05]

    # Mix of normal + scenario-specific events
    is_sanctioned_scenario = scenario in ("sanctioned", "all")
    is_mixer_scenario      = scenario in ("mixer_exposure", "all")

    for i in range(n):
        entity  = rng.choice(entities)
        chain   = rng.choices(CHAINS, weights=chain_weights, k=1)[0]
        tok_addr, tok_sym, decimals = rng.choice(CHAIN_TOKENS[chain])
        from_w  = rng.choice(entity.wallet_addrs)
        to_w    = rng.choice(entity.wallet_addrs) if rng.random() < 0.3 else _wallet()

        # Inject bad addresses for specific scenarios
        inject_sanctioned = is_sanctioned_scenario and i < n * 0.05 and chain == "ethereum"
        inject_mixer      = is_mixer_scenario      and i < n * 0.08 and chain == "ethereum"

        if inject_sanctioned:
            to_w = rng.choice(SANCTIONED_ADDRS)
        elif inject_mixer:
            to_w = rng.choice(MIXER_ADDRS)

        value_raw = _rand_value(tok_sym, decimals, rng)
        events.append({
            "event_id":        str(uuid.uuid4()),
            "chain":           chain,
            "block_number":    rng.randint(19_000_000, 21_500_000),
            "block_timestamp": _ts(rng.uniform(0, minutes_window), rng=rng),
            "tx_hash":         "0x" + hashlib.sha256(os.urandom(32)).hexdigest(),
            "from_address":    from_w.lower(),
            "to_address":      to_w.lower(),
            "value_raw":       str(value_raw),
            "value_decimal":   round(value_raw / (10 ** decimals), 8),
            "token_address":   tok_addr.lower() if tok_addr else None,
            "token_symbol":    tok_sym,
            "decimals":        decimals,
            "event_type":      rng.choices(
                                   ["transfer","transfer","transfer","swap","mint","burn"],
                                   weights=[60, 60, 60, 15, 3, 2], k=1
                               )[0],
            "gas_used":        rng.randint(21_000, 300_000),
            "gas_price_gwei":  round(rng.uniform(10, 120), 2),
            "ingested_at":     datetime.now(timezone.utc).isoformat(),
            "environment":     "synthetic",
        })

    return events


def _rand_value(symbol: str, decimals: int, rng: random.Random) -> int:
    """Return a plausible raw token value for the given symbol."""
    usd_ranges = {
        "ETH":   (100,   50_000),
        "BTC":   (500,  200_000),
        "USDT":  (50,    500_000),
        "USDC":  (50,    500_000),
        "SOL":   (20,     50_000),
        "MATIC": (10,     10_000),
    }
    lo_usd, hi_usd = usd_ranges.get(symbol, (1, 10_000))
    usd_value  = rng.uniform(lo_usd, hi_usd)
    token_price = BASE_PRICES.get(f"{symbol}-USDT", 1.0)
    token_amount = usd_value / token_price
    return int(token_amount * (10 ** decimals))


# ─── Snowflake writers ────────────────────────────────────────────────────────

BATCH = 500


def _batched(lst: list, size: int = BATCH) -> Iterator[list]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _insert_raw(conn, table: str, records: list[dict], source: str = "SYNTHETIC") -> int:
    if not records:
        return 0
    cs   = conn.cursor()
    rows = [(json.dumps(r), source, "SYNTHETIC_GENERATOR") for r in records]
    total = 0
    for batch in _batched(rows):
        cs.executemany(
            f"""INSERT INTO {table} (PAYLOAD, _SOURCE, _INGEST_METHOD)
                SELECT PARSE_JSON(%s), %s, %s""",
            batch,
        )
        total += len(batch)
    cs.close()
    return total


def write_to_snowflake(conn, trades: list[dict], orders: list[dict],
                        balances: list[dict], onchain: list[dict]) -> dict[str, int]:
    db = "CRYPTO_SURVEILLANCE"
    counts = {}
    logger.info("Writing %d trades …", len(trades))
    counts["trades"]   = _insert_raw(conn, f"{db}.RAW.CEX_TRADES_RAW",    trades)
    logger.info("Writing %d orders …", len(orders))
    counts["orders"]   = _insert_raw(conn, f"{db}.RAW.CEX_ORDERS_RAW",    orders)
    logger.info("Writing %d balances …", len(balances))
    counts["balances"] = _insert_raw(conn, f"{db}.RAW.CEX_BALANCES_RAW",  balances)
    logger.info("Writing %d on-chain events …", len(onchain))
    counts["onchain"]  = _insert_raw(conn, f"{db}.RAW.ONCHAIN_EVENTS_RAW", onchain,
                                     source="SYNTHETIC_ONCHAIN")
    return counts


def write_to_kinesis(trades: list[dict], orders: list[dict],
                      balances: list[dict], onchain: list[dict]) -> dict[str, int]:
    import boto3
    prefix  = cfg.aws.stream_prefix
    region  = cfg.aws.region
    client  = boto3.client("kinesis", region_name=region)
    mapping = {
        f"{prefix}-cex-trades":   trades,
        f"{prefix}-cex-orders":   orders,
        f"{prefix}-cex-balances": balances,
        f"{prefix}-cex-logs":     onchain,
    }
    counts: dict[str, int] = {}
    for stream, records in mapping.items():
        sent = 0
        for batch in _batched(records, 500):
            resp = client.put_records(
                StreamName = stream,
                Records    = [
                    {"Data": json.dumps(r).encode(), "PartitionKey": r.get("account_id","x")}
                    for r in batch
                ],
            )
            sent += len(batch) - resp.get("FailedRecordCount", 0)
        counts[stream] = sent
        logger.info("Kinesis %s → %d records sent", stream, sent)
    return counts


# ─── Entity seeder ────────────────────────────────────────────────────────────

def seed_entity_wallet_tables(conn, entities: list[Entity]) -> None:
    db = "CRYPTO_SURVEILLANCE"
    cs = conn.cursor()
    logger.info("Seeding %d entities …", len(entities))
    for e in entities:
        cs.execute(f"""
            MERGE INTO {db}.HARMONISED.ENTITY AS tgt
            USING (SELECT
                '{e.entity_id}'    AS entity_id,
                'Synthetic {e.entity_id}' AS full_name,
                '{e.kyc_tier}'     AS kyc_tier,
                '{e.aml_risk}'     AS aml_risk_rating,
                {'TRUE' if e.pep        else 'FALSE'} AS pep_flag,
                {'TRUE' if e.sanctioned else 'FALSE'} AS sanctions_flag,
                '{e.account_type}' AS account_type,
                'ACTIVE'           AS status,
                DATEADD('day', -UNIFORM(30,730,RANDOM()), CURRENT_TIMESTAMP()) AS onboarded_at
            ) AS src ON tgt.entity_id = src.entity_id
            WHEN NOT MATCHED THEN INSERT
                (entity_id, full_name, kyc_tier, aml_risk_rating,
                 pep_flag, sanctions_flag, account_type, status, onboarded_at)
            VALUES
                (src.entity_id, src.full_name, src.kyc_tier, src.aml_risk_rating,
                 src.pep_flag, src.sanctions_flag, src.account_type, src.status, src.onboarded_at)
        """)

        for addr in e.wallet_addrs:
            wid = hashlib.md5(addr.encode()).hexdigest()
            cs.execute(f"""
                MERGE INTO {db}.HARMONISED.WALLET AS tgt
                USING (SELECT
                    '{wid}'          AS wallet_id,
                    '{addr}'         AS wallet_address,
                    'ethereum'       AS chain,
                    'EOA'            AS wallet_type,
                    '{e.entity_id}'  AS owner_entity_id,
                    {'TRUE' if e.sanctioned else 'FALSE'} AS is_sanctioned,
                    FALSE            AS is_mixer
                ) AS src ON tgt.wallet_id = src.wallet_id
                WHEN NOT MATCHED THEN INSERT
                    (wallet_id, wallet_address, chain, wallet_type,
                     owner_entity_id, is_sanctioned, is_mixer, source_system)
                VALUES
                    (src.wallet_id, src.wallet_address, src.chain, src.wallet_type,
                     src.owner_entity_id, src.is_sanctioned, src.is_mixer, 'SYNTHETIC')
            """)

    # Seed sanctioned + mixer wallets
    for addr in SANCTIONED_ADDRS:
        wid = hashlib.md5(addr.encode()).hexdigest()
        cs.execute(f"""
            MERGE INTO {db}.HARMONISED.WALLET AS tgt
            USING (SELECT '{wid}' AS wid, '{addr}' AS addr) AS src ON tgt.wallet_id = src.wid
            WHEN NOT MATCHED THEN INSERT
                (wallet_id, wallet_address, chain, wallet_type,
                 is_sanctioned, is_mixer, label, source_system)
            VALUES (src.wid, src.addr, 'ethereum', 'UNKNOWN',
                    TRUE, FALSE, 'OFAC SDN - Synthetic Demo', 'SYNTHETIC')
        """)
    for addr in MIXER_ADDRS:
        wid = hashlib.md5(addr.encode()).hexdigest()
        cs.execute(f"""
            MERGE INTO {db}.HARMONISED.WALLET AS tgt
            USING (SELECT '{wid}' AS wid, '{addr}' AS addr) AS src ON tgt.wallet_id = src.wid
            WHEN NOT MATCHED THEN INSERT
                (wallet_id, wallet_address, chain, wallet_type,
                 is_sanctioned, is_mixer, label, source_system)
            VALUES (src.wid, src.addr, 'ethereum', 'MIXER',
                    FALSE, TRUE, 'Tornado Cash-style Mixer - Synthetic', 'SYNTHETIC')
        """)
    cs.close()
    logger.info("Entity/wallet seeding complete")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate synthetic CEX + on-chain data for the surveillance demo",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--trades",         type=int,   default=cfg.data.synthetic_trade_count,
                   help="Number of trade events to generate")
    p.add_argument("--onchain",        type=int,   default=cfg.data.synthetic_onchain_count,
                   help="Number of on-chain events to generate")
    p.add_argument("--entities",       type=int,   default=cfg.data.synthetic_entity_count,
                   help="Number of synthetic entity/customer profiles")
    p.add_argument("--scenario",       default=cfg.data.synthetic_scenario,
                   choices=list(SCENARIOS.keys()),
                   help="Detection scenario mix")
    p.add_argument("--seed",           type=int,   default=cfg.data.random_seed,
                   help="Random seed for reproducibility")
    p.add_argument("--ingest-method",  default=cfg.snowflake.ingest_method,
                   choices=["direct", "kinesis"],
                   help="'direct' = INSERT into Snowflake RAW; 'kinesis' = push to Kinesis")
    p.add_argument("--dry-run",        action="store_true",
                   help="Generate data but do not write to Snowflake/Kinesis")
    p.add_argument("--output-jsonl",   type=str,   default=None,
                   help="If set, also write all events to this JSONL file")
    p.add_argument("--seed-and-refresh", action="store_true",
                   help="After inserting data, force-refresh DTs and run alert/case SPs")
    p.add_argument("--quick",          action="store_true",
                   help="Quick demo reset: 5000 trades, 1000 onchain, 50 entities (<60s)")
    return p.parse_args()


def _run_seed_and_refresh(conn) -> None:
    """Force-refresh all Dynamic Tables and run detection + case SPs."""
    cs = conn.cursor()
    logger.info("Force-refreshing Dynamic Tables...")
    for dt in [
        "CRYPTO_SURVEILLANCE.HARMONISED.TRADES",
        "CRYPTO_SURVEILLANCE.HARMONISED.ORDERS",
        "CRYPTO_SURVEILLANCE.HARMONISED.BALANCES_SNAPSHOT",
        "CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS",
        "CRYPTO_SURVEILLANCE.HARMONISED.WALLET_DISCOVERY",
        "CRYPTO_SURVEILLANCE.FEATURES.TRADE_FEATURES",
        "CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES",
        "CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES",
    ]:
        try:
            cs.execute(f"ALTER DYNAMIC TABLE IF EXISTS {dt} REFRESH")
            logger.info("  Refreshed %s", dt)
        except Exception as exc:
            logger.warning("  Skip %s: %s", dt, exc)

    logger.info("Running SP_REFRESH_ALERTS...")
    cs.execute("CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_REFRESH_ALERTS()")
    logger.info("  %s", cs.fetchone()[0])

    logger.info("Running SP_AUTO_CREATE_CASES...")
    cs.execute("CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_AUTO_CREATE_CASES()")
    logger.info("  %s", cs.fetchone()[0])

    logger.info("Resuming tasks...")
    for task in [
        "CRYPTO_SURVEILLANCE.ANALYTICS.TASK_REFRESH_ALERTS",
        "CRYPTO_SURVEILLANCE.ANALYTICS.TASK_AUTO_CREATE_CASES",
        "CRYPTO_SURVEILLANCE.ANALYTICS.TASK_SLA_CHECK",
        "CRYPTO_SURVEILLANCE.HARMONISED.TASK_SYNC_WALLET_DISCOVERY",
    ]:
        try:
            cs.execute(f"ALTER TASK IF EXISTS {task} RESUME")
        except Exception:
            pass

    logger.info("Running health check...")
    cs.execute("""
        SELECT
            (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW)     AS raw_trades,
            (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES)      AS harm_trades,
            (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY)      AS entities,
            (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS)       AS alerts,
            (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES)        AS cases
    """)
    row = cs.fetchone()
    logger.info("  RAW trades=%s | HARM trades=%s | Entities=%s | Alerts=%s | Cases=%s",
                row[0], row[1], row[2], row[3], row[4])
    cs.close()


def main() -> None:
    args = parse_args()

    if args.quick:
        args.trades   = 5000
        args.onchain  = 1000
        args.entities = 50
        args.seed_and_refresh = True
        logger.info("=== QUICK MODE: 5000 trades, 1000 onchain, 50 entities ===")

    rng  = random.Random(args.seed)

    logger.info("=== Synthetic Data Generator ===")
    logger.info("Scenario       : %s", args.scenario)
    logger.info("Entities       : %d", args.entities)
    logger.info("Trades target  : %d", args.trades)
    logger.info("On-chain target: %d", args.onchain)
    logger.info("Ingest method  : %s", args.ingest_method)
    logger.info("Dry run        : %s", args.dry_run)
    logger.info("Config mode    : %s", "MARKETPLACE+SYNTHETIC" if cfg.use_marketplace else "SYNTHETIC ONLY")

    # 1. Build entity/wallet universe
    entities = build_entity_registry(args.entities, rng)
    logger.info("Entity registry built: %d entities, %d total wallets",
                len(entities), sum(len(e.wallet_addrs) for e in entities))

    # 2. Generate correlated CEX data
    weights = SCENARIOS.get(args.scenario, SCENARIOS["all"])
    trades, orders, balances = gen_trades(entities, args.trades, rng, weights)
    logger.info("Generated: %d trades, %d orders, %d balance updates",
                len(trades), len(orders), len(balances))

    # 3. Generate on-chain events
    onchain = gen_onchain(entities, args.onchain, rng, args.scenario)
    logger.info("Generated: %d on-chain events", len(onchain))

    # 4. Optional JSONL dump
    if args.output_jsonl:
        out_path = Path(args.output_jsonl)
        with out_path.open("w") as f:
            for rec in [*trades, *orders, *balances, *onchain]:
                f.write(json.dumps(rec) + "\n")
        logger.info("Written %d records to %s",
                    len(trades) + len(orders) + len(balances) + len(onchain),
                    out_path)

    if args.dry_run:
        logger.info("Dry run complete — no data written to Snowflake/Kinesis")
        return

    # 5. Write to target
    if args.ingest_method == "kinesis":
        counts = write_to_kinesis(trades, orders, balances, onchain)
    else:
        conn = get_sf_connection()
        try:
            seed_entity_wallet_tables(conn, entities)
            counts = write_to_snowflake(conn, trades, orders, balances, onchain)
            if args.seed_and_refresh:
                _run_seed_and_refresh(conn)
        finally:
            conn.close()

    logger.info("=== Write complete ===")
    for k, v in counts.items():
        logger.info("  %-12s : %d records", k, v)
    if not args.seed_and_refresh:
        logger.info("")
        logger.info("Next: CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_REFRESH_ALERTS();")
        logger.info("      CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_AUTO_CREATE_CASES();")


if __name__ == "__main__":
    main()
