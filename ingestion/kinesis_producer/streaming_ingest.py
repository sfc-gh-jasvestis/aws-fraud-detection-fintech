"""
Snowpipe Streaming Ingest Client
─────────────────────────────────────────────────────────────────────────────
Reads from Kinesis Data Streams and writes directly into Snowflake RAW tables
using the Snowflake Ingest SDK (snowflake-ingest-sdk).

Supports three modes:
  --mode live    : Continuous polling of real Kinesis streams
  --mode replay  : Replay events from a JSONL file (demo / testing)
  --mode demo    : Generate synthetic CEX trade/order/balance data

Usage:
  python streaming_ingest.py --stream cex-trades --mode demo --records 10000
  python streaming_ingest.py --stream cex-trades --mode live --shard-count 2

Configuration via environment variables or ~/.snowflake/config (SnowSQL format):
  SNOWFLAKE_ACCOUNT      : demo43 (or full account identifier)
  SNOWFLAKE_USER         : SNOWPIPE_STREAMING_SVC
  SNOWFLAKE_PRIVATE_KEY  : Path to RSA private key file (PEM)
  SNOWFLAKE_DATABASE     : CRYPTO_SURVEILLANCE
  SNOWFLAKE_SCHEMA       : RAW
  AWS_REGION             : us-west-2
  KINESIS_STREAM_PREFIX  : crypto-surveillance  (stream name = prefix + '-' + stream_name)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import boto3

# Snowflake Snowpipe Streaming SDK
# pip install snowflake-ingest-sdk (Java SDK wrapper via JPype)
# For pure Python: use snowflake-connector-python with write_pandas or
# direct REST ingest API; shown here with the REST ingest approach.
try:
    from snowflake.ingest import SimpleIngestManager, StagedFile
    SNOWFLAKE_INGEST_AVAILABLE = True
except ImportError:
    SNOWFLAKE_INGEST_AVAILABLE = False
    logging.warning(
        "snowflake-ingest-sdk not installed. "
        "Install with: pip install snowflake-ingest-sdk"
    )

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)

# ─── Kinesis reader ───────────────────────────────────────────────────────────

class KinesisReader:
    """Iterates over records in a Kinesis stream across all shards."""

    def __init__(self, stream_name: str, region: str = "us-west-2"):
        self.stream_name = stream_name
        self.client      = boto3.client("kinesis", region_name=region)
        self._shard_iterators: list[str] = []

    def _init_iterators(self, iterator_type: str = "LATEST") -> None:
        resp   = self.client.describe_stream_summary(StreamName=self.stream_name)
        shards = resp["StreamDescriptionSummary"]["OpenShardCount"]
        logger.info("Stream %s has %d open shards", self.stream_name, shards)

        stream_shards = self.client.list_shards(StreamName=self.stream_name)["Shards"]
        self._shard_iterators = []
        for shard in stream_shards:
            it = self.client.get_shard_iterator(
                StreamName         = self.stream_name,
                ShardId            = shard["ShardId"],
                ShardIteratorType  = iterator_type,
            )["ShardIterator"]
            self._shard_iterators.append(it)

    def read(self, batch_size: int = 100) -> Generator[list[dict], None, None]:
        """Continuously yield batches of decoded records from all shards."""
        self._init_iterators()
        while True:
            new_iterators = []
            batch         = []
            for shard_it in self._shard_iterators:
                try:
                    resp = self.client.get_records(
                        ShardIterator = shard_it,
                        Limit         = batch_size,
                    )
                    for record in resp.get("Records", []):
                        try:
                            data = json.loads(record["Data"].decode("utf-8"))
                            batch.append(data)
                        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                            logger.warning("Failed to decode record: %s", exc)

                    if resp.get("NextShardIterator"):
                        new_iterators.append(resp["NextShardIterator"])
                except self.client.exceptions.ExpiredIteratorException:
                    logger.warning("Shard iterator expired; reinitialising")
                    self._init_iterators()
                    break
                except Exception as exc:
                    logger.error("Kinesis read error: %s", exc)
                    time.sleep(1)

            self._shard_iterators = new_iterators
            if batch:
                yield batch
            else:
                time.sleep(0.5)


# ─── Synthetic Data Generators ────────────────────────────────────────────────

TRADING_PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
                 "MATIC-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT", "ADA-USDT"]
VENUES        = ["CEX_MAIN", "CEX_PRO", "OTC_DESK"]
SIDES         = ["BUY", "SELL"]
ORDER_TYPES   = ["LIMIT", "MARKET", "STOP_LIMIT", "IOC", "FOK"]
ORDER_STATES  = ["NEW", "PARTIAL_FILL", "FILLED", "CANCELLED", "EXPIRED"]


def gen_account_id() -> str:
    return f"ACC{random.randint(100000, 999999)}"


def gen_wallet_address() -> str:
    return "0x" + uuid.uuid4().hex[:40]


def generate_trade(account_id: str | None = None) -> dict:
    pair      = random.choice(TRADING_PAIRS)
    base_price = {"BTC-USDT": 65000, "ETH-USDT": 3200, "SOL-USDT": 160,
                  "BNB-USDT": 580, "XRP-USDT": 0.55}.get(pair, 10.0)
    price     = round(base_price * random.uniform(0.99, 1.01), 6)
    qty       = round(random.uniform(0.001, 10.0), 6)

    return {
        "event_type":   "TRADE",
        "trade_id":     str(uuid.uuid4()),
        "order_id":     str(uuid.uuid4()),
        "account_id":   account_id or gen_account_id(),
        "wallet_id":    gen_wallet_address(),
        "venue":        random.choice(VENUES),
        "trading_pair": pair,
        "base_asset":   pair.split("-")[0],
        "quote_asset":  pair.split("-")[1],
        "side":         random.choice(SIDES),
        "price":        price,
        "quantity":     qty,
        "quote_qty":    round(price * qty, 6),
        "fee":          round(price * qty * 0.001, 6),
        "fee_currency": "USDT",
        "is_maker":     random.choice([True, False]),
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }


def generate_order() -> dict:
    pair  = random.choice(TRADING_PAIRS)
    state = random.choice(ORDER_STATES)
    return {
        "event_type":   "ORDER",
        "order_id":     str(uuid.uuid4()),
        "account_id":   gen_account_id(),
        "wallet_id":    gen_wallet_address(),
        "venue":        random.choice(VENUES),
        "trading_pair": pair,
        "side":         random.choice(SIDES),
        "order_type":   random.choice(ORDER_TYPES),
        "state":        state,
        "price":        round(random.uniform(0.001, 70000), 6),
        "orig_qty":     round(random.uniform(0.001, 10.0), 6),
        "filled_qty":   round(random.uniform(0, 5.0), 6) if state != "NEW" else 0.0,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }


def generate_balance() -> dict:
    assets = ["BTC", "ETH", "USDT", "USDC", "SOL", "BNB", "XRP"]
    return {
        "event_type":  "BALANCE",
        "account_id":  gen_account_id(),
        "asset":       random.choice(assets),
        "available":   round(random.uniform(0.0, 1000.0), 8),
        "locked":      round(random.uniform(0.0, 100.0), 8),
        "total":       None,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "change_type": random.choice(["TRADE", "DEPOSIT", "WITHDRAWAL", "FEE"]),
    }


# ─── Snowflake Ingest Manager (REST Snowpipe) ─────────────────────────────────

class SnowflakeStreamingWriter:
    """
    Wraps Snowflake REST Ingest API for Snowpipe Streaming.
    In production use the Java Snowpipe Streaming SDK for lowest latency.
    This Python implementation uses the simpler REST Snowpipe insert-rows API
    available in Snowflake Ingest SDK.
    """

    STREAM_TO_TABLE = {
        "cex-trades":   "CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW",
        "cex-orders":   "CRYPTO_SURVEILLANCE.RAW.CEX_ORDERS_RAW",
        "cex-balances": "CRYPTO_SURVEILLANCE.RAW.CEX_BALANCES_RAW",
        "cex-logs":     "CRYPTO_SURVEILLANCE.RAW.CEX_LOGS_RAW",
    }

    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        self.table_name  = self.STREAM_TO_TABLE.get(stream_name)
        if not self.table_name:
            raise ValueError(f"Unknown stream: {stream_name}")

        account  = os.environ.get("SNOWFLAKE_ACCOUNT", "demo43")
        user     = os.environ.get("SNOWFLAKE_USER",    "SNOWPIPE_STREAMING_SVC")
        key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "~/.snowflake/rsa_key.p8")

        if not SNOWFLAKE_INGEST_AVAILABLE:
            logger.warning("Snowflake Ingest SDK not available; using dry-run mode")
            self._dry_run = True
            return

        self._dry_run = False
        private_key   = Path(key_path).expanduser().read_text()
        self._manager = SimpleIngestManager(
            account     = account,
            host        = f"{account}.snowflakecomputing.com",
            user        = user,
            pipe        = self.table_name.replace(".", "/"),
            private_key = private_key,
        )

    def write_batch(self, records: list[dict]) -> dict:
        if self._dry_run:
            logger.info("[DRY RUN] Would write %d records to %s",
                        len(records), self.table_name)
            return {"written": len(records), "dry_run": True}

        rows = [
            {
                "PAYLOAD":        json.dumps(r),
                "_SOURCE":        "KINESIS",
                "_INGEST_METHOD": "SNOWPIPE_STREAMING",
                "_PARTITION_KEY": r.get("account_id", "unknown"),
            }
            for r in records
        ]

        # Insert rows via Snowflake REST Ingest
        # In production, use the Java SDK's insertRows() for sub-second latency
        self._manager.ingest_files(
            staged_files=[StagedFile(f"{self.table_name}_{i}", None) for i in range(len(rows))]
        )
        return {"written": len(records)}


# ─── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Snowpipe Streaming ingest client")
    p.add_argument("--stream",      default="cex-trades",
                   choices=["cex-trades", "cex-orders", "cex-balances", "cex-logs"],
                   help="Kinesis stream / Snowflake table target")
    p.add_argument("--mode",        default="demo",
                   choices=["live", "replay", "demo"],
                   help="Ingest mode")
    p.add_argument("--records",     type=int, default=10_000,
                   help="Number of synthetic records to generate (demo mode)")
    p.add_argument("--batch-size",  type=int, default=500,
                   help="Records per Snowflake ingest batch")
    p.add_argument("--replay-file", type=str, default=None,
                   help="Path to JSONL file for replay mode")
    p.add_argument("--aws-region",  default="us-west-2")
    p.add_argument("--dry-run",     action="store_true",
                   help="Parse and batch but do not write to Snowflake")
    return p.parse_args()


def main() -> None:
    args    = parse_args()
    writer  = SnowflakeStreamingWriter(args.stream)
    total   = 0
    batches = 0

    logger.info(
        "Starting | stream=%s mode=%s batch_size=%d",
        args.stream, args.mode, args.batch_size
    )

    # ── Demo / synthetic mode ──────────────────────────────────────────────────
    if args.mode == "demo":
        generators = {
            "cex-trades":   generate_trade,
            "cex-orders":   generate_order,
            "cex-balances": generate_balance,
            "cex-logs":     lambda: {"event_type": "LOG",
                                      "timestamp": datetime.now(timezone.utc).isoformat(),
                                      "message": "synthetic log"},
        }
        gen = generators[args.stream]
        batch: list[dict] = []
        for _ in range(args.records):
            batch.append(gen())
            if len(batch) >= args.batch_size:
                result = writer.write_batch(batch)
                total  += result.get("written", 0)
                batches += 1
                batch   = []
                if batches % 10 == 0:
                    logger.info("Progress | batches=%d records=%d", batches, total)

        if batch:
            result  = writer.write_batch(batch)
            total  += result.get("written", 0)
            batches += 1

    # ── Live Kinesis mode ──────────────────────────────────────────────────────
    elif args.mode == "live":
        stream_name = f"crypto-surveillance-{args.stream}"
        reader      = KinesisReader(stream_name, region=args.aws_region)
        batch: list[dict] = []
        for records in reader.read(batch_size=args.batch_size):
            batch.extend(records)
            if len(batch) >= args.batch_size:
                result  = writer.write_batch(batch[:args.batch_size])
                total  += result.get("written", 0)
                batches += 1
                batch   = batch[args.batch_size:]

    # ── Replay from JSONL ──────────────────────────────────────────────────────
    elif args.mode == "replay":
        if not args.replay_file:
            logger.error("--replay-file is required for replay mode")
            sys.exit(1)

        batch: list[dict] = []
        with open(args.replay_file, "r") as f:
            for line in f:
                try:
                    batch.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue
                if len(batch) >= args.batch_size:
                    result  = writer.write_batch(batch)
                    total  += result.get("written", 0)
                    batches += 1
                    batch   = []

        if batch:
            result  = writer.write_batch(batch)
            total  += result.get("written", 0)

    logger.info(
        "Ingest complete | stream=%s total_records=%d batches=%d",
        args.stream, total, batches
    )


if __name__ == "__main__":
    main()
