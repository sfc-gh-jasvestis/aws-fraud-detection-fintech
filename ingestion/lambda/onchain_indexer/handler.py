"""
On-Chain Indexer Lambda
─────────────────────────────────────────────────────────────────────────────
Triggered by EventBridge (every 1 minute). Polls third-party blockchain APIs
(Chainalysis, TRM Labs, or a self-hosted Ethereum/Polygon node via eth_getFilterLogs)
for new on-chain events relevant to the CEX's known wallet addresses.

Normalizes events to a canonical schema and writes to:
  - Kinesis Data Stream (cex-logs) for real-time path → Snowpipe Streaming
  - S3 (onchain-events/ prefix) for batch path → Snowpipe auto-ingest

Environment Variables:
  KINESIS_STREAM_NAME  : Name of the Kinesis stream
  S3_BUCKET            : Raw landing S3 bucket name
  S3_PREFIX            : S3 prefix (default: onchain-events/)
  ENVIRONMENT          : demo | staging | prod
  LOG_LEVEL            : INFO | DEBUG
"""

import json
import os
import time
import uuid
import logging
import hashlib
import boto3
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

KINESIS_STREAM  = os.environ["KINESIS_STREAM_NAME"]
S3_BUCKET       = os.environ["S3_BUCKET"]
S3_PREFIX       = os.environ.get("S3_PREFIX", "onchain-events/")
ENVIRONMENT     = os.environ.get("ENVIRONMENT", "demo")
AWS_REGION      = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")

kinesis = boto3.client("kinesis", region_name=AWS_REGION)
s3      = boto3.client("s3",      region_name=AWS_REGION)

# Chains to monitor (extend as needed)
SUPPORTED_CHAINS = ["ethereum", "polygon", "arbitrum", "optimism", "solana", "bitcoin"]

# ─── Canonical Event Schema ────────────────────────────────────────────────────

def build_onchain_event(
    chain: str,
    block_number: int,
    block_timestamp: str,
    tx_hash: str,
    from_address: str,
    to_address: str,
    value_raw: str,
    token_address: str | None,
    token_symbol: str | None,
    decimals: int,
    event_type: str,
    raw_log: dict,
) -> dict:
    value_decimal = int(value_raw) / (10 ** decimals) if value_raw else 0.0
    return {
        "event_id":        str(uuid.uuid4()),
        "chain":           chain,
        "block_number":    block_number,
        "block_timestamp": block_timestamp,
        "tx_hash":         tx_hash,
        "from_address":    from_address.lower() if from_address else None,
        "to_address":      to_address.lower()   if to_address   else None,
        "value_raw":       value_raw,
        "value_decimal":   round(value_decimal, 8),
        "token_address":   token_address.lower() if token_address else None,
        "token_symbol":    token_symbol,
        "decimals":        decimals,
        "event_type":      event_type,  # transfer | swap | mint | burn | contract_call
        "is_cex_wallet":   None,        # enriched downstream in Snowflake
        "risk_score":      None,        # enriched by TRM/Chainalysis downstream
        "ingested_at":     datetime.now(timezone.utc).isoformat(),
        "environment":     ENVIRONMENT,
        "_raw":            raw_log,
    }


# ─── Mock / Synthetic Data for Demo ───────────────────────────────────────────
# Replace with real API calls in production (e.g. Alchemy, TRM, Chainalysis)

def fetch_onchain_events_demo() -> list[dict]:
    """Generate synthetic on-chain events for the demo environment."""
    import random

    demo_wallets = [
        "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
        "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",
    ]

    chains = ["ethereum", "polygon", "arbitrum"]
    event_types = ["transfer", "swap", "transfer", "transfer", "mint", "burn"]
    token_map = {
        "ethereum": [("0xdac17f958d2ee523a2206206994597c13d831ec7", "USDT", 6),
                     ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USDC", 6),
                     (None, "ETH", 18)],
        "polygon":  [("0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "USDC", 6),
                     (None, "MATIC", 18)],
        "arbitrum": [(None, "ETH", 18),
                     ("0xff970a61a04b1ca14834a43f5de4533ebddb5cc8", "USDC", 6)],
    }

    events = []
    now = datetime.now(timezone.utc)
    for _ in range(random.randint(5, 20)):
        chain = random.choice(chains)
        token_addr, token_sym, decimals = random.choice(token_map[chain])
        value_raw = str(random.randint(1_000_000, 50_000_000_000_000_000_000))
        events.append(build_onchain_event(
            chain          = chain,
            block_number   = random.randint(19_000_000, 21_000_000),
            block_timestamp= now.isoformat(),
            tx_hash        = "0x" + hashlib.sha256(os.urandom(32)).hexdigest(),
            from_address   = random.choice(demo_wallets),
            to_address     = random.choice(demo_wallets),
            value_raw      = value_raw,
            token_address  = token_addr,
            token_symbol   = token_sym,
            decimals       = decimals,
            event_type     = random.choice(event_types),
            raw_log        = {},
        ))
    return events


# ─── Write to Kinesis ──────────────────────────────────────────────────────────

def write_to_kinesis(events: list[dict]) -> dict:
    records = [
        {
            "Data":         json.dumps(e).encode("utf-8"),
            "PartitionKey": e.get("chain", "unknown"),
        }
        for e in events
    ]

    results = {"success": 0, "failed": 0}
    for i in range(0, len(records), 500):
        batch = records[i:i + 500]
        resp = kinesis.put_records(StreamName=KINESIS_STREAM, Records=batch)
        results["failed"]  += resp.get("FailedRecordCount", 0)
        results["success"] += len(batch) - resp.get("FailedRecordCount", 0)
    return results


# ─── Write to S3 (fallback / batch path) ──────────────────────────────────────

def write_to_s3(events: list[dict]) -> str:
    now  = datetime.now(timezone.utc)
    key  = (
        f"{S3_PREFIX}chain=mixed/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hour={now.hour:02d}/{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.jsonl"
    )
    body = "\n".join(json.dumps(e) for e in events)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"),
                  ContentType="application/x-ndjson")
    return key


# ─── Handler ──────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    start = time.time()
    logger.info("On-chain indexer invoked | environment=%s", ENVIRONMENT)

    try:
        if ENVIRONMENT == "demo":
            events = fetch_onchain_events_demo()
        else:
            # Production: replace with real blockchain API integration
            # e.g. Alchemy webhooks, TRM Labs webhooks, Chainalysis Reactor
            events = fetch_onchain_events_demo()

        if not events:
            logger.info("No new on-chain events")
            return {"statusCode": 200, "events_processed": 0}

        kinesis_result = write_to_kinesis(events)
        logger.info("Kinesis write | success=%d failed=%d",
                    kinesis_result["success"], kinesis_result["failed"])

        # Write failed records to S3 as fallback
        if kinesis_result["failed"] > 0:
            s3_key = write_to_s3(events)
            logger.warning("Fallback S3 write | key=%s", s3_key)

        elapsed = round((time.time() - start) * 1000)
        logger.info("Completed | events=%d elapsed_ms=%d", len(events), elapsed)

        return {
            "statusCode":       200,
            "events_processed": len(events),
            "kinesis":          kinesis_result,
            "elapsed_ms":       elapsed,
        }

    except Exception as exc:
        logger.error("On-chain indexer failed: %s", str(exc), exc_info=True)
        raise
