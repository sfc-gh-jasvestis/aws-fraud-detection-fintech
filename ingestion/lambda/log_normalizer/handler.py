"""
Log Normalizer Lambda (Kinesis Firehose Data Transformation)
─────────────────────────────────────────────────────────────────────────────
Invoked by Kinesis Firehose as a transformation function before S3 delivery.
Parses raw CEX application/system log lines and normalizes to structured JSON
consumable by Snowpipe.

Input: Firehose records (base64-encoded raw log strings)
Output: Transformed records (base64-encoded JSON) → S3 landing

Log format support:
  - JSON structured logs (passthrough with enrichment)
  - Syslog-style: <priority>timestamp hostname service[pid]: message
  - Matching engine binary logs → decoded text format
"""

import base64
import json
import re
import uuid
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

ENVIRONMENT = os.environ.get("ENVIRONMENT", "demo")

# Syslog pattern: Jan 10 12:34:56 hostname service[1234]: message
SYSLOG_RE = re.compile(
    r"^(?P<month>\w{3})\s+(?P<day>\d+)\s+(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<hostname>\S+)\s+(?P<service>[^[:\s]+)(?:\[(?P<pid>\d+)\])?:\s+(?P<message>.+)$"
)

# ISO-ish pattern: 2024-01-10T12:34:56.789Z [LEVEL] service - message
ISO_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)\s+"
    r"\[?(?P<level>DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL)\]?\s+"
    r"(?P<service>\S+)\s+-\s+(?P<message>.+)$",
    re.IGNORECASE
)


def parse_log_line(raw_line: str) -> dict:
    """Attempt to parse a raw log line into a structured dict."""
    raw_line = raw_line.strip()
    now_iso  = datetime.now(timezone.utc).isoformat()

    # Try JSON first
    try:
        parsed = json.loads(raw_line)
        if isinstance(parsed, dict):
            return {
                "timestamp":   parsed.get("timestamp") or parsed.get("ts") or now_iso,
                "level":       parsed.get("level") or parsed.get("severity") or "INFO",
                "service":     parsed.get("service") or parsed.get("logger") or "unknown",
                "hostname":    parsed.get("hostname") or parsed.get("host"),
                "pid":         parsed.get("pid"),
                "message":     parsed.get("message") or parsed.get("msg") or raw_line,
                "extra":       {k: v for k, v in parsed.items()
                                if k not in ("timestamp","ts","level","severity",
                                              "service","logger","hostname","host",
                                              "pid","message","msg")},
                "log_format":  "json",
            }
    except (json.JSONDecodeError, ValueError):
        pass

    # Try ISO log format
    m = ISO_RE.match(raw_line)
    if m:
        return {
            "timestamp":  m.group("ts"),
            "level":      m.group("level").upper(),
            "service":    m.group("service"),
            "hostname":   None,
            "pid":        None,
            "message":    m.group("message"),
            "extra":      {},
            "log_format": "iso_text",
        }

    # Try syslog format
    m = SYSLOG_RE.match(raw_line)
    if m:
        return {
            "timestamp":  f"{m.group('month')} {m.group('day')} {m.group('time')}",
            "level":      "INFO",
            "service":    m.group("service"),
            "hostname":   m.group("hostname"),
            "pid":        m.group("pid"),
            "message":    m.group("message"),
            "extra":      {},
            "log_format": "syslog",
        }

    # Fallback: unstructured
    return {
        "timestamp":  now_iso,
        "level":      "UNKNOWN",
        "service":    "unknown",
        "hostname":   None,
        "pid":        None,
        "message":    raw_line,
        "extra":      {},
        "log_format": "unstructured",
    }


def transform_record(firehose_record: dict) -> dict:
    """Transform a single Firehose record. Return dict with recordId + result."""
    record_id = firehose_record["recordId"]
    raw_data  = base64.b64decode(firehose_record["data"]).decode("utf-8", errors="replace")

    try:
        normalized = parse_log_line(raw_data)
        normalized["_ingest_id"]  = str(uuid.uuid4())
        normalized["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        normalized["_environment"] = ENVIRONMENT
        normalized["_raw"]        = raw_data

        output = base64.b64encode(
            (json.dumps(normalized) + "\n").encode("utf-8")
        ).decode("utf-8")

        return {
            "recordId": record_id,
            "result":   "Ok",
            "data":     output,
        }

    except Exception as exc:
        logger.warning("Failed to transform record %s: %s", record_id, str(exc))
        # Return original data (dropped in Firehose unless error prefix is set)
        return {
            "recordId": record_id,
            "result":   "ProcessingFailed",
            "data":     firehose_record["data"],
        }


def lambda_handler(event: dict, context: Any) -> dict:
    records = event.get("records", [])
    logger.info("Log normalizer invoked | record_count=%d", len(records))

    transformed = [transform_record(r) for r in records]

    success = sum(1 for r in transformed if r["result"] == "Ok")
    failed  = len(transformed) - success
    logger.info("Transformation complete | success=%d failed=%d", success, failed)

    return {"records": transformed}
