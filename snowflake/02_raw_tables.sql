-- =============================================================================
-- Phase 3: RAW Layer DDL
-- File: 02_raw_tables.sql
-- Schema: CRYPTO_SURVEILLANCE.RAW
-- Pattern: schema-on-read VARIANT columns for maximum ingestion flexibility;
--          metadata columns (_LOAD_TS, _SOURCE, _INGEST_METHOD) for lineage.
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE SCHEMA RAW;
USE WAREHOUSE WH_SURVEILLANCE;

-- ─── CEX Trade Events ─────────────────────────────────────────────────────────
-- Source: Kinesis stream cex-trades / S3 prefix events/trades/
-- Schema: Matching engine trade executions (fill events)
CREATE TABLE IF NOT EXISTS CEX_TRADES_RAW (
    _RECORD_ID       STRING         DEFAULT UUID_STRING()  NOT NULL,
    _LOAD_TS         TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    _SOURCE          STRING         NOT NULL  COMMENT 'kinesis|s3|snowpipe_streaming',
    _INGEST_METHOD   STRING         NOT NULL  COMMENT 'snowpipe|streaming_sdk',
    _PARTITION_KEY   STRING         COMMENT 'Kinesis partition key or S3 key',
    _FILE_NAME       STRING         COMMENT 'S3 object key if batch ingest',
    PAYLOAD          VARIANT        NOT NULL  COMMENT 'Raw JSON payload from source',
    -- Promoted key fields for partition pruning
    EVENT_TS         TIMESTAMP_LTZ  AS (TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)),
    TRADE_ID         STRING         AS (PAYLOAD:trade_id::STRING),
    VENUE            STRING         AS (PAYLOAD:venue::STRING)
)
CLUSTER BY (DATE_TRUNC('DAY', EVENT_TS), VENUE)
DATA_RETENTION_TIME_IN_DAYS = 3
COMMENT = 'Raw CEX trade execution events (Kinesis cex-trades → Snowpipe Streaming)';

-- ─── CEX Order Events ─────────────────────────────────────────────────────────
-- Source: Kinesis stream cex-orders
-- Schema: Order lifecycle (new, cancel, modify, partial-fill, full-fill)
CREATE TABLE IF NOT EXISTS CEX_ORDERS_RAW (
    _RECORD_ID       STRING         DEFAULT UUID_STRING()  NOT NULL,
    _LOAD_TS         TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    _SOURCE          STRING         NOT NULL,
    _INGEST_METHOD   STRING         NOT NULL,
    _PARTITION_KEY   STRING,
    _FILE_NAME       STRING,
    PAYLOAD          VARIANT        NOT NULL,
    EVENT_TS         TIMESTAMP_LTZ  AS (TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)),
    ORDER_ID         STRING         AS (PAYLOAD:order_id::STRING),
    VENUE            STRING         AS (PAYLOAD:venue::STRING)
)
CLUSTER BY (DATE_TRUNC('DAY', EVENT_TS), VENUE)
DATA_RETENTION_TIME_IN_DAYS = 3
COMMENT = 'Raw CEX order lifecycle events (new/cancel/modify/fill)';

-- ─── CEX Balance Snapshots ────────────────────────────────────────────────────
-- Source: Kinesis stream cex-balances (CDC from Aurora via DMS)
CREATE TABLE IF NOT EXISTS CEX_BALANCES_RAW (
    _RECORD_ID       STRING         DEFAULT UUID_STRING()  NOT NULL,
    _LOAD_TS         TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    _SOURCE          STRING         NOT NULL,
    _INGEST_METHOD   STRING         NOT NULL,
    _PARTITION_KEY   STRING,
    _FILE_NAME       STRING,
    PAYLOAD          VARIANT        NOT NULL,
    EVENT_TS         TIMESTAMP_LTZ  AS (TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)),
    ACCOUNT_ID       STRING         AS (PAYLOAD:account_id::STRING)
)
CLUSTER BY (DATE_TRUNC('DAY', EVENT_TS))
DATA_RETENTION_TIME_IN_DAYS = 3
COMMENT = 'Raw CEX account balance CDC events (Aurora → Kinesis cex-balances)';

-- ─── CEX System / Application Logs ───────────────────────────────────────────
-- Source: CloudWatch Logs → Firehose → S3 → Snowpipe batch
CREATE TABLE IF NOT EXISTS CEX_LOGS_RAW (
    _RECORD_ID       STRING         DEFAULT UUID_STRING()  NOT NULL,
    _LOAD_TS         TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    _SOURCE          STRING         NOT NULL,
    _INGEST_METHOD   STRING         NOT NULL,
    _FILE_NAME       STRING,
    PAYLOAD          VARIANT        NOT NULL,
    LOG_TS           TIMESTAMP_LTZ  AS (TRY_TO_TIMESTAMP_LTZ(PAYLOAD:timestamp::STRING)),
    LOG_LEVEL        STRING         AS (PAYLOAD:level::STRING),
    SERVICE          STRING         AS (PAYLOAD:service::STRING)
)
CLUSTER BY (DATE_TRUNC('HOUR', LOG_TS), SERVICE)
DATA_RETENTION_TIME_IN_DAYS = 3
COMMENT = 'CEX system/application logs (CloudWatch → Firehose → S3 → Snowpipe)';

-- ─── On-Chain Events ──────────────────────────────────────────────────────────
-- Source: Lambda on-chain indexer → Kinesis/S3 → Snowpipe
-- Covers: Ethereum, Polygon, Arbitrum, Solana, BTC (via indexer abstraction)
CREATE TABLE IF NOT EXISTS ONCHAIN_EVENTS_RAW (
    _RECORD_ID       STRING         DEFAULT UUID_STRING()  NOT NULL,
    _LOAD_TS         TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    _SOURCE          STRING         NOT NULL,
    _INGEST_METHOD   STRING         NOT NULL,
    _PARTITION_KEY   STRING,
    _FILE_NAME       STRING,
    PAYLOAD          VARIANT        NOT NULL,
    -- Promoted for partition pruning
    BLOCK_TS         TIMESTAMP_LTZ  AS (TRY_TO_TIMESTAMP_LTZ(PAYLOAD:block_timestamp::STRING)),
    CHAIN            STRING         AS (PAYLOAD:chain::STRING),
    TX_HASH          STRING         AS (PAYLOAD:tx_hash::STRING)
)
CLUSTER BY (DATE_TRUNC('DAY', BLOCK_TS), CHAIN)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw on-chain events: transfers, swaps, contract interactions across chains';

-- ─── DLQ: Malformed / Parse-Error Records ────────────────────────────────────
CREATE TABLE IF NOT EXISTS DLQ_ERRORS (
    _DLQ_ID          STRING         DEFAULT UUID_STRING()  NOT NULL,
    _LOAD_TS         TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP() NOT NULL,
    SOURCE_TABLE     STRING         NOT NULL  COMMENT 'Which RAW table the record failed from',
    _SOURCE          STRING,
    _PARTITION_KEY   STRING,
    RAW_PAYLOAD      STRING         COMMENT 'Raw string payload (may not be valid JSON)',
    ERROR_MESSAGE    STRING,
    RETRY_COUNT      NUMBER         DEFAULT 0,
    RESOLVED         BOOLEAN        DEFAULT FALSE
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Dead-letter queue for malformed ingest records; reviewed by SRE daily';

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT SELECT, INSERT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW    TO ROLE SURVEILLANCE_INGEST;
GRANT SELECT, INSERT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_ORDERS_RAW    TO ROLE SURVEILLANCE_INGEST;
GRANT SELECT, INSERT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_BALANCES_RAW  TO ROLE SURVEILLANCE_INGEST;
GRANT SELECT, INSERT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_LOGS_RAW      TO ROLE SURVEILLANCE_INGEST;
GRANT SELECT, INSERT ON TABLE CRYPTO_SURVEILLANCE.RAW.ONCHAIN_EVENTS_RAW TO ROLE SURVEILLANCE_INGEST;
GRANT SELECT, INSERT ON TABLE CRYPTO_SURVEILLANCE.RAW.DLQ_ERRORS         TO ROLE SURVEILLANCE_INGEST;

GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW     TO ROLE SURVEILLANCE_ADMIN;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_ORDERS_RAW     TO ROLE SURVEILLANCE_ADMIN;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_BALANCES_RAW   TO ROLE SURVEILLANCE_ADMIN;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_LOGS_RAW       TO ROLE SURVEILLANCE_ADMIN;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.RAW.ONCHAIN_EVENTS_RAW  TO ROLE SURVEILLANCE_ADMIN;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.RAW.DLQ_ERRORS          TO ROLE SURVEILLANCE_ADMIN;
