-- =============================================================================
-- Phase 3: Snowpipe Definitions (batch S3 auto-ingest)
-- File: 03_pipes.sql
-- Note: Snowpipe Streaming (Kinesis path) uses the Java/Python SDK client
--       in ingestion/kinesis_producer/ — no CREATE PIPE needed for that path.
--
-- IMPORTANT: This script requires a valid S3 storage integration and stage.
-- For DEMO MODE (synthetic data via direct INSERT), skip this entire file.
-- The demo_seed.sql script handles pipe existence checks gracefully.
--
-- To enable: update SNS_TOPIC_ARN below with: terraform output snowpipe_sqs_arn
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE SCHEMA RAW;
USE WAREHOUSE WH_INGEST;

-- ─── CEX Trades (S3 batch fallback / replay) ─────────────────────────────────
-- NOTE: Remove AUTO_INGEST and AWS_SNS_TOPIC if S3 notifications are not configured.
-- For demo mode, these pipes exist but remain paused (no S3 data flows).
CREATE PIPE IF NOT EXISTS PIPE_CEX_TRADES
    AUTO_INGEST = FALSE
    COMMENT = 'S3 batch ingest: events/trades/ → CEX_TRADES_RAW (paused for demo; enable AUTO_INGEST + SNS for production)'
AS
COPY INTO CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW (PAYLOAD, _SOURCE, _INGEST_METHOD, _FILE_NAME)
FROM (
    SELECT
        $1,
        'S3' AS _SOURCE,
        'SNOWPIPE_BATCH' AS _INGEST_METHOD,
        METADATA$FILENAME AS _FILE_NAME
    FROM @CRYPTO_SURVEILLANCE.RAW.RAW_S3_STAGE/events/trades/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- ─── CEX Orders ───────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_CEX_ORDERS
    AUTO_INGEST = FALSE
    COMMENT = 'S3 batch ingest: events/orders/ → CEX_ORDERS_RAW (paused for demo)'
AS
COPY INTO CRYPTO_SURVEILLANCE.RAW.CEX_ORDERS_RAW (PAYLOAD, _SOURCE, _INGEST_METHOD, _FILE_NAME)
FROM (
    SELECT $1, 'S3', 'SNOWPIPE_BATCH', METADATA$FILENAME
    FROM @CRYPTO_SURVEILLANCE.RAW.RAW_S3_STAGE/events/orders/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- ─── CEX Balances ─────────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_CEX_BALANCES
    AUTO_INGEST = FALSE
    COMMENT = 'S3 batch ingest: events/balances/ → CEX_BALANCES_RAW (paused for demo)'
AS
COPY INTO CRYPTO_SURVEILLANCE.RAW.CEX_BALANCES_RAW (PAYLOAD, _SOURCE, _INGEST_METHOD, _FILE_NAME)
FROM (
    SELECT $1, 'S3', 'SNOWPIPE_BATCH', METADATA$FILENAME
    FROM @CRYPTO_SURVEILLANCE.RAW.RAW_S3_STAGE/events/balances/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- ─── CEX Logs (Firehose → S3 → Snowpipe) ─────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_CEX_LOGS
    AUTO_INGEST = FALSE
    COMMENT = 'S3 batch ingest: firehose/logs/ → CEX_LOGS_RAW (paused for demo)'
AS
COPY INTO CRYPTO_SURVEILLANCE.RAW.CEX_LOGS_RAW (PAYLOAD, _SOURCE, _INGEST_METHOD, _FILE_NAME)
FROM (
    SELECT $1, 'S3_FIREHOSE', 'SNOWPIPE_BATCH', METADATA$FILENAME
    FROM @CRYPTO_SURVEILLANCE.RAW.RAW_S3_STAGE/firehose/logs/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE IGNORE_UTF8_ERRORS = TRUE)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- ─── On-Chain Events ──────────────────────────────────────────────────────────
CREATE PIPE IF NOT EXISTS PIPE_ONCHAIN_EVENTS
    AUTO_INGEST = FALSE
    COMMENT = 'S3 batch ingest: onchain-events/ → ONCHAIN_EVENTS_RAW (paused for demo)'
AS
COPY INTO CRYPTO_SURVEILLANCE.RAW.ONCHAIN_EVENTS_RAW (PAYLOAD, _SOURCE, _INGEST_METHOD, _FILE_NAME)
FROM (
    SELECT $1, 'S3_ONCHAIN', 'SNOWPIPE_BATCH', METADATA$FILENAME
    FROM @CRYPTO_SURVEILLANCE.RAW.RAW_S3_STAGE/onchain-events/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- ─── To enable AUTO_INGEST for production: ──────────────────────────────────
-- 1. Configure S3 bucket notifications → SQS
-- 2. Run:
--      ALTER PIPE PIPE_CEX_TRADES SET AUTO_INGEST = TRUE;
--      ALTER PIPE PIPE_CEX_ORDERS SET AUTO_INGEST = TRUE;
--      (etc.)
-- 3. Get notification channels:
--      SHOW PIPES LIKE 'PIPE_%' IN SCHEMA CRYPTO_SURVEILLANCE.RAW;
-- 4. Update SQS policy to allow Snowflake's notification_channel ARN.

-- Grants
GRANT MONITOR ON ALL PIPES IN SCHEMA CRYPTO_SURVEILLANCE.RAW TO ROLE SURVEILLANCE_ADMIN;
