-- =============================================================================
-- Phase 2: Integrations, Stages & Snowpipe Streaming
-- File: 01_integrations.sql
-- Pre-requisites:
--   1. Run 00_setup.sql first
--   2. After CREATE STORAGE INTEGRATION, run DESC INTEGRATION to get
--      STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID — paste
--      these into Terraform demo.tfvars and re-apply to update the IAM
--      trust policy on the snowflake-s3-role.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE SCHEMA RAW;
USE WAREHOUSE WH_SURVEILLANCE;

-- ─── S3 Storage Integration ───────────────────────────────────────────────────
-- Allows Snowflake to access s3://crypto-surveillance-raw-<AWS_ACCOUNT_ID>
CREATE STORAGE INTEGRATION IF NOT EXISTS AWS_S3_CRYPTO
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/crypto-surveillance-snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://crypto-surveillance-raw-<AWS_ACCOUNT_ID>/')
    COMMENT = 'Storage integration for crypto-surveillance raw S3 bucket';

-- After running the above, execute:
--   DESC INTEGRATION AWS_S3_CRYPTO;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Update Terraform demo.tfvars and re-apply to fix the IAM trust policy.

GRANT USAGE ON INTEGRATION AWS_S3_CRYPTO TO ROLE SURVEILLANCE_ADMIN;

-- ─── External Stage → S3 Raw Bucket ──────────────────────────────────────────
CREATE STAGE IF NOT EXISTS CRYPTO_SURVEILLANCE.RAW.RAW_S3_STAGE
    URL = 's3://crypto-surveillance-raw-<AWS_ACCOUNT_ID>/'
    STORAGE_INTEGRATION = AWS_S3_CRYPTO
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE IGNORE_UTF8_ERRORS = TRUE)
    COMMENT = 'External stage pointing at raw CEX + on-chain event landing zone';

-- ─── Snowpipe Auto-Ingest via SQS ─────────────────────────────────────────────
-- SQS ARN: output from Terraform (snowpipe_sqs_arn). Update below.
-- Each pipe targets a specific S3 prefix; Snowpipe triggers on SQS notification.
-- Pipes are defined in 03_pipes.sql once RAW tables exist.

-- ─── Kinesis / Snowpipe Streaming Channel Setup ───────────────────────────────
-- Snowpipe Streaming uses the Java/Python ingest SDK directly.
-- No Snowflake-side DDL is required to "create" a channel — channels are
-- created programmatically by the ingest client (see ingestion/kinesis_producer/).
-- The SDK requires a Snowflake user with KEY_PAIR authentication.

-- Create a service user for the streaming ingest SDK:
CREATE USER IF NOT EXISTS SNOWPIPE_STREAMING_SVC
    DEFAULT_ROLE = SURVEILLANCE_INGEST
    DEFAULT_WAREHOUSE = WH_SURVEILLANCE
    COMMENT = 'Service account for Snowpipe Streaming SDK (key-pair auth)';

GRANT ROLE SURVEILLANCE_INGEST TO USER SNOWPIPE_STREAMING_SVC;

-- Key-pair setup (run once, offline — not in script):
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out rsa_key.p8
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- Then:
--   ALTER USER SNOWPIPE_STREAMING_SVC SET RSA_PUBLIC_KEY='<contents of rsa_key.pub>';

-- NOTE: CREATE API INTEGRATION (API_PROVIDER = aws_api_gateway) is for Snowflake
-- External Functions backed by API Gateway — NOT for Snowpark External Access.
-- Bedrock is called directly via HTTP from Snowpark using the Network Rule +
-- External Access Integration defined below. No API Integration is needed here.

-- ─── Network Rule for Bedrock External Access ─────────────────────────────────
CREATE NETWORK RULE IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_NETWORK_RULE
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('bedrock-runtime.us-west-2.amazonaws.com:443')
    COMMENT = 'Allow Snowpark to reach Amazon Bedrock runtime endpoint';

-- ─── Secret for Bedrock (AWS STS credentials via assumed role) ────────────────
-- Snowflake will use the API integration's IAM role directly via assume-role;
-- store any additional auth tokens here if needed.
CREATE SECRET IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '{}'
    COMMENT = 'Placeholder secret for Bedrock integration (auth handled by IAM role)';

-- ─── External Access Integration (Bedrock) ────────────────────────────────────
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS BEDROCK_EXTERNAL_ACCESS
    ALLOWED_NETWORK_RULES = (CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET)
    ENABLED = TRUE
    COMMENT = 'External Access Integration allowing Snowpark to call Amazon Bedrock';

GRANT USAGE ON INTEGRATION BEDROCK_EXTERNAL_ACCESS TO ROLE SURVEILLANCE_ADMIN;
