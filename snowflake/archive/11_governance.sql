-- =============================================================================
-- Phase 9: Security, Governance & Ops (Snowflake Horizon)
-- File: 11_governance.sql
-- Implements: PII tagging, masking policies, row access policies,
-- object classification, lineage comments.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE WAREHOUSE WH_ANALYTICS;

-- ─── Tags ─────────────────────────────────────────────────────────────────────
CREATE TAG IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.PII
    ALLOWED_VALUES = 'name', 'email', 'phone', 'dob', 'address', 'tax_id'
    COMMENT = 'Marks personally identifiable information fields';

CREATE TAG IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.DATA_SENSITIVITY
    ALLOWED_VALUES = 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data sensitivity classification per information security policy';

CREATE TAG IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.REGULATORY
    ALLOWED_VALUES = 'KYC', 'AML', 'SAR', 'GDPR', 'MiFID2', 'FATF'
    COMMENT = 'Regulatory obligation that governs this data element';

-- Tag PII columns on ENTITY
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN full_name         SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'name';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN date_of_birth     SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'dob';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN email             SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'email';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN phone             SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'phone';

-- Tag entire ENTITY table as RESTRICTED
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    SET TAG CRYPTO_SURVEILLANCE.HARMONISED.DATA_SENSITIVITY = 'RESTRICTED';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    SET TAG CRYPTO_SURVEILLANCE.HARMONISED.REGULATORY = 'KYC';
ALTER TABLE CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
    SET TAG CRYPTO_SURVEILLANCE.HARMONISED.REGULATORY = 'AML';
ALTER TABLE CRYPTO_SURVEILLANCE.ANALYTICS.CASES
    SET TAG CRYPTO_SURVEILLANCE.HARMONISED.REGULATORY = 'SAR';

-- ─── Masking Policies ─────────────────────────────────────────────────────────
-- Mask full_name: show only first letter + asterisks for non-privileged roles
CREATE MASKING POLICY IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.MASK_FULL_NAME
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('SURVEILLANCE_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'SURVEILLANCE_ANALYST' THEN
             SUBSTR(val, 1, 1) || REPEAT('*', GREATEST(LENGTH(val) - 1, 0))
        ELSE '***REDACTED***'
    END
COMMENT = 'Masks entity full name for non-admin roles';

CREATE MASKING POLICY IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.MASK_EMAIL
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('SURVEILLANCE_ADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'SURVEILLANCE_ANALYST' THEN
            REGEXP_REPLACE(val, '(^[^@]{1,3})[^@]+(@)', '\\1***\\2')
        ELSE '***REDACTED***'
    END
COMMENT = 'Partially masks email addresses';

CREATE MASKING POLICY IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.MASK_PHONE
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('SURVEILLANCE_ADMIN', 'ACCOUNTADMIN') THEN val
        ELSE REGEXP_REPLACE(val, '[0-9]', '*')
    END
COMMENT = 'Masks all digits in phone number';

CREATE MASKING POLICY IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.MASK_DOB
    AS (val DATE) RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('SURVEILLANCE_ADMIN', 'ACCOUNTADMIN') THEN val
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)   -- return birth year only
    END
COMMENT = 'Returns only birth year for non-admin roles';

-- Apply masking policies to ENTITY
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN full_name     SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_FULL_NAME;
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN email         SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_EMAIL;
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN phone         SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_PHONE;
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN date_of_birth SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_DOB;

-- ─── Row Access Policy ────────────────────────────────────────────────────────
-- Controls access to CASES: analysts see only their own assigned cases
-- or unassigned cases; admins see all.
CREATE ROW ACCESS POLICY IF NOT EXISTS CRYPTO_SURVEILLANCE.ANALYTICS.RAP_CASES
    AS (assigned_to STRING) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('SURVEILLANCE_ADMIN', 'ACCOUNTADMIN')
    OR assigned_to IS NULL
    OR assigned_to = CURRENT_USER()
COMMENT = 'Analysts see only unassigned cases or cases assigned to them';

ALTER TABLE CRYPTO_SURVEILLANCE.ANALYTICS.CASES
    ADD ROW ACCESS POLICY CRYPTO_SURVEILLANCE.ANALYTICS.RAP_CASES ON (assigned_to);

-- ─── Object Lineage Comments (manual documentation of data lineage) ───────────
COMMENT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW IS
    'Lineage: AWS Kinesis cex-trades → Snowpipe Streaming → CEX_TRADES_RAW → HARMONISED.TRADES';
COMMENT ON TABLE CRYPTO_SURVEILLANCE.RAW.ONCHAIN_EVENTS_RAW IS
    'Lineage: Blockchain Indexer (Lambda) → Kinesis/S3 → Snowpipe → ONCHAIN_EVENTS_RAW → HARMONISED.ONCHAIN_TRANSFERS';
COMMENT ON DYNAMIC TABLE CRYPTO_SURVEILLANCE.HARMONISED.TRADES IS
    'Lineage: RAW.CEX_TRADES_RAW → Dynamic Table (1 min lag) → FEATURES.TRADE_FEATURES → ANALYTICS.ALERTS';
COMMENT ON TABLE CRYPTO_SURVEILLANCE.ANALYTICS.CASES IS
    'Lineage: ANALYTICS.ALERTS (auto-case) → CASES → SP_GENERATE_CASE_NARRATIVE (Bedrock) → SAR';

-- ─── Data Classification (Snowflake Horizon auto-classify) ────────────────────
-- Enable auto-classification on ENTITY and CASES tables
-- (requires Snowflake Business Critical or above with Governance features)
-- ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY SET ENABLE_SCHEMA_EVOLUTION = TRUE;
-- CALL SYSTEM$CLASSIFY('CRYPTO_SURVEILLANCE.HARMONISED.ENTITY', {'auto_tag': true});
-- CALL SYSTEM$CLASSIFY('CRYPTO_SURVEILLANCE.ANALYTICS.CASES',   {'auto_tag': true});
-- (Uncomment above if Governance tier available on <SF_CONNECTION>)

-- ─── Snowgrid / Cross-Region Replication (optional resilience) ────────────────
-- For production, enable Snowgrid replication to a secondary region:
-- ALTER DATABASE CRYPTO_SURVEILLANCE ENABLE REPLICATION TO ACCOUNTS aws_us_west_2.<SF_CONNECTION>_dr;

-- ─── Audit Logging ────────────────────────────────────────────────────────────
-- Access history is available via:
--   SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
--   WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
--     AND ARRAY_CONTAINS('CRYPTO_SURVEILLANCE'::VARIANT, base_objects_accessed);

-- Policy query logs (who queried PII)
CREATE OR REPLACE VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_PII_ACCESS_LOG AS
SELECT
    query_id,
    query_start_time,
    user_name,
    role_name,
    query_text,
    direct_objects_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND ARRAY_SIZE(direct_objects_accessed) > 0
  -- Filter for ENTITY table access
  AND EXISTS (
      SELECT 1
      FROM TABLE(FLATTEN(input => direct_objects_accessed)) f
      WHERE f.value:objectName::STRING LIKE '%ENTITY%'
  );

GRANT SELECT ON VIEW CRYPTO_SURVEILLANCE.ANALYTICS.VW_PII_ACCESS_LOG TO ROLE SURVEILLANCE_ADMIN;
