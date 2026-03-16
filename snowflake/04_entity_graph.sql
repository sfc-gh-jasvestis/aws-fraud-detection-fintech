-- =============================================================================
-- Phase 4: Entity / Wallet Graph + Governance (Tags, Masking, RAP)
-- File: 04_entity_graph.sql
-- Tables: ENTITY, WALLET, ENTITY_RELATION, WALLET_DISCOVERY DT
-- Includes: PII tags, masking policies, row access policy (folded from
--           11_governance.sql for demo simplicity)
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE SCHEMA HARMONISED;
USE WAREHOUSE WH_SURVEILLANCE;

-- ─── ENTITY ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.ENTITY (
    entity_id           STRING        NOT NULL PRIMARY KEY,
    full_name           STRING        COMMENT 'PII: legal full name',
    date_of_birth       DATE          COMMENT 'PII: date of birth',
    nationality         STRING,
    country_of_residence STRING,
    email               STRING        COMMENT 'PII: contact email',
    phone               STRING        COMMENT 'PII: phone number',
    kyc_tier            STRING        DEFAULT 'UNVERIFIED'
                        COMMENT 'KYC verification level',
    kyc_verified_at     TIMESTAMP_LTZ,
    kyc_provider        STRING,
    aml_risk_rating     STRING        DEFAULT 'UNKNOWN'
                        COMMENT 'Latest AML risk classification',
    pep_flag            BOOLEAN       DEFAULT FALSE  COMMENT 'Politically Exposed Person',
    sanctions_flag      BOOLEAN       DEFAULT FALSE  COMMENT 'Matched on OFAC/UN/EU sanction lists',
    adverse_media_flag  BOOLEAN       DEFAULT FALSE,
    account_type        STRING        DEFAULT 'INDIVIDUAL'
                        COMMENT 'Account classification',
    onboarded_at        TIMESTAMP_LTZ,
    status              STRING        DEFAULT 'ACTIVE'
                        COMMENT 'Current account status',
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system       STRING        DEFAULT 'MANUAL',
    _version            NUMBER        DEFAULT 1
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Master entity/customer table with KYC attributes and risk ratings';

-- ─── WALLET (forward-declared in 03_harmonised.sql; IF NOT EXISTS is safe) ──
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.WALLET (
    wallet_id           STRING        NOT NULL PRIMARY KEY,
    wallet_address      STRING        COMMENT 'On-chain address or CEX account ID',
    chain               STRING        COMMENT 'ethereum|polygon|bitcoin|solana|cex_internal',
    owner_entity_id     STRING        REFERENCES CRYPTO_SURVEILLANCE.HARMONISED.ENTITY(entity_id),
    wallet_type         STRING        DEFAULT 'UNKNOWN'
                        COMMENT 'Wallet behavioural classification',
    is_exchange_wallet  BOOLEAN       DEFAULT FALSE,
    is_mixer            BOOLEAN       DEFAULT FALSE  COMMENT 'Associated with mixing/tumbling services',
    is_sanctioned       BOOLEAN       DEFAULT FALSE  COMMENT 'Wallet on OFAC/SDN or equivalent',
    is_high_risk        BOOLEAN       DEFAULT FALSE,
    risk_score          FLOAT         COMMENT 'Composite risk score 0-100',
    label               STRING        COMMENT 'Human-readable label',
    cluster_id          STRING        COMMENT 'Blockchain analytics cluster ID',
    first_seen_ts       TIMESTAMP_LTZ,
    last_seen_ts        TIMESTAMP_LTZ,
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system       STRING        DEFAULT 'ONCHAIN_INDEXER'
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Wallet/address registry with risk tags; links on-chain to off-chain identities';

-- ─── ENTITY_RELATION ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION (
    relation_id         STRING        DEFAULT UUID_STRING() PRIMARY KEY,
    from_entity_type    STRING        NOT NULL
                        COMMENT 'Source node type',
    from_id             STRING        NOT NULL  COMMENT 'Source entity_id or wallet_id',
    relation_type       STRING        NOT NULL  COMMENT 'OWNS|CONTROLS|TRANSACTS_WITH|ASSOCIATED_WITH|SHARES_DEVICE',
    to_entity_type      STRING        NOT NULL
                        COMMENT 'Target node type',
    to_id               STRING        NOT NULL  COMMENT 'Target entity_id or wallet_id',
    confidence          FLOAT         DEFAULT 1.0  COMMENT 'Confidence score 0-1',
    evidence_type       STRING        COMMENT 'KYC_DOCUMENT|TRANSACTION_PATTERN|IP_ADDRESS|DEVICE_FINGERPRINT|ANALYST_REVIEW',
    valid_from          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to            TIMESTAMP_LTZ COMMENT 'NULL = currently valid',
    created_by          STRING        DEFAULT 'SYSTEM',
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Graph edges connecting entities, wallets, and counterparties for network analysis';

ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION
    CLUSTER BY (from_id, to_id, relation_type);

-- ─── Dynamic Table: Auto-populate WALLET from on-chain observations ───────────
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.WALLET_DISCOVERY
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = WH_SURVEILLANCE
    COMMENT    = 'Auto-discovered wallets from on-chain transfers; feeds WALLET table'
AS
SELECT DISTINCT
    addr                                    AS wallet_address,
    chain,
    MIN(block_ts) OVER (PARTITION BY addr, chain) AS first_seen_ts,
    MAX(block_ts) OVER (PARTITION BY addr, chain) AS last_seen_ts,
    COUNT(*) OVER (PARTITION BY addr, chain)       AS tx_count
FROM (
    SELECT from_address AS addr, chain, block_ts FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS
    UNION ALL
    SELECT to_address   AS addr, chain, block_ts FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS
)
WHERE addr IS NOT NULL
  AND LENGTH(addr) > 0;

-- ─── Procedure: Merge Discovered Wallets into WALLET table ────────────────────
CREATE OR REPLACE PROCEDURE CRYPTO_SURVEILLANCE.HARMONISED.SP_SYNC_WALLET_DISCOVERY()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO CRYPTO_SURVEILLANCE.HARMONISED.WALLET AS tgt
    USING (
        SELECT
            MD5(wallet_address || '::' || chain)  AS wallet_id,
            wallet_address,
            chain,
            'UNKNOWN'                              AS wallet_type,
            first_seen_ts,
            last_seen_ts
        FROM CRYPTO_SURVEILLANCE.HARMONISED.WALLET_DISCOVERY
    ) AS src
    ON tgt.wallet_id = src.wallet_id
    WHEN NOT MATCHED THEN INSERT (
        wallet_id, wallet_address, chain, wallet_type,
        first_seen_ts, last_seen_ts, source_system
    )
    VALUES (
        src.wallet_id, src.wallet_address, src.chain, src.wallet_type,
        src.first_seen_ts, src.last_seen_ts, 'WALLET_DISCOVERY'
    )
    WHEN MATCHED THEN UPDATE SET
        last_seen_ts = src.last_seen_ts,
        updated_at   = CURRENT_TIMESTAMP();

    RETURN 'WALLET sync complete: ' || SQLROWCOUNT || ' rows affected';
END;
$$;

CREATE TASK IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.TASK_SYNC_WALLET_DISCOVERY
    WAREHOUSE  = WH_SURVEILLANCE
    SCHEDULE   = '10 MINUTE'
    COMMENT    = 'Syncs newly discovered wallet addresses into WALLET master table'
AS
    CALL CRYPTO_SURVEILLANCE.HARMONISED.SP_SYNC_WALLET_DISCOVERY();

-- Task starts SUSPENDED; resume explicitly via demo_seed.sql or manually:
-- ALTER TASK CRYPTO_SURVEILLANCE.HARMONISED.TASK_SYNC_WALLET_DISCOVERY RESUME;

-- =============================================================================
-- Governance: Tags, Masking Policies, Row Access Policy
-- (folded from 11_governance.sql)
-- =============================================================================

-- Switch to ACCOUNTADMIN for tag/policy creation
USE ROLE ACCOUNTADMIN;

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

ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN full_name         SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'name';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN date_of_birth     SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'dob';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN email             SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'email';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN phone             SET TAG CRYPTO_SURVEILLANCE.HARMONISED.PII = 'phone';

ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    SET TAG CRYPTO_SURVEILLANCE.HARMONISED.DATA_SENSITIVITY = 'RESTRICTED';
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    SET TAG CRYPTO_SURVEILLANCE.HARMONISED.REGULATORY = 'KYC';

-- ─── Masking Policies ─────────────────────────────────────────────────────────
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
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)
    END
COMMENT = 'Returns only birth year for non-admin roles';

ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN full_name     SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_FULL_NAME;
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN email         SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_EMAIL;
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN phone         SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_PHONE;
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
    MODIFY COLUMN date_of_birth SET MASKING POLICY CRYPTO_SURVEILLANCE.HARMONISED.MASK_DOB;

-- ─── Row Access Policy ────────────────────────────────────────────────────────
-- Applied later in 07_cases.sql after CASES table exists:
-- ALTER TABLE CRYPTO_SURVEILLANCE.ANALYTICS.CASES
--     ADD ROW ACCESS POLICY CRYPTO_SURVEILLANCE.ANALYTICS.RAP_CASES ON (assigned_to);

-- ─── Lineage Comments ─────────────────────────────────────────────────────────
COMMENT ON TABLE CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW IS
    'Lineage: AWS Kinesis cex-trades → Snowpipe Streaming → CEX_TRADES_RAW → HARMONISED.TRADES';
COMMENT ON TABLE CRYPTO_SURVEILLANCE.RAW.ONCHAIN_EVENTS_RAW IS
    'Lineage: Blockchain Indexer (Lambda) → Kinesis/S3 → Snowpipe → ONCHAIN_EVENTS_RAW → HARMONISED.ONCHAIN_TRANSFERS';

-- Switch back to SURVEILLANCE_ADMIN
USE ROLE SURVEILLANCE_ADMIN;

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY          TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.WALLET           TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION  TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON DYNAMIC TABLE CRYPTO_SURVEILLANCE.HARMONISED.WALLET_DISCOVERY TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY          TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.WALLET           TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION  TO ROLE SURVEILLANCE_ML;
