-- =============================================================================
-- Phase 4: Entity / Wallet Graph
-- File: 05_entity_graph.sql
-- Tables: ENTITY, WALLET, ENTITY_RELATION
-- These seed from harmonised data and can be enriched by KYC/CDD systems,
-- TRM Labs, Chainalysis, and the Payments Fraud / KYC RA patterns.
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE SCHEMA HARMONISED;
USE WAREHOUSE WH_TRANSFORM;

-- ─── ENTITY ───────────────────────────────────────────────────────────────────
-- One row per customer / legal entity known to the exchange.
-- PII fields tagged (governance in 11_governance.sql).
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.ENTITY (
    entity_id           STRING        NOT NULL PRIMARY KEY,
    -- PII / KYC (masked for non-privileged roles)
    full_name           STRING        COMMENT 'PII: legal full name',
    date_of_birth       DATE          COMMENT 'PII: date of birth',
    nationality         STRING,
    country_of_residence STRING,
    email               STRING        COMMENT 'PII: contact email',
    phone               STRING        COMMENT 'PII: phone number',
    -- KYC / compliance
    kyc_tier            STRING        DEFAULT 'UNVERIFIED'  -- UNVERIFIED|BASIC|ENHANCED|INSTITUTIONAL
                        COMMENT 'KYC verification level',
    kyc_verified_at     TIMESTAMP_LTZ,
    kyc_provider        STRING,
    aml_risk_rating     STRING        DEFAULT 'UNKNOWN'  -- LOW|MEDIUM|HIGH|CRITICAL|UNKNOWN
                        COMMENT 'Latest AML risk classification',
    pep_flag            BOOLEAN       DEFAULT FALSE  COMMENT 'Politically Exposed Person',
    sanctions_flag      BOOLEAN       DEFAULT FALSE  COMMENT 'Matched on OFAC/UN/EU sanction lists',
    adverse_media_flag  BOOLEAN       DEFAULT FALSE,
    -- Account metadata
    account_type        STRING        DEFAULT 'INDIVIDUAL'  -- INDIVIDUAL|CORPORATE|INSTITUTIONAL
                        COMMENT 'Account classification',
    onboarded_at        TIMESTAMP_LTZ,
    status              STRING        DEFAULT 'ACTIVE'  -- ACTIVE|SUSPENDED|CLOSED|UNDER_REVIEW
                        COMMENT 'Current account status',
    -- Audit
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system       STRING        DEFAULT 'MANUAL',
    _version            NUMBER        DEFAULT 1
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Master entity/customer table with KYC attributes and risk ratings';

-- ─── WALLET ───────────────────────────────────────────────────────────────────
-- One row per wallet address (on-chain) or exchange account (off-chain).
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.WALLET (
    wallet_id           STRING        NOT NULL PRIMARY KEY,
    wallet_address      STRING        COMMENT 'On-chain address or CEX account ID',
    chain               STRING        COMMENT 'ethereum|polygon|bitcoin|solana|cex_internal',
    owner_entity_id     STRING        REFERENCES CRYPTO_SURVEILLANCE.HARMONISED.ENTITY(entity_id),
    -- Classification / tags
    wallet_type         STRING        DEFAULT 'UNKNOWN'  -- EOA|CONTRACT|EXCHANGE_HOT|EXCHANGE_COLD|DEX|BRIDGE|MIXER|UNKNOWN
                        COMMENT 'Wallet behavioural classification',
    is_exchange_wallet  BOOLEAN       DEFAULT FALSE,
    is_mixer            BOOLEAN       DEFAULT FALSE  COMMENT 'Associated with mixing/tumbling services',
    is_sanctioned       BOOLEAN       DEFAULT FALSE  COMMENT 'Wallet on OFAC/SDN or equivalent',
    is_high_risk        BOOLEAN       DEFAULT FALSE,
    risk_score          FLOAT         COMMENT 'Composite risk score 0–100 (TRM/Chainalysis/internal)',
    -- Enrichment metadata
    label               STRING        COMMENT 'Human-readable label (e.g. "Binance Hot Wallet")',
    cluster_id          STRING        COMMENT 'Blockchain analytics cluster ID',
    first_seen_ts       TIMESTAMP_LTZ,
    last_seen_ts        TIMESTAMP_LTZ,
    -- Audit
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system       STRING        DEFAULT 'ONCHAIN_INDEXER'
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Wallet/address registry with risk tags; links on-chain to off-chain identities';

-- ─── ENTITY_RELATION ─────────────────────────────────────────────────────────
-- Graph edges: entity ↔ wallet ↔ counterparty relationships
CREATE TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION (
    relation_id         STRING        DEFAULT UUID_STRING() PRIMARY KEY,
    from_entity_type    STRING        NOT NULL  -- ENTITY|WALLET
                        COMMENT 'Source node type',
    from_id             STRING        NOT NULL  COMMENT 'Source entity_id or wallet_id',
    relation_type       STRING        NOT NULL  COMMENT 'OWNS|CONTROLS|TRANSACTS_WITH|ASSOCIATED_WITH|SHARES_DEVICE',
    to_entity_type      STRING        NOT NULL  -- ENTITY|WALLET
                        COMMENT 'Target node type',
    to_id               STRING        NOT NULL  COMMENT 'Target entity_id or wallet_id',
    confidence          FLOAT         DEFAULT 1.0  COMMENT 'Confidence score 0–1',
    evidence_type       STRING        COMMENT 'KYC_DOCUMENT|TRANSACTION_PATTERN|IP_ADDRESS|DEVICE_FINGERPRINT|ANALYST_REVIEW',
    valid_from          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to            TIMESTAMP_LTZ COMMENT 'NULL = currently valid',
    created_by          STRING        DEFAULT 'SYSTEM',
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Graph edges connecting entities, wallets, and counterparties for network analysis';

-- Index for graph traversal (Snowflake: cluster key approximation)
ALTER TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION
    CLUSTER BY (from_id, to_id, relation_type);

-- ─── Dynamic Table: Auto-populate WALLET from on-chain observations ───────────
-- Discovers new wallet addresses from harmonised on-chain transfers
-- and creates placeholder WALLET records for analyst enrichment.
CREATE DYNAMIC TABLE IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.WALLET_DISCOVERY
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = WH_TRANSFORM
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

-- Task: run wallet discovery sync every 10 minutes
CREATE TASK IF NOT EXISTS CRYPTO_SURVEILLANCE.HARMONISED.TASK_SYNC_WALLET_DISCOVERY
    WAREHOUSE  = WH_TRANSFORM
    SCHEDULE   = '10 MINUTE'
    COMMENT    = 'Syncs newly discovered wallet addresses into WALLET master table'
AS
    CALL CRYPTO_SURVEILLANCE.HARMONISED.SP_SYNC_WALLET_DISCOVERY();

-- Task starts SUSPENDED; resume explicitly via demo_seed.sql or manually:
-- ALTER TASK CRYPTO_SURVEILLANCE.HARMONISED.TASK_SYNC_WALLET_DISCOVERY RESUME;

-- ─── Grants ───────────────────────────────────────────────────────────────────
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY          TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.WALLET           TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION  TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON DYNAMIC TABLE CRYPTO_SURVEILLANCE.HARMONISED.WALLET_DISCOVERY TO ROLE SURVEILLANCE_ANALYST;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY          TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.WALLET           TO ROLE SURVEILLANCE_ML;
GRANT SELECT ON TABLE CRYPTO_SURVEILLANCE.HARMONISED.ENTITY_RELATION  TO ROLE SURVEILLANCE_ML;
