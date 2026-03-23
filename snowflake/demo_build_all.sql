-- =============================================================================
-- Demo Build Orchestrator
-- File: snowflake/demo_build_all.sql
-- Purpose: Single script to build the entire CRYPTO_SURVEILLANCE platform.
-- Run as: ACCOUNTADMIN on demo43
--
-- Usage:
--   snowsql -c demo43 -f snowflake/demo_build_all.sql
--
-- This executes scripts 00-08 in order. Each script is idempotent.
-- Pipes (03_pipes.sql) are intentionally excluded from demo builds.
-- =============================================================================

!echo '===== [1/9] 00_setup.sql — Database, schemas, roles, warehouses =====';
!source snowflake/00_setup.sql;

!echo '===== [2/9] 01_integrations.sql — S3 + Bedrock integrations =====';
!source snowflake/01_integrations.sql;

!echo '===== [3/9] 02_raw_tables.sql — RAW layer DDL =====';
!source snowflake/02_raw_tables.sql;

!echo '===== [4/9] 03_harmonised.sql — Dynamic Tables + synthetic views =====';
!source snowflake/03_harmonised.sql;

!echo '===== [5/9] 04_entity_graph.sql — Entity/wallet graph + governance =====';
!source snowflake/04_entity_graph.sql;

!echo '===== [6/9] 05_features.sql — Feature engineering DTs =====';
!source snowflake/05_features.sql;

!echo '===== [7/9] 06_analytics.sql — ML scoring + detection rules + ALERTS =====';
!source snowflake/06_analytics.sql;

!echo '===== [8/9] 07_cases.sql — Case management + QuickSight views =====';
!source snowflake/07_cases.sql;

!echo '===== [9/9] 08_bedrock.sql — Bedrock narrative integration =====';
!source snowflake/08_bedrock.sql;

!echo '===== BUILD COMPLETE =====';
!echo 'Next: python scripts/generate_synthetic_data.py --seed-and-refresh --quick';
