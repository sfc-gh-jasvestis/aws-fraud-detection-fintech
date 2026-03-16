# Digital Asset Market Surveillance & Financial Crime Analytics
### Snowflake + AWS | Track 2: CEX Security · Web3 · RWA Infrastructure

> **Booth Story**: A regulator-friendly, end-to-end financial crime detection platform for crypto exchanges — governed data in Snowflake AI Data Cloud, high-throughput ingestion on AWS, and an agentic investigator copilot powered by Amazon Bedrock.

---

## Solution Overview

| Dimension | Details |
|---|---|
| **Target Audience** | CEX compliance teams, crypto risk officers, regulators |
| **Track Alignment** | CEX Security & Resilience · AI + Stablecoins + Web3 · RWA Unified Settlement |
| **AWS Account** | `<AWS_ACCOUNT_ID>` |
| **Snowflake Account** | `<SF_CONNECTION>` |
| **AWS Region** | `us-west-2` (primary) |

---

## Architecture Summary

```
CEX Exchange Infra (AWS)          Snowflake AI Data Cloud
─────────────────────────         ──────────────────────────────────────────
Matching Engine                   RAW  →  HARMONISED  →  FEATURES  →  ANALYTICS
Order Books          ──KDS──────▶ (Snowpipe Streaming)   (Dynamic Tables)
Balances (Aurora)                 Entity/Wallet Graph     ML Scoring (Snowpark)
System Logs          ──S3───────▶ (Snowpipe Batch)        Detection Rules (SQL)

On-Chain Indexers    ──Lambda──▶  ALERTS + CASES
3rd-Party APIs (TRM)              │
                                  ▼
                           Bedrock Copilot (External Access)
                                  │
                    ┌─────────────┴──────────────┐
               Streamlit UI                  QuickSight
           (Investigator Copilot)        (Exec Dashboards)
```

| Layer | AWS | Snowflake |
|---|---|---|
| **Ingest** | Kinesis Data Streams, S3 | Snowpipe Streaming, External Stages |
| **Transform** | — | Dynamic Tables (RAW → HARMONISED → FEATURES) |
| **Detect** | — | SQL Detection Rules + Snowpark ML (XGBoost) |
| **Investigate** | Amazon Bedrock (Claude) | Streamlit in Snowflake (Investigator Copilot) |
| **Report (CCO)** | Amazon QuickSight | Governed Views (VW_QUICKSIGHT_*, VW_KRIs, VW_KPIs) |
| **Govern** | IAM, KMS | Horizon (Tags, Masking, RAP, Lineage) |

**Warehouses**: `WH_SURVEILLANCE` (Medium, multi-cluster) + `WH_ML` (Large)

---

## Repository Structure

```
aws-fraud-detection-fintech/
├── architecture/
│   └── architecture.drawio      # draw.io diagram (export to PNG/SVG for slides)
├── terraform/                   # Phase 1 – AWS Foundations
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── vpc.tf
│   ├── kinesis.tf
│   ├── s3.tf
│   ├── iam.tf
│   └── lambda.tf
├── snowflake/                   # Phases 2–8 – Snowflake SQL (9 files)
│   ├── 00_setup.sql             # DB, schemas, 4 roles, 2 warehouses
│   ├── 01_integrations.sql      # S3 integration, Bedrock External Access
│   ├── 02_raw_tables.sql        # RAW layer DDL (6 VARIANT tables)
│   ├── 03_harmonised.sql        # 4 Dynamic Tables + synthetic reference views
│   ├── 04_entity_graph.sql      # Entity/Wallet/Relation + governance (tags, masking, RAP)
│   ├── 05_features.sql          # TRADE_FEATURES + ENTITY_FEATURES Dynamic Tables
│   ├── 06_analytics.sql         # ML scoring UDF + 6 detection rules + ALERTS
│   ├── 07_cases.sql             # Cases + lifecycle SPs + QuickSight views + KRI/KPI
│   ├── 08_bedrock.sql           # SP_GENERATE_CASE_NARRATIVE (Bedrock Claude via SigV4)
│   ├── demo_build_all.sql       # Single orchestrator (SnowSQL !source)
│   └── archive/                 # Old pre-streamlined scripts (kept for reference)
├── sql/
│   └── demo/
│       └── demo_seed.sql        # Manual seed helper (alternative to Python generator)
├── scripts/
│   └── generate_synthetic_data.py  # Data generator (--quick, --seed-and-refresh)
├── ingestion/
│   ├── kinesis_producer/        # Snowpipe Streaming SDK client (Python)
│   │   ├── requirements.txt
│   │   └── streaming_ingest.py
│   └── lambda/
│       ├── onchain_indexer/     # On-chain event → Kinesis
│       │   └── handler.py
│       └── log_normalizer/      # CloudWatch log → S3/Kinesis
│           └── handler.py
├── ml/
│   └── train_fraud_model.py     # XGBoost/LightGBM training (Snowpark)
├── streamlit/
│   └── investigator_app.py      # Streamlit in Snowflake – Investigator Copilot UI
├── config/
│   └── settings.py              # Default connection settings
├── demo/
│   ├── demo_runbook.md          # 5–7 min live demo script + runbook
│   ├── demo_video_script.md     # 2–3 min recorded demo narrative
│   ├── seed_data.py             # Legacy seed helper
│   └── sample_sar_narrative.md  # Bedrock fallback narrative
└── slides/
    └── booth_assets.md          # Slide checklist + talking points
```

---

## Quick Start (Demo Environment)

### Prerequisites
- AWS CLI configured for account `<AWS_ACCOUNT_ID>`, region `us-west-2`
- Terraform >= 1.5
- SnowSQL connected to `<SF_CONNECTION>` (`snowsql -c <SF_CONNECTION>`)
- Python 3.10+

### 1. Provision AWS Infrastructure
```bash
cd terraform
terraform init
terraform plan -var-file="demo.tfvars"
terraform apply -var-file="demo.tfvars" -auto-approve
```

### 2. Build Snowflake Platform (one command)
```bash
snowsql -c <SF_CONNECTION> -f snowflake/demo_build_all.sql
```

### 3. Load Data + Activate Pipeline

**Quick reset** (<60 seconds, 5K trades):
```bash
SNOWFLAKE_CONNECTION_NAME=<SF_CONNECTION> python scripts/generate_synthetic_data.py --quick
```

**Full dataset** (~3 min, 50K trades):
```bash
SNOWFLAKE_CONNECTION_NAME=<SF_CONNECTION> python scripts/generate_synthetic_data.py \
    --scenario all --trades 50000 --seed-and-refresh
```

The `--seed-and-refresh` flag automatically refreshes all Dynamic Tables, runs detection + case creation SPs, resumes tasks, and prints a health check.

### 4. (Optional) Update Bedrock Credentials
```sql
ALTER SECRET CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET
    SET SECRET_STRING = '{"aws_access_key_id":"AKIA...","aws_secret_access_key":"..."}';
```

### 5. Health Check
```sql
SELECT
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW)     AS raw_trades,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES)      AS harm_trades,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY)      AS entities,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS)       AS alerts,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES)        AS cases;
```

---

## SQL File Map (Streamlined: 9 files, 00–08)

| # | File | What it creates |
|---|---|---|
| 00 | `00_setup.sql` | DB, 5 schemas, 4 roles, **2 warehouses** (WH_SURVEILLANCE M, WH_ML L) |
| 01 | `01_integrations.sql` | S3 integration, Bedrock External Access, streaming user |
| 02 | `02_raw_tables.sql` | 6 RAW tables (VARIANT schema-on-read) |
| 03 | `03_harmonised.sql` | 4 Dynamic Tables + synthetic price/wallet views |
| 04 | `04_entity_graph.sql` | ENTITY, WALLET, ENTITY_RELATION + governance (tags, masking, RAP) |
| 05 | `05_features.sql` | TRADE_FEATURES + ENTITY_FEATURES Dynamic Tables |
| 06 | `06_analytics.sql` | ML scoring UDF + ALERT_SCORES DT + 6 detection rules + ALERTS |
| 07 | `07_cases.sql` | CASES + case lifecycle SPs + QuickSight views + KRI/KPI views |
| 08 | `08_bedrock.sql` | SP_GENERATE_CASE_NARRATIVE (Bedrock Claude via SigV4) |

---

## Joint Solution Narrative

**Why Snowflake + AWS?**

| Layer | AWS Role | Snowflake Role |
|---|---|---|
| **Data Plane** | High-throughput CEX event streaming (KDS), S3 landing, Lambda enrichment | Governed, queryable copy of all events |
| **Analytics** | — | Harmonisation, entity graph, feature engineering, ML scoring |
| **AI** | Amazon Bedrock LLMs (Claude) | External Access, Streamlit UI |
| **Governance** | IAM, KMS, VPC | Horizon: tags, masking, lineage |
| **Consumption** | QuickSight dashboards | Streamlit Investigator UI |

**Regulator Story**: Every alert, case, and narrative is derived from governed data in Snowflake — full lineage from raw trade event to SAR draft.

---

## Reference Architectures Used

1. **Payments Fraud Detection RA** – Base RAW→HARMONISED→ACTIONABLE pattern, Streams/Tasks, Snowpark
2. **Collaborative Fraud Detection** – Multi-institution shared risk signals via Snowflake Native Apps
3. **KYC for Financial Services RA** (IS-47) – Entity/customer profile, KYC/AML, case management
4. **Fintech RA** (IS-523) – Unified Snowflake AI Data Cloud for fraud, risk, personalization
5. **Snowflake + AWS Streaming Blueprints** – MSK/Kinesis → Snowpipe Streaming patterns
6. **Cortex Agents + Bedrock RA** – External Access, Bedrock/SageMaker interaction models
