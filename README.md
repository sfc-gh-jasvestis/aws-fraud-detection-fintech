# Digital Asset Market Surveillance & Financial Crime Analytics
### Snowflake + AWS | Demo

> A regulator-friendly, end-to-end financial crime detection platform for crypto exchanges — governed data in Snowflake AI Data Cloud, high-throughput ingestion on AWS, and an investigator copilot powered by Amazon Bedrock.

---

## Two Personas, One Governed Platform

| Persona | Tool | What they see |
|---|---|---|
| **Compliance Investigator** | Streamlit in Snowflake | Cases, alerts, entity profiles, SAR drafts, masked PII |
| **Chief Compliance Officer** | Amazon QuickSight + Amazon Q | Alert trends, risk heatmaps, case SLA, KRIs/KPIs |

---

## Architecture

```
AWS Data Plane                     Snowflake AI Data Cloud
───────────────────                ──────────────────────────────────
Amazon MSK / Kinesis       ──────▶ RAW (Snowpipe Streaming)
Amazon S3                  ──────▶ HARMONISED (Dynamic Tables, 1-10 min lag)
Amazon AppFlow / Glue              FEATURES (Trade + Entity features)
Amazon Cognito                     ANALYTICS (ML scoring, detection rules, alerts, cases)
Amazon Bedrock (Claude)    ◀─────▶ External Access (SigV4 → Converse API)
Amazon QuickSight          ◀─────  Governed Views (DIRECT_QUERY via QUICKSIGHT_SVC)
```

| Layer | AWS | Snowflake |
|---|---|---|
| **Ingest** | MSK, S3, AppFlow, Glue, Cognito | Snowpipe Streaming, External Stages |
| **Transform** | — | Dynamic Tables (RAW → HARMONISED → FEATURES) |
| **Detect** | — | 6 SQL Detection Rules + XGBoost UDF |
| **Investigate** | Amazon Bedrock (Claude) | Streamlit (Investigator Copilot) |
| **Report** | Amazon QuickSight + Amazon Q | Governed Views (VW_KRIS, VW_QUICKSIGHT_*) |
| **Govern** | IAM, KMS | Horizon (Tags, Masking Policies, Row Access Policies) |

---

## Repository Structure

```
aws-fraud-detection-fintech/
├── snowflake/                        # Snowflake SQL (9 scripts, 00–08)
│   ├── 00_setup.sql                  # DB, schemas, roles, warehouses, QuickSight SVC user
│   ├── 01_integrations.sql           # S3 storage integration, Bedrock External Access
│   ├── 02_raw_tables.sql             # 6 RAW tables (VARIANT schema-on-read)
│   ├── 03_harmonised.sql             # 4 Dynamic Tables + synthetic reference views
│   ├── 04_entity_graph.sql           # Entity/Wallet graph + governance (tags, masking, RAP)
│   ├── 05_features.sql               # TRADE_FEATURES + ENTITY_FEATURES Dynamic Tables
│   ├── 06_analytics.sql              # ML scoring UDF + 6 detection rules + ALERTS
│   ├── 07_cases.sql                  # Cases + lifecycle SPs + QuickSight views + KRI/KPI
│   ├── 08_bedrock.sql                # SP_GENERATE_CASE_NARRATIVE (Bedrock Claude via SigV4)
│   └── demo_build_all.sql            # Single build orchestrator (SnowSQL !source)
├── sql/
│   └── demo/
│       └── demo_seed.sql             # Demo reset: truncate + reload + refresh
├── scripts/
│   └── generate_synthetic_data.py    # Synthetic data generator (--quick, --seed-and-refresh)
├── streamlit/
│   └── investigator_app.py           # Streamlit in Snowflake — Investigator Copilot
├── demo/
│   ├── demo_runbook.md               # 5–7 min live demo script
│   ├── demo_video_script.md          # 2–3 min recorded demo narrative
│   └── sample_sar_narrative.md       # Bedrock fallback SAR narrative
├── architecture/
│   └── architecture.drawio           # Architecture diagram (export to PNG/SVG)
├── .gitignore
└── README.md
```

> **Note for partners**: AWS infrastructure (MSK, S3, Lambda, QuickSight) is built separately by the partner team. This repo contains only the Snowflake platform, Streamlit app, and demo materials.

---

## Quick Start

### Prerequisites
- SnowSQL connected to your Snowflake account (`snowsql -c <CONNECTION>`)
- Python 3.10+ (for synthetic data generator)
- AWS CLI (for QuickSight setup — partner-led)

### 1. Build Snowflake Platform
```bash
snowsql -c <CONNECTION> -f snowflake/demo_build_all.sql
```

### 2. Load Data + Activate Pipeline

**Quick reset** (<60 seconds, 5K trades):
```bash
SNOWFLAKE_CONNECTION_NAME=<CONNECTION> python scripts/generate_synthetic_data.py --quick
```

**Full dataset** (~3 min, 50K trades):
```bash
SNOWFLAKE_CONNECTION_NAME=<CONNECTION> python scripts/generate_synthetic_data.py \
    --scenario all --trades 50000 --seed-and-refresh
```

The `--seed-and-refresh` flag refreshes all Dynamic Tables, runs detection + case creation SPs, resumes tasks, and prints a health check.

### 3. (Optional) Update Bedrock Credentials
```sql
ALTER SECRET CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET
    SET SECRET_STRING = '{"aws_access_key_id":"AKIA...","aws_secret_access_key":"..."}';
```

### 4. Health Check
```sql
SELECT
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW)     AS raw_trades,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES)      AS harm_trades,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY)      AS entities,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS)       AS alerts,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES)        AS cases;
```

Expected: raw_trades ~5K (quick) or ~50K (full), alerts > 50, cases > 5.

---

## SQL File Map

| # | File | What it creates |
|---|---|---|
| 00 | `00_setup.sql` | DB, 5 schemas, 4 roles, 2 warehouses, QuickSight SVC user (network policy commented — see notes) |
| 01 | `01_integrations.sql` | S3 storage integration, Bedrock External Access, streaming user |
| 02 | `02_raw_tables.sql` | 6 RAW tables (VARIANT schema-on-read) |
| 03 | `03_harmonised.sql` | 4 Dynamic Tables + synthetic price/wallet/marketplace views |
| 04 | `04_entity_graph.sql` | ENTITY, WALLET, ENTITY_RELATION + governance (tags, masking, RAP) |
| 05 | `05_features.sql` | TRADE_FEATURES + ENTITY_FEATURES Dynamic Tables |
| 06 | `06_analytics.sql` | ML scoring UDF (XGBoost) + ALERT_SCORES DT + 6 detection rules + ALERTS |
| 07 | `07_cases.sql` | CASES + case lifecycle SPs + QuickSight views + KRI/KPI views |
| 08 | `08_bedrock.sql` | SP_GENERATE_CASE_NARRATIVE (Bedrock Claude via SigV4) |

---

## SE Demo Account Notes

- **Network policy**: `00_setup.sql` has a commented-out `QUICKSIGHT_NETWORK_POLICY`. If your demo account has an account-level network policy blocking external IPs, uncomment and adjust the IP range for your QuickSight region (us-west-2: `54.70.204.128/27`). User-level policies override account-level.
- **QUICKSIGHT_SVC**: Uses `TYPE = LEGACY_SERVICE` to avoid MFA. Change the password in both `00_setup.sql` and the QuickSight data source config.
- **Marketplace data**: Script assumes `SNOWFLAKE_PUBLIC_DATA_FREE` is provisioned. Most demo accounts have this pre-installed.
- **Bedrock**: Requires valid AWS credentials in `BEDROCK_SECRET`. Without them, use the fallback SAR narrative in `demo/sample_sar_narrative.md`.

---

## Key Lessons Learned

- **QuickSight DIRECT_QUERY** has ~15 min server-side cache. Use SQL comment changes (`/* qs-refresh-vN */`) to bust cache.
- **Row Access Policies** affect aggregate views (KRI counts). Include dashboard service roles in the policy.
- **User-level network policies** override account-level policies for specific users.
- **Amazon Q Topics** answer data questions but not causal "why" questions.
- **Never round-trip QuickSight dashboard definitions** (describe → update) — causes silent SQL exceptions.

---

## Demo Scripts

| Script | Duration | Audience |
|---|---|---|
| `demo/demo_video_script.md` | 2–3 min | Recorded video walkthrough |
| `demo/demo_runbook.md` | 5–7 min | Live booth demo |

---

## Legal

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).

This is a personal project and is **not an official Snowflake offering**. It comes with **no support or warranty**. Use it at your own risk. Snowflake has no obligation to maintain, update, or support this code. Do not use this code in production without thorough review and testing.
