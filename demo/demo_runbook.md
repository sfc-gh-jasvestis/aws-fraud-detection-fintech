# Demo Runbook — Digital Asset Market Surveillance
## 5–7 Minute Booth Demo Script
**Event**: Track 2 | CEX Security · Web3 · RWA Infrastructure  
**Account**: demo43 (Snowflake) | `018437500440` (AWS)  
**Region**: us-west-2  
**Pre-loaded scenario**: `all` (50,000 synthetic events, 5 active alert types)

---

## Two Personas

| Persona | Role | Tool | What they care about |
|---|---|---|---|
| **Compliance Investigator** | SURVEILLANCE_ANALYST / ADMIN | Streamlit in Snowflake | Cases, alerts, entity profiles, SAR drafts, masked PII |
| **Chief Compliance Officer (CCO)** | Executive stakeholder | Amazon QuickSight | Alert trends, risk heatmaps, case SLA, KRIs/KPIs |

Both personas read from the **same governed Snowflake views** — different tools, one source of truth.

---

## Architecture: Snowflake + AWS Better Together

| Layer | AWS | Snowflake |
|---|---|---|
| **Ingest** | Kinesis Data Streams, S3 | Snowpipe Streaming, External Stages |
| **Transform** | — | Dynamic Tables (RAW → HARMONISED → FEATURES) |
| **Detect** | — | SQL Detection Rules + Snowpark ML (XGBoost) |
| **Investigate** | Amazon Bedrock (Claude) | Streamlit in Snowflake (Investigator Copilot) |
| **Report (CCO)** | Amazon QuickSight | Governed Views (VW_QUICKSIGHT_*, VW_KRIs, VW_KPIs) |
| **Govern** | IAM, KMS | Horizon (Tags, Masking, RAP, Lineage) |

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

**Build all at once:**
```bash
snowsql -c demo43 -f snowflake/demo_build_all.sql
```

---

## Pre-Demo Setup (one-time, ~10 min)

### 1. Build the platform
```bash
snowsql -c demo43 -f snowflake/demo_build_all.sql
```

### 2. Load synthetic data + auto-refresh
```bash
# Full dataset (50K trades, ~3 min):
SNOWFLAKE_CONNECTION_NAME=demo43 python scripts/generate_synthetic_data.py \
    --scenario all --trades 50000 --seed-and-refresh

# Quick reset (<60s):
SNOWFLAKE_CONNECTION_NAME=demo43 python scripts/generate_synthetic_data.py --quick
```

The `--seed-and-refresh` flag automatically:
- Force-refreshes all Dynamic Tables
- Runs `SP_REFRESH_ALERTS()` + `SP_AUTO_CREATE_CASES()`
- Resumes all tasks
- Prints a health check summary

### 3. (Optional) Update Bedrock credentials
```sql
ALTER SECRET CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET
    SET SECRET_STRING = '{"aws_access_key_id":"AKIA...","aws_secret_access_key":"..."}';
```

---

## Pre-Demo Checklist (30 min before booth opens)

- [ ] `--quick` or full data load completed (see step 2 above)
- [ ] Health check passed (alerts > 0, cases > 0)
- [ ] Streamlit Investigator Copilot opened in Snowsight (full screen)
- [ ] QuickSight dashboards open (see URLs below)
- [ ] Browser tab 1: Streamlit app (Case Queue)
- [ ] Browser tab 2: Snowsight → `SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS` ready
- [ ] Browser tab 3: Architecture diagram (`architecture.drawio` exported to PNG/SVG)
- [ ] AWS Console tab: Kinesis Data Streams → `crypto-surveillance-cex-trades`

### Health Check Query
```sql
SELECT
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW)     AS raw_trades,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES)      AS harm_trades,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY)      AS entities,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.WALLET)      AS wallets,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES) AS entity_features,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS)       AS alerts,
    (SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES)        AS cases;
```

Expected: raw_trades ~50K, alerts > 50, cases > 5.

### QuickSight Dashboard URLs
> Replace `<DASHBOARD_ID>` with your actual QuickSight dashboard IDs after publishing.

| Dashboard | URL |
|---|---|
| Alert Volume | `https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/<DASHBOARD_ID>` |
| Entity Risk Heatmap | `https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/<DASHBOARD_ID>` |
| Case SLA | `https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/<DASHBOARD_ID>` |
| Trading Summary | `https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/<DASHBOARD_ID>` |
| On-Chain Flows | `https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/<DASHBOARD_ID>` |

QuickSight connects to these Snowflake views:
- `ANALYTICS.VW_QUICKSIGHT_ALERT_VOLUME`
- `ANALYTICS.VW_QUICKSIGHT_ENTITY_HEATMAP`
- `ANALYTICS.VW_QUICKSIGHT_CASE_SLA`
- `ANALYTICS.VW_QUICKSIGHT_TRADING_SUMMARY`
- `ANALYTICS.VW_QUICKSIGHT_ONCHAIN_FLOWS`

---

## Demo Script

### [00:00–00:45] Hook & Problem Statement
> *"Crypto exchanges face a unique challenge: they're running at the speed of high-frequency trading but also have to meet the compliance standards of a bank. The problem today is that the data needed — CEX order flow, on-chain wallet activity, KYC records — is siloed across five different systems."*

**Show**: Architecture slide — fragmented legacy view (point to left side)

> *"What we've built is a unified Snowflake + AWS platform that brings all of this together."*

**Show**: End-state architecture diagram (`architecture.drawio` exported to PNG/SVG) — walk through left-to-right in 30 seconds.

---

### [00:45–02:00] Data Plane: AWS → Snowflake
> *"The data plane runs on AWS. CEX trading events hit Kinesis Data Streams — we're talking sub-second latency — and flow directly into Snowflake via Snowpipe Streaming. On-chain events come in through a Lambda indexer polling Ethereum, Polygon, Arbitrum."*

**Show**: AWS Console → Kinesis Data Streams → `crypto-surveillance-cex-trades`  
- Point to: incoming record rate, shard metrics

> *"Once it lands in Snowflake, Dynamic Tables transform the raw VARIANT data into a typed, deduplicated harmonised schema within 60 seconds. We're running the same RAW → HARMONISED → ACTIONABLE pattern from the Payments Fraud RA — just adapted for crypto."*

**Show**: Snowsight → Quick query:
```sql
SELECT COUNT(*), MAX(_LOAD_TS) FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW;
SELECT COUNT(*) FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES;
```

---

### [02:00–03:00] Persona 1: CCO — QuickSight Executive Dashboards
> *"Let's start with the CCO's view. They don't investigate individual cases — they need to know: are we catching enough? How fast are we closing? Where is risk concentrating?"*

**Show**: QuickSight → Alert Volume Dashboard (pre-loaded)
- Point to severity breakdown bar chart, daily trend line
- *"Alert volume by severity — the CCO sees wash trading spiked this week."*

**Show**: QuickSight → Case SLA Dashboard
- Point to time-to-close, SLA breach count, backlog by priority
- *"Average time-to-close, cases breaching SLA. This is board-report data."*

**Show**: QuickSight → Entity Risk Heatmap (2 seconds)
- *"Which customer segments carry the most concentrated risk — all powered by KRI and KPI views in Snowflake."*

> *"All of this is Amazon QuickSight reading directly from governed Snowflake views. No data copies, no extracts, no stale CSVs."*

---

### [03:00–05:00] Persona 2: Investigator — Streamlit Copilot
> *"Now let's switch to the investigator's world. This is a Streamlit app running directly inside Snowflake — no data leaves the platform."*

**Show**: Switch to Streamlit app

**Demo flow**:
1. Open **Case Queue** tab → filter by Priority = CRITICAL
2. Select a sanctioned counterparty case (e.g. `CASE-YYYYMMDD-00001`)
3. Switch to **Case Detail** tab
4. Point out: entity profile (PEP flag, sanctions flag, AML rating)
5. Scroll to alert list — show the sanctioned address details
6. Click **"✨ Generate Case Narrative & SAR Draft (Bedrock)"**

> *"This calls Amazon Bedrock — specifically Claude — via Snowflake's External Access integration. The LLM receives the full case context: the entity profile, all linked alerts, on-chain transaction graph, trading patterns. It generates a case summary, a SAR draft in FinCEN format, and recommended next steps."*

**Show**: Bedrock narrative appears (10–15 seconds)
- Read out the **Recommended Action** field
- Scroll through the SAR narrative

> *"The investigator closes the case here in Streamlit. The CCO immediately sees the SLA metric update in QuickSight. Two personas, one source of truth."*

---

### [05:00–06:00] Governance & Regulator Story
> *"This is the regulator-friendly story. Every alert traces back to a raw event via full lineage in Snowflake Horizon. PII is masked by role. KYC fields are tagged. The SAR narrative is generated from governed, auditable data — and the generation event is logged in the case audit trail."*

**Show**: Snowsight → quick query:
```sql
USE ROLE SURVEILLANCE_ANALYST;
SELECT entity_id, full_name, email, aml_risk_rating
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
LIMIT 5;
-- Shows masked name/email for analyst role
```

> *"Switch to admin role and the full data appears. Compliance sees what they need, front-line analysts get masked views."*

```sql
USE ROLE SURVEILLANCE_ADMIN;
SELECT entity_id, full_name, email, aml_risk_rating
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY
LIMIT 5;
-- Shows unmasked data for admin role
```

---

### [06:00–07:00] Close & Joint Story
> *"To summarise: AWS provides the data plane — Kinesis for streaming, S3 for scale, Bedrock for AI, QuickSight for the CCO's executive dashboards. Snowflake is the governance and analytics control plane — Dynamic Tables, Snowpark ML, Streamlit for the investigator, Horizon for compliance. Two personas, two tools, one governed data platform."*

**Key soundbites**:
- *"The CCO sees risk trends in QuickSight. The investigator files SARs in Streamlit. Same data, same governance, different tools."*
- *"Sub-second ingest to investigated case in under 10 minutes."*
- *"Not just another trading analytics demo — this is a SAR-filing-ready compliance platform."*

---

## Rapid Demo Reset (between booth visitors)

```bash
SNOWFLAKE_CONNECTION_NAME=demo43 python scripts/generate_synthetic_data.py --quick
```

This loads 5,000 trades in <60 seconds, refreshes all DTs, runs detection, and creates cases.

---

## FAQ / Objections

| Question | Answer |
|---|---|
| "How does this differ from existing AML tools?" | "Unlike point solutions (Chainalysis, TRM), this is the data layer. It ingests their signals too, combines with CEX data, and lets you build custom typologies in SQL — no vendor lock-in." |
| "What about real-time latency?" | "Snowpipe Streaming gives sub-second ingest. Dynamic Tables refresh within 60 seconds. Alerts fire within 5 minutes of a trade." |
| "How do you handle on-chain scale?" | "Lambda indexer + Kinesis handles the fan-out. We can process 100K on-chain events/second at this shard count." |
| "Does this work for Solana / non-EVM?" | "Yes — the on-chain indexer abstraction supports any chain. VARIANT schema in RAW means no re-DDL for new chains." |
| "What's the Snowflake list price for this?" | "Price based on compute and storage. ML + streaming typically $50–100K/year for a mid-size exchange. Much less than a dedicated AML vendor per-transaction fee." |
| "Where does Bedrock fit vs. Cortex?" | "Bedrock for the narrative/SAR generation (Claude). Cortex Search for unstructured case notes search. Both callable from Snowpark." |
| "Why QuickSight and not just Streamlit?" | "Different personas. The CCO uses QuickSight for alert trends, SLA tracking, risk heatmaps — executive KRIs/KPIs. The investigator uses Streamlit for case triage, entity profiling, and SAR generation. Both read from the same governed Snowflake views." |

---

## Emergency Fallbacks

| Issue | Fallback |
|---|---|
| Snowflake is slow | Show architecture diagram + pre-recorded screen recording |
| Bedrock is slow/down | Show pre-generated narrative in `demo/sample_sar_narrative.md` |
| Data isn't loaded | `python scripts/generate_synthetic_data.py --quick` (<60 seconds) |
| QuickSight not connected | Show the Snowflake views directly: `SELECT * FROM VW_QUICKSIGHT_ALERT_VOLUME LIMIT 20;` |
