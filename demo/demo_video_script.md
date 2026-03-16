# Demo Video Script — Digital Asset Market Surveillance
## 2–3 Minute Recorded Walkthrough
**Format**: Screen recording with voiceover  
**Target**: Booth video loop / pre-meeting teaser / social share  
**Pre-requisites**: Data loaded via `--quick` or full dataset, Streamlit app deployed, QuickSight dashboards open

---

## Two Personas

| Persona | Role | Tool | What they care about |
|---|---|---|---|
| **Compliance Investigator** | SURVEILLANCE_ANALYST / ADMIN | Streamlit in Snowflake | Cases, alerts, entity profiles, SAR drafts, masked PII |
| **Chief Compliance Officer (CCO)** | Executive stakeholder | Amazon QuickSight | Alert trends, risk heatmaps, case SLA, KRIs/KPIs |

---

## Pre-Recording Checklist

- [ ] Run `SNOWFLAKE_CONNECTION_NAME=<SF_CONNECTION> python scripts/generate_synthetic_data.py --quick`
- [ ] Verify health check: alerts > 50, cases > 5
- [ ] Open Snowsight in Chrome (full screen, dark mode)
- [ ] Open Streamlit Investigator Copilot (full screen)
- [ ] Open QuickSight dashboards (Alert Volume + Case SLA at minimum)
- [ ] Open architecture diagram (`architecture.drawio` exported to PNG/SVG)
- [ ] Audio: quiet room, external mic recommended
- [ ] Resolution: 1920x1080, 60fps

---

## Script

### [0:00–0:20] HOOK — The Problem (Show: Architecture Diagram)

> *"Every crypto exchange has the same problem: they're processing millions of trades per day, but compliance teams are still investigating fraud with spreadsheets — and the CCO has no real-time view of risk exposure."*

> *"We built a unified surveillance platform on Snowflake and AWS that serves two personas from one governed data set: the investigator who triages cases, and the executive who tracks risk."*

**Screen**: Architecture diagram (full screen, 3 seconds) → zoom to left side (AWS) → pan right (Snowflake) → highlight Consumption Layer (Streamlit + QuickSight side by side)

---

### [0:20–0:50] DATA FLOW — AWS to Snowflake (Show: Snowsight SQL)

> *"On the AWS side, Kinesis Data Streams captures trade events at matching-engine speed. An on-chain Lambda indexer pulls Ethereum and Polygon transactions. Everything lands in Snowflake's RAW layer in under a second via Snowpipe Streaming."*

**Screen**: Run in Snowsight:
```sql
SELECT COUNT(*) AS total_raw_trades, MAX(_LOAD_TS) AS latest_ingest
FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW;
```

> *"Dynamic Tables automatically transform raw JSON into typed, deduplicated data — no orchestrator, no Airflow, no DAGs. Six detection rules and an XGBoost ML model score every entity and trade continuously."*

**Screen**: Run:
```sql
SELECT alert_type, severity, COUNT(*) AS alert_count,
       ROUND(AVG(ml_fraud_probability), 3) AS avg_ml_score
FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
GROUP BY 1, 2 ORDER BY 3 DESC;
```

---

### [0:50–1:20] PERSONA 1: CCO / EXECUTIVE — QuickSight Dashboards

> *"Let's start with the CCO's view. They don't investigate individual cases — they need to know: are we catching enough? How fast are we closing? Where is risk concentrating?"*

**Screen**: Switch to QuickSight → Alert Volume dashboard

> *"This is Amazon QuickSight reading directly from governed Snowflake views. Alert volume by severity and type — the CCO sees wash trading spiked this week."*

**Screen**: Point to severity breakdown bar chart → daily trend line

> *"Switch to the Case SLA dashboard. Average time-to-close, cases breaching SLA, backlog by priority. This is the data that goes into the board report."*

**Screen**: Switch to QuickSight → Case SLA dashboard → point to SLA breach count

> *"And the Entity Risk Heatmap — which customer segments carry the most concentrated risk. All powered by the same KRI and KPI views in Snowflake."*

**Screen**: Switch to QuickSight → Entity Heatmap (2 seconds)

---

### [1:20–2:10] PERSONA 2: INVESTIGATOR — Streamlit + Bedrock

> *"Now let's switch to the investigator's world. This is a Streamlit app running inside Snowflake — no data leaves the platform."*

**Screen**: Switch to Streamlit app

> *"The investigator filters cases by priority — CRITICAL. They select a sanctions case."*

**Screen**: Click Priority = CRITICAL → select top case → Case Detail tab

> *"Every case shows the entity profile, linked alerts, on-chain transactions, and ML score. One click generates a SAR narrative."*

**Screen**: Scroll to show entity profile → click "Generate Case Narrative & SAR Draft (Bedrock)"

> *"This calls Amazon Bedrock — Claude — through Snowflake's External Access. The LLM gets the full case context and produces a FinCEN-format SAR narrative, a case summary, and recommended next steps."*

**Screen**: Narrative appears → scroll through SAR → highlight "Recommended Action: FILE_SAR"

> *"The CCO sees the case SLA ticking in QuickSight. The investigator closes the case here in Streamlit. Both personas, one platform, one source of truth."*

---

### [2:10–2:40] GOVERNANCE — The Regulator Story (Show: Snowsight SQL)

> *"And here's what makes this regulator-ready. PII is masked by role — the investigator sees what they need, nothing more."*

**Screen**: Run:
```sql
USE ROLE SURVEILLANCE_ANALYST;
SELECT entity_id, full_name, email, aml_risk_rating
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY LIMIT 5;
```

> *"Switch to admin — full access appears. Same table, same query, different governance."*

**Screen**: Run:
```sql
USE ROLE SURVEILLANCE_ADMIN;
SELECT entity_id, full_name, email, aml_risk_rating
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY LIMIT 5;
```

> *"Every field is tagged, every table has lineage, and every case event is logged in an immutable audit trail. The CCO's QuickSight dashboards and the investigator's Streamlit app both read from these same governed views."*

---

### [2:40–3:00] CLOSE — Better Together (Show: Architecture Diagram)

> *"AWS provides the data plane — Kinesis for streaming, S3 for scale, Bedrock for AI, QuickSight for the executive dashboards. Snowflake is the governance and analytics control plane — Dynamic Tables, Snowpark ML, Streamlit for the investigator, Horizon for compliance."*

> *"Two personas, two tools, one governed platform. That's how you build a financial crime system that a regulator will actually trust."*

**Screen**: Architecture diagram (full frame) → fade to title card:

**Title Card** (3 seconds):
```
Digital Asset Market Surveillance
Snowflake + AWS | Better Together

Investigator: Streamlit in Snowflake
Executive: Amazon QuickSight
One governed data platform.

<YOUR_EMAIL>
```

---

## Timing Summary

| Segment | Duration | Persona | Screen |
|---|---|---|---|
| Hook & Problem | 0:00–0:20 | Both | Architecture diagram |
| Data Flow + Detection | 0:20–0:50 | — | Snowsight SQL queries |
| CCO / Executive View | 0:50–1:20 | CCO | QuickSight dashboards |
| Investigator View | 1:20–2:10 | Investigator | Streamlit app + Bedrock |
| Governance | 2:10–2:40 | Both | Snowsight SQL (role switching) |
| Close | 2:40–3:00 | Both | Architecture diagram → title card |

**Total: ~3 minutes**

---

## Recording Tips

1. **Pre-run all queries** so results are cached — eliminates warehouse spin-up delay
2. **Use keyboard shortcuts** to switch tabs (Cmd+1/2/3) for clean transitions
3. **Pause 1 second** between screen transitions for viewer orientation
4. **Keep mouse movements slow** and deliberate — the viewer is reading
5. **If Bedrock is slow**: pre-generate the narrative, then show the pre-filled result
6. **QuickSight**: have all 3 dashboards pre-loaded in separate tabs, don't show login
7. **Snowsight**: use dark mode for better contrast on recording
8. **Screen zoom**: 110-125% in browser for readability at 1080p
9. **Persona transitions**: pause briefly and say "now let's switch to the [persona]'s view" for clarity

---

## Fallback: If Bedrock Isn't Configured

Replace the Bedrock segment (1:45–2:10) with:

> *"The SAR narrative is generated by Amazon Bedrock — here's what that looks like."*

**Screen**: Open `demo/sample_sar_narrative.md` in a viewer → scroll through the SAR narrative

This preserves the Bedrock story without needing live credentials.

## Fallback: If QuickSight Isn't Connected

Replace the CCO segment (0:50–1:20) with Snowsight queries against the same views:

```sql
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ALERT_VOLUME LIMIT 20;
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_CASE_SLA LIMIT 20;
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_KRIs;
```

> *"These are the same governed views that power the CCO's QuickSight dashboards."*
