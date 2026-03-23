# Demo Video Script — Digital Asset Market Surveillance
## 2–3 Minute Recorded Walkthrough
**Format**: Screen recording with voiceover
**Target**: Booth video loop / pre-meeting teaser / social share
**Pre-requisites**: Data loaded, Streamlit app deployed, QuickSight dashboard published

---

## Two Personas

| Persona | Role | Tool | What they care about |
|---|---|---|---|
| **Compliance Investigator** | SURVEILLANCE_ANALYST / SURVEILLANCE_ADMIN | Streamlit in Snowflake | Cases, alerts, entity profiles, SAR drafts, masked PII |
| **Chief Compliance Officer (CCO)** | Executive stakeholder | Amazon QuickSight | Alert trends, risk heatmaps, case SLA, KRIs/KPIs |

---

## What's Built

| Layer | Component | Detail |
|---|---|---|
| **Ingest (AWS)** | Amazon MSK | Streaming trade events into Snowflake via Snowpipe Streaming |
| | Amazon AppFlow | CRM data sync |
| | Amazon Transfer Family + S3 | Historical transactions (fraudulent + legitimate) |
| | AWS Glue | Client application data processing |
| | Amazon Cognito | Client biometric/identity data |
| | Snowflake Marketplace | Demographics, Experian, LexisNexis, Refinitiv, D&B |
| **RAW** | 6 tables | CEX_TRADES_RAW, CEX_ORDERS_RAW, CEX_BALANCES_RAW, CEX_LOGS_RAW, ONCHAIN_EVENTS_RAW, DLQ_ERRORS |
| **HARMONISED** | 5 Dynamic Tables | TRADES (1 min), ORDERS (1 min), BALANCES_SNAPSHOT (5 min), ONCHAIN_TRANSFERS (2 min), WALLET_DISCOVERY (5 min) |
| **FEATURES** | 2 Dynamic Tables | TRADE_FEATURES (5 min), ENTITY_FEATURES (10 min) |
| **ANALYTICS** | 1 Dynamic Table | ALERT_SCORES (10 min) — runs XGBoost UDF per entity |
| | 6 Detection Rule Views | Wash Trade, Cross-Exchange Arbitrage, Sanctioned Counterparty, Pump & Dump, Structuring, Mixer Exposure |
| | 5 QuickSight Views | Alert Volume, Case SLA, Entity Heatmap, On-Chain Flows, Trading Summary |
| | KRI + KPI Views | VW_KRIS, VW_KPIS |
| | 4 Tables | ALERTS, CASES, CASE_EVENTS, ALERT_SCORES |
| | 3 Tasks | TASK_REFRESH_ALERTS (5 min), TASK_AUTO_CREATE_CASES (10 min), TASK_SLA_CHECK (30 min) |
| **ML** | 1 Python UDF | FRAUD_RISK_SCORE — XGBoost model returning probability 0–1 |
| **Governance** | 4 Masking Policies | MASK_FULL_NAME, MASK_EMAIL, MASK_PHONE, MASK_DOB |
| | 4 Roles | SURVEILLANCE_ADMIN, SURVEILLANCE_ANALYST, SURVEILLANCE_INGEST, SURVEILLANCE_ML |
| **Consumption** | Streamlit | INVESTIGATOR_COPILOT — case triage + Bedrock SAR generation |
| | Amazon QuickSight | Unified CCO Dashboard (5 tabs) + Amazon Q natural language Q&A — all from governed Snowflake views |
| | Amazon Bedrock | Claude Sonnet via External Access for SAR narrative generation |

**Current data**: 5,400 trades | 1,000 on-chain transfers | 857 wallets | 50 entities | 772 alerts | 84 cases

---

## Pre-Recording Checklist

- [ ] Run `SNOWFLAKE_CONNECTION_NAME=<SF_CONNECTION> python scripts/generate_synthetic_data.py --quick`
- [ ] Verify health check: alerts > 50, cases > 5
- [ ] Open Snowsight in Chrome (full screen, dark mode)
- [ ] Open Streamlit Investigator Copilot (full screen)
- [ ] Open QuickSight → Dashboards → "CCO Weekly Surveillance Briefing" (5 tabs + Q bar)
- [ ] Test Amazon Q: type "How many SLA breached cases?" in the Q bar to confirm the Topic works
- [ ] Open architecture diagram (`architecture.drawio` exported to PNG/SVG)
- [ ] Pre-warm Bedrock: click "Pre-warm Bedrock" button in Streamlit sidebar
- [ ] Audio: quiet room, external mic recommended
- [ ] Resolution: 1920x1080, 60fps

---

## Script

### [0:00–0:20] HOOK — The Problem (Show: Architecture Diagram)

> *"Every crypto exchange has the same problem: they're processing millions of trades per day, but compliance teams are still investigating fraud with spreadsheets — and the CCO has no real-time view of risk exposure."*

> *"We built a unified surveillance platform on Snowflake and AWS that serves two personas from one governed data set: the investigator who triages cases, and the executive who tracks risk."*

**Screen**: Architecture diagram (full screen, 3 seconds) — point to the six AWS data flows on the left, then pan right to the Snowflake processing and consumption layer

---

### [0:20–0:50] DATA FLOW — AWS to Snowflake (Show: Snowsight SQL)

> *"On the AWS side, Amazon MSK streams trade events in real time. AppFlow syncs CRM data, Transfer Family loads historical transactions through S3, Glue processes client applications, and Cognito handles identity. Everything lands in Snowflake's RAW layer via Snowpipe Streaming."*

**Screen**: Run in Snowsight:
```sql
SELECT COUNT(*) AS total_raw_trades, MAX(_LOAD_TS) AS latest_ingest
FROM CRYPTO_SURVEILLANCE.RAW.CEX_TRADES_RAW;
```

> *"Eight Dynamic Tables automatically transform raw JSON into typed, deduplicated data — no orchestrator, no Airflow, no DAGs. Six detection rule views and an XGBoost ML model score every entity continuously."*

**Screen**: Run:
```sql
SELECT alert_type, COUNT(*) AS alert_count,
       ROUND(AVG(ml_fraud_probability), 3) AS avg_ml_score
FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
GROUP BY 1 ORDER BY 2 DESC;
```

> *"Wash trading, cross-exchange arbitrage, sanctions exposure, pump-and-dump, structuring, and mixer exposure — each rule runs as a Snowflake view. The XGBoost model adds a fraud probability score to every entity."*

---

### [0:50–1:20] PERSONA 1: CCO / EXECUTIVE — QuickSight Dashboard + Amazon Q

> *"Let's start with the CCO's view. This is a single QuickSight dashboard with five tabs — every surveillance question answered in one place. And there's an Amazon Q bar at the top for natural language questions. All powered by governed Snowflake views."*

**Screen**: Switch to QuickSight → Dashboards → "CCO Weekly Surveillance Briefing" → Tab 1

**Tab 1 — "KRI Headlines"**

> *"The headlines. Over 800 alerts in the last 24 hours — 452 critical. 82 open cases, 2 escalated to senior management, and 2 sanctioned entities flagged. SLA breaches are climbing — the team is falling behind, and the CCO can see it in real time."*

**Screen**: 6 KPI cards showing key risk indicators

**Tab 2 — "Alert Volume"**

> *"What's firing. Wash trading dominates — 396 critical alerts, more than half the total. Sanctions alerts have the highest ML confidence at 0.93. The model is confirming what the rules are catching."*

**Screen**: Click tab 2 → alert volume bar chart by type and severity

**Tab 3 — "Case SLA"**

> *"Are we keeping up? 16 critical cases, 18 high, 48 medium — and 2 have already been escalated with a perfect ML fraud score of 1.0. The team is falling behind. The CCO sees this and knows exactly where to add headcount."*

**Screen**: Click tab 3 → case backlog by priority + SLA KPI + detail table

**Tab 4 — "Entity Risk"**

> *"Where is risk concentrating? Entity 18 — sanctions-flagged, ML score 1.0, $1.7 million in sanctioned transaction volume. Unverified accounts are showing mixer interactions. The CCO flags this for immediate escalation."*

**Screen**: Click tab 4 → entity risk bar chart + detail table

**Tab 5 — "On-Chain Flows"**

> *"On-chain flows. USDT on Ethereum is the highest-volume token with confirmed sanctioned transactions. Cross-chain movement across Ethereum, Polygon, Arbitrum, and Optimism. This is the data that shapes the board report."*

**Screen**: Click tab 5 → line chart (volume by chain over time) + sanctioned bar by token + cross-chain table

**Amazon Q Demo** (optional, if time allows)

> *"And here's the game-changer — the CCO types a question directly. 'How many sanctioned transactions on Ethereum?' Amazon Q answers instantly from the same governed Snowflake data. No dashboards, no navigation — just ask."*

**Screen**: Click Q bar → type "How many sanctioned transactions on Ethereum?" → show the answer

---

### [1:20–2:10] PERSONA 2: INVESTIGATOR — Streamlit + Bedrock

> *"Now let's switch to the investigator's world. This is a Streamlit app running inside Snowflake — no data leaves the platform."*

**Screen**: Switch to Streamlit app — show the KRI dashboard row at the top

> *"At the top, Key Risk Indicators: alerts in the last 24 hours, critical count, open cases, SLA breaches. The investigator filters by priority in the sidebar — CRITICAL — and picks a case."*

**Screen**: Sidebar → Priority = CRITICAL → select top case from the dropdown

> *"The case detail shows the entity profile — KYC tier, AML risk rating, PEP and sanctions flags. Below that, the linked alerts, seven days of CEX trades, and 30 days of on-chain activity."*

**Screen**: Scroll through Entity Profile → Alerts table → CEX Trades → On-Chain Activity

> *"One click calls Amazon Bedrock — Claude — through Snowflake's External Access Integration. The LLM gets the full case context and produces a FinCEN-format SAR narrative, a case summary, risk indicators, and recommended next steps."*

**Screen**: Click "Generate Case Narrative & SAR Draft (Bedrock)" → narrative appears → scroll through SAR → highlight "Recommended Action: FILE_SAR"

> *"The investigator can escalate, close, or mark false positive — all from the same screen. Every action is logged to the case audit trail."*

**Screen**: Point to the action buttons (Start Review, Escalate, Close — TP, False Positive)

---

### [2:10–2:40] GOVERNANCE — The Regulator Story (Show: Snowsight SQL)

> *"And here's what makes this regulator-ready. Four masking policies protect PII by role. Watch — the analyst sees masked names and emails."*

**Screen**: Run:
```sql
USE ROLE SURVEILLANCE_ANALYST;
SELECT entity_id, full_name, email, aml_risk_rating
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY LIMIT 5;
```

> *"Switch to admin — full PII appears. Same table, same query, different governance."*

**Screen**: Run:
```sql
USE ROLE SURVEILLANCE_ADMIN;
SELECT entity_id, full_name, email, aml_risk_rating
FROM CRYPTO_SURVEILLANCE.HARMONISED.ENTITY LIMIT 5;
```

> *"Four roles control access across the entire pipeline — ingest, ML, analyst, admin. Every case action is logged in an immutable audit trail. The CCO's QuickSight dashboard and the investigator's Streamlit app both read from the same governed views."*

---

### [2:40–3:00] CLOSE — Better Together (Show: Architecture Diagram)

> *"AWS provides the data plane — MSK for streaming, S3 for storage, AppFlow and Glue for integration, Bedrock for AI, QuickSight for the executive dashboards. Snowflake is the governance and analytics control plane — Dynamic Tables, Snowpark ML, Streamlit for the investigator, Horizon for compliance."*

> *"Two personas, two tools, one governed platform. That's how you build a financial crime system that a regulator will actually trust."*

**Screen**: Architecture diagram (full frame) → fade to title card:

**Title Card** (3 seconds):
```
Digital Asset Market Surveillance
Snowflake + AWS | Better Together

Investigator: Streamlit in Snowflake
Executive: Amazon QuickSight + Amazon Q
One governed data platform.

<YOUR_EMAIL>
```

---

## Timing Summary

| Segment | Duration | Persona | Screen |
|---|---|---|---|
| Hook & Problem | 0:00–0:20 | Both | Architecture diagram |
| Data Flow + Detection | 0:20–0:50 | — | Snowsight SQL queries |
| CCO / Executive View | 0:50–1:20 | CCO | QuickSight Dashboard (5 tabs) + Amazon Q |
| Investigator View | 1:20–2:10 | Investigator | Streamlit app + Bedrock |
| Governance | 2:10–2:40 | Both | Snowsight SQL (role switching) |
| Close | 2:40–3:00 | Both | Architecture diagram → title card |

**Total: ~3 minutes**

---

## QuickSight Dashboard + Amazon Q Setup

### Unified CCO Dashboard (✅ Deployed)

**Dashboard ID**: `<DASHBOARD_ID>`
**Dashboard Name**: "CCO Weekly Surveillance Briefing"

| Tab | Sheet ID | Visuals | Snowflake Source |
|---|---|---|---|
| **1 - KRI Headlines** | `cco-kri-headlines` | 6 KPI cards (Alerts 24h, Critical 24h, Open Cases, Escalated, SLA Breached, Sanctioned) | `VW_KRIS` |
| **2 - Alert Volume** | `cco-alert-volume` | Stacked bar (type × severity) | `VW_QUICKSIGHT_ALERT_VOLUME` |
| **3 - Case SLA** | `cco-case-sla` | Bar chart (priority × state) + SLA KPI + detail table | `VW_QUICKSIGHT_CASE_SLA` |
| **4 - Entity Risk** | `cco-entity-risk` | Risk indicator bar + KRI detail table | `VW_QUICKSIGHT_ENTITY_HEATMAP` |
| **5 - On-Chain Flows** | `cco-onchain-flows` | Line chart (volume × chain) + sanctioned bar by token + cross-chain table | `VW_QUICKSIGHT_ONCHAIN_FLOWS` |

### Amazon Q Topic (✅ Deployed)

**Topic ID**: `<TOPIC_ID>`
**Topic Name**: "Crypto Surveillance"
**Datasets**: All 5 (KRI Summary, Alert Volume, Case SLA, Entity Risk, On-Chain Flows)
**Total Columns**: 37 with NL synonyms

Sample questions the CCO can ask in the Q bar:
- "How many alerts in the last 24 hours?"
- "Show me SLA breached cases"
- "Which tokens have sanctioned transactions?"
- "What is the average ML score for critical alerts?"
- "How many open cases by priority?"

### Individual Dashboards (also deployed, for reference)

| Dashboard | Dashboard ID |
|---|---|
| KRI Summary | `crypto-kri-summary-dashboard` |
| Alert Volume | `crypto-alert-volume-dashboard` |
| Case SLA | `crypto-case-sla-dashboard-v2` |
| Entity Risk Heatmap | `crypto-entity-risk-dashboard-v2` |
| On-Chain Flows | `crypto-onchain-flows-dashboard` |

### Demo Flow

1. Open the unified dashboard → start on Tab 1 (KRI Headlines)
2. Click through tabs 1→2→3→4→5, reading the talking points for each
3. After Tab 5, click the **Q bar** at top → type a question → show instant answer
4. Total: 5 tabs × ~6 seconds + Q demo = ~35 seconds (fits the 0:50–1:20 window)

---

## CCO Board Briefing — Talking Points

These are the executive talking points the CCO would deliver to the board using the QuickSight dashboard. Every number comes from the actual governed views.

### The Headlines (VW_KRIS)
> *"We processed 5,400 trades and 1,000 on-chain transfers this period. The system generated 772 alerts — 452 critical. We have 82 open cases and 2 sanctioned entities with confirmed exposure."*

### Alert Landscape (VW_QUICKSIGHT_ALERT_VOLUME)
> *"Wash trading is our biggest exposure — 396 critical alerts, more than half the total. Sanctions alerts carry the highest ML confidence at 93%. Mixer exposure affects 15 entities with moderate ML confirmation at 63%. Pump-and-dump activity is concentrated across 9 entities."*

### Operational Capacity (VW_QUICKSIGHT_CASE_SLA)
> *"We opened 82 cases this week and closed zero. The CRITICAL backlog is 16 cases with an average ML score of 0.69 — the model confirms these are real. SLA compliance is 100% today, but with zero cases in active review, breaches are imminent. Recommendation: prioritise the 16 critical cases and reassign analyst capacity from MEDIUM."*

### Risk Concentration (VW_QUICKSIGHT_ENTITY_HEATMAP)
> *"Our highest-risk customer is Entity 18 — CRITICAL AML rating, sanctions-flagged, ML score 1.0, $1.7 million in sanctioned transaction volume. We also have unverified accounts showing mixer interactions — a KYC gap that needs immediate remediation. Institutional accounts are not clean — Entity 1 has $106K in sanctioned volume."*

### Cross-Chain Exposure (VW_QUICKSIGHT_ONCHAIN_FLOWS)
> *"USDT on Ethereum carries the highest volume and has confirmed sanctioned transactions. We're seeing active movement across four chains — Ethereum, Polygon, Arbitrum, and Optimism. Cross-chain monitoring is critical; risk doesn't stay on one chain."*

### Executive Recommendation
> *"Immediate actions: (1) Follow up on the 2 escalated cases — both have ML score 1.0, confirming high-confidence fraud. (2) Deploy additional analyst capacity to the 16 CRITICAL cases before more SLA breaches occur. (3) Initiate enhanced KYC review on unverified accounts with mixer exposure. (4) File SAR on Entity 18 — ML and rules both confirm high-confidence fraud."*

---

## Q&A — Anticipated Questions

### Architecture & Data Flow
| Question | Answer | What to Show |
|---|---|---|
| "How does trade data get from the exchange into Snowflake?" | MSK → Snowpipe Streaming → RAW layer, sub-second | `SELECT COUNT(*), MAX(_LOAD_TS) FROM RAW.CEX_TRADES_RAW` |
| "What happens if bad data comes in?" | Dead letter queue catches malformed events | `SELECT * FROM RAW.DLQ_ERRORS LIMIT 5` |
| "How do you orchestrate the pipeline?" | We don't — 8 Dynamic Tables with target lags from 1 to 10 minutes | `SHOW DYNAMIC TABLES IN DATABASE CRYPTO_SURVEILLANCE` |

### Detection & ML
| Question | Answer | What to Show |
|---|---|---|
| "How do you detect wash trading vs structuring?" | 6 rule views + 1 XGBoost UDF — rules catch known patterns, ML catches what rules miss | `SELECT alert_type, COUNT(*) FROM ALERTS GROUP BY 1` |
| "Can the model explain its scores?" | 18 input features: trade velocity, mixer interactions, sanctioned volume, burst behaviour | `SELECT * FROM FEATURES.ENTITY_FEATURES LIMIT 5` |
| "What's the ML model confidence?" | Sanctions: 0.93 avg, Mixer: 0.63 avg — high-confidence vs moderate | Alert volume query with avg_ml_score |

### Governance & Compliance
| Question | Answer | What to Show |
|---|---|---|
| "How do you handle PII?" | 4 masking policies (name, email, phone, DOB) applied by role | Run ANALYST vs ADMIN query side-by-side |
| "Can you prove who did what?" | Immutable audit trail — every state change logged with timestamp and user | `SELECT * FROM CASE_EVENTS ORDER BY event_ts DESC LIMIT 10` |
| "How many roles control access?" | 4 — INGEST (write RAW only), ML (FEATURES only), ANALYST (masked PII), ADMIN (full access) | `SHOW ROLES LIKE 'SURVEILLANCE%'` |

### Bedrock / AI
| Question | Answer | What to Show |
|---|---|---|
| "Why Bedrock and not a Snowflake LLM?" | Bedrock gives Claude for complex narrative generation; External Access keeps data governed | Click "Generate Case Narrative" in Streamlit |
| "What context does the LLM receive?" | Full case: entity profile, all alerts, trade history, on-chain activity, ML scores — assembled by a stored procedure | Scroll through the SAR output sections |

### QuickSight / Executive View
| Question | Answer | What to Show |
|---|---|---|
| "Why QuickSight instead of Streamlit for the CCO?" | Different personas, different tools — CCO wants tabbed dashboards, NL questions via Amazon Q, drill-downs, email delivery | Click through the dashboard tabs + type a Q question |
| "Are the dashboards real-time?" | QuickSight reads directly from Snowflake views (DIRECT_QUERY); refreshes on each page load | Show the data timestamps in the KRI tab |

### The "Better Together" Closer
| Question | Answer |
|---|---|
| "Why can't you do this with just AWS or just Snowflake?" | AWS gives MSK for streaming, S3 for scale, Bedrock for AI, QuickSight for the exec. Snowflake gives Dynamic Tables for zero-orchestration, Snowpark ML for in-platform scoring, Streamlit for the investigator, Horizon for governance. Neither alone covers both personas end-to-end. |

---

## Recording Tips

1. **Pre-run all queries** so results are cached — eliminates warehouse spin-up delay
2. **Pre-warm Bedrock** using the sidebar button — the SAR narrative is cached for 10 minutes
3. **QuickSight**: have the dashboard open on Tab 1 ("KRI Headlines") — click through tabs during recording
4. **Pause 1 second** between tab transitions for viewer orientation
5. **Keep mouse movements slow** and deliberate — the viewer is reading
6. **Snowsight**: use dark mode for better contrast on recording
7. **Screen zoom**: 110-125% in browser for readability at 1080p
8. **Persona transitions**: pause briefly and say "now let's switch to the [persona]'s view" for clarity

---

## Fallback: If Bedrock Isn't Configured

Replace the Bedrock segment (1:50–2:10) with:

> *"The SAR narrative is generated by Amazon Bedrock — here's what that looks like."*

**Screen**: Open `demo/sample_sar_narrative.md` in a viewer → scroll through the SAR narrative

This preserves the Bedrock story without needing live credentials.

## Fallback: If QuickSight Isn't Connected

Replace the CCO segment (0:50–1:20) with Snowsight queries against the same governed views:

```sql
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_KRIS;
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ALERT_VOLUME LIMIT 20;
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_CASE_SLA LIMIT 20;
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ENTITY_HEATMAP LIMIT 10;
SELECT * FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_QUICKSIGHT_ONCHAIN_FLOWS LIMIT 20;
```

> *"These are the same governed views that power the CCO's QuickSight dashboard — alert volume, case SLA, entity risk heatmap, and cross-chain flows."*
