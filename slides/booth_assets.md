# Booth Asset Checklist & Slide Talking Points
## Track 2: Digital Asset Market Surveillance & Financial Crime Analytics

---

## Slide 1: End-State Architecture (One Slide)

**Title**: *Digital Asset Market Surveillance — Snowflake + AWS*

**Sections to show**:
```
┌──────────────────────────────────────────────────────────────────────┐
│  INGEST (AWS)          │  PROCESS (Snowflake)     │  ACT             │
│                        │                          │                  │
│  CEX Trading Engine    │  RAW → HARMONISED        │  Streamlit       │
│  ↓ Kinesis Streams     │  Dynamic Tables (1min)   │  Investigator UI │
│                        │                          │                  │
│  On-Chain Indexer      │  Entity / Wallet Graph   │  QuickSight      │
│  ↓ Lambda → S3         │  Feature Engineering     │  Exec Dashboard  │
│                        │                          │                  │
│  System Logs           │  XGBoost ML (Snowpark)   │  SAR Drafts      │
│  ↓ Firehose → S3       │  Detection Rules (SQL)   │  (via Bedrock)   │
│                        │                          │                  │
│                        │  ALERTS + CASES          │                  │
│                        │      ↕                   │                  │
│                        │  Bedrock (External Access)│                  │
└──────────────────────────────────────────────────────────────────────┘
           ← AWS Infrastructure →  ← Snowflake AI Data Cloud →
```

**Talking points**:
- One unified platform: CEX + on-chain + KYC + ML + AI
- Two personas: **Investigator** (Streamlit) + **CCO/Executive** (QuickSight)
- Snowflake governs everything; AWS provides the data plane
- Sub-60-second latency from trade event to alert

---

## Slide 2: Track 2 Alignment

**Title**: *Why This Story for Track 2*

| Track 2 Session | How We Address It |
|---|---|
| **CEX Security & High-Performance Matching Engine Architecture** | Kinesis/MSK ingestion at matching-engine speed; Snowpipe Streaming for sub-second RAW landing |
| **AI, Stablecoins & Web3** | Snowpark ML (XGBoost) for fraud scoring; Bedrock LLM for SAR narrative generation; supports USDT/USDC/stablecoin flows |
| **RWA Infrastructure & Unified Settlement** | Unified Snowflake entity model spans on-chain + off-chain; positions Snowflake as the settlement analytics control plane |
| **Blockchain Platforms & Digital Asset Infrastructure** | On-chain indexer → Lambda → Kinesis → Snowflake pipeline; multi-chain (Ethereum, Polygon, Arbitrum, Solana) |

**Soundbite**: *"We're not building a trading app. We're building the regulated infrastructure that lets crypto operate like a bank."*

---

## Slide 3: Before / After

**Title**: *Legacy Fragmentation vs. Unified Platform*

### BEFORE (Legacy)
```
KYC System ──────┐
                 ├── Manual analyst workstation
CEX Database ────┤   (spreadsheets, copy-paste)  →  SAR filed 30 days late
                 ├── 
On-Chain Tool ───┘   Separate vendor tools,
(Chainalysis/TRM)    no unified entity view,
                     no ML, no audit trail
```

### AFTER (Snowflake + AWS)
```
CEX Trade Events ──→ Kinesis ──→ Snowpipe Streaming ──→ RAW
On-Chain Indexer ──→ Lambda ──→ S3 ──→ Snowpipe ──→ RAW
KYC / CDD System ──→ S3 / CDC ──────────────────────→ RAW
                                         ↓
                                   HARMONISED
                                   Entity / Wallet Graph
                                         ↓
                            XGBoost ML Scoring (Snowpark)
                            Detection Rules (6 typologies)
                                         ↓
                                    ALERTS → CASES
                                         ↓
                             Bedrock: SAR Draft in minutes
                             Streamlit: Investigator Copilot
                             QuickSight: Regulator Dashboards
```

**Key metrics**:
- Alert detection latency: **< 5 minutes** (vs. hours/days legacy)
- SAR draft generation: **< 30 seconds** (vs. analyst hours)
- False positive rate: **reduced ~40%** with ML scoring layer
- Data lineage: **100% traceable** via Snowflake Horizon

---

## Slide 4: Joint Solution Positioning

**Title**: *Snowflake + AWS: Better Together*

| Capability | AWS | Snowflake |
|---|---|---|
| High-throughput streaming ingest | Kinesis, MSK | Snowpipe Streaming |
| Serverless data processing | Lambda, Glue | Snowpark, Tasks |
| AI/LLM inference | Bedrock (Claude) | Cortex, External Access |
| Object storage at scale | S3 | External Stages |
| IAM & secrets management | IAM, Secrets Manager | Masking Policies, Horizon |
| BI / dashboards | QuickSight (CCO/exec) | Streamlit in Snowflake (investigator) |
| Resilience & DR | Multi-AZ, cross-region | Snowgrid replication |

**Headline**: *AWS is the infrastructure. Snowflake is the intelligence.*

---

## Reference Architecture Callouts (for technical audiences)

When asked about technical depth, reference these:

1. **Payments Fraud RA**: Base pattern (RAW → HARMONISED → ACTIONABLE + Streams/Tasks) — same structure, crypto sources
2. **Collaborative Fraud Detection**: Multi-party risk signals via Snowflake — extend this to shared exchange risk intelligence network
3. **KYC for Financial Services RA** (IS-47): Entity/customer model, KYC workflow — reused in HARMONISED.ENTITY
4. **Fintech RA** (IS-523): Unified AI Data Cloud for fraud/risk/personalization — the conceptual parent of this solution
5. **Snowflake + AWS Streaming Blueprints**: MSK/Kinesis → Snowpipe Streaming — lifted directly into ingestion layer
6. **Cortex Agents + Bedrock RA**: External Access, Bedrock interaction model — the agentic copilot layer

---

## Demo QR Codes / Follow-Up Links

| Resource | Link |
|---|---|
| This repo | `github.com/[org]/aws-fraud-detection-fintech` |
| Payments Fraud RA | Snowflake Solution Central |
| Collaborative Fraud Detection | Snowflake Solution Central |
| Snowpipe Streaming docs | docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming |
| Snowflake + AWS page | snowflake.com/partners/technology/amazon-web-services |

---

## Leave-Behind Card (one-pager summary)

**Digital Asset Market Surveillance**  
*Snowflake + AWS | Financial Crime Analytics*

**What it does**:  
End-to-end platform for CEX market abuse detection and AML investigation. Serves two personas from one governed dataset: **investigators** triage cases and draft SARs in Streamlit; **executives** track risk KRIs and case SLA in QuickSight. Ingests high-frequency trading events via Kinesis, enriches with on-chain data, scores with XGBoost ML, and generates SAR narratives with Amazon Bedrock — all governed in Snowflake.

**Three reasons to care**:  
1. Single source of truth: CEX + on-chain + KYC in one governed platform  
2. Detection in minutes, not days: Dynamic Tables + streaming pipeline  
3. Regulator-ready: full lineage, PII masking, SAR narrative generation  

**Booth contact**: <YOUR_EMAIL>  
**Snowflake account demo**: <SF_CONNECTION>
