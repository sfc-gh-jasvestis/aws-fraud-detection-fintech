# Market Surveillance

**APJ - Digital Asset Exchange**
Use case: Trade surveillance and financial crime

> Surveillance for 40 accounts on a fictional digital-asset exchange across 5 APJ markets: dynamic tables, a holdout-evaluated suspicious-activity classifier, an alert-volume forecast and grounded AI answers.

## Why Snowflake

- **Dynamic tables** reconcile alerts, confirmed alerts, SARs and review compliance from RAW account data, with checks in `run_core.py`
- **Suspicious-activity classification** gives a holdout-evaluated next-7-day probability per account
- **Alert forecast** projects 14 days of exchange-wide alert volume with prediction intervals, for investigator staffing
- **Grounded AI**: the Cortex Agent (Analyst over a semantic view, plus Search over SOPs) shows its SQL and SOP citations
- **Live trades**: a native simulator (Snowflake only) or Firehose, S3 and Snowpipe (AWS build), then an alert and email

## What is built

| | |
|---|---|
| Dimension table | `RAW.ACCOUNTS` (40 rows) |
| Fact table | `RAW.ACCOUNT_DAILY` (3,600 account-days, 90 days) |
| Curated layer | `CURATED.KPI_SUMMARY`, `PERFORMANCE_SUMMARY`, `RULE_SUMMARY`, `TREND_ANALYSIS` |
| ML | `ML.SUSPICION_RISK_SCORES`, `ML.SUSPICION_RISK_HOLDOUT_METRICS`, `ML.ALERT_FORECAST`, `ML.SELF_MATCH_ANOMALIES` |

Markets: Singapore, Hong Kong, Sydney, Tokyo, Seoul.
Account types: Retail, VIP, Market Maker, Institutional, OTC Desk.

## KPI cards (live from `CURATED.KPI_SUMMARY`; no fallback values)

| Card | Value from the seeded data |
|---|---|
| Alert Precision | 35.6% |
| Alerts Raised | 584 |
| Confirmed Suspicious | 208 |
| SARs Filed | 101 |
| Notional Monitored (USD M) | 14,455 |
| Account Review Compliance | 78.5% |
| Accounts Monitored | 40 |
| KYC Document Coverage | 65.3% |
| KYC Documents Pending | 23 |

Values are synthetic. A rebuild reproduces them because the data is HASH-seeded; dates are relative to the build day.

## Demo flow

1. Executive Cockpit: KPIs, daily alerts against confirmed alerts, alerts and confirmed alerts by detection rule, account table
2. Predictive: holdout metrics, risk bands, 14-day alert forecast, self-match ratio anomalies
3. Account Review: review compliance, KYC coverage and pending documents, review compliance against confirmed alerts, then generate the action memo
4. Live Trades: run `CALL APP.SIMULATE_TRADES(20)` (Snowflake only) or `python aws/publish_trades.py --count 20` (AWS build). Then run `EXECUTE ALERT APP.LIVE_TRADE_ALERT` and show the alert log and email.
5. Ask AI: the Cortex Agent answers metric questions through the semantic view and cites SOPs from Cortex Search. The SQL is shown.
6. QuickSight (AWS build): the same Snowflake tables through DIRECT_QUERY
7. Architecture: both builds side by side

## Talking points

- About one alert in three is confirmed as suspicious (35.6%). Most alert reviews end as false positives, which is where investigator time goes.
- Structuring produces the most confirmed alerts. Market volatility spike alerts hit every account in a market at once and are never confirmed.
- The risk model is evaluated on a time-based holdout: precision 0.40 and recall 0.38 at 0.5, against a 0.21 base rate. Present it as triage, not a verdict.
- Market volatility spikes are excluded from model training, because they are not account-driven.

## Business impact

Use only the sourced references in `README.md` (Business Impact).
