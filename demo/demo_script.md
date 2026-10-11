# Singapore Trade Surveillance

**Singapore - Harbourline Digital Exchange (fictional)**
Use case: Trade surveillance and financial crime

> Surveillance for 40 accounts at Harbourline Digital Exchange, a fictional Singapore digital-asset exchange, registered across 5 Singapore districts: dynamic tables, a holdout-evaluated suspicious-activity classifier, an alert-volume forecast and grounded AI answers. Amounts are in SGD.

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

Districts (registered address): Raffles Place, Tampines, Jurong East, Woodlands, Punggol.
Account types: Retail, VIP, Market Maker, Institutional, OTC Desk.

## KPI cards (live from `CURATED.KPI_SUMMARY`; no fallback values)

| Card | Value from the seeded data |
|---|---|
| Alert Precision | 31.4% |
| Alerts Raised | 606 |
| Confirmed Suspicious | 190 |
| SARs Filed | 88 |
| Notional Monitored (SGD M) | 19,437 |
| Account Review Compliance | 75.9% |
| Accounts Monitored | 40 |
| KYC Document Coverage | 55.4% |
| KYC Documents Pending | 19 |

Values are synthetic. A rebuild reproduces them because the data is HASH-seeded; dates are relative to the build day, and forecast and anomaly figures can shift slightly with the build day.

## Demo flow

1. Executive Cockpit (Priya Nair, Chief Compliance Officer): KPIs, daily alerts against confirmed alerts, alerts and confirmed alerts by detection rule, account table by district
2. Predictive (Farah Rahman, Financial Crime Investigator): holdout metrics, risk bands, 14-day alert forecast, self-match ratio anomalies
3. Account Review: review compliance, KYC coverage and pending documents, review compliance against confirmed alerts, then generate the action memo
4. Live Trades: run `CALL APP.SIMULATE_TRADES(20)` (Snowflake only) or `python aws/publish_trades.py --count 20` (AWS build). Then run `EXECUTE ALERT APP.LIVE_TRADE_ALERT` and show the alert log and email.
5. Ask AI: the Cortex Agent answers metric questions through the semantic view and cites SOPs from Cortex Search. The SQL is shown.
6. QuickSight (AWS build): the same Snowflake tables through DIRECT_QUERY
7. Architecture: both builds side by side

## Talking points

- About one alert in three is confirmed as suspicious (31.4%). Most alert reviews end as false positives, which is where investigator time goes.
- Structuring produces the most confirmed alerts (54 of 152). The 16 phishing campaign sweep alerts hit every account registered in Tampines or Woodlands on one day and are never confirmed.
- Tampines accounts carry the most confirmed alerts (57); Jurong East the fewest (20).
- The risk model is evaluated on a time-based holdout: precision 0.48 and recall 0.34 at 0.5, against a 0.27 base rate. Present it as triage, not a verdict.
- Phishing campaign sweeps are excluded from model training, because they are not account-driven.

## Business impact

Use only the sourced references in `README.md` (Business Impact).
