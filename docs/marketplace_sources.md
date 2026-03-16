# Snowflake Marketplace Data Sources
## Digital Asset Market Surveillance — Marketplace Integration Guide

> **Feature flag**: set `SURV_USE_MARKETPLACE_DATA=true` (or `USE_MARKETPLACE_DATA=true` in `.env`)
> to activate Marketplace views in the HARMONISED and FEATURES layers.
> In `false` mode (default), all analytics run on synthetic data only — no external dependencies.

---

## Pre-wired: Snowflake Public Data (Free) — No Subscription Required

The platform ships with **real market data** from `SNOWFLAKE_PUBLIC_DATA_FREE`, a free
Marketplace listing that is auto-provisioned on most Snowflake accounts (including `demo43`).

### What's Available (zero-cost, already wired into `03_harmonised.sql`)

| View | Source Table | Data | Date Range |
|---|---|---|---|
| `VW_MARKETPLACE_PRICES` | `STOCK_PRICE_TIMESERIES` | Daily OHLCV for BTC, ETH, GBTC, ETHE, COIN, BITO, MARA, RIOT, MSTR | 2018 – present |
| `VW_MARKETPLACE_FX_RATES` | `FX_RATES_TIMESERIES` | USD exchange rates vs 150+ currencies | Rolling |
| `VW_MARKET_PRICES` | Union of marketplace + synthetic | Unified price view — marketplace data takes priority | Always |

### How It Works

1. `VW_MARKETPLACE_PRICES` pivots the EAV `STOCK_PRICE_TIMESERIES` into OHLCV format
   (open, high, low, close, volume) for crypto-related ETFs and equities
2. `VW_MARKET_PRICES` is a UNION ALL of marketplace prices + synthetic fallback —
   for any asset/date where marketplace data exists, synthetic is excluded
3. `VW_PUMP_AND_DUMP_CANDIDATES` (in `06_analytics.sql`) joins `VW_MARKET_PRICES`
   to compute `marketplace_price_deviation_pct` — the gap between trade price and
   real reference close price, a key signal for price manipulation detection

### Verify the Integration (after build)

```sql
-- Confirm real price data flows through the marketplace view
SELECT asset_ticker, price_date, close_price, data_source
FROM CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKET_PRICES
WHERE asset_ticker IN ('BTC', 'ETH', 'COIN')
  AND price_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY asset_ticker, price_date DESC
LIMIT 20;

-- Confirm FX rates
SELECT * FROM CRYPTO_SURVEILLANCE.HARMONISED.VW_MARKETPLACE_FX_RATES
WHERE QUOTE_CURRENCY_ID IN ('EUR', 'GBP', 'JPY')
  AND rate_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY rate_date DESC LIMIT 10;

-- Confirm Pump & Dump detection now uses reference prices
SELECT account_id, trading_pair, marketplace_ref_price,
       marketplace_price_deviation_pct, ref_price_source
FROM CRYPTO_SURVEILLANCE.ANALYTICS.VW_PUMP_AND_DUMP_CANDIDATES
LIMIT 10;
```

### Tickers Available in SNOWFLAKE_PUBLIC_DATA_FREE

| Ticker | Description | Asset Class | Exchange | Data Since |
|---|---|---|---|---|
| BTC | iShares Bitcoin Trust ETF | ETF | NYSE ARCA | 2021-04 |
| ETH | Grayscale Ethereum Trust | Equity | NYSE | 2018-05 |
| GBTC | Grayscale Bitcoin Trust | ETF | NYSE ARCA | 2024-01 |
| ETHE | Grayscale Ethereum Classic ETF | ETF | NYSE ARCA | 2024-07 |
| COIN | Coinbase Global | Equity | NASDAQ | 2021-04 |
| BITO | ProShares Bitcoin Strategy ETF | ETF | NYSE ARCA | 2021-10 |
| MARA | Marathon Digital Holdings | Equity | NASDAQ | historical |
| RIOT | Riot Platforms | Equity | NASDAQ | historical |
| MSTR | MicroStrategy (Strategy) | Equity | NASDAQ | historical |

---

## Optional: Premium Marketplace Listings

For production or enhanced POC deployments, these premium listings provide
richer data (not required for booth demo):

### Category: Crypto / Digital Assets

| Search Term | Recommended Listing | Provider | What You Get |
|---|---|---|---|
| `crypto price` | **Cybersyn Financial Market Data** | Cybersyn | OHLCV prices for 500+ crypto assets, daily and hourly |
| `digital asset` | **Nasdaq Global Data Service** | Nasdaq | Institutional-grade crypto price feeds |
| `blockchain analytics` | **Allium On-Chain Analytics** | Allium | Decoded on-chain events, wallet labels, DeFi flows |
| `crypto risk` | **TRM Labs Crypto Risk Intelligence** | TRM Labs | Wallet risk scores, sanctions, mixer exposure |
| `defi` | **Flipside Crypto** | Flipside | DeFi protocol activity, wallet analytics |
| `market data` | **Kaiko Digital Assets Market Data** | Kaiko | Order book data, CEX trade data, volatility |
| `sanctions` | **Dow Jones Risk & Compliance** | Dow Jones | Sanctions lists (OFAC, UN, EU), PEP lists |

> **Note**: Availability varies by account tier and region. For demo43 (internal), try Cybersyn first — it is typically pre-approved for Snowflake internal accounts.

### Subscribe (UI walkthrough)

1. Click the listing → **Get** (top-right)
2. Select your target database name:
   - Cybersyn: `CYBERSYN__FINANCIAL_MARKET_DATA`
   - Allium: `ALLIUM__ONCHAIN_ANALYTICS`
   - TRM Labs: `TRM__CRYPTO_RISK_INTELLIGENCE`
   - Kaiko: `KAIKO__DIGITAL_ASSETS`
3. Select your Snowflake account (`demo43`) as the consumer
4. Click **Get Data** → accept terms
5. Update `.env`:
```bash
SURV_MARKETPLACE_PRICE_DB=CYBERSYN__FINANCIAL_MARKET_DATA
SURV_MARKETPLACE_ONCHAIN_DB=ALLIUM__ONCHAIN_ANALYTICS
SURV_MARKETPLACE_KYC_DB=TRM__CRYPTO_RISK_INTELLIGENCE
SURV_USE_MARKETPLACE_DATA=true
```

---

## How Marketplace Data Enriches the Platform

### In `VW_PUMP_AND_DUMP_CANDIDATES` (06_analytics.sql)
Joins `VW_MARKET_PRICES` to compute:
- `marketplace_ref_price` — real ETF/equity closing price for the trade day
- `marketplace_price_deviation_pct` — how far the executed price deviates from reference
- `ref_price_source` — indicates SNOWFLAKE_PUBLIC_DATA_FREE or SYNTHETIC

### In `HARMONISED.ONCHAIN_TRANSFERS`
When premium data is available, the Dynamic Table joins wallet labels to populate:
- `risk_score` — from TRM wallet risk score
- `risk_category` — from TRM (SANCTIONS, MIXER, EXCHANGE, etc.)
- `is_sanctioned_counterparty` — from TRM + Dow Jones list match

### In `FEATURES.ENTITY_FEATURES`
With premium data, additionally computes:
- `max_counterparty_risk_30d` — max TRM risk score seen in 30d on-chain interactions
- `sanctioned_volume_30d` — volume transacted with Dow Jones sanctioned addresses

---

## Data Mode Comparison

| Capability | Synthetic Only (`false`) | With Free Marketplace | Premium Marketplace (`true`) |
|---|---|---|---|
| CEX trade data | Random synthetic | Random synthetic | Random synthetic |
| On-chain event data | Random synthetic | Random synthetic | Random synthetic |
| Price reference | Synthetic price index | **Real BTC/ETH/COIN OHLCV** | Real OHLCV 500+ assets |
| FX rates | N/A | **Real USD FX rates** | Real FX rates |
| Wallet labels | Synthetic flags | Synthetic flags | Real labels from Allium |
| Wallet risk scores | Heuristic | Heuristic | TRM Labs ML scores |
| Sanctions lists | Hardcoded demo addresses | Hardcoded demo addresses | Dow Jones live feed |
| P&D ref price deviation | Not available | **Available (BTC/ETH/COIN)** | Available (all assets) |
| Setup dependencies | None | None (auto-provisioned) | Marketplace subscriptions |
| Suitable for | Offline dev | **Booth demo (default)** | POC, production |

---

## Troubleshooting

**"Object does not exist" on SNOWFLAKE_PUBLIC_DATA_FREE**
→ This DB is auto-provisioned on most accounts. Verify:
```sql
SHOW DATABASES LIKE 'SNOWFLAKE_PUBLIC%';
```
→ If missing, search Marketplace for "Snowflake Public Data (Free)" and click Get.

**Marketplace price view returns no rows for a specific ticker**
→ Check the ticker exists in the source:
```sql
SELECT DISTINCT TICKER FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE TICKER IN ('BTC','ETH','COIN');
```

**"Object does not exist" on premium shared DB tables**
→ Verify subscription completed: `SHOW DATABASES LIKE 'CYBERSYN%';`
→ Ensure your role has `IMPORTED PRIVILEGES`:
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE CYBERSYN__FINANCIAL_MARKET_DATA
    TO ROLE SURVEILLANCE_ADMIN;
```

---

## Provider Contact / Licensing

| Provider | Contact | Trial Available |
|---|---|---|
| Snowflake Public Data (Free) | Built-in | **Yes — free, auto-provisioned** |
| Cybersyn | [cybersyn.com](https://cybersyn.com) | Yes — free tier on Marketplace |
| Allium | [allium.so](https://allium.so) | Yes — via Marketplace trial |
| TRM Labs | [trmlabs.com](https://trmlabs.com) | Request via Marketplace |
| Kaiko | [kaiko.com](https://kaiko.com) | Request via Marketplace |
| Dow Jones | [djrisk.com](https://risk.dowjones.com) | Request via Marketplace |
