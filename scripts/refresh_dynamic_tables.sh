#!/usr/bin/env bash
# =============================================================================
# Force-refresh all Dynamic Tables in dependency order.
# Run after loading synthetic data to see results immediately (bypasses lag).
# =============================================================================
set -euo pipefail

SNOW=${SNOWSQL_BINARY:-snowsql}

refresh_table() {
    local tbl="$1"
    echo "  → Refreshing ${tbl}..."
    $SNOW -q "ALTER DYNAMIC TABLE ${tbl} REFRESH;" 2>&1 | grep -v "^Row\|^---\|^1 Row" || true
}

echo "==> Refreshing HARMONISED layer..."
refresh_table "CRYPTO_SURVEILLANCE.HARMONISED.TRADES"
refresh_table "CRYPTO_SURVEILLANCE.HARMONISED.ORDERS"
refresh_table "CRYPTO_SURVEILLANCE.HARMONISED.BALANCES_SNAPSHOT"
refresh_table "CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS"
refresh_table "CRYPTO_SURVEILLANCE.HARMONISED.WALLET_DISCOVERY"

echo "==> Refreshing FEATURES layer..."
refresh_table "CRYPTO_SURVEILLANCE.FEATURES.TRADE_FEATURES"
refresh_table "CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES"

echo "==> Refreshing ANALYTICS layer..."
refresh_table "CRYPTO_SURVEILLANCE.ANALYTICS.ALERT_SCORES"

echo "✓ All Dynamic Tables refreshed"
