#!/usr/bin/env bash
# Executes all Snowflake SQL scripts in order against the <SF_CONNECTION> account.
# Usage:
#   ./run_all.sh                           # core setup only (synthetic data mode)
#   ./run_all.sh <SF_CONNECTION> --marketplace      # also run Marketplace sources SQL
# Requires SnowSQL CLI installed and configured.

set -euo pipefail

CONNECTION="${1:-<SF_CONNECTION>}"
INCLUDE_MARKETPLACE="${2:-}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== Running Snowflake setup against connection: $CONNECTION ==="

run_sql() {
    local file="$1"
    echo "→ Executing: $file"
    snowsql -c "$CONNECTION" -f "$file" --noup --option log_level=ERROR
    echo "  ✅ Done: $file"
}

SCRIPT_DIR="$ROOT_DIR/snowflake"

# ── Core platform (always run) ────────────────────────────────────────────────
run_sql "$SCRIPT_DIR/00_setup.sql"
run_sql "$SCRIPT_DIR/01_integrations.sql"
run_sql "$SCRIPT_DIR/02_raw_tables.sql"
run_sql "$SCRIPT_DIR/03_pipes.sql"
run_sql "$SCRIPT_DIR/04_harmonised.sql"
run_sql "$SCRIPT_DIR/05_entity_graph.sql"
run_sql "$SCRIPT_DIR/06_features.sql"
run_sql "$SCRIPT_DIR/07_analytics.sql"
run_sql "$SCRIPT_DIR/08_surveillance_rules.sql"
run_sql "$SCRIPT_DIR/09_cases.sql"
run_sql "$SCRIPT_DIR/10_bedrock_integration.sql"
run_sql "$SCRIPT_DIR/11_governance.sql"
run_sql "$SCRIPT_DIR/12_monitoring.sql"

# ── Marketplace sources (opt-in: pass --marketplace flag) ─────────────────────
if [[ "$INCLUDE_MARKETPLACE" == "--marketplace" ]]; then
    echo ""
    echo "→ Running Marketplace sources setup …"
    run_sql "$ROOT_DIR/sql/setup/10_marketplace_sources.sql"
    echo "  ✅ Marketplace views created."
    echo "  Remember to: GRANT IMPORTED PRIVILEGES on each shared DB"
    echo "               and set SURV_USE_MARKETPLACE_DATA=true in .env"
fi

echo ""
echo "=== All Snowflake scripts complete ==="
echo ""
echo "Next steps:"
echo "  1. Update terraform/demo.tfvars with Snowflake IAM ARN + External ID"
echo "     (from: snowsql -c <SF_CONNECTION> -q 'DESC INTEGRATION AWS_S3_CRYPTO;')"
echo "  2. cd terraform && terraform apply -var-file=demo.tfvars"
echo "  3. cp .env.example .env && edit .env (set SURV_USE_MARKETPLACE_DATA)"
echo "  4. pip install -r demo/requirements.txt"
echo "  5. python scripts/generate_synthetic_data.py --scenario all --trades 50000"
echo "  6. snowsql -c <SF_CONNECTION> -f sql/demo/demo_seed.sql   (step 8 onwards)"
echo "  7. Open Streamlit Investigator Copilot in Snowsight"
