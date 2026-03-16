# =============================================================================
# Digital Asset Market Surveillance — Demo Makefile
# =============================================================================
# Usage:
#   make setup       Full first-time Snowflake deployment (all SQL phases)
#   make seed        Generate synthetic data + force-refresh Dynamic Tables
#   make reset       Truncate tables and re-seed (between-demo reset, ~2 min)
#   make demo        seed + open Streamlit in browser
#   make tf-plan     Terraform plan (AWS infra)
#   make tf-apply    Terraform apply (AWS infra)
#   make lint        Check SQL/Python for obvious issues
#   make clean       Remove generated artefacts
# =============================================================================

SHELL        := /bin/bash
SF_CONN      ?= $(shell cat .env 2>/dev/null | grep SNOWSQL_CONNECTION | cut -d= -f2)
PYTHON       := python3
SNOW_CLI     := snowsql
TERRAFORM    := terraform

.PHONY: help setup seed reset demo tf-plan tf-apply lint clean check-env

# ─── Default target ──────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Digital Asset Market Surveillance — Demo Targets"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make setup       Deploy all Snowflake objects (run once per environment)"
	@echo "  make seed        Load synthetic data + refresh Dynamic Tables"
	@echo "  make reset       Fast demo reset: truncate → seed → refresh"
	@echo "  make demo        reset + launch Streamlit investigator app"
	@echo "  make tf-plan     Terraform plan for AWS infrastructure"
	@echo "  make tf-apply    Terraform apply for AWS infrastructure"
	@echo "  make lint        Basic syntax checks on SQL and Python"
	@echo "  make clean       Remove __pycache__, .pyc, terraform artefacts"
	@echo ""

# ─── Environment check ───────────────────────────────────────────────────────
check-env:
	@test -f .env || (echo "ERROR: .env file not found. Copy .env.example → .env and fill values." && exit 1)
	@$(PYTHON) -c "import snowflake.connector" 2>/dev/null || \
	    (echo "ERROR: snowflake-connector-python not installed. Run: pip install -r demo/requirements.txt" && exit 1)
	@echo "✓ Environment checks passed"

# ─── Snowflake setup (all phases in order) ───────────────────────────────────
setup: check-env
	@echo "==> Deploying Snowflake objects..."
	@bash snowflake/run_all.sh
	@echo "✓ Snowflake setup complete"

# ─── Seed synthetic data ─────────────────────────────────────────────────────
seed: check-env
	@echo "==> Generating synthetic data (pump_and_dump scenario)..."
	@$(PYTHON) scripts/generate_synthetic_data.py \
	    --scenario pump_and_dump \
	    --trades 3000 \
	    --entities 80 \
	    --ingest-method snowflake
	@echo "==> Force-refreshing Dynamic Tables..."
	@bash scripts/refresh_dynamic_tables.sh
	@echo "==> Running detection and case creation..."
	@$(SNOW_CLI) -q "CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_REFRESH_ALERTS();"
	@$(SNOW_CLI) -q "CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_AUTO_CREATE_CASES();"
	@echo "✓ Seed complete — data is live"

# ─── Fast demo reset (~2 min) ────────────────────────────────────────────────
reset: check-env
	@echo "==> Resetting demo data..."
	@$(SNOW_CLI) -f sql/demo/demo_seed.sql
	@$(PYTHON) scripts/generate_synthetic_data.py \
	    --scenario all \
	    --trades 3000 \
	    --entities 80 \
	    --ingest-method snowflake
	@bash scripts/refresh_dynamic_tables.sh
	@$(SNOW_CLI) -q "CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_REFRESH_ALERTS();"
	@$(SNOW_CLI) -q "CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_AUTO_CREATE_CASES();"
	@echo "✓ Reset complete"

# ─── Full demo (reset + Streamlit) ───────────────────────────────────────────
demo: reset
	@echo "==> Launching Investigator Copilot..."
	@echo "    (Ctrl+C to stop)"
	@streamlit run streamlit/investigator_app.py

# ─── Terraform ───────────────────────────────────────────────────────────────
tf-plan:
	@$(TERRAFORM) -chdir=terraform plan -var-file=demo.tfvars

tf-apply:
	@$(TERRAFORM) -chdir=terraform apply -var-file=demo.tfvars -auto-approve

# ─── Lint ────────────────────────────────────────────────────────────────────
lint:
	@echo "==> Python syntax check..."
	@find . -name "*.py" -not -path "*/\.*" -not -path "*/node_modules/*" | \
	    xargs $(PYTHON) -m py_compile && echo "  ✓ Python OK"
	@echo "==> Checking for leftover us-east-1 references..."
	@! grep -r "us-east-1" --include="*.tf" --include="*.py" --include="*.sql" . && \
	    echo "  ✓ Region OK" || echo "  WARN: Found us-east-1 references"
	@echo "✓ Lint complete"

# ─── Clean ───────────────────────────────────────────────────────────────────
clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	@find . -name "*.pyc" -delete 2>/dev/null; true
	@rm -f terraform/.terraform.lock.hcl terraform/terraform.tfstate terraform/terraform.tfstate.backup 2>/dev/null; true
	@echo "✓ Clean complete"
