-- =============================================================================
-- Phase 7: Bedrock Investigator Copilot
-- File: 10_bedrock_integration.sql
-- Implements SP_GENERATE_CASE_NARRATIVE using Snowpark External Access
-- to call Amazon Bedrock (Claude) for SAR narrative generation.
-- Pre-requisites: 01_integrations.sql (BEDROCK_EXTERNAL_ACCESS integration)
-- =============================================================================

USE ROLE SURVEILLANCE_ADMIN;
USE DATABASE CRYPTO_SURVEILLANCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE WH_SURVEILLANCE;

-- ─── Stored Procedure: Generate Case Narrative via Bedrock ────────────────────
-- Pre-requisite: update BEDROCK_SECRET with real AWS credentials (JSON):
--   ALTER SECRET CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET
--     SET SECRET_STRING = '{"aws_access_key_id":"AKIA...","aws_secret_access_key":"..."}';
-- The IAM user/role must have bedrock:InvokeModel on the target model ARN.
-- Snowpark External Access does NOT support boto3 credential chains (no instance
-- metadata, no IAM role assumption). Credentials must be injected via SECRETS.
-- This handler reads the secret JSON and uses requests + manual SigV4 signing.
CREATE OR REPLACE PROCEDURE CRYPTO_SURVEILLANCE.ANALYTICS.SP_GENERATE_CASE_NARRATIVE(
    P_CASE_ID   STRING,
    P_MODEL_ID  STRING DEFAULT 'anthropic.claude-3-5-sonnet-20241022-v2:0'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'generate_narrative'
EXTERNAL_ACCESS_INTEGRATIONS = (BEDROCK_EXTERNAL_ACCESS)
SECRETS = ('bedrock_creds' = CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET)
COMMENT = 'Calls Amazon Bedrock to generate SAR narrative and investigative summary for a case'
AS
$$
import hashlib
import hmac
import json
import os
import requests
from datetime import datetime, timezone
from snowflake.snowpark import Session

BEDROCK_REGION   = "us-west-2"
BEDROCK_ENDPOINT = f"https://bedrock-runtime.{BEDROCK_REGION}.amazonaws.com"

SAR_PROMPT_TEMPLATE = """You are a senior financial crime investigator at a cryptocurrency exchange.
Review the following investigation case and generate:
1. A concise case summary (3-5 sentences) suitable for internal reporting
2. A formal SAR (Suspicious Activity Report) narrative in FinCEN format
3. 3-5 specific investigative next steps the analyst should take

Case Reference: {case_ref}
Entity ID: {entity_id}
Case Priority: {priority}
Alert Types Detected: {alert_types}
ML Fraud Probability: {ml_score:.1%}
AML Risk Rating: {aml_risk_rating}
PEP Flag: {pep_flag}
Sanctions Flag: {sanctions_flag}

Trading Activity (30 days):
- CEX Trades: {cex_trade_count} trades | Volume: ${cex_volume:,.0f}
- On-chain Transactions: {onchain_tx_count} | Volume: {onchain_volume:.4f} ETH equiv.
- Mixer Interactions: {mixer_interactions}
- Sanctioned Counterparty Exposure: {sanctioned_exposure}
- Burst Trade Behaviour: {burst_behaviour}
- Wallet Count: {wallet_count}

Recent Alerts:
{alert_details}

Respond in JSON format:
{{
  "case_summary": "...",
  "sar_narrative": "...",
  "next_steps": ["step1", "step2", "step3"],
  "risk_indicators": ["indicator1", "indicator2"],
  "recommended_action": "CLOSE_NO_ACTION | CONTINUE_MONITORING | ESCALATE_TO_COMPLIANCE | FILE_SAR | FREEZE_ACCOUNT"
}}"""


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date    = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region  = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    return k_signing


def _aws_sigv4_headers(
    access_key: str, secret_key: str, region: str,
    service: str, method: str, host: str, path: str, payload: bytes
) -> dict:
    """Minimal AWS SigV4 request signing for HTTPS POST."""
    now          = datetime.now(timezone.utc)
    amz_date     = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp   = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(payload).hexdigest()

    canonical_headers  = f"content-type:application/json\nhost:{host}\nx-amz-date:{amz_date}\n"
    signed_headers     = "content-type;host;x-amz-date"
    canonical_request  = "\n".join([
        method, path, "",
        canonical_headers, signed_headers, payload_hash
    ])

    credential_scope   = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign     = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    ])

    signing_key = _signing_key(secret_key, date_stamp, region, service)
    signature   = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return {
        "Content-Type":  "application/json",
        "X-Amz-Date":    amz_date,
        "Authorization": auth_header,
    }


def _load_aws_creds() -> tuple[str, str]:
    """Extract AWS credentials from the Snowflake secret (injected as env var)."""
    raw = os.environ.get("bedrock_creds", "{}")
    creds = json.loads(raw)
    access_key = creds.get("aws_access_key_id", "")
    secret_key = creds.get("aws_secret_access_key", "")
    if not access_key or not secret_key:
        raise RuntimeError(
            "AWS credentials missing from BEDROCK_SECRET. "
            "Update the secret with {\"aws_access_key_id\": \"...\", \"aws_secret_access_key\": \"...\"}"
        )
    return access_key, secret_key


def fetch_case_context(snowpark_session: Session, case_id: str) -> dict:
    """Pull all context needed for the Bedrock prompt from Snowflake."""
    case_row = snowpark_session.sql(f"""
        SELECT
            c.case_id, c.case_ref, c.entity_id, c.state, c.priority,
            c.alert_types, c.peak_ml_probability, c.title,
            e.aml_risk_rating, e.pep_flag, e.sanctions_flag,
            e.kyc_tier, e.account_type
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES c
        JOIN CRYPTO_SURVEILLANCE.HARMONISED.ENTITY e ON c.entity_id = e.entity_id
        WHERE c.case_id = '{case_id}'
    """).collect()

    if not case_row:
        raise ValueError(f"Case not found: {case_id}")
    case = case_row[0].as_dict()

    # Entity features
    ef_rows = snowpark_session.sql(f"""
        SELECT
            cex_trade_count_30d, cex_volume_usd_30d, onchain_tx_count_30d,
            onchain_volume_30d, mixer_interactions_30d, sanctioned_volume_30d,
            has_burst_behaviour, wallet_count
        FROM CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES
        WHERE entity_id = '{case["ENTITY_ID"]}'
        LIMIT 1
    """).collect()
    ef = ef_rows[0].as_dict() if ef_rows else {}

    # Recent alerts
    alert_rows = snowpark_session.sql(f"""
        SELECT alert_type, severity, reason, detected_at
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        WHERE case_id = '{case_id}'
        ORDER BY severity DESC, detected_at DESC
        LIMIT 10
    """).collect()
    alerts = [r.as_dict() for r in alert_rows]

    return {"case": case, "entity_features": ef, "alerts": alerts}


def build_prompt(ctx: dict) -> str:
    case = ctx["case"]
    ef   = ctx.get("entity_features", {})
    alerts = ctx.get("alerts", [])

    alert_details = "\n".join([
        f"  - [{a.get('SEVERITY','?')}] {a.get('ALERT_TYPE','?')}: {a.get('REASON','')}"
        for a in alerts
    ]) or "  - No specific alert details available"

    return SAR_PROMPT_TEMPLATE.format(
        case_ref          = case.get("CASE_REF", "N/A"),
        entity_id         = case.get("ENTITY_ID", "N/A"),
        priority          = case.get("PRIORITY", "MEDIUM"),
        alert_types       = str(case.get("ALERT_TYPES", [])),
        ml_score          = float(case.get("PEAK_ML_PROBABILITY") or 0.0),
        aml_risk_rating   = case.get("AML_RISK_RATING", "UNKNOWN"),
        pep_flag          = "YES" if case.get("PEP_FLAG") else "NO",
        sanctions_flag    = "YES" if case.get("SANCTIONS_FLAG") else "NO",
        cex_trade_count   = int(ef.get("CEX_TRADE_COUNT_30D") or 0),
        cex_volume        = float(ef.get("CEX_VOLUME_USD_30D") or 0.0),
        onchain_tx_count  = int(ef.get("ONCHAIN_TX_COUNT_30D") or 0),
        onchain_volume    = float(ef.get("ONCHAIN_VOLUME_30D") or 0.0),
        mixer_interactions= int(ef.get("MIXER_INTERACTIONS_30D") or 0),
        sanctioned_exposure = "YES" if float(ef.get("SANCTIONED_VOLUME_30D") or 0) > 0 else "NO",
        burst_behaviour   = "YES" if ef.get("HAS_BURST_BEHAVIOUR") else "NO",
        wallet_count      = int(ef.get("WALLET_COUNT") or 0),
        alert_details     = alert_details,
    )


def call_bedrock(model_id: str, prompt: str) -> dict:
    """Invoke Bedrock Claude via signed HTTPS POST (no boto3 — creds via SECRETS)."""
    access_key, secret_key = _load_aws_creds()
    host    = f"bedrock-runtime.{BEDROCK_REGION}.amazonaws.com"
    path    = f"/model/{model_id}/invoke"
    url     = f"https://{host}{path}"

    body_dict = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
    }
    payload = json.dumps(body_dict).encode("utf-8")

    headers = _aws_sigv4_headers(
        access_key, secret_key, BEDROCK_REGION,
        "bedrock", "POST", host, path, payload
    )

    resp = requests.post(url, headers=headers, data=payload, timeout=60)
    resp.raise_for_status()
    response_body = resp.json()
    text = response_body["content"][0]["text"]

    # Claude sometimes wraps JSON in markdown fences
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()

    return json.loads(text)


def generate_narrative(snowpark_session: Session, p_case_id: str,
                        p_model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0") -> dict:
    try:
        ctx    = fetch_case_context(snowpark_session, p_case_id)
        prompt = build_prompt(ctx)
        result = call_bedrock(p_model_id, prompt)

        # Persist narrative back to CASES table
        narrative   = result.get("sar_narrative", "")
        summary     = result.get("case_summary", "")
        rec_action  = result.get("recommended_action", "CONTINUE_MONITORING")

        snowpark_session.sql(f"""
            UPDATE CRYPTO_SURVEILLANCE.ANALYTICS.CASES
            SET
                summary       = $${summary}$$,
                sar_narrative = $${narrative}$$,
                updated_at    = CURRENT_TIMESTAMP()
            WHERE case_id = '{p_case_id}'
        """).collect()

        # Log as case event
        snowpark_session.sql(f"""
            INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.CASE_EVENTS
                (case_id, event_type, event_data, performed_by)
            VALUES (
                '{p_case_id}',
                'BEDROCK_NARRATIVE_GENERATED',
                PARSE_JSON('{json.dumps({"model": p_model_id, "recommended_action": rec_action})}'),
                'BEDROCK_COPILOT'
            )
        """).collect()

        return result

    except Exception as exc:
        return {"error": str(exc), "case_id": p_case_id}
$$;

GRANT USAGE ON PROCEDURE CRYPTO_SURVEILLANCE.ANALYTICS.SP_GENERATE_CASE_NARRATIVE(STRING, STRING)
    TO ROLE SURVEILLANCE_ANALYST;
GRANT USAGE ON PROCEDURE CRYPTO_SURVEILLANCE.ANALYTICS.SP_GENERATE_CASE_NARRATIVE(STRING, STRING)
    TO ROLE SURVEILLANCE_ADMIN;

-- ─── Quick test (replace with a real case_id after demo data load) ───────────
-- CALL CRYPTO_SURVEILLANCE.ANALYTICS.SP_GENERATE_CASE_NARRATIVE('REPLACE_WITH_CASE_ID');
