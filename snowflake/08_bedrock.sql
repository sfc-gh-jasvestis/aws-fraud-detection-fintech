-- =============================================================================
-- Phase 7: Bedrock Investigator Copilot
-- File: 08_bedrock.sql
-- Implements SP_GENERATE_CASE_NARRATIVE using Snowpark External Access
-- to call Amazon Bedrock (Claude Sonnet 4) via the Converse API.
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
--
-- Key implementation notes:
--   1. Uses Converse API (/model/{modelId}/converse) NOT InvokeModel
--   2. Model ID must be an inference profile ID (e.g. us.anthropic.claude-sonnet-4-...)
--   3. SigV4 canonical path must URL-encode the model ID (: → %3A)
--      but the actual request URL must use the raw model ID
--      because the requests library will encode it again
--   4. Secret values are read via _snowflake.get_generic_secret_string()
--   5. PARSE_JSON is not supported in VALUES clause — use INSERT...SELECT
CREATE OR REPLACE PROCEDURE CRYPTO_SURVEILLANCE.ANALYTICS.SP_GENERATE_CASE_NARRATIVE(
    P_CASE_ID   STRING,
    P_MODEL_ID  STRING DEFAULT 'us.anthropic.claude-sonnet-4-20250514-v1:0'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'generate_narrative'
EXTERNAL_ACCESS_INTEGRATIONS = (BEDROCK_EXTERNAL_ACCESS)
SECRETS = ('bedrock_creds' = CRYPTO_SURVEILLANCE.ANALYTICS.BEDROCK_SECRET)
COMMENT = 'Calls Amazon Bedrock Converse API to generate SAR narrative and investigative summary for a case'
AS
$$
import hashlib
import hmac
import json
import requests
import _snowflake
from datetime import datetime, timezone
from snowflake.snowpark import Session
from urllib.parse import quote

BEDROCK_REGION = "us-west-2"
NL = chr(10)

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


def _sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret_key, date_stamp, region, service):
    k = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k = _sign(k, region)
    k = _sign(k, service)
    return _sign(k, "aws4_request")


def _aws_sigv4_headers(access_key, secret_key, region, service, method, host, canonical_path, payload):
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(payload).hexdigest()

    can_headers = "content-type:application/json" + NL + "host:" + host + NL + "x-amz-date:" + amz_date + NL
    signed_headers = "content-type;host;x-amz-date"
    can_req = NL.join([method, canonical_path, "", can_headers, signed_headers, payload_hash])

    cred_scope = date_stamp + "/" + region + "/" + service + "/aws4_request"
    sts = NL.join(["AWS4-HMAC-SHA256", amz_date, cred_scope, hashlib.sha256(can_req.encode("utf-8")).hexdigest()])

    signing_key = _signing_key(secret_key, date_stamp, region, service)
    sig = hmac.new(signing_key, sts.encode("utf-8"), hashlib.sha256).hexdigest()

    auth = "AWS4-HMAC-SHA256 Credential=" + access_key + "/" + cred_scope + ", SignedHeaders=" + signed_headers + ", Signature=" + sig
    return {"Content-Type": "application/json", "X-Amz-Date": amz_date, "Authorization": auth}


def _load_aws_creds():
    raw = _snowflake.get_generic_secret_string('bedrock_creds')
    creds = json.loads(raw)
    ak = creds.get("aws_access_key_id", "")
    sk = creds.get("aws_secret_access_key", "")
    if not ak or not sk:
        raise RuntimeError("AWS credentials missing from BEDROCK_SECRET.")
    return ak, sk


def fetch_case_context(session, case_id):
    case_row = session.sql(f"""
        SELECT c.case_id, c.case_ref, c.entity_id, c.state, c.priority,
               c.alert_types, c.peak_ml_probability, c.title,
               e.aml_risk_rating, e.pep_flag, e.sanctions_flag, e.kyc_tier, e.account_type
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES c
        JOIN CRYPTO_SURVEILLANCE.HARMONISED.ENTITY e ON c.entity_id = e.entity_id
        WHERE c.case_id = '{case_id}'
    """).collect()
    if not case_row:
        raise ValueError(f"Case not found: {case_id}")
    case = case_row[0].as_dict()

    ef_rows = session.sql(f"""
        SELECT cex_trade_count_30d, cex_volume_usd_30d, onchain_tx_count_30d,
               onchain_volume_30d, mixer_interactions_30d, sanctioned_volume_30d,
               has_burst_behaviour, wallet_count
        FROM CRYPTO_SURVEILLANCE.FEATURES.ENTITY_FEATURES
        WHERE entity_id = '{case["ENTITY_ID"]}' LIMIT 1
    """).collect()
    ef = ef_rows[0].as_dict() if ef_rows else {}

    alert_rows = session.sql(f"""
        SELECT alert_type, severity, reason, detected_at
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        WHERE case_id = '{case_id}'
        ORDER BY severity DESC, detected_at DESC LIMIT 10
    """).collect()
    return {"case": case, "entity_features": ef, "alerts": [r.as_dict() for r in alert_rows]}


def build_prompt(ctx):
    case = ctx["case"]
    ef = ctx.get("entity_features", {})
    alerts = ctx.get("alerts", [])
    lines = [f"  - [{a.get('SEVERITY','?')}] {a.get('ALERT_TYPE','?')}: {a.get('REASON','')}" for a in alerts]
    alert_details = NL.join(lines) if lines else "  - No specific alert details available"
    return SAR_PROMPT_TEMPLATE.format(
        case_ref=case.get("CASE_REF", "N/A"), entity_id=case.get("ENTITY_ID", "N/A"),
        priority=case.get("PRIORITY", "MEDIUM"), alert_types=str(case.get("ALERT_TYPES", [])),
        ml_score=float(case.get("PEAK_ML_PROBABILITY") or 0.0),
        aml_risk_rating=case.get("AML_RISK_RATING", "UNKNOWN"),
        pep_flag="YES" if case.get("PEP_FLAG") else "NO",
        sanctions_flag="YES" if case.get("SANCTIONS_FLAG") else "NO",
        cex_trade_count=int(ef.get("CEX_TRADE_COUNT_30D") or 0),
        cex_volume=float(ef.get("CEX_VOLUME_USD_30D") or 0.0),
        onchain_tx_count=int(ef.get("ONCHAIN_TX_COUNT_30D") or 0),
        onchain_volume=float(ef.get("ONCHAIN_VOLUME_30D") or 0.0),
        mixer_interactions=int(ef.get("MIXER_INTERACTIONS_30D") or 0),
        sanctioned_exposure="YES" if float(ef.get("SANCTIONED_VOLUME_30D") or 0) > 0 else "NO",
        burst_behaviour="YES" if ef.get("HAS_BURST_BEHAVIOUR") else "NO",
        wallet_count=int(ef.get("WALLET_COUNT") or 0),
        alert_details=alert_details,
    )


def call_bedrock(model_id, prompt):
    access_key, secret_key = _load_aws_creds()
    host = "bedrock-runtime." + BEDROCK_REGION + ".amazonaws.com"
    encoded_model = quote(model_id, safe="")
    raw_path = "/model/" + model_id + "/converse"
    canonical_path = "/model/" + encoded_model + "/converse"
    url = "https://" + host + raw_path

    body_dict = {
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "inferenceConfig": {"maxTokens": 2048, "temperature": 0.1}
    }
    payload = json.dumps(body_dict).encode("utf-8")
    headers = _aws_sigv4_headers(access_key, secret_key, BEDROCK_REGION, "bedrock", "POST", host, canonical_path, payload)
    resp = requests.post(url, headers=headers, data=payload, timeout=90)
    resp.raise_for_status()
    response_body = resp.json()
    text = response_body["output"]["message"]["content"][0]["text"]
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
    return json.loads(text)


def generate_narrative(snowpark_session, p_case_id, p_model_id="us.anthropic.claude-sonnet-4-20250514-v1:0"):
    try:
        ctx = fetch_case_context(snowpark_session, p_case_id)
        prompt = build_prompt(ctx)
        result = call_bedrock(p_model_id, prompt)
        narrative = result.get("sar_narrative", "")
        summary = result.get("case_summary", "")
        rec_action = result.get("recommended_action", "CONTINUE_MONITORING")
        safe_summary = summary.replace("'", "''")
        safe_narrative = narrative.replace("'", "''")
        snowpark_session.sql(f"""
            UPDATE CRYPTO_SURVEILLANCE.ANALYTICS.CASES
            SET summary = '{safe_summary}', sar_narrative = '{safe_narrative}', updated_at = CURRENT_TIMESTAMP()
            WHERE case_id = '{p_case_id}'
        """).collect()
        safe_event = json.dumps({"model": p_model_id, "recommended_action": rec_action}).replace("'", "''")
        snowpark_session.sql(f"""
            INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.CASE_EVENTS (case_id, event_type, event_data, performed_by)
            SELECT '{p_case_id}', 'BEDROCK_NARRATIVE_GENERATED', PARSE_JSON('{safe_event}'), 'BEDROCK_COPILOT'
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
