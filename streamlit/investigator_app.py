"""
Streamlit in Snowflake: Digital Asset Market Surveillance — Investigator Copilot
─────────────────────────────────────────────────────────────────────────────────
Deploy to Snowflake via:
  CREATE STREAMLIT CRYPTO_SURVEILLANCE.ANALYTICS.INVESTIGATOR_COPILOT
    ROOT_LOCATION = '@CRYPTO_SURVEILLANCE.ANALYTICS.STREAMLIT_STAGE/investigator_copilot/'
    MAIN_FILE     = 'investigator_app.py'
    QUERY_WAREHOUSE = WH_SURVEILLANCE;

Or paste directly into a Snowflake Notebook/Streamlit tile in Snowsight.
"""

import json
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(
    page_title="Market Surveillance Investigator",
    page_icon="🔍",
    layout="wide",
)

st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1a1f3a 0%, #29B5E8 100%);
        padding: 1.5rem 2rem;
        border-radius: 8px;
        color: white;
        margin-bottom: 1.5rem;
    }
    .severity-critical { color: #FF4B4B; font-weight: bold; }
    .severity-high     { color: #FF8C00; font-weight: bold; }
    .severity-medium   { color: #FFA500; }
    .severity-low      { color: #00C851; }
    .bedrock-output {
        background: #0d1117;
        color: #e6edf3;
        border-left: 3px solid #29B5E8;
        padding: 1rem;
        border-radius: 4px;
        font-size: 0.9rem;
        white-space: pre-wrap;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h2>🔍 Digital Asset Market Surveillance</h2>
    <p>Investigator Copilot | Powered by Snowflake + Amazon Bedrock</p>
</div>
""", unsafe_allow_html=True)


# ─── Helper functions ─────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def get_kri_metrics() -> dict:
    rows = session.sql("""
        SELECT
            COUNT_IF(detected_at >= DATEADD('day',-1,CURRENT_TIMESTAMP())) AS alerts_24h,
            COUNT_IF(severity = 'CRITICAL'
                     AND detected_at >= DATEADD('day',-1,CURRENT_TIMESTAMP())) AS critical_24h,
            COUNT_IF(status = 'OPEN')  AS open_alerts,
            COUNT_IF(status = 'IN_REVIEW') AS in_review_alerts
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
    """).collect()
    case_rows = session.sql("""
        SELECT
            COUNT_IF(state = 'OPEN') AS open_cases,
            COUNT_IF(sla_breached = TRUE AND state NOT IN ('CLOSED','FALSE_POSITIVE'))
                AS sla_breached,
            COUNT_IF(state = 'ESCALATED') AS escalated
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES
    """).collect()
    return {**(rows[0].as_dict() if rows else {}),
            **(case_rows[0].as_dict() if case_rows else {})}


@st.cache_data(ttl=30)
def get_open_cases(priority_filter: str, state_filter: str) -> pd.DataFrame:
    filters = []
    if priority_filter != "All":
        filters.append(f"c.priority = '{priority_filter}'")
    if state_filter != "All":
        filters.append(f"c.state = '{state_filter}'")
    where = "WHERE " + " AND ".join(filters) if filters else ""

    return session.sql(f"""
        SELECT
            c.case_id,
            c.case_ref,
            c.entity_id,
            c.state,
            c.priority,
            c.title,
            c.peak_ml_probability,
            c.composite_severity,
            c.sla_breached,
            c.assigned_to,
            c.created_at,
            c.due_by,
            e.aml_risk_rating,
            e.kyc_tier,
            e.pep_flag,
            e.sanctions_flag
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES c
        LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.ENTITY e ON c.entity_id = e.entity_id
        {where}
        ORDER BY
            CASE c.priority
                WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM'   THEN 3 ELSE 4
            END,
            c.created_at DESC
        LIMIT 200
    """).to_pandas()


@st.cache_data(ttl=30)
def get_case_detail(case_id: str) -> dict:
    case = session.sql(f"""
        SELECT c.*, e.full_name, e.email, e.aml_risk_rating,
               e.kyc_tier, e.pep_flag, e.sanctions_flag, e.account_type,
               e.onboarded_at
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES c
        LEFT JOIN CRYPTO_SURVEILLANCE.HARMONISED.ENTITY e ON c.entity_id = e.entity_id
        WHERE c.case_id = '{case_id}'
    """).collect()

    alerts = session.sql(f"""
        SELECT alert_type, severity, reason, detected_at, status, ml_fraud_probability
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        WHERE case_id = '{case_id}'
        ORDER BY severity, detected_at DESC
    """).to_pandas()

    trades = session.sql(f"""
        SELECT t.trade_id, t.trading_pair, t.side, t.price, t.quantity,
               t.quote_qty, t.venue, t.trade_ts
        FROM CRYPTO_SURVEILLANCE.HARMONISED.TRADES t
        JOIN CRYPTO_SURVEILLANCE.ANALYTICS.CASES c ON t.account_id = c.entity_id
        WHERE c.case_id = '{case_id}'
          AND t.trade_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        ORDER BY t.trade_ts DESC
        LIMIT 100
    """).to_pandas()

    onchain = session.sql(f"""
        SELECT ot.tx_hash, ot.chain, ot.event_type, ot.value_decimal,
               ot.token_symbol, ot.from_address, ot.to_address, ot.block_ts
        FROM CRYPTO_SURVEILLANCE.HARMONISED.ONCHAIN_TRANSFERS ot
        JOIN CRYPTO_SURVEILLANCE.HARMONISED.WALLET w
            ON (ot.from_address = w.wallet_address OR ot.to_address = w.wallet_address)
            AND ot.chain = w.chain
        JOIN CRYPTO_SURVEILLANCE.ANALYTICS.CASES c ON w.owner_entity_id = c.entity_id
        WHERE c.case_id = '{case_id}'
          AND ot.block_ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        ORDER BY ot.block_ts DESC
        LIMIT 50
    """).to_pandas()

    events = session.sql(f"""
        SELECT event_type, event_data, performed_by, event_ts
        FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASE_EVENTS
        WHERE case_id = '{case_id}'
        ORDER BY event_ts DESC
        LIMIT 20
    """).to_pandas()

    return {
        "case":    case[0].as_dict() if case else {},
        "alerts":  alerts,
        "trades":  trades,
        "onchain": onchain,
        "events":  events,
    }


@st.cache_data(ttl=600, show_spinner=False)
def call_bedrock_copilot(case_id: str) -> dict:
    result = session.call(
        "CRYPTO_SURVEILLANCE.ANALYTICS.SP_GENERATE_CASE_NARRATIVE",
        case_id,
        "us.anthropic.claude-sonnet-4-20250514-v1:0"
    )
    if isinstance(result, str):
        return json.loads(result)
    return result or {}


def prewarm_bedrock_for_top_case() -> None:
    rows = session.sql("""
        SELECT case_id FROM CRYPTO_SURVEILLANCE.ANALYTICS.CASES
        WHERE state = 'OPEN'
          AND sar_narrative IS NULL
        ORDER BY CASE priority WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
                               WHEN 'MEDIUM' THEN 3 ELSE 4 END,
                 created_at ASC
        LIMIT 1
    """).collect()
    if rows:
        call_bedrock_copilot(rows[0]["CASE_ID"])


def update_case_state(case_id: str, new_state: str, analyst: str) -> None:
    session.sql(f"""
        UPDATE CRYPTO_SURVEILLANCE.ANALYTICS.CASES
        SET state = '{new_state}', assigned_to = '{analyst}',
            updated_at = CURRENT_TIMESTAMP()
        WHERE case_id = '{case_id}'
    """).collect()
    session.sql(f"""
        UPDATE CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
        SET status = '{new_state}', updated_at = CURRENT_TIMESTAMP()
        WHERE case_id = '{case_id}'
    """).collect()
    session.sql(f"""
        INSERT INTO CRYPTO_SURVEILLANCE.ANALYTICS.CASE_EVENTS
            (case_id, event_type, event_data, performed_by)
        SELECT '{case_id}', 'STATE_CHANGE',
               PARSE_JSON('{json.dumps({"new_state": new_state})}'),
               '{analyst}'
    """).collect()
    st.cache_data.clear()


# ─── Sidebar: filters + case picker ──────────────────────────────────────────

df_cases = pd.DataFrame()

with st.sidebar:
    st.header("Filters")
    priority_filter = st.selectbox("Priority", ["All", "CRITICAL", "HIGH", "MEDIUM", "LOW"])
    state_filter = st.selectbox("State", ["All", "OPEN", "IN_REVIEW", "ESCALATED", "CLOSED"])

    df_cases = get_open_cases(priority_filter, state_filter)

    st.divider()
    st.header("Case Picker")

    if df_cases.empty:
        st.caption("No cases match filters.")
        case_options = []
    else:
        case_options = []
        for _, row in df_cases.iterrows():
            label = f"{row['PRIORITY'][0]}  {row['CASE_REF']}  —  {row['TITLE'][:40]}"
            case_options.append(label)

    def _on_case_pick():
        idx = st.session_state.get("_case_pick_idx")
        if idx is not None and idx > 0:
            st.session_state["selected_case_id"] = df_cases.iloc[idx - 1]["CASE_ID"]

    selected_idx = st.selectbox(
        "Select case",
        range(len(case_options) + 1),
        format_func=lambda i: "— pick a case —" if i == 0 else case_options[i - 1],
        key="_case_pick_idx",
        on_change=_on_case_pick,
        label_visibility="collapsed",
    )

    st.divider()
    analyst_name = st.text_input("Your Name", value="Analyst", key="analyst_name")
    st.divider()
    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("Demo Controls")
    if st.button("Pre-warm Bedrock", use_container_width=True,
                 help="Generate narrative for the top case now so the demo button is instant"):
        with st.spinner("Pre-warming Bedrock (runs once, cached 10 min)..."):
            try:
                prewarm_bedrock_for_top_case()
                st.success("Ready — Bedrock response cached")
            except Exception as exc:
                st.warning(f"Pre-warm skipped: {exc}")


# ─── KRI dashboard row ───────────────────────────────────────────────────────
st.subheader("Key Risk Indicators")
kri = get_kri_metrics()

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Alerts (24h)", kri.get("ALERTS_24H", 0))
col2.metric("Critical (24h)", kri.get("CRITICAL_24H", 0), delta=None)
col3.metric("Open Alerts", kri.get("OPEN_ALERTS", 0))
col4.metric("Open Cases", kri.get("OPEN_CASES", 0))
col5.metric("SLA Breached", kri.get("SLA_BREACHED", 0), delta=None)
col6.metric("Escalated", kri.get("ESCALATED", 0), delta=None)

st.divider()

# ─── Main content ─────────────────────────────────────────────────────────────

case_id = st.session_state.get("selected_case_id")

if not case_id:
    st.subheader("Case Queue")
    if df_cases.empty:
        st.info("No cases match the current filters.")
    else:
        st.caption(f"{len(df_cases)} cases — pick one from the sidebar to investigate")

        def style_priority(val: str) -> str:
            return {
                "CRITICAL": "color: #FF4B4B; font-weight: bold;",
                "HIGH":     "color: #FF8C00; font-weight: bold;",
                "MEDIUM":   "color: #FFA500;",
                "LOW":      "color: #00C851;",
            }.get(val, "")

        styled = df_cases[["CASE_REF", "ENTITY_ID", "PRIORITY", "STATE", "TITLE",
                           "PEAK_ML_PROBABILITY", "AML_RISK_RATING", "SLA_BREACHED",
                           "CREATED_AT"]].style.map(
            style_priority, subset=["PRIORITY"]
        )
        st.dataframe(styled, use_container_width=True, height=500)

    with st.expander("Alert Analytics (30d)"):
        @st.cache_data(ttl=120)
        def get_alert_analytics() -> tuple:
            by_type = session.sql("""
                SELECT alert_type, severity, COUNT(*) AS cnt
                FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                WHERE detected_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
                GROUP BY 1, 2 ORDER BY cnt DESC
            """).to_pandas()
            by_day = session.sql("""
                SELECT DATE_TRUNC('DAY', detected_at)::DATE AS alert_date,
                       severity, COUNT(*) AS cnt
                FROM CRYPTO_SURVEILLANCE.ANALYTICS.ALERTS
                WHERE detected_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
                GROUP BY 1, 2 ORDER BY 1
            """).to_pandas()
            return by_type, by_day

        by_type_df, by_day_df = get_alert_analytics()
        a_col1, a_col2 = st.columns(2)
        with a_col1:
            st.markdown("**Alerts by Type**")
            if not by_type_df.empty:
                pivot = by_type_df.pivot_table(
                    index="ALERT_TYPE", columns="SEVERITY", values="CNT", fill_value=0
                )
                sev_colors = {"CRITICAL": "#FF4B4B", "HIGH": "#FF8C00", "MEDIUM": "#FFA500", "LOW": "#00C851"}
                st.bar_chart(pivot, color=[sev_colors.get(c, "#888888") for c in pivot.columns])
        with a_col2:
            st.markdown("**Daily Alert Volume**")
            if not by_day_df.empty:
                pivot_day = by_day_df.pivot_table(
                    index="ALERT_DATE", columns="SEVERITY", values="CNT", fill_value=0
                )
                sev_colors_day = {"CRITICAL": "#FF4B4B", "HIGH": "#FF8C00", "MEDIUM": "#FFA500", "LOW": "#00C851"}
                st.area_chart(pivot_day, color=[sev_colors_day.get(c, "#888888") for c in pivot_day.columns])

else:
    detail = get_case_detail(case_id)
    c = detail["case"]

    if not c:
        st.warning("Case not found. Pick another from the sidebar.")
    else:
        sev_class = {
            "CRITICAL": "severity-critical",
            "HIGH":     "severity-high",
            "MEDIUM":   "severity-medium",
        }.get(c.get("PRIORITY", ""), "severity-low")

        st.markdown(f"""
        ### {c.get('CASE_REF','N/A')} — {c.get('TITLE','Untitled')}
        **Entity:** `{c.get('ENTITY_ID')}` | **State:** `{c.get('STATE')}` |
        **Priority:** <span class="{sev_class}">{c.get('PRIORITY')}</span> |
        **ML Score:** {float(c.get('PEAK_ML_PROBABILITY') or 0):.1%}
        """, unsafe_allow_html=True)

        col_a, col_b, col_c, col_d = st.columns(4)
        with col_a:
            if st.button("Start Review", use_container_width=True):
                update_case_state(case_id, "IN_REVIEW", analyst_name)
                st.rerun()
        with col_b:
            if st.button("Escalate", use_container_width=True):
                update_case_state(case_id, "ESCALATED", analyst_name)
                st.rerun()
        with col_c:
            if st.button("Close — TP", use_container_width=True):
                update_case_state(case_id, "CLOSED", analyst_name)
                st.rerun()
        with col_d:
            if st.button("False Positive", use_container_width=True):
                update_case_state(case_id, "FALSE_POSITIVE", analyst_name)
                st.rerun()

        st.divider()

        with st.expander("Entity Profile", expanded=True):
            e_col1, e_col2, e_col3 = st.columns(3)
            e_col1.markdown(f"**KYC Tier:** `{c.get('KYC_TIER','?')}`")
            e_col1.markdown(f"**AML Rating:** `{c.get('AML_RISK_RATING','?')}`")
            e_col2.markdown(f"**PEP:** {'YES' if c.get('PEP_FLAG') else 'NO'}")
            e_col2.markdown(f"**Sanctions:** {'YES' if c.get('SANCTIONS_FLAG') else 'NO'}")
            e_col3.markdown(f"**Account Type:** `{c.get('ACCOUNT_TYPE','?')}`")
            e_col3.markdown(f"**Onboarded:** `{c.get('ONBOARDED_AT','?')}`")

        with st.expander(f"Alerts ({len(detail['alerts'])})", expanded=True):
            if not detail["alerts"].empty:
                st.dataframe(detail["alerts"], use_container_width=True)
            else:
                st.caption("No alerts linked to this case.")

        col_t, col_oc = st.columns(2)
        with col_t:
            with st.expander(f"CEX Trades (7d, {len(detail['trades'])} rows)"):
                if not detail["trades"].empty:
                    st.dataframe(detail["trades"], use_container_width=True, height=300)
                    if "TRADE_TS" in detail["trades"].columns:
                        chart_data = (
                            detail["trades"]
                            .assign(trade_date=pd.to_datetime(detail["trades"]["TRADE_TS"]).dt.date)
                            .groupby("trade_date")["QUOTE_QTY"].sum()
                            .reset_index()
                        )
                        st.line_chart(chart_data.set_index("trade_date")["QUOTE_QTY"],
                                      color="#29B5E8")

        with col_oc:
            with st.expander(f"On-Chain Activity (30d, {len(detail['onchain'])} rows)"):
                if not detail["onchain"].empty:
                    st.dataframe(detail["onchain"], use_container_width=True, height=300)

        st.divider()
        st.subheader("Bedrock Investigator Copilot")

        if c.get("SAR_NARRATIVE"):
            st.success("Narrative already generated. Re-generate to refresh.")
            st.markdown(f'<div class="bedrock-output">{c["SAR_NARRATIVE"]}</div>',
                        unsafe_allow_html=True)

        if st.button("Generate Case Narrative & SAR Draft (Bedrock)", type="primary",
                     use_container_width=True):
            with st.spinner("Calling Amazon Bedrock (Claude)..."):
                try:
                    result = call_bedrock_copilot(case_id)

                    if "error" in result:
                        st.error(f"Bedrock error: {result['error']}")
                    else:
                        st.success("Narrative generated!")

                        n_col1, n_col2 = st.columns(2)
                        with n_col1:
                            st.markdown("**Case Summary**")
                            st.markdown(
                                f'<div class="bedrock-output">{result.get("case_summary","")}</div>',
                                unsafe_allow_html=True
                            )
                            st.markdown("**Risk Indicators**")
                            for ri in result.get("risk_indicators", []):
                                st.markdown(f"- {ri}")

                        with n_col2:
                            st.markdown("**Recommended Action**")
                            rec = result.get("recommended_action", "")
                            colour = "🔴" if "SAR" in rec or "FREEZE" in rec else "🟡"
                            st.markdown(f"{colour} **{rec}**")

                            st.markdown("**Next Steps**")
                            for i, step in enumerate(result.get("next_steps", []), 1):
                                st.markdown(f"{i}. {step}")

                        with st.expander("Full SAR Narrative"):
                            st.markdown(
                                f'<div class="bedrock-output">{result.get("sar_narrative","")}</div>',
                                unsafe_allow_html=True
                            )
                        st.cache_data.clear()

                except Exception as exc:
                    st.error(f"Error calling copilot: {exc}")

        with st.expander("Case Audit Trail"):
            if not detail["events"].empty:
                st.dataframe(detail["events"], use_container_width=True)
