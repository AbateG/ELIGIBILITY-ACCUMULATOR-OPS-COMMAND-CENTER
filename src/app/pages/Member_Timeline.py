# ═══════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════

import sys
import os
# Add project root to path for module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import streamlit as st
import altair as alt
import sqlite3
from pathlib import Path
from datetime import datetime

from src.common.db import fetch_all
from config.settings import DB_PATH
from src.app.utils import to_dataframe, add_age_hours_column


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Member Timeline",
    page_icon="👤",
    layout="wide",
)


# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════

OPEN_CASE_STATUSES = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"}


# ═══════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def safe_val(value, fallback=0):
    """Return a numeric-safe value. None and NaN become *fallback*."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    return value


def safe_text(value, fallback="—"):
    """Return a display-safe string for UI labels."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    if str(value).strip() == "":
        return fallback
    return str(value)


def fmt_number(value, fallback="—"):
    """Format a number with commas for display."""
    v = safe_val(value, None)
    if v is None:
        return fallback
    try:
        num = float(v)
        return f"{int(num):,}" if num == int(num) else f"{num:,.2f}"
    except (TypeError, ValueError):
        return fallback


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner="Loading member data …")
def load_member_options():
    """Load available member IDs for selection."""
    rows = fetch_all("""
        SELECT DISTINCT member_id
        FROM eligibility_periods
        WHERE member_id IS NOT NULL
        ORDER BY member_id
    """)
    return [row["member_id"] for row in rows]


@st.cache_data(ttl=300, show_spinner="Loading eligibility periods …")
def load_eligibility(member_id):
    """Load eligibility periods for a member."""
    rows = fetch_all("""
        SELECT
            ep.eligibility_id, ep.member_id, ep.plan_id,
            p.plan_code, p.plan_name,
            ep.coverage_start, ep.coverage_end,
            ep.status, ep.created_at
        FROM eligibility_periods ep
        LEFT JOIN benefit_plans p ON ep.plan_id = p.plan_id
        WHERE ep.member_id = ?
        ORDER BY ep.coverage_start DESC
    """, (member_id,))
    df = to_dataframe(rows)
    return df


@st.cache_data(ttl=300, show_spinner="Loading claims …")
def load_claims(member_id):
    """Load claims for a member."""
    rows = fetch_all("""
        SELECT
            c.claim_record_id, c.claim_id, c.line_id,
            c.member_id, c.subscriber_id,
            c.service_date, c.paid_date,
            c.allowed_amount, c.paid_amount, c.member_responsibility,
            c.deductible_amount, c.coinsurance_amount, c.copay_amount,
            c.claim_status, c.reversal_flag,
            c.created_at, c.source_file_id AS file_id
        FROM claims c
        WHERE c.member_id = ?
        ORDER BY c.service_date DESC
    """, (member_id,))
    df = to_dataframe(rows)
    return df


@st.cache_data(ttl=300, show_spinner="Loading accumulator transactions …")
def load_accumulator_transactions(member_id):
    """Load accumulator transactions for a member."""
    rows = fetch_all("""
        SELECT
            at.accumulator_txn_id, at.member_id, at.family_id,
            at.plan_id, at.claim_record_id,
            at.benefit_year, at.accumulator_type,
            at.delta_amount, at.service_date, at.source_type,
            at.source_file_id, at.created_at
        FROM accumulator_transactions at
        WHERE at.member_id = ?
        ORDER BY at.service_date DESC, at.created_at DESC
    """, (member_id,))
    df = to_dataframe(rows)
    return df


@st.cache_data(ttl=300, show_spinner="Loading accumulator snapshots …")
def load_snapshots(member_id):
    """Load accumulator snapshots for a member."""
    rows = fetch_all("""
        SELECT
            s.snapshot_id, s.member_id, s.family_id, s.plan_id,
            s.individual_deductible_accum, s.family_deductible_accum,
            s.individual_oop_accum, s.family_oop_accum,
            s.snapshot_ts, s.benefit_year
        FROM accumulator_snapshots s
        WHERE s.member_id = ?
        ORDER BY s.snapshot_ts DESC
    """, (member_id,))
    df = to_dataframe(rows)
    return df


@st.cache_data(ttl=300, show_spinner="Loading anomalies …")
def load_anomalies(member_id):
    """
    Load anomalies for a member from data_quality_issues
    (accumulator_anomalies table is not in this schema;
    equivalent data lives in data_quality_issues with issue_type = 'ACCUMULATOR').
    """
    rows = fetch_all("""
        SELECT
            i.issue_id       AS anomaly_id,
            i.member_id,
            i.issue_code     AS anomaly_type,
            i.severity,
            i.issue_description AS description,
            i.detected_at,
            NULL             AS resolved_at
        FROM data_quality_issues i
        WHERE i.member_id = ?
          AND i.issue_type = 'ACCUMULATOR'
        ORDER BY i.detected_at DESC
    """, (member_id,))
    return to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner="Loading issues …")
def load_issues_for_member(member_id):
    """Load issues for a member."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status, i.issue_description,
            i.detected_at, i.file_id, i.run_id
        FROM data_quality_issues i
        WHERE i.member_id = ?
        ORDER BY i.detected_at DESC
    """, (member_id,))
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "detected_at", "issue_age_hours")
    return df


@st.cache_data(ttl=300, show_spinner="Loading support cases …")
def load_cases_for_member(member_id):
    """Load support cases for a member."""
    rows = fetch_all("""
        SELECT
            sc.case_id, sc.case_number, sc.case_type, sc.priority,
            sc.severity, sc.status, sc.short_description,
            sc.opened_at, sc.resolved_at,
            st.is_at_risk, st.is_breached
        FROM support_cases sc
        LEFT JOIN sla_tracking st ON sc.case_id = st.case_id
        WHERE sc.member_id = ?
        ORDER BY sc.opened_at DESC
    """, (member_id,))
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "opened_at", "case_age_hours")
    return df


@st.cache_data(ttl=300, show_spinner="Loading SLA exposure …")
def load_sla_for_member(member_id):
    """Load SLA records for a member's cases."""
    rows = fetch_all("""
        SELECT
            st.sla_id, st.case_id, sc.case_number, st.sla_type,
            st.target_hours, st.target_due_at, st.status,
            st.is_at_risk, st.is_breached, st.breached_at
        FROM sla_tracking st
        JOIN support_cases sc ON st.case_id = sc.case_id
        WHERE sc.member_id = ?
        ORDER BY st.target_due_at DESC
    """, (member_id,))
    df = to_dataframe(rows)
    return df


@st.cache_data(ttl=300, show_spinner="Loading processing runs …")
def load_runs_for_member(member_id):
    """Load processing runs that involved the member's data via claim source files."""
    rows = fetch_all("""
        SELECT DISTINCT
            pr.run_id, pr.run_type, pr.file_id, f.file_name,
            pr.started_at, pr.completed_at, pr.run_status,
            pr.rows_read, pr.rows_passed, pr.rows_failed
        FROM processing_runs pr
        JOIN inbound_files f ON pr.file_id = f.file_id
        WHERE pr.file_id IN (
            SELECT source_file_id FROM claims WHERE member_id = ? AND source_file_id IS NOT NULL
        )
        OR pr.run_id IN (
            SELECT run_id FROM data_quality_issues WHERE member_id = ? AND run_id IS NOT NULL
        )
        ORDER BY pr.started_at DESC
    """, (member_id, member_id))
    df = to_dataframe(rows)
    return df


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("👤 Member Timeline & Root Cause Investigation")
st.caption(
    "Comprehensive view of a member's eligibility, claims, accumulators, issues, "
    "and support cases for root cause analysis and operational investigation."
)

member_options = load_member_options()
if not member_options:
    st.error("No members found in the system.")
    st.stop()

# Load default from session state
default_member = st.session_state.get("selected_member", None)
if default_member and str(default_member) in member_options:
    index = member_options.index(str(default_member))
else:
    index = 0

selected_member = st.selectbox("Select Member ID", member_options, index=index)

# Save selection to session state
st.session_state["selected_member"] = selected_member

if selected_member:
    st.divider()

    # Load all data
    eligibility_df = load_eligibility(selected_member)
    claims_df = load_claims(selected_member)
    transactions_df = load_accumulator_transactions(selected_member)
    snapshots_df = load_snapshots(selected_member)
    anomalies_df = load_anomalies(selected_member)
    issues_df = load_issues_for_member(selected_member)
    cases_df = load_cases_for_member(selected_member)
    sla_df = load_sla_for_member(selected_member)
    runs_df = load_runs_for_member(selected_member)

    # Summary metrics
    st.markdown("### 📊 Member Summary")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Eligibility Periods", len(eligibility_df))
    col2.metric("Claims", len(claims_df))
    col3.metric("Transactions", len(transactions_df))
    col4.metric("Issues", len(issues_df))
    col5.metric("Cases", len(cases_df))

    # Tabs for different views
    tab_eligibility, tab_claims, tab_accumulators, tab_issues, tab_cases, tab_timeline = st.tabs([
        "📅 Eligibility",
        "🧾 Claims",
        "💰 Accumulators",
        "🐛 Issues & Anomalies",
        "🎫 Cases & SLA",
        "⏳ Unified Timeline"
    ])

    with tab_eligibility:
        st.subheader("📅 Eligibility Periods")
        if eligibility_df.empty:
            st.info("No eligibility periods found for this member.")
        else:
            st.dataframe(
                eligibility_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "coverage_start": st.column_config.DateColumn("Start Date"),
                    "coverage_end": st.column_config.DateColumn("End Date"),
                }
            )

    with tab_claims:
        st.subheader("🧾 Claims History")
        if claims_df.empty:
            st.info("No claims found for this member.")
        else:
            st.dataframe(
                claims_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "service_date": st.column_config.DateColumn("Service Date"),
                    "paid_date": st.column_config.DateColumn("Paid Date"),
                    "paid_amount": st.column_config.NumberColumn("Paid Amount", format="$%.2f"),
                    "allowed_amount": st.column_config.NumberColumn("Allowed", format="$%.2f"),
                    "member_responsibility": st.column_config.NumberColumn("Member Resp.", format="$%.2f"),
                }
            )

    with tab_accumulators:
        st.subheader("💰 Accumulator Data")
        sub_tab_transactions, sub_tab_snapshots = st.tabs(["Transactions", "Snapshots"])

        with sub_tab_transactions:
            if transactions_df.empty:
                st.info("No accumulator transactions found.")
            else:
                st.dataframe(
                    transactions_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "service_date": st.column_config.DateColumn("Service Date"),
                        "delta_amount": st.column_config.NumberColumn("Delta Amount", format="$%.2f"),
                        "created_at": st.column_config.DatetimeColumn("Created"),
                    }
                )

        with sub_tab_snapshots:
            if snapshots_df.empty:
                st.info("No accumulator snapshots found.")
            else:
                st.dataframe(
                    snapshots_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "snapshot_ts": st.column_config.DatetimeColumn("Snapshot Time"),
                    }
                )

    with tab_issues:
        st.subheader("🐛 Issues & Anomalies")
        sub_tab_issues, sub_tab_anomalies = st.tabs(["Data Quality Issues", "Accumulator Anomalies"])

        with sub_tab_issues:
            if issues_df.empty:
                st.info("No issues found.")
            else:
                st.dataframe(
                    issues_df,
                    use_container_width=True,
                    hide_index=True,
                )

        with sub_tab_anomalies:
            if anomalies_df.empty:
                st.info("No anomalies found.")
            else:
                st.dataframe(
                    anomalies_df,
                    use_container_width=True,
                    hide_index=True,
                )

    with tab_cases:
        st.subheader("🎫 Support Cases & SLA")
        if cases_df.empty:
            st.info("No cases found.")
        else:
            st.dataframe(
                cases_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "opened_at": st.column_config.DatetimeColumn("Opened"),
                    "case_age_hours": st.column_config.NumberColumn("Age (hrs)", format="%.1f"),
                }
            )

        st.subheader("⏱️ SLA Exposure")
        if sla_df.empty:
            st.info("No SLA records.")
        else:
            st.dataframe(
                sla_df,
                use_container_width=True,
                hide_index=True,
            )

    with tab_timeline:
        st.subheader("⏳ Unified Timeline")
        # Combine all events into a timeline
        timeline_events = []

        if not eligibility_df.empty:
            for _, row in eligibility_df.iterrows():
                timeline_events.append({
                    "date": row["coverage_start"],
                    "type": "Eligibility Start",
                    "description": f"Plan: {row['plan_code']}",
                    "status": row["status"]
                })
                if pd.notna(row["coverage_end"]):
                    timeline_events.append({
                        "date": row["coverage_end"],
                        "type": "Eligibility End",
                        "description": f"Plan: {row['plan_code']}",
                        "status": row["status"]
                    })

        if not claims_df.empty:
            for _, row in claims_df.iterrows():
                amt = row.get("paid_amount") or row.get("allowed_amount") or 0
                try:
                    amt_str = f"${float(amt):.2f}"
                except (TypeError, ValueError):
                    amt_str = "—"
                timeline_events.append({
                    "date": row.get("service_date"),
                    "type": "Claim",
                    "description": f"Amount: {amt_str}",
                    "status": row.get("claim_status", "—"),
                })

        if not issues_df.empty:
            for _, row in issues_df.iterrows():
                timeline_events.append({
                    "date": row["detected_at"],
                    "type": "Issue",
                    "description": row["issue_description"],
                    "status": row["status"]
                })

        if not cases_df.empty:
            for _, row in cases_df.iterrows():
                timeline_events.append({
                    "date": row["opened_at"],
                    "type": "Case Opened",
                    "description": row["short_description"],
                    "status": row["status"]
                })

        if timeline_events:
            timeline_df = pd.DataFrame(timeline_events).sort_values("date", ascending=False)
            st.dataframe(
                timeline_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "date": st.column_config.DatetimeColumn("Date"),
                }
            )
        else:
            st.info("No timeline events found.")

    # Processing runs involved
    st.markdown("### ⚙️ Processing Runs Involved")
    if runs_df.empty:
        st.info("No processing runs found for this member's data.")
    else:
        st.dataframe(
            runs_df,
            use_container_width=True,
            hide_index=True,
        )

else:
    st.info("Select a member to view their timeline.")