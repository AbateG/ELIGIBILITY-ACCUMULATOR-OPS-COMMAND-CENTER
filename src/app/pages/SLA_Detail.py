# ═══════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import streamlit as st
import altair as alt
from datetime import datetime

from src.common.db import fetch_all, fetch_one
from src.app.utils import to_dataframe
from src.app.shared_ui import (
    render_entity_header, render_metric_row, render_context_section,
    render_navigation_section, render_audit_section, render_timeline_section,
    safe_text, fmt_number
)


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="SLA Detail",
    page_icon="⏱️",
    layout="wide",
)


# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════


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


def sla_status_emoji(status):
    """Return emoji for SLA status."""
    return {
        "MET": "✅",
        "AT_RISK": "⚠️",
        "BREACHED": "⛔",
        "OPEN": "📂",
    }.get(str(status).upper(), "❓")


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner="Loading SLA options …")
def load_sla_options():
    """Load available SLA IDs and case numbers for selection."""
    rows = fetch_all("""
        SELECT st.sla_id, sc.case_number
        FROM sla_tracking st
        JOIN support_cases sc ON st.case_id = sc.case_id
        ORDER BY st.sla_id DESC
    """)
    return [(row["sla_id"], f"SLA {row['sla_id']} - Case {row['case_number']}") for row in rows]


@st.cache_data(ttl=300, show_spinner="Loading SLA details …")
def load_sla_details(sla_id):
    """Load detailed information for a specific SLA."""
    row = fetch_one("""
        SELECT
            st.sla_id, st.sla_type, st.target_hours, st.target_due_at,
            st.status AS sla_status, st.is_at_risk, st.is_breached,
            st.breached_at, st.last_evaluated_at, st.created_at,
            st.case_id, sc.case_number, sc.priority, sc.status AS case_status,
            sc.short_description
        FROM sla_tracking st
        JOIN support_cases sc ON st.case_id = sc.case_id
        WHERE st.sla_id = ?
    """, (sla_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading linked case …")
def load_linked_case(case_id):
    """Load the support case for this SLA."""
    row = fetch_one("""
        SELECT
            sc.case_id, sc.case_number, sc.case_type, sc.priority,
            sc.severity, sc.status, sc.assigned_to,
            sc.opened_at, sc.resolved_at, sc.updated_at
        FROM support_cases sc
        WHERE sc.case_id = ?
    """, (case_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading linked issue …")
def load_linked_issue(case_id):
    """Load the data quality issue linked to the case."""
    row = fetch_one("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status, i.detected_at, i.member_id, i.file_id
        FROM data_quality_issues i
        JOIN support_cases sc ON i.issue_id = sc.issue_id
        WHERE sc.case_id = ?
    """, (case_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading SLA audit …")
def load_sla_audit(sla_id):
    """Load audit events for a specific SLA."""
    try:
        rows = fetch_all("""
            SELECT event_type, actor, event_details, created_at
            FROM audit_log
            WHERE entity_name = 'sla_tracking' AND entity_key = ?
            ORDER BY created_at DESC
        """, (str(sla_id),))
        return to_dataframe(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Loading SLA escalation …")
def load_sla_escalation(sla_id):
    """Load escalation events for a specific SLA."""
    try:
        rows = fetch_all("""
            SELECT event_type, actor, event_details, created_at
            FROM audit_log
            WHERE entity_name = 'support_case' AND entity_key IN (
                SELECT CAST(case_id AS TEXT) FROM sla_tracking WHERE sla_id = ?
            ) AND event_type LIKE '%ESCALAT%'
            ORDER BY created_at DESC
        """, (sla_id,))
        return to_dataframe(rows)
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("⏱️ SLA Detail")
st.caption(
    "Comprehensive view of a Service Level Agreement including target, status, "
    "compliance details, linked entities, and escalation history."
)

sla_options = load_sla_options()
if not sla_options:
    st.error("No SLAs found in the system.")
    st.stop()

# Load default from session state
default_sla = st.session_state.get("selected_sla", None)
if default_sla:
    selected_option = next((opt for sid, opt in sla_options if sid == int(default_sla)), sla_options[0][1])
    index = [opt for _, opt in sla_options].index(selected_option)
else:
    index = 0

selected_option = st.selectbox("Select SLA", [opt for _, opt in sla_options], index=index)
selected_sla_id = next(sid for sid, opt in sla_options if opt == selected_option)

# Save selection to session state
st.session_state["selected_sla"] = selected_sla_id

if selected_sla_id:
    st.divider()

    # Load all data
    sla_data = load_sla_details(selected_sla_id)
    if not sla_data:
        st.error("SLA not found.")
        st.stop()

    linked_case = load_linked_case(sla_data["case_id"])
    linked_issue = load_linked_issue(sla_data["case_id"])
    sla_audit = load_sla_audit(selected_sla_id)
    sla_escalation = load_sla_escalation(selected_sla_id)

    # SLA header using shared component
    render_entity_header(
        title=f"SLA {sla_data['sla_id']} - {sla_data['sla_type']}",
        subtitle=f"Case: {sla_data['case_number']}",
        status=sla_data["sla_status"],
        priority=sla_data["priority"],
        updated_at=sla_data.get("last_evaluated_at")
    )

    # Key metrics
    metrics = {
        "Target Hours": sla_data["target_hours"],
        "At Risk": "Yes" if sla_data["is_at_risk"] else "No",
        "Breached": "Yes" if sla_data["is_breached"] else "No",
    }
    render_metric_row(metrics)

    # Tabs for organization
    tab_overview, tab_context, tab_risk, tab_actions, tab_audit = st.tabs([
        "📋 Overview",
        "🔗 Context",
        "⚠️ Risk & Compliance",
        "🚀 Actions & Escalation",
        "📊 Audit & Timeline"
    ])

    with tab_overview:
        # SLA metadata using shared components
        metadata = {
            "SLA ID": sla_data['sla_id'],
            "SLA Type": sla_data['sla_type'],
            "Target Hours": sla_data['target_hours'],
            "Target Due": sla_data.get('target_due_at'),
            "Case ID": sla_data['case_id'],
            "Case Number": sla_data['case_number'],
            "Case Status": sla_data['case_status'],
            "Created": sla_data.get('created_at'),
            "Last Evaluated": sla_data.get('last_evaluated_at'),
        }
        render_context_section("SLA Metadata", metadata)

        st.markdown("### Current State")
        state_metrics = {
            "At Risk": "Yes" if sla_data["is_at_risk"] else "No",
            "Breached": "Yes" if sla_data["is_breached"] else "No",
        }
        if sla_data["is_breached"]:
            state_metrics["Breached At"] = sla_data.get("breached_at")
        else:
            state_metrics["Due At"] = sla_data.get("target_due_at")
        render_metric_row(state_metrics)

        st.markdown("### Case Description")
        st.write(safe_text(sla_data.get("short_description")))

    with tab_context:
        st.markdown("### Linked Support Case")
        if linked_case:
            st.markdown(f"**Case ID:** `{linked_case['case_id']}`")
            st.markdown(f"**Case Number:** `{linked_case['case_number']}`")
            st.markdown(f"**Case Type:** {linked_case['case_type']}")
            _priority_icons = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🔵"}
            _priority_emoji = _priority_icons.get(str(linked_case.get("priority", "")).upper(), "⚪")
            st.markdown(f"**Priority:** {_priority_emoji} {linked_case['priority']}")
            st.markdown(f"**Severity:** {linked_case['severity']}")
            st.markdown(f"**Status:** {linked_case['status']}")
            st.markdown(f"**Assigned To:** {safe_text(linked_case.get('assigned_to'), 'Unassigned')}")
            st.markdown(f"**Opened:** {safe_text(linked_case.get('opened_at'))}")
            st.markdown(f"**Resolved:** {safe_text(linked_case.get('resolved_at'))}")
            st.markdown(f"**Updated:** {safe_text(linked_case.get('updated_at'))}")
        else:
            st.write("No linked case found.")

        st.markdown("### Linked Data Quality Issue")
        if linked_issue:
            st.markdown(f"**Issue ID:** `{linked_issue['issue_id']}`")
            st.markdown(f"**Code:** {safe_text(linked_issue['issue_code'])}")
            st.markdown(f"**Type:** {safe_text(linked_issue['issue_type'])}")
            st.markdown(f"**Subtype:** {safe_text(linked_issue['issue_subtype'])}")
            st.markdown(f"**Severity:** {linked_issue['severity']}")
            st.markdown(f"**Status:** {linked_issue['status']}")
            st.markdown(f"**Detected:** {linked_issue['detected_at']}")
            st.markdown(f"**Member ID:** `{safe_text(linked_issue.get('member_id'))}`")
            st.markdown(f"**File ID:** `{safe_text(linked_issue.get('file_id'))}`")
        else:
            st.write("No linked issue found.")

    with tab_risk:
        st.markdown("### Compliance Status")
        compliance1, compliance2 = st.columns(2)
        with compliance1:
            st.metric("SLA Status", f"{sla_status_emoji(sla_data['sla_status'])} {sla_data['sla_status']}")
            st.metric("Target Hours", fmt_number(sla_data["target_hours"]))
        with compliance2:
            st.metric("At Risk", "Yes" if sla_data["is_at_risk"] else "No")
            st.metric("Breached", "Yes" if sla_data["is_breached"] else "No")

        if sla_data["is_breached"]:
            st.markdown("### Breach Details")
            st.error(f"**BREACHED** at {safe_text(sla_data.get('breached_at'))}")
            st.markdown("This SLA has exceeded its resolution deadline.")
        elif sla_data["is_at_risk"]:
            st.markdown("### Risk Details")
            st.warning("**AT RISK** - This SLA is approaching its deadline.")
            st.markdown("Monitor closely and consider escalation if resolution is delayed.")
        else:
            st.markdown("### Compliance Status")
            st.success("**ON TRACK** - This SLA is within acceptable time limits.")

        st.markdown("### Evaluation History")
        st.markdown(f"**Created:** {safe_text(sla_data.get('created_at'))}")
        st.markdown(f"**Last Evaluated:** {safe_text(sla_data.get('last_evaluated_at'))}")
        if sla_data.get("target_due_at"):
            now = datetime.now()
            due = pd.to_datetime(sla_data["target_due_at"])
            if now > due:
                overdue = (now - due).total_seconds() / 3600
                st.error(f"**Overdue by:** {overdue:.1f} hours")
            else:
                remaining = (due - now).total_seconds() / 3600
                st.info(f"**Hours Remaining:** {remaining:.1f}")

    with tab_actions:
        st.markdown("### Escalation Status")
        if sla_data["is_breached"]:
            st.error("**BREACHED** - Immediate escalation recommended.")
        elif sla_data["is_at_risk"]:
            st.warning("**AT RISK** - Consider escalation if resolution is not imminent.")
        else:
            st.success("**ON TRACK** - No escalation needed at this time.")

        if not sla_escalation.empty:
            st.markdown("### Escalation History")
            for _, esc in sla_escalation.iterrows():
                st.markdown(f"**{esc['event_type']}** by {esc['actor']} at {esc['created_at']}:")
                st.write(esc['event_details'])
                st.divider()
        else:
            st.markdown("### Escalation History")
            st.write("No escalations recorded for this SLA.")

        st.markdown("### Recommended Actions")
        if sla_data["is_breached"]:
            st.error("**URGENT:** Escalate to management, assign senior resources, communicate to stakeholders.")
        elif sla_data["is_at_risk"]:
            st.warning("**MONITOR:** Ensure active assignment, consider priority increase, prepare escalation plan.")
        else:
            st.success("**NORMAL:** Continue standard case management procedures.")

    with tab_audit:
        render_audit_section(sla_audit)

        # Timeline
        timeline_events = [
            ("SLA Created", sla_data.get("created_at")),
            ("Last Evaluated", sla_data.get("last_evaluated_at")),
            ("Breached", sla_data.get("breached_at")),
        ]
        if linked_case:
            timeline_events.extend([
                ("Case Opened", linked_case.get("opened_at")),
                ("Case Resolved", linked_case.get("resolved_at")),
            ])
        render_timeline_section(timeline_events)

else:
    st.info("Select an SLA to view details.")