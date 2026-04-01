import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

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
from datetime import datetime

from src.common.db import fetch_all, fetch_one
from src.issues.support_case_service import add_case_note
from src.app.utils import to_dataframe
from src.app.shared_ui import (
    render_entity_header, render_metric_row, render_context_section,
    render_navigation_section, render_notes_section, render_audit_section,
    render_timeline_section, safe_text, fmt_number
)


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Support Case Detail",
    page_icon="🎫",
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

@st.cache_data(ttl=300, show_spinner="Loading case options …")
def load_case_options():
    """Load available case IDs and numbers for selection."""
    rows = fetch_all("""
        SELECT case_id, case_number
        FROM support_cases
        ORDER BY case_id DESC
    """)
    return [(row["case_id"], f"{row['case_number']} (ID: {row['case_id']})") for row in rows]


@st.cache_data(ttl=300, show_spinner="Loading case details …")
def load_case_details(case_id):
    """Load detailed information for a specific support case."""
    row = fetch_one("""
        SELECT
            sc.case_id, sc.case_number, sc.issue_id,
            sc.client_id, c.client_code,
            sc.vendor_id, v.vendor_code,
            sc.file_id, sc.run_id, sc.member_id, sc.claim_record_id,
            sc.case_type, sc.priority, sc.severity, sc.status,
            sc.assigned_team, sc.assigned_to,
            sc.short_description, sc.description,
            sc.root_cause_category, sc.escalation_level,
            sc.opened_at, sc.acknowledged_at, sc.resolved_at, sc.closed_at,
            sc.updated_at,
            st.sla_id, st.sla_type, st.target_hours, st.target_due_at,
            st.status AS sla_status, st.is_at_risk, st.is_breached,
            st.breached_at, st.last_evaluated_at
        FROM support_cases sc
        LEFT JOIN clients c ON sc.client_id = c.client_id
        LEFT JOIN vendors v ON sc.vendor_id = v.vendor_id
        LEFT JOIN sla_tracking st ON sc.case_id = st.case_id
        WHERE sc.case_id = ?
    """, (case_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading linked issue …")
def load_linked_issue(issue_id):
    """Load the data quality issue linked to the case."""
    if not issue_id:
        return None
    row = fetch_one("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status, i.issue_description,
            i.detected_at, i.file_id, i.run_id, i.member_id
        FROM data_quality_issues i
        WHERE i.issue_id = ?
    """, (issue_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading case notes …")
def load_case_notes(case_id):
    """Load notes for a specific case."""
    rows = fetch_all("""
        SELECT note, author, created_at
        FROM case_notes
        WHERE case_id = ?
        ORDER BY created_at DESC
    """, (case_id,))
    return to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner="Loading audit events …")
def load_audit_events(case_id):
    """Load audit events for a specific case."""
    try:
        rows = fetch_all("""
            SELECT event_type, actor, event_details, created_at
            FROM audit_log
            WHERE entity_name = 'support_case' AND entity_key = ?
            ORDER BY created_at DESC
        """, (str(case_id),))
        return to_dataframe(rows)
    except Exception:
        # If audit_log table doesn't exist, return empty
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("🎫 Support Case Detail")
st.caption(
    "Comprehensive view of a support case including metadata, linked entities, "
    "SLA status, investigation notes, and action history."
)

case_options = load_case_options()
if not case_options:
    st.error("No support cases found in the system.")
    st.stop()

# Load default from session state
default_case = st.session_state.get("selected_case", None)
if default_case:
    # Find the option for this case_id
    selected_option = next((opt for cid, opt in case_options if cid == int(default_case)), case_options[0][1])
    index = [opt for _, opt in case_options].index(selected_option)
else:
    index = 0

selected_option = st.selectbox("Select Support Case", [opt for _, opt in case_options], index=index)
selected_case_id = next(cid for cid, opt in case_options if opt == selected_option)

# Save selection to session state
st.session_state["selected_case"] = selected_case_id

if selected_case_id:
    st.divider()

    # Load all data
    case_data = load_case_details(selected_case_id)
    if not case_data:
        st.error("Case not found.")
        st.stop()

    linked_issue = load_linked_issue(case_data["issue_id"])
    notes_df = load_case_notes(selected_case_id)
    audit_df = load_audit_events(selected_case_id)

    # Case header using shared component
    render_entity_header(
        title=f"Case {case_data['case_number']}",
        subtitle=f"Type: {case_data['case_type']}",
        status=case_data["status"],
        priority=case_data["priority"],
        assignee=case_data["assigned_to"],
        updated_at=case_data["updated_at"]
    )

    # Tabs for organization
    tab_overview, tab_entities, tab_sla, tab_notes, tab_audit = st.tabs([
        "📋 Overview",
        "🔗 Linked Entities",
        "⏱️ SLA Status",
        "📝 Notes",
        "📊 Audit History"
    ])

    with tab_overview:
        # Case metadata using shared components
        metadata = {
            "Case ID": case_data['case_id'],
            "Case Number": case_data['case_number'],
            "Issue ID": case_data.get('issue_id'),
            "Case Type": case_data['case_type'],
            "Severity": case_data['severity'],
            "Assigned Team": case_data['assigned_team'],
            "Client": case_data.get('client_code'),
            "Vendor": case_data.get('vendor_code'),
            "File ID": case_data.get('file_id'),
            "Processing Run": case_data.get('run_id'),
            "Member ID": case_data.get('member_id'),
            "Claim Record": case_data.get('claim_record_id'),
            "Root Cause Category": case_data.get('root_cause_category'),
            "Escalation Level": case_data.get('escalation_level'),
        }
        render_context_section("Case Metadata", metadata)

        # Timeline
        timeline_events = [
            ("Opened", case_data["opened_at"]),
            ("Acknowledged", case_data.get("acknowledged_at")),
            ("Resolved", case_data.get("resolved_at")),
            ("Closed", case_data.get("closed_at")),
            ("Last Updated", case_data.get("updated_at"))
        ]
        render_timeline_section(timeline_events)

        st.markdown("### Description")
        st.markdown(f"**Short:** {safe_text(case_data['short_description'])}")
        if case_data.get("description"):
            st.markdown(f"**Full:** {safe_text(case_data['description'])}")

    with tab_entities:
        st.markdown("### 🔗 Linked Entities")
        entities = {
            "Member ID": case_data.get('member_id'),
            "File ID": case_data.get('file_id'),
            "Processing Run": case_data.get('run_id'),
            "Claim Record": case_data.get('claim_record_id'),
        }
        render_context_section("", entities, columns=2)

        # Navigation
        nav_links = []
        if case_data.get("member_id"):
            nav_links.append({
                "label": "👤 View Member Timeline",
                "session_key": "selected_member",
                "value": case_data["member_id"],
                "info": f"Navigate to Member Timeline and select Member ID: {case_data['member_id']}",
                "key": "view_member"
            })
        if case_data.get("file_id"):
            nav_links.append({
                "label": "📁 View File Detail",
                "session_key": "selected_file",
                "value": case_data["file_id"],
                "info": f"Navigate to File Detail and select File ID: {case_data['file_id']}",
                "key": "view_file"
            })
        if case_data.get("run_id"):
            nav_links.append({
                "label": "⚙️ View Processing Run",
                "session_key": "selected_run",
                "value": case_data["run_id"],
                "info": f"Navigate to Processing Run Detail and select Run ID: {case_data['run_id']}",
                "key": "view_run"
            })
        if case_data.get("sla_id"):
            nav_links.append({
                "label": "⏱️ View SLA Detail",
                "session_key": "selected_sla",
                "value": case_data["sla_id"],
                "info": f"Navigate to SLA Detail and select SLA ID: {case_data['sla_id']}",
                "key": "view_sla"
            })
        render_navigation_section(nav_links)

        if linked_issue:
            st.markdown("### 🐛 Linked Data Quality Issue")
            st.markdown(f"**Issue ID:** `{linked_issue['issue_id']}`")
            st.markdown(f"**Code:** {safe_text(linked_issue['issue_code'])}")
            st.markdown(f"**Type:** {safe_text(linked_issue['issue_type'])}")
            st.markdown(f"**Subtype:** {safe_text(linked_issue['issue_subtype'])}")
            st.markdown(f"**Severity:** {linked_issue['severity']}")
            st.markdown(f"**Status:** {linked_issue['status']}")
            st.markdown(f"**Detected:** {linked_issue['detected_at']}")
            st.markdown(f"**Description:** {safe_text(linked_issue['issue_description'])}")
        else:
            st.markdown("### 🐛 Linked Data Quality Issue")
            st.write("No linked issue found.")

    with tab_sla:
        st.markdown("### ⏱️ SLA Status")
        if case_data.get("sla_id"):
            sla_status = case_data["sla_status"]
            is_at_risk = case_data.get("is_at_risk", 0)
            is_breached = case_data.get("is_breached", 0)

            status_emoji = {
                "MET": "✅",
                "AT_RISK": "⚠️",
                "BREACHED": "⛔"
            }.get(sla_status, "❓")

            st.markdown(f"**SLA Status:** {status_emoji} {sla_status}")
            st.markdown(f"**SLA Type:** {safe_text(case_data['sla_type'])}")
            st.markdown(f"**Target Hours:** {safe_text(case_data['target_hours'])}")
            st.markdown(f"**Target Due:** {safe_text(case_data['target_due_at'])}")
            st.markdown(f"**At Risk:** {'Yes' if is_at_risk else 'No'}")
            st.markdown(f"**Breached:** {'Yes' if is_breached else 'No'}")
            if is_breached:
                st.markdown(f"**Breached At:** {safe_text(case_data.get('breached_at'))}")
            st.markdown(f"**Last Evaluated:** {safe_text(case_data.get('last_evaluated_at'))}")
        else:
            st.write("No SLA associated with this case.")

    with tab_notes:
        render_notes_section(notes_df)

        # Add note section
        st.markdown("### Add New Note")
        note_text = st.text_area("New Note", height=100, key=f"note_{selected_case_id}")
        if st.button("📝 Add Note"):
            if note_text.strip():
                try:
                    add_case_note(selected_case_id, note_text.strip())
                    st.success("Note added successfully!")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to add note: {e}")
            else:
                st.warning("Please enter a note.")

    with tab_audit:
        render_audit_section(audit_df)

else:
    st.info("Select a support case to view details.")