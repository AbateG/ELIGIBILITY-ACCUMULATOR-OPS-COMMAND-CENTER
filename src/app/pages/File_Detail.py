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
from src.app.utils import to_dataframe
from src.app.shared_ui import (
    render_entity_header, render_metric_row, render_context_section,
    render_navigation_section, safe_text, fmt_number
)


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="File Detail",
    page_icon="📁",
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

@st.cache_data(ttl=300, show_spinner="Loading file options …")
def load_file_options():
    """Load available file IDs and names for selection."""
    rows = fetch_all("""
        SELECT file_id, file_name
        FROM inbound_files
        ORDER BY file_id DESC
    """)
    return [(row["file_id"], f"{row['file_name']} (ID: {row['file_id']})") for row in rows]


@st.cache_data(ttl=300, show_spinner="Loading file details …")
def load_file_details(file_id):
    """Load detailed information for a specific inbound file."""
    row = fetch_one("""
        SELECT
            f.file_id, f.file_name, f.file_type,
            f.client_id, c.client_code,
            f.vendor_id, v.vendor_code,
            f.expected_date, f.received_ts,
            f.processing_status, f.row_count,
            f.error_count, f.duplicate_flag,
            f.file_hash, f.landing_path, f.archived_path,
            f.created_at
        FROM inbound_files f
        LEFT JOIN clients c ON f.client_id = c.client_id
        LEFT JOIN vendors v ON f.vendor_id = v.vendor_id
        WHERE f.file_id = ?
    """, (file_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading linked processing run …")
def load_linked_run(file_id):
    """Load the processing run for this file."""
    row = fetch_one("""
        SELECT
            r.run_id, r.run_type, r.run_status,
            r.started_at, r.completed_at,
            r.rows_read, r.rows_passed, r.rows_failed, r.issue_count,
            r.notes
        FROM processing_runs r
        WHERE r.file_id = ?
        ORDER BY r.run_id DESC
        LIMIT 1
    """, (file_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading issues generated …")
def load_file_issues(file_id):
    """Load data quality issues generated from this file."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status, i.issue_description,
            i.detected_at, i.member_id
        FROM data_quality_issues i
        WHERE i.file_id = ?
        ORDER BY i.detected_at DESC
    """, (file_id,))
    return to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner="Loading support cases linked …")
def load_file_cases(file_id):
    """Load support cases linked to issues from this file."""
    rows = fetch_all("""
        SELECT
            sc.case_id, sc.case_number, sc.case_type, sc.priority,
            sc.severity, sc.status, sc.short_description,
            sc.opened_at, sc.resolved_at,
            sc.assigned_to
        FROM support_cases sc
        JOIN data_quality_issues i ON sc.issue_id = i.issue_id
        WHERE i.file_id = ?
        ORDER BY sc.opened_at DESC
    """, (file_id,))
    return to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner="Loading affected members …")
def load_affected_members(file_id):
    """Load members affected by issues from this file."""
    rows = fetch_all("""
        SELECT DISTINCT
            i.member_id
        FROM data_quality_issues i
        WHERE i.file_id = ? AND i.member_id IS NOT NULL
        ORDER BY i.member_id
    """, (file_id,))
    return [row["member_id"] for row in rows]


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("📁 File Detail")
st.caption(
    "Comprehensive view of an inbound file including processing status, "
    "validation results, generated issues, and downstream operational impact."
)

file_options = load_file_options()
if not file_options:
    st.error("No files found in the system.")
    st.stop()

# Load default from session state
default_file = st.session_state.get("selected_file", None)
if default_file:
    selected_option = next((opt for fid, opt in file_options if fid == int(default_file)), file_options[0][1])
    index = [opt for _, opt in file_options].index(selected_option)
else:
    index = 0

selected_option = st.selectbox("Select Inbound File", [opt for _, opt in file_options], index=index)
selected_file_id = next(fid for fid, opt in file_options if opt == selected_option)

# Save selection to session state
st.session_state["selected_file"] = selected_file_id

if selected_file_id:
    st.divider()

    # Load all data
    file_data = load_file_details(selected_file_id)
    if not file_data:
        st.error("File not found.")
        st.stop()

    linked_run = load_linked_run(selected_file_id)
    issues_df = load_file_issues(selected_file_id)
    cases_df = load_file_cases(selected_file_id)
    affected_members = load_affected_members(selected_file_id)

    # File header using shared component
    status = file_data["processing_status"]
    duplicate_indicator = " (Duplicate)" if file_data["duplicate_flag"] else ""
    render_entity_header(
        title=f"{file_data['file_name']}{duplicate_indicator}",
        subtitle=f"Type: {file_data['file_type']}",
        status=status,
        updated_at=file_data.get("created_at")
    )

    # Key metrics
    metrics = {
        "Total Rows": file_data.get("row_count"),
        "Errors": file_data.get("error_count"),
        "Duplicate": "Yes" if file_data.get("duplicate_flag") else "No",
    }
    render_metric_row(metrics)

    # Tabs for organization
    tab_overview, tab_processing, tab_impact, tab_navigation = st.tabs([
        "📋 Overview",
        "⚙️ Processing Results",
        "🎯 Impact",
        "🔗 Navigation"
    ])

    with tab_overview:
        # File metadata using shared components
        metadata = {
            "File ID": file_data['file_id'],
            "File Name": file_data['file_name'],
            "File Type": file_data['file_type'],
            "Processing Status": file_data['processing_status'],
            "Duplicate": 'Yes' if file_data.get('duplicate_flag') else 'No',
            "Expected Date": file_data.get('expected_date'),
            "Client": file_data.get('client_code'),
            "Vendor": file_data.get('vendor_code'),
            "Received": file_data.get('received_ts'),
            "File Hash": file_data.get('file_hash'),
            "Created": file_data.get('created_at'),
        }
        render_context_section("File Metadata", metadata)

        if linked_run:
            st.markdown("### Linked Processing Run")
            st.markdown(f"**Run ID:** `{linked_run['run_id']}`")
            st.markdown(f"**Run Type:** {linked_run['run_type']}")
            st.markdown(f"**Run Status:** {linked_run['run_status']}")
            st.markdown(f"**Started:** {safe_text(linked_run.get('started_at'))}")
            st.markdown(f"**Completed:** {safe_text(linked_run.get('completed_at'))}")
            st.markdown(f"**Rows Read:** {fmt_number(linked_run['rows_read'])}")
            st.markdown(f"**Rows Passed:** {fmt_number(linked_run['rows_passed'])}")
            st.markdown(f"**Rows Failed:** {fmt_number(linked_run['rows_failed'])}")
            st.markdown(f"**Issues Generated:** {fmt_number(linked_run['issue_count'])}")
            if linked_run.get("notes"):
                st.markdown(f"**Notes:** {linked_run['notes']}")
        else:
            st.markdown("### Linked Processing Run")
            st.write("No processing run found for this file.")

    with tab_processing:
        st.markdown("### Processing Results")
        if not issues_df.empty:
            st.markdown(f"**Issues Generated:** {len(issues_df)}")

            # Issues by severity
            sev_counts = issues_df["severity"].value_counts().reset_index()
            sev_counts.columns = ["Severity", "Count"]
            sev_chart = alt.Chart(sev_counts).mark_bar().encode(
                x=alt.X("Severity:N", sort=["CRITICAL", "HIGH", "MEDIUM", "LOW"]),
                y="Count:Q",
                color="Severity:N"
            ).properties(height=200)
            st.altair_chart(sev_chart, use_container_width=True)

            # Issues table
            st.dataframe(
                issues_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "detected_at": st.column_config.DatetimeColumn("Detected"),
                }
            )
        else:
            st.write("No issues generated from this file.")

    with tab_impact:
        st.markdown("### Operational Impact")
        imp1, imp2 = st.columns(2)
        with imp1:
            st.metric("Issues Generated", len(issues_df))
            st.metric("Support Cases", len(cases_df))
        with imp2:
            st.metric("Affected Members", len(affected_members))
            st.metric("Unique Issue Codes", issues_df["issue_code"].nunique() if not issues_df.empty else 0)

        if not cases_df.empty:
            st.markdown("### Linked Support Cases")
            st.dataframe(
                cases_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "opened_at": st.column_config.DatetimeColumn("Opened"),
                    "resolved_at": st.column_config.DatetimeColumn("Resolved"),
                }
            )
        else:
            st.markdown("### Linked Support Cases")
            st.write("No support cases linked to issues from this file.")

        if affected_members:
            st.markdown("### Affected Members")
            for member_id in affected_members[:10]:  # Limit display
                st.markdown(f"- `{member_id}`")
            if len(affected_members) > 10:
                st.write(f"... and {len(affected_members) - 10} more")
        else:
            st.markdown("### Affected Members")
            st.write("No specific members affected by issues from this file.")

    with tab_navigation:
        nav_links = []
        if linked_run:
            nav_links.append({
                "label": "⚙️ View Processing Run",
                "session_key": "selected_run",
                "value": linked_run["run_id"],
                "info": f"Navigate to Processing Run Detail and select Run ID: {linked_run['run_id']}",
                "key": "view_run"
            })
        for member_id in affected_members[:5]:  # Limit to 5 buttons
            nav_links.append({
                "label": f"👤 Member {member_id}",
                "session_key": "selected_member",
                "value": member_id,
                "info": f"Navigate to Member Timeline and select Member ID: {member_id}",
                "key": f"member_{member_id}"
            })
        if not cases_df.empty:
            nav_links.append({
                "label": "🎫 View Issue Triage",
                "session_key": "triage_sel_files",
                "value": [str(selected_file_id)],
                "info": f"Navigate to Issue Triage and filter for File ID: {selected_file_id}",
                "key": "view_triage"
            })
        render_navigation_section(nav_links)

        if not issues_df.empty:
            st.markdown(f"**🐛 Related Issues:** {len(issues_df)} issues generated from this file.")

else:
    st.info("Select a file to view details.")