import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

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
    render_navigation_section, safe_text, fmt_number
)


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Processing Run Detail",
    page_icon="⚙️",
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

@st.cache_data(ttl=300, show_spinner="Loading run options …")
def load_run_options():
    """Load available processing run IDs and types for selection."""
    rows = fetch_all("""
        SELECT run_id, run_type, run_status
        FROM processing_runs
        ORDER BY run_id DESC
    """)
    return [(row["run_id"], f"Run {row['run_id']} - {row['run_type']} ({row['run_status']})") for row in rows]


@st.cache_data(ttl=300, show_spinner="Loading run details …")
def load_run_details(run_id):
    """Load detailed information for a specific processing run."""
    row = fetch_one("""
        SELECT
            r.run_id, r.run_type, r.file_id,
            r.run_status, r.started_at, r.completed_at,
            r.rows_read, r.rows_passed, r.rows_failed, r.issue_count,
            r.notes
        FROM processing_runs r
        WHERE r.run_id = ?
    """, (run_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading linked file …")
def load_linked_file(run_id):
    """Load the inbound file for this run."""
    row = fetch_one("""
        SELECT
            f.file_id, f.file_name, f.file_type,
            f.client_id, c.client_code,
            f.vendor_id, v.vendor_code,
            f.processing_status, f.row_count, f.error_count
        FROM inbound_files f
        LEFT JOIN clients c ON f.client_id = c.client_id
        LEFT JOIN vendors v ON f.vendor_id = v.vendor_id
        JOIN processing_runs r ON f.file_id = r.file_id
        WHERE r.run_id = ?
    """, (run_id,))
    return row


@st.cache_data(ttl=300, show_spinner="Loading issues from run …")
def load_run_issues(run_id):
    """Load data quality issues generated during this run."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status, i.issue_description,
            i.detected_at, i.member_id, i.file_id
        FROM data_quality_issues i
        WHERE i.run_id = ?
        ORDER BY i.detected_at DESC
    """, (run_id,))
    return to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner="Loading cases from run …")
def load_run_cases(run_id):
    """Load support cases linked to issues from this run."""
    rows = fetch_all("""
        SELECT
            sc.case_id, sc.case_number, sc.case_type, sc.priority,
            sc.severity, sc.status, sc.short_description,
            sc.opened_at, sc.resolved_at,
            sc.assigned_to
        FROM support_cases sc
        JOIN data_quality_issues i ON sc.issue_id = i.issue_id
        WHERE i.run_id = ?
        ORDER BY sc.opened_at DESC
    """, (run_id,))
    return to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner="Loading run metrics …")
def load_run_metrics(run_id):
    """Load summary metrics for the run."""
    run_data = load_run_details(run_id)
    if not run_data:
        return {}

    issues_df = load_run_issues(run_id)
    cases_df = load_run_cases(run_id)

    metrics = {
        "total_issues": len(issues_df),
        "total_cases": len(cases_df),
        "affected_members": issues_df["member_id"].nunique() if not issues_df.empty else 0,
        "success_rate": (run_data["rows_passed"] / run_data["rows_read"] * 100) if run_data["rows_read"] > 0 else 0,
        "failure_rate": (run_data["rows_failed"] / run_data["rows_read"] * 100) if run_data["rows_read"] > 0 else 0,
    }

    if run_data["completed_at"] and run_data["started_at"]:
        from datetime import datetime
        start = pd.to_datetime(run_data["started_at"])
        end = pd.to_datetime(run_data["completed_at"])
        duration_seconds = (end - start).total_seconds()
        metrics["duration_seconds"] = duration_seconds
        metrics["throughput_per_minute"] = (run_data["rows_read"] / duration_seconds * 60) if duration_seconds > 0 else 0
    else:
        metrics["duration_seconds"] = 0
        metrics["throughput_per_minute"] = 0

    return metrics


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("⚙️ Processing Run Detail")
st.caption(
    "Comprehensive view of a processing run including execution metrics, "
    "linked files, generated issues, and operational outcomes."
)

run_options = load_run_options()
if not run_options:
    st.error("No processing runs found in the system.")
    st.stop()

# Load default from session state
default_run = st.session_state.get("selected_run", None)
if default_run:
    selected_option = next((opt for rid, opt in run_options if rid == int(default_run)), run_options[0][1])
    index = [opt for _, opt in run_options].index(selected_option)
else:
    index = 0

selected_option = st.selectbox("Select Processing Run", [opt for _, opt in run_options], index=index)
selected_run_id = next(rid for rid, opt in run_options if opt == selected_option)

# Save selection to session state
st.session_state["selected_run"] = selected_run_id

if selected_run_id:
    st.divider()

    # Load all data
    run_data = load_run_details(selected_run_id)
    if not run_data:
        st.error("Run not found.")
        st.stop()

    linked_file = load_linked_file(selected_run_id)
    issues_df = load_run_issues(selected_run_id)
    cases_df = load_run_cases(selected_run_id)
    computed_metrics = load_run_metrics(selected_run_id)

    # Run header using shared component
    render_entity_header(
        title=f"Run {run_data['run_id']} - {run_data['run_type']}",
        status=run_data["run_status"],
        updated_at=run_data.get("completed_at") or run_data.get("started_at")
    )

    # Key metrics (display dict — does NOT overwrite computed_metrics)
    display_metrics = {
        "Rows Read": run_data["rows_read"],
        "Rows Passed": run_data["rows_passed"],
        "Rows Failed": run_data["rows_failed"],
        "Issues Generated": run_data["issue_count"]
    }
    render_metric_row(display_metrics)

    # Tabs for organization
    tab_overview, tab_files, tab_outcomes, tab_impact, tab_navigation = st.tabs([
        "📋 Overview",
        "📁 Files",
        "🎯 Outcomes",
        "📊 Operational Impact",
        "🔗 Navigation"
    ])

    with tab_overview:
        # Run metadata using shared components
        metadata = {
            "Run ID": run_data['run_id'],
            "Run Type": run_data['run_type'],
            "Run Status": run_data['run_status'],
            "File ID": run_data.get('file_id'),
            "Started": run_data.get('started_at'),
            "Completed": run_data.get('completed_at'),
            "Duration": f"{computed_metrics.get('duration_seconds', 0):.1f} seconds" if computed_metrics.get("duration_seconds", 0) > 0 else None,
            "Throughput": f"{computed_metrics.get('throughput_per_minute', 0):.1f} rows/minute" if computed_metrics.get("throughput_per_minute", 0) > 0 else None,
        }
        render_context_section("Run Metadata", metadata)

        st.markdown("### Performance Metrics")
        perf1, perf2, perf3 = st.columns(3)
        perf1.metric("Success Rate", f"{computed_metrics.get('success_rate', 0):.1f}%")
        perf2.metric("Failure Rate", f"{computed_metrics.get('failure_rate', 0):.1f}%")
        perf3.metric("Issues Created", fmt_number(computed_metrics.get('total_issues', 0)))

        if run_data.get("notes"):
            st.markdown("### Run Notes")
            st.write(run_data["notes"])

        if linked_file:
            st.markdown("### Linked File")
            st.markdown(f"**File ID:** `{linked_file['file_id']}`")
            st.markdown(f"**File Name:** {linked_file['file_name']}")
            st.markdown(f"**File Type:** {linked_file['file_type']}")
            st.markdown(f"**Client:** {safe_text(linked_file.get('client_code'))}")
            st.markdown(f"**Vendor:** {safe_text(linked_file.get('vendor_code'))}")
            st.markdown(f"**File Status:** {linked_file['processing_status']}")
            st.markdown(f"**File Row Count:** {fmt_number(linked_file['row_count'])}")
            st.markdown(f"**File Errors:** {fmt_number(linked_file['error_count'])}")
        else:
            st.markdown("### Linked File")
            st.write("No linked file found for this run.")

    with tab_files:
        st.markdown("### Files Processed")
        if linked_file:
            st.dataframe(
                pd.DataFrame([linked_file]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.write("No files linked to this run.")

    with tab_outcomes:
        st.markdown("### Processing Outcomes")
        if not issues_df.empty:
            st.markdown(f"**Total Issues:** {len(issues_df)}")

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
            st.write("No issues generated during this run.")

        if not cases_df.empty:
            st.markdown("### Support Cases Created")
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
            st.markdown("### Support Cases Created")
            st.write("No support cases linked to issues from this run.")

    with tab_impact:
        st.markdown("### Operational Impact")
        imp1, imp2, imp3, imp4 = st.columns(4)
        imp1.metric("Issues Generated", fmt_number(computed_metrics.get('total_issues', 0)))
        imp2.metric("Cases Created", fmt_number(computed_metrics.get('total_cases', 0)))
        imp3.metric("Members Affected", fmt_number(computed_metrics.get('affected_members', 0)))
        imp4.metric("Run Duration", f"{computed_metrics.get('duration_seconds', 0):.1f}s")

        if not issues_df.empty:
            # Clients/Vendors involved
            clients = issues_df.dropna(subset=["file_id"]).merge(
                pd.DataFrame([linked_file]) if linked_file else pd.DataFrame(),
                on="file_id", how="left"
            )["client_code"].dropna().unique()
            vendors = issues_df.dropna(subset=["file_id"]).merge(
                pd.DataFrame([linked_file]) if linked_file else pd.DataFrame(),
                on="file_id", how="left"
            )["vendor_code"].dropna().unique()

            st.markdown("### Scope Impact")
            scope1, scope2 = st.columns(2)
            with scope1:
                st.markdown("**Clients Involved:**")
                for client in clients:
                    st.write(f"- {client}")
            with scope2:
                st.markdown("**Vendors Involved:**")
                for vendor in vendors:
                    st.write(f"- {vendor}")

    with tab_navigation:
        nav_links = []
        if linked_file:
            nav_links.append({
                "label": "📁 View File Detail",
                "session_key": "selected_file",
                "value": linked_file["file_id"],
                "info": f"Navigate to File Detail and select File ID: {linked_file['file_id']}",
                "key": "view_file"
            })
        if not cases_df.empty:
            nav_links.append({
                "label": "🎫 View Issue Triage",
                "session_key": "triage_sel_files",
                "value": [str(run_data.get('file_id'))] if run_data.get('file_id') else [],
                "info": f"Navigate to Issue Triage and filter for File ID: {safe_text(run_data.get('file_id'))}",
                "key": "view_triage"
            })
        render_navigation_section(nav_links)

        if not issues_df.empty:
            st.markdown(f"**🐛 Related Issues:** {len(issues_df)} issues generated during this run.")
        if not cases_df.empty:
            st.markdown(f"**🎫 Related Cases:** {len(cases_df)} support cases created from run issues.")

else:
    st.info("Select a processing run to view details.")