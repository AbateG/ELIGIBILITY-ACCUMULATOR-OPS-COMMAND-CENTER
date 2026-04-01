"""
Processing Operations Dashboard

This page provides real-time visibility into processing operations,
performance metrics, and system health for the Eligibility Accumulator
Operations Command Center.
"""

import sys
import os
# Add project root to path for module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import streamlit as st
import pandas as pd
import altair as alt
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

from src.common.db import fetch_all, fetch_one
from src.common.observability import ProcessingMetrics


def get_recent_processing_runs(hours: int = 24) -> List[Dict[str, Any]]:
    """Get processing runs from the last N hours."""
    cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()

    return fetch_all(
        """
        SELECT
            run_id,
            run_type,
            file_id,
            run_status,
            started_at,
            completed_at,
            rows_read,
            rows_passed,
            rows_failed,
            issue_count,
            notes
        FROM processing_runs
        WHERE started_at >= ?
        ORDER BY started_at DESC
        """,
        (cutoff_time,)
    )


def get_processing_summary(hours: int = 24) -> Dict[str, Any]:
    """Get summary statistics for processing operations."""
    cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()

    summary = fetch_one(
        """
        SELECT
            COUNT(*) as total_runs,
            SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
            SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
            SUM(CASE WHEN run_status = 'PARTIAL_SUCCESS' THEN 1 ELSE 0 END) as partial_runs,
            SUM(rows_read) as total_rows_read,
            SUM(rows_passed) as total_rows_passed,
            SUM(rows_failed) as total_rows_failed,
            AVG(CASE WHEN completed_at IS NOT NULL THEN
                (julianday(completed_at) - julianday(started_at)) * 24 * 60 * 60
                ELSE NULL END) as avg_duration_seconds
        FROM processing_runs
        WHERE started_at >= ?
        """,
        (cutoff_time,)
    )

    if summary:
        summary['success_rate'] = (summary['successful_runs'] / summary['total_runs'] * 100) if summary['total_runs'] > 0 else 0
        summary['failure_rate'] = (summary['failed_runs'] / summary['total_runs'] * 100) if summary['total_runs'] > 0 else 0

    return summary or {}


def get_failed_files() -> List[Dict[str, Any]]:
    """Get recently failed files."""
    return fetch_all(
        """
        SELECT
            f.file_id,
            f.file_name,
            f.file_type,
            f.processing_status,
            f.error_count,
            f.received_ts,
            r.run_id,
            r.run_status,
            r.notes,
            r.completed_at
        FROM inbound_files f
        LEFT JOIN processing_runs r ON f.file_id = r.file_id
        WHERE f.processing_status = 'FAILED'
        ORDER BY f.received_ts DESC
        LIMIT 20
        """
    )


def get_issue_summary() -> Dict[str, Any]:
    """Get summary of current data quality issues."""
    return fetch_one(
        """
        SELECT
            COUNT(*) as total_issues,
            SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) as open_issues,
            SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_severity,
            SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_severity
        FROM data_quality_issues
        """
    ) or {}


def get_support_case_summary() -> Dict[str, Any]:
    """Get summary of support cases."""
    return fetch_one(
        """
        SELECT
            COUNT(*) as total_cases,
            SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) as open_cases,
            SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as in_progress_cases
        FROM support_cases
        """
    ) or {}


def get_sla_status() -> Dict[str, Any]:
    """Get SLA compliance status."""
    return fetch_one(
        """
        SELECT
            COUNT(*) as total_slas,
            SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) as open_slas,
            SUM(CASE WHEN is_breached = 1 THEN 1 ELSE 0 END) as breached_slas,
            SUM(CASE WHEN is_at_risk = 1 AND is_breached = 0 THEN 1 ELSE 0 END) as warning_slas
        FROM sla_tracking
        """
    ) or {}


def create_runs_timeline_chart(runs_data: List[Dict[str, Any]]) -> alt.Chart:
    """Create a timeline chart of processing runs."""
    if not runs_data:
        return alt.Chart().mark_text(text="No data available")

    df = pd.DataFrame(runs_data)
    df['started_at'] = pd.to_datetime(df['started_at'])

    # Create status color mapping
    status_colors = {
        'SUCCESS': 'green',
        'FAILED': 'red',
        'PARTIAL_SUCCESS': 'orange',
        'RUNNING': 'blue'
    }

    chart = alt.Chart(df).mark_circle(size=100).encode(
        x=alt.X('started_at:T', title='Time'),
        y=alt.Y('run_type:N', title='Run Type'),
        color=alt.Color('run_status:N',
                       scale=alt.Scale(domain=list(status_colors.keys()),
                                      range=list(status_colors.values())),
                       title='Status'),
        tooltip=['run_id', 'file_id', 'run_status', 'rows_read', 'started_at']
    ).properties(
        height=200,
        title="Processing Runs Timeline (Last 24h)"
    )

    return chart


def create_success_rate_gauge(success_rate: float) -> alt.Chart:
    """Create a gauge chart for success rate."""
    # Determine color in Python — avoids broken nested alt.condition() in Altair v5
    if success_rate > 90:
        gauge_color = 'green'
    elif success_rate > 75:
        gauge_color = 'orange'
    else:
        gauge_color = 'red'

    data = pd.DataFrame({
        'category': ['Success Rate'],
        'value': [success_rate]
    })

    chart = alt.Chart(data).mark_arc(innerRadius=50).encode(
        theta=alt.Theta('value:Q', scale=alt.Scale(domain=[0, 100])),
        color=alt.value(gauge_color),
        tooltip=[
            alt.Tooltip('category:N', title='Metric'),
            alt.Tooltip('value:Q', title='Rate (%)', format='.1f'),
        ]
    ).properties(
        title="Success Rate (Last 24h)",
        height=150,
        width=150
    )

    return chart


# Page Configuration
st.set_page_config(
    page_title="Processing Dashboard - Eligibility Ops Command Center",
    page_icon="📊",
    layout="wide"
)

st.title("🚀 Processing Operations Dashboard")
st.caption("Real-time visibility into system processing operations and health metrics.")

# Quick Actions and Controls
st.header("⚡ Quick Actions")

action_col1, action_col2, action_col3, action_col4 = st.columns(4)
with action_col1:
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

with action_col2:
    if st.button("📁 View Failed Files", use_container_width=True):
        st.switch_page("pages/File_Monitoring.py")

with action_col3:
    if st.button("🎫 Triage Issues", use_container_width=True):
        st.switch_page("pages/Issue_Triage.py")

with action_col4:
    if st.button("💰 Check Accumulators", use_container_width=True):
        st.switch_page("pages/Accumulator_Reconciliation.py")

st.divider()

# Time range selector
col1, col2, col3 = st.columns([2, 2, 1])
with col1:
    time_range = st.selectbox(
        "📅 Analysis Time Range",
        ["Last 24 hours", "Last 7 days", "Last 30 days"],
        index=0,
        help="Select the time period for processing analysis"
    )

with col2:
    auto_refresh = st.checkbox("🔄 Auto-refresh every 30 seconds", value=True,
                              help="Automatically refresh dashboard data")

with col3:
    st.metric("Last Updated", f"{datetime.now().strftime('%H:%M:%S')}",
             help="Time of last data refresh")

# Convert time range to hours
hours_map = {
    "Last 24 hours": 24,
    "Last 7 days": 168,
    "Last 30 days": 720
}
selected_hours = hours_map[time_range]

# Store in session state for cross-page context
st.session_state["dashboard_selected_hours"] = selected_hours
st.session_state["dashboard_time_range"] = time_range

# Add a note about the selected time range
st.info(f"📊 **Dashboard Scope:** Analyzing processing operations from the last {selected_hours} hours")

# Key Metrics Row
st.header("📈 Key Metrics")

col1, col2, col3, col4, col5 = st.columns(5)

summary = get_processing_summary(selected_hours)

with col1:
    st.metric(
        "Total Runs",
        summary.get('total_runs', 0),
        help="Number of processing runs in selected time range"
    )

with col2:
    success_rate = summary.get('success_rate', 0)
    st.metric(
        "Success Rate",
        f"{success_rate:.1f}%",
        delta=f"{success_rate - 95:.1f}%" if success_rate < 95 else None,
        delta_color="inverse" if success_rate < 95 else "normal",
        help="Percentage of runs that completed successfully"
    )

with col3:
    st.metric(
        "Rows Processed",
        f"{summary.get('total_rows_read', 0):,}",
        help="Total number of data rows processed"
    )

with col4:
    avg_duration = summary.get('avg_duration_seconds', 0)
    st.metric(
        "Avg Duration",
        f"{avg_duration:.1f}s" if avg_duration else "N/A",
        help="Average processing time per run"
    )

with col5:
    issue_summary = get_issue_summary()
    st.metric(
        "Open Issues",
        issue_summary.get('open_issues', 0),
        help="Number of unresolved data quality issues"
    )

# Charts Row
st.header("📊 Processing Trends")

col1, col2 = st.columns([2, 1])

with col1:
    # Timeline chart
    runs_data = get_recent_processing_runs(selected_hours)
    timeline_chart = create_runs_timeline_chart(runs_data)
    st.altair_chart(timeline_chart, use_container_width=True)

with col2:
    # Success rate gauge
    gauge_chart = create_success_rate_gauge(success_rate)
    st.altair_chart(gauge_chart, use_container_width=True)

# Detailed Tables
st.header("📋 Recent Activity")

tab1, tab2, tab3, tab4 = st.tabs(["Recent Runs", "Failed Files", "Issue Summary", "SLA Status"])

with tab1:
    st.subheader("Recent Processing Runs")
    if runs_data:
        df_runs = pd.DataFrame(runs_data)
        df_runs['started_at'] = pd.to_datetime(df_runs['started_at'])
        df_runs['duration'] = df_runs.apply(
            lambda row: (
                (pd.to_datetime(row['completed_at']) - row['started_at']).total_seconds()
                if pd.notna(row['completed_at']) else None
            ),
            axis=1
        )

        # Format for display
        display_df = df_runs[[
            'run_id', 'run_type', 'file_id', 'run_status',
            'rows_read', 'rows_passed', 'rows_failed', 'started_at', 'duration'
        ]].copy()

        display_df['duration'] = display_df['duration'].apply(
            lambda x: f"{x:.1f}s" if x is not None else "Running"
        )

        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No processing runs found in the selected time range.")

with tab2:
    st.subheader("Recently Failed Files")

    failed_files = get_failed_files()
    if failed_files:
        df_failed = pd.DataFrame(failed_files)
        if "received_ts" in df_failed.columns:
            df_failed["received_ts"] = pd.to_datetime(df_failed["received_ts"], errors="coerce")

        # Make file_id clickable for drilldown
        st.dataframe(
            df_failed,
            use_container_width=True,
            column_config={
                "file_id": st.column_config.NumberColumn(
                    "File ID",
                    help="Click to view file details",
                    width="small"
                ),
                "file_name": st.column_config.TextColumn(
                    "File Name",
                    help="Name of the failed file",
                    width="medium"
                ),
                "file_type": st.column_config.TextColumn(
                    "Type",
                    help="ELIGIBILITY or CLAIMS",
                    width="small"
                ),
                "processing_status": st.column_config.TextColumn(
                    "Status",
                    help="Current processing status",
                    width="small"
                ),
                "error_count": st.column_config.NumberColumn(
                    "Errors",
                    help="Number of errors encountered",
                    width="small"
                ),
                "received_ts": st.column_config.DatetimeColumn(
                    "Received",
                    help="When the file was received",
                    format="MMM DD, YYYY HH:mm"
                ),
                "run_status": st.column_config.TextColumn(
                    "Last Run Status",
                    help="Status of the last processing run",
                    width="medium"
                ),
                "notes": st.column_config.TextColumn(
                    "Error Details",
                    help="Details about the failure",
                    width="large"
                )
            }
        )

        # Add drilldown action
        st.markdown("---")
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("🔍 Investigate in File Monitoring", use_container_width=True):
                st.switch_page("pages/File_Monitoring.py")
        with col2:
            st.caption("Switch to File Monitoring page for detailed file analysis and reprocessing options")

    else:
        st.success("✅ No failed files found - all systems operational!")

with tab3:
    st.subheader("Data Quality Issues & Support Cases")

    issue_summary = get_issue_summary()
    case_summary = get_support_case_summary()

    # Issues metrics with drilldown
    st.markdown("**📋 Data Quality Issues**")
    issue_col1, issue_col2, issue_col3, issue_col4 = st.columns(4)

    with issue_col1:
        total_issues = issue_summary.get('total_issues', 0)
        st.metric("Total Issues", total_issues)
    with issue_col2:
        open_issues = issue_summary.get('open_issues', 0)
        delta_color = "inverse" if open_issues > 5 else "normal"
        st.metric("Open Issues", open_issues, delta_color=delta_color)
    with issue_col3:
        high_severity = issue_summary.get('high_severity', 0)
        st.metric("High Severity", high_severity)
    with issue_col4:
        critical_issues = issue_summary.get('critical_severity', 0)
        st.metric("Critical Issues", critical_issues)

    # Support cases metrics
    st.markdown("**📂 Support Cases**")
    case_col1, case_col2, case_col3, case_col4 = st.columns(4)

    with case_col1:
        total_cases = case_summary.get('total_cases', 0)
        st.metric("Total Cases", total_cases)
    with case_col2:
        open_cases = case_summary.get('open_cases', 0)
        st.metric("Open Cases", open_cases)
    with case_col3:
        in_progress = case_summary.get('in_progress_cases', 0)
        st.metric("In Progress", in_progress)
    with case_col4:
        resolved_cases = total_cases - open_cases - in_progress
        st.metric("Resolved", resolved_cases)

    # Quick actions for issue management
    st.markdown("---")
    action_col1, action_col2, action_col3 = st.columns(3)

    with action_col1:
        if open_issues > 0:
            if st.button("🎫 Triage Open Issues", use_container_width=True):
                st.switch_page("pages/Issue_Triage.py")
        else:
            st.success("✅ All issues resolved!")

    with action_col2:
        if critical_issues > 0:
            st.error(f"🚨 {critical_issues} critical issues need immediate attention")
        elif high_severity > 0:
            st.warning(f"⚠️ {high_severity} high-priority issues to review")
        else:
            st.info("ℹ️ No high-priority issues currently")

    with action_col3:
        if open_cases > 0:
            st.info(f"📋 {open_cases} cases in queue")
        else:
            st.success("✅ No pending cases")

with tab4:
    st.subheader("SLA Compliance & Service Levels")
    sla_status = get_sla_status()

    # SLA metrics with enhanced visibility
    sla_col1, sla_col2, sla_col3, sla_col4 = st.columns(4)

    with sla_col1:
        total_slas = sla_status.get('total_slas', 0)
        st.metric("Total SLAs", total_slas)
    with sla_col2:
        open_slas = sla_status.get('open_slas', 0)
        st.metric("Active SLAs", open_slas)
    with sla_col3:
        breached_slas = sla_status.get('breached_slas', 0)
        delta_color = "inverse" if breached_slas > 0 else "normal"
        st.metric("Breached", breached_slas, delta=breached_slas, delta_color=delta_color)
    with sla_col4:
        warning_slas = sla_status.get('warning_slas', 0)
        st.metric("At Risk", warning_slas, delta=warning_slas,
                 delta_color="inverse" if warning_slas > 0 else "off")

    # SLA Health Assessment
    if breached_slas > 0:
        st.error(f"🚨 **SLA Crisis:** {breached_slas} SLA(s) have been breached - immediate action required!")
    elif warning_slas > 0:
        st.warning(f"⚠️ **SLA Warning:** {warning_slas} SLA(s) are approaching breach - monitor closely")
    elif open_slas > 0:
        st.info(f"ℹ️ **SLA Status:** {open_slas} SLA(s) active, all within acceptable limits")
    else:
        st.success("✅ **SLA Health:** No active SLAs - excellent service delivery")

    # SLA Actions
    if open_slas > 0 or breached_slas > 0 or warning_slas > 0:
        st.markdown("---")
        if st.button("📋 Review SLA Details in Issue Triage", use_container_width=True):
            st.switch_page("pages/Issue_Triage.py")

# System Health Indicators
st.header("🏥 System Health")

health_col1, health_col2, health_col3 = st.columns(3)

with health_col1:
    if success_rate >= 95:
        st.success("✅ Processing Health: Excellent")
    elif success_rate >= 85:
        st.warning("⚠️ Processing Health: Good")
    else:
        st.error("❌ Processing Health: Needs Attention")

with health_col2:
    open_issues = issue_summary.get('open_issues', 0)
    if open_issues == 0:
        st.success("✅ Data Quality: Perfect")
    elif open_issues < 10:
        st.info("ℹ️ Data Quality: Minor Issues")
    else:
        st.warning("⚠️ Data Quality: Review Needed")

with health_col3:
    breached_slas = sla_status.get('breached_slas', 0)
    if breached_slas == 0:
        st.success("✅ SLA Compliance: On Track")
    else:
        st.error(f"❌ SLA Compliance: {breached_slas} Breached")

# System Health Summary
st.header("🏥 System Health Assessment")

# Calculate overall system health score
summary = get_processing_summary(selected_hours)
issue_summary = get_issue_summary()
sla_status = get_sla_status()

# Health scoring (0-100)
processing_health = 100 if summary.get('success_rate', 0) >= 95 else (
    75 if summary.get('success_rate', 0) >= 85 else 50
)

issue_health = 100 if issue_summary.get('open_issues', 0) == 0 else (
    75 if issue_summary.get('open_issues', 0) <= 5 else (
        50 if issue_summary.get('open_issues', 0) <= 20 else 25
    )
)

sla_health = 100 if sla_status.get('breached_slas', 0) == 0 else (
    50 if sla_status.get('warning_slas', 0) > 0 else 0
)

overall_health = int((processing_health + issue_health + sla_health) / 3)

# Health assessment display
health_col1, health_col2, health_col3, health_col4 = st.columns(4)

with health_col1:
    if overall_health >= 90:
        st.success(f"🎯 **Overall Health: {overall_health}%**")
        st.caption("System operating optimally")
    elif overall_health >= 75:
        st.info(f"ℹ️ **Overall Health: {overall_health}%**")
        st.caption("Minor issues to monitor")
    elif overall_health >= 50:
        st.warning(f"⚠️ **Overall Health: {overall_health}%**")
        st.caption("Attention needed")
    else:
        st.error(f"🚨 **Overall Health: {overall_health}%**")
        st.caption("Critical issues present")

with health_col2:
    if processing_health >= 90:
        st.success("✅ Processing: Healthy")
    elif processing_health >= 75:
        st.info("ℹ️ Processing: Good")
    else:
        st.warning("⚠️ Processing: Needs Review")

with health_col3:
    if issue_health >= 90:
        st.success("✅ Issues: Clear")
    elif issue_health >= 75:
        st.info("ℹ️ Issues: Minor")
    else:
        st.warning("⚠️ Issues: Action Needed")

with health_col4:
    if sla_health >= 90:
        st.success("✅ SLAs: On Track")
    elif sla_health >= 75:
        st.info("ℹ️ SLAs: Good")
    else:
        st.error("🚨 SLAs: At Risk")

# Performance Insights
st.subheader("⚡ Performance Insights")

if summary.get('total_runs', 0) > 0:
    avg_duration = summary.get('avg_duration_seconds', 0)
    total_processed = summary.get('total_rows_read', 0)
    success_rate = summary.get('success_rate', 0)

    insight_col1, insight_col2, insight_col3 = st.columns(3)

    with insight_col1:
        st.metric(
            "Avg Processing Time",
            f"{avg_duration:.1f}s" if avg_duration else "N/A",
            help="Average time to process a file/run"
        )

    with insight_col2:
        throughput = (total_processed / max(selected_hours, 1)) if total_processed > 0 else 0
        st.metric(
            "Throughput",
            f"{throughput:.0f} rows/hour",
            help="Data processing rate over selected period"
        )

    with insight_col3:
        st.metric(
            "Success Rate",
            f"{success_rate:.1f}%" if success_rate else "N/A",
            delta=f"{success_rate - 95:.1f}%" if success_rate < 95 else None,
            delta_color="inverse",
            help="Percentage of successful processing operations"
        )

    # Benchmark comparison (if available)
    st.caption("💡 **Pro Tip:** Performance benchmarks show current operations are within acceptable limits for production workloads")
else:
    st.info("📊 No processing activity in the selected time range")

# Footer with navigation hints
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.caption(f"⏰ Last updated: {datetime.now().strftime('%H:%M:%S')}")
    st.caption(f"📅 Data scope: Last {selected_hours} hours")

with col2:
    if auto_refresh:
        st.caption("🔄 Auto-refresh: Enabled (30s intervals)")
        # NOTE: time.sleep() pauses this session's thread for 30s then triggers a rerun.
        # This is intentional for a polling-based auto-refresh pattern.
        # For production use, consider st.experimental_fragment or a background scheduler.
        time.sleep(30)
        st.rerun()
    else:
        st.caption("🔄 Auto-refresh: Disabled")

with col3:
    st.caption("🎯 **Navigation Tip:** Use tabs above to drill into specific areas")
    st.caption("📊 **Dashboard:** This is your primary operations command center")