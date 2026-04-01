"""
Home Page — Eligibility & Accumulator Operations Command Center
================================================================
The landing page and operational dashboard for the entire simulator.
Provides system-wide health metrics, active alert summaries, navigation
to investigation pages, scenario status, and a plain-English introduction
to the project — the first thing a reviewer or demo audience sees.

Design principles
-----------------
- First impression: clean, professional, immediately informative
- Exception-first: surface problems in the key findings banner
- Navigation hub: guide users to the right investigation page
- Portfolio-grade: screenshot-ready from the moment the page loads
- Accessible: plain-language explanations anyone can follow
"""

# ═══════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════

import sys
from pathlib import Path

# Ensure repo root is on sys.path so `from src.* import ...` works
_repo_root = Path(__file__).resolve().parents[2]  # src/app/Home.py → ../.. = repo root
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import pandas as pd
import streamlit as st
import altair as alt
import sqlite3
from pathlib import Path
from datetime import datetime

from src.common.db import fetch_all
from config.settings import DB_PATH


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Eligibility & Accumulator Ops Command Center",
    page_icon="🏥",
    layout="wide",
)


# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════

OPEN_CASE_STATUSES = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"}
OPEN_ISSUE_STATUSES = {"OPEN", "ACKNOWLEDGED"}

PRIORITY_CONFIG = {
    "CRITICAL": {"icon": "🔴", "color": "#FF4B4B"},
    "HIGH":     {"icon": "🟠", "color": "#FFA500"},
    "MEDIUM":   {"icon": "🟡", "color": "#FFD700"},
    "LOW":      {"icon": "🔵", "color": "#4B9DFF"},
}

PROCESSING_COLORS = {
    "PROCESSED":        "#2ECC71",
    "SUCCESS":          "#2ECC71",
    "VALIDATED":        "#4B9DFF",
    "RECEIVED":         "#4B9DFF",
    "PENDING":          "#BDC3C7",
    "RUNNING":          "#FFD700",
    "FAILED":           "#FF4B4B",
    "REJECTED":         "#FF4B4B",
    "PARTIAL_SUCCESS":  "#FFA500",
}

SEVERITY_COLORS = {
    "CRITICAL": "#FF4B4B",
    "HIGH":     "#FFA500",
    "MEDIUM":   "#FFD700",
    "LOW":      "#4B9DFF",
}

NAV_PAGES = [
    {
        "name": "Processing Run Dashboard",
        "icon": "📊",
        "description": "Real-time processing operations, performance metrics, system health, and operational visibility",
        "use_when": "You want to monitor processing operations, check system health, or investigate processing issues (recommended starting point)",
    },
    {
        "name": "Issue Triage",
        "icon": "🎫",
        "description": "Support case queue, SLA watchlist, case investigation, and root cause analysis",
        "use_when": "You need to triage cases, track SLAs, or investigate incidents",
    },
    {
        "name": "Member Timeline",
        "icon": "👤",
        "description": "Member-specific investigation view combining eligibility, claims, accumulators, issues, and cases",
        "use_when": "You need to investigate a specific member's data and timeline for root cause analysis",
    },
    {
        "name": "Support Case Detail",
        "icon": "🎫",
        "description": "Detailed view of a support case including metadata, linked entities, SLA status, notes, and audit history",
        "use_when": "You need comprehensive case investigation and action history",
    },
    {
        "name": "File Detail",
        "icon": "📁",
        "description": "Detailed view of an inbound file including processing status, validation results, generated issues, and operational impact",
        "use_when": "You need to investigate file-level processing, validation outcomes, and downstream effects",
    },
    {
        "name": "Processing Run Detail",
        "icon": "⚙️",
        "description": "Detailed view of a processing run including execution metrics, linked files, outcomes, and operational impact",
        "use_when": "You need to investigate run-level execution, throughput, issues generated, and systemic processing context",
    },
    {
        "name": "SLA Detail",
        "icon": "⏱️",
        "description": "Detailed view of a Service Level Agreement including target, status, compliance, linked entities, and escalation history",
        "use_when": "You need to investigate SLA compliance, risk status, breach details, and operational accountability",
    },
    {
        "name": "File Monitoring",
        "icon": "📁",
        "description": "Inbound file tracking, missing file alerts, duplicate detection, processing runs",
        "use_when": "You need to check file delivery, processing outcomes, or file-related issues",
    },
    {
        "name": "Accumulator Reconciliation",
        "icon": "💰",
        "description": "OOP max breach detection, family rollup reconciliation, accumulator investigation",
        "use_when": "You need to investigate accumulator exceptions or family discrepancies",
    },
    {
        "name": "Scenario Control Center",
        "icon": "🎛️",
        "description": "Trigger deterministic support scenarios and observe downstream artifact generation",
        "use_when": "You want to inject a test incident or demo the full support workflow",
    },
]


# ── Constants ──────────────────────────────────────────────────
TTL_SECONDS = 300  # 5-minute cache


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
    if value is None:
        return fallback
    try:
        num = float(safe_val(value, 0))
        return f"{int(num):,}" if num == int(num) else f"{num:,.2f}"
    except (TypeError, ValueError):
        return fallback


def safe_col_list(desired_cols, available_cols):
    """Return only the columns that actually exist."""
    return [c for c in desired_cols if c in available_cols]


def safe_value_counts(series):
    """
    Return a two-column DataFrame from value_counts(),
    handling different pandas versions.
    """
    vc = series.value_counts().reset_index()
    if vc.shape[1] == 2:
        vc.columns = [series.name or "Value", "Count"]
    return vc


# ── Helpers ────────────────────────────────────────────────────
def get_connection():
    """Return a read-only SQLite connection."""
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


def safe_val_df(df, col, default=0):
    """Safely extract a single scalar from a 1-row DataFrame."""
    if df is None or df.empty or col not in df.columns:
        return default
    val = df.iloc[0][col]
    if pd.isna(val):
        return default
    return val


def fmt_number_new(n, prefix="", suffix=""):
    """Format a number with optional prefix/suffix."""
    if isinstance(n, float) and n == int(n):
        n = int(n)
    return f"{prefix}{n:,}{suffix}"


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS (cached, 5-minute TTL)
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner="Loading inbound files …")
def load_files():
    """Load inbound file records with client/vendor context."""
    rows = fetch_all("""
        SELECT
            f.file_id, f.file_name, f.file_type,
            c.client_code, v.vendor_code,
            f.expected_date, f.received_ts,
            f.row_count, f.processing_status,
            f.duplicate_flag, f.error_count, f.created_at
        FROM inbound_files f
        LEFT JOIN clients c ON f.client_id = c.client_id
        LEFT JOIN vendors v ON f.vendor_id  = v.vendor_id
        ORDER BY f.file_id DESC
    """)
    df = pd.DataFrame(rows)
    if not df.empty and "duplicate_flag" in df.columns:
        df["duplicate_flag"] = df["duplicate_flag"].fillna(0).astype(int)
    return df


@st.cache_data(ttl=300, show_spinner="Loading data quality issues …")
def load_issues():
    """Load all data quality issues with client/vendor context."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status,
            c.client_code, v.vendor_code,
            i.issue_description, i.detected_at
        FROM data_quality_issues i
        LEFT JOIN clients c ON i.client_id = c.client_id
        LEFT JOIN vendors v ON i.vendor_id  = v.vendor_id
        ORDER BY i.issue_id DESC
    """)
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner="Loading processing runs …")
def load_processing_runs():
    """Load processing run records."""
    rows = fetch_all("""
        SELECT
            run_id, run_type, file_id,
            started_at, completed_at, run_status,
            rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        ORDER BY run_id DESC
    """)
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner="Loading support cases …")
def load_case_summary():
    """
    Load support case summary with SLA tracking.

    SCHEMA NOTE: support_cases uses:
    - assignment_group (NOT assigned_team)
    - short_description (NOT title)
    - case_number
    """
    rows = fetch_all("""
        SELECT
            sc.case_id, sc.case_number, sc.case_type,
            sc.priority, sc.severity, sc.status,
            sc.assigned_team, sc.short_description,
            sc.opened_at,
            st.status       AS sla_status,
            st.target_hours,
            st.is_at_risk,  st.is_breached
        FROM support_cases sc
        LEFT JOIN sla_tracking st ON sc.case_id = st.case_id
        ORDER BY sc.case_id DESC
    """)
    df = pd.DataFrame(rows)
    if not df.empty:
        if "is_at_risk" in df.columns:
            df["is_at_risk"] = df["is_at_risk"].fillna(0).astype(int)
        if "is_breached" in df.columns:
            df["is_breached"] = df["is_breached"].fillna(0).astype(int)
    return df


@st.cache_data(ttl=300, show_spinner="Loading accumulator snapshots …")
def load_accumulator_summary():
    """Load accumulator snapshot summary for dashboard metrics."""
    rows = fetch_all("""
        SELECT
            s.snapshot_id, s.member_id, s.family_id,
            s.individual_oop_accum, s.family_oop_accum,
            p.individual_oop_max, p.family_oop_max,
            s.benefit_year
        FROM accumulator_snapshots s
        LEFT JOIN benefit_plans p ON s.plan_id = p.plan_id
    """)
    df = pd.DataFrame(rows)
    if not df.empty:
        df["ind_breach"] = (
            (df["individual_oop_accum"] - df["individual_oop_max"]).clip(lower=0)
        )
        df["fam_breach"] = (
            (df["family_oop_accum"] - df["family_oop_max"]).clip(lower=0)
        )
        df["has_breach"] = (df["ind_breach"] > 0) | (df["fam_breach"] > 0)
    return df


# ── Cached Data Loaders ───────────────────────────────────────
@st.cache_data(ttl=TTL_SECONDS)
def load_kpi_metrics():
    """Load headline KPI numbers for the metric cards."""
    conn = get_connection()
    try:
        files_df = pd.read_sql_query("""
            SELECT
                COUNT(*)                                               AS total_files,
                SUM(CASE WHEN processing_status = 'PROCESSED'
                         THEN 1 ELSE 0 END)                           AS processed_files,
                SUM(CASE WHEN processing_status IN ('FAILED', 'REJECTED')
                         THEN 1 ELSE 0 END)                           AS exception_files
            FROM inbound_files
        """, conn)

        cases_df = pd.read_sql_query("""
            SELECT
                COUNT(*)                                               AS total_cases,
                SUM(CASE WHEN status IN ('OPEN', 'IN_PROGRESS')
                         THEN 1 ELSE 0 END)                           AS active_cases,
                SUM(CASE WHEN priority = 'CRITICAL'
                              AND status IN ('OPEN', 'IN_PROGRESS')
                         THEN 1 ELSE 0 END)                           AS critical_cases
            FROM support_cases
        """, conn)

        sla_df = pd.read_sql_query("""
            SELECT
                COUNT(*)                                               AS total_slas,
                SUM(CASE WHEN is_breached = 1 THEN 1 ELSE 0 END)      AS breached_slas,
                SUM(CASE WHEN is_at_risk  = 1
                              AND is_breached = 0
                         THEN 1 ELSE 0 END)                           AS at_risk_slas
            FROM sla_tracking
        """, conn)

        members_df = pd.read_sql_query("""
            SELECT COUNT(DISTINCT member_id) AS total_members
            FROM eligibility_periods
            WHERE status = 'ACTIVE'
        """, conn)

        return files_df, cases_df, sla_df, members_df
    finally:
        conn.close()


@st.cache_data(ttl=TTL_SECONDS)
def load_file_status_chart_data():
    """File inventory grouped by status for the bar chart."""
    conn = get_connection()
    try:
        return pd.read_sql_query("""
            SELECT
                processing_status AS status,
                COUNT(*)    AS count
            FROM inbound_files
            GROUP BY processing_status
            ORDER BY count DESC
        """, conn)
    finally:
        conn.close()


@st.cache_data(ttl=TTL_SECONDS)
def load_case_priority_chart_data():
    """Open support cases by priority for the donut chart."""
    conn = get_connection()
    try:
        return pd.read_sql_query("""
            SELECT
                priority,
                COUNT(*) AS count
            FROM support_cases
            WHERE status IN ('OPEN', 'IN_PROGRESS')
            GROUP BY priority
            ORDER BY
                CASE priority
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH'     THEN 2
                    WHEN 'MEDIUM'   THEN 3
                    WHEN 'LOW'      THEN 4
                    ELSE 5
                END
        """, conn)
    finally:
        conn.close()


@st.cache_data(ttl=TTL_SECONDS)
def load_sla_health_chart_data():
    """SLA records by status for the stacked bar."""
    conn = get_connection()
    try:
        return pd.read_sql_query("""
            SELECT
                status      AS sla_status,
                COUNT(*)    AS count
            FROM sla_tracking
            GROUP BY status
            ORDER BY count DESC
        """, conn)
    finally:
        conn.close()


@st.cache_data(ttl=TTL_SECONDS)
def load_recent_activity():
    """Last 10 support case events for the activity feed."""
    conn = get_connection()
    try:
        return pd.read_sql_query("""
            SELECT
                case_number,
                short_description,
                priority,
                status,
                assigned_team,
                opened_at
            FROM support_cases
            ORDER BY opened_at DESC
            LIMIT 10
        """, conn)
    finally:
        conn.close()


# ── Rendering ─────────────────────────────────────────────────
def render_overview_tab():
    """Render the Overview tab content on the Home page."""

    # Key Findings Banner
    files_df, cases_df, sla_df, members_df = load_kpi_metrics()

    active_cases = safe_val_df(cases_df, "active_cases", 0)
    critical_cases = safe_val_df(cases_df, "critical_cases", 0)
    breached_slas = safe_val_df(sla_df, "breached_slas", 0)
    exception_files = safe_val_df(files_df, "exception_files", 0)

    findings = []
    if critical_cases > 0:
        findings.append(f"🔴 {critical_cases} critical case(s) require immediate attention")
    if breached_slas > 0:
        findings.append(f"⚠️ {breached_slas} SLA(s) breached")
    if exception_files > 0:
        findings.append(f"📁 {exception_files} file exception(s) detected")
    if not findings:
        findings.append("✅ All systems nominal — no active exceptions")

    st.info("  \n".join(["**Key Findings:**"] + findings))

    # Metric Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Active Members",
            fmt_number_new(safe_val_df(members_df, "total_members", 0)),
        )
    with col2:
        st.metric(
            "Files Tracked",
            fmt_number_new(safe_val_df(files_df, "total_files", 0)),
            delta=f"{exception_files} exceptions" if exception_files > 0 else None,
            delta_color="inverse" if exception_files > 0 else "off",
        )
    with col3:
        st.metric(
            "Open Cases",
            fmt_number_new(active_cases),
            delta=f"{critical_cases} critical" if critical_cases > 0 else None,
            delta_color="inverse" if critical_cases > 0 else "off",
        )
    with col4:
        st.metric(
            "SLA Health",
            f"{safe_val_df(sla_df, 'total_slas', 0) - breached_slas}/{safe_val_df(sla_df, 'total_slas', 0)}",
            delta=f"{breached_slas} breached" if breached_slas > 0 else "All on track",
            delta_color="inverse" if breached_slas > 0 else "normal",
        )

    st.divider()

    # Charts Row
    chart_col1, chart_col2, chart_col3 = st.columns(3)

    # File Status Chart
    with chart_col1:
        st.subheader("File Status")
        file_chart_df = load_file_status_chart_data()
        if not file_chart_df.empty:
            color_scale = alt.Scale(
                domain=["PROCESSED", "RECEIVED", "VALIDATED", "REJECTED", "FAILED"],
                range=["#2ecc71", "#3498db", "#9b59b6", "#e74c3c", "#c0392b"],
            )
            chart = (
                alt.Chart(file_chart_df)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("status:N", sort="-y", title="Status"),
                    y=alt.Y("count:Q", title="Files"),
                    color=alt.Color("status:N", scale=color_scale, legend=None),
                    tooltip=["status", "count"],
                )
                .properties(height=250)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No file data yet — run a scenario to generate files.")

    # Case Priority Chart
    with chart_col2:
        st.subheader("Open Cases by Priority")
        case_chart_df = load_case_priority_chart_data()
        if not case_chart_df.empty:
            priority_colors = alt.Scale(
                domain=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                range=["#c0392b", "#e74c3c", "#f39c12", "#2ecc71"],
            )
            chart = (
                alt.Chart(case_chart_df)
                .mark_arc(innerRadius=50)
                .encode(
                    theta=alt.Theta("count:Q"),
                    color=alt.Color("priority:N", scale=priority_colors),
                    tooltip=["priority", "count"],
                )
                .properties(height=250)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No open cases — system is clean.")

    # SLA Health Chart
    with chart_col3:
        st.subheader("SLA Health")
        sla_chart_df = load_sla_health_chart_data()
        if not sla_chart_df.empty:
            sla_colors = alt.Scale(
                domain=["MET", "OPEN", "AT_RISK", "BREACHED"],
                range=["#2ecc71", "#3498db", "#f39c12", "#c0392b"],
            )
            chart = (
                alt.Chart(sla_chart_df)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("sla_status:N", sort="-y", title="SLA Status"),
                    y=alt.Y("count:Q", title="Count"),
                    color=alt.Color("sla_status:N", scale=sla_colors, legend=None),
                    tooltip=["sla_status", "count"],
                )
                .properties(height=250)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No SLA records yet — run a scenario to generate SLAs.")

    st.divider()

    # Recent Activity Feed
    st.subheader("Recent Activity")
    activity_df = load_recent_activity()
    if not activity_df.empty:
        st.dataframe(
            activity_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "case_number": st.column_config.TextColumn("Case #", width="small"),
                "short_description": st.column_config.TextColumn("Description", width="large"),
                "priority": st.column_config.TextColumn("Priority", width="small"),
                "status": st.column_config.TextColumn("Status", width="small"),
                "assigned_team": st.column_config.TextColumn("Team", width="medium"),
                "opened_at": st.column_config.TextColumn("Opened", width="medium"),
            },
        )
        csv = activity_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download Activity Feed",
            csv,
            "recent_activity.csv",
            "text/csv",
            key="download_activity",
        )
    else:
        st.caption("No support case activity yet — run a scenario to see case events here.")

    # System Timestamp
    st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ═══════════════════════════════════════════════════════════════════════
# LOAD ALL DATA
# ═══════════════════════════════════════════════════════════════════════

files_df    = load_files()
issues_df   = load_issues()
runs_df     = load_processing_runs()
cases_df    = load_case_summary()
accum_df    = load_accumulator_summary()


# ═══════════════════════════════════════════════════════════════════════
# COMPUTE METRICS
# ═══════════════════════════════════════════════════════════════════════

total_files = len(files_df)
processed_files = (
    int(files_df["processing_status"].isin(["PROCESSED", "SUCCESS"]).sum())
    if not files_df.empty else 0
)
failed_files = (
    int(files_df["processing_status"].isin(["FAILED", "REJECTED"]).sum())
    if not files_df.empty else 0
)
duplicate_files = (
    int((files_df["duplicate_flag"] == 1).sum())
    if not files_df.empty else 0
)
files_with_errors = (
    int((files_df["error_count"].fillna(0) > 0).sum())
    if not files_df.empty else 0
)

total_issues = len(issues_df)
open_issues = (
    int(issues_df["status"].isin(OPEN_ISSUE_STATUSES).sum())
    if not issues_df.empty else 0
)
critical_issues = (
    int((issues_df["severity"] == "CRITICAL").sum())
    if not issues_df.empty else 0
)
high_critical_issues = (
    int(issues_df["severity"].isin(["HIGH", "CRITICAL"]).sum())
    if not issues_df.empty else 0
)

total_cases = len(cases_df)
open_cases = (
    int(cases_df["status"].isin(OPEN_CASE_STATUSES).sum())
    if not cases_df.empty else 0
)
breached_slas = (
    int((cases_df["is_breached"] == 1).sum())
    if not cases_df.empty else 0
)
at_risk_slas = (
    int((cases_df["is_at_risk"] == 1).sum())
    if not cases_df.empty else 0
)
resolved_cases = (
    int((cases_df["status"] == "RESOLVED").sum())
    if not cases_df.empty else 0
)

total_runs = len(runs_df)
failed_runs = (
    int(runs_df["run_status"].isin(["FAILED", "PARTIAL_SUCCESS"]).sum())
    if not runs_df.empty else 0
)

oop_breaches = (
    int(accum_df["has_breach"].sum())
    if not accum_df.empty and "has_breach" in accum_df.columns else 0
)
total_members_tracked = (
    int(accum_df["member_id"].nunique())
    if not accum_df.empty else 0
)


# ═══════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.header("🔄 Page Actions")
    if st.button("🔄 Refresh All Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.header("📍 Quick Navigation")
    for page in NAV_PAGES:
        st.markdown(f"{page['icon']} **{page['name']}**")
        st.caption(page["use_when"])

    st.divider()
    st.header("📊 System Totals")
    st.markdown(f"**Files:** {fmt_number(total_files)}")
    st.markdown(f"**Issues:** {fmt_number(total_issues)}")
    st.markdown(f"**Cases:** {fmt_number(total_cases)}")
    st.markdown(f"**Runs:** {fmt_number(total_runs)}")
    st.markdown(f"**Members Tracked:** {fmt_number(total_members_tracked)}")


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("🏥 Eligibility & Accumulator Operations Command Center")
st.warning("**Disclaimer:** This is a simulated portfolio project using synthetic data only. No real PII, PHI, or sensitive information is included.")
st.caption(
    "Healthcare data operations simulator focused on eligibility, accumulators, "
    "file monitoring, issue triage, and production support workflows. "
    "Built to demonstrate real-world operational skills at zero cost."
)

# ── Key Findings Auto-Summary ──
findings = []
if breached_slas > 0:
    findings.append(f"⛔ **{breached_slas}** SLA(s) breached")
if at_risk_slas > 0:
    findings.append(f"⚠️ **{at_risk_slas}** SLA(s) at risk")
if critical_issues > 0:
    findings.append(f"🔴 **{critical_issues}** CRITICAL issue(s)")
if open_cases > 0:
    findings.append(f"📂 **{open_cases}** open support case(s)")
if oop_breaches > 0:
    findings.append(f"💰 **{oop_breaches}** OOP max breach(es)")
if duplicate_files > 0:
    findings.append(f"🟣 **{duplicate_files}** duplicate file(s)")
if failed_runs > 0:
    findings.append(f"🟠 **{failed_runs}** non-success run(s)")

if findings:
    if breached_slas > 0:
        st.error("**System Alerts:**  " + "  ·  ".join(findings))
    else:
        st.warning("**System Alerts:**  " + "  ·  ".join(findings))
elif total_issues > 0:
    st.success(
        "✅ **System healthy.** All scenarios processed — no active SLA concerns."
    )
else:
    st.info(
        "🏥 **Ready to launch.** No data yet. Visit the **Scenario Control "
        "Center** to trigger your first support scenario."
    )

# ── Top-level metrics — two rows ──
st.markdown("### 📊 Operational Dashboard")

row1_c1, row1_c2, row1_c3, row1_c4, row1_c5, row1_c6 = st.columns(6)
row1_c1.metric("📁 Inbound Files", fmt_number(total_files))
row1_c2.metric("✅ Processed", fmt_number(processed_files))
row1_c3.metric("🔴 Failed", fmt_number(failed_files))
row1_c4.metric("🟣 Duplicates", fmt_number(duplicate_files))
row1_c5.metric("🐛 Open Issues", fmt_number(open_issues))
row1_c6.metric("🔴 Critical Issues", fmt_number(critical_issues))

row2_c1, row2_c2, row2_c3, row2_c4, row2_c5, row2_c6 = st.columns(6)
row2_c1.metric("🎫 Open Cases", fmt_number(open_cases))
row2_c2.metric("⛔ Breached SLAs", fmt_number(breached_slas))
row2_c3.metric("⚠️ At-Risk SLAs", fmt_number(at_risk_slas))
row2_c4.metric("✅ Resolved Cases", fmt_number(resolved_cases))
row2_c5.metric("💰 OOP Breaches", fmt_number(oop_breaches))
row2_c6.metric("👥 Members Tracked", fmt_number(total_members_tracked))

st.divider()


# ═══════════════════════════════════════════════════════════════════════
# TABBED LAYOUT
# ═══════════════════════════════════════════════════════════════════════

tab_overview, tab_files, tab_cases, tab_activity, tab_howto = st.tabs([
    "📊 System Overview",
    "📁 File & Processing Health",
    "🎫 Case & SLA Status",
    "📋 Recent Activity",
    "❓ About This Project",
])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1 — SYSTEM OVERVIEW
# ═══════════════════════════════════════════════════════════════════════

with tab_overview:
    render_overview_tab()


# ═══════════════════════════════════════════════════════════════════════
# TAB 2 — FILE & PROCESSING HEALTH
# ═══════════════════════════════════════════════════════════════════════

with tab_files:
    st.subheader("📁 File & Processing Health")
    st.caption(
        "Overview of inbound file delivery and processing outcomes. "
        "For detailed investigation, visit the File Monitoring page."
    )

    if files_df.empty and runs_df.empty:
        st.info("No file or processing data yet. Run a scenario to populate.")
    else:
        # ── File metrics ──
        f1, f2, f3, f4, f5 = st.columns(5)
        f1.metric("Total Files", fmt_number(total_files))
        f2.metric("Processed", fmt_number(processed_files))
        f3.metric("Failed", fmt_number(failed_files))
        f4.metric("Duplicates", fmt_number(duplicate_files))
        f5.metric("With Errors", fmt_number(files_with_errors))

        # ── File type breakdown ──
        if not files_df.empty and "file_type" in files_df.columns:
            ft_left, ft_right = st.columns(2)

            with ft_left:
                st.markdown("#### Files by Type")
                ft_counts = safe_value_counts(files_df["file_type"])
                ft_chart = (
                    alt.Chart(ft_counts)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X(f"{ft_counts.columns[0]}:N", title=None),
                        y=alt.Y("Count:Q", title="Files"),
                        color=alt.value("#4B9DFF"),
                        tooltip=[ft_counts.columns[0], "Count"],
                    )
                    .properties(height=220)
                )
                st.altair_chart(ft_chart, use_container_width=True)

            with ft_right:
                st.markdown("#### Recent Files")
                file_cols = [
                    "file_id", "file_name", "file_type",
                    "client_code", "vendor_code", "row_count",
                    "processing_status", "duplicate_flag",
                    "error_count", "received_ts",
                ]
                st.dataframe(
                    files_df[safe_col_list(file_cols, files_df.columns)].head(15),
                    use_container_width=True,
                    hide_index=True,
                )

        # ── Processing runs ──
        if not runs_df.empty:
            st.divider()
            st.markdown("#### ⚙️ Processing Runs")

            pr1, pr2, pr3 = st.columns(3)
            pr1.metric("Total Runs", fmt_number(total_runs))

            success_runs = (
                int((runs_df["run_status"] == "SUCCESS").sum())
                if "run_status" in runs_df.columns else 0
            )
            pr2.metric("✅ Successful", fmt_number(success_runs))
            pr3.metric("🔴 Non-Success", fmt_number(failed_runs))

            run_cols = [
                "run_id", "run_type", "file_id", "run_status",
                "rows_read", "rows_passed", "rows_failed",
                "issue_count", "started_at",
            ]
            st.dataframe(
                runs_df[safe_col_list(run_cols, runs_df.columns)].head(15),
                use_container_width=True,
                hide_index=True,
            )


# ═══════════════════════════════════════════════════════════════════════
# TAB 3 — CASE & SLA STATUS
# ═══════════════════════════════════════════════════════════════════════

with tab_cases:
    st.subheader("🎫 Case & SLA Status")
    st.caption(
        "Overview of support cases and SLA compliance. "
        "For detailed triage, visit the Issue Triage page."
    )

    if cases_df.empty:
        st.info("No support cases yet. Run a scenario to generate cases.")
    else:
        # ── Case metrics ──
        cs1, cs2, cs3, cs4, cs5 = st.columns(5)
        cs1.metric("Total Cases", fmt_number(total_cases))
        cs2.metric("Open", fmt_number(open_cases))
        cs3.metric("Resolved", fmt_number(resolved_cases))
        cs4.metric("⛔ Breached", fmt_number(breached_slas))
        cs5.metric("⚠️ At Risk", fmt_number(at_risk_slas))

        # ── Case mix charts ──
        cm_left, cm_right = st.columns(2)

        with cm_left:
            st.markdown("#### Cases by Type")
            if "case_type" in cases_df.columns:
                ct_counts = safe_value_counts(cases_df["case_type"])
                ct_chart = (
                    alt.Chart(ct_counts)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X("Count:Q", title="Count"),
                        y=alt.Y(f"{ct_counts.columns[0]}:N", sort="-x", title=None),
                        color=alt.value("#9B59B6"),
                        tooltip=[ct_counts.columns[0], "Count"],
                    )
                    .properties(height=220)
                )
                st.altair_chart(ct_chart, use_container_width=True)

        with cm_right:
            st.markdown("#### Cases by Queue")
            if "assigned_team" in cases_df.columns:
                aq_counts = safe_value_counts(cases_df["assigned_team"])
                aq_chart = (
                    alt.Chart(aq_counts)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X("Count:Q", title="Count"),
                        y=alt.Y(f"{aq_counts.columns[0]}:N", sort="-x", title=None),
                        color=alt.value("#3498DB"),
                        tooltip=[aq_counts.columns[0], "Count"],
                    )
                    .properties(height=220)
                )
                st.altair_chart(aq_chart, use_container_width=True)

        # ── Case table ──
        st.markdown("#### Recent Cases")
        case_cols = [
            "case_id", "case_number", "case_type", "priority",
            "status",             "assigned_team", "short_description",
            "is_at_risk", "is_breached", "opened_at",
        ]
        st.dataframe(
            cases_df[safe_col_list(case_cols, cases_df.columns)].head(20),
            use_container_width=True,
            hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 4 — RECENT ACTIVITY
# ═══════════════════════════════════════════════════════════════════════

with tab_activity:
    st.subheader("📋 Recent Activity")
    st.caption(
        "Latest issues, cases, and files across the entire system. "
        "A quick pulse-check of what's happening."
    )

    act_left, act_right = st.columns(2)

    with act_left:
        st.markdown("#### 🐛 Latest Data Quality Issues")
        if issues_df.empty:
            st.info("No issues yet.")
        else:
            issue_cols = [
                "issue_id", "issue_code", "issue_type",
                "severity", "status",
                "client_code", "vendor_code",
                "issue_description", "detected_at",
            ]
            st.dataframe(
                issues_df[safe_col_list(issue_cols, issues_df.columns)].head(15),
                use_container_width=True,
                hide_index=True,
            )

    with act_right:
        st.markdown("#### 🎫 Latest Support Cases")
        if cases_df.empty:
            st.info("No cases yet.")
        else:
            case_cols = [
                "case_id", "case_number", "case_type",
                "priority", "status", "assigned_team",
                "short_description", "opened_at",
            ]
            st.dataframe(
                cases_df[safe_col_list(case_cols, cases_df.columns)].head(15),
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    act2_left, act2_right = st.columns(2)

    with act2_left:
        st.markdown("#### 📁 Latest Files")
        if files_df.empty:
            st.info("No files yet.")
        else:
            file_cols = [
                "file_id", "file_name", "file_type",
                "client_code", "vendor_code",
                "processing_status", "received_ts",
            ]
            st.dataframe(
                files_df[safe_col_list(file_cols, files_df.columns)].head(15),
                use_container_width=True,
                hide_index=True,
            )

    with act2_right:
        st.markdown("#### ⚙️ Latest Processing Runs")
        if runs_df.empty:
            st.info("No runs yet.")
        else:
            run_cols = [
                "run_id", "run_type", "file_id",
                "run_status", "rows_read", "rows_passed",
                "rows_failed", "started_at",
            ]
            st.dataframe(
                runs_df[safe_col_list(run_cols, runs_df.columns)].head(15),
                use_container_width=True,
                hide_index=True,
            )

    # ── Download all recent data ──
    st.divider()
    dl1, dl2, dl3, dl4 = st.columns(4)
    with dl1:
        if not files_df.empty:
            st.download_button(
                "⬇️ Files CSV",
                files_df.to_csv(index=False),
                "inbound_files.csv", "text/csv",
                use_container_width=True,
            )
    with dl2:
        if not issues_df.empty:
            st.download_button(
                "⬇️ Issues CSV",
                issues_df.to_csv(index=False),
                "issues.csv", "text/csv",
                use_container_width=True,
            )
    with dl3:
        if not cases_df.empty:
            st.download_button(
                "⬇️ Cases CSV",
                cases_df.to_csv(index=False),
                "cases.csv", "text/csv",
                use_container_width=True,
            )
    with dl4:
        if not runs_df.empty:
            st.download_button(
                "⬇️ Runs CSV",
                runs_df.to_csv(index=False),
                "runs.csv", "text/csv",
                use_container_width=True,
            )


# ═══════════════════════════════════════════════════════════════════════
# TAB 5 — ABOUT THIS PROJECT
# ═══════════════════════════════════════════════════════════════════════

with tab_howto:
    st.subheader("❓ About This Project")
    st.caption(
        "What this simulator is, why it exists, what it demonstrates, "
        "and how to use it — written so anyone can understand."
    )

    st.markdown(r"""
---

### 🏥 What Is This?

This is the **Eligibility & Accumulator Operations Command Center** — a
simulator that recreates what it's like to support healthcare data pipelines
in production.

Every major health plan, pharmacy benefit manager, and claims processor runs
**nightly data pipelines** that determine:
- **Who has insurance** (eligibility)
- **How much they've paid** toward their deductible and out-of-pocket maximum (accumulators)
- **Whether a claim should be paid** (claims processing)

When these pipelines break — a file arrives late, a member loads twice, a
deductible overstates — **real people are affected at the pharmacy counter
and the doctor's office.**

This simulator recreates that entire operational surface so the skills can
be demonstrated without real patient data or enterprise infrastructure.

---

### 🎯 What Does It Demonstrate?

| Skill | How It Appears Here |
|---|---|
| **Healthcare data pipelines** | File generation, intake, validation, processing |
| **Eligibility domain** | Member enrollment, coverage dates, plan relationships |
| **Accumulator logic** | Deductible/OOP tracking, family rollup, plan thresholds |
| **Production support** | Issue detection, case routing, SLA management |
| **SQL root cause analysis** | Diagnostic queries per scenario |
| **File monitoring** | Delivery tracking, duplicate detection, missing alerts |
| **Incident triage** | Priority routing, SLA enforcement, escalation |
| **Documentation** | Runbooks, SQL playbooks, scenario catalog |

---

### 🎛️ The 5 Core Scenarios

| # | Scenario | Priority | SLA | What Happens |
|---|---|---|---|---|
| 1 | 🔴 Missing Inbound File | CRITICAL | 4h | Expected file never arrives |
| 2 | 🟣 Duplicate Eligibility Resend | MEDIUM | 24h | Same file arrives twice |
| 3 | 🟠 Claim for Ineligible Member | HIGH | 8h | Claim for someone without coverage |
| 4 | 🔴 Accumulator Exceeds OOP Max | HIGH | 8h | Spending counter goes past limit |
| 5 | 🔵 Family Rollup Discrepancy | MEDIUM | 24h | Family total ≠ sum of members |

---

### 📋 The Pages

| Page | Purpose |
|---|---|
| **🏥 Home** (you are here) | System dashboard and navigation hub |
| **🎫 Issue Triage** | Support case queue, SLA watchlist, case investigation |
| **📁 File Monitoring** | File delivery tracking, processing runs, duplicate detection |
| **💰 Accumulator Reconciliation** | OOP breach detection, family rollup, member investigation |
| **🎛️ Scenario Control Center** | Trigger scenarios and observe artifact generation |

---

### 🚀 How to Demo (5 Minutes)

1. **Start on this Home page** — show the operational dashboard
2. **Go to Scenario Control Center** — trigger `ACCUMULATOR_EXCEEDS_OOP_MAX`
3. **Go to Accumulator Reconciliation** — find the OOP breach, investigate the member
4. **Go to Issue Triage** — show the support case and SLA watchlist
5. **Return to Scenario Control Center** — trigger `MISSING_INBOUND_FILE`
6. **Go to File Monitoring** — show the missing file alert

Every click shows real data, real routing, real SLA math.

---

### 💻 Technical Stack

| Component | Technology |
|---|---|
| **Language** | Python 3.9+ |
| **Database** | SQLite (zero-cost, portable) |
| **UI** | Streamlit with Altair charts |
| **Testing** | pytest (40+ tests passing) |
| **Cost** | \$0 — no cloud, no APIs, no licenses |

---

### 📄 Documentation

- **README.md** — Project overview and setup instructions
- **docs/scenario_catalog.md** — All 5 scenarios with full details
- **docs/runbooks/** — Step-by-step incident response per scenario
- **docs/sql_playbooks/** — Diagnostic SQL queries per scenario
    """)

    st.info(
        "💡 **First time here?** Start by visiting the **Scenario Control "
        "Center** (🎛️) in the sidebar to trigger your first support scenario. "
        "Then come back to this dashboard to see the metrics update."
    )


# ═══════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════

st.divider()
st.caption(
    "Eligibility & Accumulator Operations Command Center · "
    "Data is simulated — no real PHI · "
    "Built for portfolio demonstration"
)