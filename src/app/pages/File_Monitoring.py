"""
File Monitoring Page
====================
Operational hub for tracking inbound file delivery, detecting missing
files, flagging duplicate resends, monitoring processing outcomes, and
managing file-related support cases — the front door of every
healthcare data pipeline.

Design principles
-----------------
- Exception-first: surface missing, duplicate, and failed files before normal ones
- Investigation-oriented: guide users from alert → file → run → issue → case
- Schema-accurate: every query matches the confirmed database schema
- Accessible: plain-language explanations anyone can follow
- Interactive: charts, downloads, tabs, expandable guidance, column formatting
"""

# ═══════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import streamlit as st
import altair as alt

from src.common.db import fetch_all
from src.app.utils import to_dataframe, add_age_hours_column, bool_flag_to_label


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="File Monitoring",
    page_icon="📁",
    layout="wide",
)


# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════

# Statuses that mean "still needs attention"
OPEN_CASE_STATUSES = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"}
OPEN_ISSUE_STATUSES = {"OPEN", "ACKNOWLEDGED"}
NON_SUCCESS_RUN_STATUSES = {"FAILED", "PARTIAL_SUCCESS", "RUNNING"}

# Visual configuration for each exception category
EXCEPTION_CONFIG = {
    "DUPLICATE / RESEND":   {"icon": "🟣", "rank": 1, "color": "#9B59B6"},
    "PROCESSING FAILURE":   {"icon": "🔴", "rank": 2, "color": "#FF4B4B"},
    "FILE ERRORS":          {"icon": "🟠", "rank": 3, "color": "#FFA500"},
    "OPEN FILE ISSUE":      {"icon": "🟡", "rank": 4, "color": "#FFD700"},
    "IN PROGRESS":          {"icon": "🔵", "rank": 5, "color": "#4B9DFF"},
    "REVIEW":               {"icon": "⚪", "rank": 6, "color": "#BDC3C7"},
    "NORMAL":               {"icon": "🟢", "rank": 7, "color": "#2ECC71"},
}

PROCESSING_STATUS_ICONS = {
    "PROCESSED":  "✅",
    "SUCCESS":    "✅",
    "VALIDATED":  "🔵",
    "RECEIVED":   "🔵",
    "RUNNING":    "⏳",
    "PENDING":    "⏳",
    "FAILED":     "🔴",
    "REJECTED":   "🔴",
    "PARTIAL_SUCCESS": "🟠",
}

SEVERITY_ICONS = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🔵"}

RUN_STATUS_COLORS = {
    "SUCCESS": "#2ECC71",
    "FAILED": "#FF4B4B",
    "PARTIAL_SUCCESS": "#FFA500",
    "RUNNING": "#4B9DFF",
}


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


def safe_col_list(desired_cols, available_cols):
    """Return only the columns that actually exist."""
    return [c for c in desired_cols if c in available_cols]


def exception_badge(exception_type):
    """Return an icon + label string for an exception type."""
    cfg = EXCEPTION_CONFIG.get(exception_type, {"icon": "⚪"})
    return f"{cfg['icon']} {exception_type}"


def status_badge(status):
    """Return an icon + label string for a processing status."""
    icon = PROCESSING_STATUS_ICONS.get(str(status).upper(), "⚪") if status else "⚪"
    return f"{icon} {safe_text(status)}"


def severity_icon(sev):
    """Return a severity icon."""
    return SEVERITY_ICONS.get(str(sev).upper(), "⚪") if sev else "⚪"


def classify_file_exception(row):
    """
    Classify each file into exactly one exception category.
    Highest-priority match wins.
    """
    duplicate_flag = int(safe_val(row.get("duplicate_flag_raw"), 0))
    error_count = int(safe_val(row.get("error_count"), 0))
    processing_status = str(safe_val(row.get("processing_status"), "")).upper()
    open_issue_count = int(safe_val(row.get("open_issue_count"), 0))

    if duplicate_flag == 1:
        return "DUPLICATE / RESEND"
    if processing_status in {"FAILED", "REJECTED"}:
        return "PROCESSING FAILURE"
    if error_count > 0:
        return "FILE ERRORS"
    if open_issue_count > 0:
        return "OPEN FILE ISSUE"
    if processing_status in {"RECEIVED", "VALIDATED", "RUNNING", "PENDING"}:
        return "IN PROGRESS"
    if processing_status in {"PROCESSED", "SUCCESS"}:
        return "NORMAL"
    return "REVIEW"


def build_file_label(row):
    """Human-readable label for the file investigation dropdown."""
    exc = row.get("monitoring_exception", "REVIEW")
    icon = EXCEPTION_CONFIG.get(exc, {}).get("icon", "⚪")
    return (
        f"{icon} File {safe_text(row.get('file_id'))} · "
        f"{safe_text(row.get('file_type'))} · "
        f"{safe_text(row.get('client_code'))} / {safe_text(row.get('vendor_code'))} · "
        f"{safe_text(row.get('file_name'))}"
    )


def compute_pass_rate(rows_passed, rows_read):
    """Calculate pass rate as a percentage, guarded against division by zero."""
    r = float(safe_val(rows_read, 0))
    p = float(safe_val(rows_passed, 0))
    if r <= 0:
        return 0.0
    return round((p / r) * 100, 1)


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS (cached, 5-minute TTL)
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner="Loading inbound files …")
def load_files():
    """Load all registered inbound files with client/vendor context."""
    rows = fetch_all("""
        SELECT
            f.file_id, f.file_name, f.file_type,
            f.client_id, c.client_code,
            f.vendor_id, v.vendor_code,
            f.expected_date, f.received_ts,
            f.file_hash, f.row_count,
            f.processing_status, f.duplicate_flag, f.error_count,
            f.landing_path, f.archived_path, f.created_at
        FROM inbound_files f
        LEFT JOIN clients c ON f.client_id = c.client_id
        LEFT JOIN vendors v ON f.vendor_id  = v.vendor_id
        ORDER BY COALESCE(f.received_ts, f.created_at) DESC, f.file_id DESC
    """)
    df = to_dataframe(rows)
    if df.empty:
        return df

    df = add_age_hours_column(df, "created_at", "file_record_age_hours")

    # Clean duplicate flag for safe numeric operations
    df["duplicate_flag_raw"] = df["duplicate_flag"].fillna(0).astype(int)
    df["duplicate_flag_label"] = df["duplicate_flag_raw"].map(
        {1: "⚠️ Yes", 0: "✅ No"}
    )

    # Processing status badges
    df["status_badge"] = df["processing_status"].apply(status_badge)

    return df


@st.cache_data(ttl=300, show_spinner="Loading processing runs …")
def load_runs():
    """Load all processing run records."""
    rows = fetch_all("""
        SELECT
            pr.run_id, pr.run_type, pr.file_id,
            pr.started_at, pr.completed_at, pr.run_status,
            pr.rows_read, pr.rows_passed, pr.rows_failed,
            pr.issue_count, pr.notes
        FROM processing_runs pr
        ORDER BY pr.started_at DESC, pr.run_id DESC
    """)
    df = to_dataframe(rows)
    if df.empty:
        return df

    df = add_age_hours_column(df, "started_at", "run_age_hours")

    # Calculate pass rate for each run
    df["pass_rate_pct"] = df.apply(
        lambda r: compute_pass_rate(r.get("rows_passed"), r.get("rows_read")),
        axis=1,
    )

    return df


@st.cache_data(ttl=300, show_spinner="Loading file-related issues …")
def load_file_issues():
    """Load data quality issues linked to files or representing missing files."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status,
            i.client_id, c.client_code,
            i.vendor_id, v.vendor_code,
            i.file_id, i.run_id,
            i.entity_name, i.entity_key,
            i.issue_message, i.issue_description, i.detected_at
        FROM data_quality_issues i
        LEFT JOIN clients c ON i.client_id = c.client_id
        LEFT JOIN vendors v ON i.vendor_id  = v.vendor_id
        WHERE i.file_id IS NOT NULL
           OR i.issue_code = 'MISSING_INBOUND_FILE'
        ORDER BY i.detected_at DESC, i.issue_id DESC
    """)
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "detected_at", "issue_age_hours")
    return df


@st.cache_data(ttl=300, show_spinner="Loading file support cases …")
def load_file_cases():
    """Load support cases linked to files, with SLA tracking data."""
    rows = fetch_all("""
        SELECT
            sc.case_id, sc.case_number, sc.issue_id,
            sc.client_id, c.client_code,
            sc.vendor_id, v.vendor_code,
            sc.file_id,
            sc.run_id AS processing_run_id,
            sc.case_type, sc.priority, sc.severity, sc.status,
            sc.assigned_team AS assignment_group,
            sc.assigned_to,
            sc.short_description, sc.description,
            sc.root_cause_category, sc.escalation_level,
            sc.opened_at, sc.acknowledged_at,
            sc.resolved_at, sc.closed_at,
            sc.updated_at AS last_updated_at,
            st.sla_id, st.sla_type, st.target_hours,
            st.target_due_at,
            st.status AS sla_status,
            st.is_at_risk, st.is_breached,
            st.breached_at, st.last_evaluated_at
        FROM support_cases sc
        LEFT JOIN clients c ON sc.client_id = c.client_id
        LEFT JOIN vendors v ON sc.vendor_id = v.vendor_id
        LEFT JOIN sla_tracking st ON sc.case_id = st.case_id
        WHERE sc.file_id IS NOT NULL
           OR sc.assigned_team = 'ops_file_queue'
        ORDER BY sc.opened_at DESC, sc.case_id DESC
    """)
    df = to_dataframe(rows)
    if df.empty:
        return df

    df = add_age_hours_column(df, "opened_at", "case_age_hours")
    if "is_at_risk" in df.columns:
        df["is_at_risk_label"] = df["is_at_risk"].apply(bool_flag_to_label)
    if "is_breached" in df.columns:
        df["is_breached_label"] = df["is_breached"].apply(bool_flag_to_label)

    return df


@st.cache_data(ttl=300, show_spinner="Loading missing file alerts …")
def load_missing_file_issues():
    """Load issues specifically for expected files that never arrived."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status,
            i.client_id, c.client_code,
            i.vendor_id, v.vendor_code,
            i.file_id,
            i.issue_message, i.issue_description, i.detected_at
        FROM data_quality_issues i
        LEFT JOIN clients c ON i.client_id = c.client_id
        LEFT JOIN vendors v ON i.vendor_id  = v.vendor_id
        WHERE i.issue_code = 'MISSING_INBOUND_FILE'
        ORDER BY i.detected_at DESC, i.issue_id DESC
    """)
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "detected_at", "issue_age_hours")
    return df


@st.cache_data(ttl=300, show_spinner="Loading duplicate file alerts …")
def load_duplicate_issues():
    """Load issues for files flagged as duplicate resends."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status,
            i.client_id, c.client_code,
            i.vendor_id, v.vendor_code,
            i.file_id,
            i.issue_message, i.issue_description, i.detected_at
        FROM data_quality_issues i
        LEFT JOIN clients c ON i.client_id = c.client_id
        LEFT JOIN vendors v ON i.vendor_id  = v.vendor_id
        WHERE i.issue_code = 'DUPLICATE_ELIGIBILITY_RESEND'
           OR i.issue_subtype = 'DUPLICATE_FILE'
        ORDER BY i.detected_at DESC, i.issue_id DESC
    """)
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "detected_at", "issue_age_hours")
    return df


# ═══════════════════════════════════════════════════════════════════════
# LOAD ALL DATA
# ═══════════════════════════════════════════════════════════════════════

files_df       = load_files()
runs_df        = load_runs()
file_issues_df = load_file_issues()
file_cases_df  = load_file_cases()
missing_df     = load_missing_file_issues()
dup_issue_df   = load_duplicate_issues()


# ═══════════════════════════════════════════════════════════════════════
# BUILD ENRICHED MONITORING DATAFRAME
# ═══════════════════════════════════════════════════════════════════════

def build_monitor_dataframe(f_df, runs, issues, cases):
    """
    Merge files with run counts, issue counts, and case counts
    to produce the master file monitoring dataframe.
    """
    if f_df.empty:
        return pd.DataFrame()

    monitor = f_df.copy()

    # --- issue counts per file ---
    if not issues.empty:
        valid_issues = issues[issues["file_id"].notna()]
        if not valid_issues.empty:
            issue_counts = (
                valid_issues.groupby("file_id")
                .agg(
                    file_issue_count=("issue_id", "count"),
                    open_issue_count=(
                        "status",
                        lambda s: int(s.isin(OPEN_ISSUE_STATUSES).sum()),
                    ),
                )
                .reset_index()
            )
            monitor = monitor.merge(issue_counts, on="file_id", how="left")
        else:
            monitor["file_issue_count"] = 0
            monitor["open_issue_count"] = 0
    else:
        monitor["file_issue_count"] = 0
        monitor["open_issue_count"] = 0

    # --- case counts per file ---
    if not cases.empty:
        valid_cases = cases[cases["file_id"].notna()]
        if not valid_cases.empty:
            case_counts = (
                valid_cases.groupby("file_id")
                .agg(
                    file_case_count=("case_id", "count"),
                    open_case_count=(
                        "status",
                        lambda s: int(s.isin(OPEN_CASE_STATUSES).sum()),
                    ),
                )
                .reset_index()
            )
            monitor = monitor.merge(case_counts, on="file_id", how="left")
        else:
            monitor["file_case_count"] = 0
            monitor["open_case_count"] = 0
    else:
        monitor["file_case_count"] = 0
        monitor["open_case_count"] = 0

    # --- run summary per file ---
    if not runs.empty:
        run_summary = (
            runs.groupby("file_id")
            .agg(
                run_count=("run_id", "count"),
                last_run_status=("run_status", "first"),
                total_run_issues=("issue_count", "sum"),
                best_pass_rate=("pass_rate_pct", "max"),
            )
            .reset_index()
        )
        monitor = monitor.merge(run_summary, on="file_id", how="left")
    else:
        monitor["run_count"] = 0
        monitor["last_run_status"] = None
        monitor["total_run_issues"] = 0
        monitor["best_pass_rate"] = None

    # --- fill NaN counts ---
    int_fill_cols = [
        "file_issue_count", "open_issue_count",
        "file_case_count", "open_case_count",
        "run_count", "total_run_issues",
    ]
    for col in int_fill_cols:
        if col in monitor.columns:
            monitor[col] = monitor[col].fillna(0).astype(int)

    # --- classify exceptions ---
    monitor["monitoring_exception"] = monitor.apply(classify_file_exception, axis=1)
    monitor["exception_badge"] = monitor["monitoring_exception"].apply(exception_badge)

    # --- sort: worst exceptions first ---
    monitor["exception_rank"] = monitor["monitoring_exception"].map(
        {k: v["rank"] for k, v in EXCEPTION_CONFIG.items()}
    ).fillna(99)

    monitor = monitor.sort_values(
        by=[
            "exception_rank", "open_case_count", "open_issue_count",
            "error_count", "received_ts", "file_id",
        ],
        ascending=[True, False, False, False, False, False],
    )

    return monitor


monitor_df = build_monitor_dataframe(
    files_df, runs_df, file_issues_df, file_cases_df
)


# ═══════════════════════════════════════════════════════════════════════
# SIDEBAR — actions + filters
# ═══════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.header("🔄 Page Actions")
    if st.button("🔄 Refresh All Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.header("🔍 Filters")

    if not monitor_df.empty:
        ftype_opts = sorted(monitor_df["file_type"].dropna().unique().tolist())
        pstat_opts = sorted(monitor_df["processing_status"].dropna().unique().tolist())
        client_opts = sorted(monitor_df["client_code"].dropna().unique().tolist())
        vendor_opts = sorted(monitor_df["vendor_code"].dropna().unique().tolist())
        exc_opts = sorted(monitor_df["monitoring_exception"].dropna().unique().tolist())
    else:
        ftype_opts, pstat_opts, client_opts, vendor_opts, exc_opts = [], [], [], [], []

    sel_ftypes = st.multiselect("File Type", ftype_opts, default=ftype_opts)
    sel_pstats = st.multiselect("Processing Status", pstat_opts, default=pstat_opts)
    sel_clients = st.multiselect("Client", client_opts, default=client_opts)
    sel_vendors = st.multiselect("Vendor", vendor_opts, default=vendor_opts)
    sel_exceptions = st.multiselect("Exception Type", exc_opts, default=exc_opts)

    st.divider()
    st.markdown("**Quick Filters**")
    only_duplicates = st.checkbox("🟣 Duplicates / resends only", value=False)
    only_with_issues = st.checkbox("🟡 Files with open issues only", value=False)
    only_with_cases = st.checkbox("🔴 Files with open cases only", value=False)


# ═══════════════════════════════════════════════════════════════════════
# APPLY FILTERS
# ═══════════════════════════════════════════════════════════════════════

def apply_filters(df):
    """Apply all sidebar filters to the monitoring dataframe."""
    if df.empty:
        return df
    out = df.copy()

    if sel_ftypes:
        out = out[out["file_type"].isin(sel_ftypes)]
    if sel_pstats:
        out = out[out["processing_status"].isin(sel_pstats)]
    if sel_clients:
        out = out[out["client_code"].isin(sel_clients)]
    if sel_vendors:
        out = out[out["vendor_code"].isin(sel_vendors)]
    if sel_exceptions:
        out = out[out["monitoring_exception"].isin(sel_exceptions)]
    if only_duplicates:
        out = out[out["duplicate_flag_raw"] == 1]
    if only_with_issues:
        out = out[out["open_issue_count"] > 0]
    if only_with_cases:
        out = out[out["open_case_count"] > 0]

    return out


filtered_df = apply_filters(monitor_df)


# ═══════════════════════════════════════════════════════════════════════
# COMPUTE SUMMARY METRICS
# ═══════════════════════════════════════════════════════════════════════

total_files = len(files_df) if not files_df.empty else 0
duplicate_files = (
    int((files_df["duplicate_flag_raw"] == 1).sum())
    if not files_df.empty else 0
)
total_missing = len(missing_df) if not missing_df.empty else 0
open_missing = (
    int(missing_df["status"].isin(OPEN_ISSUE_STATUSES).sum())
    if not missing_df.empty else 0
)
non_success_runs = (
    int(runs_df["run_status"].isin(NON_SUCCESS_RUN_STATUSES).sum())
    if not runs_df.empty else 0
)
total_runs = len(runs_df) if not runs_df.empty else 0
files_with_errors = (
    int((files_df["error_count"].fillna(0) > 0).sum())
    if not files_df.empty else 0
)
open_file_cases = (
    int(file_cases_df["status"].isin(OPEN_CASE_STATUSES).sum())
    if not file_cases_df.empty else 0
)
breached_sla = (
    int(file_cases_df["is_breached"].fillna(0).sum())
    if not file_cases_df.empty and "is_breached" in file_cases_df.columns
    else 0
)
at_risk_sla = (
    int(file_cases_df["is_at_risk"].fillna(0).sum())
    if not file_cases_df.empty and "is_at_risk" in file_cases_df.columns
    else 0
)
total_exceptions = (
    int((monitor_df["monitoring_exception"] != "NORMAL").sum())
    if not monitor_df.empty else 0
)
filtered_count = len(filtered_df)


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("📁 File Monitoring")
st.caption(
    "Track every inbound file from arrival through processing. Spot missing "
    "deliveries, flag duplicates, monitor processing outcomes, and manage "
    "file-related support cases — the front door of the data pipeline."
)

# ── Key Findings Auto-Summary ──
findings = []
if open_missing > 0:
    findings.append(f"🔴 **{open_missing}** missing file alert(s) — CRITICAL")
if duplicate_files > 0:
    findings.append(f"🟣 **{duplicate_files}** duplicate / resend file(s)")
if non_success_runs > 0:
    findings.append(f"🟠 **{non_success_runs}** non-success processing run(s)")
if breached_sla > 0:
    findings.append(f"⛔ **{breached_sla}** SLA(s) breached")
if at_risk_sla > 0:
    findings.append(f"⚠️ **{at_risk_sla}** SLA(s) at risk")
if open_file_cases > 0:
    findings.append(f"🟡 **{open_file_cases}** open file case(s)")

if findings:
    st.warning("**Key Findings:** " + "  ·  ".join(findings))
else:
    st.success(
        "✅ **All clear.** No missing files, duplicates, processing failures, "
        "or open cases detected."
    )

# ── Top-level metrics ──
k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
k1.metric("Registered Files", fmt_number(total_files))
k2.metric("Duplicates", fmt_number(duplicate_files))
k3.metric("Missing File Alerts", fmt_number(open_missing))
k4.metric("Non-Success Runs", fmt_number(non_success_runs))
k5.metric("Files w/ Errors", fmt_number(files_with_errors))
k6.metric("Open Cases", fmt_number(open_file_cases))
k7.metric("Showing", f"{filtered_count} / {len(monitor_df) if not monitor_df.empty else 0}")

st.divider()


# ═══════════════════════════════════════════════════════════════════════
# TABBED LAYOUT
# ═══════════════════════════════════════════════════════════════════════

tab_worklist, tab_investigate, tab_scenarios, tab_runs, tab_inventory, tab_howto = st.tabs([
    "📋 Exception Worklist",
    "🔎 File Investigation",
    "⚠️ Missing & Duplicate Alerts",
    "⚙️ Processing Runs",
    "📂 Full Inventory",
    "❓ How It Works",
])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1 — EXCEPTION WORKLIST
# ═══════════════════════════════════════════════════════════════════════

with tab_worklist:
    st.subheader("📋 File Exception Worklist")
    st.caption(
        "Every inbound file ranked by urgency. Duplicates and processing "
        "failures appear first, then files with errors or open issues. "
        "Clean files appear last."
    )

    if filtered_df.empty:
        st.info("No inbound files match the current filters.")
    else:
        # ── Exception distribution chart ──
        # Assign column names directly — works across all pandas versions.
        exc_counts = filtered_df["monitoring_exception"].value_counts().reset_index()
        exc_counts.columns = ["Exception", "Count"]

        col_chart, col_legend = st.columns([3, 1])

        with col_chart:
            color_domain = list(EXCEPTION_CONFIG.keys())
            color_range = [v["color"] for v in EXCEPTION_CONFIG.values()]

            chart = (
                alt.Chart(exc_counts)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("Exception:N", sort=color_domain, title=None),
                    y=alt.Y("Count:Q", title="File Count"),
                    color=alt.Color(
                        "Exception:N",
                        scale=alt.Scale(domain=color_domain, range=color_range),
                        legend=None,
                    ),
                    tooltip=["Exception", "Count"],
                )
                .properties(height=250, title="File Exception Distribution")
            )
            st.altair_chart(chart, use_container_width=True)

        with col_legend:
            st.markdown("**Legend**")
            for name, cfg in EXCEPTION_CONFIG.items():
                count_val = int(
                    exc_counts.loc[exc_counts["Exception"] == name, "Count"].sum()
                ) if name in exc_counts["Exception"].values else 0
                st.markdown(f"{cfg['icon']} **{name}**: {count_val}")

        # ── Worklist table ──
        worklist_cols = [
            "exception_badge", "file_id", "file_name", "file_type",
            "client_code", "vendor_code",
            "expected_date", "received_ts",
            "status_badge", "duplicate_flag_label",
            "error_count", "row_count",
            "open_issue_count", "open_case_count",
            "run_count", "last_run_status",
        ]
        st.dataframe(
            filtered_df[safe_col_list(worklist_cols, filtered_df.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "exception_badge": st.column_config.TextColumn("Exception"),
                "status_badge": st.column_config.TextColumn("Processing"),
                "duplicate_flag_label": st.column_config.TextColumn("Duplicate?"),
                "row_count": st.column_config.NumberColumn("Rows", format="%d"),
                "error_count": st.column_config.NumberColumn("Errors", format="%d"),
            },
        )

        # ── Download ──
        csv_wl = filtered_df[
            safe_col_list(worklist_cols, filtered_df.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Worklist CSV",
            csv_wl,
            "file_worklist.csv",
            "text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 2 — FILE INVESTIGATION
# ═══════════════════════════════════════════════════════════════════════

with tab_investigate:
    st.subheader("🔎 Selected File Investigation")
    st.caption(
        "Pick a file from the dropdown to see its full lifecycle: "
        "delivery metadata, processing runs, linked issues, and "
        "support cases — everything needed for root cause analysis."
    )

    if filtered_df.empty:
        st.info("No files available. Run a scenario or adjust filters.")
    else:
        # ── File selector ──
        selectable = filtered_df.copy()
        selectable["label"] = selectable.apply(build_file_label, axis=1)

        selected_label = st.selectbox(
            "Choose a file to investigate",
            options=selectable["label"].tolist(),
            help="Files with exceptions (🟣🔴🟠🟡) appear first.",
        )
        row = selectable[selectable["label"] == selected_label].iloc[0].to_dict()

        sel_file_id = row.get("file_id")
        sel_exc = row.get("monitoring_exception", "REVIEW")

        # ── Operational guidance banner ──
        if sel_exc == "DUPLICATE / RESEND":
            st.warning(
                "🟣 **Duplicate / Resend** — This file matches a previously "
                "received file. Verify whether it was an intentional replacement "
                "or an accidental re-drop. Check whether the original was fully "
                "processed before deciding to suppress or reprocess."
            )
        elif sel_exc == "PROCESSING FAILURE":
            st.error(
                "🔴 **Processing Failure** — This file did not process "
                "successfully. Check the run history for error details, "
                "review linked issues, and determine whether a re-run, "
                "vendor correction, or manual fix is needed."
            )
        elif sel_exc == "FILE ERRORS":
            st.warning(
                "🟠 **File Errors** — Validation found errors in this file's "
                "content. Review the error count, linked issues, and run "
                "details to identify the specific rows or fields affected."
            )
        elif sel_exc == "OPEN FILE ISSUE":
            st.info(
                "🟡 **Open File Issue** — There's an unresolved data quality "
                "issue linked to this file. Review the issue details below "
                "and check if a support case has been created."
            )
        elif sel_exc == "IN PROGRESS":
            st.info(
                "🔵 **In Progress** — This file is currently being validated "
                "or processed. Check back shortly or review run status below."
            )
        else:
            st.success(
                "🟢 **Normal** — No active exceptions. This file was received "
                "and processed without issues."
            )

        # ── Context cards ──
        ctx1, ctx2, ctx3 = st.columns([1.2, 1.2, 1])

        with ctx1:
            st.markdown("##### 📄 File Identity")
            st.markdown(f"**File ID:** `{safe_text(sel_file_id)}`")
            st.markdown(f"**File Name:** `{safe_text(row.get('file_name'))}`")
            st.markdown(f"**File Type:** {safe_text(row.get('file_type'))}")
            st.markdown(f"**File Hash:** `{safe_text(row.get('file_hash'))}`")
            st.markdown(f"**Row Count:** {fmt_number(row.get('row_count'))}")

        with ctx2:
            st.markdown("##### 🏢 Business Context")
            st.markdown(f"**Client:** {safe_text(row.get('client_code'))}")
            st.markdown(f"**Vendor:** {safe_text(row.get('vendor_code'))}")
            st.markdown(f"**Expected Date:** {safe_text(row.get('expected_date'))}")
            st.markdown(f"**Received:** {safe_text(row.get('received_ts'))}")
            st.markdown(f"**Landing Path:** `{safe_text(row.get('landing_path'))}`")

        with ctx3:
            st.markdown("##### ⚡ Control Indicators")
            st.markdown(f"**Exception:** {exception_badge(sel_exc)}")
            st.markdown(f"**Processing:** {status_badge(row.get('processing_status'))}")
            st.markdown(f"**Duplicate:** {safe_text(row.get('duplicate_flag_label'))}")
            st.markdown(f"**Errors:** {fmt_number(row.get('error_count'))}")
            st.markdown(
                f"**Issues / Cases:** "
                f"{safe_val(row.get('open_issue_count'), 0)} / "
                f"{safe_val(row.get('open_case_count'), 0)}"
            )
            st.markdown(f"**Runs:** {safe_val(row.get('run_count'), 0)}")

        # ── Processing pass rate bar ──
        best_rate = safe_val(row.get("best_pass_rate"), None)
        if best_rate is not None and best_rate > 0:
            st.markdown("##### 📊 Best Processing Pass Rate")
            st.progress(min(max(float(best_rate) / 100.0, 0.0), 1.0))
            st.caption(f"{best_rate}% of rows passed validation")

        # ── Drill-down: runs, issues, cases ──
        st.divider()
        st.markdown("##### 🔍 Drill-Down: Runs, Issues & Cases")

        file_runs = (
            runs_df[runs_df["file_id"] == sel_file_id]
            if not runs_df.empty else pd.DataFrame()
        )
        file_issues = (
            file_issues_df[file_issues_df["file_id"] == sel_file_id]
            if not file_issues_df.empty else pd.DataFrame()
        )
        file_cases = (
            file_cases_df[file_cases_df["file_id"] == sel_file_id]
            if not file_cases_df.empty else pd.DataFrame()
        )

        dr1, dr2, dr3 = st.columns(3)

        with dr1:
            st.markdown("**Processing Runs**")
            if file_runs.empty:
                st.write("No processing runs linked to this file.")
            else:
                run_cols = [
                    "run_id", "run_type", "run_status",
                    "started_at", "completed_at",
                    "rows_read", "rows_passed", "rows_failed",
                    "pass_rate_pct", "issue_count", "notes",
                ]
                st.dataframe(
                    file_runs[safe_col_list(run_cols, file_runs.columns)],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "pass_rate_pct": st.column_config.ProgressColumn(
                            "Pass Rate", min_value=0, max_value=100, format="%d%%"
                        ),
                    },
                )

        with dr2:
            st.markdown("**Linked Data Quality Issues**")
            if file_issues.empty:
                st.write("No file-linked issues found.")
            else:
                issue_cols = [
                    "issue_id", "issue_code", "issue_type", "issue_subtype",
                    "severity", "status", "issue_message", "detected_at",
                ]
                st.dataframe(
                    file_issues[safe_col_list(issue_cols, file_issues.columns)],
                    use_container_width=True,
                    hide_index=True,
                )

        with dr3:
            st.markdown("**Linked Support Cases**")
            if file_cases.empty:
                st.write("No file-linked support cases found.")
            else:
                case_cols = [
                    "case_id", "case_number", "case_type", "priority",
                    "severity", "status", "assignment_group",
                    "target_hours", "target_due_at",
                    "sla_status", "is_at_risk_label", "is_breached_label",
                    "short_description",
                ]
                st.dataframe(
                    file_cases[safe_col_list(case_cols, file_cases.columns)],
                    use_container_width=True,
                    hide_index=True,
                )

        # ── RCA Guidance (expandable) ──
        with st.expander("🧭 Root Cause Analysis Guidance", expanded=False):
            st.markdown("""
**If Missing File:**
1. Check whether the vendor's other files arrived today (vendor-wide vs file-specific)
2. Verify the expected delivery schedule — is the expected date correct?
3. Look at the landing path — could the file have arrived with a different name?
4. Contact vendor operations if no file is found
5. Check SLA countdown — missing files have a 4-hour CRITICAL SLA

**If Duplicate / Resend:**
1. Compare file hashes — identical hash confirms identical content
2. Check whether the original was fully processed (run_status = SUCCESS)
3. If the duplicate was also processed, look for doubled records downstream
4. Determine if the resend was intentional (vendor retry) or accidental
5. Verify idempotency protections in the processing pipeline

**If Processing Failure:**
1. Check the run notes for error messages
2. Look at rows_failed vs rows_read — partial or total failure?
3. Review linked issues for specific validation errors
4. Determine if the file content is malformed or if the pipeline had an error
5. Decide: re-run, request correction from vendor, or manual fix

**If File Errors:**
1. Check error_count against row_count — what percentage failed?
2. Review linked issues for specific field-level errors
3. Look at the issue severity — are errors blocking or informational?
4. Determine if errors are vendor-side (bad data) or pipeline-side (bad rules)

**General Checks:**
- Does the file name match the expected naming convention?
- Is the row count within normal range for this file type?
- Has this vendor/client combination had similar issues recently?
            """)


# ═══════════════════════════════════════════════════════════════════════
# TAB 3 — MISSING & DUPLICATE ALERTS
# ═══════════════════════════════════════════════════════════════════════

with tab_scenarios:
    st.subheader("⚠️ Missing & Duplicate File Alerts")
    st.caption(
        "Focused views for the two most critical file scenarios: files that "
        "never arrived and files that arrived more than once."
    )

    alert_left, alert_right = st.columns(2)

    # ── Missing files ──
    with alert_left:
        st.markdown("### 🔴 Missing Inbound Files")
        st.caption(
            "Expected files that never arrived. These are CRITICAL — members "
            "may show as ineligible if eligibility files are missing."
        )

        if missing_df.empty:
            st.success("✅ No missing file alerts. All expected files arrived.")
        else:
            open_m = missing_df[missing_df["status"].isin(OPEN_ISSUE_STATUSES)]
            resolved_m = missing_df[~missing_df["status"].isin(OPEN_ISSUE_STATUSES)]

            mm1, mm2 = st.columns(2)
            mm1.metric("🔴 Open", len(open_m))
            mm2.metric("✅ Resolved", len(resolved_m))

            if not open_m.empty:
                st.error(f"**{len(open_m)} open missing file alert(s)** require attention.")

            miss_cols = [
                "issue_id", "issue_code", "severity", "status",
                "client_code", "vendor_code", "file_id",
                "issue_age_hours", "issue_message",
                "issue_description", "detected_at",
            ]
            st.dataframe(
                missing_df[safe_col_list(miss_cols, missing_df.columns)],
                use_container_width=True,
                hide_index=True,
            )

            csv_miss = missing_df[
                safe_col_list(miss_cols, missing_df.columns)
            ].to_csv(index=False)
            st.download_button(
                "⬇️ Download Missing File Alerts",
                csv_miss,
                "missing_file_alerts.csv",
                "text/csv",
                use_container_width=True,
            )

    # ── Duplicates ──
    with alert_right:
        st.markdown("### 🟣 Duplicate / Resend Files")
        st.caption(
            "Files flagged as duplicates of previously received files. "
            "Risk: double-loaded eligibility records or inflated accumulators."
        )

        if dup_issue_df.empty:
            st.success("✅ No duplicate file alerts detected.")
        else:
            open_d = dup_issue_df[dup_issue_df["status"].isin(OPEN_ISSUE_STATUSES)]
            resolved_d = dup_issue_df[~dup_issue_df["status"].isin(OPEN_ISSUE_STATUSES)]

            dd1, dd2 = st.columns(2)
            dd1.metric("🟣 Open", len(open_d))
            dd2.metric("✅ Resolved", len(resolved_d))

            if not open_d.empty:
                st.warning(
                    f"**{len(open_d)} open duplicate alert(s).** "
                    "Verify whether duplicates were blocked from processing."
                )

            dup_cols = [
                "issue_id", "issue_code", "issue_subtype",
                "severity", "status",
                "client_code", "vendor_code", "file_id",
                "issue_age_hours", "issue_message",
                "issue_description", "detected_at",
            ]
            st.dataframe(
                dup_issue_df[safe_col_list(dup_cols, dup_issue_df.columns)],
                use_container_width=True,
                hide_index=True,
            )

            csv_dup = dup_issue_df[
                safe_col_list(dup_cols, dup_issue_df.columns)
            ].to_csv(index=False)
            st.download_button(
                "⬇️ Download Duplicate Alerts",
                csv_dup,
                "duplicate_file_alerts.csv",
                "text/csv",
                use_container_width=True,
            )


# ═══════════════════════════════════════════════════════════════════════
# TAB 4 — PROCESSING RUNS
# ═══════════════════════════════════════════════════════════════════════

with tab_runs:
    st.subheader("⚙️ Processing Run Monitor")
    st.caption(
        "Every processing run in the system. A 'run' is one attempt to "
        "validate and load a file's data. Green means success, red means "
        "failure, orange means some rows passed and some didn't."
    )

    if runs_df.empty:
        st.info("No processing runs found.")
    else:
        # ── Run summary metrics ──
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Total Runs", fmt_number(total_runs))

        success_runs = int(
            (runs_df["run_status"] == "SUCCESS").sum()
        ) if "run_status" in runs_df.columns else 0
        failed_runs = int(
            (runs_df["run_status"] == "FAILED").sum()
        ) if "run_status" in runs_df.columns else 0
        partial_runs = int(
            (runs_df["run_status"] == "PARTIAL_SUCCESS").sum()
        ) if "run_status" in runs_df.columns else 0

        r2.metric("✅ Success", fmt_number(success_runs))
        r3.metric("🔴 Failed", fmt_number(failed_runs))
        r4.metric("🟠 Partial", fmt_number(partial_runs))

        # ── Run status distribution chart ──
        run_status_counts = runs_df["run_status"].value_counts().reset_index()
        run_status_counts.columns = ["Status", "Count"]

        rs_domain = list(RUN_STATUS_COLORS.keys())
        rs_range = list(RUN_STATUS_COLORS.values())

        run_chart_left, run_chart_right = st.columns([2, 1])

        with run_chart_left:
            run_chart = (
                alt.Chart(run_status_counts)
                .mark_arc(innerRadius=50, cornerRadius=4)
                .encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color(
                        "Status:N",
                        scale=alt.Scale(domain=rs_domain, range=rs_range),
                    ),
                    tooltip=["Status", "Count"],
                )
                .properties(height=250, title="Run Status Distribution")
            )
            st.altair_chart(run_chart, use_container_width=True)

        with run_chart_right:
            st.markdown("**By Run Status**")
            st.dataframe(
                run_status_counts,
                use_container_width=True,
                hide_index=True,
            )

            run_type_summary = (
                runs_df.groupby("run_type")
                .size()
                .reset_index(name="Count")
                .sort_values("Count", ascending=False)
            )
            st.markdown("**By Run Type**")
            st.dataframe(
                run_type_summary,
                use_container_width=True,
                hide_index=True,
            )

        # ── Status filter ──
        run_filter = st.radio(
            "Filter runs by status",
            ["All", "SUCCESS", "FAILED", "PARTIAL_SUCCESS", "RUNNING"],
            horizontal=True,
        )
        display_runs = (
            runs_df
            if run_filter == "All"
            else runs_df[runs_df["run_status"] == run_filter]
        )

        # ── Run table ──
        run_display_cols = [
            "run_id", "run_type", "file_id",
            "started_at", "completed_at", "run_status",
            "rows_read", "rows_passed", "rows_failed",
            "pass_rate_pct", "issue_count",
            "run_age_hours", "notes",
        ]
        st.dataframe(
            display_runs[safe_col_list(run_display_cols, display_runs.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "pass_rate_pct": st.column_config.ProgressColumn(
                    "Pass Rate", min_value=0, max_value=100, format="%d%%"
                ),
                "rows_read": st.column_config.NumberColumn("Read", format="%d"),
                "rows_passed": st.column_config.NumberColumn("Passed", format="%d"),
                "rows_failed": st.column_config.NumberColumn("Failed", format="%d"),
            },
        )

        csv_runs = display_runs[
            safe_col_list(run_display_cols, display_runs.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Run Data",
            csv_runs,
            "processing_runs.csv",
            "text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 5 — FULL INVENTORY
# ═══════════════════════════════════════════════════════════════════════

with tab_inventory:
    st.subheader("📂 Inbound File Inventory")
    st.caption(
        "Complete registry of every file the system knows about — received "
        "or expected. Use this as the reference table for all file investigations."
    )

    if filtered_df.empty:
        st.info("No inbound files match the current filters.")
    else:
        # ── Quick summary charts ──
        inv_left, inv_right = st.columns(2)

        with inv_left:
            # File type distribution
            if "file_type" in filtered_df.columns:
                ft_counts = filtered_df["file_type"].value_counts().reset_index()
                ft_counts.columns = ["Type", "Count"]

                ft_chart = (
                    alt.Chart(ft_counts)
                    .mark_arc(innerRadius=40, cornerRadius=4)
                    .encode(
                        theta=alt.Theta("Count:Q"),
                        color=alt.Color("Type:N"),
                        tooltip=["Type", "Count"],
                    )
                    .properties(height=220, title="Files by Type")
                )
                st.altair_chart(ft_chart, use_container_width=True)

        with inv_right:
            # Processing status distribution
            if "processing_status" in filtered_df.columns:
                ps_counts = filtered_df["processing_status"].value_counts().reset_index()
                ps_counts.columns = ["Status", "Count"]

                ps_chart = (
                    alt.Chart(ps_counts)
                    .mark_arc(innerRadius=40, cornerRadius=4)
                    .encode(
                        theta=alt.Theta("Count:Q"),
                        color=alt.Color("Status:N"),
                        tooltip=["Status", "Count"],
                    )
                    .properties(height=220, title="Files by Processing Status")
                )
                st.altair_chart(ps_chart, use_container_width=True)

        # ── Full inventory table ──
        inventory_cols = [
            "file_id", "file_name", "file_type",
            "client_code", "vendor_code",
            "expected_date", "received_ts",
            "row_count", "status_badge",
            "duplicate_flag_label", "error_count",
            "file_record_age_hours",
            "landing_path", "archived_path",
        ]
        st.dataframe(
            filtered_df[safe_col_list(inventory_cols, filtered_df.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "status_badge": st.column_config.TextColumn("Processing Status"),
                "duplicate_flag_label": st.column_config.TextColumn("Duplicate?"),
                "row_count": st.column_config.NumberColumn("Rows", format="%d"),
                "error_count": st.column_config.NumberColumn("Errors", format="%d"),
                "file_record_age_hours": st.column_config.NumberColumn(
                    "Age (hrs)", format="%.1f"
                ),
            },
        )

        csv_inv = filtered_df[
            safe_col_list(inventory_cols, filtered_df.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Full Inventory CSV",
            csv_inv,
            "file_inventory.csv",
            "text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 6 — HOW IT WORKS
# ═══════════════════════════════════════════════════════════════════════

with tab_howto:
    st.subheader("❓ How File Monitoring Works")
    st.caption(
        "A plain-English explanation of what this page does, why it matters, "
        "and what the key terms mean — written so anyone can understand."
    )

    st.markdown("""
---

### 📬 What Is File Monitoring?

Imagine a company that gets important mail deliveries every day. Each piece
of mail has a **sender** (the vendor), a **recipient** (the client), and a
**schedule** (when it should arrive).

File monitoring is like having a **mail room manager** who:
- ✅ Checks that every expected delivery arrived on time
- 🟣 Catches when the same letter is delivered twice (duplicate)
- 🔴 Raises an alarm when a delivery is missing
- ⚙️ Tracks whether each letter was opened and read correctly (processing)
- 🎫 Creates a trouble ticket when something goes wrong

In healthcare, the "mail" is **data files** containing member eligibility
records, claims, and accumulator updates. If a file is missing or broken,
real people can be affected — their insurance coverage might not show up
when they visit the doctor.

---

### 📄 What Kinds of Files Are There?

| File Type | What It Contains |
|---|---|
| **Eligibility** | Who is covered, under what plan, and for what dates |
| **Claims** | Medical bills — what services happened and what was charged |
| **Accumulator** | Running totals of what members have paid toward their deductible/OOP max |

---

### ⚠️ What Can Go Wrong?

| Problem | What Happens | How Urgent |
|---|---|---|
| **Missing file** | Expected file never arrives; members may show as uninsured | 🔴 CRITICAL — 4 hour SLA |
| **Duplicate file** | Same file arrives twice; records might be doubled | 🟣 MEDIUM — 24 hour SLA |
| **Processing failure** | File arrived but couldn't be read or loaded | 🔴 HIGH — needs immediate review |
| **File errors** | Some rows in the file have bad data | 🟠 MEDIUM — partial impact |

---

### ⚙️ What Is a Processing Run?

When a file arrives, the system tries to **read, validate, and load** it.
This attempt is called a "processing run." Each run reports:
- **Rows read** — how many records were in the file
- **Rows passed** — how many loaded successfully
- **Rows failed** — how many had errors
- **Pass rate** — what percentage succeeded

A file can have multiple runs if it was retried after fixing errors.

---

### 📋 What Does This Page Do?

1. **Exception Worklist** — Every file ranked by urgency. Problems first.
2. **File Investigation** — Pick any file to see its full lifecycle:
   delivery info, processing runs, linked issues, and support cases.
3. **Missing & Duplicate Alerts** — Focused views for the two most
   critical file scenarios, with open/resolved counts and downloads.
4. **Processing Runs** — Monitor every attempt to validate and load files,
   with pass rates, status charts, and filtering by outcome.
5. **Full Inventory** — Complete registry of every file in the system,
   with type and status breakdowns.

---

### 🔑 Key Terms

| Term | What It Means |
|---|---|
| **Inbound file** | A data file received from a vendor for processing |
| **Expected date** | The date a file was supposed to arrive |
| **Received timestamp** | When the file actually arrived |
| **File hash** | A digital fingerprint — identical files have identical hashes |
| **Duplicate flag** | Indicates this file matches one already received |
| **Processing status** | Where the file is in its lifecycle (received → validated → processed) |
| **Processing run** | One attempt to validate and load a file's data |
| **Pass rate** | Percentage of rows that loaded successfully |
| **Landing path** | Where the file was placed when it first arrived |
| **Archived path** | Where the file was moved after processing |
| **SLA** | Service Level Agreement — a deadline for resolving a problem |
| **RCA** | Root Cause Analysis — figuring out *why* something went wrong |

---

### 🔗 How Files Connect to Everything Else

Vendor sends file
→ File lands in inbound_files table
→ Processing run attempts to validate and load it
→ If problems found → data_quality_issues created
→ If serious enough → support_cases created
→ SLA clock starts ticking
→ Operations team triages on Issue Triage page


A missing file follows a different path:
Expected file doesn't arrive
→ Monitoring detects the gap
→ MISSING_INBOUND_FILE issue created (CRITICAL)
→ Support case routed to ops_file_queue
→ 4-hour SLA starts
→ Vendor contacted for resolution


---

### 🎯 Who Uses This Page?

- **File operations analysts** check daily that all expected files arrived
  and processed cleanly
- **Data engineers** investigate processing failures and file content errors
- **Vendor managers** track vendor reliability and communicate about
  missing or duplicate deliveries
- **Support teams** triage file-related cases and track SLA compliance

---

### 🚀 Quick Start Guide

1. Look at the **Key Findings** banner — it tells you immediately if
   anything needs attention today
2. Check the **Exception Worklist** for files ranked by urgency
3. If a file has a problem, click **File Investigation** and select it
   from the dropdown to see the full picture
4. Use **Missing & Duplicate Alerts** for scenario-specific focused views
5. Review **Processing Runs** to check whether recent loads succeeded
6. Reference **Full Inventory** for the complete file registry
    """)

    st.info(
        "💡 **Tip:** Use the sidebar filters to narrow down by file type, "
        "client, vendor, or processing status. The quick-filter checkboxes "
        "let you jump straight to duplicates, files with issues, or files "
        "with open cases."
    )

    # ── Connected pages guidance ──
    with st.expander("🔗 Connected Pages — Where to Go Next", expanded=False):
        st.markdown("""
**From File Monitoring, you might need to visit:**

| If You See… | Go To… | Why |
|---|---|---|
| A missing file caused claim rejections | **Issue Triage** | To review the support case and SLA status |
| A duplicate file may have doubled records | **Accumulator Reconciliation** | To check if accumulators were inflated |
| Multiple file failures for one vendor | **Issue Triage** | To see if a vendor-wide pattern exists |
| A processing run partially succeeded | **Issue Triage** | To review which specific rows/records failed |
| You want to trigger a test scenario | **Scenario Control Center** | To inject MISSING_INBOUND_FILE or DUPLICATE_ELIGIBILITY_RESEND |
        """)


# ═══════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════

st.divider()
st.caption(
    "File Monitoring · Eligibility & Accumulator Operations Command Center · "
    "Data is simulated — no real PHI"
)