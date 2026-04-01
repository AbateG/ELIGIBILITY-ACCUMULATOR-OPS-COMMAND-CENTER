import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

"""
Scenario Control Center Page
=============================
Deterministic incident launcher for healthcare eligibility, claims,
file monitoring, and accumulator support operations. Trigger realistic
scenarios, observe downstream artifact generation (issues, cases, SLAs),
and navigate to investigation pages for triage and root cause analysis.

Design principles
-----------------
- Launch-first: one-click scenario execution with before/after deltas
- Traceable: every scenario produces countable, queryable artifacts
- Investigation-oriented: guide users from launch → artifacts → page handoff
- Portfolio-grade: screenshot-ready scenario cards and analytics
- Accessible: plain-language explanations anyone can follow
"""

# ═══════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════

import sys
import os
# Add project root to path for module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from importlib import import_module

from src.common.db import get_connection


# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Scenario Control Center",
    page_icon="🎛️",
    layout="wide",
)


# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════

OPEN_ISSUE_STATUSES = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS"}
OPEN_CASE_STATUSES = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"}

PRIORITY_CONFIG = {
    "CRITICAL": {"icon": "🔴", "rank": 1, "color": "#FF4B4B"},
    "HIGH":     {"icon": "🟠", "rank": 2, "color": "#FFA500"},
    "MEDIUM":   {"icon": "🟡", "rank": 3, "color": "#FFD700"},
    "LOW":      {"icon": "🔵", "rank": 4, "color": "#4B9DFF"},
}

SLA_WATCH_CONFIG = {
    "BREACHED": {"icon": "⛔", "color": "#FF4B4B"},
    "AT_RISK":  {"icon": "⚠️", "color": "#FFA500"},
    "ON_TRACK": {"icon": "✅", "color": "#2ECC71"},
}

STATUS_ICONS = {
    "OPEN": "🟡", "ACKNOWLEDGED": "🟠", "IN_PROGRESS": "🟠",
    "ESCALATED": "🔴", "RESOLVED": "✅", "CLOSED": "✅",
}

DOMAIN_ICONS = {
    "File Monitoring":              "📁",
    "Eligibility / File Intake":    "👤",
    "Claims / Eligibility":         "🧾",
    "Accumulators / Reconciliation":"💰",
    "Accumulators / Family Logic":  "👨‍👩‍👧‍👦",
}

SCENARIO_COLORS = {
    "MISSING_INBOUND_FILE":         "#FF4B4B",
    "DUPLICATE_ELIGIBILITY_RESEND": "#9B59B6",
    "CLAIM_INELIGIBLE_MEMBER":      "#FFA500",
    "ACCUMULATOR_EXCEEDS_OOP_MAX":  "#E74C3C",
    "FAMILY_ROLLUP_DISCREPANCY":    "#3498DB",
}


# ═══════════════════════════════════════════════════════════════════════
# SCENARIO REGISTRY
# ═══════════════════════════════════════════════════════════════════════

SCENARIO_REGISTRY: List[Dict[str, Any]] = [
    {
        "scenario_code": "MISSING_INBOUND_FILE",
        "title": "Missing Inbound File",
        "domain": "File Monitoring",
        "priority": "CRITICAL",
        "sla_hours": 4,
        "expected_queue": "ops_file_queue",
        "primary_page": "File Monitoring",
        "description": (
            "Simulates a missing expected inbound file. Demonstrates file "
            "monitoring, vendor/client follow-up, issue creation, support "
            "case generation, and urgent SLA handling."
        ),
        "business_impact": (
            "Delayed file availability can block downstream eligibility or "
            "claims processing and introduce client reporting or operational "
            "SLA risk."
        ),
        "likely_rca": (
            "File not delivered, file delivered under incorrect naming "
            "convention, or file registration/monitoring expectation mismatch."
        ),
        "first_checks": [
            "Confirm expected file pattern and delivery window.",
            "Check inbound file registration and missing-file issue records.",
            "Confirm prior successful deliveries from same client/vendor.",
            "Validate whether true missing vs delayed vs naming mismatch.",
        ],
        "module_path": "src.scenarios.scenario_missing_inbound_file",
    },
    {
        "scenario_code": "DUPLICATE_ELIGIBILITY_RESEND",
        "title": "Duplicate Eligibility Resend",
        "domain": "Eligibility / File Intake",
        "priority": "MEDIUM",
        "sla_hours": 24,
        "expected_queue": "ops_eligibility_queue",
        "primary_page": "Issue Triage",
        "description": (
            "Simulates a duplicate eligibility resend or duplicate ingestion "
            "condition. Demonstrates duplicate detection, eligibility file "
            "validation, issue triage, and support routing."
        ),
        "business_impact": (
            "Duplicate loads may create operational confusion, duplicate "
            "downstream updates, or inaccurate membership state."
        ),
        "likely_rca": (
            "Client/vendor resend, duplicate registration, reprocessing "
            "without dedupe control, or file checksum/content duplication."
        ),
        "first_checks": [
            "Compare file names, load timestamps, and source vendor/client.",
            "Review duplicate detection flags and prior load history.",
            "Validate whether duplicate content was loaded or only registered.",
            "Confirm remediation: ignore, rollback, or controlled reload.",
        ],
        "module_path": "src.scenarios.scenario_duplicate_eligibility_resend",
    },
    {
        "scenario_code": "CLAIM_INELIGIBLE_MEMBER",
        "title": "Claim for Ineligible Member",
        "domain": "Claims / Eligibility",
        "priority": "HIGH",
        "sla_hours": 8,
        "expected_queue": "ops_claims_queue",
        "primary_page": "Issue Triage",
        "description": (
            "Simulates a claim received for a member who is inactive or "
            "ineligible on the date of service. Demonstrates claims support "
            "and eligibility root cause analysis."
        ),
        "business_impact": (
            "Claims may pend, deny, or process incorrectly if eligibility "
            "state is misaligned with claim service dates."
        ),
        "likely_rca": (
            "Eligibility gap, late eligibility load, enrollment termination "
            "mismatch, bad effective dates, or claim outside active coverage."
        ),
        "first_checks": [
            "Confirm member eligibility span for date of service.",
            "Review latest eligibility file and load timing.",
            "Compare claim service date to coverage effective/term dates.",
            "Determine: data timing, enrollment accuracy, or claim error.",
        ],
        "module_path": "src.scenarios.scenario_claim_for_ineligible_member",
    },
    {
        "scenario_code": "ACCUMULATOR_EXCEEDS_OOP_MAX",
        "title": "Accumulator Exceeds OOP Max",
        "domain": "Accumulators / Reconciliation",
        "priority": "HIGH",
        "sla_hours": 8,
        "expected_queue": "ops_recon_queue",
        "primary_page": "Accumulator Reconciliation",
        "description": (
            "Simulates out-of-pocket accumulator balances exceeding configured "
            "maximums. Demonstrates accumulator validation, benefit limit "
            "review, and reconciliation support."
        ),
        "business_impact": (
            "Accumulator overages can create member financial exposure, "
            "benefit misapplication, and compliance concerns."
        ),
        "likely_rca": (
            "Duplicate accumulator transaction posting, incorrect benefit "
            "configuration, improper cap enforcement, or timing defect."
        ),
        "first_checks": [
            "Review plan-level OOP max configuration.",
            "Trace accumulator transactions contributing to the overage.",
            "Confirm duplicate or erroneous postings.",
            "Compare transaction detail to current accumulator snapshot.",
        ],
        "module_path": "src.scenarios.scenario_accumulator_oop_exceeded",
    },
    {
        "scenario_code": "FAMILY_ROLLUP_DISCREPANCY",
        "title": "Family Rollup Discrepancy",
        "domain": "Accumulators / Family Logic",
        "priority": "MEDIUM",
        "sla_hours": 24,
        "expected_queue": "ops_recon_queue",
        "primary_page": "Accumulator Reconciliation",
        "description": (
            "Simulates a discrepancy between individual accumulator totals "
            "and family rollup balances. Demonstrates family accumulation "
            "logic validation and reconciliation support."
        ),
        "business_impact": (
            "Family accumulator discrepancies can lead to incorrect benefit "
            "progression, member confusion, and reconciliation exceptions."
        ),
        "likely_rca": (
            "Broken family aggregation logic, missing dependent contribution, "
            "duplicate family assignment, or snapshot inconsistency."
        ),
        "first_checks": [
            "Compare individual member balances within the family unit.",
            "Review family grouping identifiers and dependent relationships.",
            "Validate family rollup calculations against member transactions.",
            "Confirm discrepancy in transactions, snapshots, or both.",
        ],
        "module_path": "src.scenarios.scenario_family_rollup_discrepancy",
    },
]


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


def safe_text(value: Any, fallback: str = "—") -> str:
    """Return a display-safe string for UI labels."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text if text else fallback


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


def priority_badge(priority: str) -> str:
    """Return an icon + label string for a priority level."""
    cfg = PRIORITY_CONFIG.get(str(priority).upper(), {"icon": "⚪"})
    return f"{cfg['icon']} {safe_text(priority)}"


def status_badge(status: str) -> str:
    """Return an icon + label string for a case/issue status."""
    icon = STATUS_ICONS.get(str(status).upper(), "⚪") if status else "⚪"
    return f"{icon} {safe_text(status)}"


def domain_icon(domain: str) -> str:
    """Return an icon for a scenario domain."""
    return DOMAIN_ICONS.get(domain, "🎛️")


def format_timestamp(ts: Any) -> str:
    """Format a timestamp for display."""
    if ts is None:
        return "—"
    return str(ts)


# ═══════════════════════════════════════════════════════════════════════
# SCENARIO LOADER RESOLUTION
# ═══════════════════════════════════════════════════════════════════════

def resolve_scenario_loader(module_path: str) -> Callable[[], Any]:
    """
    Resolve a scenario loader function from a scenario module.
    Tries: load_scenario, run_scenario, run, main.
    """
    module = import_module(module_path)
    for name in ["load_scenario", "run_scenario", "run", "main"]:
        fn = getattr(module, name, None)
        if callable(fn):
            return fn
    raise ImportError(
        f"No loader function found in '{module_path}'. "
        f"Expected: load_scenario, run_scenario, run, or main"
    )


# ═══════════════════════════════════════════════════════════════════════
# DATABASE QUERY HELPERS (cached with TTL)
# ═══════════════════════════════════════════════════════════════════════

def query_df(sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame."""
    conn = get_connection()
    try:
        return pd.read_sql_query(sql, conn, params=params or [])
    finally:
        conn.close()


def query_scalar(sql: str, params: Optional[List[Any]] = None, default: int = 0) -> Any:
    """Execute a SQL query and return the first value of the first row."""
    df = query_df(sql, params)
    if df.empty:
        return default
    val = df.iloc[0, 0]
    return val if val is not None else default


@st.cache_data(ttl=120, show_spinner=False)
def get_operational_snapshot() -> Dict[str, Any]:
    """Get system-wide operational counts for the dashboard header."""
    return {
        "open_issues": int(query_scalar(
            "SELECT COUNT(*) FROM data_quality_issues WHERE status IN ('OPEN','ACKNOWLEDGED','IN_PROGRESS')"
        )),
        "open_cases": int(query_scalar(
            "SELECT COUNT(*) FROM support_cases WHERE status IN ('OPEN','ACKNOWLEDGED','IN_PROGRESS','ESCALATED')"
        )),
        "at_risk_slas": int(query_scalar(
            "SELECT COUNT(*) FROM sla_tracking WHERE is_at_risk = 1 AND is_breached = 0"
        )),
        "breached_slas": int(query_scalar(
            "SELECT COUNT(*) FROM sla_tracking WHERE is_breached = 1"
        )),
        "total_issues": int(query_scalar("SELECT COUNT(*) FROM data_quality_issues")),
        "total_cases": int(query_scalar("SELECT COUNT(*) FROM support_cases")),
        "total_slas": int(query_scalar("SELECT COUNT(*) FROM sla_tracking")),
    }


def get_scenario_counts(scenario_code: str) -> Dict[str, int]:
    """Get current artifact counts for a specific scenario code."""
    issues = int(query_scalar(
        "SELECT COUNT(*) FROM data_quality_issues WHERE issue_code = ?",
        [scenario_code],
    ))
    cases = int(query_scalar("""
        SELECT COUNT(*) FROM support_cases c
        INNER JOIN data_quality_issues i ON c.issue_id = i.issue_id
        WHERE i.issue_code = ?
    """, [scenario_code]))
    slas = int(query_scalar("""
        SELECT COUNT(*) FROM sla_tracking s
        INNER JOIN support_cases c ON s.case_id = c.case_id
        INNER JOIN data_quality_issues i ON c.issue_id = i.issue_id
        WHERE i.issue_code = ?
    """, [scenario_code]))
    return {"issues": issues, "cases": cases, "slas": slas}


@st.cache_data(ttl=120, show_spinner=False)
def get_all_scenario_counts() -> pd.DataFrame:
    """Get artifact counts for all scenarios at once — used for comparison chart."""
    rows = []
    for s in SCENARIO_REGISTRY:
        counts = get_scenario_counts(s["scenario_code"])
        rows.append({
            "Scenario": s["scenario_code"],
            "Title": s["title"],
            "Priority": s["priority"],
            "Issues": counts["issues"],
            "Cases": counts["cases"],
            "SLAs": counts["slas"],
            "Total Artifacts": counts["issues"] + counts["cases"] + counts["slas"],
        })
    return pd.DataFrame(rows)


def get_issue_summary(scenario_code: str) -> pd.DataFrame:
    """Get aggregated issue summary for a scenario."""
    return query_df("""
        SELECT issue_code, status, severity, COUNT(*) AS issue_count
        FROM data_quality_issues WHERE issue_code = ?
        GROUP BY issue_code, status, severity
        ORDER BY issue_count DESC
    """, [scenario_code])


def get_case_summary(scenario_code: str) -> pd.DataFrame:
    """Get aggregated case summary for a scenario."""
    return query_df("""
        SELECT
            c.status,
            c.priority,
            c.assigned_team AS assignment_group,
            COUNT(*) AS case_count
        FROM support_cases c
        INNER JOIN data_quality_issues i ON c.issue_id = i.issue_id
        WHERE i.issue_code = ?
        GROUP BY c.status, c.priority, c.assigned_team
        ORDER BY case_count DESC
    """, [scenario_code])


def get_sla_summary(scenario_code: str) -> pd.DataFrame:
    """Get aggregated SLA summary for a scenario."""
    return query_df("""
        SELECT s.status, s.is_at_risk, s.is_breached, s.target_hours,
               COUNT(*) AS sla_count
        FROM sla_tracking s
        INNER JOIN support_cases c ON s.case_id = c.case_id
        INNER JOIN data_quality_issues i ON c.issue_id = i.issue_id
        WHERE i.issue_code = ?
        GROUP BY s.status, s.is_at_risk, s.is_breached, s.target_hours
        ORDER BY sla_count DESC
    """, [scenario_code])


def get_recent_issues(scenario_code: str, limit: int = 10) -> pd.DataFrame:
    """Get most recent issues for a scenario."""
    return query_df(f"""
        SELECT i.issue_id, i.issue_code, i.issue_type, i.status, i.severity,
               c.client_code, v.vendor_code,
               i.entity_name, i.entity_key, i.created_at,
               i.issue_message, i.issue_description
        FROM data_quality_issues i
        LEFT JOIN clients c ON i.client_id = c.client_id
        LEFT JOIN vendors v ON i.vendor_id = v.vendor_id
        WHERE i.issue_code = ?
        ORDER BY datetime(i.created_at) DESC, i.issue_id DESC
        LIMIT {int(limit)}
    """, [scenario_code])


def get_recent_cases(scenario_code: str, limit: int = 10) -> pd.DataFrame:
    """Get most recent support cases for a scenario."""
    return query_df(f"""
        SELECT
            c.case_id, c.case_number, c.issue_id, c.status, c.priority,
            c.assigned_team AS assignment_group,
            c.assigned_to,
            cl.client_code, v.vendor_code,
            c.opened_at, c.resolved_at, c.short_description
        FROM support_cases c
        INNER JOIN data_quality_issues i ON c.issue_id = i.issue_id
        LEFT JOIN clients cl ON c.client_id = cl.client_id
        LEFT JOIN vendors v ON c.vendor_id = v.vendor_id
        WHERE i.issue_code = ?
        ORDER BY datetime(c.opened_at) DESC, c.case_id DESC
        LIMIT {int(limit)}
    """, [scenario_code])


def get_recent_slas(scenario_code: str, limit: int = 10) -> pd.DataFrame:
    """Get most recent SLA records for a scenario."""
    return query_df(f"""
        SELECT s.case_id, s.sla_type, s.status, s.target_hours,
               s.is_at_risk, s.is_breached,
               s.target_due_at, s.created_at, s.last_evaluated_at
        FROM sla_tracking s
        INNER JOIN support_cases c ON s.case_id = c.case_id
        INNER JOIN data_quality_issues i ON c.issue_id = i.issue_id
        WHERE i.issue_code = ?
        ORDER BY datetime(s.created_at) DESC, s.sla_id DESC
        LIMIT {int(limit)}
    """, [scenario_code])


# ═══════════════════════════════════════════════════════════════════════
# SCENARIO EXECUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════

def execute_scenario(module_path: str, scenario_code: str) -> Dict[str, Any]:
    """
    Execute a scenario and capture before/after artifact counts.
    Returns a result dict with timing and delta information.
    """
    loader = resolve_scenario_loader(module_path)
    before_counts = get_scenario_counts(scenario_code)
    started_at = datetime.utcnow()

    result = loader()

    completed_at = datetime.utcnow()
    # Clear cached snapshot so new counts appear immediately
    get_operational_snapshot.clear()
    get_all_scenario_counts.clear()

    after_counts = get_scenario_counts(scenario_code)
    delta = {
        "issues_created": after_counts["issues"] - before_counts["issues"],
        "cases_created": after_counts["cases"] - before_counts["cases"],
        "slas_created": after_counts["slas"] - before_counts["slas"],
    }

    run_result = {
        "scenario_code": scenario_code,
        "started_at": started_at,
        "completed_at": completed_at,
        "before_counts": before_counts,
        "after_counts": after_counts,
        "delta": delta,
        "loader_result": result,
    }

    st.session_state["last_run_result"] = run_result
    st.session_state["last_run_scenario_code"] = scenario_code

    history = st.session_state.get("scenario_run_history", [])
    history.insert(0, run_result)
    st.session_state["scenario_run_history"] = history[:50]

    return run_result


def execute_all_scenarios() -> List[Dict[str, Any]]:
    """Execute all scenarios in registry order. Returns list of results."""
    results = []
    for scenario in SCENARIO_REGISTRY:
        try:
            result = execute_scenario(
                scenario["module_path"], scenario["scenario_code"]
            )
            results.append(result)
        except Exception as exc:
            results.append({
                "scenario_code": scenario["scenario_code"],
                "error": str(exc),
            })
    return results


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("🎛️ Scenario Control Center")
st.caption(
    "Deterministic incident launcher for healthcare eligibility, claims, "
    "file monitoring, and accumulator support operations. Trigger scenarios, "
    "observe artifact generation, and navigate to investigation pages."
)

# ── Sidebar ──
with st.sidebar:
    st.header("🔄 Page Actions")
    if st.button("🔄 Refresh Page", use_container_width=True):
        get_operational_snapshot.clear()
        get_all_scenario_counts.clear()
        st.rerun()

    st.divider()

    st.header("🚀 Quick Launch")
    quick_scenario = st.radio(
        "Select scenario",
        options=[s["scenario_code"] for s in SCENARIO_REGISTRY],
        format_func=lambda code: next(
            (f"{PRIORITY_CONFIG.get(s['priority'], {}).get('icon', '⚪')} {s['title']}"
             for s in SCENARIO_REGISTRY if s["scenario_code"] == code),
            code,
        ),
    )

    if st.button("▶️ Run Selected Scenario", use_container_width=True):
        scenario = next(
            (s for s in SCENARIO_REGISTRY if s["scenario_code"] == quick_scenario),
            None,
        )
        if scenario:
            with st.spinner(f"Running {scenario['scenario_code']}…"):
                try:
                    execute_scenario(scenario["module_path"], scenario["scenario_code"])
                    st.success("✅ Executed!")
                    st.rerun()
                except Exception as exc:
                    st.error(f"❌ Failed: {exc}")

    st.divider()
    if st.button("🚀 Run ALL Scenarios", use_container_width=True):
        with st.spinner("Running all 5 scenarios…"):
            results = execute_all_scenarios()
            successes = sum(1 for r in results if "error" not in r)
            st.success(f"✅ {successes}/{len(results)} scenarios completed")
            st.rerun()


# ── Key Findings Banner ──
snapshot = get_operational_snapshot()

findings = []
if snapshot["breached_slas"] > 0:
    findings.append(f"⛔ **{snapshot['breached_slas']}** SLA(s) breached")
if snapshot["at_risk_slas"] > 0:
    findings.append(f"⚠️ **{snapshot['at_risk_slas']}** SLA(s) at risk")
if snapshot["open_cases"] > 0:
    findings.append(f"📂 **{snapshot['open_cases']}** open case(s)")
if snapshot["open_issues"] > 0:
    findings.append(f"🔵 **{snapshot['open_issues']}** open issue(s)")

if findings:
    st.warning("**Operational Status:**  " + "  ·  ".join(findings))
elif snapshot["total_issues"] > 0:
    st.success(
        "✅ **System healthy.** Scenarios have been run — no open SLA concerns."
    )
else:
    st.info(
        "🎛️ **Ready to launch.** No scenarios have been run yet. Use the "
        "launcher below or the sidebar Quick Launch to get started."
    )

# ── Top metrics ──
m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Open Issues", fmt_number(snapshot["open_issues"]))
m2.metric("Open Cases", fmt_number(snapshot["open_cases"]))
m3.metric("⚠️ At-Risk SLAs", fmt_number(snapshot["at_risk_slas"]))
m4.metric("⛔ Breached SLAs", fmt_number(snapshot["breached_slas"]))
m5.metric("Total Issues", fmt_number(snapshot["total_issues"]))
m6.metric("Total Cases", fmt_number(snapshot["total_cases"]))

st.divider()


# ═══════════════════════════════════════════════════════════════════════
# TABBED LAYOUT
# ═══════════════════════════════════════════════════════════════════════

tab_launcher, tab_snapshot, tab_catalog, tab_history, tab_explorer, tab_howto = st.tabs([
    "🚀 Scenario Launcher",
    "📊 Operational Snapshot",
    "📋 Scenario Catalog",
    "📜 Run History",
    "🔎 Artifact Explorer",
    "❓ How It Works",
])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1 — SCENARIO LAUNCHER
# ═══════════════════════════════════════════════════════════════════════

with tab_launcher:
    st.subheader("🚀 Scenario Launcher")
    st.caption(
        "Each card represents one deterministic support scenario. Click the "
        "Run button to inject the scenario, then review the generated "
        "artifacts and navigate to the investigation page."
    )

    # ── Last run banner ──
    last_run = st.session_state.get("last_run_result")
    if last_run:
        scenario_meta = next(
            (s for s in SCENARIO_REGISTRY
             if s["scenario_code"] == last_run["scenario_code"]),
            None,
        )
        delta = last_run["delta"]

        st.markdown("### 🏁 Last Execution Result")
        lr1, lr2, lr3, lr4, lr5 = st.columns(5)
        lr1.metric("Scenario", last_run["scenario_code"])
        lr2.metric("Issues Created", delta["issues_created"])
        lr3.metric("Cases Created", delta["cases_created"])
        lr4.metric("SLAs Created", delta["slas_created"])
        lr5.metric(
            "Total New",
            delta["issues_created"] + delta["cases_created"] + delta["slas_created"],
        )

        st.caption(
            f"Started: {format_timestamp(last_run['started_at'])} · "
            f"Completed: {format_timestamp(last_run['completed_at'])}"
        )

        if scenario_meta:
            st.info(
                f"👉 **Next step:** Navigate to "
                f"**{scenario_meta['primary_page']}** to investigate. "
                f"Expected queue: `{scenario_meta['expected_queue']}`"
            )

        if last_run.get("loader_result") is not None:
            with st.expander("📄 Loader Return Payload", expanded=False):
                st.write(last_run["loader_result"])

        st.divider()

    # ── Scenario cards ──
    for scenario in SCENARIO_REGISTRY:
        counts = get_scenario_counts(scenario["scenario_code"])
        is_last = (
            last_run and last_run.get("scenario_code") == scenario["scenario_code"]
        )
        pri_cfg = PRIORITY_CONFIG.get(scenario["priority"], {"icon": "⚪", "color": "#BDC3C7"})
        d_icon = domain_icon(scenario["domain"])

        with st.container(border=True):
            card_left, card_right = st.columns([3, 1])

            with card_left:
                st.markdown(
                    f"### {d_icon} {scenario['title']}  "
                    f"{pri_cfg['icon']} {scenario['priority']}"
                )
                st.markdown(
                    f"**Code:** `{scenario['scenario_code']}` · "
                    f"**Domain:** {scenario['domain']} · "
                    f"**Queue:** `{scenario['expected_queue']}` · "
                    f"**SLA:** {scenario['sla_hours']}h · "
                    f"**Page:** {scenario['primary_page']}"
                )
                st.write(scenario["description"])

                with st.expander("🧭 Operational Context & RCA Guide", expanded=False):
                    st.markdown(f"**Business Impact:** {scenario['business_impact']}")
                    st.markdown(f"**Likely Root Causes:** {scenario['likely_rca']}")
                    st.markdown("**Suggested First Checks:**")
                    for check in scenario["first_checks"]:
                        st.markdown(f"- {check}")

            with card_right:
                st.markdown("**Current Artifacts**")
                st.metric("Issues", counts["issues"])
                st.metric("Cases", counts["cases"])
                st.metric("SLAs", counts["slas"])

                if is_last and last_run:
                    d = last_run["delta"]
                    st.caption(
                        f"Last delta: +{d['issues_created']}i "
                        f"+{d['cases_created']}c +{d['slas_created']}s"
                    )

                if st.button(
                    f"▶️ Run {scenario['title']}",
                    key=f"btn_{scenario['scenario_code']}",
                    use_container_width=True,
                ):
                    with st.spinner(f"Executing {scenario['scenario_code']}…"):
                        try:
                            result = execute_scenario(
                                scenario["module_path"],
                                scenario["scenario_code"],
                            )
                            st.success(
                                f"✅ +{result['delta']['issues_created']}i "
                                f"+{result['delta']['cases_created']}c "
                                f"+{result['delta']['slas_created']}s"
                            )
                            st.rerun()
                        except Exception as exc:
                            st.error(f"❌ Failed: {exc}")

            # ── Artifact detail tabs (show if artifacts exist) ──
            has_artifacts = (
                counts["issues"] > 0 or counts["cases"] > 0
                or counts["slas"] > 0 or is_last
            )

            if has_artifacts:
                detail_tabs = st.tabs([
                    "📊 Summary", "🐛 Recent Issues",
                    "🎫 Recent Cases", "⏱️ Recent SLAs",
                ])

                with detail_tabs[0]:
                    sum_l, sum_m, sum_r = st.columns(3)
                    with sum_l:
                        df = get_issue_summary(scenario["scenario_code"])
                        st.markdown("**Issue Summary**")
                        if df.empty:
                            st.caption("No issues.")
                        else:
                            st.dataframe(df, use_container_width=True, hide_index=True)
                    with sum_m:
                        df = get_case_summary(scenario["scenario_code"])
                        st.markdown("**Case Summary**")
                        if df.empty:
                            st.caption("No cases.")
                        else:
                            st.dataframe(df, use_container_width=True, hide_index=True)
                    with sum_r:
                        df = get_sla_summary(scenario["scenario_code"])
                        st.markdown("**SLA Summary**")
                        if df.empty:
                            st.caption("No SLAs.")
                        else:
                            st.dataframe(df, use_container_width=True, hide_index=True)

                with detail_tabs[1]:
                    df = get_recent_issues(scenario["scenario_code"])
                    if df.empty:
                        st.caption("No recent issues.")
                    else:
                        st.dataframe(df, use_container_width=True, hide_index=True)

                with detail_tabs[2]:
                    df = get_recent_cases(scenario["scenario_code"])
                    if df.empty:
                        st.caption("No recent cases.")
                    else:
                        st.dataframe(df, use_container_width=True, hide_index=True)

                with detail_tabs[3]:
                    df = get_recent_slas(scenario["scenario_code"])
                    if df.empty:
                        st.caption("No recent SLAs.")
                    else:
                        st.dataframe(df, use_container_width=True, hide_index=True)

            st.caption(
                f"👉 After running, investigate on **{scenario['primary_page']}**"
            )


# ═══════════════════════════════════════════════════════════════════════
# TAB 2 — OPERATIONAL SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════

with tab_snapshot:
    st.subheader("📊 Operational Snapshot")
    st.caption(
        "System-wide view of all scenario artifacts. Compare which "
        "scenarios have been run, how many artifacts each generated, "
        "and the overall operational posture."
    )

    # ── Scenario comparison chart ──
    all_counts_df = get_all_scenario_counts()

    if all_counts_df["Total Artifacts"].sum() == 0:
        st.info(
            "🎛️ No scenarios have been executed yet. Run one from the "
            "Launcher tab or the sidebar Quick Launch."
        )
    else:
        st.markdown("### Artifact Counts by Scenario")

        melted = all_counts_df.melt(
            id_vars=["Scenario", "Title", "Priority"],
            value_vars=["Issues", "Cases", "SLAs"],
            var_name="Artifact Type",
            value_name="Count",
        )

        sc_domain = [s["scenario_code"] for s in SCENARIO_REGISTRY]
        sc_range = [SCENARIO_COLORS.get(s, "#BDC3C7") for s in sc_domain]

        comparison_chart = (
            alt.Chart(melted)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("Scenario:N", sort=sc_domain, title=None),
                y=alt.Y("Count:Q", title="Artifact Count"),
                color=alt.Color(
                    "Artifact Type:N",
                    scale=alt.Scale(
                        domain=["Issues", "Cases", "SLAs"],
                        range=["#FF4B4B", "#FFA500", "#4B9DFF"],
                    ),
                ),
                xOffset="Artifact Type:N",
                tooltip=["Scenario", "Artifact Type", "Count"],
            )
            .properties(height=320, title="Scenario Artifact Comparison")
        )
        st.altair_chart(comparison_chart, use_container_width=True)

        # ── Summary table ──
        st.dataframe(
            all_counts_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Total Artifacts": st.column_config.ProgressColumn(
                    "Total",
                    min_value=0,
                    max_value=int(all_counts_df["Total Artifacts"].max()) + 1,
                    format="%d",
                ),
            },
        )

        csv_snap = all_counts_df.to_csv(index=False)
        st.download_button(
            "⬇️ Download Snapshot CSV",
            csv_snap,
            "scenario_snapshot.csv",
            "text/csv",
            use_container_width=True,
        )

    # ── System-wide operational donut ──
    st.divider()
    st.markdown("### System-Wide Operational Posture")

    op_data = pd.DataFrame({
        "Category": ["Open Issues", "Open Cases", "At-Risk SLAs", "Breached SLAs"],
        "Count": [
            snapshot["open_issues"], snapshot["open_cases"],
            snapshot["at_risk_slas"], snapshot["breached_slas"],
        ],
    })
    op_data = op_data[op_data["Count"] > 0]

    if op_data.empty:
        st.success("✅ No open operational concerns.")
    else:
        op_chart = (
            alt.Chart(op_data)
            .mark_arc(innerRadius=50, cornerRadius=4)
            .encode(
                theta=alt.Theta("Count:Q"),
                color=alt.Color(
                    "Category:N",
                    scale=alt.Scale(
                        domain=["Open Issues", "Open Cases", "At-Risk SLAs", "Breached SLAs"],
                        range=["#4B9DFF", "#FFD700", "#FFA500", "#FF4B4B"],
                    ),
                ),
                tooltip=["Category", "Count"],
            )
            .properties(height=260, title="Open Operational Items")
        )
        st.altair_chart(op_chart, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 3 — SCENARIO CATALOG
# ═══════════════════════════════════════════════════════════════════════

with tab_catalog:
    st.subheader("📋 Scenario Catalog")
    st.caption(
        "Reference table of all 5 deterministic support scenarios with "
        "their domains, priorities, SLA targets, routing queues, and "
        "investigation pages."
    )

    catalog_data = []
    for s in SCENARIO_REGISTRY:
        catalog_data.append({
            "Icon": domain_icon(s["domain"]),
            "Scenario Code": s["scenario_code"],
            "Title": s["title"],
            "Domain": s["domain"],
            "Priority": priority_badge(s["priority"]),
            "SLA (hrs)": s["sla_hours"],
            "Queue": s["expected_queue"],
            "Investigation Page": s["primary_page"],
        })

    catalog_df = pd.DataFrame(catalog_data)
    st.dataframe(
        catalog_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Priority": st.column_config.TextColumn("Priority"),
            "SLA (hrs)": st.column_config.NumberColumn("SLA (hrs)", format="%d"),
        },
    )

    # ── Navigation map ──
    st.divider()
    st.markdown("### 🗺️ Scenario → Page Navigation Map")
    st.caption(
        "After running a scenario, navigate to the appropriate page "
        "to investigate the generated artifacts."
    )

    nav_left, nav_right = st.columns(2)

    with nav_left:
        st.markdown("""
| Scenario | Primary Page | What to Look For |
|---|---|---|
| `MISSING_INBOUND_FILE` | **File Monitoring** | Missing file alert, file gap in inventory |
| `DUPLICATE_ELIGIBILITY_RESEND` | **File Monitoring** → **Issue Triage** | Duplicate flag, resend detection issue |
| `CLAIM_INELIGIBLE_MEMBER` | **Issue Triage** | Claim-eligibility mismatch case |
        """)

    with nav_right:
        st.markdown("""
| Scenario | Primary Page | What to Look For |
|---|---|---|
| `ACCUMULATOR_EXCEEDS_OOP_MAX` | **Accumulator Reconciliation** | OOP breach in exception worklist |
| `FAMILY_ROLLUP_DISCREPANCY` | **Accumulator Reconciliation** | Family rollup delta on family tab |
        """)

    # ── Recommended demo flow ──
    st.divider()
    st.markdown("### 🎬 Recommended Demo Flow")
    st.markdown("""
1. **Start here** — Review the Operational Snapshot (no scenarios run yet = clean slate)
2. **Run one scenario** — Pick `ACCUMULATOR_EXCEEDS_OOP_MAX` for visual impact
3. **Check the delta** — See issues, cases, and SLAs created in the result banner
4. **Navigate to Accumulator Reconciliation** — Find the OOP breach in the exception worklist
5. **Investigate the member** — Drill into transactions, plan limits, and the linked case
6. **Return here** — Run `MISSING_INBOUND_FILE` for a second scenario
7. **Navigate to File Monitoring** — See the missing file alert and file gap
8. **Check Issue Triage** — See both cases in the SLA Watchlist

Total demo time: ~5 minutes. Every click shows real data.
    """)

    csv_cat = catalog_df.to_csv(index=False)
    st.download_button(
        "⬇️ Download Catalog CSV",
        csv_cat,
        "scenario_catalog.csv",
        "text/csv",
        use_container_width=True,
    )


# ═══════════════════════════════════════════════════════════════════════
# TAB 4 — RUN HISTORY
# ═══════════════════════════════════════════════════════════════════════

with tab_history:
    st.subheader("📜 Session Run History")
    st.caption(
        "Every scenario execution in this session, newest first. "
        "Shows before/after artifact deltas for each run."
    )

    history = st.session_state.get("scenario_run_history", [])

    if not history:
        st.info(
            "🎛️ No scenarios have been executed in this session yet. "
            "Use the Launcher tab or sidebar Quick Launch."
        )
    else:
        st.metric("Runs This Session", len(history))

        history_rows = []
        for item in history:
            if "error" in item:
                history_rows.append({
                    "Scenario": item["scenario_code"],
                    "Status": "❌ FAILED",
                    "Issues +": "—",
                    "Cases +": "—",
                    "SLAs +": "—",
                    "Started": format_timestamp(item.get("started_at")),
                    "Completed": "—",
                    "Error": item.get("error", ""),
                })
            else:
                d = item["delta"]
                history_rows.append({
                    "Scenario": item["scenario_code"],
                    "Status": "✅ SUCCESS",
                    "Issues +": d["issues_created"],
                    "Cases +": d["cases_created"],
                    "SLAs +": d["slas_created"],
                    "Started": format_timestamp(item["started_at"]),
                    "Completed": format_timestamp(item["completed_at"]),
                    "Error": "",
                })

        history_df = pd.DataFrame(history_rows)
        st.dataframe(
            history_df,
            use_container_width=True,
            hide_index=True,
        )

        # ── Cumulative artifacts chart ──
        if len(history) > 1:
            success_runs = [h for h in history if "error" not in h]
            if len(success_runs) > 1:
                cum_data = []
                running_issues, running_cases, running_slas = 0, 0, 0
                for idx, run in enumerate(reversed(success_runs)):
                    d = run["delta"]
                    running_issues += d["issues_created"]
                    running_cases += d["cases_created"]
                    running_slas += d["slas_created"]
                    cum_data.append({
                        "Run #": idx + 1,
                        "Cumulative Issues": running_issues,
                        "Cumulative Cases": running_cases,
                        "Cumulative SLAs": running_slas,
                    })

                cum_df = pd.DataFrame(cum_data)
                cum_melted = cum_df.melt(
                    id_vars=["Run #"],
                    value_vars=["Cumulative Issues", "Cumulative Cases", "Cumulative SLAs"],
                    var_name="Metric",
                    value_name="Count",
                )

                cum_chart = (
                    alt.Chart(cum_melted)
                    .mark_line(point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("Run #:O", title="Run Number"),
                        y=alt.Y("Count:Q", title="Cumulative Artifacts"),
                        color=alt.Color(
                            "Metric:N",
                            scale=alt.Scale(
                                domain=["Cumulative Issues", "Cumulative Cases", "Cumulative SLAs"],
                                range=["#FF4B4B", "#FFA500", "#4B9DFF"],
                            ),
                        ),
                        tooltip=["Run #", "Metric", "Count"],
                    )
                    .properties(height=260, title="Cumulative Artifact Generation")
                )
                st.altair_chart(cum_chart, use_container_width=True)

        csv_hist = history_df.to_csv(index=False)
        st.download_button(
            "⬇️ Download Run History CSV",
            csv_hist,
            "scenario_run_history.csv",
            "text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 5 — ARTIFACT EXPLORER
# ═══════════════════════════════════════════════════════════════════════

with tab_explorer:
    st.subheader("🔎 Artifact Explorer")
    st.caption(
        "Deep-dive into the artifacts generated by a specific scenario. "
        "Select a scenario to see its issues, cases, and SLA records."
    )

    explore_code = st.selectbox(
        "Select scenario to explore",
        options=[s["scenario_code"] for s in SCENARIO_REGISTRY],
        format_func=lambda code: next(
            (f"{domain_icon(s['domain'])} {s['title']} ({code})"
             for s in SCENARIO_REGISTRY if s["scenario_code"] == code),
            code,
        ),
    )

    explore_counts = get_scenario_counts(explore_code)

    ec1, ec2, ec3, ec4 = st.columns(4)
    ec1.metric("Issues", explore_counts["issues"])
    ec2.metric("Cases", explore_counts["cases"])
    ec3.metric("SLAs", explore_counts["slas"])
    ec4.metric(
        "Total",
        explore_counts["issues"] + explore_counts["cases"] + explore_counts["slas"],
    )

    if explore_counts["issues"] == 0 and explore_counts["cases"] == 0:
        st.info(
            f"No artifacts found for `{explore_code}`. "
            f"Run this scenario from the Launcher tab first."
        )
    else:
        exp_tabs = st.tabs([
            "🐛 Issues", "🎫 Cases", "⏱️ SLAs",
            "📊 Issue Summary", "📊 Case Summary", "📊 SLA Summary",
        ])

        with exp_tabs[0]:
            df = get_recent_issues(explore_code, limit=50)
            if df.empty:
                st.caption("No issues found.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.download_button(
                    "⬇️ Download Issues",
                    df.to_csv(index=False),
                    f"{explore_code}_issues.csv",
                    "text/csv",
                    use_container_width=True,
                )

        with exp_tabs[1]:
            df = get_recent_cases(explore_code, limit=50)
            if df.empty:
                st.caption("No cases found.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.download_button(
                    "⬇️ Download Cases",
                    df.to_csv(index=False),
                    f"{explore_code}_cases.csv",
                    "text/csv",
                    use_container_width=True,
                )

        with exp_tabs[2]:
            df = get_recent_slas(explore_code, limit=50)
            if df.empty:
                st.caption("No SLA records found.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.download_button(
                    "⬇️ Download SLAs",
                    df.to_csv(index=False),
                    f"{explore_code}_slas.csv",
                    "text/csv",
                    use_container_width=True,
                )

        with exp_tabs[3]:
            df = get_issue_summary(explore_code)
            if df.empty:
                st.caption("No issue summary available.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)

        with exp_tabs[4]:
            df = get_case_summary(explore_code)
            if df.empty:
                st.caption("No case summary available.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)

        with exp_tabs[5]:
            df = get_sla_summary(explore_code)
            if df.empty:
                st.caption("No SLA summary available.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 6 — HOW IT WORKS
# ═══════════════════════════════════════════════════════════════════════

with tab_howto:
    st.subheader("❓ How the Scenario Control Center Works")
    st.caption(
        "A plain-English explanation of what this page does, why it matters, "
        "and how scenarios work — written so anyone can understand."
    )

    st.markdown("""
---

### 🔥 What Is a Scenario?

Think of a scenario like a **fire drill** for a data system. In a real fire
drill, someone pulls the alarm on purpose so everyone can practice what
to do. Nobody is in real danger, but the response is real.

A scenario works the same way:
- We **create a specific problem on purpose** (like a missing file)
- The system **detects the problem automatically** (just like it would in real life)
- It **creates a trouble ticket** and **starts a deadline clock**
- Then the operations team **practices investigating and fixing it**

The difference from a real incident is that we control exactly what happens,
so the results are **predictable and repeatable** every time.

---

### 🎛️ The 5 Scenarios

| # | Scenario | What Happens | Like… |
|---|---|---|---|
| 1 | 🔴 **Missing Inbound File** | An expected file never arrives | A package that didn't get delivered |
| 2 | 🟣 **Duplicate Eligibility Resend** | The same file arrives twice | Getting the same letter in the mail twice |
| 3 | 🟠 **Claim for Ineligible Member** | A bill arrives for someone without insurance | A ticket for a passenger not on the flight list |
| 4 | 🔴 **Accumulator Exceeds OOP Max** | Someone's spending counter goes past the limit | A gas tank overflowing past "full" |
| 5 | 🔵 **Family Rollup Discrepancy** | Family total doesn't match individual totals | A budget where the categories don't add up |

---

### ⚙️ What Happens When You Run a Scenario?
You click "Run"
→ Scenario loader creates a specific data condition
→ System detects the problem automatically
→ data_quality_issues record created
→ support_cases record created (routed to right team)
→ sla_tracking record created (deadline starts)
→ You see the before/after delta on this page
    → Navigate to the investigation page
    → Triage, investigate, and resolve

    Every step creates **real database records** that you can query, filter,
and investigate on the other pages — just like a real production incident.

---

### 📊 What Are Artifacts?

Artifacts are the **records that a scenario creates**:

| Artifact | What It Is | Where to See It |
|---|---|---|
| **Issue** | The raw problem detection | Issue Triage → Data Quality Issues |
| **Case** | The trouble ticket | Issue Triage → Support Queue |
| **SLA** | The deadline record | Issue Triage → SLA Watchlist |

The delta counts ("+1 issue, +1 case, +1 SLA") tell you exactly what
was created each time you run a scenario.

---

### 📋 What Does This Page Do?

1. **Scenario Launcher** — Run any scenario with one click. See
   before/after artifact deltas and navigate to investigation pages.
2. **Operational Snapshot** — Compare all scenarios side by side with
   artifact count charts and system-wide operational posture.
3. **Scenario Catalog** — Reference table with scenario details,
   priorities, SLA targets, queues, and navigation map.
4. **Run History** — Every execution in this session with cumulative
   artifact generation tracking.
5. **Artifact Explorer** — Deep-dive into a specific scenario's
   generated issues, cases, and SLA records.

---

### 🔑 Key Terms

| Term | What It Means |
|---|---|
| **Deterministic** | Same scenario always produces the same type of result |
| **Scenario code** | The unique identifier like `MISSING_INBOUND_FILE` |
| **Delta** | The difference in artifact counts before vs after running |
| **Artifact** | A database record created by the scenario (issue, case, SLA) |
| **Primary page** | The best page to investigate after running the scenario |
| **Assignment queue** | Which team the case is routed to |
| **SLA target** | How many hours the team has to resolve the case |
| **Before/after** | Snapshot of counts taken before and after execution |

---

### 🚀 Quick Start

1. Look at the **Operational Snapshot** metrics — is the system clean?
2. Go to **Scenario Launcher** and click ▶️ Run on any scenario
3. Check the **delta banner** — were issues, cases, and SLAs created?
4. Click the **navigation hint** to go to the right investigation page
5. Come back and run another scenario to build up operational volume
6. Use **Run History** to track everything you've done
7. Use **Artifact Explorer** to deep-dive into any scenario's records

💡 **Power move:** Click **Run ALL Scenarios** in the sidebar to populate
the entire system at once, then tour all the investigation pages.
    """)

    st.info(
        "💡 **Tip:** Use the sidebar Quick Launch for fast one-click "
        "execution. The 'Run ALL Scenarios' button populates the entire "
        "system in seconds."
    )

    with st.expander("🔗 Connected Pages — Where to Go Next", expanded=False):
        st.markdown("""
**After running scenarios, visit these pages:**

| Page | What You'll Find |
|---|---|
| **File Monitoring** | Missing file alerts, duplicate detection, processing runs |
| **Issue Triage** | Support case queue, SLA watchlist, case investigation |
| **Accumulator Reconciliation** | OOP breaches, family rollup discrepancies |

**The full investigation flow:**
1. **Scenario Control Center** → trigger the incident
2. **Issue Triage** → triage the case, check the SLA
3. **File Monitoring** or **Accumulator Reconciliation** → investigate root cause
4. **Issue Triage** → document resolution and close the case
        """)


# ═══════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════

st.divider()
st.caption(
    "Scenario Control Center · Eligibility & Accumulator Operations "
    "Command Center · Data is simulated — no real PHI"
)