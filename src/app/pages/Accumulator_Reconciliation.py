import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

"""
Accumulator Reconciliation Page
================================
Operational hub for investigating and resolving accumulator exceptions:
OOP max breaches, family rollup discrepancies, linked issues, and support
case workflows — all in one investigation-oriented, portfolio-grade view.

Design principles
-----------------
- Exception-first: surface problems before raw data
- Investigation-oriented: guide users through root cause analysis
- Schema-accurate: every query matches the confirmed database schema
- Accessible: plain-language explanations anyone can follow
- Interactive: charts, progress bars, tabs, downloads, expandable guidance
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

import pandas as pd
import streamlit as st
import altair as alt

from src.common.db import fetch_all
from src.app.utils import to_dataframe, add_age_hours_column, bool_flag_to_label

# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION — must be the very first Streamlit command
# ═══════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Accumulator Reconciliation",
    page_icon="💰",
    layout="wide",
)

# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════

# Statuses that mean "still needs work"
OPEN_CASE_STATUSES = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"}
OPEN_ISSUE_STATUSES = {"OPEN", "ACKNOWLEDGED"}

# Visual badges for each exception category
EXCEPTION_CONFIG = {
    "OOP MAX BREACH":            {"icon": "🔴", "rank": 1, "color": "#FF4B4B"},
    "FAMILY ROLLUP DISCREPANCY": {"icon": "🟠", "rank": 2, "color": "#FFA500"},
    "OPEN RECON CASE":           {"icon": "🟡", "rank": 3, "color": "#FFD700"},
    "OPEN ACCUMULATOR ISSUE":    {"icon": "🔵", "rank": 4, "color": "#4B9DFF"},
    "NORMAL":                    {"icon": "🟢", "rank": 5, "color": "#2ECC71"},
}

SEVERITY_ICONS = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🔵"}


# ═══════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def safe_val(value, fallback=0):
    """Return a numeric-safe value.  None and NaN become *fallback*."""
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


def fmt_currency(value, fallback="—"):
    """Format a number as $X,XXX.XX for display."""
    try:
        num = float(safe_val(value, None))
        return f"${num:,.2f}"
    except (TypeError, ValueError):
        return fallback


def fmt_number(value, fallback="—"):
    """Format a number with commas for display."""
    try:
        num = float(safe_val(value, None))
        return f"{int(num):,}" if num == int(num) else f"{num:,.2f}"
    except (TypeError, ValueError):
        return fallback


def pct_of_max(accum, maximum):
    """Calculate what percentage of a maximum has been reached (capped display at 100 for progress bars)."""
    a = float(safe_val(accum, 0))
    m = float(safe_val(maximum, 0))
    if m <= 0:
        return 0.0
    return round((a / m) * 100, 1)


def progress_bar_value(pct):
    """Return a 0.0–1.0 float for st.progress(), clamped from a 0–100 percentage."""
    return min(max(float(pct) / 100.0, 0.0), 1.0)


def exception_badge(exception_type):
    """Return an icon + label string for an exception type."""
    cfg = EXCEPTION_CONFIG.get(exception_type, {"icon": "⚪"})
    return f"{cfg['icon']} {exception_type}"


def severity_icon(sev):
    """Return an icon for a severity level."""
    return SEVERITY_ICONS.get(str(sev).upper(), "⚪") if sev else "⚪"


def classify_recon_exception(row):
    """Classify a snapshot row into exactly one exception category, highest priority wins."""
    if float(safe_val(row.get("individual_oop_breach_amount"), 0)) > 0:
        return "OOP MAX BREACH"
    if float(safe_val(row.get("family_oop_breach_amount"), 0)) > 0:
        return "OOP MAX BREACH"
    if abs(float(safe_val(row.get("deductible_diff"), 0))) > 0:
        return "FAMILY ROLLUP DISCREPANCY"
    if abs(float(safe_val(row.get("oop_diff"), 0))) > 0:
        return "FAMILY ROLLUP DISCREPANCY"
    if int(safe_val(row.get("open_case_count"), 0)) > 0:
        return "OPEN RECON CASE"
    if int(safe_val(row.get("open_issue_count"), 0)) > 0:
        return "OPEN ACCUMULATOR ISSUE"
    return "NORMAL"


def build_snapshot_label(row):
    """Human-readable label for the member investigation dropdown."""
    exc = row.get("recon_exception", "NORMAL")
    icon = EXCEPTION_CONFIG.get(exc, {}).get("icon", "⚪")
    return (
        f"{icon} Member {safe_text(row.get('member_id'))} · "
        f"Family {safe_text(row.get('family_id'))} · "
        f"{safe_text(row.get('plan_name'))} · "
        f"BY {safe_text(row.get('benefit_year'))}"
    )


def safe_col_list(desired_cols, available_cols):
    """Return only the columns that actually exist in a dataframe."""
    return [c for c in desired_cols if c in available_cols]


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING FUNCTIONS  (cached, 5-minute TTL)
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner="Loading accumulator snapshots …")
def load_snapshots():
    """Load member-level accumulator snapshots with plan thresholds."""
    rows = fetch_all("""
        SELECT
            s.snapshot_id, s.member_id, s.family_id, s.client_id,
            c.client_code,
            s.plan_id, p.plan_code, p.plan_name, p.plan_type,
            p.benefit_year  AS plan_benefit_year,
            p.individual_deductible, p.family_deductible,
            p.individual_oop_max,    p.family_oop_max,
            p.family_accumulation_type,
            s.benefit_year,
            s.individual_deductible_accum, s.family_deductible_accum,
            s.individual_oop_accum,        s.family_oop_accum,
            s.individual_deductible_met_flag, s.family_deductible_met_flag,
            s.individual_oop_met_flag,        s.family_oop_met_flag,
            s.snapshot_ts
        FROM accumulator_snapshots s
        LEFT JOIN clients c ON s.client_id = c.client_id
        LEFT JOIN benefit_plans   p ON s.plan_id   = p.plan_id
        ORDER BY s.benefit_year DESC, s.snapshot_id DESC
    """)
    df = to_dataframe(rows)
    if df.empty:
        return df

    df = add_age_hours_column(df, "snapshot_ts", "snapshot_age_hours")

    # Human-readable met-flag labels
    for col in [
        "individual_deductible_met_flag", "family_deductible_met_flag",
        "individual_oop_met_flag",        "family_oop_met_flag",
    ]:
        if col in df.columns:
            df[f"{col}_label"] = df[col].apply(bool_flag_to_label)

    # Breach amounts — how far over OOP max
    df["individual_oop_breach_amount"] = (
        df["individual_oop_accum"] - df["individual_oop_max"]
    ).clip(lower=0).round(2)
    df["family_oop_breach_amount"] = (
        df["family_oop_accum"] - df["family_oop_max"]
    ).clip(lower=0).round(2)

    # Friendly flags
    df["individual_over_max"] = (df["individual_oop_breach_amount"] > 0).map(
        {True: "⚠️ Yes", False: "✅ No"}
    )
    df["family_over_max"] = (df["family_oop_breach_amount"] > 0).map(
        {True: "⚠️ Yes", False: "✅ No"}
    )

    # OOP usage percentages
    df["ind_oop_pct"] = df.apply(
        lambda r: pct_of_max(r.get("individual_oop_accum"), r.get("individual_oop_max")), axis=1
    )
    df["fam_oop_pct"] = df.apply(
        lambda r: pct_of_max(r.get("family_oop_accum"), r.get("family_oop_max")), axis=1
    )

    return df


@st.cache_data(ttl=300, show_spinner="Loading accumulator transactions …")
def load_transactions():
    """Load every accumulator transaction with client/plan context."""
    rows = fetch_all("""
        SELECT
            at.accumulator_txn_id, at.member_id, at.family_id, at.client_id,
            c.client_code,
            at.plan_id, p.plan_code, p.plan_name,
            at.claim_record_id, at.benefit_year, at.accumulator_type,
            at.delta_amount, at.service_date, at.source_type,
            at.source_file_id, at.created_at
        FROM accumulator_transactions at
        LEFT JOIN clients c ON at.client_id = c.client_id
        LEFT JOIN benefit_plans   p ON at.plan_id   = p.plan_id
        ORDER BY at.created_at DESC, at.accumulator_txn_id DESC
    """)
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "created_at", "txn_age_hours")
    return df


@st.cache_data(ttl=300, show_spinner="Loading claims …")
def load_claims():
    """Load claim records for linking to accumulator transactions."""
    rows = fetch_all("""
        SELECT
            cl.claim_record_id, cl.claim_id, cl.line_id,
            cl.member_id, cl.subscriber_id, cl.client_id,
            c.client_code,
            cl.plan_id, p.plan_name,
            cl.vendor_id, v.vendor_code,
            cl.service_date, cl.paid_date,
            cl.allowed_amount, cl.paid_amount, cl.member_responsibility,
            cl.deductible_amount, cl.coinsurance_amount, cl.copay_amount,
            cl.preventive_flag, cl.reversal_flag, cl.claim_status,
            cl.source_file_id, cl.created_at
        FROM claims cl
        LEFT JOIN clients c ON cl.client_id = c.client_id
        LEFT JOIN benefit_plans   p ON cl.plan_id   = p.plan_id
        LEFT JOIN vendors v ON cl.vendor_id  = v.vendor_id
        ORDER BY cl.service_date DESC, cl.claim_record_id DESC
    """)
    return to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner="Loading accumulator issues …")
def load_accumulator_issues():
    """Load data quality issues related to accumulators."""
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status,
            i.client_id, c.client_code,
            i.vendor_id, v.vendor_code,
            i.file_id, i.run_id,
            i.member_id, i.claim_record_id,
            i.issue_message, i.issue_description, i.detected_at
        FROM data_quality_issues i
        LEFT JOIN clients c ON i.client_id = c.client_id
        LEFT JOIN vendors v ON i.vendor_id  = v.vendor_id
        WHERE i.issue_code IN (
                'ACCUMULATOR_EXCEEDS_OOP_MAX',
                'FAMILY_ROLLUP_DISCREPANCY'
              )
           OR i.issue_type = 'ACCUMULATOR'
        ORDER BY i.detected_at DESC, i.issue_id DESC
    """)
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "detected_at", "issue_age_hours")
    return df


@st.cache_data(ttl=300, show_spinner="Loading accumulator support cases …")
def load_accumulator_cases():
    """Load support cases tied to accumulator scenarios."""
    rows = fetch_all("""
        SELECT
            sc.case_id, sc.case_number, sc.issue_id,
            sc.client_id, c.client_code,
            sc.vendor_id, v.vendor_code,
            sc.file_id,
            sc.run_id AS processing_run_id,
            sc.member_id, sc.claim_record_id,
            sc.case_type, sc.priority, sc.severity, sc.status,
            sc.assigned_team, sc.assigned_to,
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
        WHERE sc.case_type IN (
                'ACCUMULATOR_EXCEEDS_OOP_MAX',
                'FAMILY_ROLLUP_DISCREPANCY'
              )
           OR sc.assigned_team = 'ops_recon_queue'
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


@st.cache_data(ttl=300, show_spinner="Building family rollup comparison …")
def load_family_rollup():
    """
    Build a family-level reconciliation view: compare the recorded
    family accumulator total against the actual sum of individual
    member-level accumulators.
    """
    rows = fetch_all("""
        SELECT
            s.family_id, s.client_id, c.client_code,
            s.plan_id, p.plan_code, p.plan_name,
            p.family_accumulation_type,
            p.family_deductible, p.family_oop_max,
            s.benefit_year,
            COUNT(DISTINCT s.member_id)           AS member_count,
            SUM(s.individual_deductible_accum)    AS summed_member_deductible,
            MAX(s.family_deductible_accum)         AS recorded_family_deductible,
            SUM(s.individual_oop_accum)            AS summed_member_oop,
            MAX(s.family_oop_accum)                AS recorded_family_oop
        FROM accumulator_snapshots s
        LEFT JOIN clients c ON s.client_id = c.client_id
        LEFT JOIN benefit_plans   p ON s.plan_id   = p.plan_id
        GROUP BY
            s.family_id, s.client_id, c.client_code,
            s.plan_id, p.plan_code, p.plan_name,
            p.family_accumulation_type,
            p.family_deductible, p.family_oop_max,
            s.benefit_year
        ORDER BY s.benefit_year DESC, s.family_id
    """)
    df = to_dataframe(rows)
    if df.empty:
        return df

    df["deductible_diff"] = (
        df["summed_member_deductible"] - df["recorded_family_deductible"]
    ).round(2)
    df["oop_diff"] = (
        df["summed_member_oop"] - df["recorded_family_oop"]
    ).round(2)
    df["has_discrepancy"] = (
        (df["deductible_diff"] != 0) | (df["oop_diff"] != 0)
    ).map({True: "⚠️ Yes", False: "✅ No"})

    return df


# ═══════════════════════════════════════════════════════════════════════
# LOAD ALL DATA
# ═══════════════════════════════════════════════════════════════════════

snapshots_df        = load_snapshots()
txns_df             = load_transactions()
claims_df           = load_claims()
accum_issues_df     = load_accumulator_issues()
accum_cases_df      = load_accumulator_cases()
family_rollup_df    = load_family_rollup()


# ═══════════════════════════════════════════════════════════════════════
# BUILD ENRICHED RECONCILIATION VIEW
# ═══════════════════════════════════════════════════════════════════════

def build_recon_dataframe(snap_df, fam_df, issues_df, cases_df):
    """
    Merge snapshots with family rollup deltas, issue counts, and case
    counts to produce the master reconciliation dataframe.
    """
    if snap_df.empty:
        return pd.DataFrame()

    recon = snap_df.copy()

    # --- merge family rollup deltas ---
    if not fam_df.empty:
        rollup_cols = [
            "family_id", "plan_id", "benefit_year", "member_count",
            "summed_member_deductible", "recorded_family_deductible",
            "deductible_diff", "summed_member_oop", "recorded_family_oop",
            "oop_diff", "has_discrepancy",
        ]
        rollup_merge = fam_df[safe_col_list(rollup_cols, fam_df.columns)].copy()
        recon = recon.merge(
            rollup_merge,
            on=safe_col_list(["family_id", "plan_id", "benefit_year"], rollup_merge.columns),
            how="left",
        )

    # --- merge issue counts per member ---
    if not issues_df.empty:
        issue_counts = (
            issues_df.groupby("member_id")
            .agg(
                accumulator_issue_count=("issue_id", "count"),
                open_issue_count=(
                    "status",
                    lambda s: int(s.isin(OPEN_ISSUE_STATUSES).sum()),
                ),
            )
            .reset_index()
        )
        recon = recon.merge(issue_counts, on="member_id", how="left")
    else:
        recon["accumulator_issue_count"] = 0
        recon["open_issue_count"] = 0

    # --- merge case counts per member ---
    if not cases_df.empty:
        case_counts = (
            cases_df.groupby("member_id")
            .agg(
                accumulator_case_count=("case_id", "count"),
                open_case_count=(
                    "status",
                    lambda s: int(s.isin(OPEN_CASE_STATUSES).sum()),
                ),
            )
            .reset_index()
        )
        recon = recon.merge(case_counts, on="member_id", how="left")
    else:
        recon["accumulator_case_count"] = 0
        recon["open_case_count"] = 0

    # --- fill NaN counts ---
    int_cols = [
        "member_count", "accumulator_issue_count", "open_issue_count",
        "accumulator_case_count", "open_case_count",
    ]
    for col in int_cols:
        if col in recon.columns:
            recon[col] = recon[col].fillna(0).astype(int)

    float_cols = [
        "summed_member_deductible", "recorded_family_deductible",
        "deductible_diff", "summed_member_oop", "recorded_family_oop",
        "oop_diff",
    ]
    for col in float_cols:
        if col in recon.columns:
            recon[col] = recon[col].fillna(0).round(2)

    # --- classify each row ---
    recon["recon_exception"] = recon.apply(classify_recon_exception, axis=1)
    recon["exception_badge"] = recon["recon_exception"].apply(exception_badge)

    # --- sort: worst exceptions first ---
    recon["exception_rank"] = recon["recon_exception"].map(
        {k: v["rank"] for k, v in EXCEPTION_CONFIG.items()}
    ).fillna(99)

    recon = recon.sort_values(
        by=[
            "exception_rank", "open_case_count", "open_issue_count",
            "individual_oop_breach_amount", "family_oop_breach_amount",
            "snapshot_id",
        ],
        ascending=[True, False, False, False, False, False],
    )

    return recon


recon_df = build_recon_dataframe(
    snapshots_df, family_rollup_df, accum_issues_df, accum_cases_df
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

    # --- build filter options from recon data ---
    if not recon_df.empty:
        client_opts = sorted(recon_df["client_code"].dropna().unique().tolist())
        plan_opts = sorted(recon_df["plan_name"].dropna().unique().tolist())
        by_opts = sorted(
            recon_df["benefit_year"].dropna().unique().tolist(), reverse=True
        )
        exc_opts = sorted(recon_df["recon_exception"].dropna().unique().tolist())
    else:
        client_opts, plan_opts, by_opts, exc_opts = [], [], [], []

    sel_clients = st.multiselect("Client", client_opts, default=client_opts)
    sel_plans = st.multiselect("Plan", plan_opts, default=plan_opts)
    sel_years = st.multiselect("Benefit Year", by_opts, default=by_opts)
    sel_exceptions = st.multiselect(
        "Exception Type", exc_opts, default=exc_opts
    )

    st.divider()
    st.markdown("**Quick Filters**")
    only_breaches = st.checkbox("🔴 OOP max breaches only", value=False)
    only_discrepancies = st.checkbox("🟠 Family rollup discrepancies only", value=False)
    only_open_issues = st.checkbox("🔵 Members with open issues only", value=False)
    only_open_cases = st.checkbox("🟡 Members with open cases only", value=False)


# ═══════════════════════════════════════════════════════════════════════
# APPLY FILTERS
# ═══════════════════════════════════════════════════════════════════════

def apply_filters(df):
    """Apply all sidebar filters to the reconciliation dataframe."""
    if df.empty:
        return df

    out = df.copy()

    if sel_clients:
        out = out[out["client_code"].isin(sel_clients)]
    if sel_plans:
        out = out[out["plan_name"].isin(sel_plans)]
    if sel_years:
        out = out[out["benefit_year"].isin(sel_years)]
    if sel_exceptions:
        out = out[out["recon_exception"].isin(sel_exceptions)]
    if only_breaches:
        out = out[
            (out["individual_oop_breach_amount"] > 0)
            | (out["family_oop_breach_amount"] > 0)
        ]
    if only_discrepancies:
        disc_mask = pd.Series(False, index=out.index)
        if "deductible_diff" in out.columns:
            disc_mask = disc_mask | (out["deductible_diff"] != 0)
        if "oop_diff" in out.columns:
            disc_mask = disc_mask | (out["oop_diff"] != 0)
        out = out[disc_mask]
    if only_open_issues:
        out = out[out["open_issue_count"] > 0]
    if only_open_cases:
        out = out[out["open_case_count"] > 0]

    return out


filtered_df = apply_filters(recon_df)

# Also filter the family rollup separately for its own views
filtered_family_df = family_rollup_df.copy()
if not filtered_family_df.empty:
    if sel_clients:
        filtered_family_df = filtered_family_df[
            filtered_family_df["client_code"].isin(sel_clients)
        ]
    if sel_plans:
        filtered_family_df = filtered_family_df[
            filtered_family_df["plan_name"].isin(sel_plans)
        ]
    if sel_years:
        filtered_family_df = filtered_family_df[
            filtered_family_df["benefit_year"].isin(sel_years)
        ]
    if only_discrepancies:
        filtered_family_df = filtered_family_df[
            filtered_family_df["has_discrepancy"] == "⚠️ Yes"
        ]


# ═══════════════════════════════════════════════════════════════════════
# COMPUTE SUMMARY METRICS
# ═══════════════════════════════════════════════════════════════════════

total_snapshots = len(snapshots_df) if not snapshots_df.empty else 0
total_txns = len(txns_df) if not txns_df.empty else 0

open_issues = (
    int(accum_issues_df["status"].isin(OPEN_ISSUE_STATUSES).sum())
    if not accum_issues_df.empty else 0
)
open_cases = (
    int(accum_cases_df["status"].isin(OPEN_CASE_STATUSES).sum())
    if not accum_cases_df.empty else 0
)
breached_sla_count = (
    int(accum_cases_df["is_breached"].fillna(0).sum())
    if not accum_cases_df.empty and "is_breached" in accum_cases_df.columns
    else 0
)
at_risk_sla_count = (
    int(accum_cases_df["is_at_risk"].fillna(0).sum())
    if not accum_cases_df.empty and "is_at_risk" in accum_cases_df.columns
    else 0
)

oop_breach_count = (
    int(
        (
            (recon_df["individual_oop_breach_amount"] > 0)
            | (recon_df["family_oop_breach_amount"] > 0)
        ).sum()
    )
    if not recon_df.empty else 0
)
family_disc_count = (
    int(
        (
            (family_rollup_df["deductible_diff"] != 0)
            | (family_rollup_df["oop_diff"] != 0)
        ).sum()
    )
    if not family_rollup_df.empty else 0
)
total_exceptions = (
    int((recon_df["recon_exception"] != "NORMAL").sum())
    if not recon_df.empty else 0
)
filtered_count = len(filtered_df)


# ═══════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════

st.title("💰 Accumulator Reconciliation")
st.caption(
    "Investigate deductible and out-of-pocket accumulator balances. "
    "Surface OOP max breaches, family rollup mismatches, linked issues, "
    "and support case workflows — all from one investigation dashboard."
)

# ── Key Findings Auto-Summary ──
findings = []
if oop_breach_count > 0:
    findings.append(f"🔴 **{oop_breach_count}** OOP max breach(es) detected")
if family_disc_count > 0:
    findings.append(f"🟠 **{family_disc_count}** family rollup discrepancy(ies)")
if breached_sla_count > 0:
    findings.append(f"⛔ **{breached_sla_count}** SLA(s) breached")
if at_risk_sla_count > 0:
    findings.append(f"⚠️ **{at_risk_sla_count}** SLA(s) at risk")
if open_cases > 0:
    findings.append(f"🟡 **{open_cases}** open recon case(s)")

if findings:
    st.warning("**Key Findings:**  " + "  ·  ".join(findings))
else:
    st.success(
        "✅ **All clear.** No OOP breaches, family discrepancies, or open "
        "cases detected."
    )

# ── Top-level metrics ──
m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
m1.metric("Snapshots", fmt_number(total_snapshots))
m2.metric("Transactions", fmt_number(total_txns))
m3.metric("Open Issues", fmt_number(open_issues))
m4.metric("Open Cases", fmt_number(open_cases))
m5.metric("OOP Breaches", fmt_number(oop_breach_count))
m6.metric("Family Discrepancies", fmt_number(family_disc_count))
m7.metric("Showing", f"{filtered_count} / {len(recon_df) if not recon_df.empty else 0}")

st.divider()


# ═══════════════════════════════════════════════════════════════════════
# TABBED LAYOUT
# ═══════════════════════════════════════════════════════════════════════

tab_worklist, tab_investigate, tab_breaches, tab_family, tab_txns, tab_howto = st.tabs([
    "📋 Exception Worklist",
    "🔎 Member Investigation",
    "🔴 OOP Breaches",
    "👨‍👩‍👧‍👦 Family Rollup",
    "📒 Transactions & Claims",
    "❓ How It Works",
])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1 — EXCEPTION WORKLIST
# ═══════════════════════════════════════════════════════════════════════

with tab_worklist:
    st.subheader("📋 Reconciliation Exception Worklist")
    st.caption(
        "Every accumulator snapshot ranked by urgency. OOP max breaches appear "
        "first, then family rollup mismatches, then open cases and issues. "
        "Records with no problems appear last."
    )

    if filtered_df.empty:
        st.info("No accumulator records match the current filters.")
    else:
        # ── Exception distribution chart ──
        # value_counts().reset_index() always produces exactly 2 columns;
        # assign names directly to avoid pandas-version rename fragility.
        exc_counts = filtered_df["recon_exception"].value_counts().reset_index()
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
                    y=alt.Y("Count:Q", title="Snapshot Count"),
                    color=alt.Color(
                        "Exception:N",
                        scale=alt.Scale(domain=color_domain, range=color_range),
                        legend=None,
                    ),
                    tooltip=["Exception", "Count"],
                )
                .properties(height=250)
            )
            st.altair_chart(chart, use_container_width=True)

        with col_legend:
            st.markdown("**Legend**")
            for name, cfg in EXCEPTION_CONFIG.items():
                count_for = int(
                    exc_counts.loc[exc_counts["Exception"] == name, "Count"].sum()
                ) if name in exc_counts["Exception"].values else 0
                st.markdown(f"{cfg['icon']} **{name}**: {count_for}")

        # ── Worklist table ──
        worklist_cols = [
            "exception_badge", "snapshot_id", "member_id", "family_id",
            "client_code", "plan_name", "benefit_year",
            "individual_oop_accum", "individual_oop_max",
            "individual_oop_breach_amount",
            "family_oop_accum", "family_oop_max", "family_oop_breach_amount",
            "deductible_diff", "oop_diff",
            "open_issue_count", "open_case_count", "snapshot_ts",
        ]
        st.dataframe(
            filtered_df[safe_col_list(worklist_cols, filtered_df.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "exception_badge": st.column_config.TextColumn("Exception"),
                "individual_oop_breach_amount": st.column_config.NumberColumn(
                    "Ind OOP Breach $", format="$%.2f"
                ),
                "family_oop_breach_amount": st.column_config.NumberColumn(
                    "Fam OOP Breach $", format="$%.2f"
                ),
            },
        )

        # ── Download ──
        csv = filtered_df[
            safe_col_list(worklist_cols, filtered_df.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Worklist CSV",
            csv,
            "accumulator_worklist.csv",
            "text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 2 — MEMBER INVESTIGATION
# ═══════════════════════════════════════════════════════════════════════

with tab_investigate:
    st.subheader("🔎 Selected Member / Family Investigation")
    st.caption(
        "Pick a member from the dropdown to see their full accumulator picture: "
        "current balances, plan limits, linked claims, transactions, issues, "
        "and support cases — everything you need for root cause analysis."
    )

    if filtered_df.empty:
        st.info("No records available. Run a scenario or adjust filters.")
    else:
        # ── Snapshot selector ──
        selectable = filtered_df.copy()
        selectable["label"] = selectable.apply(build_snapshot_label, axis=1)

        selected_label = st.selectbox(
            "Choose a member snapshot to investigate",
            options=selectable["label"].tolist(),
            help="Members with exceptions (🔴🟠🟡🔵) appear first.",
        )
        row = selectable[selectable["label"] == selected_label].iloc[0].to_dict()

        sel_member   = row.get("member_id")
        sel_family   = row.get("family_id")
        sel_plan     = row.get("plan_id")
        sel_year     = row.get("benefit_year")
        sel_exc      = row.get("recon_exception", "NORMAL")

        # ── Operational guidance banner ──
        if sel_exc == "OOP MAX BREACH":
            st.error(
                "🔴 **OOP Max Breach** — This member's out-of-pocket spending exceeds "
                "the plan maximum. Check for duplicate claims, missing reversals, "
                "or stale plan thresholds."
            )
        elif sel_exc == "FAMILY ROLLUP DISCREPANCY":
            st.warning(
                "🟠 **Family Rollup Discrepancy** — The family total doesn't match "
                "the sum of individual members. Check for mid-year member changes "
                "or rollup logic errors."
            )
        elif sel_exc == "OPEN RECON CASE":
            st.info(
                "🟡 **Open Recon Case** — There's an active support case for this "
                "member. Review the case details and SLA status below."
            )
        elif sel_exc == "OPEN ACCUMULATOR ISSUE":
            st.info(
                "🔵 **Open Accumulator Issue** — A data quality issue is flagged "
                "but no support case exists yet. Review the issue details below."
            )
        else:
            st.success(
                "🟢 **Normal** — No active exceptions. This record is healthy."
            )

        # ── Context cards ──
        ctx1, ctx2, ctx3 = st.columns([1.2, 1.2, 1])

        with ctx1:
            st.markdown("##### 👤 Member & Family")
            st.markdown(f"**Member ID:** `{safe_text(sel_member)}`")
            st.markdown(f"**Family ID:** `{safe_text(sel_family)}`")
            st.markdown(f"**Client:** {safe_text(row.get('client_code'))}")
            st.markdown(f"**Benefit Year:** {safe_text(sel_year)}")
            st.markdown(f"**Snapshot Taken:** {safe_text(row.get('snapshot_ts'))}")

        with ctx2:
            st.markdown("##### 📋 Plan Details")
            st.markdown(f"**Plan:** {safe_text(row.get('plan_name'))} (`{safe_text(row.get('plan_code'))}`)")
            st.markdown(f"**Type:** {safe_text(row.get('plan_type'))}")
            st.markdown(f"**Family Accum Type:** {safe_text(row.get('family_accumulation_type'))}")
            st.markdown(f"**Ind OOP Max:** {fmt_currency(row.get('individual_oop_max'))}")
            st.markdown(f"**Fam OOP Max:** {fmt_currency(row.get('family_oop_max'))}")
            st.markdown(f"**Ind Deductible:** {fmt_currency(row.get('individual_deductible'))}")
            st.markdown(f"**Fam Deductible:** {fmt_currency(row.get('family_deductible'))}")

        with ctx3:
            st.markdown("##### ⚡ Reconciliation Status")
            st.markdown(f"**Exception:** {exception_badge(sel_exc)}")
            st.markdown(
                f"**Ind OOP Breach:** {fmt_currency(row.get('individual_oop_breach_amount'))}"
            )
            st.markdown(
                f"**Fam OOP Breach:** {fmt_currency(row.get('family_oop_breach_amount'))}"
            )
            st.markdown(f"**Ded Diff:** {fmt_currency(row.get('deductible_diff'))}")
            st.markdown(f"**OOP Diff:** {fmt_currency(row.get('oop_diff'))}")
            st.markdown(
                f"**Open Issues / Cases:** "
                f"{safe_val(row.get('open_issue_count'), 0)} / "
                f"{safe_val(row.get('open_case_count'), 0)}"
            )

        # ── Visual progress bars — OOP usage ──
        st.markdown("##### 📊 OOP Usage Gauges")
        g1, g2 = st.columns(2)

        ind_pct = pct_of_max(row.get("individual_oop_accum"), row.get("individual_oop_max"))
        fam_pct = pct_of_max(row.get("family_oop_accum"), row.get("family_oop_max"))

        with g1:
            st.markdown(
                f"**Individual OOP:** {fmt_currency(row.get('individual_oop_accum'))} "
                f"of {fmt_currency(row.get('individual_oop_max'))} "
                f"({ind_pct}%)"
            )
            st.progress(progress_bar_value(ind_pct))
            if ind_pct > 100:
                st.error(f"⚠️ Over max by {fmt_currency(row.get('individual_oop_breach_amount'))}")

        with g2:
            st.markdown(
                f"**Family OOP:** {fmt_currency(row.get('family_oop_accum'))} "
                f"of {fmt_currency(row.get('family_oop_max'))} "
                f"({fam_pct}%)"
            )
            st.progress(progress_bar_value(fam_pct))
            if fam_pct > 100:
                st.error(f"⚠️ Over max by {fmt_currency(row.get('family_oop_breach_amount'))}")

        # ── Accumulator metrics ──
        st.markdown("##### 💰 Current Accumulator Balances")
        a1, a2, a3, a4 = st.columns(4)
        a1.metric(
            "Ind Deductible",
            fmt_currency(row.get("individual_deductible_accum")),
            help="How much of the individual deductible has been used",
        )
        a2.metric(
            "Fam Deductible",
            fmt_currency(row.get("family_deductible_accum")),
            help="How much of the family deductible has been used",
        )
        a3.metric(
            "Ind OOP",
            fmt_currency(row.get("individual_oop_accum")),
            help="Total individual out-of-pocket spending",
        )
        a4.metric(
            "Fam OOP",
            fmt_currency(row.get("family_oop_accum")),
            help="Total family out-of-pocket spending",
        )

        # ── Drill-down data: transactions, claims, issues, cases ──
        st.divider()
        st.markdown("##### 🔍 Drill-Down: Transactions, Claims, Issues & Cases")

        # Member transactions
        member_txns = (
            txns_df[
                (txns_df["member_id"] == sel_member)
                & (txns_df["plan_id"] == sel_plan)
                & (txns_df["benefit_year"] == sel_year)
            ]
            if not txns_df.empty else pd.DataFrame()
        )

        # Family transactions
        family_txns = (
            txns_df[
                (txns_df["family_id"] == sel_family)
                & (txns_df["plan_id"] == sel_plan)
                & (txns_df["benefit_year"] == sel_year)
            ]
            if not txns_df.empty else pd.DataFrame()
        )

        # Linked claims
        claim_ids = (
            member_txns["claim_record_id"].dropna().unique().tolist()
            if not member_txns.empty and "claim_record_id" in member_txns.columns
            else []
        )
        linked_claims = (
            claims_df[claims_df["claim_record_id"].isin(claim_ids)]
            if not claims_df.empty and claim_ids else pd.DataFrame()
        )

        # Member issues and cases
        member_issues = (
            accum_issues_df[accum_issues_df["member_id"] == sel_member]
            if not accum_issues_df.empty else pd.DataFrame()
        )
        member_cases = (
            accum_cases_df[accum_cases_df["member_id"] == sel_member]
            if not accum_cases_df.empty else pd.DataFrame()
        )

        drill_left, drill_right = st.columns(2)

        with drill_left:
            st.markdown("**Member-Level Transactions**")
            if member_txns.empty:
                st.write("No member-level transactions for this snapshot.")
            else:
                txn_cols = [
                    "accumulator_txn_id", "claim_record_id", "accumulator_type",
                    "delta_amount", "service_date", "source_type",
                    "source_file_id", "created_at",
                ]
                st.dataframe(
                    member_txns[safe_col_list(txn_cols, member_txns.columns)],
                    use_container_width=True, hide_index=True,
                )

                # Transaction timeline chart
                if "service_date" in member_txns.columns and "delta_amount" in member_txns.columns:
                    chart_data = member_txns.copy()
                    chart_data["service_date"] = pd.to_datetime(
                        chart_data["service_date"], errors="coerce"
                    )
                    chart_data = chart_data.dropna(subset=["service_date"])
                    if not chart_data.empty:
                        timeline = (
                            alt.Chart(chart_data)
                            .mark_bar()
                            .encode(
                                x=alt.X("service_date:T", title="Service Date"),
                                y=alt.Y("delta_amount:Q", title="Amount ($)"),
                                color=alt.Color("accumulator_type:N", title="Type"),
                                tooltip=[
                                    "service_date:T", "accumulator_type:N",
                                    "delta_amount:Q", "claim_record_id:N",
                                ],
                            )
                            .properties(height=200, title="Transaction Timeline")
                        )
                        st.altair_chart(timeline, use_container_width=True)

        with drill_right:
            st.markdown("**Family-Level Transactions**")
            if family_txns.empty:
                st.write("No family-level transactions for this context.")
            else:
                fam_txn_cols = [
                    "accumulator_txn_id", "member_id", "claim_record_id",
                    "accumulator_type", "delta_amount", "service_date",
                    "source_type", "source_file_id", "created_at",
                ]
                st.dataframe(
                    family_txns[safe_col_list(fam_txn_cols, family_txns.columns)],
                    use_container_width=True, hide_index=True,
                )

        claim_col, issue_col = st.columns(2)

        with claim_col:
            st.markdown("**Linked Claims**")
            if linked_claims.empty:
                st.write("No claims linked via accumulator transactions.")
            else:
                cl_cols = [
                    "claim_record_id", "claim_id", "line_id",
                    "service_date", "paid_date", "claim_status",
                    "allowed_amount", "paid_amount", "member_responsibility",
                    "deductible_amount", "coinsurance_amount", "copay_amount",
                    "reversal_flag",
                ]
                st.dataframe(
                    linked_claims[safe_col_list(cl_cols, linked_claims.columns)],
                    use_container_width=True, hide_index=True,
                )

        with issue_col:
            st.markdown("**Linked Issues & Support Cases**")
            if member_issues.empty and member_cases.empty:
                st.write("No linked issues or cases for this member.")
            else:
                if not member_issues.empty:
                    st.markdown("_Data Quality Issues_")
                    i_cols = [
                        "issue_id", "issue_code", "severity", "status",
                        "claim_record_id", "issue_message", "detected_at",
                    ]
                    st.dataframe(
                        member_issues[safe_col_list(i_cols, member_issues.columns)],
                        use_container_width=True, hide_index=True,
                    )
                if not member_cases.empty:
                    st.markdown("_Support Cases_")
                    sc_cols = [
                        "case_id", "case_number", "case_type", "priority",
                        "status", "assigned_team", "assigned_to",
                        "target_due_at", "sla_status",
                        "is_at_risk_label", "is_breached_label",
                        "short_description",
                    ]
                    st.dataframe(
                        member_cases[safe_col_list(sc_cols, member_cases.columns)],
                        use_container_width=True, hide_index=True,
                    )

        # ── RCA Guidance (expandable) ──
        with st.expander("🧭 Root Cause Analysis Guidance", expanded=False):
            st.markdown("""
**If OOP Max Breach:**
1. Check the transaction timeline for duplicate amounts from the same claim
2. Look for unusually large single transactions
3. Verify the plan's OOP max is current for this benefit year
4. Check whether any claim reversals are missing from the accumulator
5. Compare `individual_oop_accum` against the sum of all `delta_amount` values

**If Family Rollup Discrepancy:**
1. Sum all individual member OOP/deductible values for this family
2. Compare against the recorded family-level total
3. Check if any family member was added or removed mid-year
4. Look for transactions posted to individual level but not rolled up
5. Verify `family_accumulation_type` matches the plan's rollup rules

**General Checks:**
- Was a snapshot rebuilt recently? Check `snapshot_ts` age
- Are there pending processing runs that haven't updated accumulators?
- Do the linked support cases have a root cause category assigned?
            """)


# ═══════════════════════════════════════════════════════════════════════
# TAB 3 — OOP BREACH RECORDS
# ═══════════════════════════════════════════════════════════════════════

with tab_breaches:
    st.subheader("🔴 OOP Max Breach Records")
    st.caption(
        "Members whose out-of-pocket accumulator exceeds the plan maximum. "
        "These are the highest-priority reconciliation exceptions because "
        "the member may be overpaying for healthcare services."
    )

    breach_df = (
        filtered_df[
            (filtered_df["individual_oop_breach_amount"] > 0)
            | (filtered_df["family_oop_breach_amount"] > 0)
        ]
        if not filtered_df.empty else pd.DataFrame()
    )

    if breach_df.empty:
        st.success("✅ No OOP max breaches found — all members are within plan limits.")
    else:
        st.error(f"**{len(breach_df)} breach(es) detected.** Review each one below.")

        breach_cols = [
            "snapshot_id", "member_id", "family_id",
            "client_code", "plan_name", "benefit_year",
            "individual_oop_accum", "individual_oop_max",
            "individual_oop_breach_amount", "individual_over_max",
            "family_oop_accum", "family_oop_max",
            "family_oop_breach_amount", "family_over_max",
            "open_issue_count", "open_case_count",
        ]
        st.dataframe(
            breach_df[safe_col_list(breach_cols, breach_df.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "individual_oop_breach_amount": st.column_config.NumberColumn(
                    "Ind Breach $", format="$%.2f"
                ),
                "family_oop_breach_amount": st.column_config.NumberColumn(
                    "Fam Breach $", format="$%.2f"
                ),
            },
        )

        # ── Breach magnitude chart ──
        if len(breach_df) > 0:
            chart_data = breach_df[["member_id", "individual_oop_breach_amount", "family_oop_breach_amount"]].copy()
            chart_data["member_id"] = chart_data["member_id"].astype(str)
            chart_melted = chart_data.melt(
                id_vars=["member_id"],
                value_vars=["individual_oop_breach_amount", "family_oop_breach_amount"],
                var_name="Breach Type",
                value_name="Amount",
            )
            chart_melted["Breach Type"] = chart_melted["Breach Type"].map({
                "individual_oop_breach_amount": "Individual",
                "family_oop_breach_amount": "Family",
            })
            chart_melted = chart_melted[chart_melted["Amount"] > 0]

            if not chart_melted.empty:
                breach_chart = (
                    alt.Chart(chart_melted)
                    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                    .encode(
                        x=alt.X("member_id:N", title="Member"),
                        y=alt.Y("Amount:Q", title="Breach Amount ($)"),
                        color=alt.Color(
                            "Breach Type:N",
                            scale=alt.Scale(
                                domain=["Individual", "Family"],
                                range=["#FF4B4B", "#FF8C00"],
                            ),
                        ),
                        tooltip=["member_id", "Breach Type", "Amount"],
                    )
                    .properties(height=280, title="Breach Amounts by Member")
                )
                st.altair_chart(breach_chart, use_container_width=True)

        # Download
        csv_b = breach_df[
            safe_col_list(breach_cols, breach_df.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Breach Records",
            csv_b,
            "oop_breach_records.csv",
            "text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 4 — FAMILY ROLLUP
# ═══════════════════════════════════════════════════════════════════════

with tab_family:
    st.subheader("👨‍👩‍👧‍👦 Family Rollup Reconciliation")
    st.caption(
        "Compares the recorded family-level accumulator total against the "
        "sum of all individual member accumulators within each family. "
        "Any difference signals a rollup error that needs investigation."
    )

    if filtered_family_df.empty:
        st.info("No family rollup data available for the current filters.")
    else:
        disc_only = filtered_family_df[filtered_family_df["has_discrepancy"] == "⚠️ Yes"]
        clean_only = filtered_family_df[filtered_family_df["has_discrepancy"] == "✅ No"]

        fc1, fc2, fc3 = st.columns(3)
        fc1.metric("Total Families", len(filtered_family_df))
        fc2.metric("With Discrepancies", len(disc_only))
        fc3.metric("Clean", len(clean_only))

        if not disc_only.empty:
            st.warning(
                f"**{len(disc_only)} family(ies)** have rollup discrepancies."
            )

            # Discrepancy comparison chart
            chart_fam = disc_only[["family_id", "deductible_diff", "oop_diff"]].copy()
            chart_fam["family_id"] = chart_fam["family_id"].astype(str)
            chart_fam_melted = chart_fam.melt(
                id_vars=["family_id"],
                value_vars=["deductible_diff", "oop_diff"],
                var_name="Diff Type",
                value_name="Difference",
            )
            chart_fam_melted["Diff Type"] = chart_fam_melted["Diff Type"].map({
                "deductible_diff": "Deductible",
                "oop_diff": "OOP",
            })
            fam_chart = (
                alt.Chart(chart_fam_melted)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("family_id:N", title="Family"),
                    y=alt.Y("Difference:Q", title="Discrepancy ($)"),
                    color=alt.Color(
                        "Diff Type:N",
                        scale=alt.Scale(
                            domain=["Deductible", "OOP"],
                            range=["#FFA500", "#FF4B4B"],
                        ),
                    ),
                    tooltip=["family_id", "Diff Type", "Difference"],
                )
                .properties(height=280, title="Family Rollup Discrepancies")
            )
            st.altair_chart(fam_chart, use_container_width=True)

        # Full family table
        fam_cols = [
            "family_id", "client_code", "plan_code", "plan_name",
            "family_accumulation_type", "benefit_year", "member_count",
            "summed_member_deductible", "recorded_family_deductible",
            "deductible_diff",
            "summed_member_oop", "recorded_family_oop", "oop_diff",
            "family_deductible", "family_oop_max",
            "has_discrepancy",
        ]
        st.dataframe(
            filtered_family_df[safe_col_list(fam_cols, filtered_family_df.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "deductible_diff": st.column_config.NumberColumn(
                    "Ded Diff $", format="$%.2f"
                ),
                "oop_diff": st.column_config.NumberColumn(
                    "OOP Diff $", format="$%.2f"
                ),
            },
        )

        csv_f = filtered_family_df[
            safe_col_list(fam_cols, filtered_family_df.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Family Rollup Data",
            csv_f,
            "family_rollup.csv",
            "text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 5 — TRANSACTIONS & CLAIMS
# ═══════════════════════════════════════════════════════════════════════

with tab_txns:
    st.subheader("📒 Accumulator Transactions")
    st.caption(
        "Every dollar that increased or decreased an accumulator. Each row "
        "is one transaction tied to a claim, showing exactly how money flowed "
        "into deductible or OOP totals."
    )

    if txns_df.empty:
        st.info("No accumulator transactions found.")
    else:
        # Summary metrics
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("Total Transactions", fmt_number(len(txns_df)))
        t2.metric(
            "Unique Members",
            fmt_number(txns_df["member_id"].nunique()),
        )
        t3.metric(
            "Deductible Txns",
            fmt_number(
                len(txns_df[txns_df["accumulator_type"] == "deductible"])
                if "accumulator_type" in txns_df.columns else 0
            ),
        )
        t4.metric(
            "OOP Txns",
            fmt_number(
                len(txns_df[txns_df["accumulator_type"] == "oop"])
                if "accumulator_type" in txns_df.columns else 0
            ),
        )

        # Type filter
        if "accumulator_type" in txns_df.columns:
            type_filter = st.radio(
                "Filter by type",
                ["All", "deductible", "oop"],
                horizontal=True,
            )
            display_txns = (
                txns_df
                if type_filter == "All"
                else txns_df[txns_df["accumulator_type"] == type_filter]
            )
        else:
            display_txns = txns_df

        txn_display_cols = [
            "accumulator_txn_id", "member_id", "family_id",
            "client_code", "plan_name",
            "claim_record_id", "benefit_year", "accumulator_type",
            "delta_amount", "service_date", "source_type",
            "source_file_id", "txn_age_hours", "created_at",
        ]
        st.dataframe(
            display_txns[safe_col_list(txn_display_cols, display_txns.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "delta_amount": st.column_config.NumberColumn(
                    "Amount ($)", format="$%.2f"
                ),
            },
        )

        csv_t = display_txns[
            safe_col_list(txn_display_cols, display_txns.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Transaction Data",
            csv_t,
            "accumulator_transactions.csv",
            "text/csv",
            use_container_width=True,
        )

    st.divider()

    st.subheader("🧾 All Claims")
    st.caption(
        "Reference table of all claims. Use this to verify claim details "
        "when investigating accumulator exceptions."
    )

    if claims_df.empty:
        st.info("No claim records found.")
    else:
        cl_display_cols = [
            "claim_record_id", "claim_id", "line_id", "member_id",
            "client_code", "plan_name", "vendor_code",
            "service_date", "paid_date", "claim_status",
            "allowed_amount", "paid_amount", "member_responsibility",
            "deductible_amount", "coinsurance_amount", "copay_amount",
            "reversal_flag", "preventive_flag",
        ]
        st.dataframe(
            claims_df[safe_col_list(cl_display_cols, claims_df.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "allowed_amount": st.column_config.NumberColumn(
                    "Allowed $", format="$%.2f"
                ),
                "paid_amount": st.column_config.NumberColumn(
                    "Paid $", format="$%.2f"
                ),
                "member_responsibility": st.column_config.NumberColumn(
                    "Member Resp $", format="$%.2f"
                ),
            },
        )


# ═══════════════════════════════════════════════════════════════════════
# TAB 6 — HOW IT WORKS
# ═══════════════════════════════════════════════════════════════════════

with tab_howto:
    st.subheader("❓ How Accumulator Reconciliation Works")
    st.caption(
        "A plain-English explanation of what this page does, why it matters, "
        "and what the key terms mean — written so anyone can understand."
    )

    st.markdown("""
---

### 🏥 What Is an Accumulator?

Think of an accumulator like a **piggy bank counter**. Every time you pay
money for a doctor visit or medicine, the counter goes up. Your health
insurance plan has two important limits:

- **Deductible** — the amount you pay *before* insurance starts helping
- **Out-of-Pocket Maximum (OOP Max)** — the *most* you ever have to pay
  in one year; after you hit this, insurance pays 100%

The accumulator tracks how close you are to these limits.

---

### 👨‍👩‍👧‍👦 What About Families?

Many plans have **both individual and family limits**. If your family has
four people, each person has their own counter *and* the family has a
combined counter. This page checks that the family counter matches the
sum of all individual counters. If they don't match, that's called a
**family rollup discrepancy**.

---

### 🔴 What Is an OOP Max Breach?

If someone's counter goes *above* their plan's maximum, something is wrong.
It's like filling a glass past the top — the extra water shouldn't be there.
This could mean:

- A bill was counted twice (duplicate claim)
- A refund wasn't subtracted (missing reversal)
- The plan's limit is outdated (stale threshold)

---

### 📋 What Does This Page Do?

1. **Exception Worklist** — Shows every member, sorted by urgency.
   Problems appear at the top in red or orange.
2. **Member Investigation** — Pick any member to see their full picture:
   balances, claims, transactions, issues, and support cases.
3. **OOP Breaches** — A focused list of members who are over their limit.
4. **Family Rollup** — Compares family totals against individual sums.
5. **Transactions & Claims** — The raw data for deep investigation.

---

### 🔑 Key Terms

| Term | What It Means |
|---|---|
| **Snapshot** | A point-in-time photo of someone's accumulator balances |
| **Transaction** | One addition to an accumulator (usually from one claim) |
| **Breach** | When the accumulator goes above the plan's maximum |
| **Rollup** | Adding up individual member amounts to get the family total |
| **Discrepancy** | When the family total doesn't match the individual sum |
| **SLA** | Service Level Agreement — a deadline for fixing a problem |
| **RCA** | Root Cause Analysis — figuring out *why* something went wrong |

---

### 🎯 Who Uses This?

- **Operations analysts** triage exceptions and resolve cases
- **Data engineers** investigate pipeline issues causing bad data
- **Client managers** need to explain impacts to employer groups
- **Compliance teams** monitor for overpayment and regulatory risk

---

### 🚀 Quick Start

1. Look at the **Key Findings** banner at the top — it tells you
   immediately if anything needs attention
2. Go to the **Exception Worklist** tab to see the prioritized list
3. Click **Member Investigation** and select a member to drill down
4. Use **OOP Breaches** and **Family Rollup** tabs for focused views
5. Reference **Transactions & Claims** for raw verification
    """)

    st.info(
        "💡 **Tip:** Use the sidebar filters to narrow down to a specific "
        "client, plan, or benefit year. Use the quick-filter checkboxes "
        "to jump straight to problem records."
    )


# ═══════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════

st.divider()
st.caption(
    "Accumulator Reconciliation · Eligibility & Accumulator Operations "
    "Command Center · Data is simulated — no real PHI"
)