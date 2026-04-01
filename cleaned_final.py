
import pandas as pd
import streamlit as st
import altair as alt

from src.common.db import fetch_all
from src.sla.sla_service import evaluate_open_slas
from src.issues.support_case_service import assign_case, add_case_note, resolve_case, escalate_case
from src.app.utils import (
    to_dataframe,
    add_age_hours_column,
    sort_priority_series,
    bool_flag_to_label,
)



st.set_page_config(
    page_title="Issue Triage",
    page_icon="🎫",
    layout="wide",
)



OPEN_CASE_STATUSES = {"OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"}
OPEN_ISSUE_STATUSES = {"OPEN", "ACKNOWLEDGED"}

PRIORITY_CONFIG = {
    "CRITICAL": {"icon": "🔴", "rank": 1, "color": "#FF4B4B"},
    "HIGH":     {"icon": "🟠", "rank": 2, "color": "#FFA500"},
    "MEDIUM":   {"icon": "🟡", "rank": 3, "color": "#FFD700"},
    "LOW":      {"icon": "🔵", "rank": 4, "color": "#4B9DFF"},
}

SLA_WATCH_CONFIG = {
    "BREACHED": {"icon": "⛔", "rank": 1, "color": "#FF4B4B"},
    "AT_RISK":  {"icon": "⚠️", "rank": 2, "color": "#FFA500"},
    "ON_TRACK": {"icon": "✅", "rank": 3, "color": "#2ECC71"},
    "NO_SLA":   {"icon": "⚪", "rank": 4, "color": "#BDC3C7"},
}

SEVERITY_ICONS = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🔵"}

QUEUE_ICONS = {
    "ops_file_queue":        "📁",
    "ops_eligibility_queue": "👤",
    "ops_claims_queue":      "🧾",
    "ops_recon_queue":       "💰",
    "ops_triage_queue":      "🎫",
}



def safe_val(value, fallback=0):
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    return value


def safe_text(value, fallback="—"):
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
    v = safe_val(value, None)
    if v is None:
        return fallback
    try:
        num = float(v)
        return f"{int(num):,}" if num == int(num) else f"{num:,.2f}"
    except (TypeError, ValueError):
        return fallback


def safe_col_list(desired_cols, available_cols):
    return [c for c in desired_cols if c in available_cols]


def priority_badge(priority):
    cfg = PRIORITY_CONFIG.get(str(priority).upper(), {"icon": "⚪"})
    return f"{cfg['icon']} {safe_text(priority)}"


def sla_watch_badge(watch_status):
    cfg = SLA_WATCH_CONFIG.get(str(watch_status).upper(), {"icon": "⚪"})
    return f"{cfg['icon']} {safe_text(watch_status)}"


def queue_badge(queue_name):
    icon = QUEUE_ICONS.get(str(queue_name).lower(), "🎫") if queue_name else "🎫"
    return f"{icon} {safe_text(queue_name)}"


def derive_sla_watch(row):
    is_breached = int(safe_val(row.get("is_breached"), 0))
    is_at_risk = int(safe_val(row.get("is_at_risk"), 0))
    sla_status = row.get("sla_status")

    if is_breached == 1:
        return "BREACHED"
    if is_at_risk == 1:
        return "AT_RISK"
    if sla_status is None or (isinstance(sla_status, float) and pd.isna(sla_status)):
        return "NO_SLA"
    return "ON_TRACK"


def derive_assignment_label(row):
    group = safe_text(row.get("assignment_group"))
    owner = safe_text(row.get("assigned_to"))
    if group == "—" and owner == "—":
        return "⚠️ Unassigned"
    if owner == "—":
        return f"{group} (unassigned)"
    return f"{group} / {owner}"


def build_case_label(row):
    pri = row.get("priority", "")
    icon = PRIORITY_CONFIG.get(str(pri).upper(), {}).get("icon", "⚪")
    sla = row.get("sla_watch", "")
    sla_icon = SLA_WATCH_CONFIG.get(str(sla).upper(), {}).get("icon", "")
    return (
        f"{icon}{sla_icon} Case {safe_text(row.get('case_number', row.get('case_id')))} · "
        f"{safe_text(row.get('priority'))} · "
        f"{safe_text(row.get('status'))} · "
        f"{safe_text(row.get('case_type'))} · "
        f"{safe_text(row.get('short_description'))}"
    )


def compute_sla_pct_elapsed(row):
    Calculate what percentage of the SLA window has elapsed.
    Returns 0-100+ (can exceed 100 if breached).
    target_hours = float(safe_val(row.get("target_hours"), 0))
    case_age = float(safe_val(row.get("case_age_hours"), 0))
    if target_hours <= 0:
        return 0.0
    return round((case_age / target_hours) * 100, 1)


def progress_bar_value(pct):
    return min(max(float(pct) / 100.0, 0.0), 1.0)



def run_sla_evaluation():
    try:
        evaluate_open_slas()
        return None
    except Exception as e:
        return str(e)


sla_eval_error = run_sla_evaluation()



@st.cache_data(ttl=300, show_spinner="Loading support cases …")
def load_cases():
    rows = fetch_all("""
        SELECT
            sc.case_id, sc.case_number, sc.issue_id,
            sc.client_id, c.client_code,
            sc.vendor_id, v.vendor_code,
            sc.file_id,
            sc.run_id AS processing_run_id,
            sc.member_id, sc.claim_record_id,
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
        ORDER BY sc.opened_at DESC, sc.case_id DESC
    df = to_dataframe(rows)
    if df.empty:
        return df

    df = add_age_hours_column(df, "opened_at", "case_age_hours")

    if "is_at_risk" not in df.columns:
        df["is_at_risk"] = 0
    if "is_breached" not in df.columns:
        df["is_breached"] = 0

    df["is_at_risk"] = df["is_at_risk"].fillna(0).astype(int)
    df["is_breached"] = df["is_breached"].fillna(0).astype(int)

    df["is_at_risk_label"] = df["is_at_risk"].map({1: "⚠️ Yes", 0: "✅ No"})
    df["is_breached_label"] = df["is_breached"].map({1: "⛔ Yes", 0: "✅ No"})
    df["priority_badge"] = df["priority"].apply(priority_badge)

    df["priority_sort"] = sort_priority_series(df["priority"].fillna("LOW"))
    df["sla_watch"] = df.apply(derive_sla_watch, axis=1)
    df["sla_watch_badge"] = df["sla_watch"].apply(sla_watch_badge)
    df["assignment_label"] = df.apply(derive_assignment_label, axis=1)
    df["queue_badge"] = df["assignment_group"].apply(queue_badge)
    df["is_open_case"] = df["status"].isin(OPEN_CASE_STATUSES)
    df["sla_pct_elapsed"] = df.apply(compute_sla_pct_elapsed, axis=1)

    return df


@st.cache_data(ttl=300, show_spinner="Loading data quality issues …")
def load_issues():
    rows = fetch_all("""
        SELECT
            i.issue_id, i.issue_code, i.issue_type, i.issue_subtype,
            i.severity, i.status,
            i.client_id, c.client_code,
            i.vendor_id, v.vendor_code,
            i.file_id, i.run_id,
            i.member_id, i.claim_record_id,
            i.entity_name, i.entity_key, i.source_row_number,
            i.issue_description, i.issue_message,
            i.detected_at,
            i.file_id AS source_file_id,
            i.run_id AS processing_run_id,
            i.created_at
        FROM data_quality_issues i
        LEFT JOIN clients c ON i.client_id = c.client_id
        LEFT JOIN vendors v ON i.vendor_id = v.vendor_id
        ORDER BY i.detected_at DESC, i.issue_id DESC
    df = to_dataframe(rows)
    if not df.empty:
        df = add_age_hours_column(df, "detected_at", "issue_age_hours")
    return df


@st.cache_data(ttl=300, show_spinner="Loading case notes …")
def load_case_notes(case_id):
    rows = fetch_all("""
        SELECT note, author, created_at
        FROM case_notes
        WHERE case_id = ?
        ORDER BY created_at DESC
    return to_dataframe(rows)



cases_df = load_cases()
issues_df = load_issues()



with st.sidebar:
    st.header("🔄 Page Actions")
    if st.button("🔄 Refresh All Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    if sla_eval_error:
        st.warning(f"⚠️ SLA evaluation error: {sla_eval_error}")
    else:
        st.success("✅ SLAs evaluated")

    if st.button("🧹 Clear Saved Filters", use_container_width=True):
        triage_keys = [k for k in st.session_state.keys() if k.startswith("triage_")]
        for k in triage_keys:
            del st.session_state[k]
        st.rerun()

    st.divider()
    st.header("🔍 Filters")

    if not cases_df.empty:
        status_opts = sorted(cases_df["status"].dropna().unique().tolist())
        priority_opts = sorted(cases_df["priority"].dropna().unique().tolist())
        type_opts = sorted(cases_df["case_type"].dropna().unique().tolist())
        queue_opts = sorted(cases_df["assignment_group"].dropna().unique().tolist())
        client_opts = sorted(cases_df["client_code"].dropna().unique().tolist())
        sla_opts = sorted(cases_df["sla_watch"].dropna().unique().tolist())
    else:
        status_opts, priority_opts, type_opts = [], [], []
        queue_opts, client_opts, sla_opts = [], [], []

    # Load defaults from session state
    default_statuses = st.session_state.get("triage_sel_statuses", [
        s for s in ["OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED", "RESOLVED"]
        if s in status_opts
    ] or status_opts)
    default_priorities = st.session_state.get("triage_sel_priorities", priority_opts)
    default_types = st.session_state.get("triage_sel_types", type_opts)
    default_queues = st.session_state.get("triage_sel_queues", queue_opts)
    default_clients = st.session_state.get("triage_sel_clients", client_opts)
    default_sla = st.session_state.get("triage_sel_sla", sla_opts)
    default_members = st.session_state.get("triage_sel_members", [])
    default_files = st.session_state.get("triage_sel_files", [])
    default_start_date = st.session_state.get("triage_start_date", None)
    default_end_date = st.session_state.get("triage_end_date", None)
    default_sort_by = st.session_state.get("triage_sort_by", "Priority (default)")
    default_only_open = st.session_state.get("triage_only_open", True)
    default_only_unassigned = st.session_state.get("triage_only_unassigned", False)
    default_only_at_risk = st.session_state.get("triage_only_at_risk", False)
    default_only_breached = st.session_state.get("triage_only_breached", False)

    sel_statuses = st.multiselect("Case Status", status_opts, default=default_statuses)
    sel_priorities = st.multiselect("Priority", priority_opts, default=default_priorities)
    sel_types = st.multiselect("Case Type", type_opts, default=default_types)
    sel_queues = st.multiselect("Assignment Queue", queue_opts, default=default_queues)
    sel_clients = st.multiselect("Client", client_opts, default=default_clients)
    sel_sla = st.multiselect("SLA Watch", sla_opts, default=default_sla)

    # New filters
    member_opts = sorted(cases_df["member_id"].dropna().unique().astype(str).tolist()) if not cases_df.empty else []
    file_opts = sorted(cases_df["file_id"].dropna().unique().astype(str).tolist()) if not cases_df.empty else []
    sel_members = st.multiselect("Member ID", member_opts, default=default_members)
    sel_files = st.multiselect("File ID", file_opts, default=default_files)

    start_date = st.date_input("Opened From", value=default_start_date)
    end_date = st.date_input("Opened To", value=default_end_date)

    st.divider()
    st.markdown("**Quick Filters**")
    only_open = st.checkbox("📂 Open cases only", value=default_only_open)
    only_unassigned = st.checkbox("⚠️ Unassigned only", value=default_only_unassigned)
    only_at_risk = st.checkbox("⚠️ At-risk SLA only", value=default_only_at_risk)
    only_breached = st.checkbox("⛔ Breached SLA only", value=default_only_breached)

    st.divider()
    st.markdown("**Sorting Options**")
    sort_options = ["Priority (default)", "Newest First", "Oldest First", "Age Desc", "Status", "Client"]
    sort_index = sort_options.index(default_sort_by) if default_sort_by in sort_options else 0
    sort_by = st.radio("Sort by", sort_options, index=sort_index)

    # Save current selections to session state
    st.session_state["triage_sel_statuses"] = sel_statuses
    st.session_state["triage_sel_priorities"] = sel_priorities
    st.session_state["triage_sel_types"] = sel_types
    st.session_state["triage_sel_queues"] = sel_queues
    st.session_state["triage_sel_clients"] = sel_clients
    st.session_state["triage_sel_sla"] = sel_sla
    st.session_state["triage_sel_members"] = sel_members
    st.session_state["triage_sel_files"] = sel_files
    st.session_state["triage_start_date"] = start_date
    st.session_state["triage_end_date"] = end_date
    st.session_state["triage_sort_by"] = sort_by
    st.session_state["triage_only_open"] = only_open
    st.session_state["triage_only_unassigned"] = only_unassigned
    st.session_state["triage_only_at_risk"] = only_at_risk
    st.session_state["triage_only_breached"] = only_breached



def apply_filters(df):
    if df.empty:
        return df
    out = df.copy()

    if sel_statuses:
        out = out[out["status"].isin(sel_statuses)]
    if sel_priorities:
        out = out[out["priority"].isin(sel_priorities)]
    if sel_types:
        out = out[out["case_type"].isin(sel_types)]
    if sel_queues:
        out = out[out["assignment_group"].isin(sel_queues)]
    if sel_clients:
        out = out[out["client_code"].isin(sel_clients)]
    if sel_sla:
        out = out[out["sla_watch"].isin(sel_sla)]
    if only_open:
        out = out[out["status"].isin(OPEN_CASE_STATUSES)]
    if only_unassigned:
        out = out[
            out["assigned_to"].isna()
            | (out["assigned_to"].astype(str).str.strip() == "")
        ]
    if only_at_risk:
        out = out[out["is_at_risk"] == 1]
    if only_breached:
        out = out[out["is_breached"] == 1]

    if sel_members:
        out = out[out["member_id"].astype(str).isin(sel_members)]
    if sel_files:
        out = out[out["file_id"].astype(str).isin(sel_files)]
    if start_date:
        out = out[out["opened_at"] >= pd.Timestamp(start_date)]
    if end_date:
        out = out[out["opened_at"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]

    # Sorting
    if sort_by == "Priority (default)":
        out = out.sort_values(by=["priority_sort", "case_age_hours", "case_id"], ascending=[True, False, True])
    elif sort_by == "Newest First":
        out = out.sort_values(by=["opened_at"], ascending=[False])
    elif sort_by == "Oldest First":
        out = out.sort_values(by=["opened_at"], ascending=[True])
    elif sort_by == "Age Desc":
        out = out.sort_values(by=["case_age_hours"], ascending=[False])
    elif sort_by == "Status":
        out = out.sort_values(by=["status"], ascending=[True])
    elif sort_by == "Client":
        out = out.sort_values(by=["client_code"], ascending=[True])

    return out


filtered_df = apply_filters(cases_df)



total_cases = len(cases_df) if not cases_df.empty else 0
open_cases = (
    int(cases_df["status"].isin(OPEN_CASE_STATUSES).sum())
    if not cases_df.empty else 0
)
critical_open = (
    int(
        ((cases_df["priority"] == "CRITICAL") & cases_df["status"].isin(OPEN_CASE_STATUSES)).sum()
    )
    if not cases_df.empty else 0
)
at_risk_count = (
    int((cases_df["is_at_risk"] == 1).sum())
    if not cases_df.empty else 0
)
breached_count = (
    int((cases_df["is_breached"] == 1).sum())
    if not cases_df.empty else 0
)
resolved_count = (
    int((cases_df["status"] == "RESOLVED").sum())
    if not cases_df.empty else 0
)
total_issues = len(issues_df) if not issues_df.empty else 0
open_issues = (
    int(issues_df["status"].isin(OPEN_ISSUE_STATUSES).sum())
    if not issues_df.empty else 0
)
filtered_count = len(filtered_df)



st.title("🎫 Issue Triage")
st.caption(
    "Operational support queue for data quality incidents, support case "
    "management, SLA tracking, assignment routing, and root cause analysis "
    "— the nerve center of healthcare data operations."
)

findings = []
if breached_count > 0:
    findings.append(f"⛔ **{breached_count}** SLA(s) **breached**")
if at_risk_count > 0:
    findings.append(f"⚠️ **{at_risk_count}** SLA(s) **at risk**")
if critical_open > 0:
    findings.append(f"🔴 **{critical_open}** CRITICAL open case(s)")
if open_cases > 0:
    findings.append(f"📂 **{open_cases}** total open case(s)")
if open_issues > 0:
    findings.append(f"🔵 **{open_issues}** open data quality issue(s)")

if findings:
    if breached_count > 0:
        st.error("**Key Findings:**  " + "  ·  ".join(findings))
    else:
        st.warning("**Key Findings:**  " + "  ·  ".join(findings))
else:
    st.success(
        "✅ **All clear.** No open cases, SLA concerns, or unresolved issues."
    )

m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
m1.metric("Open Cases", fmt_number(open_cases))
m2.metric("Critical Open", fmt_number(critical_open))
m3.metric("⚠️ At-Risk SLAs", fmt_number(at_risk_count))
m4.metric("⛔ Breached SLAs", fmt_number(breached_count))
m5.metric("Resolved", fmt_number(resolved_count))
m6.metric("Total Issues", fmt_number(total_issues))
m7.metric("Showing", f"{filtered_count} / {total_cases}")

st.divider()



tab_queue, tab_investigate, tab_sla, tab_issues, tab_analytics, tab_howto = st.tabs([
    "📋 Support Queue",
    "🔎 Case Investigation",
    "⏱️ SLA Watchlist",
    "🐛 Data Quality Issues",
    "📊 Case Analytics",
    "❓ How It Works",
])



with tab_queue:
    st.subheader("📋 Support Case Queue")
    st.caption(
        "All support cases ordered by priority and age. CRITICAL cases appear "
        "first, oldest cases within each priority level rise to the top. "
        "This is the primary worklist for operations triage."
    )

    if filtered_df.empty:
        st.info("No support cases match the current filters.")
    else:
        pri_counts = (
            filtered_df["priority"]
            .value_counts()
            .reset_index()
        )
        if pri_counts.columns.tolist() == ["index", "priority"]:
            pri_counts.columns = ["Priority", "Count"]
        elif "priority" in pri_counts.columns and "count" in pri_counts.columns:
            pri_counts = pri_counts.rename(
                columns={"priority": "Priority", "count": "Count"}
            )
        else:
            pri_counts.columns = ["Priority", "Count"]

        chart_col, legend_col = st.columns([3, 1])

        with chart_col:
            p_domain = list(PRIORITY_CONFIG.keys())
            p_range = [v["color"] for v in PRIORITY_CONFIG.values()]

            pri_chart = (
                alt.Chart(pri_counts)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("Priority:N", sort=p_domain, title=None),
                    y=alt.Y("Count:Q", title="Case Count"),
                    color=alt.Color(
                        "Priority:N",
                        scale=alt.Scale(domain=p_domain, range=p_range),
                        legend=None,
                    ),
                    tooltip=["Priority", "Count"],
                )
                .properties(height=240, title="Cases by Priority")
            )
            st.altair_chart(pri_chart, use_container_width=True)

        with legend_col:
            st.markdown("**Priority Legend**")
            for name, cfg in PRIORITY_CONFIG.items():
                count_val = int(
                    pri_counts.loc[pri_counts["Priority"] == name, "Count"].sum()
                ) if name in pri_counts["Priority"].values else 0
                st.markdown(f"{cfg['icon']} **{name}**: {count_val}")

            st.divider()
            st.markdown("**SLA Summary**")
            for name, cfg in SLA_WATCH_CONFIG.items():
                count_val = int((filtered_df["sla_watch"] == name).sum())
                if count_val > 0:
                    st.markdown(f"{cfg['icon']} **{name}**: {count_val}")

        queue_cols = [
            "priority_badge", "case_number", "case_id",
            "case_type", "status", "sla_watch_badge",
            "queue_badge", "assigned_to",
            "client_code", "vendor_code",
            "file_id", "member_id", "claim_record_id",
            "case_age_hours", "target_due_at",
            "short_description",
        ]
        st.dataframe(
            filtered_df[safe_col_list(queue_cols, filtered_df.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "priority_badge": st.column_config.TextColumn("Priority"),
                "sla_watch_badge": st.column_config.TextColumn("SLA"),
                "queue_badge": st.column_config.TextColumn("Queue"),
                "case_age_hours": st.column_config.NumberColumn(
                    "Age (hrs)", format="%.1f"
                ),
            },
        )

        csv_q = filtered_df[
            safe_col_list(queue_cols, filtered_df.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Queue CSV",
            csv_q,
            "support_case_queue.csv",
            "text/csv",
            use_container_width=True,
        )



with tab_investigate:
    st.subheader("🔎 Case Detail & Root Cause Analysis")
    st.caption(
        "Select a case to see its full lifecycle: operational context, "
        "business context, SLA status, linked issue details, and "
        "root cause analysis guidance."
    )

    if filtered_df.empty:
        st.info("No cases available. Run a scenario or adjust filters.")
    else:
        selectable = filtered_df.copy()
        selectable["label"] = selectable.apply(build_case_label, axis=1)

        selected_label = st.selectbox(
            "Choose a case to investigate",
            options=selectable["label"].tolist(),
            help="Cases are sorted by priority. Breached/at-risk SLA icons appear in the label.",
        )
        row = selectable[selectable["label"] == selected_label].iloc[0].to_dict()

        if row.get("is_breached") == 1:
            st.error(
                "⛔ **SLA BREACHED** — This case has exceeded its resolution "
                "deadline. Prioritize immediate containment, confirm ownership, "
                "and escalate if root cause is not yet identified."
            )
        elif row.get("is_at_risk") == 1:
            st.warning(
                "⚠️ **SLA AT RISK** — This case is approaching its deadline. "
                "Validate that investigation is in progress, confirm the "
                "assigned owner is actively working, and prepare escalation "
                "if needed."
            )
        elif row.get("status") in OPEN_CASE_STATUSES:
            st.info(
                "📂 **Open Case** — Active in the support queue. Confirm "
                "the impacted entity, review the linked issue, and validate "
                "the remediation path."
            )
        else:
            st.success(
                "✅ **Resolved / Closed** — This case is no longer in active "
                "triage. Review for lessons learned or documentation needs."
            )

        ctx1, ctx2, ctx3 = st.columns([1.2, 1.2, 1])

        with ctx1:
            st.markdown("##### 🎫 Operational Context")
            st.markdown(f"**Case Number:** `{safe_text(row.get('case_number'))}`")
            st.markdown(f"**Case ID:** `{safe_text(row.get('case_id'))}`")
            st.markdown(f"**Issue ID:** `{safe_text(row.get('issue_id'))}`")
            st.markdown(f"**Case Type:** {safe_text(row.get('case_type'))}")
            st.markdown(f"**Priority:** {priority_badge(row.get('priority'))}")
            st.markdown(f"**Severity:** {safe_text(row.get('severity'))}")
            st.markdown(f"**Status:** {safe_text(row.get('status'))}")
            st.markdown(f"**Assignment:** {safe_text(row.get('assignment_label'))}")
            st.markdown(f"**Escalation Level:** {safe_text(row.get('escalation_level'))}")

        with ctx2:
            st.markdown("##### 🏢 Business Context")
            st.markdown(f"**Client:** {safe_text(row.get('client_code'))}")
            st.markdown(f"**Vendor:** {safe_text(row.get('vendor_code'))}")
            st.markdown(f"**File ID:** `{safe_text(row.get('file_id'))}`")
            st.markdown(f"**Processing Run:** `{safe_text(row.get('processing_run_id'))}`")
            st.markdown(f"**Member ID:** `{safe_text(row.get('member_id'))}`")
            st.markdown(f"**Claim Record:** `{safe_text(row.get('claim_record_id'))}`")

        with ctx3:
            st.markdown("##### ⏱️ SLA Context")
            st.markdown(f"**SLA Watch:** {sla_watch_badge(row.get('sla_watch'))}")
            st.markdown(f"**SLA Type:** {safe_text(row.get('sla_type'))}")
            st.markdown(f"**Target Hours:** {safe_text(row.get('target_hours'))}")
            st.markdown(f"**Due At:** {safe_text(row.get('target_due_at'))}")
            st.markdown(f"**Breached At:** {safe_text(row.get('breached_at'))}")
            st.markdown(f"**Last Evaluated:** {safe_text(row.get('last_evaluated_at'))}")

            # SLA elapsed progress bar
            sla_pct = float(safe_val(row.get("sla_pct_elapsed"), 0))
            if float(safe_val(row.get("target_hours"), 0)) > 0:
                st.markdown(f"**SLA Elapsed:** {sla_pct}%")
                st.progress(progress_bar_value(sla_pct))

        st.markdown("##### 🔗 Related Navigation")
        nav_col1, nav_col2, nav_col3 = st.columns(3)
        with nav_col1:
            if st.button("📁 View File in Monitoring"):
                st.session_state["selected_file"] = row.get('file_id')
                st.info(f"Navigate to File Monitoring page and filter for File ID: {safe_text(row.get('file_id'))}")
            if st.button("📄 View File Detail", key="view_file_detail"):
                st.session_state["selected_file"] = row.get('file_id')
                st.info(f"Navigate to File Detail and select File ID: {safe_text(row.get('file_id'))}")
            if st.button("⚙️ View Processing Run", key="view_run_detail"):
                st.session_state["selected_run"] = row.get('processing_run_id')
                st.info(f"Navigate to Processing Run Detail and select Run ID: {safe_text(row.get('processing_run_id'))}")
        with nav_col2:
            if st.button("👤 View Member Timeline"):
                st.session_state["selected_member"] = row.get('member_id')
                st.info(f"Navigate to Member Timeline page and select Member ID: {safe_text(row.get('member_id'))}")
        with nav_col3:
            if st.button("⏱️ View SLA Details"):
                st.session_state["selected_sla"] = row.get('sla_id')
                st.info(f"Navigate to SLA Detail and select SLA ID: {safe_text(row.get('sla_id'))}")
            if st.button("📄 View Case Detail", key="view_case_detail"):
                st.session_state["selected_case"] = row["case_id"]
                st.info(f"Navigate to Support Case Detail and select Case: {row['case_number']}")

        st.markdown("##### ⚙️ Workflow Actions")
        note_text = st.text_area("Add Note", height=50, key=f"note_{row['case_id']}", placeholder="Enter investigation notes...")

        action_col1, action_col2, action_col3, action_col4 = st.columns(4)
        with action_col1:
            if st.button("✅ Mark Resolved", use_container_width=True):
                try:
                    resolve_case(row["case_id"])
                    st.success("Case marked as resolved!")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to resolve case: {e}")
        with action_col2:
            if st.button("👤 Assign to Me", use_container_width=True):
                try:
                    assign_case(row["case_id"], "operator")
                    st.success("Case assigned to you!")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to assign case: {e}")
        with action_col3:
            if st.button("🚀 Escalate", use_container_width=True):
                try:
                    escalate_case(row["case_id"], "Manual escalation from triage")
                    st.success("Case escalated!")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to escalate case: {e}")
        with action_col4:
            if st.button("📝 Add Note", use_container_width=True):
                if note_text.strip():
                    try:
                        add_case_note(row["case_id"], note_text.strip())
                        st.success("Note added successfully!")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add note: {e}")
                else:
                    st.warning("Please enter a note.")

        st.divider()
        st.markdown("##### 📝 Case Narrative")

        narr1, narr2 = st.columns(2)
        with narr1:
            st.markdown("**Short Description**")
            st.write(safe_text(row.get("short_description")))
            st.markdown("**Full Description**")
            st.write(safe_text(row.get("description")))

        with narr2:
            st.markdown("**Root Cause Category**")
            rca = safe_text(row.get("root_cause_category"))
            if rca == "—":
                st.write("⚠️ Not yet identified — needs investigation")
            else:
                st.write(rca)

            st.markdown("**Case Timeline**")
            st.write(f"Opened: {safe_text(row.get('opened_at'))}")
            st.write(f"Acknowledged: {safe_text(row.get('acknowledged_at'))}")
            st.write(f"Resolved: {safe_text(row.get('resolved_at'))}")
            st.write(f"Closed: {safe_text(row.get('closed_at'))}")
            st.write(f"Last Updated: {safe_text(row.get('last_updated_at'))}")

        st.divider()
        related_issue_df = (
            issues_df[issues_df["issue_id"] == row.get("issue_id")]
            if not issues_df.empty and row.get("issue_id") is not None
            else pd.DataFrame()
        )

        # Always define `related` so the expander's JSON export is always safe.
        related: dict = {}
        if not related_issue_df.empty:
            related = related_issue_df.iloc[0].to_dict()

            st.markdown("##### 🐛 Linked Data Quality Issue")

            il, ir = st.columns(2)
            with il:
                st.markdown(f"**Issue Code:** `{safe_text(related.get('issue_code'))}`")
                st.markdown(f"**Issue Type:** {safe_text(related.get('issue_type'))}")
                st.markdown(f"**Issue Subtype:** {safe_text(related.get('issue_subtype'))}")
                st.markdown(f"**Severity:** {SEVERITY_ICONS.get(str(related.get('severity', '')).upper(), '⚪')} {safe_text(related.get('severity'))}")
                st.markdown(f"**Status:** {safe_text(related.get('status'))}")
                st.markdown(f"**Detected At:** {safe_text(related.get('detected_at'))}")

            with ir:
                st.markdown(f"**Entity Name:** {safe_text(related.get('entity_name'))}")
                st.markdown(f"**Entity Key:** `{safe_text(related.get('entity_key'))}`")
                st.markdown(f"**Source File ID:** `{safe_text(related.get('source_file_id'))}`")
                st.markdown(f"**Processing Run:** `{safe_text(related.get('processing_run_id'))}`")
                st.markdown(f"**Source Row #:** {safe_text(related.get('source_row_number'))}")

            st.markdown("**Issue Message**")
            st.info(safe_text(related.get("issue_message")))

            st.markdown("**Issue Description**")
            st.write(safe_text(related.get("issue_description")))
        else:
            st.markdown("##### 🐛 Linked Data Quality Issue")
            st.write("No linked issue found for this case.")

        with st.expander("📄 Raw Case & Issue Records (JSON)", expanded=False):
            raw_case_cols = [
                "case_id", "case_number", "issue_id", "case_type",
                "priority", "severity", "status",
                "assignment_group", "assigned_to", "escalation_level",
                "client_id", "client_code", "vendor_id", "vendor_code",
                "file_id", "processing_run_id",
                "member_id", "claim_record_id",
                "short_description", "description",
                "root_cause_category",
                "opened_at", "acknowledged_at",
                "resolved_at", "closed_at", "last_updated_at",
                "sla_id", "sla_type", "target_hours", "target_due_at",
                "sla_status", "is_at_risk", "is_breached",
                "breached_at", "last_evaluated_at",
            ]
            case_json = {
                k: str(row.get(k)) if row.get(k) is not None else None
                for k in raw_case_cols
            }

            if not related_issue_df.empty:
                raw_issue_cols = [
                    "issue_id", "issue_code", "issue_type", "issue_subtype",
                    "severity", "status",
                    "client_id", "client_code", "vendor_id", "vendor_code",
                    "file_id", "run_id", "member_id", "claim_record_id",
                    "entity_name", "entity_key", "source_row_number",
                    "issue_description", "issue_message",
                    "detected_at", "source_file_id", "processing_run_id",
                    "created_at",
                ]
                issue_json = {
                    k: str(related.get(k)) if related.get(k) is not None else None
                    for k in raw_issue_cols
                }
            else:
                issue_json = None

            st.json({"case": case_json, "issue": issue_json})

        st.markdown("##### 📝 Case Notes")
        notes_df = load_case_notes(row["case_id"])
        if notes_df.empty:
            st.write("No notes yet.")
        else:
            for _, note_row in notes_df.iterrows():
                st.markdown(f"**{note_row['author']}** at {note_row['created_at']}: {note_row['note']}")

        with st.expander("🧭 Root Cause Analysis Guidance", expanded=False):
            st.markdown("""
**MISSING_INBOUND_FILE:**
1. Check File Monitoring for the expected file and delivery gap
2. Verify vendor's other files arrived (vendor-wide vs file-specific)
3. Look at the landing path for mis-named files
4. Contact vendor operations; document communication
5. After file arrives, verify downstream eligibility loads correctly

**DUPLICATE_ELIGIBILITY_RESEND:**
1. Compare file hashes to confirm identical content
2. Check whether the original was fully processed
3. If duplicate was also loaded, look for doubled eligibility records
4. Verify accumulator impact on affected members
5. Determine if resend was intentional or accidental

**CLAIM_INELIGIBLE_MEMBER:**
1. Check eligibility records for the member around the service date
2. Look for pending eligibility files that might resolve the gap
3. Determine if this is isolated or part of a broader pattern
4. If widespread, check for MISSING_INBOUND_FILE connection
5. Decide: hold claim, reprocess after eligibility loads, or deny

**ACCUMULATOR_EXCEEDS_OOP_MAX:**
1. Compare accumulator snapshot against plan OOP max
2. Trace transaction history for duplicates or missing reversals
3. Verify plan threshold is current for this benefit year
4. Check linked claims for duplicate postings
5. After correction, verify snapshot ≤ OOP max

**FAMILY_ROLLUP_DISCREPANCY:**
1. Sum individual member accumulators and compare to family total
2. Check for mid-year member additions or removals
3. Look for transactions at individual level not rolled up to family
4. Verify family_accumulation_type setting on the plan
5. After correction, verify family total = sum of members

**General RCA Steps:**
- Always check the linked issue code for scenario context
- Review the assignment queue — is the right team working this?
- Check SLA elapsed — how much time remains?
- Look at root_cause_category — has the team started RCA?
- Review the case timeline — opened → acknowledged → resolved
            """)



with tab_sla:
    st.subheader("⏱️ SLA Watchlist")
    st.caption(
        "Focused view of SLA compliance. Breached cases appear first, "
        "then at-risk, then on-track. Use this to manage escalation "
        "and ensure deadlines are met."
    )

    if filtered_df.empty:
        st.info("No cases match the current filters.")
    else:
        sla_counts = (
            filtered_df["sla_watch"]
            .value_counts()
            .reset_index()
        )
        if sla_counts.columns.tolist() == ["index", "sla_watch"]:
            sla_counts.columns = ["SLA Status", "Count"]
        elif "sla_watch" in sla_counts.columns and "count" in sla_counts.columns:
            sla_counts = sla_counts.rename(
                columns={"sla_watch": "SLA Status", "count": "Count"}
            )
        else:
            sla_counts.columns = ["SLA Status", "Count"]

        sla_chart_col, sla_detail_col = st.columns([2, 1])

        with sla_chart_col:
            s_domain = list(SLA_WATCH_CONFIG.keys())
            s_range = [v["color"] for v in SLA_WATCH_CONFIG.values()]

            sla_donut = (
                alt.Chart(sla_counts)
                .mark_arc(innerRadius=50, cornerRadius=4)
                .encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color(
                        "SLA Status:N",
                        scale=alt.Scale(domain=s_domain, range=s_range),
                    ),
                    tooltip=["SLA Status", "Count"],
                )
                .properties(height=260, title="SLA Status Distribution")
            )
            st.altair_chart(sla_donut, use_container_width=True)

        with sla_detail_col:
            st.markdown("**SLA Breakdown**")
            for name, cfg in SLA_WATCH_CONFIG.items():
                count_val = int(
                    sla_counts.loc[sla_counts["SLA Status"] == name, "Count"].sum()
                ) if name in sla_counts["SLA Status"].values else 0
                st.markdown(f"{cfg['icon']} **{name}**: {count_val}")

        breached_df = filtered_df[filtered_df["is_breached"] == 1]
        at_risk_df = filtered_df[
            (filtered_df["is_breached"] != 1) & (filtered_df["is_at_risk"] == 1)
        ]
        unassigned_df = filtered_df[
            filtered_df["status"].isin(OPEN_CASE_STATUSES)
            & (
                filtered_df["assigned_to"].isna()
                | (filtered_df["assigned_to"].astype(str).str.strip() == "")
            )
        ]

        sla_table_cols = [
            "priority_badge", "case_number", "case_type", "status",
            "sla_watch_badge", "queue_badge", "assigned_to",
            "target_hours", "target_due_at", "case_age_hours",
            "sla_pct_elapsed", "short_description",
        ]

        unassigned_cols = [
            "priority_badge", "case_number", "case_type", "status",
            "queue_badge", "assigned_to", "case_age_hours",
            "target_due_at", "short_description",
        ]

        st.markdown("### ⛔ Breached Cases")
        if breached_df.empty:
            st.success("✅ No breached SLAs — all deadlines have been met.")
        else:
            st.error(f"**{len(breached_df)} case(s)** have breached their SLA deadline.")
            st.dataframe(
                breached_df[safe_col_list(sla_table_cols, breached_df.columns)],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "priority_badge": st.column_config.TextColumn("Priority"),
                    "sla_watch_badge": st.column_config.TextColumn("SLA"),
                    "queue_badge": st.column_config.TextColumn("Queue"),
                    "case_age_hours": st.column_config.NumberColumn("Age (hrs)", format="%.1f"),
                    "sla_pct_elapsed": st.column_config.ProgressColumn(
                        "SLA Elapsed", min_value=0, max_value=200, format="%d%%"
                    ),
                },
            )

        st.markdown("### ⚠️ At-Risk Cases")
        if at_risk_df.empty:
            st.info("No at-risk SLAs at the moment.")
        else:
            st.warning(f"**{len(at_risk_df)} case(s)** are approaching their SLA deadline.")
            st.dataframe(
                at_risk_df[safe_col_list(sla_table_cols, at_risk_df.columns)],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "priority_badge": st.column_config.TextColumn("Priority"),
                    "sla_watch_badge": st.column_config.TextColumn("SLA"),
                    "queue_badge": st.column_config.TextColumn("Queue"),
                    "case_age_hours": st.column_config.NumberColumn("Age (hrs)", format="%.1f"),
                    "sla_pct_elapsed": st.column_config.ProgressColumn(
                        "SLA Elapsed", min_value=0, max_value=200, format="%d%%"
                    ),
                },
            )

        st.markdown("### ⚠️ Unassigned Open Cases")
        if unassigned_df.empty:
            st.success("✅ All open cases have an assigned owner.")
        else:
            st.warning(f"**{len(unassigned_df)} open case(s)** do not have an assigned owner.")
            st.dataframe(
                unassigned_df[safe_col_list(unassigned_cols, unassigned_df.columns)],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "priority_badge": st.column_config.TextColumn("Priority"),
                    "queue_badge": st.column_config.TextColumn("Queue"),
                    "case_age_hours": st.column_config.NumberColumn("Age (hrs)", format="%.1f"),
                },
            )

        csv_sla = filtered_df[
            safe_col_list(sla_table_cols, filtered_df.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download SLA Watchlist CSV",
            csv_sla,
            "sla_watchlist.csv",
            "text/csv",
            use_container_width=True,
        )



with tab_issues:
    st.subheader("🐛 Data Quality Issues")
    st.caption(
        "Every data quality issue detected across all pipelines. Issues are "
        "the raw signals that trigger support case creation. Each issue has "
        "a code, severity, and link to the source file or entity."
    )

    if issues_df.empty:
        st.info("No data quality issues found in the system.")
    else:
        # ── Issue metrics ──
        iq1, iq2, iq3, iq4 = st.columns(4)
        iq1.metric("Total Issues", fmt_number(total_issues))
        iq2.metric("Open Issues", fmt_number(open_issues))
        iq3.metric(
            "Unique Issue Codes",
            fmt_number(issues_df["issue_code"].nunique()),
        )
        iq4.metric(
            "Affected Members",
            fmt_number(issues_df["member_id"].dropna().nunique()),
        )

        # ── Severity distribution chart ──
        sev_counts = (
            issues_df["severity"]
            .value_counts()
            .reset_index()
        )
        if sev_counts.columns.tolist() == ["index", "severity"]:
            sev_counts.columns = ["Severity", "Count"]
        elif "severity" in sev_counts.columns and "count" in sev_counts.columns:
            sev_counts = sev_counts.rename(
                columns={"severity": "Severity", "count": "Count"}
            )
        else:
            sev_counts.columns = ["Severity", "Count"]

        sev_chart_col, sev_table_col = st.columns([2, 1])

        with sev_chart_col:
            sev_domain = list(PRIORITY_CONFIG.keys())
            sev_range = [v["color"] for v in PRIORITY_CONFIG.values()]

            sev_chart = (
                alt.Chart(sev_counts)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("Severity:N", sort=sev_domain, title=None),
                    y=alt.Y("Count:Q", title="Issue Count"),
                    color=alt.Color(
                        "Severity:N",
                        scale=alt.Scale(domain=sev_domain, range=sev_range),
                        legend=None,
                    ),
                    tooltip=["Severity", "Count"],
                )
                .properties(height=240, title="Issues by Severity")
            )
            st.altair_chart(sev_chart, use_container_width=True)

        with sev_table_col:
            # Issue code summary
            code_counts = (
                issues_df.groupby("issue_code")
                .size()
                .reset_index(name="Count")
                .sort_values("Count", ascending=False)
            )
            st.markdown("**By Issue Code**")
            st.dataframe(code_counts, use_container_width=True, hide_index=True)

        # ── Status filter ──
        issue_status_filter = st.radio(
            "Filter by status",
            ["All Issues", "Open Only", "Resolved Only"],
            horizontal=True,
        )
        display_issues = issues_df.copy()
        if issue_status_filter == "Open Only":
            display_issues = display_issues[
                display_issues["status"].isin(OPEN_ISSUE_STATUSES)
            ]
        elif issue_status_filter == "Resolved Only":
            display_issues = display_issues[
                ~display_issues["status"].isin(OPEN_ISSUE_STATUSES)
            ]

        # ── Issue table ──
        issue_cols = [
            "issue_id", "issue_code", "issue_type", "issue_subtype",
            "severity", "status",
            "client_code", "vendor_code",
            "file_id", "member_id", "claim_record_id",
            "entity_name", "entity_key",
            "issue_age_hours", "issue_message",
            "issue_description", "detected_at",
        ]
        st.dataframe(
            display_issues[safe_col_list(issue_cols, display_issues.columns)],
            use_container_width=True,
            hide_index=True,
            column_config={
                "issue_age_hours": st.column_config.NumberColumn(
                    "Age (hrs)", format="%.1f"
                ),
            },
        )

        csv_issues = display_issues[
            safe_col_list(issue_cols, display_issues.columns)
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Issues CSV",
            csv_issues,
            "data_quality_issues.csv",
            "text/csv",
            use_container_width=True,
        )



with tab_analytics:
    st.subheader("📊 Case Analytics")
    st.caption(
        "Operational analytics showing how cases distribute across types, "
        "priorities, queues, and statuses. Use this to identify patterns, "
        "bottlenecks, and workload imbalances."
    )

    if filtered_df.empty:
        st.info("No cases match the current filters for analytics.")
    else:
        # ── Row 1: Case Type + Status ──
        a_left, a_right = st.columns(2)

        with a_left:
            type_counts = (
                filtered_df["case_type"]
                .value_counts()
                .reset_index()
            )
            if type_counts.columns.tolist() == ["index", "case_type"]:
                type_counts.columns = ["Case Type", "Count"]
            elif "case_type" in type_counts.columns and "count" in type_counts.columns:
                type_counts = type_counts.rename(
                    columns={"case_type": "Case Type", "count": "Count"}
                )
            else:
                type_counts.columns = ["Case Type", "Count"]

            type_chart = (
                alt.Chart(type_counts)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("Count:Q", title="Count"),
                    y=alt.Y("Case Type:N", sort="-x", title=None),
                    color=alt.value("#4B9DFF"),
                    tooltip=["Case Type", "Count"],
                )
                .properties(height=250, title="Cases by Type")
            )
            st.altair_chart(type_chart, use_container_width=True)
            st.dataframe(type_counts, use_container_width=True, hide_index=True)

        with a_right:
            status_counts = (
                filtered_df["status"]
                .value_counts()
                .reset_index()
            )
            if status_counts.columns.tolist() == ["index", "status"]:
                status_counts.columns = ["Status", "Count"]
            elif "status" in status_counts.columns and "count" in status_counts.columns:
                status_counts = status_counts.rename(
                    columns={"status": "Status", "count": "Count"}
                )
            else:
                status_counts.columns = ["Status", "Count"]

            status_chart = (
                alt.Chart(status_counts)
                .mark_arc(innerRadius=50, cornerRadius=4)
                .encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color("Status:N"),
                    tooltip=["Status", "Count"],
                )
                .properties(height=250, title="Cases by Status")
            )
            st.altair_chart(status_chart, use_container_width=True)
            st.dataframe(status_counts, use_container_width=True, hide_index=True)

        # ── Row 2: Queue + Priority ──
        b_left, b_right = st.columns(2)

        with b_left:
            queue_counts = (
                filtered_df["assignment_group"]
                .value_counts()
                .reset_index()
            )
            if queue_counts.columns.tolist() == ["index", "assignment_group"]:
                queue_counts.columns = ["Queue", "Count"]
            elif "assignment_group" in queue_counts.columns and "count" in queue_counts.columns:
                queue_counts = queue_counts.rename(
                    columns={"assignment_group": "Queue", "count": "Count"}
                )
            else:
                queue_counts.columns = ["Queue", "Count"]

            queue_chart = (
                alt.Chart(queue_counts)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("Count:Q", title="Count"),
                    y=alt.Y("Queue:N", sort="-x", title=None),
                    color=alt.value("#9B59B6"),
                    tooltip=["Queue", "Count"],
                )
                .properties(height=220, title="Cases by Assignment Queue")
            )
            st.altair_chart(queue_chart, use_container_width=True)
            st.dataframe(queue_counts, use_container_width=True, hide_index=True)

        with b_right:
            # Priority already charted on Tab 1, but show table here too
            pri_table = (
                filtered_df.groupby("priority")
                .agg(
                    total=("case_id", "count"),
                    open=("is_open_case", "sum"),
                    at_risk=("is_at_risk", "sum"),
                    breached=("is_breached", "sum"),
                )
                .reset_index()
            )
            if not pri_table.empty:
                pri_table["priority_sort"] = sort_priority_series(pri_table["priority"])
                pri_table = pri_table.sort_values("priority_sort").drop(
                    columns=["priority_sort"]
                )
            st.markdown("**Priority Breakdown (Detail)**")
            st.dataframe(pri_table, use_container_width=True, hide_index=True)

            # Client distribution
            client_counts = (
                filtered_df["client_code"]
                .value_counts()
                .reset_index()
            )
            if client_counts.columns.tolist() == ["index", "client_code"]:
                client_counts.columns = ["Client", "Count"]
            elif "client_code" in client_counts.columns and "count" in client_counts.columns:
                client_counts = client_counts.rename(
                    columns={"client_code": "Client", "count": "Count"}
                )
            else:
                client_counts.columns = ["Client", "Count"]
                            # Client distribution (continued from b_right column)
            client_counts = (
                filtered_df["client_code"]
                .value_counts()
                .reset_index()
            )
            if client_counts.columns.tolist() == ["index", "client_code"]:
                client_counts.columns = ["Client", "Count"]
            elif "client_code" in client_counts.columns and "count" in client_counts.columns:
                client_counts = client_counts.rename(
                    columns={"client_code": "Client", "count": "Count"}
                )
            else:
                client_counts.columns = ["Client", "Count"]

            st.markdown("**Cases by Client**")
            st.dataframe(client_counts, use_container_width=True, hide_index=True)

        # ── Row 3: RCA Category + Vendor ──
        c_left, c_right = st.columns(2)

        with c_left:
            st.markdown("### 🔍 Root Cause Analysis Coverage")
            st.caption(
                "How many cases have an identified root cause category? "
                "Cases without RCA need investigation attention."
            )

            rca_filled = filtered_df["root_cause_category"].notna() & (
                filtered_df["root_cause_category"].astype(str).str.strip() != ""
            )
            rca_yes = int(rca_filled.sum())
            rca_no = int((~rca_filled).sum())

            rca_data = pd.DataFrame({
                "RCA Status": ["Identified", "Not Yet Identified"],
                "Count": [rca_yes, rca_no],
            })

            rca_chart = (
                alt.Chart(rca_data)
                .mark_arc(innerRadius=50, cornerRadius=4)
                .encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color(
                        "RCA Status:N",
                        scale=alt.Scale(
                            domain=["Identified", "Not Yet Identified"],
                            range=["#2ECC71", "#FF4B4B"],
                        ),
                    ),
                    tooltip=["RCA Status", "Count"],
                )
                .properties(height=230, title="Root Cause Identification")
            )
            st.altair_chart(rca_chart, use_container_width=True)

            # RCA category breakdown (for cases that have one)
            rca_categories = (
                filtered_df[rca_filled]
                .groupby("root_cause_category")
                .size()
                .reset_index(name="Count")
                .sort_values("Count", ascending=False)
            )
            if not rca_categories.empty:
                st.markdown("**Root Cause Categories**")
                st.dataframe(
                    rca_categories,
                    use_container_width=True,
                    hide_index=True,
                )

        with c_right:
            st.markdown("### 📈 Case Age Distribution")
            st.caption(
                "How old are the current filtered cases? Older cases may "
                "need escalation or re-prioritization."
            )

            if "case_age_hours" in filtered_df.columns:
                age_df = filtered_df[["case_age_hours"]].dropna().copy()
                if not age_df.empty:
                    # Bucket ages
                    def age_bucket(hours):
                        h = float(safe_val(hours, 0))
                        if h <= 4:
                            return "0–4 hrs"
                        elif h <= 8:
                            return "4–8 hrs"
                        elif h <= 24:
                            return "8–24 hrs"
                        elif h <= 72:
                            return "1–3 days"
                        else:
                            return "3+ days"

                    age_df["Age Bucket"] = age_df["case_age_hours"].apply(age_bucket)
                    bucket_order = ["0–4 hrs", "4–8 hrs", "8–24 hrs", "1–3 days", "3+ days"]
                    bucket_counts = (
                        age_df["Age Bucket"]
                        .value_counts()
                        .reindex(bucket_order, fill_value=0)
                        .reset_index()
                    )
                    if bucket_counts.columns.tolist() == ["index", "Age Bucket"]:
                        bucket_counts.columns = ["Age Bucket", "Count"]
                    elif "Age Bucket" in bucket_counts.columns and "count" in bucket_counts.columns:
                        bucket_counts = bucket_counts.rename(
                            columns={"count": "Count"}
                        )
                    else:
                        bucket_counts.columns = ["Age Bucket", "Count"]

                    age_chart = (
                        alt.Chart(bucket_counts)
                        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                        .encode(
                            x=alt.X(
                                "Age Bucket:N",
                                sort=bucket_order,
                                title=None,
                            ),
                            y=alt.Y("Count:Q", title="Cases"),
                            color=alt.Color(
                                "Age Bucket:N",
                                scale=alt.Scale(
                                    domain=bucket_order,
                                    range=[
                                        "#2ECC71", "#4B9DFF", "#FFD700",
                                        "#FFA500", "#FF4B4B",
                                    ],
                                ),
                                legend=None,
                            ),
                            tooltip=["Age Bucket", "Count"],
                        )
                        .properties(height=230, title="Case Age Buckets")
                    )
                    st.altair_chart(age_chart, use_container_width=True)
                    st.dataframe(
                        bucket_counts,
                        use_container_width=True,
                        hide_index=True,
                    )

            # Vendor distribution
            vendor_counts = (
                filtered_df["vendor_code"]
                .dropna()
                .value_counts()
                .reset_index()
            )
            if vendor_counts.columns.tolist() == ["index", "vendor_code"]:
                vendor_counts.columns = ["Vendor", "Count"]
            elif "vendor_code" in vendor_counts.columns and "count" in vendor_counts.columns:
                vendor_counts = vendor_counts.rename(
                    columns={"vendor_code": "Vendor", "count": "Count"}
                )
            else:
                vendor_counts.columns = ["Vendor", "Count"]

            if not vendor_counts.empty:
                st.markdown("**Cases by Vendor**")
                st.dataframe(
                    vendor_counts,
                    use_container_width=True,
                    hide_index=True,
                )

        # ── Download analytics summary ──
        st.divider()
        csv_analytics = filtered_df[
            safe_col_list(
                [
                    "case_number", "case_id", "case_type", "priority",
                    "severity", "status", "sla_watch",
                    "assignment_group", "assigned_to",
                    "client_code", "vendor_code",
                    "root_cause_category", "case_age_hours",
                    "target_hours", "target_due_at",
                    "is_at_risk", "is_breached",
                    "short_description",
                ],
                filtered_df.columns,
            )
        ].to_csv(index=False)
        st.download_button(
            "⬇️ Download Analytics Data CSV",
            csv_analytics,
            "case_analytics.csv",
            "text/csv",
            use_container_width=True,
        )



with tab_howto:
    st.subheader("❓ How Issue Triage Works")
    st.caption(
        "A plain-English explanation of what this page does, why it matters, "
        "and what all the terms mean — written so anyone can understand."
    )

    st.markdown(r"""

Imagine a hospital's front desk. When something goes wrong — a patient's
insurance doesn't show up, a bill seems wrong, a record is missing — someone
at the front desk **writes a trouble ticket**, figures out **how urgent** it
is, sends it to the **right team**, and makes sure it gets **fixed before
the deadline**.

Issue Triage is exactly that, but for **data problems** instead of people
problems. When the data pipeline detects something wrong — a missing file,
a member without insurance, an overcharged deductible — this page is where
the operations team comes to:

1. **See all the problems** ranked by urgency
2. **Pick up a problem** and investigate it
3. **Track the deadline** (SLA) for fixing it
4. **Find the root cause** and resolve it

---


A support case is a **trouble ticket** for a data problem. It includes:

- **What happened** — the short description and linked issue
- **How bad is it** — priority (CRITICAL, HIGH, MEDIUM, LOW)
- **Who should fix it** — the assignment queue (which team)
- **When it must be fixed by** — the SLA deadline
- **Why it happened** — the root cause (once investigated)

Cases are created automatically when the system detects a data quality
issue that's serious enough to need human attention.

---


SLA stands for **Service Level Agreement**. It's a promise about how fast
a problem will be fixed. Think of it like a pizza delivery guarantee:

| Priority | SLA Target | Like Ordering… |
|---|---|---|
| 🔴 CRITICAL | 4 hours | Emergency — the kitchen drops everything |
| 🟠 HIGH | 8 hours | Rush order — top of the queue |
| 🟡 MEDIUM | 24 hours | Normal order — done by tomorrow |
| 🔵 LOW | 72 hours | Whenever you get to it — no rush |

The SLA clock starts when the case is opened. If the deadline passes
without resolution, the SLA is **breached** (⛔). If the case is getting
close to the deadline (80% elapsed), it's **at risk** (⚠️).

---


When a case is created, it's automatically routed to the right team
based on what kind of problem it is:

| Queue | What It Handles | Icon |
|---|---|---|
| `ops_file_queue` | Missing files, file errors, duplicates | 📁 |
| `ops_eligibility_queue` | Member enrollment and coverage issues | 👤 |
| `ops_claims_queue` | Claim processing and eligibility check failures | 🧾 |
| `ops_recon_queue` | Accumulator breaches and family rollup mismatches | 💰 |
| `ops_triage_queue` | Anything that doesn't fit the other queues | 🎫 |

---


When a problem is found, someone needs to figure out **why** it happened,
not just **what** happened. This is called Root Cause Analysis.

For example:
- **What happened:** A member shows as ineligible
- **Why it happened (RCA):** The vendor's eligibility file was 2 hours late,
  so the member's coverage wasn't loaded before the claim arrived

Knowing the root cause helps prevent the same problem from happening again.

---


1. **Support Queue** — Every case ranked by priority and age. CRITICAL
   cases at the top, oldest within each priority rise first.
2. **Case Investigation** — Pick any case to see its full story: who
   reported it, what's affected, the SLA countdown, the linked issue,
   and guidance on how to investigate.
3. **SLA Watchlist** — Focused view of breached and at-risk cases with
   visual SLA progress bars.
4. **Data Quality Issues** — The raw problem signals that created the
   cases, with severity charts and status filters.
5. **Case Analytics** — Charts showing how cases break down by type,
   priority, queue, status, age, client, vendor, and RCA coverage.

---


| Term | What It Means |
|---|---|
| **Support case** | A trouble ticket for a data problem |
| **Data quality issue** | The raw signal — what the system detected as wrong |
| **Priority** | How urgent the case is (CRITICAL → LOW) |
| **Severity** | How impactful the underlying problem is |
| **SLA** | The deadline for resolving the case |
| **At risk** | Case has used 80%+ of its SLA window |
| **Breached** | Case missed its SLA deadline |
| **Assignment queue** | Which team should work on the case |
| **Assigned to** | The specific person working the case |
| **Escalation level** | How many times the case has been escalated |
| **Root cause category** | Why the problem happened |
| **Case type** | Which scenario or issue pattern the case represents |

---


Data pipeline runs  
→ Validation detects a problem  
→ `data_quality_issues` record created  
→ `support_cases` record created (with routing)  
→ `sla_tracking` record created (with deadline)  
→ Operations team triages here on Issue Triage  
→ Investigation may lead to:
• File Monitoring
• Accumulator Reconciliation
• Vendor communication
• Data correction

---


1. Look at the **Key Findings** banner — it tells you immediately
   what needs attention
2. Check the **Support Queue** tab for the prioritized worklist
3. If an SLA is breached or at risk, switch to the **SLA Watchlist**
   tab for a focused view
4. Click **Case Investigation** and select a case to see the full
   picture: context, SLA countdown, linked issue, and RCA guidance
5. Review **Data Quality Issues** to see the raw signals
6. Use **Case Analytics** to spot patterns and workload imbalances

---


- **Operations analysts** triage cases, assign owners, and track SLAs
- **Team leads** monitor queue depth, SLA compliance, and workload balance
- **Data engineers** investigate root causes and fix pipeline issues
- **Client managers** track cases affecting their client groups
- **Compliance teams** monitor SLA adherence and escalation patterns
""")

    st.info(
        "💡 **Tip:** Use the sidebar filters to focus on specific priorities, "
        "queues, or SLA statuses. The quick-filter checkboxes let you jump "
        "straight to open, unassigned, at-risk, or breached cases."
    )

    # ── Connected pages guidance ──
    with st.expander("🔗 Connected Pages — Where to Go Next", expanded=False):
        st.markdown("""
**From Issue Triage, you might need to visit:**

| If the Case Is About… | Go To… | Why |
|---|---|---|
| A missing or duplicate file | **File Monitoring** | To check file delivery status and processing history |
| An OOP max breach or family rollup issue | **Accumulator Reconciliation** | To investigate accumulator balances and plan thresholds |
| A claim for an ineligible member | **File Monitoring** | To check if an eligibility file is pending |
| You want to trigger a test scenario | **Scenario Control Center** | To inject a specific incident pattern |
| You need the raw file or run details | **File Monitoring** | To see processing runs and file metadata |

**Navigation tip:** The Case Investigation tab shows `file_id`,
`member_id`, and `claim_record_id` — use these as lookup keys
when you navigate to other pages.
        """)



st.divider()
st.caption(
    "Issue Triage · Eligibility & Accumulator Operations Command Center · "
    "Data is simulated — no real PHI"
)