import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

"""
src/app/pages/Investigation_Playbooks.py

Step-by-step interactive investigation playbooks for healthcare data
operations. Each playbook walks through a real-world scenario with
executable SQL steps, plain-English explanations, decision trees,
and guided conclusions.

Mirrors what a Jupyter notebook would contain, but rendered interactively
in Streamlit with live query execution.
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
import sqlite3
import re
import time
from pathlib import Path
from datetime import datetime

from config.settings import DB_PATH
TTL_SECONDS = 300
MAX_RESULT_ROWS = 500

UNSAFE_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|REPLACE\s+INTO|VACUUM)\b",
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


def is_safe_query(sql_text: str) -> bool:
    cleaned = re.sub(r"--.*$", "", sql_text, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    return UNSAFE_PATTERNS.search(cleaned) is None


def run_query(sql_text: str) -> tuple:
    if not is_safe_query(sql_text):
        return None, 0, "⛔ Write operations are not allowed."
    conn = get_connection()
    try:
        start = time.time()
        df = pd.read_sql_query(sql_text, conn)
        elapsed = round((time.time() - start) * 1000, 1)
        if len(df) > MAX_RESULT_ROWS:
            df = df.head(MAX_RESULT_ROWS)
        return df, elapsed, None
    except Exception as e:
        return None, 0, f"❌ Query Error: {str(e)}"
    finally:
        conn.close()


def render_step(step_num, title, explanation, sql, look_for, key_prefix):
    """Render one investigation step with explanation, SQL, and results."""
    st.markdown(f"### Step {step_num}: {title}")
    st.markdown(explanation)

    st.code(sql, language="sql")

    if st.button(
        f"▶️ Run Step {step_num}",
        key=f"{key_prefix}_step_{step_num}",
    ):
        with st.spinner(f"Running Step {step_num}..."):
            df, elapsed, error = run_query(sql)

        if error:
            st.error(error)
        elif df is not None:
            st.success(f"✅ {len(df)} rows returned in {elapsed} ms")
            st.dataframe(df, use_container_width=True, hide_index=True)

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                f"⬇️ Download Step {step_num} Results",
                csv,
                f"{key_prefix}_step_{step_num}.csv",
                "text/csv",
                key=f"dl_{key_prefix}_step_{step_num}",
            )

    st.info(f"🔎 **What to Look For:** {look_for}")
    st.divider()


# ═══════════════════════════════════════════════════════════════
# PLAYBOOK DEFINITIONS
# ═══════════════════════════════════════════════════════════════

def playbook_missing_file():
    """Investigation Playbook: Missing Inbound File"""

    st.markdown("""
    ## 📋 Investigation Summary

    | Field | Value |
    |-------|-------|
    | **Scenario** | Missing Inbound File |
    | **Severity** | 🔴 CRITICAL |
    | **SLA** | 4 hours |
    | **Assignment** | ops_file_queue |
    | **Goal** | Determine if the expected file was truly missing, identify the vendor and client impacted, assess downstream consequences, and initiate resolution |

    ---

    ### 🧒 What's Happening?

    **Imagine your school bus is supposed to arrive every morning at 8:00 AM.
    Today it's 9:00 AM and there's no bus. Was it cancelled? Is it stuck in traffic?
    Did the driver forget? We need to investigate!**

    In healthcare data operations, vendors send us files every day — eligibility
    lists, claims, accumulator updates. When an expected file doesn't arrive,
    members might not get their coverage updated, claims might be processed against
    stale data, and accumulators might drift. This playbook walks you through
    investigating a missing file step by step.

    ---
    """)

    # Step 1
    render_step(
        step_num=1,
        title="Check the File Schedule — What Were We Expecting?",
        explanation=(
            "🧒 **First, check the bus schedule.** Before we panic, let's confirm "
            "what files were expected today, from which vendors, and at what time. "
            "This query shows all active file schedules."
        ),
        sql="""SELECT
    fs.schedule_id,
    fs.file_type,
    fs.file_direction,
    fs.frequency,
    fs.expected_time,
    c.client_name,
    v.vendor_name,
    fs.day_of_week,
    fs.day_of_month
FROM file_schedules fs
JOIN clients c ON fs.client_id = c.client_id
JOIN vendors v ON fs.vendor_id = v.vendor_id
WHERE fs.is_active = 1
ORDER BY fs.expected_time;""",
        look_for=(
            "Identify the schedule that matches the missing file. Note the "
            "`expected_time`, `frequency`, and `vendor_name`. Daily files "
            "that haven't arrived past their expected time are late. "
            "Weekly files need to be checked against `day_of_week`."
        ),
        key_prefix="missing_file",
    )

    # Step 2
    render_step(
        step_num=2,
        title="Check File Inventory — Did Anything Arrive Recently?",
        explanation=(
            "🧒 **Check the mailbox — maybe the package arrived but was put in "
            "the wrong spot.** This query looks at the most recent files we've "
            "received to see if the file arrived under a different name or status."
        ),
        sql="""SELECT
    f.file_id,
    f.file_name,
    f.file_type,
    f.processing_status,
    f.expected_date,
    f.received_ts,
    c.client_name,
    v.vendor_name,
    f.row_count,
    f.file_size
FROM inbound_files fi
LEFT JOIN clients c ON f.client_id = c.client_id
LEFT JOIN vendors v ON f.vendor_id = v.vendor_id
ORDER BY f.expected_date DESC, f.received_ts DESC
LIMIT 15;""",
        look_for=(
            "Look for files with `file_status` = 'MISSING' — these were expected "
            "but never arrived. Also check for recent files with status 'ERROR' — "
            "the file may have arrived but failed on intake. Compare `expected_date` "
            "to `received_at` to assess lateness."
        ),
        key_prefix="missing_file",
    )

    # Step 3
    render_step(
        step_num=3,
        title="Check for Related Data Quality Issues",
        explanation=(
            "🧒 **Check if the teacher already noticed the bus is late and wrote "
            "it on the board.** Our system automatically creates data quality "
            "issues when files go missing. Let's see if one was already detected."
        ),
        sql="""SELECT
    dqi.issue_id,
    dqi.issue_code,
    dqi.severity,
    dqi.status          AS issue_status,
    dqi.issue_description,
    dqi.detected_at,
    c.client_name,
    v.vendor_name
FROM data_quality_issues dqi
LEFT JOIN clients c ON dqi.client_id = c.client_id
LEFT JOIN vendors v ON dqi.vendor_id = v.vendor_id
WHERE dqi.issue_code = 'MISSING_INBOUND_FILE'
ORDER BY dqi.detected_at DESC;""",
        look_for=(
            "If rows appear, the system already detected the missing file. "
            "Note the `detected_at` timestamp — this is when the clock started "
            "for SLA purposes. If no rows appear, the detection may not have "
            "run yet, or the file isn't actually scheduled for today."
        ),
        key_prefix="missing_file",
    )

    # Step 4
    render_step(
        step_num=4,
        title="Check the Support Case and SLA Status",
        explanation=(
            "🧒 **Did someone already call the bus company?** When a missing file "
            "issue is detected, a support case should be created automatically "
            "with an SLA timer. Let's check its status."
        ),
        sql="""SELECT
    sc.case_number,
    sc.short_description,
    sc.priority,
    sc.status           AS case_status,
    sc.assignment_group,
    sc.assigned_to,
    sc.opened_at,
    sc.resolved_at,
    sla.target_hours,
    sla.target_due_at,
    sla.status          AS sla_status,
    sla.is_at_risk,
    sla.is_breached,
    ROUND(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                   AS elapsed_hours
FROM support_cases sc
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE sc.short_description LIKE '%missing%'
   OR sc.short_description LIKE '%MISSING%'
ORDER BY sc.opened_at DESC;""",
        look_for=(
            "Check `sla_status` — if 'AT_RISK' or 'BREACHED', escalation is needed "
            "immediately. The `elapsed_hours` vs `target_hours` comparison tells you "
            "how much time is left. If no case exists (empty result), there's an "
            "automation gap that needs to be addressed."
        ),
        key_prefix="missing_file",
    )

    # Step 5
    render_step(
        step_num=5,
        title="Assess Downstream Impact — Who Is Affected?",
        explanation=(
            "🧒 **The bus didn't come — so which kids missed school?** If a file "
            "is missing, the members in that file don't get updated. This query "
            "checks how many members belong to the affected client and vendor "
            "combination."
        ),
        sql="""SELECT
    c.client_name,
    COUNT(DISTINCT m.member_id) AS affected_members,
    COUNT(DISTINCT m.family_id) AS affected_families,
    COUNT(DISTINCT CASE
        WHEN a.individual_oop_accum > a.individual_oop_max * 0.8
        THEN m.member_id
    END)                        AS high_utilizers_at_risk
FROM members m
JOIN clients c ON m.client_id = c.client_id
LEFT JOIN accumulator_snapshots a
    ON m.member_id = a.member_id
   AND a.individual_oop_accum > 0
WHERE ep.status = 'ACTIVE'
GROUP BY c.client_name
ORDER BY affected_members DESC;""",
        look_for=(
            "The `affected_members` count tells you the blast radius. "
            "`high_utilizers_at_risk` counts members already over 80% of their "
            "OOP max — these members are most vulnerable to stale accumulator data. "
            "If the number is high, this is a priority escalation."
        ),
        key_prefix="missing_file",
    )

    # Step 6
    render_step(
        step_num=6,
        title="Check the Vendor's Recent Delivery History",
        explanation=(
            "🧒 **Has the bus been late before? Check the attendance record!** "
            "This query looks at the vendor's file delivery history to see if "
            "missing or late files are a pattern."
        ),
        sql="""SELECT
    v.vendor_name,
    f.file_type,
    f.processing_status,
    COUNT(*)            AS file_count,
    MIN(f.expected_date)   AS earliest,
    MAX(f.expected_date)   AS latest
FROM inbound_files fi
JOIN vendors v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name, f.file_type, f.processing_status
ORDER BY v.vendor_name, f.file_type, file_count DESC;""",
        look_for=(
            "If the vendor has multiple 'MISSING' or 'ERROR' files, this is a "
            "recurring vendor reliability issue. Include this evidence in your "
            "escalation to the vendor relationship manager. A single occurrence "
            "may be a one-time infrastructure issue."
        ),
        key_prefix="missing_file",
    )

    # Decision Tree
    st.markdown("""
    ---
    ### 🌳 Decision Tree — What Do I Do Next?

    ```
    Missing File Confirmed?
    ├─ YES — File never arrived
    │  ├─ Vendor contacted?
    │  │  ├─ YES → Awaiting vendor response
    │  │  │  └─ Set case status to IN_PROGRESS
    │  │  └─ NO → Contact vendor primary contact
    │  │     └─ Use Vendor Contacts in Schema Explorer
    │  ├─ Is this a recurring pattern?
    │  │  ├─ YES → Escalate to vendor management
    │  │  └─ NO → Log as one-time incident
    │  └─ SLA at risk?
    │     ├─ YES → Escalate immediately
    │     └─ NO → Monitor and follow standard process
    │
    └─ NO — File arrived but with issues
       ├─ Status = ERROR → Check processing_runs for error details
       ├─ Status = RECEIVED but not PROCESSED → Pipeline stall
       └─ Status = PROCESSED → False alarm, close the case
    ```

    ---
    ### ✅ Conclusion Checklist

    After completing this investigation, you should be able to answer:

    - [ ] Was the file truly missing, or did it arrive with issues?
    - [ ] Which client and vendor are affected?
    - [ ] How many members are impacted?
    - [ ] Is there a support case with an active SLA?
    - [ ] Is this a one-time issue or a recurring vendor problem?
    - [ ] What is the recommended next action?
    """)


def playbook_oop_breach():
    """Investigation Playbook: Accumulator OOP Max Breach"""

    st.markdown("""
    ## 📋 Investigation Summary

    | Field | Value |
    |-------|-------|
    | **Scenario** | Accumulator Exceeds OOP Maximum |
    | **Severity** | 🟠 HIGH |
    | **SLA** | 8 hours |
    | **Assignment** | ops_recon_queue |
    | **Goal** | Determine if the OOP breach is legitimate (high utilizer) or caused by data error (duplicate claim, wrong plan limit, missed void), and take corrective action |

    ---

    ### 🧒 What's Happening?

    **Imagine you have a jar that can hold exactly 100 marbles. Someone says there
    are 115 marbles in it — that's impossible! Either someone counted wrong,
    put in extra marbles by mistake, or the jar is actually bigger than we thought.**

    In healthcare, every member has an Out-of-Pocket (OOP) maximum — the most they
    should pay in a year. When our accumulator shows they've paid MORE than the
    maximum, something is wrong. Either a claim was posted twice, the plan limit
    is configured wrong, or we need to verify it's a legitimate high-cost situation.

    ---
    """)

    # Step 1
    render_step(
        step_num=1,
        title="Find All Members Who Have Breached Their OOP Max",
        explanation=(
            "🧒 **Check all the jars — which ones have overflowed?** "
            "This query finds every accumulator where the current amount "
            "is higher than the allowed maximum."
        ),
        sql="""SELECT
    a.accumulator_id,
    a.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    m.family_id,
    bp.plan_name,
    a.benefit_year,
    a.individual_oop_accum,
    a.individual_oop_max,
    ROUND(a.individual_oop_accum - a.individual_oop_max, 2)          AS overage,
    ROUND(a.individual_oop_accum * 100.0 / a.individual_oop_max, 1)  AS pct_of_limit,
    a.snapshot_ts
FROM accumulator_snapshots a
JOIN members m ON a.member_id = m.member_id
JOIN benefit_plans bp ON a.plan_id = bp.plan_id
WHERE a.individual_oop_accum > a.individual_oop_max
  AND a.individual_oop_max > 0
ORDER BY overage DESC;""",
        look_for=(
            "Focus on the largest `overage` values first. Members over 110-120% "
            "are likely data errors, not just high utilization. Note the "
            "`accumulator_type` — breaches in `oop_individual` can cascade to "
            "`oop_family`."
        ),
        key_prefix="oop_breach",
    )

    # Step 2
    render_step(
        step_num=2,
        title="Pull the Breached Member's Complete Claim History",
        explanation=(
            "🧒 **Open the jar and count every marble one by one.** "
            "This query shows all claims for the first seeded member, in "
            "date order. Change the member_id to investigate a different member."
        ),
        sql="""SELECT
    cr.claim_record_id,
    cr.claim_number,
    cr.service_date,
    cr.provider_name,
    cr.procedure_code,
    cr.diagnosis_code,
    cr.billed_amount,
    cr.allowed_amount,
    cr.paid_amount,
    cr.member_responsibility,
    cr.claim_status,
    cr.source_file_id,
    cr.created_at       AS claim_loaded_at
FROM claims cr
WHERE cr.member_id = 'MBR-001'
  AND cr.claim_status NOT IN ('VOIDED', 'REJECTED')
ORDER BY cr.service_date ASC, cr.created_at ASC;""",
        look_for=(
            "Look for duplicate `claim_number` values — same claim posted twice. "
            "Look for claims with identical `service_date`, `procedure_code`, and "
            "`billed_amount` — classic duplicate pattern. Check if any single "
            "`member_responsibility` value is unusually large."
        ),
        key_prefix="oop_breach",
    )

    # Step 3
    render_step(
        step_num=3,
        title="Cross-Check: Accumulator vs. Claim Sum (Drift Detection)",
        explanation=(
            "🧒 **Count the marbles yourself and compare to what the label says.** "
            "This query independently sums the claims and compares to the "
            "accumulator snapshot to detect drift."
        ),
        sql="""WITH claim_sum AS (
    SELECT
        cr.member_id,
        SUM(cr.member_responsibility) AS claims_total
    FROM claims cr
    WHERE cr.member_id = 'MBR-001'
      AND cr.claim_status NOT IN ('VOIDED', 'REJECTED')
    GROUP BY cr.member_id
)
SELECT
    a.member_id,
    a.benefit_year,
    a.individual_oop_accum        AS accumulator_value,
    cs.claims_total,
    a.individual_oop_max          AS oop_max,
    ROUND(a.individual_oop_accum - cs.claims_total, 2) AS drift,
    CASE
        WHEN ABS(a.individual_oop_accum - cs.claims_total) < 0.01
            THEN '✅ IN SYNC'
        WHEN a.individual_oop_accum > cs.claims_total
            THEN '⬆️ ACCUMULATOR OVER'
        ELSE '⬇️ ACCUMULATOR UNDER'
    END AS sync_status
FROM accumulator_snapshots a
JOIN claim_sum cs ON a.member_id = cs.member_id
WHERE a.individual_oop_accum > 0;""",
        look_for=(
            "`✅ IN SYNC` means the accumulator correctly reflects the claims — "
            "the breach is real (claims are genuinely that high). "
            "`⬆️ ACCUMULATOR OVER` means the snapshot is inflated — look for "
            "voided claims that weren't backed out. "
            "`⬇️ ACCUMULATOR UNDER` means claims were missed."
        ),
        key_prefix="oop_breach",
    )

    # Step 4
    render_step(
        step_num=4,
        title="Find the Breach Point — Which Claim Caused the Overflow?",
        explanation=(
            "🧒 **Stack the blocks one by one. Which block made the tower "
            "taller than allowed?** This running total identifies the exact "
            "claim that pushed the member over their OOP maximum."
        ),
        sql="""WITH oop_limit AS (
    SELECT a.individual_oop_max
    FROM accumulator_snapshots a
    WHERE a.member_id = 'MBR-001'
      AND a.individual_oop_accum > 0
    LIMIT 1
),
running AS (
    SELECT
        cr.claim_number,
        cr.service_date,
        cr.provider_name,
        cr.member_responsibility,
        SUM(cr.member_responsibility) OVER (
            ORDER BY cr.service_date, cr.created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM claims cr
    WHERE cr.member_id = 'MBR-001'
      AND cr.claim_status NOT IN ('VOIDED', 'REJECTED')
)
SELECT
    r.claim_number,
    r.service_date,
    r.provider_name,
    r.member_responsibility,
    r.running_total,
    ol.limit_amount AS oop_max,
    ROUND(r.running_total - ol.limit_amount, 2) AS overage,
    CASE
        WHEN r.running_total > ol.limit_amount
         AND r.running_total - r.member_responsibility <= ol.limit_amount
            THEN '>>> BREACH POINT <<<'
        WHEN r.running_total > ol.limit_amount
            THEN 'POST-BREACH'
        ELSE 'WITHIN LIMIT'
    END AS breach_flag
FROM running r
CROSS JOIN oop_limit ol
ORDER BY r.service_date;""",
        look_for=(
            "Find the `>>> BREACH POINT <<<` row — that's THE claim. "
            "Was it a legitimate expensive service or a suspicious entry? "
            "Look at `POST-BREACH` rows too — the member should have had $0 "
            "member_responsibility after hitting the max."
        ),
        key_prefix="oop_breach",
    )

    # Step 5
    render_step(
        step_num=5,
        title="Verify the Plan Limit Is Correct",
        explanation=(
            "🧒 **Maybe the jar is actually supposed to hold 200 marbles, not 100. "
            "Someone put the wrong label on it!** This query checks the benefit "
            "plan configuration to verify the OOP max is correct."
        ),
        sql="""SELECT
    bp.plan_id,
    bp.plan_name,
    bp.plan_type,
    bp.individual_oop_max,
    bp.family_oop_max,
    bp.individual_deductible,
    bp.family_deductible,
    bp.benefit_year,
    bp.benefit_year,
    bp.status,
    c.client_name
FROM benefit_plans bp
JOIN clients c ON bp.client_id = c.client_id
ORDER BY bp.plan_name;""",
        look_for=(
            "Verify `individual_oop_max` matches what the member's accumulator "
            "`limit_amount` shows. If they differ, the plan was changed but the "
            "accumulator wasn't updated. Also verify the benefit year dates — "
            "accumulators should reset at year boundaries."
        ),
        key_prefix="oop_breach",
    )

    # Step 6
    render_step(
        step_num=6,
        title="Check the Support Case and SLA Status",
        explanation=(
            "🧒 **Has someone already started fixing this? Let's check the "
            "repair ticket.** This traces the breach to any existing support "
            "case and SLA record."
        ),
        sql="""SELECT
    dqi.issue_id,
    dqi.issue_code,
    dqi.severity,
    dqi.status           AS issue_status,
    dqi.detected_at,
    sc.case_number,
    sc.priority,
    sc.status            AS case_status,
    sc.assignment_group,
    sc.root_cause_category,
    sla.target_hours,
    sla.target_due_at,
    sla.status           AS sla_status,
    sla.is_breached,
    ROUND(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                    AS elapsed_hours
FROM data_quality_issues dqi
LEFT JOIN support_cases sc ON dqi.issue_id = sc.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE dqi.issue_code = 'ACCUMULATOR_EXCEEDS_OOP_MAX'
ORDER BY dqi.detected_at DESC;""",
        look_for=(
            "If `sla_status` is 'AT_RISK' or 'BREACHED', escalate immediately. "
            "If `root_cause_category` is already populated, someone has begun "
            "the investigation. If no rows return, the scenario may not have "
            "been run yet."
        ),
        key_prefix="oop_breach",
    )

    # Decision Tree
    st.markdown("""
    ---
    ### 🌳 Decision Tree — What's the Root Cause?

    ```
    OOP Breach Detected
    ├─ Accumulator matches claim sum? (Step 3)
    │  ├─ YES → Claims are real
    │  │  ├─ Plan limit correct? (Step 5)
    │  │  │  ├─ YES → Genuine high utilizer
    │  │  │  │  └─ Close as expected, flag for care management
    │  │  │  └─ NO → Plan config error
    │  │  │     └─ Fix limit, recompute accumulator
    │  │  └─ Duplicate claims near breach point? (Step 2 + Step 4)
    │  │     ├─ YES → Void duplicate, recompute
    │  │     └─ NO → Legitimate, review cost-share logic
    │  └─ NO → Accumulator is stale
    │     ├─ OVER → Voided claims not backed out
    │     │  └─ Recompute accumulator from claims
    │     └─ UNDER → Claims processed, accumulator not updated
    │        └─ Check for missed recompute run
    ```

    ---
    ### ✅ Conclusion Checklist

    - [ ] Is the breach caused by real claims or data error?
    - [ ] Is the plan OOP max limit configured correctly?
    - [ ] Are there duplicate claims that need voiding?
    - [ ] Does the accumulator match the independent claim sum?
    - [ ] Is there an active support case with SLA tracking?
    - [ ] What corrective action is needed (recompute, void, plan fix)?
    """)


def playbook_family_rollup():
    """Investigation Playbook: Family Rollup Discrepancy"""

    st.markdown("""
    ## 📋 Investigation Summary

    | Field | Value |
    |-------|-------|
    | **Scenario** | Family Rollup Discrepancy |
    | **Severity** | 🟡 MEDIUM |
    | **SLA** | 24 hours |
    | **Assignment** | ops_recon_queue |
    | **Goal** | Determine why the family-level OOP total doesn't equal the sum of individual member totals, identify the root cause, and correct the discrepancy |

    ---

    ### 🧒 What's Happening?

    **Imagine three siblings each have a piggy bank. Dad also keeps a big jar that
    should always have the same total as all three piggy banks combined. Today,
    the big jar says $50 but the three piggy banks add up to $45. Someone's
    count is wrong — but whose?**

    In healthcare, family-level accumulators must always equal the sum of their
    individual member accumulators. When they don't match, it can cause incorrect
    cost-sharing calculations, leading to members being overcharged or undercharged.

    ---
    """)

    # Step 1
    render_step(
        step_num=1,
        title="Detect All Family Rollup Discrepancies",
        explanation=(
            "🧒 **Check every family's big jar against their individual piggy banks.** "
            "This query compares family-level accumulators to the sum of individual "
            "member accumulators for every family."
        ),
        sql="""WITH individual_totals AS (
    SELECT
        m.family_id,
        SUM(a.individual_oop_accum) AS sum_individual_oop,
        COUNT(DISTINCT m.member_id) AS member_count
    FROM accumulator_snapshots a
    JOIN members m ON a.member_id = m.member_id
    WHERE a.individual_oop_accum > 0
    GROUP BY m.family_id
),
family_snapshots AS (
    SELECT
        m.family_id,
        a.individual_oop_accum AS family_oop_snapshot,
        a.individual_oop_max   AS family_oop_limit
    FROM accumulator_snapshots a
    JOIN members m ON a.member_id = m.member_id
    WHERE a.benefit_year = 'oop_family'
)
SELECT
    it.family_id,
    it.member_count,
    it.sum_individual_oop,
    fs.family_oop_snapshot,
    ROUND(fs.family_oop_snapshot - it.sum_individual_oop, 2) AS discrepancy,
    fs.family_oop_limit,
    CASE
        WHEN ABS(fs.family_oop_snapshot - it.sum_individual_oop) < 0.05
            THEN '✅ MATCHED'
        ELSE '❌ DISCREPANCY'
    END AS rollup_status
FROM individual_totals it
JOIN family_snapshots fs ON it.family_id = fs.family_id
ORDER BY ABS(discrepancy) DESC;""",
        look_for=(
            "Any `❌ DISCREPANCY` row needs investigation. Small discrepancies "
            "under \\$0.05 may be rounding artifacts. Large discrepancies suggest "
            "a member was skipped or has a wrong family_id."
        ),
        key_prefix="family_rollup",
    )

    # Step 2
    render_step(
        step_num=2,
        title="Examine the Family — List Every Member and Their Accumulators",
        explanation=(
            "🧒 **Open each piggy bank and count.** This query lists all members "
            "in a specific family with their individual accumulator values. "
            "Change the family_id to investigate a different family."
        ),
        sql="""SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    m.relationship_code,
    ep.status,
    COALESCE(a.benefit_year, 'MISSING')     AS accum_type,
    COALESCE(a.individual_oop_accum, 0)               AS current_amount,
    COALESCE(a.individual_oop_max, 0)                 AS limit_amount,
    CASE
        WHEN a.accumulator_id IS NULL THEN '🔴 NO ACCUMULATOR'
        ELSE '✅ Present'
    END                                         AS accum_status,
    a.snapshot_ts
FROM members m
LEFT JOIN accumulator_snapshots a
    ON m.member_id = a.member_id
   AND a.individual_oop_accum > 0
WHERE m.family_id = 'FAM-001'
ORDER BY
    CASE m.relationship_code
        WHEN 'SUBSCRIBER' THEN 1
        WHEN 'SPOUSE'     THEN 2
        ELSE 3
    END;""",
        look_for=(
            "Members with `🔴 NO ACCUMULATOR` were skipped during accumulation — "
            "this is likely the cause of the discrepancy. Also check `eligibility_status` — "
            "terminated members might still have stale accumulators. Verify `last_updated_at` "
            "is recent for all members."
        ),
        key_prefix="family_rollup",
    )

    # Step 3
    render_step(
        step_num=3,
        title="Verify Each Member's Accumulator Against Their Claims",
        explanation=(
            "🧒 **For each piggy bank, count the actual coins and compare to the "
            "label.** This query checks whether each individual member's accumulator "
            "matches the sum of their claims."
        ),
        sql="""SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    COALESCE(a.individual_oop_accum, 0)       AS accumulator_amount,
    COALESCE(c.claims_total, 0)         AS claims_total,
    ROUND(COALESCE(a.individual_oop_accum, 0) -
          COALESCE(c.claims_total, 0), 2) AS drift,
    CASE
        WHEN ABS(COALESCE(a.individual_oop_accum, 0) -
                 COALESCE(c.claims_total, 0)) < 0.01
            THEN '✅ IN SYNC'
        ELSE '⚠️ DRIFTED'
    END AS sync_status
FROM members m
LEFT JOIN accumulator_snapshots a
    ON m.member_id = a.member_id
   AND a.individual_oop_accum > 0
LEFT JOIN (
    SELECT member_id,
           SUM(member_responsibility) AS claims_total
    FROM claims
    WHERE claim_status NOT IN ('VOIDED', 'REJECTED')
    GROUP BY member_id
) c ON m.member_id = c.member_id
WHERE m.family_id = 'FAM-001'
ORDER BY ABS(drift) DESC;""",
        look_for=(
            "If all members show `✅ IN SYNC` but the family total is still wrong, "
            "the family-level snapshot itself is the problem (not the individuals). "
            "If any member shows `⚠️ DRIFTED`, their individual accumulator needs "
            "recomputing first, then the family rollup."
        ),
        key_prefix="family_rollup",
    )

    # Step 4
    render_step(
        step_num=4,
        title="Check Processing Run Coverage — Was Everyone Included?",
        explanation=(
            "🧒 **Did every kid get their homework graded, or did the teacher "
            "accidentally skip someone's paper?** This checks whether all family "
            "members were included in the same processing runs."
        ),
        sql="""SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    COUNT(DISTINCT cr.source_file_id) AS runs_involved,
    MAX(pr.completed_at)                 AS latest_run_completed,
    SUM(CASE WHEN pr.run_status = 'SUCCESS'
             THEN 1 ELSE 0 END)          AS successful_runs,
    SUM(CASE WHEN pr.run_status = 'FAILED'
             THEN 1 ELSE 0 END)          AS failed_runs
FROM members m
LEFT JOIN claims cr ON m.member_id = cr.member_id
LEFT JOIN processing_runs pr ON cr.source_file_id = pr.run_id
WHERE m.family_id = 'FAM-001'
GROUP BY m.member_id, m.first_name, m.last_name
ORDER BY m.member_id;""",
        look_for=(
            "Members with zero `runs_involved` may not have any claims (healthy "
            "dependents). Members with `failed_runs` > 0 may have had claims "
            "that weren't accumulated. Compare `latest_run_completed` across "
            "members — if one is days behind, their accumulator is stale."
        ),
        key_prefix="family_rollup",
    )

    # Step 5
    render_step(
        step_num=5,
        title="Check for Related Issues and Cases",
        explanation=(
            "🧒 **Has someone already noticed the piggy bank problem and started "
            "working on it?** This checks for existing data quality issues and "
            "support cases related to family rollup discrepancies."
        ),
        sql="""SELECT
    dqi.issue_id,
    dqi.issue_code,
    dqi.severity,
    dqi.status           AS issue_status,
    dqi.issue_description,
    dqi.detected_at,
    sc.case_number,
    sc.priority,
    sc.status            AS case_status,
    sc.assignment_group,
    sla.target_hours,
    sla.status           AS sla_status,
    sla.is_breached,
    ROUND(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                    AS elapsed_hours
FROM data_quality_issues dqi
LEFT JOIN support_cases sc ON dqi.issue_id = sc.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE dqi.issue_code = 'FAMILY_ROLLUP_DISCREPANCY'
ORDER BY dqi.detected_at DESC;""",
        look_for=(
            "With a 24-hour SLA, there's usually more time than for critical "
            "scenarios, but don't let it slide. Check `elapsed_hours` vs "
            "`target_hours`. If no rows return, the scenario hasn't been run yet."
        ),
        key_prefix="family_rollup",
    )

    # Decision Tree
    st.markdown("""
    ---
    ### 🌳 Decision Tree — What's the Root Cause?

    ```
    Family Rollup Mismatch
    ├─ Missing individual accumulators? (Step 2)
    │  ├─ YES → Member was skipped
    │  │  ├─ Member ACTIVE? → Accumulation bug, recompute
    │  │  └─ Member TERMINATED? → Check if exclusion is correct
    │  └─ NO → All members have accumulators
    │     ├─ Individuals match claims? (Step 3)
    │     │  ├─ All IN SYNC → Family snapshot is stale
    │     │  │  └─ Recompute family rollup only
    │     │  └─ Some DRIFTED → Fix individuals first
    │     │     └─ Then recompute family rollup
    │     └─ Processing coverage gaps? (Step 4)
    │        ├─ YES → Reprocess missed runs
    │        └─ NO → Check family_id linkage for errors
    ```

    ---
    ### ✅ Conclusion Checklist

    - [ ] Which families have discrepancies and how large?
    - [ ] Are any individual member accumulators missing?
    - [ ] Do individual accumulators match their claim sums?
    - [ ] Were all family members included in processing runs?
    - [ ] Is there an active support case with SLA tracking?
    - [ ] What corrective action is needed (recompute individual, recompute family, fix linkage)?
    """)


def playbook_duplicate_eligibility():
    """Investigation Playbook: Duplicate Eligibility Resend"""

    st.markdown("""
    ## 📋 Investigation Summary

    | Field | Value |
    |-------|-------|
    | **Scenario** | Duplicate Eligibility File Resend |
    | **Severity** | 🟡 MEDIUM |
    | **SLA** | 24 hours |
    | **Assignment** | ops_eligibility_queue |
    | **Goal** | Detect duplicate eligibility file submissions, assess impact on member records, prevent double-processing, and coordinate with the vendor |

    ---

    ### 🧒 What's Happening?

    **The mail carrier brought the same package twice! The first one is already
    opened and put away. If we open the second one and put THOSE items away too,
    we'll have duplicates of everything.**

    When a vendor accidentally resends an eligibility file, processing it again
    could create duplicate member records, overlapping eligibility periods, or
    incorrect coverage dates. We need to catch it before damage is done.

    ---
    """)

    # Step 1
    render_step(
        step_num=1,
        title="Check for Duplicate Files in Inventory",
        explanation=(
            "🧒 **Look through the mailbox for two packages with the same label.** "
            "This query finds files from the same vendor with the same type and "
            "similar dates that might be duplicates."
        ),
        sql="""SELECT
    f.file_id,
    f.file_name,
    f.file_type,
    f.processing_status,
    f.expected_date,
    f.received_ts,
    f.row_count,
    f.file_size,
    v.vendor_name,
    c.client_name
FROM inbound_files fi
JOIN vendors v ON f.vendor_id = v.vendor_id
JOIN clients c ON f.client_id = c.client_id
WHERE f.file_type = 'ELIGIBILITY'
  AND f.file_direction = 'INBOUND'
ORDER BY f.expected_date DESC, f.received_ts DESC;""",
        look_for=(
            "Look for files with the same `expected_date`, `record_count`, and "
            "`file_size` from the same vendor — these are likely duplicates. "
            "Different `file_name` but identical content is the classic resend pattern. "
            "Check if both have `file_status` = 'PROCESSED' — that means both were loaded."
        ),
        key_prefix="dup_elig",
    )

    # Step 2
    render_step(
        step_num=2,
        title="Compare Processing Runs for Duplicate Files",
        explanation=(
            "🧒 **Did we open both packages? Let's check the unpacking logs.** "
            "This query shows processing runs for eligibility files to detect "
            "double-processing."
        ),
        sql="""SELECT
    pr.run_id,
    pr.run_type,
    pr.run_status,
    f.file_name,
    f.file_id,
    pr.rows_read,
    pr.rows_passed,
    pr.rows_failed,
    pr.started_at,
    pr.completed_at
FROM processing_runs pr
JOIN inbound_files fi ON pr.file_id = f.file_id
WHERE pr.run_type LIKE '%ELIGIBILITY%'
ORDER BY pr.started_at DESC;""",
        look_for=(
            "If two runs have nearly identical `records_processed` counts and "
            "close `started_at` times, the same file content was processed twice. "
            "One run should have been blocked. Check if both have `run_status` = 'SUCCESS'."
        ),
        key_prefix="dup_elig",
    )

    # Step 3
    render_step(
        step_num=3,
        title="Look for Overlapping Eligibility Periods",
        explanation=(
            "🧒 **Check if any kid is listed in two classrooms at the same time.** "
            "Duplicate processing can create overlapping eligibility periods "
            "for the same member."
        ),
        sql="""SELECT
    e1.member_id,
    m.first_name || ' ' || m.last_name AS member_name,
    e1.eligibility_period_id AS period_1,
    e1.start_date            AS start_1,
    e1.end_date              AS end_1,
    e2.eligibility_period_id AS period_2,
    e2.start_date            AS start_2,
    e2.end_date              AS end_2
FROM eligibility_periods e1
JOIN eligibility_periods e2
    ON e1.member_id = e2.member_id
   AND e1.eligibility_period_id < e2.eligibility_period_id
   AND e1.start_date <= e2.end_date
   AND e2.start_date <= e1.end_date
JOIN members m ON e1.member_id = m.member_id
ORDER BY e1.member_id;""",
        look_for=(
            "Any rows returned indicate overlapping eligibility — a member has "
            "two active coverage periods that overlap in time. This is a direct "
            "consequence of processing the same file twice. The newer period "
            "should be removed or the older one should be end-dated."
        ),
        key_prefix="dup_elig",
    )

    # Step 4
    render_step(
        step_num=4,
        title="Check Data Quality Issues and Cases",
        explanation=(
            "🧒 **Has someone already noticed the double delivery?** "
            "This query checks if the system detected the duplicate and "
            "whether a support case was created to handle it."
        ),
        sql="""SELECT
    dqi.issue_id,
    dqi.issue_code,
    dqi.severity,
    dqi.status              AS issue_status,
    dqi.issue_description,
    dqi.detected_at,
    sc.case_number,
    sc.priority,
    sc.status               AS case_status,
    sc.assignment_group,
    sc.assigned_to,
    sla.target_hours,
    sla.status              AS sla_status,
    sla.is_breached,
    ROUND(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                       AS elapsed_hours
FROM data_quality_issues dqi
LEFT JOIN support_cases sc ON dqi.issue_id = sc.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE dqi.issue_code = 'DUPLICATE_ELIGIBILITY_RESEND'
ORDER BY dqi.detected_at DESC;""",
        look_for=(
            "A 24-hour SLA gives time for investigation, but don't wait. "
            "If `case_status` is 'OPEN' and no one is `assigned_to`, pick it up. "
            "If no rows return, the duplicate may not have been caught yet — "
            "the detection logic may need improvement."
        ),
        key_prefix="dup_elig",
    )

    # Step 5
    render_step(
        step_num=5,
        title="Assess Impact — How Many Members Were Double-Loaded?",
        explanation=(
            "🧒 **How many kids got listed in two classrooms?** "
            "This query counts members who appear in both processing runs "
            "to quantify the blast radius of the duplicate."
        ),
        sql="""SELECT
    m.client_id,
    c.client_name,
    COUNT(DISTINCT m.member_id)     AS total_members,
    COUNT(DISTINCT ep.eligibility_period_id) AS total_elig_periods,
    ROUND(
        COUNT(DISTINCT ep.eligibility_period_id) * 1.0 /
        NULLIF(COUNT(DISTINCT m.member_id), 0), 2
    )                               AS periods_per_member
FROM members m
JOIN clients c ON m.client_id = c.client_id
LEFT JOIN eligibility_periods ep ON m.member_id = ep.member_id
WHERE ep.status = 'ACTIVE'
GROUP BY m.client_id, c.client_name
ORDER BY periods_per_member DESC;""",
        look_for=(
            "If `periods_per_member` is significantly above 1.0, members have "
            "multiple eligibility periods — a sign of duplicate loading. "
            "Healthy data should show exactly 1.0 for most members (one active "
            "period per benefit year)."
        ),
        key_prefix="dup_elig",
    )

    # Step 6
    render_step(
        step_num=6,
        title="Get Vendor Contact for Escalation",
        explanation=(
            "🧒 **Time to call the mail carrier and ask why they delivered twice!** "
            "This query pulls the primary contact for the vendor so you can "
            "coordinate on the resolution."
        ),
        sql="""SELECT
    v.vendor_id,
    v.vendor_name,
    v.vendor_type,
    vc.contact_name,
    vc.contact_email,
    vc.contact_phone,
    vc.contact_type
FROM vendors v
LEFT JOIN vendor_contacts vc
    ON v.vendor_id = vc.vendor_id
   AND vc.is_primary = 1
WHERE v.status = 'ACTIVE'
ORDER BY v.vendor_name;""",
        look_for=(
            "Use the primary operations contact for initial outreach. If no "
            "primary contact exists (NULL), check for any contact with "
            "`contact_type` = 'ESCALATION'. Document the outreach in the "
            "support case description."
        ),
        key_prefix="dup_elig",
    )

    # Decision Tree
    st.markdown("""
    ---
    ### 🌳 Decision Tree — What Do I Do Next?

    ```
    Duplicate File Detected
    ├─ Was the duplicate processed?
    │  ├─ YES — Both files loaded
    │  │  ├─ Overlapping eligibility periods created? (Step 3)
    │  │  │  ├─ YES → Remove duplicate periods
    │  │  │  │  └─ Verify member records after cleanup
    │  │  │  └─ NO → Idempotent load (no damage)
    │  │  │     └─ Mark duplicate file as DUPLICATE, close case
    │  │  └─ Were accumulators affected?
    │  │     ├─ YES → Recompute accumulators after cleanup
    │  │     └─ NO → Eligibility-only impact
    │  └─ NO — Duplicate was caught before processing
    │     ├─ Mark file as DUPLICATE in file_inventory
    │     └─ Notify vendor to prevent future resends
    └─ Is this a recurring vendor issue?
       ├─ YES → Escalate to vendor management
       │  └─ Request root cause from vendor
       └─ NO → Log as one-time incident
          └─ Close case after cleanup
    ```

    ---
    ### ✅ Conclusion Checklist

    - [ ] Were both copies of the file processed?
    - [ ] Are there overlapping eligibility periods?
    - [ ] How many members were affected?
    - [ ] Has the duplicate file been marked as DUPLICATE?
    - [ ] Has the vendor been contacted?
    - [ ] Is this a recurring pattern with this vendor?
    """)


def playbook_claim_ineligible():
    """Investigation Playbook: Claim for Ineligible Member"""

    st.markdown("""
    ## 📋 Investigation Summary

    | Field | Value |
    |-------|-------|
    | **Scenario** | Claim Filed for Ineligible Member |
    | **Severity** | 🟠 HIGH |
    | **SLA** | 8 hours |
    | **Assignment** | ops_claims_queue |
    | **Goal** | Determine why a claim was filed for a member without active eligibility, assess whether the member should be eligible, and take corrective action |

    ---

    ### 🧒 What's Happening?

    **A kid showed up to a birthday party, but their name isn't on the guest list!
    Maybe the invitation got lost in the mail. Maybe they moved away. Or maybe
    the list is just wrong. We need to figure out which one it is before we can
    decide if they can stay.**

    When a claim arrives for a member who isn't listed as eligible on the service
    date, it's rejected. But the rejection might be wrong — the member's eligibility
    file might not have been processed yet, or their coverage was terminated
    incorrectly. We need to investigate before the claim can be adjudicated.

    ---
    """)

    # Step 1
    render_step(
        step_num=1,
        title="Find Claims for Ineligible Members",
        explanation=(
            "🧒 **Check the guest list — who showed up without an invitation?** "
            "This query finds data quality issues flagged as claims for "
            "ineligible members."
        ),
        sql="""SELECT
    dqi.issue_id,
    dqi.issue_code,
    dqi.severity,
    dqi.status          AS issue_status,
    dqi.member_id,
    dqi.claim_record_id,
    dqi.issue_description,
    dqi.detected_at,
    m.first_name || ' ' || m.last_name AS member_name,
    ep.status,
    m.family_id
FROM data_quality_issues dqi
LEFT JOIN members m ON dqi.member_id = m.member_id
WHERE dqi.issue_code = 'CLAIM_INELIGIBLE_MEMBER'
ORDER BY dqi.detected_at DESC;""",
        look_for=(
            "Check the `eligibility_status` — if it's 'TERMINATED', the member "
            "lost coverage. If it's 'ACTIVE', the issue might have been created "
            "before the eligibility file was processed. A NULL `member_name` means "
            "the member doesn't even exist in our system."
        ),
        key_prefix="inelig_claim",
    )

    # Step 2
    render_step(
        step_num=2,
        title="Pull the Rejected Claim Details",
        explanation=(
            "🧒 **Look at the birthday present they brought — what is it, "
            "and when did they bring it?** This query shows the full details "
            "of claims that were rejected due to ineligibility."
        ),
        sql="""SELECT
    cr.claim_record_id,
    cr.member_id,
    cr.claim_number,
    cr.service_date,
    cr.provider_name,
    cr.procedure_code,
    cr.diagnosis_code,
    cr.billed_amount,
    cr.allowed_amount,
    cr.paid_amount,
    cr.member_responsibility,
    cr.claim_status,
    cr.adjudication_status,
    cr.source_file_id,
    cr.created_at       AS loaded_at
FROM claims cr
WHERE cr.claim_status = 'REJECTED'
   OR cr.adjudication_status = 'REJECTED'
ORDER BY cr.service_date DESC;""",
        look_for=(
            "Note the `service_date` — this is the date the member received care. "
            "We need to check if they had active eligibility on THAT specific date, "
            "not today's date. Also note the `billed_amount` — higher amounts "
            "represent greater financial exposure."
        ),
        key_prefix="inelig_claim",
    )

    # Step 3
    render_step(
        step_num=3,
        title="Check the Member's Eligibility History",
        explanation=(
            "🧒 **Look at the guest list history — were they EVER invited? "
            "Maybe their invitation expired yesterday.** This query shows "
            "all eligibility periods for affected members to find gaps."
        ),
        sql="""SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name AS member_name,
    ep.status               AS current_status,
    ep.eligibility_period_id,
    ep.plan_id,
    bp.plan_name,
    ep.start_date,
    ep.end_date,
    ep.status                          AS period_status,
    ep.created_at                      AS period_created
FROM members m
LEFT JOIN eligibility_periods ep ON m.member_id = ep.member_id
LEFT JOIN benefit_plans bp ON ep.plan_id = bp.plan_id
WHERE ep.status != 'ACTIVE'
   OR ep.status != 'ACTIVE'
ORDER BY m.member_id, ep.start_date DESC;""",
        look_for=(
            "If the member has no eligibility periods at all, they were never enrolled — "
            "possible data load failure. If they have a period that ended BEFORE the "
            "claim's service date, their coverage legitimately lapsed. If the period "
            "ended AFTER the service date, the rejection was incorrect."
        ),
        key_prefix="inelig_claim",
    )

    # Step 4
    render_step(
        step_num=4,
        title="Check if the Eligibility File Was Processed Recently",
        explanation=(
            "🧒 **Maybe the updated guest list is sitting in the mailbox "
            "and no one opened it yet!** This query checks whether recent "
            "eligibility files have been processed — a missing or failed "
            "file load could explain why the member appears ineligible."
        ),
        sql="""SELECT
    f.file_id,
    f.file_name,
    f.file_type,
    f.processing_status,
    f.expected_date,
    f.received_ts,
    f.row_count,
    pr.run_status,
    pr.rows_read,
    pr.rows_failed,
    pr.completed_at,
    c.client_name,
    v.vendor_name
FROM inbound_files fi
LEFT JOIN processing_runs pr ON f.processing_run_id = pr.run_id
LEFT JOIN clients c ON f.client_id = c.client_id
LEFT JOIN vendors v ON f.vendor_id = v.vendor_id
WHERE f.file_type = 'ELIGIBILITY'
ORDER BY f.expected_date DESC
LIMIT 10;""",
        look_for=(
            "If the most recent eligibility file has `file_status` = 'ERROR' or "
            "'MISSING', the member's eligibility may not be current. If "
            "`records_failed` > 0, specific members may have failed to load. "
            "The claim rejection might be a false positive caused by stale data."
        ),
        key_prefix="inelig_claim",
    )

    # Step 5
    render_step(
        step_num=5,
        title="Check for Related Support Cases and SLA",
        explanation=(
            "🧒 **Is someone already working on getting this kid's invitation "
            "sorted out?** This checks for existing support cases related to "
            "the ineligible member claim."
        ),
        sql="""SELECT
    dqi.issue_id,
    dqi.issue_code,
    dqi.severity,
    dqi.status          AS issue_status,
    dqi.detected_at,
    sc.case_number,
    sc.priority,
    sc.status           AS case_status,
    sc.assignment_group,
    sc.assigned_to,
    sc.root_cause_category,
    sla.target_hours,
    sla.target_due_at,
    sla.status          AS sla_status,
    sla.is_at_risk,
    sla.is_breached,
    ROUND(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                   AS elapsed_hours
FROM data_quality_issues dqi
LEFT JOIN support_cases sc ON dqi.issue_id = sc.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE dqi.issue_code = 'CLAIM_INELIGIBLE_MEMBER'
ORDER BY dqi.detected_at DESC;""",
        look_for=(
            "With an 8-hour SLA, this is more urgent than the 24-hour scenarios. "
            "If `sla_status` is 'AT_RISK', you need to either resolve it quickly "
            "or escalate. Check `root_cause_category` to see if someone has "
            "already identified whether this is a data gap or legitimate termination."
        ),
        key_prefix="inelig_claim",
    )

    # Step 6
    render_step(
        step_num=6,
        title="Cross-Reference: Other Family Members' Eligibility",
        explanation=(
            "🧒 **If one kid in the family isn't on the list, check if their "
            "siblings are — maybe only one invitation got lost.** This query "
            "checks the eligibility status of all members in the same family."
        ),
        sql="""SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name AS member_name,
    m.relationship_code,
    ep.status,
    m.family_id,
    COALESCE(ep.status, 'NO PERIOD')   AS period_status,
    ep.start_date,
    ep.end_date,
    bp.plan_name
FROM members m
LEFT JOIN eligibility_periods ep
    ON m.member_id = ep.member_id
   AND ep.status = 'ACTIVE'
LEFT JOIN benefit_plans bp ON ep.plan_id = bp.plan_id
ORDER BY m.family_id,
    CASE m.relationship_code
        WHEN 'SUBSCRIBER' THEN 1
        WHEN 'SPOUSE'     THEN 2
        ELSE 3
    END;""",
        look_for=(
            "If the subscriber is active but a dependent is not, the dependent's "
            "eligibility file record may have been missed. If the whole family is "
            "inactive, the group may have been terminated legitimately. If only "
            "one member is inactive, it's likely a data issue."
        ),
        key_prefix="inelig_claim",
    )

    # Decision Tree
    st.markdown("""
    ---
    ### 🌳 Decision Tree — What's the Root Cause?

    ```
    Claim Rejected — Member Ineligible
    ├─ Member exists in system?
    │  ├─ NO → Member never loaded
    │  │  └─ Check eligibility file for this member
    │  │     ├─ In file → Load failure, reprocess
    │  │     └─ Not in file → Vendor didn't include them
    │  │        └─ Contact vendor
    │  └─ YES → Member exists but not eligible
    │     ├─ Eligibility period exists?
    │     │  ├─ NO → Period never created
    │     │  │  └─ Eligibility file processing issue
    │     │  └─ YES → Period exists
    │     │     ├─ Period covers service_date?
    │     │     │  ├─ YES → Rejection was incorrect!
    │     │     │  │  └─ Reprocess claim, investigate detection logic
    │     │     │  └─ NO → Coverage gap on service date
    │     │     │     ├─ Terminated before service → Legitimate rejection
    │     │     │     └─ Gap between periods → Possible retroactive fix
    │     │     └─ Period status = 'TERMINATED'?
    │     │        ├─ Correct termination → Claim correctly rejected
    │     │        └─ Incorrect termination → Reinstate, reprocess claim
    │     └─ Other family members eligible? (Step 6)
    │        ├─ YES → Isolated member issue
    │        └─ NO → Group-level termination
    ```

    ---
    ### ✅ Conclusion Checklist

    - [ ] Does the member exist in the system?
    - [ ] Did the member have active coverage on the claim's service date?
    - [ ] Was the most recent eligibility file processed successfully?
    - [ ] Is this an isolated member issue or a family/group issue?
    - [ ] Is there an active support case with SLA tracking?
    - [ ] What corrective action is needed (reinstate, reprocess, vendor contact)?
    """)


# ═══════════════════════════════════════════════════════════════
# PLAYBOOK REGISTRY
# ═══════════════════════════════════════════════════════════════

PLAYBOOKS = {
    "📁 Missing Inbound File": {
        "func": playbook_missing_file,
        "severity": "🔴 CRITICAL",
        "sla": "4 hours",
        "steps": 6,
        "desc": (
            "Investigate when an expected vendor file doesn't arrive. "
            "Trace the schedule, check file inventory, assess downstream "
            "impact on members, and coordinate vendor resolution."
        ),
    },
    "📊 Accumulator OOP Max Breach": {
        "func": playbook_oop_breach,
        "severity": "🟠 HIGH",
        "sla": "8 hours",
        "steps": 6,
        "desc": (
            "Investigate when a member's out-of-pocket spending exceeds "
            "their plan maximum. Trace claims, detect drift, find the "
            "breach-point claim, and verify plan configuration."
        ),
    },
    "👨‍👩‍👧‍👦 Family Rollup Discrepancy": {
        "func": playbook_family_rollup,
        "severity": "🟡 MEDIUM",
        "sla": "24 hours",
        "steps": 5,
        "desc": (
            "Investigate when a family-level accumulator doesn't match "
            "the sum of individual member accumulators. Check member "
            "coverage, verify claims, and identify missing data."
        ),
    },
    "👯 Duplicate Eligibility Resend": {
        "func": playbook_duplicate_eligibility,
        "severity": "🟡 MEDIUM",
        "sla": "24 hours",
        "steps": 6,
        "desc": (
            "Investigate when a vendor sends the same eligibility file "
            "twice. Detect duplicates, check for overlapping periods, "
            "assess impact, and coordinate with the vendor."
        ),
    },
    "🚫 Claim for Ineligible Member": {
        "func": playbook_claim_ineligible,
        "severity": "🟠 HIGH",
        "sla": "8 hours",
        "steps": 6,
        "desc": (
            "Investigate when a claim is rejected because the member "
            "lacks active eligibility. Determine if the rejection is "
            "correct or caused by a data processing gap."
        ),
    },
}


# ═══════════════════════════════════════════════════════════════
# STREAMLIT PAGE LAYOUT
# ═══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Investigation Playbooks",
    page_icon="🔬",
    layout="wide",
)

st.title("🔬 Investigation Playbooks")
st.caption(
    "Step-by-step interactive investigations for healthcare data operations. "
    "Each playbook walks through a real scenario with executable SQL, "
    "decision trees, and guided conclusions."
)

# ── Tabs ───────────────────────────────────────────────────────
tab_playbooks, tab_overview, tab_how = st.tabs([
    "🔬 Run an Investigation",
    "📋 Playbook Overview",
    "❓ How It Works",
])


# ── Tab 1: Run an Investigation ───────────────────────────────
with tab_playbooks:
    st.subheader("🔬 Select an Investigation Playbook")

    selected_playbook = st.selectbox(
        "Choose a scenario to investigate:",
        list(PLAYBOOKS.keys()),
        key="playbook_selector",
    )

    pb = PLAYBOOKS[selected_playbook]

    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.metric("Severity", pb["severity"])
    with col_info2:
        st.metric("SLA Target", pb["sla"])
    with col_info3:
        st.metric("Investigation Steps", pb["steps"])

    st.markdown(f"**Description:** {pb['desc']}")
    st.divider()

    # Run the selected playbook function
    pb["func"]()


# ── Tab 2: Playbook Overview ──────────────────────────────────
with tab_overview:
    st.subheader("📋 All Investigation Playbooks")
    st.info(
        "🧒 **Think of these as recipe cards for solving mysteries.** "
        "Each playbook tells you exactly what steps to follow, what SQL "
        "queries to run, and what to look for in the results — like a "
        "detective's handbook!"
    )

    overview_data = []
    for name, info in PLAYBOOKS.items():
        overview_data.append({
            "Playbook": name,
            "Severity": info["severity"],
            "SLA": info["sla"],
            "Steps": info["steps"],
            "Description": info["desc"],
        })

    overview_df = pd.DataFrame(overview_data)
    st.dataframe(
        overview_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Playbook": st.column_config.TextColumn("Playbook", width="medium"),
            "Severity": st.column_config.TextColumn("Severity", width="small"),
            "SLA": st.column_config.TextColumn("SLA", width="small"),
            "Steps": st.column_config.NumberColumn("Steps", width="small"),
            "Description": st.column_config.TextColumn("Description", width="large"),
        },
    )

    st.divider()
    st.markdown("""
    ### 🗺️ Which Playbook Should I Use?

    | Situation | Playbook |
    |-----------|----------|
    | Expected file didn't arrive from vendor | 📁 Missing Inbound File |
    | Member's OOP spending exceeds plan maximum | 📊 Accumulator OOP Max Breach |
    | Family total doesn't match sum of individuals | 👨‍👩‍👧‍👦 Family Rollup Discrepancy |
    | Same eligibility file received twice | 👯 Duplicate Eligibility Resend |
    | Claim rejected because member isn't eligible | 🚫 Claim for Ineligible Member |

    ### 📊 SQL Concepts Covered Across All Playbooks

    | Concept | Playbooks Using It |
    |---------|-------------------|
    | JOIN / LEFT JOIN | All 5 |
    | GROUP BY + Aggregation | All 5 |
    | CASE Expressions | All 5 |
    | JULIANDAY Date Math | All 5 |
    | COALESCE / NULLIF | 4 of 5 |
    | CTEs (WITH clause) | 3 of 5 |
    | Window Functions (SUM OVER) | 2 of 5 |
    | Self-JOIN (overlap detection) | 1 of 5 |
    | CROSS JOIN | 1 of 5 |
    | Anti-Join (LEFT JOIN + IS NULL) | 1 of 5 |
    """)


# ── Tab 3: How It Works ──────────────────────────────────────
with tab_how:
    st.subheader("❓ How It Works")

    st.markdown("""
    ### 🧒 The Simple Version

    **Imagine you're a detective, and someone hands you a mystery folder.**

    Inside the folder is a note that says what happened:
    *"A file is missing"* or *"A member spent too much money."*

    You don't know WHY yet. That's what the investigation is for.

    Each playbook is like a **step-by-step detective guide:**

    1. **Step 1:** Look at the scene of the crime (what data do we have?)
    2. **Step 2:** Gather evidence (pull related records)
    3. **Step 3:** Compare stories (cross-check different data sources)
    4. **Step 4:** Check the timeline (when did things happen?)
    5. **Step 5:** Check if help is already on the way (support cases & SLAs)
    6. **Step 6:** Assess the blast radius (who else is affected?)

    At the end, there's a **Decision Tree** that helps you decide what to do:
    - Is this a real problem or a false alarm?
    - Who caused it — us, the vendor, or the plan configuration?
    - What's the fix?

    ---

    ### 🏗️ How This Page Is Built

    Each playbook is a Python function containing:
    - **Markdown explanations** in plain English with kid-friendly analogies
    - **Live SQL queries** that run against the same SQLite database as all dashboards
    - **"What to Look For" guidance** after each query result
    - **Decision trees** showing the diagnostic logic path
    - **Conclusion checklists** summarizing what you should know after the investigation

    ---

    ### 🔗 Connection to Other Pages

    | Playbook Step | Related Page |
    |---------------|-------------|
    | Check file schedules and inventory | File Monitoring |
    | Check support cases and SLAs | Issue Triage |
    | Check accumulators and claims | Accumulator Reconciliation |
    | Run scenarios to create test data | Scenario Control Center |
    | Write your own investigative queries | SQL Query Workbench |

    ---

    ### 💡 Tips for Getting the Most Out of These Playbooks

    1. **Run a scenario first** — playbooks are most valuable when there's
       incident data to investigate. Go to the Scenario Control Center and
       run the matching scenario.
    2. **Run every step** — don't skip steps. Each one builds on the previous.
    3. **Read the "What to Look For"** — the SQL results alone don't tell the
       story. The guidance tells you what patterns indicate which root causes.
    4. **Follow the Decision Tree** — after running all steps, the decision
       tree synthesizes your findings into an action plan.
    5. **Modify the queries** — change member_ids, family_ids, and date ranges
       to investigate different members. Use the SQL Sandbox for free-form exploration.
    6. **Use the Conclusion Checklist** — make sure you can answer every question
       before closing the investigation.

    ---

    ### 📓 Relationship to Jupyter Notebooks

    These playbooks contain exactly what a Jupyter notebook would contain:
    - Markdown cells → the explanations and decision trees
    - Code cells → the SQL queries
    - Output cells → the query results

    **The advantage of this format:**
    - Runs directly in the browser — no Python kernel setup needed
    - Queries execute against live data that changes as you run scenarios
    - Interactive — click to run each step, download results
    - Integrated with the rest of the application
    """)

    st.caption(f"Page last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")