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
    """Return a SQLite connection to the project database."""
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


def is_safe_query(sql_text: str) -> bool:
    """Return True if the query contains no destructive SQL statements."""
    cleaned = re.sub(r"--.*$", "", sql_text, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    return UNSAFE_PATTERNS.search(cleaned) is None


def run_query(sql_text: str) -> tuple:
    """Execute sql_text and return (DataFrame, elapsed_ms, error_str)."""
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


QUERY_LIBRARY = [
    # ── 🏗️ Database Exploration ─────────────────────────────
    {
        "cat": "🏗️ Database Exploration",
        "title": "Count Every Record in Every Table",
        "icon": "📊",
        "desc": (
            "🧒 **Imagine you have 16 different toy boxes. You want to know "
            "how many toys are in each one — without opening them all by hand.** "
            "This query peeks inside every table in our database and counts the rows. "
            "It's the first thing you run when you sit down at a new system."
        ),
        "sql": """SELECT 'clients'                  AS table_name, COUNT(*) AS row_count FROM clients
UNION ALL SELECT 'vendors',                    COUNT(*) FROM vendors
UNION ALL SELECT 'client_vendor_relationships', COUNT(*) FROM client_vendor_relationships
UNION ALL SELECT 'vendor_contacts',            COUNT(*) FROM vendor_contacts
UNION ALL SELECT 'benefit_plans',              COUNT(*) FROM benefit_plans
UNION ALL SELECT 'members',                    COUNT(*) FROM members
UNION ALL SELECT 'eligibility_periods',        COUNT(*) FROM eligibility_periods
UNION ALL SELECT 'file_schedules',             COUNT(*) FROM file_schedules
UNION ALL SELECT 'inbound_files',             COUNT(*) FROM inbound_files
UNION ALL SELECT 'processing_runs',            COUNT(*) FROM processing_runs
UNION ALL SELECT 'claims',              COUNT(*) FROM claims
UNION ALL SELECT 'accumulator_snapshots',      COUNT(*) FROM accumulator_snapshots
UNION ALL SELECT 'data_quality_issues',        COUNT(*) FROM data_quality_issues
UNION ALL SELECT 'support_cases',              COUNT(*) FROM support_cases
UNION ALL SELECT 'sla_tracking',               COUNT(*) FROM sla_tracking
ORDER BY row_count DESC;""",
        "look_for": (
            "Tables with zero rows haven't been populated yet — run scenarios "
            "to generate data. Tables like `clients`, `members`, and `benefit_plans` "
            "should have rows from seed data."
        ),
        "concepts": ["UNION ALL", "COUNT(*)", "ORDER BY", "Aliases"],
    },
    {
        "cat": "🏗️ Database Exploration",
        "title": "Explore Any Table's Columns and Types",
        "icon": "🔎",
        "desc": (
            "🧒 **Like reading the label on a toy box to know what kind of toys "
            "go inside.** This query shows you every column in the `members` table, "
            "what type of data it holds, and whether it can be empty. Change the "
            "table name to explore any table!"
        ),
        "sql": """PRAGMA table_info(members);""",
        "look_for": (
            "The `type` column shows TEXT, REAL (decimal numbers), or INTEGER. "
            "The `notnull` column: 1 means the field is required, 0 means optional. "
            "The `pk` column: 1 means it's the primary key (unique identifier)."
        ),
        "concepts": ["PRAGMA", "Schema inspection", "Data types", "Constraints"],
    },
    {
        "cat": "🏗️ Database Exploration",
        "title": "Database Health Dashboard",
        "icon": "🏥",
        "desc": (
            "🧒 **Like a doctor checking your temperature, heart rate, and blood "
            "pressure all at once.** This query checks the health of our entire "
            "system in one shot: how many members, files, cases, and SLA records "
            "exist, and how many are in problem states."
        ),
        "sql": """SELECT
    (SELECT COUNT(*) FROM eligibility_periods
     WHERE status = 'ACTIVE')                      AS active_members,
    (SELECT COUNT(*) FROM inbound_files)           AS total_files,
    (SELECT COUNT(*) FROM inbound_files
     WHERE processing_status IN ('FAILED','REJECTED'))
                                                   AS problem_files,
    (SELECT COUNT(*) FROM support_cases
     WHERE status IN ('OPEN','IN_PROGRESS'))       AS open_cases,
    (SELECT COUNT(*) FROM support_cases
     WHERE priority = 'CRITICAL'
       AND status IN ('OPEN','IN_PROGRESS'))       AS critical_cases,
    (SELECT COUNT(*) FROM sla_tracking
     WHERE is_breached = 1)                        AS breached_slas,
    (SELECT COUNT(*) FROM data_quality_issues
     WHERE status = 'OPEN')                        AS open_issues;""",
        "look_for": (
            "This is your morning check. If `critical_cases` > 0 or "
            "`breached_slas` > 0, something needs immediate attention. "
            "If `problem_files` > 0, check the File Monitoring page."
        ),
        "concepts": ["Scalar subqueries", "SELECT without FROM", "Aliases"],
    },

    # ── 👥 Members & Eligibility ───────────────────────────
    {
        "cat": "👥 Members & Eligibility",
        "title": "Active Member Roster with Plan Details",
        "icon": "👨‍👩‍👧‍👦",
        "desc": (
            "🧒 **Like making a class list that also shows which reading group "
            "each kid belongs to.** This query lists every active member and joins "
            "in their benefit plan information so you can see who's on which plan."
        ),
        "sql": """SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name  AS full_name,
    m.date_of_birth,
    m.gender,
    m.family_id,
    m.relationship_code,
    bp.plan_name,
    bp.plan_type,
    ep.status
FROM members m
JOIN benefit_plans bp
    ON m.plan_id = bp.plan_id
WHERE ep.status = 'ACTIVE'
ORDER BY m.family_id, m.relationship_code;""",
        "look_for": (
            "Members grouped by `family_id` — subscribers appear first, then "
            "spouses and dependents. Every active member should have a matching "
            "benefit plan. A NULL `plan_name` would indicate a data problem."
        ),
        "concepts": ["INNER JOIN", "WHERE filtering", "String concatenation (||)", "ORDER BY multiple columns"],
    },
    {
        "cat": "👥 Members & Eligibility",
        "title": "Complete Family Composition View",
        "icon": "🏠",
        "desc": (
            "🧒 **Like drawing a family tree for every family in the system.** "
            "This query groups members by family and shows their roles "
            "(subscriber, spouse, dependent), plan, and OOP accumulation."
        ),
        "sql": """SELECT
    m.family_id,
    m.member_id,
    m.first_name || ' ' || m.last_name  AS full_name,
    m.relationship_code,
    m.date_of_birth,
    bp.plan_name,
    COALESCE(a.individual_oop_accum, 0)        AS oop_accumulated,
    COALESCE(a.individual_oop_max, 0)          AS oop_limit,
    CASE
        WHEN a.individual_oop_max > 0
        THEN ROUND(a.individual_oop_accum * 100.0 / a.individual_oop_max, 1)
        ELSE 0
    END                                  AS oop_pct_used
FROM members m
JOIN benefit_plans bp ON m.plan_id = bp.plan_id
LEFT JOIN accumulator_snapshots a
    ON m.member_id = a.member_id
   AND a.individual_oop_accum > 0
WHERE ep.status = 'ACTIVE'
ORDER BY m.family_id,
    CASE m.relationship_code
        WHEN 'SUBSCRIBER' THEN 1
        WHEN 'SPOUSE'     THEN 2
        ELSE 3
    END;""",
        "look_for": (
            "Each family should have exactly one SUBSCRIBER. The `oop_pct_used` "
            "shows how close each member is to their OOP max — anything over 90% "
            "is worth watching. Members with 0 accumulated may simply be healthy."
        ),
        "concepts": ["LEFT JOIN", "COALESCE", "CASE expression", "ORDER BY with CASE", "Multiple JOINs"],
    },
    {
        "cat": "👥 Members & Eligibility",
        "title": "Eligibility Gap Detection",
        "icon": "🕳️",
        "desc": (
            "🧒 **Imagine your school attendance card — if you were absent for "
            "a week, there's a gap. We're looking for gaps in health coverage.** "
            "This query uses a window function to compare each eligibility period's "
            "start date to the previous period's end date."
        ),
        "sql": """WITH ordered_periods AS (
    SELECT
        ep.member_id,
        m.first_name || ' ' || m.last_name AS full_name,
        ep.plan_id,
        ep.start_date,
        ep.end_date,
        ep.status,
        LAG(ep.end_date) OVER (
            PARTITION BY ep.member_id
            ORDER BY ep.start_date
        ) AS prev_end_date
    FROM eligibility_periods ep
    JOIN members m ON ep.member_id = m.member_id
)
SELECT
    member_id,
    full_name,
    plan_id,
    prev_end_date,
    start_date,
    JULIANDAY(start_date) - JULIANDAY(prev_end_date) AS gap_days,
    CASE
        WHEN JULIANDAY(start_date) - JULIANDAY(prev_end_date) > 1
        THEN '⚠️ GAP DETECTED'
        ELSE '✅ Continuous'
    END AS coverage_status
FROM ordered_periods
WHERE prev_end_date IS NOT NULL
ORDER BY gap_days DESC;""",
        "look_for": (
            "Any row with `gap_days` > 1 means the member had a break in coverage. "
            "Claims during a gap period would be rejected. If you see large gaps, "
            "investigate whether the eligibility file had missing records."
        ),
        "concepts": ["CTE (WITH clause)", "LAG window function", "PARTITION BY", "JULIANDAY date math", "CASE expression"],
    },
    {
        "cat": "👥 Members & Eligibility",
        "title": "Members Without Active Eligibility",
        "icon": "❓",
        "desc": (
            "🧒 **Like finding kids on the class roster who haven't been assigned "
            "to a classroom yet.** This query finds members who exist in our system "
            "but don't have any active eligibility period — they might be terminated "
            "or have a data gap."
        ),
        "sql": """SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name  AS full_name,
    m.family_id,
    ep.status,
    m.plan_id,
    m.created_at
FROM members m
LEFT JOIN eligibility_periods ep
    ON m.member_id = ep.member_id
   AND ep.status = 'ACTIVE'
WHERE ep.eligibility_period_id IS NULL
ORDER BY m.created_at DESC;""",
        "look_for": (
            "Members appearing here have no active eligibility period. Check: "
            "are they legitimately terminated? Or was their eligibility file "
            "not processed? Cross-reference with the File Monitoring page."
        ),
        "concepts": ["LEFT JOIN", "IS NULL pattern (anti-join)", "Filtering on joined table"],
    },

    # ── 💰 Claims Analysis ─────────────────────────────────
    {
        "cat": "💰 Claims Analysis",
        "title": "Member Claims History with Running Total",
        "icon": "📋",
        "desc": (
            "🧒 **Like watching a piggy bank fill up coin by coin — each claim "
            "adds to what the member has spent.** This query shows every claim "
            "for a member in date order with a running total of their out-of-pocket "
            "spending."
        ),
        "sql": """SELECT
    cr.claim_record_id,
    cr.claim_number,
    cr.service_date,
    cr.provider_name,
    cr.procedure_code,
    cr.billed_amount,
    cr.allowed_amount,
    cr.paid_amount,
    cr.member_responsibility,
    cr.claim_status,
    SUM(cr.member_responsibility) OVER (
        ORDER BY cr.service_date, cr.created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_oop_total
FROM claims cr
WHERE cr.member_id = 'MBR-001'
  AND cr.claim_status NOT IN ('VOIDED', 'REJECTED')
ORDER BY cr.service_date, cr.created_at;""",
        "look_for": (
            "The `running_oop_total` column grows with each claim. Compare the "
            "final value to the member's OOP max to see how close they are. "
            "Large jumps in `member_responsibility` indicate expensive services."
        ),
        "concepts": ["Window function SUM OVER", "ROWS BETWEEN (frame clause)", "Running total pattern"],
    },
    {
        "cat": "💰 Claims Analysis",
        "title": "Top 10 Highest-Cost Claims",
        "icon": "💎",
        "desc": (
            "🧒 **Like finding the 10 most expensive items at a store.** "
            "This query ranks all claims by billed amount and shows the top 10, "
            "including which member and provider were involved."
        ),
        "sql": """SELECT
    cr.claim_record_id,
    cr.claim_number,
    m.first_name || ' ' || m.last_name  AS member_name,
    cr.service_date,
    cr.provider_name,
    cr.diagnosis_code,
    cr.procedure_code,
    cr.billed_amount,
    cr.allowed_amount,
    cr.paid_amount,
    cr.member_responsibility,
    cr.claim_status,
    RANK() OVER (ORDER BY cr.billed_amount DESC) AS cost_rank
FROM claims cr
JOIN members m ON cr.member_id = m.member_id
WHERE cr.claim_status NOT IN ('VOIDED', 'REJECTED')
ORDER BY cr.billed_amount DESC
LIMIT 10;""",
        "look_for": (
            "Surgical procedures and imaging tend to be highest-cost. The gap "
            "between `billed_amount` and `allowed_amount` shows the contractual "
            "discount. `member_responsibility` is what the patient actually owes."
        ),
        "concepts": ["RANK() window function", "JOIN", "ORDER BY DESC", "LIMIT"],
    },
    {
        "cat": "💰 Claims Analysis",
        "title": "Duplicate Claim Detection",
        "icon": "👯",
        "desc": (
            "🧒 **Like finding two identical stickers in your collection — "
            "sometimes claims get submitted twice by accident.** This query "
            "groups claims by member, date, procedure, and amount to find "
            "exact matches that might be duplicates."
        ),
        "sql": """SELECT
    cr.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    cr.service_date,
    cr.procedure_code,
    cr.billed_amount,
    COUNT(*)                             AS occurrence_count,
    GROUP_CONCAT(cr.claim_number, ', ')  AS claim_numbers,
    GROUP_CONCAT(cr.claim_status, ', ')  AS statuses
FROM claims cr
JOIN members m ON cr.member_id = m.member_id
GROUP BY
    cr.member_id,
    m.first_name,
    m.last_name,
    cr.service_date,
    cr.procedure_code,
    cr.billed_amount
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;""",
        "look_for": (
            "Any row with `occurrence_count` > 1 is a potential duplicate. "
            "Check the `statuses` — if both are PROCESSED, the accumulator "
            "may be over-counted. One should be VOIDED."
        ),
        "concepts": ["GROUP BY", "HAVING", "COUNT(*)", "GROUP_CONCAT", "Aggregate filtering"],
    },
    {
        "cat": "💰 Claims Analysis",
        "title": "Monthly Claims Trend Analysis",
        "icon": "📈",
        "desc": (
            "🧒 **Like a chart showing how much candy your class ate each month "
            "of the school year.** This query groups claims by month and shows "
            "volume and cost trends over time."
        ),
        "sql": """SELECT
    STRFTIME('%Y-%m', cr.service_date)    AS service_month,
    COUNT(*)                               AS claim_count,
    COUNT(DISTINCT cr.member_id)           AS unique_members,
    ROUND(SUM(cr.billed_amount), 2)        AS total_billed,
    ROUND(SUM(cr.paid_amount), 2)          AS total_paid,
    ROUND(SUM(cr.member_responsibility), 2) AS total_member_resp,
    ROUND(AVG(cr.billed_amount), 2)        AS avg_billed_per_claim
FROM claims cr
WHERE cr.claim_status NOT IN ('VOIDED', 'REJECTED')
GROUP BY STRFTIME('%Y-%m', cr.service_date)
ORDER BY service_month;""",
        "look_for": (
            "Look for months with unusually high `claim_count` or `total_billed` — "
            "could indicate a batch reprocessing event or a seasonal spike. "
            "Months with high `unique_members` relative to total may indicate "
            "a wellness season (annual checkups)."
        ),
        "concepts": ["STRFTIME (date formatting)", "GROUP BY date parts", "Multiple aggregations", "ROUND"],
    },

    # ── 📊 Accumulator Deep Dives ──────────────────────────
    {
        "cat": "📊 Accumulator Deep Dives",
        "title": "Accumulator Dashboard with Utilization Percentage",
        "icon": "📊",
        "desc": (
            "🧒 **Each member has a 'spending jar' with a maximum level. "
            "This query shows how full each jar is.** We calculate the "
            "percentage used and flag anyone who's getting close to full "
            "or has already overflowed."
        ),
        "sql": """SELECT
    a.accumulator_id,
    a.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    a.benefit_year,
    a.individual_oop_accum,
    a.individual_oop_max,
    ROUND(a.individual_oop_accum * 100.0 /
          NULLIF(a.individual_oop_max, 0), 1)  AS pct_used,
    CASE
        WHEN a.individual_oop_accum > a.individual_oop_max
            THEN '🔴 BREACH'
        WHEN a.individual_oop_accum >= a.individual_oop_max * 0.9
            THEN '🟡 APPROACHING'
        WHEN a.individual_oop_accum >= a.individual_oop_max * 0.5
            THEN '🟢 MODERATE'
        ELSE '⚪ LOW'
    END                                  AS utilization_tier,
    a.period_start,
    a.period_end,
    a.snapshot_ts
FROM accumulator_snapshots a
JOIN members m ON a.member_id = m.member_id
ORDER BY pct_used DESC;""",
        "look_for": (
            "🔴 BREACH rows need immediate investigation — the member has "
            "exceeded their plan limit. 🟡 APPROACHING rows should be monitored. "
            "Check that `last_updated_at` is recent — stale snapshots may be wrong."
        ),
        "concepts": ["NULLIF (prevent division by zero)", "CASE with thresholds", "Percentage calculation", "JOIN"],
    },
    {
        "cat": "📊 Accumulator Deep Dives",
        "title": "OOP Breach Detection — Who's Over the Limit?",
        "icon": "🚨",
        "desc": (
            "🧒 **The spending jar overflowed! Water is spilling on the floor!** "
            "This query finds every member whose accumulated out-of-pocket "
            "spending has exceeded their plan's maximum allowed amount."
        ),
        "sql": """SELECT
    a.accumulator_id,
    a.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    m.family_id,
    bp.plan_name,
    a.benefit_year,
    a.individual_oop_accum,
    a.individual_oop_max,
    ROUND(a.individual_oop_accum - a.individual_oop_max, 2) AS overage_amount,
    ROUND(a.individual_oop_accum * 100.0 / a.individual_oop_max, 1) AS pct_of_limit
FROM accumulator_snapshots a
JOIN members m ON a.member_id = m.member_id
JOIN benefit_plans bp ON a.plan_id = bp.plan_id
WHERE a.individual_oop_accum > a.individual_oop_max
  AND a.individual_oop_max > 0
ORDER BY overage_amount DESC;""",
        "look_for": (
            "Every row here is a problem. High `overage_amount` means the "
            "member may have been overcharged — they should have hit \$0 "
            "member responsibility after reaching their OOP max. Check for "
            "duplicate claims or incorrect plan limits."
        ),
        "concepts": ["Multi-table JOIN", "WHERE with calculated condition", "ROUND"],
    },
    {
        "cat": "📊 Accumulator Deep Dives",
        "title": "Running Total Breach-Point Analysis",
        "icon": "🎯",
        "desc": (
            "🧒 **Imagine stacking blocks one by one until the tower falls over. "
            "Which block made it fall?** This query rebuilds a member's OOP "
            "spending claim by claim and marks exactly which claim pushed them "
            "over the limit — the 'breach point.'"
        ),
        "sql": """WITH oop_limit AS (
    SELECT a.individual_oop_max
    FROM accumulator_snapshots a
    WHERE a.member_id = 'MBR-001'
      AND a.individual_oop_accum > 0
    LIMIT 1
),
ordered_claims AS (
    SELECT
        cr.claim_record_id,
        cr.claim_number,
        cr.service_date,
        cr.provider_name,
        cr.member_responsibility,
        cr.claim_status,
        SUM(cr.member_responsibility) OVER (
            ORDER BY cr.service_date, cr.created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM claims cr
    WHERE cr.member_id = 'MBR-001'
      AND cr.claim_status NOT IN ('VOIDED', 'REJECTED')
)
SELECT
    oc.claim_record_id,
    oc.claim_number,
    oc.service_date,
    oc.provider_name,
    oc.member_responsibility,
    oc.running_total,
    ol.limit_amount                              AS oop_max,
    ROUND(oc.running_total - ol.limit_amount, 2) AS overage,
    CASE
        WHEN oc.running_total > ol.limit_amount
         AND oc.running_total - oc.member_responsibility <= ol.limit_amount
            THEN '>>> BREACH POINT <<<'
        WHEN oc.running_total > ol.limit_amount
            THEN 'POST-BREACH'
        ELSE 'WITHIN LIMIT'
    END AS breach_flag
FROM ordered_claims oc
CROSS JOIN oop_limit ol
ORDER BY oc.service_date;""",
        "look_for": (
            "Scan for `>>> BREACH POINT <<<` — that's the exact claim that "
            "caused the overflow. Everything after is `POST-BREACH`. The "
            "`overage` column shows how far past the limit each claim pushes. "
            "If the breach-point claim has a very high `member_responsibility`, "
            "investigate whether it was adjudicated correctly."
        ),
        "concepts": [
            "Multiple CTEs", "CROSS JOIN", "Running total with window function",
            "Breach-point detection logic", "Complex CASE expression",
        ],
    },
    {
        "cat": "📊 Accumulator Deep Dives",
        "title": "Accumulator vs. Claims Cross-Check (Drift Detection)",
        "icon": "⚖️",
        "desc": (
            "🧒 **You counted your marbles and wrote the number on a sticky note. "
            "Later, you count them again. Do the numbers match?** This query "
            "compares each member's accumulator snapshot to the actual sum of "
            "their claims to find mismatches (drift)."
        ),
        "sql": """WITH claim_totals AS (
    SELECT
        cr.member_id,
        SUM(cr.member_responsibility) AS claims_total
    FROM claims cr
    WHERE cr.claim_status NOT IN ('VOIDED', 'REJECTED')
    GROUP BY cr.member_id
)
SELECT
    a.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    a.benefit_year,
    a.individual_oop_accum                     AS accumulator_value,
    COALESCE(ct.claims_total, 0)         AS claims_value,
    ROUND(a.individual_oop_accum - COALESCE(ct.claims_total, 0), 2)
                                         AS drift_amount,
    CASE
        WHEN ABS(a.individual_oop_accum - COALESCE(ct.claims_total, 0)) < 0.01
            THEN '✅ IN SYNC'
        WHEN a.individual_oop_accum > COALESCE(ct.claims_total, 0)
            THEN '⬆️ ACCUMULATOR OVER'
        ELSE '⬇️ ACCUMULATOR UNDER'
    END                                  AS sync_status
FROM accumulator_snapshots a
JOIN members m ON a.member_id = m.member_id
LEFT JOIN claim_totals ct ON a.member_id = ct.member_id
WHERE a.individual_oop_accum > 0
ORDER BY ABS(drift_amount) DESC;""",
        "look_for": (
            "`⬆️ ACCUMULATOR OVER` means the snapshot includes amounts not "
            "backed by active claims — possibly a voided claim not backed out. "
            "`⬇️ ACCUMULATOR UNDER` means claims exist that haven't been "
            "accumulated — possibly a missed recompute run."
        ),
        "concepts": ["CTE", "LEFT JOIN", "COALESCE", "ABS function", "Drift detection pattern"],
    },
    {
        "cat": "📊 Accumulator Deep Dives",
        "title": "Family Rollup Discrepancy Finder",
        "icon": "👨‍👩‍👧‍👦",
        "desc": (
            "🧒 **If three kids each have some coins, the family total should "
            "be the sum of all three. If it's not — someone miscounted!** "
            "This query checks whether family-level accumulators equal the "
            "sum of individual member accumulators."
        ),
        "sql": """WITH individual_totals AS (
    SELECT
        m.family_id,
        SUM(a.individual_oop_accum) AS sum_individual_oop
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
    it.sum_individual_oop,
    fs.family_oop_snapshot,
    ROUND(fs.family_oop_snapshot - it.sum_individual_oop, 2)
                                    AS discrepancy,
    fs.family_oop_limit,
    CASE
        WHEN ABS(fs.family_oop_snapshot - it.sum_individual_oop) < 0.05
            THEN '✅ MATCHED'
        ELSE '❌ DISCREPANCY'
    END                             AS rollup_status
FROM individual_totals it
JOIN family_snapshots fs ON it.family_id = fs.family_id
ORDER BY ABS(discrepancy) DESC;""",
        "look_for": (
            "Any `❌ DISCREPANCY` row means the family total doesn't add up. "
            "Common causes: a member was skipped during accumulation, a member's "
            "`family_id` is wrong, or the family snapshot is stale. Use the "
            "Family Composition query to investigate which member is off."
        ),
        "concepts": ["Multiple CTEs", "Aggregation + JOIN", "Discrepancy detection", "ABS threshold"],
    },

    # ── 📁 File Operations ─────────────────────────────────
    {
        "cat": "📁 File Operations",
        "title": "File Processing Pipeline Status",
        "icon": "🔄",
        "desc": (
            "🧒 **Like tracking a package: did it arrive? Was it opened? "
            "Was everything inside okay?** This query shows every file and "
            "its journey through our processing pipeline."
        ),
        "sql": """SELECT
    f.file_id,
    f.file_name,
    f.file_type,
    f.file_direction,
    f.processing_status,
    c.client_name,
    v.vendor_name,
    f.expected_date,
    f.received_ts,
    f.row_count,
    pr.run_status,
    pr.rows_read,
    pr.rows_passed,
    pr.rows_failed,
    f.updated_at
FROM inbound_files fi
LEFT JOIN clients c ON f.client_id = c.client_id
LEFT JOIN vendors v ON f.vendor_id = v.vendor_id
LEFT JOIN processing_runs pr ON f.processing_run_id = pr.run_id
ORDER BY f.expected_date DESC, f.file_name;""",
        "look_for": (
            "Files with `file_status` = 'ERROR' or 'MISSING' need attention. "
            "Check `records_failed` — even a 'SUCCESS' run can have individual "
            "record failures. Compare `record_count` to `records_processed` for "
            "completeness."
        ),
        "concepts": ["Multiple LEFT JOINs", "Denormalized reporting query", "NULL handling"],
    },
    {
        "cat": "📁 File Operations",
        "title": "Missing & Late File Detection",
        "icon": "⏰",
        "desc": (
            "🧒 **Your friend promised to bring cookies every Monday. "
            "It's Monday afternoon and no cookies yet!** This query checks "
            "file schedules against actual file inventory to find expected "
            "files that never arrived."
        ),
        "sql": """SELECT
    fs.schedule_id,
    fs.file_type,
    fs.file_direction,
    fs.frequency,
    fs.expected_time,
    c.client_name,
    v.vendor_name,
    MAX(f.received_ts)                     AS last_received,
    ROUND(
        JULIANDAY('now') -
        JULIANDAY(MAX(f.received_ts)), 1
    )                                       AS days_since_last,
    CASE
        WHEN MAX(f.received_ts) IS NULL
            THEN '🔴 NEVER RECEIVED'
        WHEN JULIANDAY('now') - JULIANDAY(MAX(f.received_ts)) > 2
            THEN '🟡 OVERDUE'
        ELSE '✅ ON TRACK'
    END                                     AS schedule_status
FROM file_schedules fs
JOIN clients c ON fs.client_id = c.client_id
JOIN vendors v ON fs.vendor_id = v.vendor_id
LEFT JOIN inbound_files fi
    ON fs.client_id = f.client_id
   AND fs.vendor_id = f.vendor_id
   AND fs.file_type = f.file_type
   AND fs.file_direction = f.file_direction
WHERE fs.is_active = 1
GROUP BY
    fs.schedule_id, fs.file_type, fs.file_direction,
    fs.frequency, fs.expected_time, c.client_name, v.vendor_name
ORDER BY days_since_last DESC;""",
        "look_for": (
            "`🔴 NEVER RECEIVED` is the most serious — the vendor may not have "
            "been onboarded correctly. `🟡 OVERDUE` means the cadence was broken. "
            "For daily files, `days_since_last` > 1 is notable."
        ),
        "concepts": ["LEFT JOIN with GROUP BY", "MAX aggregation", "JULIANDAY date math", "CASE with NULL handling"],
    },
    {
        "cat": "📁 File Operations",
        "title": "Processing Run Performance Report",
        "icon": "⚡",
        "desc": (
            "🧒 **Like a report card for each time we processed a file. "
            "Did we get all the answers right, or did some fail?** "
            "This query calculates success rates and processing duration "
            "for every run."
        ),
        "sql": """SELECT
    pr.run_id,
    pr.run_type,
    pr.run_status,
    f.file_name,
    pr.rows_read,
    pr.rows_passed,
    pr.rows_failed,
    CASE
        WHEN pr.rows_read > 0
        THEN ROUND(pr.rows_passed * 100.0 / pr.rows_read, 1)
        ELSE 0
    END                                     AS success_rate_pct,
    pr.started_at,
    pr.completed_at,
    ROUND(
        (JULIANDAY(pr.completed_at) -
         JULIANDAY(pr.started_at)) * 1440, 1
    )                                       AS duration_minutes,
    pr.error_message
FROM processing_runs pr
LEFT JOIN inbound_files fi ON pr.file_id = f.file_id
ORDER BY pr.started_at DESC;""",
        "look_for": (
            "Runs with `success_rate_pct` < 100 had some record-level failures. "
            "Check `error_message` for details. Long `duration_minutes` could "
            "indicate performance issues. FAILED runs need investigation."
        ),
        "concepts": ["Percentage calculation", "JULIANDAY time difference", "Minutes conversion (* 1440)", "LEFT JOIN"],
    },

    # ── 🎫 Support Cases & SLA ─────────────────────────────
    {
        "cat": "🎫 Support Cases & SLA",
        "title": "Active Cases with SLA Countdown",
        "icon": "⏳",
        "desc": (
            "🧒 **You have homework due at different times. Which one is due "
            "soonest? Are any already late?** This query shows every open case "
            "with its SLA deadline and how many hours are left."
        ),
        "sql": """SELECT
    sc.case_number,
    sc.short_description,
    sc.priority,
    sc.severity,
    sc.status,
    sc.assignment_group,
    sc.assigned_to,
    sla.sla_type,
    sla.target_hours,
    sla.target_due_at,
    ROUND(
        (JULIANDAY(sla.target_due_at) -
         JULIANDAY('now')) * 24, 1
    )                                       AS hours_remaining,
    sla.is_at_risk,
    sla.is_breached,
    CASE
        WHEN sla.is_breached = 1        THEN '🔴 BREACHED'
        WHEN sla.is_at_risk = 1         THEN '🟡 AT RISK'
        WHEN (JULIANDAY(sla.target_due_at) -
              JULIANDAY('now')) * 24 < 2 THEN '🟠 URGENT'
        ELSE '🟢 ON TRACK'
    END                                     AS sla_health,
    sc.opened_at
FROM support_cases sc
JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE sc.status IN ('OPEN', 'IN_PROGRESS')
ORDER BY hours_remaining ASC;""",
        "look_for": (
            "Cases are sorted by urgency — `hours_remaining` negative means "
            "the SLA is already breached. `🟠 URGENT` cases have less than 2 "
            "hours. This is your primary triage view each morning."
        ),
        "concepts": ["JOIN", "JULIANDAY with 'now'", "Hours calculation (* 24)", "Multi-level CASE"],
    },
    {
        "cat": "🎫 Support Cases & SLA",
        "title": "SLA Breach Analysis by Root Cause",
        "icon": "🔍",
        "desc": (
            "🧒 **If homework keeps being late, the teacher wants to know WHY. "
            "Is it the same subject every time?** This query groups SLA breaches "
            "by their root cause category to find systemic patterns."
        ),
        "sql": """SELECT
    sc.root_cause_category,
    COUNT(*)                                AS total_breaches,
    ROUND(AVG(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24
    ), 1)                                   AS avg_resolution_hours,
    GROUP_CONCAT(DISTINCT sc.assignment_group, ', ')
                                            AS affected_teams,
    MIN(sc.opened_at)                       AS earliest_breach,
    MAX(sc.opened_at)                       AS latest_breach
FROM support_cases sc
JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE sla.is_breached = 1
GROUP BY sc.root_cause_category
ORDER BY total_breaches DESC;""",
        "look_for": (
            "Root causes with the highest `total_breaches` are systemic issues. "
            "If one `assignment_group` appears repeatedly, they may be under-staffed "
            "or the SLA target may be too aggressive. `avg_resolution_hours` shows "
            "how long breached cases actually take to resolve."
        ),
        "concepts": ["GROUP BY with aggregation", "AVG of computed expression", "GROUP_CONCAT DISTINCT", "MIN/MAX"],
    },
    {
        "cat": "🎫 Support Cases & SLA",
        "title": "Case Lifecycle Timeline",
        "icon": "📅",
        "desc": (
            "🧒 **Like a diary for a support case: when was it opened, when did "
            "someone start working on it, and when was it finished?** "
            "This shows the complete timeline for every case."
        ),
        "sql": """SELECT
    sc.case_number,
    sc.short_description,
    sc.priority,
    sc.status,
    sc.opened_at,
    sc.acknowledged_at,
    sc.resolved_at,
    sc.closed_at,
    ROUND(
        (JULIANDAY(COALESCE(sc.acknowledged_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                                       AS hours_to_acknowledge,
    ROUND(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                                       AS hours_to_resolve,
    ROUND(
        (JULIANDAY(COALESCE(sc.closed_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                                       AS total_lifecycle_hours,
    sc.root_cause_category,
    sc.escalation_level
FROM support_cases sc
ORDER BY sc.opened_at DESC;""",
        "look_for": (
            "Long `hours_to_acknowledge` suggests the case sat in queue too long. "
            "Large gaps between `resolved_at` and `closed_at` mean follow-up "
            "documentation is delayed. `escalation_level` > 0 indicates the "
            "case needed management attention."
        ),
        "concepts": ["COALESCE with DATETIME('now')", "Multiple time interval calculations", "JULIANDAY", "ROUND"],
    },

    # ── 🔗 Cross-Domain Investigations ─────────────────────
    {
        "cat": "🔗 Cross-Domain Investigations",
        "title": "End-to-End Issue Traceability",
        "icon": "🧵",
        "desc": (
            "🧒 **Following a trail of breadcrumbs from the forest back to "
            "your house.** This query traces a data quality issue from "
            "detection → support case → SLA tracking, showing the complete "
            "chain of operational response."
        ),
        "sql": """SELECT
    dqi.issue_id,
    dqi.issue_code,
    dqi.severity                 AS issue_severity,
    dqi.status                   AS issue_status,
    dqi.detected_at,
    sc.case_number,
    sc.priority                  AS case_priority,
    sc.status                    AS case_status,
    sc.assignment_group,
    sc.assigned_to,
    sc.opened_at,
    sc.resolved_at,
    sla.sla_type,
    sla.target_hours,
    sla.status                   AS sla_status,
    sla.is_breached,
    ROUND(
        (JULIANDAY(COALESCE(sc.resolved_at, DATETIME('now'))) -
         JULIANDAY(sc.opened_at)) * 24, 1
    )                            AS elapsed_hours
FROM data_quality_issues dqi
LEFT JOIN support_cases sc ON dqi.issue_id = sc.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
ORDER BY dqi.detected_at DESC;""",
        "look_for": (
            "Issues with NULL `case_number` were detected but no case was created — "
            "a gap in the operational process. Issues with `sla_status` = 'BREACHED' "
            "need root cause analysis. The `elapsed_hours` vs. `target_hours` "
            "comparison shows SLA performance per issue."
        ),
        "concepts": ["Triple LEFT JOIN chain", "Traceability pattern", "NULL detection for process gaps"],
    },
    {
        "cat": "🔗 Cross-Domain Investigations",
        "title": "Client Operations Health Scorecard",
        "icon": "📋",
        "desc": (
            "🧒 **Like a report card for each client: how are their files, "
            "cases, members, and accumulators doing?** This single query builds "
            "a comprehensive health scorecard per client."
        ),
        "sql": """SELECT
    c.client_id,
    c.client_name,
    (SELECT COUNT(*) FROM members
     WHERE client_id = c.client_id
       AND eligibility_status = 'ACTIVE')      AS active_members,
    (SELECT COUNT(*) FROM inbound_files
     WHERE client_id = c.client_id)            AS total_files,
    (SELECT COUNT(*) FROM inbound_files
     WHERE client_id = c.client_id
       AND file_status IN ('ERROR','FAILED','MISSING'))
                                                AS problem_files,
    (SELECT COUNT(*) FROM support_cases
     WHERE client_id = c.client_id
       AND status IN ('OPEN','IN_PROGRESS'))   AS open_cases,
    (SELECT COUNT(*) FROM data_quality_issues
     WHERE client_id = c.client_id
       AND status = 'OPEN')                    AS open_issues,
    (SELECT COUNT(*) FROM accumulator_snapshots a
     JOIN members m ON a.member_id = m.member_id
     WHERE m.client_id = c.client_id
       AND a.individual_oop_accum > a.individual_oop_max
       AND a.individual_oop_max > 0)                 AS accum_breaches
FROM clients c
WHERE c.status = 'ACTIVE'
ORDER BY open_cases DESC, problem_files DESC;""",
        "look_for": (
            "Clients with `problem_files` > 0 AND `open_cases` > 0 need priority "
            "attention. `accum_breaches` indicates members who may be overcharged. "
            "This is an executive-level summary you'd present in a weekly ops review."
        ),
        "concepts": ["Correlated scalar subqueries", "Multi-metric scorecard pattern", "Subquery in SELECT"],
    },
    {
        "cat": "🔗 Cross-Domain Investigations",
        "title": "Vendor File Delivery Reliability",
        "icon": "🤝",
        "desc": (
            "🧒 **Rating each delivery person: how often do they show up on time "
            "and with the right package?** This query scores each vendor's "
            "file delivery reliability based on their history."
        ),
        "sql": """SELECT
    v.vendor_id,
    v.vendor_name,
    v.vendor_type,
    COUNT(f.file_id)                           AS total_files,
    SUM(CASE WHEN f.processing_status = 'PROCESSED'
             THEN 1 ELSE 0 END)                 AS successful_files,
    SUM(CASE WHEN f.processing_status IN ('ERROR','FAILED')
             THEN 1 ELSE 0 END)                 AS failed_files,
    SUM(CASE WHEN f.processing_status = 'MISSING'
             THEN 1 ELSE 0 END)                 AS missing_files,
    CASE
        WHEN COUNT(f.file_id) > 0
        THEN ROUND(
            SUM(CASE WHEN f.processing_status = 'PROCESSED'
                     THEN 1 ELSE 0 END) * 100.0 /
            COUNT(f.file_id), 1
        )
        ELSE 0
    END                                         AS reliability_pct,
    vc.contact_name                             AS primary_contact,
    vc.contact_email
FROM vendors v
LEFT JOIN inbound_files fi ON v.vendor_id = f.vendor_id
LEFT JOIN vendor_contacts vc
    ON v.vendor_id = vc.vendor_id AND vc.is_primary = 1
WHERE v.status = 'ACTIVE'
GROUP BY v.vendor_id, v.vendor_name, v.vendor_type,
         vc.contact_name, vc.contact_email
ORDER BY reliability_pct ASC;""",
        "look_for": (
            "Vendors with `reliability_pct` < 95% need a conversation. "
            "`missing_files` > 0 is especially concerning — it means files "
            "simply didn't arrive. The primary contact info is included so you "
            "can reach out immediately."
        ),
        "concepts": ["Conditional SUM (CASE inside aggregate)", "Percentage calculation", "Multiple LEFT JOINs", "GROUP BY with non-aggregated columns"],
    },

    # ── 🧠 Advanced SQL Patterns ───────────────────────────
    {
        "cat": "🧠 Advanced SQL Patterns",
        "title": "Percentile Ranking of Member OOP Spending",
        "icon": "📊",
        "desc": (
            "🧒 **In a race, knowing you came 75th out of 100 tells you "
            "you're in the bottom quarter. This query ranks every member's "
            "OOP spending as a percentile.** Members at the 90th+ percentile "
            "are your highest utilizers."
        ),
        "sql": """SELECT
    a.member_id,
    m.first_name || ' ' || m.last_name  AS member_name,
    a.individual_oop_accum,
    a.individual_oop_max,
    ROUND(PERCENT_RANK() OVER (
        ORDER BY a.individual_oop_accum
    ) * 100, 1)                         AS spending_percentile,
    NTILE(4) OVER (
        ORDER BY a.individual_oop_accum
    )                                   AS spending_quartile,
    CASE NTILE(4) OVER (ORDER BY a.individual_oop_accum)
        WHEN 1 THEN '🟢 Low (Q1)'
        WHEN 2 THEN '🔵 Moderate (Q2)'
        WHEN 3 THEN '🟡 High (Q3)'
        WHEN 4 THEN '🔴 Very High (Q4)'
    END                                 AS quartile_label
FROM accumulator_snapshots a
JOIN members m ON a.member_id = m.member_id
WHERE a.individual_oop_accum > 0
  AND a.individual_oop_accum > 0
ORDER BY spending_percentile DESC;""",
        "look_for": (
            "Q4 members are in the top 25% of spending — these are your "
            "high utilizers who may approach or breach their OOP max. "
            "The `spending_percentile` gives a finer-grained view."
        ),
        "concepts": ["PERCENT_RANK()", "NTILE() (quartile bucketing)", "Window functions without PARTITION BY"],
    },
    {
        "cat": "🧠 Advanced SQL Patterns",
        "title": "Gap Analysis Using LEAD Window Function",
        "icon": "🔭",
        "desc": (
            "🧒 **Imagine reading a book and noticing a page is missing. "
            "You can tell because page 4 jumps to page 6.** This query uses "
            "LEAD to compare each file's date to the next expected file, "
            "detecting gaps in the delivery schedule."
        ),
        "sql": """SELECT
    f.file_id,
    f.file_name,
    f.file_type,
    f.expected_date,
    f.received_ts,
    f.processing_status,
    LEAD(f.expected_date) OVER (
        PARTITION BY f.client_id, f.file_type
        ORDER BY f.expected_date
    )                                    AS next_expected_date,
    JULIANDAY(
        LEAD(f.expected_date) OVER (
            PARTITION BY f.client_id, f.file_type
            ORDER BY f.expected_date
        )
    ) - JULIANDAY(f.expected_date)      AS days_to_next,
    c.client_name
FROM inbound_files fi
JOIN clients c ON f.client_id = c.client_id
WHERE f.file_direction = 'INBOUND'
ORDER BY f.client_id, f.file_type, f.expected_date;""",
        "look_for": (
            "If `days_to_next` is larger than expected for the frequency "
            "(e.g., > 1 for daily files), there's a gap. A NULL `next_expected_date` "
            "means this is the most recent file in the sequence."
        ),
        "concepts": ["LEAD window function", "PARTITION BY", "Date gap detection pattern", "JULIANDAY arithmetic"],
    },
    {
        "cat": "🧠 Advanced SQL Patterns",
        "title": "Pivoted Claim Status Summary per Member",
        "icon": "🔄",
        "desc": (
            "🧒 **Instead of a long list, imagine a neat table where each row "
            "is a person and each column shows how many of each type they have.** "
            "This query pivots claim statuses into columns using conditional aggregation."
        ),
        "sql": """SELECT
    m.member_id,
    m.first_name || ' ' || m.last_name      AS member_name,
    COUNT(*)                                  AS total_claims,
    SUM(CASE WHEN cr.claim_status = 'PROCESSED'
             THEN 1 ELSE 0 END)               AS processed,
    SUM(CASE WHEN cr.claim_status = 'VOIDED'
             THEN 1 ELSE 0 END)               AS voided,
    SUM(CASE WHEN cr.claim_status = 'REJECTED'
             THEN 1 ELSE 0 END)               AS rejected,
    SUM(CASE WHEN cr.claim_status = 'DUPLICATE'
             THEN 1 ELSE 0 END)               AS duplicate,
    ROUND(SUM(cr.billed_amount), 2)           AS total_billed,
    ROUND(SUM(CASE WHEN cr.claim_status = 'PROCESSED'
                   THEN cr.member_responsibility
                   ELSE 0 END), 2)            AS active_oop
FROM members m
LEFT JOIN claims cr ON m.member_id = cr.member_id
GROUP BY m.member_id, m.first_name, m.last_name
HAVING COUNT(cr.claim_record_id) > 0
ORDER BY total_claims DESC;""",
        "look_for": (
            "Members with `voided` > 0 may have had corrections. Members with "
            "`duplicate` > 0 need investigation — were those correctly caught "
            "or are they still affecting accumulators? The `active_oop` only counts "
            "PROCESSED claims."
        ),
        "concepts": ["Conditional aggregation (pivot pattern)", "HAVING clause", "LEFT JOIN with GROUP BY"],
    },
    {
        "cat": "🧠 Advanced SQL Patterns",
        "title": "Recursive Date Series for SLA Monitoring",
        "icon": "📆",
        "desc": (
            "🧒 **Making a calendar and marking which days had problems.** "
            "This query generates a series of dates and joins them to SLA data "
            "to show a day-by-day SLA health timeline."
        ),
        "sql": """WITH RECURSIVE date_series AS (
    SELECT DATE('now', '-14 days') AS report_date
    UNION ALL
    SELECT DATE(report_date, '+1 day')
    FROM date_series
    WHERE report_date < DATE('now')
)
SELECT
    ds.report_date,
    COALESCE(counts.cases_opened, 0)      AS cases_opened,
    COALESCE(counts.cases_resolved, 0)    AS cases_resolved,
    COALESCE(counts.slas_breached, 0)     AS slas_breached
FROM date_series ds
LEFT JOIN (
    SELECT
        DATE(sc.opened_at) AS dt,
        COUNT(*)           AS cases_opened,
        SUM(CASE WHEN sc.resolved_at IS NOT NULL
                 THEN 1 ELSE 0 END)   AS cases_resolved,
        SUM(CASE WHEN sla.is_breached = 1
                 THEN 1 ELSE 0 END)   AS slas_breached
    FROM support_cases sc
    LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
    GROUP BY DATE(sc.opened_at)
) counts ON ds.report_date = counts.dt
ORDER BY ds.report_date;""",
        "look_for": (
            "Days with 0 across all columns were quiet days. Spikes in "
            "`cases_opened` indicate incident events (like a missing file). "
            "Days where `slas_breached` > 0 need retrospective review. "
            "This is a classic operational health timeline."
        ),
        "concepts": ["Recursive CTE", "Date series generation", "LEFT JOIN to aggregated subquery", "COALESCE for zero-fill"],
    },
]

QUERY_CATEGORIES = list(dict.fromkeys(q["cat"] for q in QUERY_LIBRARY))


# ═══════════════════════════════════════════════════════════════
# SCHEMA REFERENCE
# ═══════════════════════════════════════════════════════════════

SCHEMA_REFERENCE = {
    "clients": {
        "desc": "Health plan organizations",
        "cols": "client_id, client_name, client_code, status, created_at, updated_at",
    },
    "vendors": {
        "desc": "TPAs, PBMs, clearinghouses, claims processors",
        "cols": "vendor_id, vendor_name, vendor_code, vendor_type, status, created_at, updated_at",
    },
    "client_vendor_relationships": {
        "desc": "Which vendors serve which clients",
        "cols": "relationship_id, client_id, vendor_id, relationship_type, status, created_at",
    },
    "vendor_contacts": {
        "desc": "Contact people at each vendor",
        "cols": "contact_id, vendor_id, contact_name, contact_email, contact_phone, contact_type, is_primary, created_at",
    },
    "benefit_plans": {
        "desc": "Plan configurations with OOP and deductible limits",
        "cols": "plan_id, plan_name, plan_type, client_id, individual_oop_max, family_oop_max, individual_deductible, family_deductible, benefit_year_start, benefit_year_end, status, created_at, updated_at",
    },
    "members": {
        "desc": "Health plan members (subscribers, spouses, dependents)",
        "cols": "member_id, external_member_id, first_name, last_name, date_of_birth, gender, family_id, relationship_code, client_id, plan_id, eligibility_status, created_at, updated_at",
    },
    "eligibility_periods": {
        "desc": "Date ranges when a member has active coverage",
        "cols": "eligibility_period_id, member_id, plan_id, start_date, end_date, status, created_at",
    },
    "file_schedules": {
        "desc": "Expected file deliveries (daily, weekly, etc.)",
        "cols": "schedule_id, client_id, vendor_id, file_type, file_direction, frequency, expected_time, day_of_week, day_of_month, is_active, created_at",
    },
    "inbound_files": {
        "desc": "Every file received or expected in the system",
        "cols": "file_id, file_name, file_type, client_id, vendor_id, expected_date, received_ts, row_count, processing_status, duplicate_flag, error_count, file_hash, landing_path, archived_path, created_at, updated_at",
    },
    "processing_runs": {
        "desc": "Records of file processing executions",
        "cols": "run_id, file_id, run_type, run_status, started_at, completed_at, rows_read, rows_passed, rows_failed, issue_count, notes, created_at",
    },
    "claims": {
        "desc": "Individual medical/pharmacy claims",
        "cols": "claim_record_id, claim_id, line_id, member_id, subscriber_id, client_id, plan_id, vendor_id, service_date, paid_date, allowed_amount, paid_amount, member_responsibility, deductible_amount, coinsurance_amount, copay_amount, claim_status, reversal_flag, preventive_flag, source_file_id, created_at",
    },
    "accumulator_snapshots": {
        "desc": "Point-in-time OOP and deductible accumulations per member",
        "cols": "snapshot_id, member_id, family_id, client_id, plan_id, benefit_year, individual_deductible_accum, family_deductible_accum, individual_oop_accum, family_oop_accum, individual_deductible_met_flag, family_deductible_met_flag, individual_oop_met_flag, family_oop_met_flag, snapshot_ts",
    },
    "data_quality_issues": {
        "desc": "Detected data problems from validation and reconciliation",
        "cols": "issue_id, issue_code, issue_type, issue_subtype, severity, status, client_id, vendor_id, file_id, run_id, member_id, claim_record_id, entity_name, entity_key, source_row_number, issue_description, issue_message, detected_at, source_file_id, processing_run_id, created_at",
    },
    "support_cases": {
        "desc": "Operational support tickets for issue resolution",
        "cols": "case_id, case_number, issue_id, client_id, vendor_id, file_id, processing_run_id, case_type, priority, severity, status, assignment_group, assigned_to, short_description, description, root_cause_category, escalation_level, opened_at, acknowledged_at, resolved_at, closed_at, last_updated_at, member_id, claim_record_id",
    },
    "sla_tracking": {
        "desc": "Service level agreement timers for support cases",
        "cols": "sla_id, case_id, sla_type, target_hours, target_due_at, status, is_at_risk, is_breached, breached_at, last_evaluated_at, created_at, updated_at",
    },
}


# ═══════════════════════════════════════════════════════════════
# SQL CHALLENGES — test-yourself exercises
# ═══════════════════════════════════════════════════════════════

SQL_CHALLENGES = [
    {
        "level": "🟢 Beginner",
        "title": "Count All Support Cases",
        "prompt": "Write a query that counts how many support cases exist in the system, grouped by status.",
        "hint": "Use COUNT(*), GROUP BY, and ORDER BY. The table is `support_cases`.",
        "solution": "SELECT status, COUNT(*) AS case_count\nFROM support_cases\nGROUP BY status\nORDER BY case_count DESC;",
    },
    {
        "level": "🟡 Intermediate",
        "title": "Members with Open SLA Breaches",
        "prompt": "Find all support cases that have a breached SLA, showing case number, priority, and assigned team.",
        "hint": "JOIN `support_cases` with `sla_tracking` on case_id. Filter WHERE is_breached = 1.",
        "solution": "SELECT sc.case_number, sc.priority, sc.assigned_team, sc.short_description\nFROM support_cases sc\nJOIN sla_tracking st ON sc.case_id = st.case_id\nWHERE st.is_breached = 1\nORDER BY sc.priority;",
    },
    {
        "level": "🟠 Advanced",
        "title": "OOP Breach Summary by Client",
        "prompt": "Find how many members per client have an individual OOP accumulator exceeding their plan maximum.",
        "hint": "Use `accumulator_snapshots` JOIN `benefit_plans` JOIN `clients`. Filter WHERE individual_oop_accum > individual_oop_max.",
        "solution": "SELECT c.client_code, COUNT(*) AS breach_count\nFROM accumulator_snapshots s\nJOIN benefit_plans p ON s.plan_id = p.plan_id\nJOIN clients c ON s.client_id = c.client_id\nWHERE s.individual_oop_accum > p.individual_oop_max\nGROUP BY c.client_code\nORDER BY breach_count DESC;",
    },
]


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def fmt_number(value, fallback="—"):
    """Format a number with optional commas."""
    if value is None:
        return fallback
    try:
        n = float(value)
        return f"{int(n):,}" if n == int(n) else f"{n:,.2f}"
    except (TypeError, ValueError):
        return fallback


st.set_page_config(page_title="SQL Query Workbench", page_icon="🔍", layout="wide")

st.title("🔍 SQL Query Workbench")
st.caption(
    "Explore, learn, and prove your SQL skills against a live healthcare operations database. "
    "Every query runs in real time against the same data powering the dashboards."
)

tab_guided, tab_sandbox, tab_challenges, tab_schema, tab_concepts, tab_how = st.tabs([
    "📚 Guided Queries",
    "⌨️ SQL Sandbox",
    "🏆 SQL Challenges",
    "🗂️ Schema Explorer",
    "💡 SQL Concepts",
    "❓ How It Works",
])

with tab_guided:
    st.subheader("📚 Guided Query Library")
    st.info(
        "**28 production-grade SQL queries** organized by domain. Each query includes "
        "a plain-English explanation, the SQL code, a 'What to Look For' guide, and "
        "the SQL concepts demonstrated. Click **▶️ Run Query** to execute against live data."
    )

    selected_cat = st.selectbox(
        "Filter by Category",
        ["All Categories"] + QUERY_CATEGORIES,
        key="guided_cat",
    )

    filtered = QUERY_LIBRARY if selected_cat == "All Categories" else [
        q for q in QUERY_LIBRARY if q["cat"] == selected_cat
    ]

    for i, query in enumerate(filtered):
        with st.expander(f"{query['icon']} {query['title']}", expanded=(i == 0)):
            st.markdown(query["desc"])

            st.code(query["sql"], language="sql")

            col_run, col_concepts = st.columns([1, 3])
            with col_run:
                run_btn = st.button(
                    "▶️ Run Query",
                    key=f"run_guided_{query['title']}_{i}",
                )
            with col_concepts:
                st.markdown(
                    "**Concepts:** " + " · ".join(f"`{c}`" for c in query["concepts"])
                )

            if run_btn:
                with st.spinner("Executing..."):
                    df, elapsed, error = run_query(query["sql"])

                if error:
                    st.error(error)
                elif df is not None:
                    st.success(f"✅ {len(df)} rows returned in {elapsed} ms")
                    st.dataframe(df, use_container_width=True, hide_index=True)

                    csv = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Download Results",
                        csv,
                        f"query_results_{i}.csv",
                        "text/csv",
                        key=f"dl_guided_{i}",
                    )

            with st.container():
                st.markdown(f"**🔎 What to Look For:** {query['look_for']}")


with tab_sandbox:
    st.subheader("⌨️ SQL Sandbox")
    st.info(
        "🧒 **This is your playground!** Write any SELECT query you want and run it "
        "against the live database. It's like having a magnifying glass that lets you "
        "look at any part of the data you're curious about.\n\n"
        "**Safety:** Only SELECT queries are allowed. INSERT, UPDATE, DELETE, DROP, "
        "and other write operations are blocked."
    )

    default_sql = "SELECT * FROM members WHERE eligibility_status = 'ACTIVE' LIMIT 10;"

    user_sql = st.text_area(
        "Write your SQL query here:",
        value=default_sql,
        height=200,
        key="sandbox_sql",
    )

    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 4])
    with col_btn1:
        sandbox_run = st.button("▶️ Run Query", key="sandbox_run", type="primary")
    with col_btn2:
        sandbox_clear = st.button("🗑️ Clear", key="sandbox_clear")

    if sandbox_clear:
        st.rerun()

    if sandbox_run and user_sql.strip():
        with st.spinner("Executing your query..."):
            df, elapsed, error = run_query(user_sql)

        if error:
            st.error(error)
        elif df is not None:
            st.success(f"✅ {len(df)} rows returned in {elapsed} ms")
            st.dataframe(df, use_container_width=True, hide_index=True)

            if len(df) > 0 and len(df.select_dtypes(include=["number"]).columns) > 0:
                st.caption("💡 Tip: Numeric columns detected — consider charting this data!")

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Download Results",
                csv,
                "sandbox_results.csv",
                "text/csv",
                key="dl_sandbox",
            )

    st.divider()
    st.markdown("**Quick-Start Templates** — click to load into the sandbox:")
    templates = {
        "All Tables": "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
        "Members": "SELECT * FROM members LIMIT 20;",
        "Claims": "SELECT * FROM claims LIMIT 20;",
        "Accumulators": "SELECT * FROM accumulator_snapshots LIMIT 20;",
        "Cases": "SELECT * FROM support_cases LIMIT 20;",
        "Files": "SELECT * FROM inbound_files LIMIT 20;",
        "SLAs": "SELECT * FROM sla_tracking LIMIT 20;",
        "Issues": "SELECT * FROM data_quality_issues LIMIT 20;",
    }
    template_cols = st.columns(4)
    for idx, (label, sql) in enumerate(templates.items()):
        with template_cols[idx % 4]:
            if st.button(f"📋 {label}", key=f"template_{idx}"):
                st.session_state["sandbox_sql"] = sql
                st.rerun()


with tab_challenges:
    st.subheader("🏆 SQL Challenges")
    st.info(
        "🧒 **Test yourself!** Each challenge gives you a task and a hint. "
        "Write your own SQL, run it, and then reveal the solution to compare. "
        "Like a quiz where you can check your answers afterward."
    )

    for i, challenge in enumerate(SQL_CHALLENGES):
        with st.expander(
            f"{challenge['level']} — {challenge['title']}",
            expanded=(i == 0),
        ):
            st.markdown(f"**Task:** {challenge['prompt']}")

            st.markdown(f"💡 **Hint:** {challenge['hint']}")

            user_attempt = st.text_area(
                "Your SQL:",
                height=150,
                key=f"challenge_sql_{i}",
                placeholder="Write your query here...",
            )

            col_try, col_show = st.columns(2)
            with col_try:
                if st.button("▶️ Run My Query", key=f"challenge_run_{i}"):
                    if user_attempt.strip():
                        with st.spinner("Running your query..."):
                            df, elapsed, error = run_query(user_attempt)
                        if error:
                            st.error(error)
                        elif df is not None:
                            st.success(f"✅ {len(df)} rows in {elapsed} ms")
                            st.dataframe(
                                df, use_container_width=True, hide_index=True
                            )
                    else:
                        st.warning("Write some SQL first!")

            with col_show:
                if st.button("👁️ Reveal Solution", key=f"challenge_sol_{i}"):
                    st.code(challenge["solution"], language="sql")

                    with st.spinner("Running solution..."):
                        df, elapsed, error = run_query(challenge["solution"])
                    if df is not None:
                        st.success(f"Solution: {len(df)} rows in {elapsed} ms")
                        st.dataframe(
                            df, use_container_width=True, hide_index=True
                        )


with tab_schema:
    st.subheader("🗂️ Schema Explorer")
    st.info(
        "🧒 **Like a map of a building showing every room and what's inside.** "
        "This tab shows every table in the database, what it stores, and all "
        "its columns. Click any table to see its structure and sample data."
    )

    for table_name, info in SCHEMA_REFERENCE.items():
        with st.expander(f"📋 **{table_name}** — {info['desc']}"):
            st.markdown(f"**Columns:** `{info['cols']}`")

            if st.button(
                f"🔎 Show Sample Data",
                key=f"schema_sample_{table_name}",
            ):
                df, elapsed, error = run_query(
                    f"SELECT * FROM {table_name} LIMIT 5;"
                )
                if error:
                    st.error(error)
                elif df is not None:
                    st.caption(f"{len(df)} sample rows — {elapsed} ms")
                    st.dataframe(
                        df, use_container_width=True, hide_index=True
                    )

            if st.button(
                f"📊 Row Count",
                key=f"schema_count_{table_name}",
            ):
                df, elapsed, error = run_query(
                    f"SELECT COUNT(*) AS row_count FROM {table_name};"
                )
                if df is not None:
                    st.metric(
                        f"{table_name} rows",
                        fmt_number(df.iloc[0]["row_count"]),
                    )

    st.divider()
    st.markdown("### Relationship Map")
    st.code("""
clients ──┬── benefit_plans ── members ──┬── eligibility_periods
          │                              ├── claim_records
          │                              └── accumulator_snapshots
          │
          ├── client_vendor_relationships ── vendors ── vendor_contacts
          │
          └── file_schedules ── file_inventory ── processing_runs
                                     │
                        data_quality_issues ── support_cases ── sla_tracking
    """, language="text")


with tab_concepts:
    st.subheader("💡 SQL Concepts Reference")
    st.info(
        "🧒 **A dictionary for SQL words!** Every SQL concept used in the "
        "Guided Queries is explained here in plain English with a tiny example."
    )

    concepts = {
        "SELECT & FROM": {
            "explain": (
                "🧒 **Picking toys from a toy box.** SELECT says which toys "
                "you want, FROM says which box to look in."
            ),
            "example": "SELECT first_name, last_name FROM members;",
        },
        "WHERE": {
            "explain": (
                "🧒 **Only picking red toys.** WHERE filters rows to keep "
                "only the ones that match your condition."
            ),
            "example": "SELECT * FROM members WHERE eligibility_status = 'ACTIVE';",
        },
        "JOIN (INNER)": {
            "explain": (
                "🧒 **Matching kids to their backpacks by their name tags.** "
                "JOIN connects two tables using a shared column."
            ),
            "example": "SELECT m.first_name, bp.plan_name\nFROM members m\nJOIN benefit_plans bp ON m.plan_id = bp.plan_id;",
        },
        "LEFT JOIN": {
            "explain": (
                "🧒 **Listing ALL kids, even the ones without a backpack.** "
                "LEFT JOIN keeps every row from the left table, even if "
                "there's no match on the right."
            ),
            "example": "SELECT m.first_name, cr.claim_number\nFROM members m\nLEFT JOIN claims cr ON m.member_id = cr.member_id;",
        },
        "GROUP BY & Aggregations": {
            "explain": (
                "🧒 **Sorting toys into piles by color, then counting how many "
                "are in each pile.** GROUP BY creates groups. COUNT, SUM, AVG, "
                "MIN, MAX tell you something about each group."
            ),
            "example": "SELECT client_id, COUNT(*) AS member_count\nFROM members\nGROUP BY client_id;",
        },
        "HAVING": {
            "explain": (
                "🧒 **After sorting into piles, only keeping piles with more "
                "than 3 toys.** HAVING filters groups AFTER aggregation — "
                "WHERE filters rows BEFORE."
            ),
            "example": "SELECT provider_name, COUNT(*) AS claims\nFROM claim_records\nGROUP BY provider_name\nHAVING COUNT(*) > 2;",
        },
        "ORDER BY": {
            "explain": (
                "🧒 **Lining up from tallest to shortest (DESC) or shortest "
                "to tallest (ASC).** ORDER BY sorts your results."
            ),
            "example": "SELECT * FROM claims\nORDER BY billed_amount DESC;",
        },
        "LIMIT": {
            "explain": (
                "🧒 **Only looking at the first 5 toys in the line.** "
                "LIMIT cuts off the results after a certain number of rows."
            ),
            "example": "SELECT * FROM members LIMIT 5;",
        },
        "CASE Expression": {
            "explain": (
                "🧒 **If it's red, put it in the red box. If it's blue, "
                "put it in the blue box. Otherwise, put it in the 'other' box.** "
                "CASE lets you create new values based on conditions."
            ),
            "example": "SELECT member_id,\n  CASE\n    WHEN eligibility_status = 'ACTIVE' THEN '✅ Active'\n    ELSE '❌ Inactive'\n  END AS status_label\nFROM members;",
        },
        "CTE (WITH Clause)": {
            "explain": (
                "🧒 **Writing a recipe step on a sticky note so you can use "
                "it later in the bigger recipe.** A CTE is a temporary named "
                "result set that makes complex queries readable."
            ),
            "example": "WITH active_members AS (\n  SELECT * FROM members\n  WHERE eligibility_status = 'ACTIVE'\n)\nSELECT * FROM active_members;",
        },
        "Window Functions (OVER)": {
            "explain": (
                "🧒 **Looking out a window at the whole playground while still "
                "standing at your own spot.** Window functions calculate something "
                "across multiple rows without collapsing them into groups."
            ),
            "example": "SELECT claim_number, member_responsibility,\n  SUM(member_responsibility) OVER (\n    ORDER BY service_date\n  ) AS running_total\nFROM claim_records;",
        },
        "SUM() OVER (Running Total)": {
            "explain": (
                "🧒 **Adding coins to your piggy bank one by one and writing "
                "down the total after each coin.** The running total keeps a "
                "cumulative sum as it moves through the rows."
            ),
            "example": "SUM(amount) OVER (\n  ORDER BY date\n  ROWS BETWEEN UNBOUNDED PRECEDING\n    AND CURRENT ROW\n) AS running_total",
        },
        "ROW_NUMBER / RANK / PERCENT_RANK": {
            "explain": (
                "🧒 **Giving each kid a number in the race.** ROW_NUMBER "
                "gives unique numbers (1, 2, 3). RANK gives the same number "
                "to ties (1, 1, 3). PERCENT_RANK shows your position as a "
                "percentage (0.0 to 1.0)."
            ),
            "example": "SELECT member_id,\n  RANK() OVER (ORDER BY current_amount DESC) AS spending_rank\nFROM accumulator_snapshots;",
        },
        "LAG / LEAD": {
            "explain": (
                "🧒 **Looking at the kid behind you (LAG) or ahead of you "
                "(LEAD) in line.** These functions let you access a row's "
                "neighbor without a self-join."
            ),
            "example": "SELECT file_name, expected_date,\n  LAG(expected_date) OVER (ORDER BY expected_date)\n    AS previous_date\nFROM file_inventory;",
        },
        "NTILE (Bucketing)": {
            "explain": (
                "🧒 **Dividing the class into 4 equal teams.** NTILE(4) "
                "assigns each row to one of 4 buckets (quartiles). "
                "NTILE(10) creates deciles."
            ),
            "example": "SELECT member_id,\n  NTILE(4) OVER (ORDER BY current_amount)\n    AS spending_quartile\nFROM accumulator_snapshots;",
        },
        "PARTITION BY": {
            "explain": (
                "🧒 **Doing the race separately for boys and girls.** "
                "PARTITION BY restarts the window function for each group, "
                "like a GROUP BY but without collapsing rows."
            ),
            "example": "ROW_NUMBER() OVER (\n  PARTITION BY family_id\n  ORDER BY date_of_birth\n)",
        },
        "COALESCE": {
            "explain": (
                "🧒 **If the cookie jar is empty, eat an apple instead.** "
                "COALESCE returns the first non-NULL value from a list. "
                "Great for providing defaults."
            ),
            "example": "SELECT COALESCE(resolved_at, 'Still open') AS resolution\nFROM support_cases;",
        },
        "NULLIF": {
            "explain": (
                "🧒 **If the answer is zero, pretend there's no answer.** "
                "NULLIF returns NULL if two values are equal. Used to "
                "prevent division by zero."
            ),
            "example": "SELECT current_amount / NULLIF(limit_amount, 0)\nFROM accumulator_snapshots;",
        },
        "JULIANDAY (Date Math)": {
            "explain": (
                "🧒 **Counting how many days between two calendar dates.** "
                "JULIANDAY converts a date to a number so you can subtract "
                "dates. Multiply by 24 for hours, by 1440 for minutes."
            ),
            "example": "SELECT\n  ROUND((JULIANDAY('now') -\n         JULIANDAY(opened_at)) * 24, 1)\n    AS hours_elapsed\nFROM support_cases;",
        },
        "STRFTIME (Date Formatting)": {
            "explain": (
                "🧒 **Writing the date as 'June 2025' instead of '2025-06-15'.** "
                "STRFTIME reformats dates. %Y = year, %m = month, %d = day."
            ),
            "example": "SELECT STRFTIME('%Y-%m', service_date) AS month\nFROM claim_records;",
        },
        "UNION ALL": {
            "explain": (
                "🧒 **Stacking two lists on top of each other.** "
                "UNION ALL combines results from multiple queries. "
                "UNION (without ALL) removes duplicates."
            ),
            "example": "SELECT 'members' AS tbl, COUNT(*) FROM members\nUNION ALL\nSELECT 'claims', COUNT(*) FROM claims;",
        },
        "CROSS JOIN": {
            "explain": (
                "🧒 **Every kid shaking hands with every other kid.** "
                "CROSS JOIN combines every row from one table with every "
                "row from another. Used carefully with single-row CTEs."
            ),
            "example": "SELECT * FROM ordered_claims\nCROSS JOIN oop_limit;",
        },
        "Scalar Subquery": {
            "explain": (
                "🧒 **Asking a quick question inside a bigger question.** "
                "A scalar subquery returns exactly one value and can be "
                "used inside SELECT, WHERE, or HAVING."
            ),
            "example": "SELECT *,\n  (SELECT COUNT(*) FROM claims\n   WHERE member_id = m.member_id) AS claim_count\nFROM members m;",
        },
        "Correlated Subquery": {
            "explain": (
                "🧒 **For each kid, asking a question that depends on THAT "
                "specific kid.** A correlated subquery references the outer "
                "query's row — it re-runs for each outer row."
            ),
            "example": "SELECT * FROM members m\nWHERE EXISTS (\n  SELECT 1 FROM claims cr\n  WHERE cr.member_id = m.member_id\n);",
        },
        "Anti-Join (LEFT JOIN + IS NULL)": {
            "explain": (
                "🧒 **Finding kids who DON'T have a lunchbox.** A LEFT JOIN "
                "followed by WHERE right_table.id IS NULL finds rows in the "
                "left table with NO match on the right."
            ),
            "example": "SELECT m.* FROM members m\nLEFT JOIN claims cr\n  ON m.member_id = cr.member_id\nWHERE cr.claim_record_id IS NULL;",
        },
        "GROUP_CONCAT": {
            "explain": (
                "🧒 **Listing all your friends' names on one line, separated "
                "by commas.** GROUP_CONCAT combines multiple values from a "
                "group into a single comma-separated string."
            ),
            "example": "SELECT family_id,\n  GROUP_CONCAT(first_name, ', ') AS family_members\nFROM members\nGROUP BY family_id;",
        },
        "Recursive CTE": {
            "explain": (
                "🧒 **A set of dominoes — each one knocks over the next.** "
                "A recursive CTE calls itself to build sequences like date "
                "series, hierarchies, or paths. It has an anchor (first domino) "
                "and a recursive step (each next domino)."
            ),
            "example": "WITH RECURSIVE dates AS (\n  SELECT DATE('2025-01-01') AS d\n  UNION ALL\n  SELECT DATE(d, '+1 day') FROM dates\n  WHERE d < '2025-01-07'\n)\nSELECT * FROM dates;",
        },
        "Conditional Aggregation (Pivot)": {
            "explain": (
                "🧒 **Making a tally chart where each column is a different "
                "category.** Instead of GROUP BY to create rows, you use "
                "SUM(CASE WHEN ... THEN 1 ELSE 0 END) to create columns."
            ),
            "example": "SELECT member_id,\n  SUM(CASE WHEN claim_status='PROCESSED' THEN 1 ELSE 0 END) AS processed,\n  SUM(CASE WHEN claim_status='VOIDED' THEN 1 ELSE 0 END) AS voided\nFROM claim_records\nGROUP BY member_id;",
        },
        "ABS (Absolute Value)": {
            "explain": (
                "🧒 **Whether you walk 5 steps forward or 5 steps backward, "
                "you still walked 5 steps.** ABS removes the negative sign "
                "from a number. Used in drift detection to catch both over "
                "and under discrepancies."
            ),
            "example": "SELECT ABS(accumulator_value - claims_value) AS drift\nFROM comparison;",
        },
        "PRAGMA (Schema Inspection)": {
            "explain": (
                "🧒 **Reading the instruction manual for a table.** "
                "PRAGMA table_info shows you every column, its type, "
                "whether it's required, and if it's the primary key."
            ),
            "example": "PRAGMA table_info(members);",
        },
    }

    for concept_name, details in concepts.items():
        with st.expander(f"**{concept_name}**"):
            st.markdown(details["explain"])
            st.code(details["example"], language="sql")


with tab_how:
    st.subheader("❓ How It Works")

    st.markdown("""
    ### 🧒 The Simple Version

    **Imagine you have a giant filing cabinet full of health insurance papers.**

    Each drawer is a **table** (members, claims, files, accumulators).
    Each folder in a drawer is a **row** (one member, one claim).
    Each label on a folder is a **column** (name, amount, date).

    **SQL is like asking a librarian a question:**
    - *"Show me all the folders for kids in the Johnson family"* → that's a SELECT with WHERE
    - *"Count how many folders are in each drawer"* → that's GROUP BY with COUNT
    - *"Match each kid's folder with their doctor visit folders"* → that's a JOIN
    - *"Add up all their spending, visit by visit"* → that's a running total with a window function

    This workbench lets you **ask any question** about the filing cabinet — and see the answer instantly.

    ---

    ### 🏗️ What's in This Workbench

    | Tab | What It Does | Who It's For |
    |-----|-------------|--------------|
    | 📚 Guided Queries | 28 pre-built queries with explanations | Everyone — start here |
    | ⌨️ SQL Sandbox | Free-form query editor | Anyone who wants to explore |
    | 🏆 SQL Challenges | Write-your-own-SQL exercises with solutions | People practicing SQL skills |
    | 🗂️ Schema Explorer | Table descriptions, columns, sample data | Anyone who needs to understand the data model |
    | 💡 SQL Concepts | Plain-English explanations of every SQL concept used | Learners and interview prep |

    ---

    ### 🔒 Safety

    - **Read-only:** The sandbox only allows SELECT queries. You cannot modify data.
    - **Row limit:** Results are capped at 500 rows to keep things fast.
    - **Live data:** Queries run against the same database that powers all the dashboards.
      When you run scenarios, the data changes — and your query results change too.

    ---

    ### 📊 The 16 Tables at a Glance

    ```
    WHO:    clients → vendors → members → benefit_plans → eligibility_periods
    WHAT:   file_inventory → processing_runs → claim_records → accumulator_snapshots
    ISSUES: data_quality_issues → support_cases → sla_tracking
    CONFIG: file_schedules, client_vendor_relationships, vendor_contacts
    ```

    ---

    ### 💡 Tips for Getting the Most Out of This Workbench

    1. **Start with Guided Queries** — run them, read the "What to Look For," and modify them
    2. **Use the Schema Explorer** to understand what columns are available before writing your own
    3. **Try the Challenges** — write your own SQL before revealing the solution
    4. **Run Scenarios first** — the queries are more interesting when there's incident data to analyze
    5. **Modify guided queries** — change the WHERE clause, add columns, try different aggregations
    """)

    st.caption(f"Page last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")