from __future__ import annotations

from src.common.db import db_session, fetch_one
from src.common.datetime_utils import add_hours_iso, hours_elapsed, utc_now_iso


PRIORITY_TO_TARGET_HOURS = {
    "CRITICAL": 4,
    "HIGH": 8,
    "MEDIUM": 24,
    "LOW": 72,
}

CASE_TYPE_TO_TARGET_HOURS = {
    "MISSING_INBOUND_FILE": 4,
    "CLAIM_INELIGIBLE_MEMBER": 8,
    "ACCUMULATOR_EXCEEDS_OOP_MAX": 8,
    "FAMILY_ROLLUP_DISCREPANCY": 24,
    "DUPLICATE_ELIGIBILITY_RESEND": 24,
}


def determine_target_hours(priority: str | None, case_type: str | None) -> int:
    if case_type and case_type in CASE_TYPE_TO_TARGET_HOURS:
        return CASE_TYPE_TO_TARGET_HOURS[case_type]

    priority_normalized = (priority or "MEDIUM").upper()
    return PRIORITY_TO_TARGET_HOURS.get(priority_normalized, 24)


def get_case_details(case_id: int, conn=None) -> dict | None:
    query = """
        SELECT
            case_id,
            case_type,
            priority,
            status,
            opened_at,
            created_at,
            resolved_at,
            closed_at
        FROM support_cases
        WHERE case_id = ?
    """

    if conn is None:
        return fetch_one(query, (case_id,))
    row = conn.execute(query, (case_id,)).fetchone()
    return dict(row) if row else None


def find_existing_sla(case_id: int, conn=None):
    query = """
        SELECT
            sla_id,
            case_id,
            sla_type,
            target_hours,
            target_due_at,
            status,
            is_at_risk,
            is_breached,
            breached_at,
            last_evaluated_at,
            created_at,
            updated_at
        FROM sla_tracking
        WHERE case_id = ?
        ORDER BY sla_id DESC
        LIMIT 1
    """
    if conn is None:
        return fetch_one(query, (case_id,))
    row = conn.execute(query, (case_id,)).fetchone()
    return dict(row) if row else None


def create_sla_for_case(
    case_id: int,
    priority: str | None = None,
    case_type: str | None = None,
    conn=None,
) -> int | None:
    owns_connection = conn is None

    if owns_connection:
        with db_session() as conn:
            return create_sla_for_case(case_id, priority, case_type, conn)

    existing = find_existing_sla(case_id, conn)
    if existing:
        return existing["sla_id"]

    case_row = get_case_details(case_id, conn)
    if not case_row:
        raise RuntimeError(f"Cannot create SLA: support case {case_id} not found.")

    effective_priority = priority or case_row.get("priority")
    effective_case_type = case_type or case_row.get("case_type")
    opened_at = case_row.get("opened_at") or case_row.get("created_at")

    if not opened_at:
        raise RuntimeError(f"Cannot create SLA: support case {case_id} has no opened_at/created_at timestamp.")

    target_hours = determine_target_hours(effective_priority, effective_case_type)
    target_due_at = add_hours_iso(opened_at, target_hours)
    now_ts = utc_now_iso()

    cursor = conn.execute(
        """
        INSERT INTO sla_tracking (
            case_id,
            sla_type,
            target_hours,
            target_due_at,
            status,
            is_at_risk,
            is_breached,
            breached_at,
            last_evaluated_at,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            case_id,
            "CASE_RESOLUTION",
            target_hours,
            target_due_at,
            "OPEN",
            0,
            0,
            None,
            now_ts,
            now_ts,
            now_ts,
        ),
    )
    sla_id = cursor.lastrowid

    conn.execute(
        """
        INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            "SLA_CREATED",
            "sla_tracking",
            str(sla_id),
            "system",
            (
                f"Created SLA for case_id={case_id} "
                f"sla_type={effective_case_type} "
                f"target_hours={target_hours} "
                f"target_due_at={target_due_at}"
            ),
        ),
    )

    return sla_id


def evaluate_open_slas(conn=None) -> int:
    owns_connection = conn is None

    if owns_connection:
        with db_session() as conn:
            return evaluate_open_slas(conn)

    now_ts = utc_now_iso()

    rows = conn.execute(
        """
        SELECT
            st.sla_id,
            st.case_id,
            st.sla_type,
            st.target_hours,
            st.target_due_at,
            st.status AS sla_status,
            st.is_at_risk,
            st.is_breached,
            st.breached_at,
            sc.status AS case_status,
            sc.opened_at,
            sc.created_at,
            sc.resolved_at,
            sc.closed_at
        FROM sla_tracking st
        JOIN support_cases sc
          ON sc.case_id = st.case_id
        WHERE st.status IN ('OPEN', 'AT_RISK', 'BREACHED')
        ORDER BY st.sla_id ASC
        """
    ).fetchall()

    evaluated_count = 0

    for row in rows:
        row = dict(row)
        evaluated_count += 1

        case_status = row["case_status"]
        opened_at = row["opened_at"] or row["created_at"]
        target_hours = int(row["target_hours"]) if row["target_hours"] is not None else 24
        target_due_at = row["target_due_at"]

        new_status = row["sla_status"]
        is_at_risk = 0
        is_breached = row["is_breached"]
        breached_at = row["breached_at"]

        if case_status in ("RESOLVED", "CLOSED"):
            new_status = "CLOSED"
            is_at_risk = 0
            is_breached = row["is_breached"]
        else:
            elapsed = hours_elapsed(opened_at, now_ts)
            due_elapsed = hours_elapsed(opened_at, target_due_at) if opened_at and target_due_at else target_hours

            if due_elapsed <= 0:
                due_elapsed = target_hours

            if now_ts > target_due_at:
                new_status = "BREACHED"
                is_breached = 1
                is_at_risk = 0
                breached_at = breached_at or now_ts
            elif elapsed >= (0.8 * target_hours):
                new_status = "AT_RISK"
                is_at_risk = 1
                is_breached = 0
                breached_at = None
            else:
                new_status = "OPEN"
                is_at_risk = 0
                is_breached = 0
                breached_at = None

        conn.execute(
            """
            UPDATE sla_tracking
            SET status = ?,
                is_at_risk = ?,
                is_breached = ?,
                breached_at = ?,
                last_evaluated_at = ?,
                updated_at = ?
            WHERE sla_id = ?
            """,
            (
                new_status,
                is_at_risk,
                is_breached,
                breached_at,
                now_ts,
                now_ts,
                row["sla_id"],
            ),
        )

    return evaluated_count