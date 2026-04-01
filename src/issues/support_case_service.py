import logging
from datetime import datetime
from src.common.db import db_session, fetch_all, fetch_one, execute, execute_insert
from src.common.datetime_utils import utc_now_iso
from src.sla.sla_service import create_sla_for_case

logger = logging.getLogger(__name__)


AUTO_CASE_ISSUE_CODES = {
    "MISSING_INBOUND_FILE": {"case_type": "MISSING_INBOUND_FILE", "priority": "CRITICAL"},
    "DUPLICATE_ELIGIBILITY_RESEND": {"case_type": "DUPLICATE_ELIGIBILITY_RESEND", "priority": "MEDIUM"},
    "CLAIM_INELIGIBLE_MEMBER": {"case_type": "CLAIM_INELIGIBLE_MEMBER", "priority": "HIGH"},
    "ACCUMULATOR_EXCEEDS_OOP_MAX": {"case_type": "ACCUMULATOR_EXCEEDS_OOP_MAX", "priority": "HIGH"},
    "FAMILY_ROLLUP_DISCREPANCY": {"case_type": "FAMILY_ROLLUP_DISCREPANCY", "priority": "HIGH"},
}

SEVERITY_TO_PRIORITY = {
    "CRITICAL": "CRITICAL",
    "HIGH": "HIGH",
    "MEDIUM": "MEDIUM",
    "LOW": "LOW",
}


def determine_assignment_team(issue_code: str | None, issue_type: str | None) -> str:
    if issue_code == "MISSING_INBOUND_FILE":
        return "ops_file_queue"
    if issue_code == "DUPLICATE_ELIGIBILITY_RESEND":
        return "ops_eligibility_queue"
    if issue_code in ("CLAIM_INELIGIBLE_MEMBER",):
        return "ops_claims_queue"
    if issue_code in ("ACCUMULATOR_EXCEEDS_OOP_MAX", "FAMILY_ROLLUP_DISCREPANCY"):
        return "ops_recon_queue"

    if issue_type:
        issue_type_upper = issue_type.upper()
        if "FILE" in issue_type_upper:
            return "ops_file_queue"
        if "ELIGIBILITY" in issue_type_upper:
            return "ops_eligibility_queue"
        if "CLAIM" in issue_type_upper or "ACCUM" in issue_type_upper:
            return "ops_claims_queue"

    return "ops_triage_queue"


def should_create_case(issue: dict) -> bool:
    if issue["status"] != "OPEN":
        return False

    issue_code = issue.get("issue_code") or issue.get("issue_subtype")
    severity = (issue.get("severity") or "").upper()

    if issue_code in AUTO_CASE_ISSUE_CODES:
        return True

    return severity in ("CRITICAL", "HIGH")


def build_case_payload(issue: dict) -> dict:
    issue_code = issue.get("issue_code") or issue.get("issue_subtype")
    now_ts = utc_now_iso()

    mapped = AUTO_CASE_ISSUE_CODES.get(issue_code, {})
    case_type = mapped.get("case_type", issue_code or issue.get("issue_type") or "GENERAL_DATA_ISSUE")
    priority = mapped.get("priority", SEVERITY_TO_PRIORITY.get((issue.get("severity") or "").upper(), "MEDIUM"))
    severity = issue.get("severity") or "HIGH"
    assigned_team = determine_assignment_team(issue_code, issue.get("issue_type"))

    title = f"{case_type}: {issue.get('entity_key') or issue.get('member_id') or issue.get('file_id') or issue['issue_id']}"
    description = issue.get("issue_message") or issue.get("issue_description") or "Auto-generated support case from data quality issue"
    case_number = f"CASE-{issue['issue_id']}-{int(datetime.now().timestamp())}"

    return {
        "issue_id": issue["issue_id"],
        "case_number": case_number,
        "client_id": issue.get("client_id"),
        "vendor_id": issue.get("vendor_id"),
        "file_id": issue.get("file_id"),
        "run_id": issue.get("run_id"),
        "member_id": issue.get("member_id"),
        "claim_record_id": issue.get("claim_record_id"),
        "benefit_year": issue.get("benefit_year"),
        "case_type": case_type,
        "priority": priority,
        "severity": severity,
        "status": "OPEN",
        "assigned_team": assigned_team,
        "assigned_to": None,
        "escalation_level": 1 if priority == "CRITICAL" else 0,
        "source_system": "SYSTEM",
        "title": title,
        "description": description,
        "root_cause_category": None,
        "resolution_summary": None,
        "opened_at": now_ts,
        "acknowledged_at": None,
        "resolved_at": None,
        "closed_at": None,
        "created_at": now_ts,
        "updated_at": now_ts,
    }


def find_existing_open_case(conn, payload: dict):
    return conn.execute(
        """
        SELECT case_id
        FROM support_cases
        WHERE status IN ('OPEN', 'ACKNOWLEDGED', 'IN_PROGRESS', 'ESCALATED')
          AND case_type = ?
          AND COALESCE(file_id, -1) = COALESCE(?, -1)
          AND COALESCE(member_id, 'NA') = COALESCE(?, 'NA')
          AND COALESCE(claim_record_id, -1) = COALESCE(?, -1)
          AND COALESCE(issue_id, -1) = COALESCE(?, -1)
        LIMIT 1
        """,
        (
            payload["case_type"],
            payload["file_id"],
            payload["member_id"],
            payload["claim_record_id"],
            payload["issue_id"],
        ),
    ).fetchone()


def create_support_case_from_issue(issue: dict, conn=None) -> int | None:
    if not should_create_case(issue):
        return None

    payload = build_case_payload(issue)

    if conn is None:
        with db_session() as conn:
            return _create_support_case_from_issue(payload, conn)
    else:
        return _create_support_case_from_issue(payload, conn)


def _create_support_case_from_issue(payload: dict, conn) -> int | None:
    existing = find_existing_open_case(conn, payload)
    if existing:
        return existing["case_id"]

    cursor = conn.execute(
        """
        INSERT INTO support_cases (
            issue_id, case_number, client_id, vendor_id, file_id, run_id,
            member_id, claim_record_id, benefit_year,
            case_type, priority, severity, status,
            assigned_team, assigned_to, escalation_level,
            source_system, short_description, description,
            root_cause_category, resolution_summary,
            opened_at, acknowledged_at, resolved_at, closed_at,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["issue_id"],
            payload["case_number"],
            payload["client_id"],
            payload["vendor_id"],
            payload["file_id"],
            payload["run_id"],
            payload["member_id"],
            payload["claim_record_id"],
            payload["benefit_year"],
            payload["case_type"],
            payload["priority"],
            payload["severity"],
            payload["status"],
            payload["assigned_team"],
            payload["assigned_to"],
            payload["escalation_level"],
            payload["source_system"],
            payload["title"],          # maps to short_description column
            payload["description"],
            payload["root_cause_category"],
            payload["resolution_summary"],
            payload["opened_at"],
            payload["acknowledged_at"],
            payload["resolved_at"],
            payload["closed_at"],
            payload["created_at"],
            payload["updated_at"],
        ),
    )
    case_id = cursor.lastrowid

    conn.execute(
        """
        INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            "SUPPORT_CASE_CREATED",
            "support_case",
            str(case_id),
            "system",
            f"Created from issue_id={payload['issue_id']} case_type={payload['case_type']} priority={payload['priority']}",
        ),
    )

    if case_id:
        create_sla_for_case(int(case_id), payload["priority"], payload["case_type"], conn)
    return case_id


def create_support_cases_from_open_issues(conn=None) -> int:
    if conn is None:
        issues = fetch_all("""
            SELECT *
            FROM data_quality_issues
            WHERE status = 'OPEN'
            ORDER BY detected_at ASC, issue_id ASC
        """)
        created = 0
        for issue in issues:
            case_id = create_support_case_from_issue(issue)
            if case_id:
                created += 1
        return created
    else:
        issues = conn.execute("""
            SELECT *
            FROM data_quality_issues
            WHERE status = 'OPEN'
            ORDER BY detected_at ASC, issue_id ASC
        """).fetchall()
        issues = [dict(row) for row in issues]
        created = 0
        for issue in issues:
            case_id = create_support_case_from_issue(issue, conn)
            if case_id:
                created += 1
        return created


def update_case_status(case_id: int, new_status: str, actor: str = "analyst", resolution_summary: str | None = None) -> None:
    now_ts = utc_now_iso()

    resolved_at = now_ts if new_status == "RESOLVED" else None
    closed_at = now_ts if new_status == "CLOSED" else None
    acknowledged_at = now_ts if new_status == "ACKNOWLEDGED" else None

    with db_session() as conn:
        conn.execute(
            """
            UPDATE support_cases
            SET status = ?,
                acknowledged_at = COALESCE(acknowledged_at, ?),
                resolved_at = COALESCE(resolved_at, ?),
                closed_at = COALESCE(closed_at, ?),
                resolution_summary = COALESCE(?, resolution_summary),
                updated_at = ?
            WHERE case_id = ?
            """,
            (new_status, acknowledged_at, resolved_at, closed_at,
             resolution_summary, now_ts, case_id),
        )

        conn.execute(
            """
            INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("SUPPORT_CASE_STATUS_UPDATED", "support_case", str(case_id),
             actor, f"Status changed to {new_status}"),
        )


def escalate_breached_cases() -> int:
    now_ts = utc_now_iso()
    with db_session() as conn:
        rows = conn.execute("""
            SELECT sc.case_id
            FROM support_cases sc
            JOIN sla_tracking st ON st.case_id = sc.case_id
            WHERE st.is_breached = 1
              AND sc.status NOT IN ('RESOLVED', 'CLOSED', 'ESCALATED')
        """).fetchall()

        for row in rows:
            conn.execute(
                """
                UPDATE support_cases
                SET status = 'ESCALATED',
                    escalation_level = escalation_level + 1,
                    updated_at = ?
                WHERE case_id = ?
                """,
                (now_ts, row["case_id"]),
            )
            conn.execute(
                """
                INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("SUPPORT_CASE_ESCALATED", "support_case", str(row["case_id"]),
                 "system", "Escalated due to SLA breach"),
            )

        return len(rows)


def assign_case(case_id: int, assigned_to: str, actor: str = "operator"):
    """Assign a support case to an operator."""
    now_ts = utc_now_iso()
    execute(
        "UPDATE support_cases SET assigned_to = ?, updated_at = ? WHERE case_id = ?",
        (assigned_to, now_ts, case_id)
    )
    # Assume audit_log exists
    try:
        execute(
            "INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details) VALUES (?, ?, ?, ?, ?)",
            ("CASE_ASSIGNED", "support_case", str(case_id), actor, f"Assigned to {assigned_to}")
        )
    except Exception as exc:
        logger.warning(f"Failed to log audit for case assignment: {exc}")


def add_case_note(case_id: int, note: str, author: str = "operator"):
    """Add a note to a support case."""
    now_ts = utc_now_iso()
    execute_insert(
        "INSERT INTO case_notes (case_id, note, author, created_at) VALUES (?, ?, ?, ?)",
        (case_id, note, author, now_ts)
    )


def resolve_case(case_id: int, resolution_note: str = "", actor: str = "operator"):
    """Mark a support case as resolved."""
    now_ts = utc_now_iso()
    execute(
        "UPDATE support_cases SET status = 'RESOLVED', resolved_at = ?, updated_at = ? WHERE case_id = ?",
        (now_ts, now_ts, case_id)
    )
    if resolution_note:
        add_case_note(case_id, f"Resolution: {resolution_note}", actor)
    try:
        execute(
            "INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details) VALUES (?, ?, ?, ?, ?)",
            ("CASE_RESOLVED", "support_case", str(case_id), actor, "Case marked as resolved")
        )
    except Exception as exc:
        logger.warning(f"Failed to log audit for case resolution: {exc}")


def escalate_case(case_id: int, reason: str = "", actor: str = "operator"):
    """Escalate a support case."""
    now_ts = utc_now_iso()
    # Increase priority if not already CRITICAL
    execute("""
        UPDATE support_cases
        SET status = 'ESCALATED',
            priority = CASE WHEN priority != 'CRITICAL' THEN 'HIGH' ELSE priority END,
            updated_at = ?
        WHERE case_id = ?
    """, (now_ts, case_id))
    if reason:
        add_case_note(case_id, f"Escalation: {reason}", actor)
    try:
        execute(
            "INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details) VALUES (?, ?, ?, ?, ?)",
            ("CASE_ESCALATED", "support_case", str(case_id), actor, f"Escalated: {reason}")
        )
    except Exception as exc:
        logger.warning(f"Failed to log audit for case escalation: {exc}")