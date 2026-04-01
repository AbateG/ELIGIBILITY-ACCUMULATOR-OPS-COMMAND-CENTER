from datetime import datetime, timedelta, UTC
import hashlib

from src.common.db import db_session, fetch_one, execute
from src.issues.run_case_generation import main as run_case_generation_main
from src.sla.sla_service import create_sla_for_case, evaluate_open_slas


SCENARIO_NAME = "DUPLICATE_ELIGIBILITY_RESEND"
DEFAULT_FILE_TYPE = "ELIGIBILITY"
DEFAULT_SEVERITY = "MEDIUM"
DEFAULT_RUN_TYPE = "FILE_VALIDATION"


def _utcnow_iso():
    return datetime.now(UTC).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")


def _today_date():
    return datetime.now(UTC).date().isoformat()


def _make_file_hash(client_id, vendor_id, expected_date):
    payload = f"eligibility|{client_id}|{vendor_id}|{expected_date}|DUPLICATE_RESEND"
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _get_default_client_vendor():
    client = fetch_one("SELECT client_id, client_code FROM clients ORDER BY client_id LIMIT 1")

    if not client:
        now_ts = _utcnow_iso()
        with db_session() as conn:
            conn.execute(
                "INSERT INTO clients (client_id, client_code, client_name, active_flag, created_at) VALUES (?, ?, ?, ?, ?)",
                (1, "SCN_CLIENT", "Scenario Client", 1, now_ts),
            )
        client = fetch_one("SELECT client_id, client_code FROM clients WHERE client_id = ?", (1,))

    vendor = fetch_one("SELECT vendor_id, vendor_code FROM vendors ORDER BY vendor_id LIMIT 1")

    if not vendor:
        now_ts = _utcnow_iso()
        with db_session() as conn:
            conn.execute(
                "INSERT INTO vendors (vendor_id, vendor_code, vendor_name, vendor_type, active_flag, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (1, "SCN_VENDOR", "Scenario Vendor", "TPA", 1, now_ts),
            )
        vendor = fetch_one("SELECT vendor_id, vendor_code FROM vendors WHERE vendor_id = ?", (1,))

    return client, vendor


def _find_existing_open_issue(client_id, vendor_id, expected_date, file_hash):
    entity_key = f"{expected_date}|{file_hash}"
    return fetch_one(
        """
        SELECT issue_id, file_id, run_id
        FROM data_quality_issues
        WHERE issue_code = 'DUPLICATE_ELIGIBILITY_RESEND'
          AND client_id = ?
          AND vendor_id = ?
          AND status = 'OPEN'
          AND entity_key = ?
        ORDER BY issue_id DESC
        LIMIT 1
        """,
        (client_id, vendor_id, entity_key),
    )


def _insert_inbound_file(file_name, client_id, vendor_id, expected_date,
                          received_ts, file_hash, processing_status,
                          duplicate_flag, error_count):
    now_ts = _utcnow_iso()
    with db_session() as conn:
        cur = conn.execute(
            """
            INSERT INTO inbound_files (
                file_name, file_type, client_id, vendor_id, expected_date,
                received_ts, file_hash, row_count, processing_status,
                duplicate_flag, error_count, landing_path, archived_path, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_name, DEFAULT_FILE_TYPE, client_id, vendor_id, expected_date,
                received_ts, file_hash, 250, processing_status,
                duplicate_flag, error_count, f"/landing/{file_name}", None, now_ts,
            ),
        )
        return cur.lastrowid


def _insert_processing_run(file_id, note, issue_count):
    started_at = _utcnow_iso()
    completed_at = _utcnow_iso()
    with db_session() as conn:
        cur = conn.execute(
            """
            INSERT INTO processing_runs (
                run_type, file_id, started_at, completed_at, run_status,
                rows_read, rows_passed, rows_failed, issue_count, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                DEFAULT_RUN_TYPE, file_id, started_at, completed_at, "COMPLETED",
                250, 249 if issue_count else 250,
                1 if issue_count else 0, issue_count, note,
            ),
        )
        return cur.lastrowid


def _insert_duplicate_issue(client_id, vendor_id, file_id, run_id, expected_date, file_hash):
    detected_at = _utcnow_iso()
    entity_key = f"{expected_date}|{file_hash}"
    description = (
        f"Duplicate eligibility resend detected for expected_date={expected_date}. "
        f"File hash matched a previously received eligibility file."
    )
    message = (
        f"Duplicate eligibility resend for client_id={client_id}, vendor_id={vendor_id}, "
        f"expected_date={expected_date}, file_hash={file_hash}."
    )

    with db_session() as conn:
        cur = conn.execute(
            """
            INSERT INTO data_quality_issues (
                issue_type, issue_subtype, issue_code, issue_message,
                severity, status, client_id, vendor_id, file_id, run_id,
                member_id, claim_record_id, entity_name, entity_key,
                source_row_number, issue_description,
                detected_at, resolved_at, resolution_notes,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "FILE_MONITORING", "DUPLICATE_FILE",
                "DUPLICATE_ELIGIBILITY_RESEND", message,
                DEFAULT_SEVERITY, "OPEN",
                client_id, vendor_id, file_id, run_id,
                None, None, "inbound_files", entity_key,
                None, description,
                detected_at, None, None,
                detected_at, detected_at,
            ),
        )
        return cur.lastrowid


def _insert_audit_log(event_type, entity_name, entity_id, message):
    now_ts = _utcnow_iso()
    try:
        execute(
            """
            INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event_type, entity_name, str(entity_id), "system", message),
        )
    except Exception:
        pass


def run(expected_date=None):
    expected_date = expected_date or _today_date()

    client, vendor = _get_default_client_vendor()
    client_id = client["client_id"]
    vendor_id = vendor["vendor_id"]

    file_hash = _make_file_hash(client_id, vendor_id, expected_date)

    existing_issue = _find_existing_open_issue(client_id, vendor_id, expected_date, file_hash)
    if existing_issue:
        print("Scenario already active. Existing open duplicate-resend issue found.")
        result = {
            "issue_id": existing_issue["issue_id"],
            "file_id": existing_issue["file_id"],
            "run_id": existing_issue["run_id"],
            "client_id": client_id,
            "vendor_id": vendor_id,
            "expected_date": expected_date,
            "file_hash": file_hash,
            "duplicate_file_id": existing_issue["file_id"],
        }
        print(result)
        return result

    original_received_ts = _utcnow_iso()
    duplicate_received_ts = _utcnow_iso()

    original_file_name = f"eligibility_original_{client_id}_{vendor_id}_{expected_date}.csv"
    duplicate_file_name = f"eligibility_resend_duplicate_{client_id}_{vendor_id}_{expected_date}.csv"

    original_file_id = _insert_inbound_file(
        file_name=original_file_name, client_id=client_id, vendor_id=vendor_id,
        expected_date=expected_date, received_ts=original_received_ts,
        file_hash=file_hash, processing_status="VALIDATED",
        duplicate_flag=0, error_count=0,
    )

    _insert_processing_run(
        file_id=original_file_id,
        note="Scenario loader created original eligibility file receipt.",
        issue_count=0,
    )

    duplicate_file_id = _insert_inbound_file(
        file_name=duplicate_file_name, client_id=client_id, vendor_id=vendor_id,
        expected_date=expected_date, received_ts=duplicate_received_ts,
        file_hash=file_hash, processing_status="REJECTED",
        duplicate_flag=1, error_count=1,
    )

    duplicate_run_id = _insert_processing_run(
        file_id=duplicate_file_id,
        note="Scenario loader created duplicate eligibility resend validation run.",
        issue_count=1,
    )

    issue_id = _insert_duplicate_issue(
        client_id=client_id, vendor_id=vendor_id,
        file_id=duplicate_file_id, run_id=duplicate_run_id,
        expected_date=expected_date, file_hash=file_hash,
    )

    _insert_audit_log("SCENARIO_LOADED", "data_quality_issues", issue_id,
                       f"Loaded deterministic scenario: {SCENARIO_NAME}")

    try:
        run_case_generation_main()
    except TypeError:
        run_case_generation_main()

    created_case = fetch_one(
        "SELECT case_id, priority, status, assigned_team FROM support_cases WHERE issue_id = ? ORDER BY case_id DESC LIMIT 1",
        (issue_id,),
    )

    if created_case:
        try:
            create_sla_for_case(created_case["case_id"])
        except Exception:
            pass

    try:
        evaluate_open_slas()
    except Exception:
        pass

    created_sla = None
    if created_case:
        created_sla = fetch_one(
            "SELECT sla_id, sla_type, target_hours, target_due_at, status, is_at_risk, is_breached FROM sla_tracking WHERE case_id = ? ORDER BY sla_id DESC LIMIT 1",
            (created_case["case_id"],),
        )

    summary = {
        "scenario_name": SCENARIO_NAME,
        "client_id": client_id,
        "client_code": client["client_code"],
        "vendor_id": vendor_id,
        "vendor_code": vendor["vendor_code"],
        "expected_date": expected_date,
        "file_hash": file_hash,
        "original_file_id": original_file_id,
        "duplicate_file_id": duplicate_file_id,
        "duplicate_run_id": duplicate_run_id,
        "issue_id": issue_id,
        "case": dict(created_case) if created_case else None,
        "sla": dict(created_sla) if created_sla else None,
    }

    print("Scenario loaded successfully:")
    print(summary)
    return summary


if __name__ == "__main__":
    run()