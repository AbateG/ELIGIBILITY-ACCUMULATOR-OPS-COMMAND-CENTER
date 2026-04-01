import argparse
from datetime import datetime, timedelta, UTC

from src.common.db import db_session, fetch_one, execute
from src.issues.run_case_generation import main as run_case_generation_main
from src.sla.sla_service import create_sla_for_case, evaluate_open_slas


SCENARIO_NAME = "MISSING_INBOUND_FILE"
DEFAULT_FILE_TYPE = "ELIGIBILITY"
DEFAULT_SEVERITY = "HIGH"
DEFAULT_RUN_TYPE = "FILE_MONITORING"


def _utcnow_iso():
    return datetime.now(UTC).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")


def _yesterday_date():
    return (datetime.now(UTC).date() - timedelta(days=1)).isoformat()


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


def _find_existing_open_issue(client_id, vendor_id, expected_date):
    return fetch_one(
        """
        SELECT issue_id, file_id, run_id
        FROM data_quality_issues
        WHERE issue_code = 'MISSING_INBOUND_FILE'
          AND client_id = ?
          AND vendor_id = ?
          AND status = 'OPEN'
          AND entity_key = ?
        ORDER BY issue_id DESC
        LIMIT 1
        """,
        (client_id, vendor_id, expected_date),
    )


def _insert_expected_missing_file(client_id, vendor_id, expected_date):
    file_name = f"eligibility_expected_missing_{client_id}_{vendor_id}_{expected_date}.csv"
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
                None, None, 0, "RECEIVED", 0, 1, None, None, now_ts,
            ),
        )
        return cur.lastrowid


def _insert_monitoring_run(file_id):
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
                DEFAULT_RUN_TYPE, file_id, started_at, completed_at, "SUCCESS",
                0, 0, 1, 1,
                "Scenario loader created monitoring run for missing expected inbound file.",
            ),
        )
        return cur.lastrowid


def _insert_missing_file_issue(client_id, vendor_id, file_id, run_id, expected_date):
    description = (
        f"Expected daily eligibility file was not received for expected_date={expected_date}."
    )
    message = (
        f"Missing inbound eligibility file for client_id={client_id}, vendor_id={vendor_id}, "
        f"expected_date={expected_date}."
    )
    detected_at = _utcnow_iso()

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
                "FILE_MONITORING", "MISSING_EXPECTED_FILE",
                "MISSING_INBOUND_FILE", message,
                DEFAULT_SEVERITY, "OPEN",
                client_id, vendor_id, file_id, run_id,
                None, None, "inbound_files", expected_date,
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
    expected_date = expected_date or _yesterday_date()

    client, vendor = _get_default_client_vendor()
    client_id = client["client_id"]
    vendor_id = vendor["vendor_id"]

    existing_issue = _find_existing_open_issue(client_id, vendor_id, expected_date)
    if existing_issue:
        print("Scenario already active. Existing open missing-file issue found.")
        result = {
            "issue_id": existing_issue["issue_id"],
            "file_id": existing_issue["file_id"],
            "run_id": existing_issue["run_id"],
            "client_id": client_id,
            "vendor_id": vendor_id,
            "expected_date": expected_date,
        }
        print(result)
        return result

    file_id = _insert_expected_missing_file(client_id, vendor_id, expected_date)
    run_id = _insert_monitoring_run(file_id)
    issue_id = _insert_missing_file_issue(client_id, vendor_id, file_id, run_id, expected_date)

    _insert_audit_log("SCENARIO_LOADED", "data_quality_issues", issue_id,
                       f"Loaded deterministic scenario: {SCENARIO_NAME}")

    try:
        run_case_generation_main()
    except TypeError:
        run_case_generation_main()

    created_case = fetch_one(
        "SELECT case_id, priority, status, assigned_team, case_type FROM support_cases WHERE issue_id = ? ORDER BY case_id DESC LIMIT 1",
        (issue_id,),
    )

    if created_case:
        try:
            create_sla_for_case(created_case["case_id"], created_case["priority"], created_case["case_type"])
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
        "file_id": file_id,
        "run_id": run_id,
        "issue_id": issue_id,
        "case": dict(created_case) if created_case else None,
        "sla": dict(created_sla) if created_sla else None,
    }

    print("Scenario loaded successfully:")
    print(summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run missing inbound file scenario")
    parser.add_argument('--expected-date', default=None, help='Expected date for the missing file (YYYY-MM-DD)')
    args = parser.parse_args()
    run(args.expected_date)