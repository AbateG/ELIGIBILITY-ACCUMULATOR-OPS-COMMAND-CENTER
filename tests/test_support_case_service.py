import pytest
import os
import tempfile
from pathlib import Path

from src.common.db import db_session, fetch_all, fetch_one
from src.issues.support_case_service import (
    create_support_case_from_issue,
    create_support_cases_from_open_issues,
    update_case_status,
    escalate_breached_cases,
    determine_assignment_team,
    should_create_case,
    build_case_payload,
)
from src.common.datetime_utils import utc_now_iso


@pytest.fixture
def test_db():
    """Create a temporary DB using canonical init_database for schema consistency."""
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)
    db_path = Path(db_path)

    old_db_path = os.environ.get("SQLITE_DB_PATH")
    os.environ["SQLITE_DB_PATH"] = str(db_path)

    from src.db.init_db import init_database
    init_database(reset=True)

    with db_session() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO clients (client_code, client_name) VALUES ('ACME', 'ACME Corp')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO vendors (vendor_code, vendor_name, vendor_type) VALUES ('TPA1', 'TPA One', 'TPA')"
        )

    yield db_path

    if old_db_path:
        os.environ["SQLITE_DB_PATH"] = old_db_path
    else:
        os.environ.pop("SQLITE_DB_PATH", None)

    db_path.unlink(missing_ok=True)


def insert_test_issue(**kwargs):
    defaults = {
        "issue_code": "TEST_ISSUE",
        "issue_type": "SCHEMA",
        "issue_message": "Test issue message",
        "issue_description": "Test issue",
        "severity": "HIGH",
        "status": "OPEN",
        "client_id": 1,
        "vendor_id": 1,
        "file_id": None,
        "run_id": None,
        "member_id": None,
        "claim_record_id": None,
        "entity_name": "eligibility_periods",
        "entity_key": "1",
        "detected_at": utc_now_iso(),
        "resolved_at": None,
        "resolution_notes": None,
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }
    defaults.update(kwargs)

    with db_session() as conn:
        cursor = conn.execute(
            """
            INSERT INTO data_quality_issues (
                issue_code, issue_type, issue_message, issue_description,
                severity, status, client_id, vendor_id, file_id, run_id,
                member_id, claim_record_id, entity_name, entity_key,
                detected_at, resolved_at, resolution_notes,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                defaults["issue_code"], defaults["issue_type"],
                defaults["issue_message"], defaults["issue_description"],
                defaults["severity"], defaults["status"],
                defaults["client_id"], defaults["vendor_id"],
                defaults["file_id"], defaults["run_id"],
                defaults["member_id"], defaults["claim_record_id"],
                defaults["entity_name"], defaults["entity_key"],
                defaults["detected_at"], defaults["resolved_at"],
                defaults["resolution_notes"],
                defaults["created_at"], defaults["updated_at"],
            ),
        )
        return cursor.lastrowid


def test_high_severity_issue_creates_case(test_db):
    issue_id = insert_test_issue(severity="HIGH")
    issue = {
        "issue_id": issue_id, "severity": "HIGH",
        "status": "OPEN", "issue_type": "SCHEMA", "issue_code": "TEST_ISSUE",
    }
    case_id = create_support_case_from_issue(issue)
    assert case_id is not None

    with db_session() as conn:
        case = conn.execute(
            "SELECT * FROM support_cases WHERE case_id = ?", (case_id,)
        ).fetchone()
        assert case["priority"] == "HIGH"
        assert case["status"] == "OPEN"


def test_issue_code_creates_correct_case_type(test_db):
    issue_id = insert_test_issue(issue_code="MISSING_INBOUND_FILE", severity="CRITICAL")
    issue = {
        "issue_id": issue_id, "issue_code": "MISSING_INBOUND_FILE",
        "severity": "CRITICAL", "status": "OPEN", "issue_type": "FILE",
    }
    case_id = create_support_case_from_issue(issue)
    assert case_id is not None

    with db_session() as conn:
        case = conn.execute(
            "SELECT * FROM support_cases WHERE case_id = ?", (case_id,)
        ).fetchone()
        assert case["case_type"] == "MISSING_INBOUND_FILE"
        assert case["priority"] == "CRITICAL"


def test_rerun_does_not_create_duplicate_case(test_db):
    issue_id = insert_test_issue(severity="HIGH")
    issue = {
        "issue_id": issue_id, "severity": "HIGH",
        "status": "OPEN", "issue_type": "SCHEMA", "issue_code": "TEST_ISSUE",
    }
    case_id1 = create_support_case_from_issue(issue)
    case_id2 = create_support_case_from_issue(issue)
    assert case_id1 == case_id2

    with db_session() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM support_cases WHERE issue_id = ?", (issue_id,)
        ).fetchone()[0]
        assert count == 1


def test_assignment_team_correct(test_db):
    assert determine_assignment_team("MISSING_INBOUND_FILE", None) == "ops_file_queue"
    assert determine_assignment_team("DUPLICATE_ELIGIBILITY_RESEND", None) == "ops_eligibility_queue"
    assert determine_assignment_team("CLAIM_INELIGIBLE_MEMBER", None) == "ops_claims_queue"
    assert determine_assignment_team("ACCUMULATOR_EXCEEDS_OOP_MAX", None) == "ops_recon_queue"
    assert determine_assignment_team(None, "FILE") == "ops_file_queue"
    assert determine_assignment_team(None, None) == "ops_triage_queue"


def test_critical_case_sets_escalation_level(test_db):
    issue_id = insert_test_issue(severity="CRITICAL", issue_code="MISSING_INBOUND_FILE")
    issue = {
        "issue_id": issue_id, "severity": "CRITICAL",
        "status": "OPEN", "issue_type": "FILE",
        "issue_code": "MISSING_INBOUND_FILE",
    }
    case_id = create_support_case_from_issue(issue)

    with db_session() as conn:
        case = conn.execute(
            "SELECT * FROM support_cases WHERE case_id = ?", (case_id,)
        ).fetchone()
        assert case["escalation_level"] == 1


def test_should_create_case_logic(test_db):
    assert should_create_case({"status": "OPEN", "issue_code": "MISSING_INBOUND_FILE", "severity": "LOW"}) is True
    assert should_create_case({"status": "OPEN", "severity": "CRITICAL"}) is True
    assert should_create_case({"status": "OPEN", "severity": "HIGH"}) is True
    assert should_create_case({"status": "RESOLVED", "severity": "CRITICAL"}) is False
    assert should_create_case({"status": "OPEN", "severity": "LOW"}) is False


def test_build_case_payload(test_db):
    issue = {
        "issue_id": 1, "client_id": 1, "vendor_id": 1,
        "file_id": 10, "run_id": 5, "member_id": "M001",
        "claim_record_id": 100,
        "issue_code": "MISSING_INBOUND_FILE",
        "issue_message": "File missing",
        "severity": "CRITICAL", "issue_type": "FILE",
    }
    payload = build_case_payload(issue)
    assert payload["case_type"] == "MISSING_INBOUND_FILE"
    assert payload["priority"] == "CRITICAL"
    assert payload["assigned_team"] == "ops_file_queue"
    assert payload["escalation_level"] == 1
    assert "MISSING_INBOUND_FILE" in payload["title"]